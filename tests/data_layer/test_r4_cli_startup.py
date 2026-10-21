"""Cold CLI exits must not wait for data libraries that the request cannot use."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from test_consumer import arguments

HEAVY_MODULES = {"ccxt", "pandas", "numpy", "pyarrow", "duckdb"}


@pytest.fixture
def observed_python(tmp_path):
    """Observe imports without substituting any library or application result."""
    imports = tmp_path / "imports.txt"
    (tmp_path / "sitecustomize.py").write_text(
        "import sys\n"
        f"log = {str(imports)!r}\n"
        "with open(log, 'w') as stream:\n"
        "    stream.write('observer-ready\\n')\n"
        "class ObserveImports:\n"
        "    def find_spec(self, fullname, path=None, target=None):\n"
        f"        if fullname.split('.')[0] in {sorted(HEAVY_MODULES)!r}:\n"
        "            with open(log, 'a') as stream:\n"
        "                stream.write(fullname.split('.')[0] + '\\n')\n"
        "        return None\n"
        "sys.meta_path.insert(0, ObserveImports())\n"
    )
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(
        (str(tmp_path), str(Path(__file__).resolve().parents[2] / "src"))
    )

    def run(*args):
        result = subprocess.run(
            [sys.executable, "-B", *args],
            env=environment,
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        observed = imports.read_text().splitlines()
        assert observed[0] == "observer-ready"
        assert "Traceback" not in result.stderr, result.stderr
        return result, set(observed[1:])

    return run


@pytest.mark.parametrize("command", [[], ["ingest"], ["query"]])
def test_help_exits_before_loading_data_libraries(observed_python, command):
    result, imported = observed_python("-m", "candle_audit.data_layer", *command, "--help")
    assert result.returncode == 0 and "usage:" in result.stdout
    assert imported == set(), imported


@pytest.mark.parametrize("command", [[], ["ingest"], ["query", "--unknown"]])
def test_argument_errors_exit_before_loading_data_libraries(observed_python, command):
    result, imported = observed_python("-m", "candle_audit.data_layer", *command)
    assert result.returncode == 2 and "required" in result.stderr
    assert imported == set(), imported


@pytest.mark.parametrize("filename", ["bars.csv", "raw.json", "metadata.json", "source.json"])
@pytest.mark.parametrize("kind", ["fifo", "directory"])
def test_nonregular_input_is_rejected_before_loading_data_libraries(
    observed_python, tmp_path, bundle, filename, kind
):
    args = arguments(tmp_path, bundle)
    path = tmp_path / filename
    path.unlink()
    if kind == "fifo":
        os.mkfifo(path)
    else:
        path.mkdir()
    result, imported = observed_python("-m", "candle_audit.data_layer", *args)
    assert result.returncode == 1, result.stderr
    document = json.loads(result.stdout)
    assert document["status"] == "REJECTED"
    assert document["source_verification"] == "UNVERIFIED"
    assert document["admission"] == "NOT_EVALUATED"
    assert imported == set(), imported


def test_valid_ingest_still_loads_and_consumes_real_data_libraries(
    observed_python, tmp_path, bundle
):
    result, imported = observed_python("-m", "candle_audit.data_layer", *arguments(tmp_path, bundle))
    assert result.returncode == 0, result.stdout + result.stderr
    document = json.loads(result.stdout)
    assert document["status"] == "CANDIDATE_STORED"
    assert document["capture"]["ccxt_version"] == "4.5.64"
    assert document["capture"]["method"] == "publicGetMarketHistoryCandles"
    assert document["reports"]["quality"]["structural_status"] == "PASS"
    assert imported >= HEAVY_MODULES
    result, imported = observed_python(
        "-m",
        "candle_audit.data_layer",
        "query",
        "--store",
        str(tmp_path / "store"),
        "--snapshot",
        document["snapshot_id"],
        "--start",
        bundle["start"],
        "--end",
        bundle["end"],
        "--fields",
        "timestamp_ns",
        "open",
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout)["rows"] == [
        {"timestamp_ns": 1767225600000000000, "open": "100.00"},
        {"timestamp_ns": 1767229200000000000, "open": "101"},
    ]
    assert {"pyarrow", "duckdb"} <= imported


def test_public_exports_keep_identity_and_runtime_reflection(observed_python):
    script = """
import inspect
import typing
import candle_audit.data_layer as package
from candle_audit.data_layer import *
from candle_audit.data_layer import adapter, model, store
expected = {
    "CandidateStore": store.CandidateStore, "Capture": adapter.Capture,
    "DataLayerError": model.DataLayerError, "OfflineOKX": adapter.OfflineOKX,
    "QueryResult": store.QueryResult, "RejectedCandidate": model.RejectedCandidate,
    "Snapshot": store.Snapshot, "Window": model.Window, "timestamp_ns": model.timestamp_ns,
}
assert set(package.__all__) == set(expected)
assert set(expected) <= set(dir(package))
members = dict(inspect.getmembers(package))
for name, value in expected.items():
    assert getattr(package, name) is value is globals()[name] is members[name]
    typing.get_type_hints(value)
assert Window.parse(1, 2).start_ns == 1
assert issubclass(RejectedCandidate, DataLayerError)
try:
    package.missing_public_name
except AttributeError:
    pass
else:
    raise AssertionError("unknown public name did not raise AttributeError")
print("exports and reflection OK")
"""
    result, _ = observed_python("-c", script)
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "exports and reflection OK"
