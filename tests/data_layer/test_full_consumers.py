"""Actual whole-module CLI, Chinese report and task consumers with retained offline boundaries."""

import importlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from test_full_collection import request
from test_full_releases import built

from candle_audit.data_layer import DataLayerError


def cli(*args):
    result = subprocess.run(
        [sys.executable, "-B", "-m", "candle_audit.data_layer", *map(str, args)],
        cwd=Path(__file__).resolve().parents[2],
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert "Traceback" not in result.stderr, result.stderr
    return result, json.loads(result.stdout) if result.stdout.startswith("{") else None


def test_whole_cli_import_release_read_empty_error_task_and_report(tmp_path, bundle):
    req = request(bundle, count=2, provider="file")
    path = tmp_path / "request.json"
    path.write_text(json.dumps(req.as_dict()))
    (tmp_path / "raw.json").write_bytes(bundle["raw"])
    (tmp_path / "bars.csv").write_bytes(bundle["candidate_csv"])
    pages = tmp_path / "pages.json"
    pages.write_text(
        json.dumps(
            {"pages": [{"raw": str(tmp_path / "raw.json"), "csv": str(tmp_path / "bars.csv")}]}
        )
    )
    result, document = cli(
        "collect", "--root", tmp_path / "pipeline", "--request", path, "--pages", pages
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert document["status"] == "COMPLETE"
    result, released = cli(
        "release",
        "--root",
        tmp_path / "pipeline",
        "--collections",
        document["collection_id"],
        "--name",
        "cli",
        "--producer",
        "collector",
    )
    assert result.returncode == 0, result.stdout + result.stderr
    result, data = cli(
        "read",
        "--root",
        tmp_path / "pipeline",
        "--release",
        released["release_id"],
        "--start",
        bundle["start"],
        "--end",
        bundle["end"],
    )
    assert result.returncode == 0 and len(data["rows"]) == 2
    result, empty = cli(
        "read",
        "--root",
        tmp_path / "pipeline",
        "--release",
        released["release_id"],
        "--start",
        bundle["end"],
        "--end",
        "2026-01-01T03:00:00Z",
    )
    assert result.returncode == 0 and empty["query_status"] == "EMPTY"
    assert empty["schema"] == data["schema"]
    result, denied = cli(
        "read",
        "--root",
        tmp_path / "pipeline",
        "--release",
        released["release_id"],
        "--start",
        bundle["start"],
        "--end",
        bundle["end"],
        "--mode",
        "RESEARCH",
    )
    assert result.returncode == 1 and denied["status"] == "REJECTED"
    result, state = cli("task", "--root", tmp_path / "pipeline", "--request-id", req.request_id)
    assert result.returncode == 0 and state["task"]["status"] == "COMPLETE"
    result, report = cli(
        "report",
        "--root",
        tmp_path / "pipeline",
        "--release",
        released["release_id"],
        "--start",
        bundle["start"],
        "--end",
        bundle["end"],
        "--output",
        tmp_path / "reports",
    )
    assert result.returncode == 0, result.stdout + result.stderr
    html = Path(report["html_path"]).read_text()
    assert "工程合成测试数据" in html and "质量" in html and "K线" in html
    assert "100.00" in html and released["release_id"] in html
    assert Path(report["json_path"]).is_file()


def test_providers_and_module_named_exports_are_real():
    result, providers = cli("providers")
    assert result.returncode == 0
    assert set(providers["providers"]) == {"file-import", "okx-history"}
    package = importlib.import_module("candle_audit.data_layer")
    for name in (
        "CollectionRequest",
        "Collector",
        "ReleaseStore",
        "DatasetReader",
        "InstrumentRules",
        "Candle",
        "aggregate_candles",
    ):
        assert getattr(package, name), "missing actual public export " + name


def test_report_input_is_escaped_and_reader_stays_read_only(tmp_path, bundle, monkeypatch):
    assert importlib.util.find_spec("candle_audit.data_layer.reporting"), "report consumer absent"
    module = importlib.import_module("candle_audit.data_layer.reporting")
    releases, release, req = built(tmp_path, bundle)
    result = releases.reader().query(release.release_id, req.window)
    release.document["name"] = "</script><img src=x onerror=alert(1)>"
    html = module.render_report(result, release).decode()
    assert "<img src=x" not in html
    assert "&lt;/script&gt;" in html

    def forbidden(*args, **kwargs):
        raise AssertionError("read-only consumer must not publish")

    monkeypatch.setattr(releases.artifacts, "put", forbidden)
    assert releases.reader().query(release.release_id, req.window).table.num_rows == 4


@pytest.mark.parametrize(
    "command", ["collect", "release", "read", "aggregate", "report", "task", "benchmark"]
)
def test_new_cli_help_is_available(command):
    result, _ = cli(command, "--help")
    assert result.returncode == 0 and "usage:" in result.stdout


def test_live_acquisition_is_not_enabled_by_import_request(tmp_path, bundle):
    req = request(bundle, count=2)
    path = tmp_path / "request.json"
    path.write_text(json.dumps(req.as_dict()))
    result, refused = cli("collect", "--root", tmp_path / "pipeline", "--request", path)
    assert result.returncode == 1 and refused["status"] == "REJECTED"
    assert "授权" in refused["error"] or "grant" in refused["error"]


def test_benchmark_measures_current_reader_without_inventing_scale(tmp_path, bundle):
    releases, release, req = built(tmp_path, bundle)
    assert importlib.util.find_spec("candle_audit.data_layer.reporting"), (
        "resource measurement absent"
    )
    module = importlib.import_module("candle_audit.data_layer.reporting")
    result = module.measure_reader(releases.reader(), release.release_id, req.window, repeats=2)
    assert result["rows"] == 4 and len(result["seconds"]) == 2
    assert result["peak_rss_bytes"] > 0
    assert result["market_scale_slo"] == "NOT_VERIFIED"
    with pytest.raises(DataLayerError):
        module.measure_reader(releases.reader(), release.release_id, req.window, repeats=1000)
