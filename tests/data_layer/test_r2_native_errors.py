"""Native parser failures must obey the actual public CLI/API rejection protocol."""

import json
import subprocess
import sys

import pytest
from r2_helpers import resign
from test_store import publish

from candle_audit.data_layer import CandidateStore, DataLayerError, OfflineOKX, Window


def test_corrupt_native_parquet_is_a_structured_api_error(tmp_path, bundle):
    store = CandidateStore(tmp_path / "store")
    original = publish(store, bundle)
    bad, _ = resign(original, {"bars.parquet": b"PAR1-synthetic-invalid-parquet"})
    with pytest.raises(DataLayerError, match="Parquet"):
        store.query(bad, Window.parse(bundle["start"], bundle["end"]))


def test_native_sdk_missing_options_is_rejected_before_the_request(bundle):
    adapter = OfflineOKX(lambda url, method: bundle["raw"])
    adapter.exchange.options = None
    with pytest.raises(DataLayerError, match="options"):
        adapter.capture(
            bundle["source"], "1h", Window.parse(bundle["start"], bundle["end"]), raw_time_unit="ms"
        )


def test_corrupt_native_parquet_cli_rejects_json_and_preserves_old_snapshot(tmp_path, bundle):
    store = CandidateStore(tmp_path / "store")
    original = publish(store, bundle)
    before = {p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in original.path.iterdir()}
    bad, _ = resign(original, {"bars.parquet": b"PAR1-synthetic-invalid-parquet"})
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            "-m",
            "candle_audit.data_layer",
            "query",
            "--store",
            str(store.root),
            "--snapshot",
            bad,
            "--start",
            bundle["start"],
            "--end",
            bundle["end"],
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1
    assert "Traceback" not in result.stderr
    assert json.loads(result.stdout)["status"] == "REJECTED"
    assert before == {
        p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in original.path.iterdir()
    }
    assert (
        store.query(
            original.snapshot_id, Window.parse(bundle["start"], bundle["end"])
        ).table.num_rows
        == 2
    )
