"""Real selection/publication contrasts; all inputs are synthetic-engineering/test-only."""

import copy
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from test_final_contract_scope import scoped_release
from test_whole_review_aggregation import HOUR, START

from candle_audit.data_layer import Collector, DataLayerError, Window
from candle_audit.data_layer.aggregation import aggregate_candles
from candle_audit.data_layer.releases import ReleaseStore
from candle_audit.data_layer.store import canonical

ROOT = Path(__file__).resolve().parents[2]
WINDOW = Window(START, START + 8 * HOUR)


def _bytes(root):
    return {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in root.rglob("*")
        if p.is_file()
    }


def _assert_retained(root, before):
    after = _bytes(root)
    assert {name: after[name] for name in before} == before


def _assert_positive(root, source_id, derived_id, shape):
    fresh = ReleaseStore(Collector(root))
    derived = fresh.load(derived_id)
    assert derived.document["report"]["status"] == "COMPLETE"
    assert derived.document["report"]["scope"]["unmatched_timeframes"] == []
    assert fresh.catalog(derived_id)["report"] == derived.document["report"]
    rows = fresh.reader().query(derived_id, WINDOW).table.to_pylist()
    assets = ["BTC"] if shape == "single" else ["BTC", "ETH"]
    assert [(r["instrument"], r["open_time_ns"]) for r in rows] == [
        (f"{asset}-USDT-SWAP", opening) for asset in assets for opening in (START, START + 4 * HOUR)
    ]
    for row in rows:
        assert row["close_time_ns"] == row["open_time_ns"] + 4 * HOUR
        assert row["timeframe"] == "4h"
        assert row["open"] == "100.000000000000000001"
        assert (row["high"], row["low"], row["close"]) == ("102", "99", "101")
        assert (row["base_qty"], row["quote_qty"], row["contracts_qty"]) == ("4", "400", "40")
    native = fresh.reader().query(source_id, WINDOW)
    assert native.table.num_rows == 8 * len(assets)
    assert native.table["base_qty"].to_pylist() == ["1"] * (8 * len(assets))
    assert fresh.catalog(source_id)["kind"] == "NATIVE"


def _cli(record, root, source_id, filters):
    argv = [
        sys.executable,
        "-B",
        "-m",
        "candle_audit.data_layer",
        "aggregate",
        "--root",
        str(root),
        "--release",
        source_id,
        "--start",
        "2026-01-01T00:00:00Z",
        "--end",
        "2026-01-01T08:00:00Z",
        "--timeframe",
        "4h",
        "--name",
        "basic-selection",
        "--producer",
        "collector",
        "--source-timeframes",
        *filters,
    ]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT / "src")
    result = subprocess.run(
        argv, cwd=ROOT, env=env, capture_output=True, text=True, timeout=60, check=False
    )
    record.write_text(
        json.dumps(
            {
                "argv": argv,
                "cwd": str(ROOT),
                "exit_code": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            },
            indent=2,
        )
        + "\n"
    )
    assert "Traceback" not in result.stderr
    return result, json.loads(result.stdout)


@pytest.mark.parametrize("shape", ["single", "complete"])
@pytest.mark.parametrize("filters", [("1h", "1m"), ("1m", "1h"), ("1m",)])
def test_api_missing_requested_timeframe_cannot_publish_after_same_source_success(
    tmp_path, bundle, shape, filters
):
    store, source = scoped_release(tmp_path, bundle, shape)
    retained = _bytes(store.collector.root)
    positive = store.aggregate(
        source.release_id,
        "4h",
        WINDOW,
        name="basic-positive",
        producer_id="collector",
        source_timeframes=("1h",),
    )
    _assert_positive(store.collector.root, source.release_id, positive.release_id, shape)
    before = _bytes(store.collector.root)
    data = store.reader().query(source.release_id, WINDOW, timeframes=filters)
    pure = aggregate_candles(data, "4h", WINDOW)
    assert pure.report["status"] == "INCOMPLETE"
    assert pure.report["scope"]["unmatched_timeframes"] == ["1m"]
    with pytest.raises(DataLayerError, match="source timeframe|complete"):
        store.aggregate(
            source.release_id,
            "4h",
            WINDOW,
            name="basic-missing",
            producer_id="collector",
            source_timeframes=filters,
        )
    assert _bytes(store.collector.root) == before
    _assert_retained(store.collector.root, retained)
    _assert_positive(store.collector.root, source.release_id, positive.release_id, shape)


@pytest.mark.parametrize("shape", ["single", "complete"])
@pytest.mark.parametrize("filters", [("1h", "1m"), ("1m", "1h"), ("1m",)])
def test_actual_cli_missing_requested_timeframe_cannot_publish_after_same_source_success(
    tmp_path, bundle, shape, filters
):
    store, source = scoped_release(tmp_path, bundle, shape)
    retained = _bytes(store.collector.root)
    result, positive = _cli(
        tmp_path / "positive-cli.json", store.collector.root, source.release_id, ("1h",)
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert positive["status"] == "DERIVED_VERIFY_RELEASE_CREATED"
    _assert_positive(store.collector.root, source.release_id, positive["release_id"], shape)
    before = _bytes(store.collector.root)
    result, negative = _cli(
        tmp_path / "missing-cli.json", store.collector.root, source.release_id, filters
    )
    assert result.returncode == 1, result.stdout + result.stderr
    assert negative["status"] == "REJECTED"
    assert "release_id" not in negative
    assert "source timeframe" in negative["error"] or "complete" in negative["error"]
    assert _bytes(store.collector.root) == before
    _assert_retained(store.collector.root, retained)
    _assert_positive(store.collector.root, source.release_id, positive["release_id"], shape)


@pytest.mark.parametrize("shape", ["single", "complete"])
def test_fresh_consumers_reject_complete_manifest_claiming_absent_source_timeframe(
    tmp_path, bundle, shape
):
    store, source = scoped_release(tmp_path, bundle, shape)
    positive = store.aggregate(
        source.release_id,
        "4h",
        WINDOW,
        name="basic-positive",
        producer_id="collector",
        source_timeframes=("1h",),
    )
    # A real successful publication's source/window/table stay fixed; only the request changes.
    invalid = copy.deepcopy(positive.document)
    invalid["derivation"]["source_timeframes"] = ["1h", "1m"]
    invalid["report"]["scope"]["unmatched_timeframes"] = ["1m"]
    invalid_id = store.artifacts.put(canonical(invalid))
    fresh = ReleaseStore(Collector(store.collector.root))
    for consume in (
        lambda: fresh.load(invalid_id),
        lambda: fresh.catalog(invalid_id),
        lambda: fresh.reader().query(invalid_id, WINDOW),
    ):
        with pytest.raises(DataLayerError, match="invalid release semantics"):
            consume()
    _assert_positive(store.collector.root, source.release_id, positive.release_id, shape)
