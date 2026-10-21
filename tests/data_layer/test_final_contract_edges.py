"""Independent single-premise consumer boundaries, including successful retained behavior."""

import copy
from dataclasses import replace

import pyarrow as pa
import pytest
from test_full_aggregation import inputs
from test_whole_review_aggregation import HOUR, START, native_pair, typed_bars

from candle_audit.contract.data_versions import metadata, volume_metadata
from candle_audit.contract.registry import RegistryError
from candle_audit.data_layer import DataLayerError, Window
from candle_audit.data_layer.aggregation import (
    aggregate_candles,
    audit_granularity,
    declare_reconstruction,
)
from candle_audit.data_layer.releases import CANDLE_SCHEMA
from candle_audit.data_layer.reporting import render_report


@pytest.mark.parametrize(
    "change,message",
    [
        ("outside", "inside"),
        ("duplicate", "duplicate"),
        ("timeframe", "coarser"),
        ("two-timeframes", "one source timeframe"),
        ("close", "unclosed"),
        ("acquired", "unclosed"),
        ("identity", "cross-market"),
        ("contracts", "quantity units"),
        ("decimal-output", "decimal cell budget"),
    ],
)
def test_aggregate_one_invalid_premise_is_rejected_after_complete_positive(change, message):
    fine = typed_bars([1] * 8)
    assert aggregate_candles(fine, "4h", fine.window).table["base_qty"].to_pylist() == ["4", "4"]
    rows, window, target = fine.table.to_pylist(), fine.window, "4h"
    if change == "outside":
        window = Window(START - 4 * HOUR, window.end_ns)
    elif change == "duplicate":
        rows.append(rows[0].copy())
    elif change == "timeframe":
        target = "1h"
    elif change == "two-timeframes":
        rows.append(rows[0] | {"timeframe": "4h"})
    elif change == "close":
        rows[0]["close_time_ns"] -= 1
    elif change == "acquired":
        rows[0]["acquired_ns"] = rows[0]["close_time_ns"] - 1
    elif change == "identity":
        rows[0]["quote_asset"] = "USDC"
    elif change == "contracts":
        rows[0]["contracts_qty"] = None
    else:
        for row in rows:
            row["base_qty"] = "1e240"
    altered = replace(fine, table=pa.Table.from_pylist(rows, schema=CANDLE_SCHEMA))
    with pytest.raises(DataLayerError, match=message):
        aggregate_candles(altered, target, window)


def test_subwindow_excludes_outside_rows_and_retains_exact_publication_max():
    data = typed_bars([1] * 8)
    rows = [r | {"published_ns": r["close_time_ns"] + 100} for r in data.table.to_pylist()]
    data = replace(data, table=pa.Table.from_pylist(rows, schema=CANDLE_SCHEMA))
    window = Window(START, START + 4 * HOUR)
    result = aggregate_candles(data, "4h", window)
    assert result.report["status"] == "COMPLETE" and result.table.num_rows == 1
    assert result.table["base_qty"].to_pylist() == ["4"]
    assert result.table["published_ns"].to_pylist() == [window.end_ns + 100]
    assert result.report["scope"]["assessed_streams"][0]["returned_count"] == 4


@pytest.mark.parametrize(
    "change",
    ["timeframe", "duplicate", "venue", "market_kind", "price_kind", "base_asset", "quote_asset"],
)
def test_audit_one_invalid_coarse_premise_rejects_after_native_match(change):
    fine, coarse = native_pair()
    assert audit_granularity(fine, coarse, "4h", fine.window)["comparison"] == "MATCH"
    rows = coarse.table.to_pylist()
    if change == "duplicate":
        rows.append(rows[0].copy())
    else:
        rows[0][change] = "1h" if change == "timeframe" else "DIFFERENT"
    coarse = replace(coarse, table=pa.Table.from_pylist(rows, schema=CANDLE_SCHEMA))
    with pytest.raises(DataLayerError):
        audit_granularity(fine, coarse, "4h", fine.window)


def test_reconstruction_evidence_and_report_size_guards_have_legal_controls():
    data = typed_bars([1] * 8)
    assert declare_reconstruction(data, [START], evidence=b"test-only").table.num_rows == 8
    with pytest.raises(DataLayerError, match="evidence"):
        declare_reconstruction(data, [START], evidence=b"")
    from candle_audit.data_layer.releases import Release

    release = Release(
        data.release_id, {"name": "engineering", "lineage": {"collections": {}}, "parent_id": None}
    )
    assert b"<!doctype html>" in render_report(data, release)
    # The actual canonical reader cap is retained: bounded 10000 rows, no oversized input file.
    rows = typed_bars([1] * 10000).table.to_pylist()
    for row in rows:
        row.update(
            open="1." + "0" * 90, high="2." + "0" * 90, low="0." + "1" * 90, close="1." + "0" * 90
        )
    many = replace(
        data,
        table=pa.Table.from_pylist(rows, schema=CANDLE_SCHEMA),
        window=Window(START, START + 10000 * HOUR),
    )
    with pytest.raises(DataLayerError, match="report byte budget"):
        render_report(many, release)


@pytest.mark.parametrize("limit", [0, True, 10001])
def test_snapshot_budget_refuses_invalid_limit_while_normal_query_is_retained(
    tmp_path, bundle, limit
):
    store, release, req = inputs(tmp_path, bundle)
    snapshot = release.document["partitions"][0]["snapshot_id"]
    assert store.collector.candidates.query(snapshot, req.window).table.num_rows == 2
    with pytest.raises(DataLayerError, match="query row limit"):
        store.collector.candidates.query(snapshot, req.window, limit=limit)


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema", "unknown"),
        ("name", "bad name"),
        ("content_sha256", "0"),
        ("kind", "historical"),
        ("source", "other-source"),
    ],
)
def test_helper_metadata_guard_rejects_one_bad_field_and_keeps_valid_declaration(
    bundle, field, value
):
    good = bundle["metadata"]
    assert metadata(good) == good
    bad = copy.deepcopy(good)
    bad[field] = value
    with pytest.raises(RegistryError):
        metadata(bad)


@pytest.mark.parametrize(
    "change",
    [
        "asset",
        "same-assets",
        "no-columns",
        "not-descriptor",
        "name",
        "duplicate",
        "size",
        "size-bool",
        "size-overflow",
        "size-unit",
    ],
)
def test_helper_unit_metadata_checks_are_not_suppressed_by_other_fields(change):
    good = {
        "base_asset": "BTC",
        "quote_asset": "USDT",
        "columns": [
            {
                "column": "qty",
                "unit": "contracts",
                "contract_size": 0.1,
                "contract_size_unit": "base",
            }
        ],
    }
    assert volume_metadata(good) == good["columns"]
    bad = copy.deepcopy(good)
    if change == "asset":
        bad["base_asset"] = "bad asset"
    elif change == "same-assets":
        bad["quote_asset"] = "BTC"
    elif change == "no-columns":
        bad["columns"] = []
    elif change == "not-descriptor":
        bad["columns"] = [None]
    elif change == "duplicate":
        bad["columns"] *= 2
    elif change == "name":
        bad["columns"][0]["column"] = "timestamp"
    elif change == "size":
        bad["columns"][0]["contract_size"] = -1.0
    elif change == "size-bool":
        bad["columns"][0]["contract_size"] = True
    elif change == "size-overflow":
        bad["columns"][0]["contract_size"] = 10**1000
    else:
        bad["columns"][0]["contract_size_unit"] = "unknown"
    with pytest.raises(RegistryError):
        volume_metadata(bad)
