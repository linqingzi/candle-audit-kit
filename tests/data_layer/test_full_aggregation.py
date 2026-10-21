"""Complete UTC buckets, missing data and observed zero quantity are different states."""

import importlib
from decimal import Decimal

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from test_full_collection import collector, history, native, request
from test_full_releases import api as releases_api

from candle_audit.data_layer import DataLayerError, Window


def api():
    assert importlib.util.find_spec("candle_audit.data_layer.aggregation"), (
        "closed-bucket aggregation absent"
    )
    return importlib.import_module("candle_audit.data_layer.aggregation")


def inputs(tmp_path, bundle, count=8):
    pipeline = collector(tmp_path)
    req = request(bundle, count=count, page_rows=300)
    rows = native(1767225600000, count)
    result = pipeline.collect(req, history(bundle, req, [rows]))
    store = releases_api().ReleaseStore(pipeline)
    release = store.create([result.collection_id], name="aggregate-test", producer_id="collector")
    return store, release, req


def test_aggregate_exact_complete_closed_buckets_and_immutable_derived_release(tmp_path, bundle):
    module = api()
    store, release, req = inputs(tmp_path, bundle)
    query = store.reader().query(release.release_id, req.window)
    result = module.aggregate_candles(query, "4h", req.window)
    assert result.table.num_rows == 2
    assert result.table["open"].to_pylist() == ["100.000000000000000001"] * 2
    assert result.table["high"].to_pylist() == ["102"] * 2
    assert result.table["base_qty"].to_pylist() == ["4"] * 2
    assert result.table["contracts_qty"].to_pylist() == ["40"] * 2
    assert result.table["state"].to_pylist() == ["AGGREGATED"] * 2
    assert result.report["status"] == "COMPLETE"
    derived = store.aggregate(
        release.release_id, "4h", req.window, name="four-hour", producer_id="collector"
    )
    assert (
        store.reader()
        .query(derived.release_id, req.window)
        .table.equals(result.table, check_metadata=True)
    )
    assert store.reader().query(release.release_id, req.window).table.num_rows == 8
    assert derived.document["derivation"]["source_release_id"] == release.release_id


def test_missing_or_partial_bucket_is_explicit_and_never_filled(tmp_path, bundle):
    module = api()
    store, release, req = inputs(tmp_path, bundle)
    query = store.reader().query(release.release_id, req.window)
    missing = query.table.take([0, 1, 3, 4, 5, 6, 7])
    altered = releases_api().DatasetResult(
        missing,
        query.release_id,
        query.window,
        query.mode,
        query.cache_key,
        query.approval_id,
        query.quality,
    )
    result = module.aggregate_candles(altered, "4h", req.window)
    assert result.table.num_rows == 1
    assert result.report["status"] == "INCOMPLETE"
    assert result.report["gaps"] == [
        {
            "instrument": "BTC-USDT-SWAP",
            "source_timeframe": "1h",
            "bucket_start_ns": 1767225600000000000,
            "status": "MISSING_COLLECTION",
            "missing_openings_ns": [1767232800000000000],
        }
    ]
    with pytest.raises(DataLayerError):
        store.aggregate(
            release.release_id,
            "4h",
            Window(req.window.start_ns, req.window.end_ns - 3600 * 10**9),
            name="partial",
            producer_id="collector",
        )


@settings(max_examples=1000, deadline=None, derandomize=True)
@given(st.lists(st.integers(0, 10**12), min_size=4, max_size=4))
def test_aggregation_quantity_conservation_has_independent_decimal_oracle(quantities):
    module = api()
    releases = releases_api()
    import pyarrow as pa

    rows = []
    for index, qty in enumerate(quantities):
        rows.append(
            dict(
                venue="OKX",
                instrument="BTC-USDT",
                market_kind="SPOT",
                price_kind="trade",
                base_asset="BTC",
                quote_asset="USDT",
                timeframe="1h",
                open_time_ns=1767225600000000000 + index * 3600 * 10**9,
                close_time_ns=1767225600000000000 + (index + 1) * 3600 * 10**9,
                event_time_ns=None,
                acquired_ns=1770000000000000000,
                published_ns=None,
                open="100.000000000000000001",
                high="102",
                low="99",
                close="101",
                base_qty=f"{qty}.001",
                quote_qty="100.1",
                contracts_qty=None,
                state="OBSERVED",
                rule_id="0" * 64,
            )
        )
    window = Window(1767225600000000000, 1767240000000000000)
    data = releases.DatasetResult(
        pa.Table.from_pylist(rows, schema=releases.CANDLE_SCHEMA),
        "0" * 64,
        window,
        "VERIFY",
        "0" * 64,
        None,
        {},
    )
    result = module.aggregate_candles(data, "4h", window)
    assert Decimal(result.table["base_qty"][0].as_py()) == Decimal(sum(quantities)) + Decimal(
        ".004"
    )
    assert Decimal(result.table["quote_qty"][0].as_py()) == Decimal("400.4")


@pytest.mark.parametrize("mismatch", [False, True])
def test_actual_two_granularity_consumer_reports_match_without_claiming_independence(
    tmp_path, bundle, mismatch
):
    import copy

    from test_full_reference import rules

    module = api()
    assert hasattr(module, "audit_granularity"), "cross-granularity audit missing"
    collection = importlib.import_module("candle_audit.data_layer.collection")
    pipeline = collector(tmp_path)
    window = Window(1767225600000000000, 1767232800000000000)
    ids = []
    for timeframe, seconds, bar in [("1m", 60, "1m"), ("1h", 3600, "1H")]:
        changed = copy.deepcopy(bundle)
        changed["source"]["bar"] = bar
        rule = rules(changed, timeframe=timeframe, effective=window)
        req = collection.CollectionRequest(
            provider_id="history",
            rules=(rule,),
            window=window,
            name="granularity",
            revision="r1",
            kind="synthetic",
            provenance="engineering/test-only",
            page_rows=300,
        )
        rows = native(window.start_ns // 10**6, 7200 // seconds, seconds * 1000)
        if timeframe == "1h":
            for row in rows:
                row[5:8] = ["600", "60", "6000"]
            if mismatch:
                rows[0][7] = "5999"
        ids.append(pipeline.collect(req, history(changed, req, [rows])).collection_id)
    store = releases_api().ReleaseStore(pipeline)
    release = store.create(ids, name="audit", producer_id="collector")
    fine = store.reader().query(release.release_id, window, timeframes=("1m",))
    coarse = store.reader().query(release.release_id, window, timeframes=("1h",))
    report = module.audit_granularity(fine, coarse, "1h", window)
    assert report["comparison"] == ("MISMATCH" if mismatch else "MATCH")
    assert report["source_independence"] == "NOT_ESTABLISHED"
    assert bool(report["differences"]) == mismatch
