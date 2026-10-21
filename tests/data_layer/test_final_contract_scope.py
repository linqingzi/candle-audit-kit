"""Selection follows every consumer; window-scoped persistence retains real V1 shapes."""

import copy
import hashlib
import json
import shutil
from dataclasses import replace
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from test_full_collection import collector, history, native
from test_full_reference import rules
from test_whole_review_aggregation import HOUR, START, native_pair, typed_bars

from candle_audit.data_layer import CollectionRequest, Collector, DataLayerError, Window
from candle_audit.data_layer.aggregation import aggregate_candles, audit_granularity
from candle_audit.data_layer.releases import ReleaseStore
from candle_audit.data_layer.reporting import report_document


def scoped_release(tmp_path, bundle, shape="separated"):
    pipeline = collector(tmp_path)
    specs = [("BTC", "1h", "1H", START, 8, HOUR)]
    if shape != "single":
        specs.append(
            ("BTC", "4h", "4H", START + 24 * HOUR, 2, 4 * HOUR)
            if shape == "dualtf"
            else ("BTC", "4h", "4H", START, 2, 4 * HOUR)
            if shape == "dualtf_inside"
            else ("ETH", "1h", "1H", START + 24 * HOUR, 8, HOUR)
            if shape == "separated"
            else ("ETH", "1h", "1H", START, 8, HOUR)
        )
    ids = []
    for asset, timeframe, bar, start, count, step in specs:
        changed = copy.deepcopy(bundle)
        changed["source"].update(base_asset=asset, instId=f"{asset}-USDT-SWAP", bar=bar)
        window = Window(start, start + count * step)
        rule = rules(changed, timeframe=timeframe, effective=window)
        req = CollectionRequest(
            provider_id="history",
            window=window,
            rules=(rule,),
            page_rows=300,
            name="scope",
            revision="r1",
            kind="synthetic",
            provenance="test-only",
        )
        rows = native(start // 10**6, count, step // 10**6)
        ids.append(pipeline.collect(req, history(changed, req, [rows])).collection_id)
    store = ReleaseStore(pipeline)
    return store, store.create(ids, name="scope", producer_id="collector")


def scope_assets(scope, field):
    return {s["instrument"] for s in scope[field]}


@pytest.mark.parametrize("shape", ["separated", "complete"])
def test_real_reader_selection_is_assessed_or_disclosed_by_pure_aggregation(
    tmp_path, bundle, shape
):
    store, release = scoped_release(tmp_path, bundle, shape)
    window = Window(START, START + 8 * HOUR)
    data = store.reader().query(release.release_id, window)
    result = aggregate_candles(data, "4h", window)
    scope = result.report["scope"]
    assert scope["universe"] == "SELECTED"
    assert scope_assets(scope, "requested_streams") == {"BTC-USDT-SWAP", "ETH-USDT-SWAP"}
    assert scope_assets(scope, "assessed_streams") == (
        {"BTC-USDT-SWAP"} if shape == "separated" else {"BTC-USDT-SWAP", "ETH-USDT-SWAP"}
    )
    assert scope_assets(scope, "unavailable_streams") == (
        {"ETH-USDT-SWAP"} if shape == "separated" else set()
    )
    assert result.report["status"] == ("INCOMPLETE" if shape == "separated" else "COMPLETE")
    assert result.table.num_rows == (2 if shape == "separated" else 4)
    selected = store.reader().query(release.release_id, window, instruments=("BTC-USDT-SWAP",))
    positive = aggregate_candles(selected, "4h", window)
    assert positive.report["status"] == "COMPLETE"
    assert scope_assets(positive.report["scope"], "requested_streams") == {"BTC-USDT-SWAP"}
    assert positive.report["scope"]["unavailable_streams"] == []


def selection(timeframe):
    return {
        "status": "RELEASE_STREAMS_SELECTED",
        "streams": [
            {
                "instrument": f"{a}-USDT-SWAP",
                "timeframe": timeframe,
                "coverage": [{"start_ns": START, "end_ns": START + 8 * HOUR}],
            }
            for a in ("BTC", "ETH")
        ],
    }


@pytest.mark.parametrize("side", ["fine", "coarse", "both"])
def test_audit_cannot_match_an_unassessed_selected_stream_even_when_btc_matches(side):
    fine, coarse = native_pair()
    fine = replace(fine, selection=selection("1h") if side in ("fine", "both") else {})
    coarse = replace(coarse, selection=selection("4h") if side in ("coarse", "both") else {})
    result = audit_granularity(fine, coarse, "4h", fine.window)
    assert result["comparison"] == "NOT_COMPARABLE"
    assert result["differences"] == []
    assert any(
        r["reason"] == "UNAVAILABLE_SELECTED_STREAMS" for r in result["comparability_reasons"]
    )
    assert result["scope"]["fine"]["universe"] == ("SELECTED" if side != "coarse" else "UNKNOWN")
    assert result["scope"]["coarse"]["universe"] == ("SELECTED" if side != "fine" else "UNKNOWN")


@pytest.mark.parametrize("outside", [False, True])
def test_two_real_native_granularities_disclose_unassessed_eth_and_explicit_btc_matches(
    tmp_path, bundle, outside
):
    pipeline = collector(tmp_path)
    ids = []
    window = Window(START, START + 8 * HOUR)
    for asset in ("BTC", "ETH"):
        for timeframe, bar, count, step in (("1h", "1H", 8, HOUR), ("4h", "4H", 2, 4 * HOUR)):
            changed = copy.deepcopy(bundle)
            changed["source"].update(base_asset=asset, instId=f"{asset}-USDT-SWAP", bar=bar)
            start = START + (24 * HOUR if asset == "ETH" and outside else 0)
            covered = Window(start, start + 8 * HOUR)
            req = CollectionRequest(
                provider_id="history",
                window=covered,
                rules=(rules(changed, timeframe=timeframe, effective=covered),),
                page_rows=300,
                name="native-pair",
                revision="r1",
                kind="synthetic",
                provenance="test-only",
            )
            rows = native(start // 10**6, count, step // 10**6)
            if timeframe == "4h":
                for row in rows:
                    row[5:8] = ["40", "4", "400"]
            ids.append(pipeline.collect(req, history(changed, req, [rows])).collection_id)
    store = ReleaseStore(pipeline)
    release = store.create(ids, name="native-pair", producer_id="collector")
    fine = store.reader().query(release.release_id, window, timeframes=("1h",))
    coarse = store.reader().query(release.release_id, window, timeframes=("4h",))
    report = audit_granularity(fine, coarse, "4h", window)
    assert report["comparison"] == ("NOT_COMPARABLE" if outside else "MATCH")
    assert report["differences"] == []
    for side in ("fine", "coarse"):
        assert len(report["scope"][side]["requested_streams"]) == 2
        assert scope_assets(report["scope"][side], "unavailable_streams") == (
            {"ETH-USDT-SWAP"} if outside else set()
        )
    btc_fine = store.reader().query(
        release.release_id, window, timeframes=("1h",), instruments=("BTC-USDT-SWAP",)
    )
    btc_coarse = store.reader().query(
        release.release_id, window, timeframes=("4h",), instruments=("BTC-USDT-SWAP",)
    )
    assert audit_granularity(btc_fine, btc_coarse, "4h", window)["comparison"] == "MATCH"


def test_unknown_universe_empty_and_partial_selected_streams_remain_distinct():
    data = typed_bars([1] * 8)
    assert aggregate_candles(data, "4h", data.window).report["scope"]["universe"] == "UNKNOWN"
    selected = replace(data, selection=selection("1h"))
    empty = replace(selected, table=selected.table.slice(0, 0))
    output = aggregate_candles(empty, "4h", empty.window)
    assert output.table.num_rows == 0 and output.report["status"] == "INCOMPLETE"
    assert len(output.report["scope"]["unavailable_streams"]) == 2
    partial = replace(
        data,
        table=data.table.take([0, 1, 3, 4, 5, 6, 7]),
        selection={"status": "RELEASE_STREAMS_SELECTED", "streams": selection("1h")["streams"][:1]},
    )
    result = aggregate_candles(partial, "4h", partial.window)
    assert result.report["status"] == "INCOMPLETE" and result.table.num_rows == 1
    assert result.report["scope"]["unavailable_streams"][0]["missing_count"] == 1


@pytest.mark.parametrize(
    "field,value", [("unmatched_instruments", "ADA-USDT-SWAP"), ("unmatched_timeframes", "1m")]
)
def test_unmatched_requested_filter_cannot_be_hidden_by_other_complete_rows(field, value):
    fine, coarse = native_pair()
    fine = replace(fine, selection={"streams": selection("1h")["streams"][:1], field: [value]})
    aggregate = aggregate_candles(fine, "4h", fine.window)
    assert aggregate.table.num_rows == 2 and aggregate.report["status"] == "INCOMPLETE"
    assert aggregate.report["scope"][field] == [value]
    assert audit_granularity(fine, coarse, "4h", fine.window)["comparison"] == "NOT_COMPARABLE"


def test_rows_cannot_claim_a_different_explicit_selection_and_empty_selection_is_typed():
    data = typed_bars([1] * 8)
    wrong = replace(data, selection={"streams": selection("1h")["streams"][1:]})
    with pytest.raises(DataLayerError, match="contradict.*selected"):
        aggregate_candles(wrong, "4h", data.window)
    empty = replace(data, table=data.table.slice(0, 0), selection={"streams": []})
    output = aggregate_candles(empty, "4h", data.window)
    assert output.report["status"] == "EMPTY"
    assert output.report["scope"]["universe"] == "SELECTED"
    assert output.report["scope"]["requested_streams"] == []


@pytest.mark.parametrize("shape", ["single", "separated", "dualtf", "complete"])
def test_persistent_transform_excludes_only_wholly_outside_streams_and_all_consumers_load(
    tmp_path, bundle, shape
):
    store, source = scoped_release(tmp_path, bundle, shape)
    window = Window(START, START + 8 * HOUR)
    derived = store.aggregate(
        source.release_id, "4h", window, name="scoped", producer_id="collector"
    )
    scope = derived.document["report"]["scope"]
    assert len(scope["excluded_streams"]) == (1 if shape in ("separated", "dualtf") else 0)
    assert scope["unavailable_streams"] == []
    assert len(scope["requested_streams"]) == (2 if shape == "complete" else 1)
    fresh = ReleaseStore(collector(tmp_path))
    assert fresh.load(derived.release_id).document == derived.document
    assert fresh.catalog(derived.release_id)["kind"] == "DERIVED"
    result = fresh.reader().query(derived.release_id, window)
    assert result.table.num_rows == (4 if shape == "complete" else 2)
    assert result.table["base_qty"].to_pylist() == ["4"] * result.table.num_rows
    assert report_document(result, derived)["query_gaps"] == []
    assert store.reader().query(source.release_id, window).release_id == source.release_id


def test_window_overlapping_incomplete_stream_cannot_be_silently_excluded(tmp_path, bundle):
    store, source = scoped_release(tmp_path, bundle, "single")
    with pytest.raises(DataLayerError, match="complete"):
        store.aggregate(
            source.release_id,
            "4h",
            Window(START, START + 12 * HOUR),
            name="partial",
            producer_id="collector",
        )


def test_explicit_source_timeframe_exclusion_is_recorded_and_default_ambiguity_rejected(
    tmp_path, bundle
):
    store, source = scoped_release(tmp_path, bundle, "dualtf_inside")
    window = Window(START, START + 8 * HOUR)
    with pytest.raises(DataLayerError, match="one source timeframe"):
        store.aggregate(source.release_id, "4h", window, name="ambiguous", producer_id="collector")
    derived = store.aggregate(
        source.release_id,
        "4h",
        window,
        source_timeframes=("1h",),
        name="selected",
        producer_id="collector",
    )
    excluded = derived.document["report"]["scope"]["excluded_streams"]
    assert len(excluded) == 1 and excluded[0]["timeframe"] == "4h"
    assert excluded[0]["reason"] == "SOURCE_TIMEFRAME_FILTER"
    assert store.load(derived.release_id).document == derived.document


@pytest.mark.parametrize("shape", ["single", "separated", "dualtf"])
@pytest.mark.skip(reason="Pre-standalone historical V1 artifacts were not supplied; backward compatibility is not claimed")
def test_real_pristine_v1_generated_artifacts_load_query_catalog_and_report_unchanged(
    tmp_path, shape
):
    root = Path(__file__).resolve().parents[2]
    control = json.loads(
        (
            root
            / "tests/fixtures/external-v1"
            / "CONTROL.json"
        ).read_text()
    )
    record = next(r for r in control["records"] if r["shape"] == shape)
    original = Path(record["pipeline"])
    before = {
        p: hashlib.sha256((original / p).read_bytes()).hexdigest() for p in record["original_bytes"]
    }
    assert before == record["original_bytes"]
    shutil.copytree(original, tmp_path / "legacy")
    store = ReleaseStore(Collector(tmp_path / "legacy"))
    release = store.load(record["derived"])
    window = Window(**release.document["derivation"]["window"])
    assert store.catalog(record["source"])["kind"] == "NATIVE"
    assert store.catalog(record["derived"])["kind"] == "DERIVED"
    result = store.reader().query(record["derived"], window)
    assert result.table.num_rows == 2 and result.table["base_qty"].to_pylist() == ["4", "4"]
    assert report_document(result, release)["query_gaps"] == []
    assert before == {p: hashlib.sha256((original / p).read_bytes()).hexdigest() for p in before}


@pytest.mark.parametrize("shape", ["single", "separated", "dualtf"])
def test_standalone_release_relocates_with_unchanged_bytes_and_semantics(tmp_path, bundle, shape):
    store, source = scoped_release(tmp_path, bundle, shape)
    window = Window(START, START + 8 * HOUR)
    derived = store.aggregate(source.release_id, "4h", window, name="portable", producer_id="collector")
    original = store.collector.root
    before = {
        str(p.relative_to(original)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in original.rglob("*") if p.is_file()
    }
    relocated = tmp_path / "relocated"
    shutil.copytree(original, relocated)
    reader = ReleaseStore(Collector(relocated))
    assert reader.catalog(source.release_id)["kind"] == "NATIVE"
    assert reader.catalog(derived.release_id)["kind"] == "DERIVED"
    result = reader.reader().query(derived.release_id, window)
    assert result.table.num_rows == 2
    assert result.table["base_qty"].to_pylist() == ["4", "4"]
    assert report_document(result, reader.load(derived.release_id))["query_gaps"] == []
    assert before == {
        str(p.relative_to(relocated)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in relocated.rglob("*") if p.is_file()
    }


@settings(max_examples=1000, deadline=None, derandomize=True)
@given(st.integers(0, 7), st.integers(1, 10**10))
def test_selection_missing_one_opening_never_reports_complete_and_preserves_other_bucket(
    index, qty
):
    data = typed_bars([qty] * 8)
    selected = replace(
        data,
        selection={"status": "RELEASE_STREAMS_SELECTED", "streams": selection("1h")["streams"][:1]},
    )
    partial = replace(selected, table=selected.table.take([i for i in range(8) if i != index]))
    result = aggregate_candles(partial, "4h", data.window)
    assert result.report["status"] == "INCOMPLETE"
    assert result.table.num_rows == 1
    assert result.table["base_qty"].to_pylist() == [str(4 * qty)]
    assert result.table["open_time_ns"].to_pylist() == [START + (4 if index < 4 else 0) * HOUR]
    assert result.report["scope"]["unavailable_streams"][0]["missing_count"] == 1


@pytest.mark.parametrize("field", ["scope", "status"])
def test_new_derived_scope_or_status_tampering_is_rejected(tmp_path, bundle, field):
    from candle_audit.data_layer.store import canonical

    store, source = scoped_release(tmp_path, bundle, "single")
    derived = store.aggregate(
        source.release_id,
        "4h",
        Window(START, START + 8 * HOUR),
        name="new",
        producer_id="collector",
    )
    doc = copy.deepcopy(derived.document)
    doc["report"][field] = {} if field == "scope" else "EMPTY"
    with pytest.raises(DataLayerError):
        store.load(store.artifacts.put(canonical(doc)))
