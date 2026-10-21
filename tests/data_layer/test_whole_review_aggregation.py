"""A-F02/A-F03: rule and state provenance across real canonical consumers."""

from dataclasses import replace
from decimal import Decimal

import pyarrow as pa
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from test_full_collection import collector, history, native, request
from test_full_reference import rules

from candle_audit.data_layer import DataLayerError, Window
from candle_audit.data_layer.aggregation import (
    aggregate_candles,
    audit_granularity,
    declare_reconstruction,
)
from candle_audit.data_layer.releases import CANDLE_SCHEMA, DatasetResult, ReleaseStore

START = 1767225600000000000
HOUR = 3600 * 10**9


def rule_release(tmp_path, bundle, boundary, factor="0.2"):
    window = Window(START, START + 8 * HOUR)
    first = rules(bundle, effective=Window(START, START + boundary * HOUR))
    second = rules(
        bundle,
        effective=Window(first.effective.end_ns, window.end_ns),
        contract_size=factor,
        basis="OBSERVED",
        rule_evidence=b"later-evidence",
    )
    req = request(bundle, count=8, rules=(first, second), page_rows=300, shard_rows=10000)
    rows = native(START // 10**6, 8)
    for row in rows:
        if int(row[0]) * 10**6 >= second.effective.start_ns:
            row[6] = str(Decimal(10) * Decimal(factor))
    pipeline = collector(tmp_path)
    collection = pipeline.collect(req, history(bundle, req, [rows]))
    store = ReleaseStore(pipeline)
    release = store.create([collection.collection_id], name="rules", producer_id="collector")
    return store, release, req, (first, second)


@pytest.mark.parametrize("factor", ["0.2", "0.1000"])
def test_bucket_inside_rule_boundary_cannot_claim_only_first_rule(tmp_path, bundle, factor):
    store, release, req, _ = rule_release(tmp_path, bundle, 2, factor)
    data = store.reader().query(release.release_id, req.window)
    with pytest.raises(DataLayerError, match="rule"):
        aggregate_candles(data, "4h", req.window)
    with pytest.raises(DataLayerError, match="rule"):
        store.aggregate(release.release_id, "4h", req.window, name="mixed", producer_id="collector")
    assert store.reader().query(release.release_id, req.window).table.num_rows == 8


@pytest.mark.parametrize("factor", ["0.2", "0.1000"])
def test_rule_boundary_on_bucket_boundary_preserves_each_identity_and_reload(
    tmp_path, bundle, factor
):
    store, release, req, refs = rule_release(tmp_path, bundle, 4, factor)
    derived = store.aggregate(
        release.release_id, "4h", req.window, name="aligned", producer_id="collector"
    )
    data = store.reader().query(derived.release_id, req.window)
    assert data.table["rule_id"].to_pylist() == [r.rule_id for r in refs]
    assert [Decimal(v) for v in data.table["base_qty"].to_pylist()] == [
        Decimal(4),
        40 * Decimal(factor),
    ]
    assert data.table["contracts_qty"].to_pylist() == ["40", "40"]
    assert ReleaseStore(store.collector).load(derived.release_id).document == derived.document


def typed_bars(quantities, rule_ids=None, states=None):
    rows = [
        dict(
            venue="OKX",
            instrument="BTC-USDT-SWAP",
            market_kind="SWAP",
            price_kind="trade",
            base_asset="BTC",
            quote_asset="USDT",
            timeframe="1h",
            open_time_ns=START + i * HOUR,
            close_time_ns=START + (i + 1) * HOUR,
            event_time_ns=None,
            acquired_ns=1770000000000000000,
            published_ns=None,
            open="100",
            high="102",
            low="99",
            close="101",
            base_qty=str(q),
            quote_qty=str(q * 100),
            contracts_qty=str(q * 10),
            state=states[i] if states else "OBSERVED",
            rule_id=rule_ids[i] if rule_ids else "0" * 64,
        )
        for i, q in enumerate(quantities)
    ]
    return DatasetResult(
        pa.Table.from_pylist(rows, schema=CANDLE_SCHEMA),
        "0" * 64,
        Window(START, START + len(rows) * HOUR),
        "VERIFY",
        "0" * 64,
        None,
        {},
    )


@settings(max_examples=1000, deadline=None, derandomize=True)
@given(st.integers(1, 3), st.lists(st.integers(0, 10**12), min_size=4, max_size=4))
def test_every_mixed_rule_bucket_is_rejected_while_fixed_bucket_conserves_quantities(split, qs):
    legal = typed_bars(qs)
    result = aggregate_candles(legal, "4h", legal.window)
    assert Decimal(result.table["base_qty"][0].as_py()) == Decimal(sum(qs))
    assert Decimal(result.table["contracts_qty"][0].as_py()) == Decimal(sum(qs) * 10)
    mixed = typed_bars(qs, ["0" * 64] * split + ["1" * 64] * (4 - split))
    with pytest.raises(DataLayerError, match="rule"):
        aggregate_candles(mixed, "4h", mixed.window)


def native_pair():
    fine = typed_bars([1] * 8)
    aggregate = aggregate_candles(fine, "4h", fine.window)
    coarse_rows = [r | {"state": "OBSERVED"} for r in aggregate.table.to_pylist()]
    coarse = replace(fine, table=pa.Table.from_pylist(coarse_rows, schema=CANDLE_SCHEMA))
    return fine, coarse


@pytest.mark.parametrize("side", ["fine", "coarse"])
@pytest.mark.parametrize("mismatch", [False, True])
def test_either_public_reconstruction_disqualifies_native_comparison_but_retains_numbers(
    side, mismatch
):
    fine, coarse = native_pair()
    if mismatch:
        rows = coarse.table.to_pylist()
        rows[0]["quote_qty"] = "399"
        coarse = replace(coarse, table=pa.Table.from_pylist(rows, schema=CANDLE_SCHEMA))
    initial = audit_granularity(fine, coarse, "4h", fine.window)
    assert initial["comparison"] == ("MISMATCH" if mismatch else "MATCH")
    chosen = fine if side == "fine" else coarse
    marked = declare_reconstruction(chosen, [START], evidence=b"engineering reconstruction")
    report = audit_granularity(
        marked if side == "fine" else fine,
        marked if side == "coarse" else coarse,
        "4h",
        fine.window,
    )
    assert report["comparison"] == "NOT_COMPARABLE"
    assert bool(report["differences"]) == mismatch
    assert report["source_independence"] == "NOT_ESTABLISHED"
    assert any(r["side"] == side for r in report["comparability_reasons"])


@pytest.mark.parametrize("state", ["UNKNOWN", "AGGREGATED", "DECLARED_RECONSTRUCTION"])
def test_non_native_coarse_states_cannot_receive_unqualified_match(state):
    fine, coarse = native_pair()
    rows = coarse.table.to_pylist()
    rows[0]["state"] = state
    report = audit_granularity(
        fine,
        replace(coarse, table=pa.Table.from_pylist(rows, schema=CANDLE_SCHEMA)),
        "4h",
        fine.window,
    )
    assert report["comparison"] == "NOT_COMPARABLE"
    assert report["comparability_reasons"]
    assert report["differences"] == []


def test_visibility_reconstruction_declaration_is_retained_even_if_row_states_are_reset():
    fine, coarse = native_pair()
    coarse = replace(coarse, visibility={"reconstruction_truth": "DECLARED_UNVERIFIED"})
    report = audit_granularity(fine, coarse, "4h", fine.window)
    assert report["comparison"] == "NOT_COMPARABLE"
    assert report["comparability_reasons"]


def test_empty_and_unknown_inputs_preserve_missing_information():
    fine, coarse = native_pair()
    empty = replace(coarse, table=coarse.table.slice(0, 0))
    report = audit_granularity(fine, empty, "4h", fine.window)
    assert report["comparison"] == "MISMATCH"
    assert len(report["missing_coarse"]) == 2
    empty_fine = replace(fine, table=fine.table.slice(0, 0))
    report = audit_granularity(empty_fine, empty, "4h", fine.window)
    assert report["comparison"] == "NOT_COMPARABLE"
    with pytest.raises(DataLayerError, match="state"):
        audit_granularity(typed_bars([1] * 8, states=["UNKNOWN"] * 8), coarse, "4h", fine.window)


@settings(max_examples=1000, deadline=None, derandomize=True)
@given(st.integers(1, 10**12), st.booleans(), st.booleans())
def test_reconstruction_never_overrides_numerical_difference_or_native_eligibility(
    delta, coarse_side, mismatch
):
    fine, coarse = native_pair()
    if mismatch:
        rows = coarse.table.to_pylist()
        rows[0]["quote_qty"] = str(400 + delta)
        coarse = replace(coarse, table=pa.Table.from_pylist(rows, schema=CANDLE_SCHEMA))
    assert audit_granularity(fine, coarse, "4h", fine.window)["comparison"] == (
        "MISMATCH" if mismatch else "MATCH"
    )
    marked = declare_reconstruction(coarse if coarse_side else fine, [START], evidence=b"test-only")
    report = audit_granularity(
        fine if coarse_side else marked, marked if coarse_side else coarse, "4h", fine.window
    )
    assert report["comparison"] == "NOT_COMPARABLE"
    assert bool(report["differences"]) == mismatch
    assert report["source_independence"] == "NOT_ESTABLISHED"


def test_derived_replay_also_rejects_mixed_rule_source(tmp_path, bundle):
    import copy

    from candle_audit.data_layer.store import canonical

    store, good, req, _ = rule_release(tmp_path / "good", bundle, 4)
    derived = store.aggregate(
        good.release_id, "4h", req.window, name="legal", producer_id="collector"
    )
    # Keep actual immutable native inputs; place the second collection in the same real store.
    mixed_store, mixed, _, _ = rule_release(tmp_path / "mixed", bundle, 2)
    for p in mixed_store.artifacts.root.iterdir():
        if p.is_file() and len(p.name) == 64:
            store.artifacts.put(p.read_bytes())
    for snapshot in mixed_store.collector.candidates.root.iterdir():
        if snapshot.is_dir() and len(snapshot.name) == 64:
            import shutil

            shutil.copytree(snapshot, store.collector.candidates.root / snapshot.name)
    doc = copy.deepcopy(derived.document)
    doc["parent_id"] = mixed.release_id
    doc["derivation"]["source_release_id"] = mixed.release_id
    doc["report"]["source_release_id"] = mixed.release_id
    doc["lineage"] = mixed.document["lineage"] | {
        "derivation_code": doc["lineage"]["derivation_code"]
    }
    doc["quality"] = mixed.document["quality"]
    with pytest.raises(DataLayerError) as failure:
        store.load(store.artifacts.put(canonical(doc)))
    assert "mixed rule bucket" in str(failure.value.__cause__)
