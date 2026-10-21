"""Whole-module contracts; all market-shaped input here is engineering test data."""

import importlib
from decimal import Decimal

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from candle_audit.data_layer import DataLayerError, Window
from candle_audit.data_layer.providers import LocalCandidateProvider, ProviderRegistry


def api():
    assert importlib.util.find_spec("candle_audit.data_layer.reference"), "reference lifecycle absent"
    return importlib.import_module("candle_audit.data_layer.reference")


def rules(bundle, **changes):
    module = api()
    values = dict(
        source=bundle["source"],
        timeframe=bundle["metadata"]["timeframe"],
        effective=Window.parse(bundle["start"], bundle["end"]),
        recorded_ns=1770000000000000000,
        tick_size="0.000000000000000001",
        lot_size="0.1",
        contract_size="0.1",
        rule_evidence=b"engineering-reference",
        basis="DECLARED",
        price_kind="trade",
    )
    values.update(changes)
    return module.InstrumentRules(**values)


def test_registry_public_listing_and_selection_are_detached(bundle):
    registry = ProviderRegistry()
    provider = LocalCandidateProvider(bundle["candidate_csv"], bundle["raw"])
    registry.register("file", provider)
    assert hasattr(registry, "list_providers"), "public provider list missing"
    assert registry.list_providers() == ("file",)
    assert registry.select("file") is provider
    with pytest.raises(DataLayerError):
        registry.select("missing")


def test_reference_effective_window_units_and_no_historical_backfill(bundle):
    rule = rules(bundle)
    assert rule.as_dict()["contract_size"] == "0.1"
    assert rule.quantity_units == {"vol": "contracts", "volCcy": "base", "volCcyQuote": "quote"}
    assert rule.lookup(rule.effective.start_ns)["historical_truth"] == "UNVERIFIED"
    with pytest.raises(DataLayerError, match="effective"):
        rule.lookup(rule.effective.end_ns)
    with pytest.raises(DataLayerError, match="UNKNOWN"):
        rule.lookup(rule.effective.start_ns, strict_historical=True)
    rule.as_dict()["source"].clear()
    assert rule.source == bundle["source"]


@pytest.mark.parametrize(
    "changes",
    [
        {"price_kind": "index"},
        {"tick_size": 0.01},
        {"lot_size": "0"},
        {"contract_size": None},
        {"basis": "VERIFIED"},
        {"recorded_ns": True},
        {"tick_size": "NaN"},
        {"rule_evidence": b""},
    ],
)
def test_unknown_or_invalid_reference_is_refused(bundle, changes):
    with pytest.raises(DataLayerError):
        rules(bundle, **changes)


def test_raw_canonical_mapping_retains_text_and_zero_status(bundle):
    module = api()
    row = ["1767225600000", "100.000000000000000001", "102", "99", "101", "10", "1", "100", "1"]
    bar = module.Candle.from_native(row, rules(bundle), acquired_ns=1770000000000000000)
    assert bar.open == "100.000000000000000001"
    assert bar.decimal("open") == Decimal("100.000000000000000001")
    assert bar.close_time_ns == 1767229200000000000
    assert bar.event_time_ns is None and bar.published_ns is None
    assert bar.state == "OBSERVED"
    row[5:8] = ["0", "0", "0"]
    assert (
        module.Candle.from_native(row, rules(bundle), acquired_ns=1770000000000000000).state
        == "OBSERVED_ZERO"
    )


@settings(max_examples=1000, deadline=None, derandomize=True)
@given(st.integers(1, 10**18))
def test_decimal_truth_cannot_round_through_binary_float(fraction):
    module = api()
    text = f"100.{fraction:018d}"
    assert module.exact_number(text).as_tuple() == Decimal(text).as_tuple()
    for bad in [float(text), True, "NaN", "1e1000"]:
        with pytest.raises(DataLayerError):
            module.exact_number(bad)


def test_blob_identity_and_atomic_state_preserve_old_value(tmp_path, monkeypatch):
    assert importlib.util.find_spec("candle_audit.data_layer.artifacts"), "artifact journal absent"
    module = importlib.import_module("candle_audit.data_layer.artifacts")
    store = module.Artifacts(tmp_path / "artifacts")
    identity = store.put(b"original bytes")
    assert store.get(identity) == b"original bytes"
    assert store.put(b"original bytes") == identity
    store.save_state("job", {"cursor": 3})
    original = module.os.replace

    def fail(*args, **kwargs):
        raise OSError("injected publish failure")

    monkeypatch.setattr(module.os, "replace", fail)
    with pytest.raises(DataLayerError):
        store.save_state("job", {"cursor": 4})
    assert store.load_state("job") == {"cursor": 3}
    monkeypatch.setattr(module.os, "replace", original)
    store.save_state("job", {"cursor": 4})
    assert store.load_state("job") == {"cursor": 4}
    for bad in ["../job", "", "import:module"]:
        with pytest.raises(DataLayerError):
            store.load_state(bad)
