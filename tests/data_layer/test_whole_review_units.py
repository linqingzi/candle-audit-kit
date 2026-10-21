"""A-F01: declared conversions must agree with every native rule (engineering only)."""

import copy
from decimal import Decimal

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from test_full_collection import collector, history, native, request
from test_full_reference import rules

from candle_audit.data_layer import CollectionRequest, DataLayerError, Window


def columns(size, unit="base"):
    return [
        {
            "column": "contracts",
            "unit": "contracts",
            "contract_size": size,
            "contract_size_unit": unit,
        }
    ]


@pytest.mark.parametrize("size,unit", [(0.2, "base"), (0.1, "quote")])
def test_incompatible_descriptor_cannot_declare_collection(bundle, size, unit):
    with pytest.raises(DataLayerError, match="contract"):
        request(bundle, volume_columns=columns(size, unit))


def test_contract_descriptor_cannot_be_used_for_spot(bundle):
    spot = copy.deepcopy(bundle)
    spot["source"].update(instId="BTC-USDT", instType="SPOT")
    rule = rules(spot, contract_size=None)
    with pytest.raises(DataLayerError, match="contract"):
        CollectionRequest(
            provider_id="history",
            rules=(rule,),
            window=rule.effective,
            name="spot",
            revision="r1",
            kind="synthetic",
            provenance="test-only",
            volume_columns=columns(0.1),
        )


def test_spot_and_swap_default_units_remain_legal(bundle):
    for spot in (False, True):
        changed = copy.deepcopy(bundle)
        if spot:
            changed["source"].update(instId="BTC-USDT", instType="SPOT")
        rule = rules(changed, contract_size=None if spot else "0.1000")
        req = CollectionRequest(
            provider_id="history",
            rules=(rule,),
            window=rule.effective,
            name="defaults",
            revision="r1",
            kind="synthetic",
            provenance="test-only",
        )
        assert req.columns == [
            {"column": "base_qty", "unit": "base"},
            {"column": "quote_qty", "unit": "quote"},
        ]
        assert CollectionRequest.from_dict(req.as_dict()).columns == req.columns


def test_scalar_conversion_must_match_later_rule_window(bundle):
    start = 1767225600000000000
    first = rules(bundle, effective=Window(start, start + 2 * 3600 * 10**9))
    second = rules(
        bundle,
        effective=Window(first.effective.end_ns, start + 6 * 3600 * 10**9),
        contract_size="0.2",
    )
    with pytest.raises(DataLayerError, match="contract"):
        request(bundle, rules=(first, second), volume_columns=columns(0.1))


def test_from_dict_cannot_bypass_descriptor_rule_binding(bundle):
    document = request(bundle, volume_columns=columns(0.1)).as_dict()
    document["volume_columns"][0]["contract_size"] = 0.2
    with pytest.raises(DataLayerError, match="invalid complete collection request"):
        CollectionRequest.from_dict(document)


@pytest.mark.parametrize("size", [0.1, 0.10, 1e-1])
def test_matching_exact_descriptor_roundtrips_and_replays(tmp_path, bundle, size):
    req = request(bundle, count=2, volume_columns=columns(size))
    assert CollectionRequest.from_dict(req.as_dict()).columns == req.columns
    pipeline = collector(tmp_path)
    result = pipeline.collect(req, history(bundle, req, [native(req.window.start_ns // 10**6, 2)]))
    assert pipeline.read(result.collection_id).status == "COMPLETE"
    assert req.columns == columns(size)


@settings(max_examples=1000, deadline=None, derandomize=True)
@given(st.integers(1, 1000000), st.integers(1, 12), st.integers(1, 1000))
def test_exact_decimal_rule_conversion_rejects_each_different_scalar(n, places, delta):
    source = {
        "venue": "OKX",
        "instId": "BTC-USDT-SWAP",
        "instType": "SWAP",
        "base_asset": "BTC",
        "quote_asset": "USDT",
        "bar": "1H",
        "endpoint": "/api/v5/market/history-candles",
        "source_type": "trade",
        "acquisition": "SYNTHETIC",
    }
    bundle = {
        "source": source,
        "metadata": {"timeframe": "1h"},
        "start": "2026-01-01T00:00:00Z",
        "end": "2026-01-01T02:00:00Z",
    }
    scale = Decimal(10) ** -places
    exact = Decimal(n) * scale
    req = request(
        bundle,
        count=2,
        rules=(rules(bundle, contract_size=str(exact)),),
        volume_columns=columns(float(exact)),
    )
    assert Decimal(str(req.columns[0]["contract_size"])) == exact
    assert CollectionRequest.from_dict(req.as_dict()).columns == req.columns
    with pytest.raises(DataLayerError, match="contract"):
        request(
            bundle, count=2, rules=req.rules, volume_columns=columns(float(exact + delta * scale))
        )
