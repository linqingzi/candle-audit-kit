"""Normal constructor operations for the decorated unit guard omitted by native mutmut."""

import copy
from decimal import Decimal

import pytest
from test_full_collection import request
from test_full_reference import rules

from candle_audit.data_layer import CollectionRequest, DataLayerError, Window


def descriptors(size, unit="base"):
    return [
        {"column": "qty", "unit": "contracts", "contract_size": size, "contract_size_unit": unit}
    ]


@pytest.mark.parametrize("change", ["size", "unit", "spot", "later-rule"])
def test_constructor_one_changed_unit_premise_rejects_and_legitimate_default_survives(
    bundle, change
):
    good = request(bundle, volume_columns=descriptors(0.1))
    restored = CollectionRequest.from_dict(good.as_dict())
    assert restored.request_id == good.request_id
    assert Decimal(str(restored.columns[0]["contract_size"])) == Decimal("0.1")
    altered = copy.deepcopy(bundle)
    columns, refs = descriptors(0.1), good.rules
    if change == "size":
        columns = descriptors(0.2)
    elif change == "unit":
        columns = descriptors(0.1, "quote")
    elif change == "spot":
        altered["source"].update(instType="SPOT", instId="BTC-USDT")
        refs = (rules(altered, contract_size=None, effective=good.window),)
    else:
        start, end = good.window.start_ns, good.window.end_ns
        first = rules(altered, effective=Window(start, start + 2 * 3600 * 10**9))
        second = rules(altered, effective=Window(first.effective.end_ns, end), contract_size="0.2")
        refs = (first, second)
    with pytest.raises(DataLayerError, match="contract descriptor"):
        CollectionRequest(
            provider_id="history",
            rules=refs,
            window=good.window,
            name="units",
            revision="r1",
            kind="synthetic",
            provenance="test-only",
            volume_columns=columns,
        )
    assert request(bundle).columns == [
        {"column": "base_qty", "unit": "base"},
        {"column": "quote_qty", "unit": "quote"},
    ]
