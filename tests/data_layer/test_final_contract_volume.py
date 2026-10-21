"""Preserve the legacy diagnostic's scalar, vector, empty and dynamic-unit behavior."""

import copy

import numpy as np
import pandas as pd
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from candle_audit.contract.data_versions import volume_columns
from candle_audit.contract.registry import RegistryError


def declaration(unit="base", size=0.125):
    return {
        "volume": {
            "base_asset": "BTC",
            "quote_asset": "USDT",
            "columns": [
                {
                    "column": "contracts",
                    "unit": "contracts",
                    "contract_size": size,
                    "contract_size_unit": unit,
                },
                {"column": "reported", "unit": unit},
            ],
        }
    }


@settings(max_examples=1000, deadline=None, derandomize=True)
@given(st.lists(st.integers(0, 10**9), min_size=0, max_size=4), st.integers(1, 256), st.booleans())
def test_float_diagnostic_conserves_binary_exact_units_in_scalar_vector_and_empty(
    rows, factor, quote
):
    doc = declaration("quote" if quote else "base", factor / 8)
    frame = pd.DataFrame({"contracts": rows, "reported": [q * factor / 8 for q in rows]})
    result = volume_columns(frame, doc)
    target = "quote_volume" if quote else "volume"
    assert set(result) == {"volume_contracts", target}
    np.testing.assert_array_equal(result["volume_contracts"], np.array(rows, dtype=float))
    np.testing.assert_array_equal(result[target], np.array([q * factor / 8 for q in rows]))
    assert all(v.dtype == np.float64 for v in result.values())
    assert doc == declaration("quote" if quote else "base", factor / 8)


@pytest.mark.parametrize("count", [1, 2])
@pytest.mark.parametrize("bad", [float("nan"), float("inf"), -0.25])
def test_nonfinite_or_negative_volume_is_rejected_for_scalar_and_vector(count, bad):
    frame = pd.DataFrame({"contracts": [bad] * count, "reported": [1.0] * count})
    with pytest.raises(RegistryError, match="finite and nonnegative: contracts"):
        volume_columns(frame, declaration())
    frame["contracts"] = [8.0] * count
    assert volume_columns(frame, declaration())["volume"].tolist() == [1.0] * count


@pytest.mark.parametrize("count", [1, 2])
def test_conversion_overflow_inconsistency_and_runtime_descriptor_change(count):
    frame = pd.DataFrame({"contracts": [8.0] * count, "reported": [1.0] * count})
    doc = declaration()
    assert volume_columns(frame, doc)["volume"].tolist() == [1.0] * count
    changed = copy.deepcopy(doc)
    changed["volume"]["columns"][0]["contract_size"] = 0.25
    with pytest.raises(RegistryError, match="disagrees"):
        volume_columns(frame, changed)
    frame["reported"] = [2.0] * count
    assert volume_columns(frame, changed)["volume"].tolist() == [2.0] * count
    overflow = declaration(size=1e308)
    with pytest.raises(RegistryError, match="overflow"):
        volume_columns(frame, overflow)


def test_unrelated_columns_and_dynamic_units_remain_supported():
    frame = pd.DataFrame({"unrelated": ["text"], "reported": [1.0], "contracts": [8.0]})
    assert volume_columns(frame, declaration())["volume"].tolist() == [1.0]
    assert volume_columns(frame, declaration("quote"))["quote_volume"].tolist() == [1.0]
    doc = declaration()
    doc["volume"]["columns"] = doc["volume"]["columns"][:1]
    assert volume_columns(frame, doc)["volume"].tolist() == [1.0]
