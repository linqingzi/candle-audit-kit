"""Real inputs that distinguish mutation survivors from the retained input contract."""

from pathlib import Path

import pytest

from candle_audit.data_layer import DataLayerError
from candle_audit.data_layer.__main__ import load_input
from candle_audit.data_layer.store import project_root


def test_fixed_fixture_traversal_is_rejected_before_reading_even_if_resolved_inside_fixture():
    root = project_root()
    path = root / "tests/fixtures/legacy/../fixtures/bars.csv"
    with pytest.raises(DataLayerError):
        load_input(str(path), ".csv", 4096)


@pytest.mark.parametrize("expected_suffix", [".json", ".parquet", ""])
def test_fixed_fixture_requires_the_requested_input_type(expected_suffix):
    path = project_root() / "tests/fixtures/legacy/bars.csv"
    with pytest.raises(DataLayerError):
        load_input(str(path), expected_suffix, 4096)


def test_valid_fixed_fixture_still_loads_its_exact_bytes():
    path = project_root() / "tests/fixtures/legacy/bars.csv"
    assert load_input(str(path), ".csv", 4096) == Path(path).read_bytes()
