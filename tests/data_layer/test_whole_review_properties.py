"""High-risk public consumer properties with independent expected values, engineering only."""

from decimal import Decimal

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from test_whole_review_reporting import HOUR, START, separated_release
from test_whole_review_streaming import minute_release

from candle_audit.data_layer import Window
from candle_audit.data_layer.releases import DatasetReader
from candle_audit.data_layer.reporting import report_document


@pytest.fixture
def streamed_case(tmp_path, bundle, monkeypatch):
    store, source, req, _ = minute_release(tmp_path, bundle, count=10, shard_rows=6)
    original = DatasetReader._tables
    holder = {"width": 1}

    def resegment(self, release, window, instruments, timeframes):
        for table in original(self, release, window, instruments, timeframes):
            # Repartition already verified actual rows; no changed values or fake release load.
            for opening in range(0, table.num_rows, holder["width"]):
                yield table.slice(opening, holder["width"])

    monkeypatch.setattr(DatasetReader, "_tables", resegment)

    return store, source, req, holder


@pytest.fixture
def report_case(tmp_path, bundle):
    return separated_release(tmp_path, bundle)


# These fixtures keep immutable, verified native inputs. Each example resets the chunk selection;
# derived publications are immutable separate outputs, so sharing fixture setup cannot hide a case.
@settings(
    max_examples=1000,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(st.integers(1, 10**12))
def test_streamed_create_and_replay_are_invariant_to_validated_arrow_chunk_boundaries(
    streamed_case, seed
):
    store, source, req, holder = streamed_case
    holder["width"] = 1 + seed % 7
    derived = store.aggregate(
        source.release_id, "5m", req.window, name="property", producer_id="collector"
    )
    data = store.reader().query(derived.release_id, req.window)
    assert data.table.num_rows == 2
    assert [Decimal(v) for v in data.table["base_qty"].to_pylist()] == [Decimal(15), Decimal(40)]
    assert [Decimal(v) for v in data.table["contracts_qty"].to_pylist()] == [
        Decimal(150),
        Decimal(400),
    ]
    assert [Decimal(v) for v in data.table["quote_qty"].to_pylist()] == [
        Decimal(1500),
        Decimal(4000),
    ]
    assert data.table["open_time_ns"].to_pylist() == [START, START + 300 * 10**9]


@settings(
    max_examples=1000,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    st.integers(0, 100000),
    st.integers(1, 100000),
    st.sampled_from(
        [None, ["BTC-USDT-SWAP"], ["ETH-USDT-SWAP"], ["BTC-USDT-SWAP", "ETH-USDT-SWAP"]]
    ),
)
def test_report_denominator_and_unavailable_counts_follow_selection_even_with_zero_rows(
    report_case, start_hour, length, selected
):
    store, release = report_case
    assets = ["BTC-USDT-SWAP", "ETH-USDT-SWAP"]
    window = Window(START + start_hour * HOUR, START + (start_hour + length) * HOUR)
    data = store.reader().query(release.release_id, window, instruments=selected)
    doc = report_document(data, release)
    wanted = set(assets if selected is None else selected)
    assert {s["instrument"] for s in doc["selected_streams"]} == wanted
    actual = {(r["instrument"], r["open_time_ns"]) for r in data.table.to_pylist()}
    expected = {
        (asset, START + h * HOUR)
        for asset, hours in zip(assets, [(0, 1), (4, 5)], strict=True)
        for h in hours
        if asset in wanted and start_hour <= h < start_hour + length
    }
    assert actual == expected
    gaps = {g["instrument"]: g for g in doc["query_gaps"] if "instrument" in g}
    for asset in wanted:
        missing = length - sum(a == asset for a, _ in expected)
        if missing:
            assert gaps[asset]["missing_count"] == missing
            assert gaps[asset]["market_gap_truth"] == "UNKNOWN"
            assert len(gaps[asset]["sample_openings_ns"]) <= 10
        else:
            assert asset not in gaps
    assert not gaps.keys() - wanted
