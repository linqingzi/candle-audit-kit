"""A-F04: bounded source shards, complete-window Decimal oracle, create and replay."""

import copy
from decimal import Decimal

import pytest
from test_full_collection import collector, history, native
from test_full_reference import rules

from candle_audit.data_layer import CollectionRequest, DataLayerError, Window
from candle_audit.data_layer.releases import ReleaseStore

START = 1767225600000000000
MINUTE = 60 * 10**9


def minute_release(tmp_path, bundle, count=10020, shard_rows=6002, start_ns=START):
    changed = copy.deepcopy(bundle)
    changed["source"]["bar"] = "1m"
    window = Window(start_ns, start_ns + count * MINUTE)
    rule = rules(changed, timeframe="1m", effective=window)
    req = CollectionRequest(
        provider_id="history",
        rules=(rule,),
        window=window,
        name="long",
        revision="r1",
        kind="synthetic",
        provenance="test-only",
        shard_rows=shard_rows,
        page_rows=300,
    )
    rows = native(start_ns // 10**6, count, 60000)
    for row in rows:
        n = (int(row[0]) * 10**6 - START) // MINUTE + 1
        row[5:8] = [str(n * 10), str(n), str(n * 100)]
    pipeline = collector(tmp_path)
    collection = pipeline.collect(
        req,
        history(changed, req, [rows[i : i + 300] for i in range(0, count, 300)]),
        sleep=lambda _: None,
    )
    store = ReleaseStore(pipeline)
    release = store.create([collection.collection_id], name="long-native", producer_id="collector")
    return store, release, req, collection


def assert_hour_oracle(data, count):
    rows = data.table.to_pylist()
    assert len(rows) == count // 60
    for i, bar in enumerate(rows):
        # Independent arithmetic-series oracle for every source minute, including shard seams.
        total = Decimal(60 * (2 * (60 * i + 1) + 59)) / Decimal(2)
        assert bar["open_time_ns"] == START + i * 60 * MINUTE
        assert bar["close_time_ns"] == START + (i + 1) * 60 * MINUTE
        assert Decimal(bar["base_qty"]) == total
        assert Decimal(bar["contracts_qty"]) == total * 10
        assert Decimal(bar["quote_qty"]) == total * 100
        assert bar["open"] == "100.000000000000000001"
        assert bar["high"] == "102" and bar["low"] == "99" and bar["close"] == "101"


def test_long_source_over_reader_budget_has_bounded_167_row_derived_create_and_reload(
    tmp_path, bundle, monkeypatch
):
    store, source, req, collection = minute_release(tmp_path, bundle)
    assert collection.rows == 10020 and len(collection.shards) == 2
    assert collection.shards[0]["end_ns"] % (60 * MINUTE) != 0
    with pytest.raises(DataLayerError, match="row budget"):
        store.reader().query(source.release_id, req.window)
    short = Window(START, START + 120 * MINUTE)
    small = store.aggregate(source.release_id, "1h", short, name="short", producer_id="collector")
    assert_hour_oracle(store.reader().query(small.release_id, short), 120)
    derived = store.aggregate(
        source.release_id, "1h", req.window, name="whole", producer_id="collector"
    )
    assert_hour_oracle(store.reader().query(derived.release_id, req.window), 10020)
    fresh = ReleaseStore(store.collector)
    assert fresh.load(derived.release_id).document == derived.document
    assert_hour_oracle(fresh.reader().query(derived.release_id, req.window), 10020)
    with pytest.raises(DataLayerError, match="row budget"):
        fresh.reader().query(source.release_id, req.window)


def test_too_small_and_missing_whole_bucket_cannot_publish(tmp_path, bundle):
    store, source, req, _ = minute_release(tmp_path, bundle, 120, 62)
    with pytest.raises(DataLayerError, match="two"):
        store.aggregate(
            source.release_id,
            "1h",
            Window(START, START + 60 * MINUTE),
            name="one",
            producer_id="collector",
        )
    with pytest.raises(DataLayerError, match="complete"):
        store.aggregate(
            source.release_id,
            "1h",
            Window(START, START + 180 * MINUTE),
            name="missing",
            producer_id="collector",
        )
    assert store.reader().query(source.release_id, req.window).table.num_rows == 120


@pytest.mark.parametrize("gap", [False, True])
def test_multiple_collection_windows_reassemble_in_time_order_or_refuse_gap(tmp_path, bundle, gap):
    # Separate collection effective windows have distinct rule IDs. The lawful positive puts
    # that reference boundary on the output bucket boundary, while shard seams remain inside it.
    store, _, _, first = minute_release(tmp_path, bundle, 60, 40)
    second_start = 62 if gap else 60
    _, _, _, second = minute_release(
        tmp_path, bundle, 120 - second_start, 40, start_ns=START + second_start * MINUTE
    )
    source = store.create(
        [second.collection_id, first.collection_id], name="union", producer_id="collector"
    )
    window = Window(START, START + 120 * MINUTE)
    if gap:
        with pytest.raises(DataLayerError, match="complete"):
            store.aggregate(source.release_id, "1h", window, name="gap", producer_id="collector")
    else:
        derived = store.aggregate(
            source.release_id, "1h", window, name="union-hour", producer_id="collector"
        )
        assert_hour_oracle(store.reader().query(derived.release_id, window), 120)


def test_output_overflow_is_explicit_without_widening_or_truncating_reader(
    tmp_path, bundle, monkeypatch
):
    from candle_audit.data_layer.releases import DatasetReader

    store, source, _, _ = minute_release(tmp_path, bundle, 120, 62)

    def forbidden(*args, **kwargs):
        raise AssertionError("overflow output must be refused before aggregate source streaming")

    monkeypatch.setattr(DatasetReader, "_tables", forbidden)
    with pytest.raises(DataLayerError, match="output row budget"):
        store.aggregate(
            source.release_id,
            "1h",
            Window(START, START + 10001 * 60 * MINUTE),
            name="overflow",
            producer_id="collector",
        )
