"""Real saved-source failures and bounded public resource edges, without weakened kernels."""

import copy

import pytest
from test_full_aggregation import inputs
from test_full_collection import collector, history, native, request
from test_full_reference import rules
from test_whole_review_aggregation import HOUR, START

from candle_audit.data_layer import CollectionRequest, DataLayerError, Window
from candle_audit.data_layer.collection import CollectionFailure
from candle_audit.data_layer.releases import ReleaseStore
from candle_audit.data_layer.store import canonical


def causes(error):
    values = []
    while error is not None:
        values.append(str(error))
        error = error.__cause__
    return " / ".join(values)


@pytest.mark.parametrize(
    "change,expected",
    [
        ("duplicate-window", "duplicate saved shard"),
        ("wrong-snapshot", "saved shard contradicts"),
        ("wrong-mapping", "saved original-row mapping"),
        ("partial-pages", "full-window boundary incomplete"),
        ("byte-budget", "complete-request byte budget"),
    ],
)
def test_saved_collection_semantics_reject_one_wrong_reference_after_legal_replay(
    tmp_path, bundle, change, expected
):
    pipeline = collector(tmp_path)
    req = request(bundle, count=4)
    rows = native(START // 10**6, 4)
    result = pipeline.collect(req, history(bundle, req, [rows[:2], rows[2:]]), sleep=lambda _: None)
    assert pipeline.read(result.collection_id).rows == 4
    bad = copy.deepcopy(result.document)
    if change == "duplicate-window":
        bad["shards"].append(bad["shards"][0].copy())
    elif change == "wrong-snapshot":
        other_req = request(bundle, count=4, name="different-valid-input")
        other_rows = native(START // 10**6, 4)
        for row in other_rows:
            row[5:8] = ["20", "2", "200"]
        other = pipeline.collect(
            other_req,
            history(bundle, other_req, [other_rows[:2], other_rows[2:]]),
            sleep=lambda _: None,
        )
        bad["shards"][0]["snapshot_id"] = other.shards[0]["snapshot_id"]
    elif change == "wrong-mapping":
        bad["shards"][0]["mapping_sha256"] = pipeline.artifacts.put(canonical({"rows": []}))
    elif change == "partial-pages":
        bad["page_head"] = pipeline.artifacts.put(canonical(pipeline.pages(result)[0]))
    else:
        altered = CollectionRequest.from_dict(req.as_dict() | {"max_total_bytes": 1})
        previous = None
        for page in pipeline.pages(result):
            previous = pipeline.artifacts.put(
                canonical(page | {"request_id": altered.request_id, "previous": previous})
            )
        bad.update(request=altered.as_dict(), request_id=altered.request_id, page_head=previous)
    with pytest.raises(DataLayerError) as error:
        pipeline.read(pipeline.artifacts.put(canonical(bad)))
    assert expected in causes(error.value)
    assert pipeline.read(result.collection_id).document == result.document


@pytest.mark.parametrize("selection", [[], "1h", ["1h", "1h"], [True]])
def test_persistent_source_timeframe_validation_rejects_bad_selection_after_valid_release(
    tmp_path, bundle, selection
):
    store, source, req = inputs(tmp_path, bundle)
    good = store.aggregate(
        source.release_id, "4h", req.window, name="good", producer_id="collector"
    )
    assert store.reader().query(good.release_id, req.window).table.num_rows == 2
    with pytest.raises(DataLayerError, match="timeframe selections"):
        store.aggregate(
            source.release_id,
            "4h",
            req.window,
            source_timeframes=selection,
            name="bad",
            producer_id="collector",
        )
    with pytest.raises(DataLayerError, match="native release"):
        store.aggregate(good.release_id, "1d", req.window, name="nested", producer_id="collector")
    with pytest.raises(DataLayerError, match="coarser multiple"):
        store.aggregate(
            source.release_id, "1h", req.window, name="same-grain", producer_id="collector"
        )


@pytest.mark.parametrize("change", ["admission", "parent-kind", "parent-identity"])
def test_derived_reader_rejects_wrong_format_or_parent_after_valid_native_replay(
    tmp_path, bundle, change
):
    store, source, req = inputs(tmp_path, bundle)
    good = store.aggregate(
        source.release_id, "4h", req.window, name="good", producer_id="collector"
    )
    assert store.load(good.release_id).document == good.document
    bad = copy.deepcopy(good.document)
    if change == "admission":
        bad["admission"] = "RESEARCH"
    elif change == "parent-kind":
        bad["parent_id"] = bad["derivation"]["source_release_id"] = good.release_id
    else:
        bad["parent_id"] = "1" * 64
    with pytest.raises(DataLayerError) as error:
        store.load(store.artifacts.put(canonical(bad)))
    assert ("schema" if change == "admission" else "native dataset") in causes(error.value)
    assert store.reader().query(good.release_id, req.window).table.num_rows == 2


def test_native_revision_outside_both_windows_skips_metadata_and_returns_empty_diff(
    tmp_path, bundle
):
    store, source, req = inputs(tmp_path, bundle)
    revision = store.create(
        source.document["requested_collections"],
        name="revision",
        producer_id="collector",
        parent_id=source.release_id,
        reason="same data new version",
    )
    report = store.reader().revisions(
        source.release_id,
        revision.release_id,
        Window(req.window.end_ns, req.window.end_ns + 4 * HOUR),
    )
    assert report["reference_changes"] == report["declaration_changes"] == []
    assert report["added"] == report["removed"] == report["changed_fields"] == []


def minute_request(bundle, count, *, name, shard_rows=10000):
    changed = copy.deepcopy(bundle)
    changed["source"]["bar"] = "1m"
    window = Window(START, START + count * 60 * 10**9)
    req = CollectionRequest(
        provider_id="history",
        rules=(rules(changed, timeframe="1m", effective=window),),
        window=window,
        name=name,
        revision="r1",
        kind="synthetic",
        provenance="test-only",
        page_rows=300,
        shard_rows=shard_rows,
        max_pages=1000,
    )
    return changed, req


def test_actual_4096_partition_catalog_cap_quarantines_4097th_and_preserves_old_release(
    tmp_path, bundle
):
    pipeline = collector(tmp_path)
    changed, good = minute_request(bundle, 2, name="small-control")
    small = pipeline.collect(good, history(changed, good, [native(START // 10**6, 2, 60000)]))
    store = ReleaseStore(pipeline)
    release = store.create([small.collection_id], name="old", producer_id="collector")
    assert store.reader().query(release.release_id, good.window).table.num_rows == 2
    changed, req = minute_request(bundle, 8194, name="actual-catalog-cap", shard_rows=2)
    rows = native(START // 10**6, 8194, 60000)
    pages = [rows[i : i + 300] for i in range(0, len(rows), 300)]
    with pytest.raises(CollectionFailure) as error:
        pipeline.collect(req, history(changed, req, pages), sleep=lambda _: None)
    assert "partition catalog budget exceeded" in causes(error.value)
    assert pipeline.task(req.request_id)["status"] == "QUARANTINED"
    assert store.reader().query(release.release_id, good.window).table.num_rows == 2


def unicode_rows(count):
    rows = native(START // 10**6, count, 60000)
    # Supported decimal digits, <=96 characters per original field, current kernel budgets.
    for row in rows:
        row[1:5] = ["\U0001d7d9" * 90] * 4
    return rows


def test_actual_byte_boundaries_split_legal_unicode_source_and_refuse_dense_output_over_16mib(
    tmp_path, bundle
):
    pipeline = collector(tmp_path)
    changed, req = minute_request(bundle, 50000, name="actual-byte-boundaries")
    rows = unicode_rows(50000)
    pages = [rows[i : i + 300] for i in range(0, len(rows), 300)]
    collection = pipeline.collect(req, history(changed, req, pages), sleep=lambda _: None)
    assert collection.rows == 50000
    assert len(collection.shards) > 4  # Actual 8MiB encoded shard budget required recursive splits.
    assert all(2 <= shard["rows"] < 10000 for shard in collection.shards)
    store = ReleaseStore(pipeline)
    source = store.create(
        [collection.collection_id], name="unicode-native", producer_id="collector"
    )
    small_window = Window(START, START + 8 * 60 * 10**9)
    small = store.aggregate(
        source.release_id,
        "5m",
        Window(START, START + 10 * 60 * 10**9),
        name="positive",
        producer_id="collector",
    )
    assert (
        store.reader()
        .query(small.release_id, Window(START, START + 10 * 60 * 10**9))
        .table.num_rows
        == 2
    )
    # 50000/5 is 10000 bounded output bars; the actual UTF-8 density exceeds16MiB.
    with pytest.raises(DataLayerError, match="dense UTF-8 budget"):
        store.aggregate(
            source.release_id, "5m", req.window, name="too-dense", producer_id="collector"
        )
    assert store.reader().query(source.release_id, small_window).table.num_rows == 8
