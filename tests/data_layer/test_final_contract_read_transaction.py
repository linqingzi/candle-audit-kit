"""One read authenticates captured snapshot bytes once; the next read is always fresh."""

import hashlib

import pytest
from test_full_aggregation import inputs
from test_full_collection import collector, history, native, request

from candle_audit.data_layer import DataLayerError
from candle_audit.data_layer import store as snapshot_store
from candle_audit.data_layer.releases import ReleaseStore
from candle_audit.data_layer.store import canonical


def test_actual_release_read_replays_both_kernels_once_per_snapshot_every_transaction(
    tmp_path, bundle, monkeypatch
):
    store, release, req = inputs(tmp_path, bundle)
    calls = []
    original = snapshot_store._verify_inputs

    def observe(candidate, metadata, raw, source, window):
        calls.append((hashlib.sha256(candidate).hexdigest(), hashlib.sha256(raw).hexdigest()))
        return original(candidate, metadata, raw, source, window)

    monkeypatch.setattr(snapshot_store, "_verify_inputs", observe)
    shard_count = len(release.document["partitions"])
    for _ in range(2):
        calls.clear()
        data = ReleaseStore(store.collector).reader().query(release.release_id, req.window)
        assert data.table.num_rows == 8 and data.table["base_qty"].to_pylist() == ["1"] * 8
        assert len(calls) == shard_count
        assert len(set(calls)) == shard_count


@pytest.mark.parametrize(
    "member", ["candidate.csv", "raw.json", "metadata.json", "source.json", "bars.parquet"]
)
def test_captured_bundle_is_stable_inside_read_and_changed_file_is_rejected_next_read(
    tmp_path, bundle, monkeypatch, member
):
    pipeline = collector(tmp_path)
    req = request(bundle, count=2)
    collection = pipeline.collect(
        req, history(bundle, req, [native(req.window.start_ns // 10**6, 2)])
    )
    snapshot = collection.shards[0]["snapshot_id"]
    path = pipeline.candidates.root / snapshot / member
    original_bytes = path.read_bytes()
    original_load = pipeline.candidates._load
    calls = []

    def capture_then_change(*args, **kwargs):
        captured = original_load(*args, **kwargs)
        calls.append(snapshot)
        path.write_bytes(b"changed after authenticated capture")
        return captured

    monkeypatch.setattr(pipeline.candidates, "_load", capture_then_change)
    assert pipeline.read(collection.collection_id).rows == 2
    assert calls == [snapshot]
    monkeypatch.setattr(pipeline.candidates, "_load", original_load)
    with pytest.raises(DataLayerError):
        pipeline.read(collection.collection_id)
    path.write_bytes(original_bytes)
    assert pipeline.read(collection.collection_id).rows == 2


def test_changed_saved_parquet_semantics_with_fresh_integrity_cannot_reuse_success(
    tmp_path, bundle
):
    import pyarrow as pa
    import pyarrow.parquet as pq

    from candle_audit.data_layer.store import decode_document

    pipeline = collector(tmp_path)
    req = request(bundle, count=2)
    collection = pipeline.collect(
        req, history(bundle, req, [native(req.window.start_ns // 10**6, 2)])
    )
    snapshot = collection.shards[0]["snapshot_id"]
    assert pipeline.candidates.query(snapshot, req.window).table.num_rows == 2
    directory = pipeline.candidates.root / snapshot
    table = pq.read_table(directory / "bars.parquet")
    rows = table.to_pylist()
    rows[0]["open"] = "100.5"
    out = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), out, compression="zstd")
    directory.joinpath("bars.parquet").write_bytes(out.getvalue().to_pybytes())
    manifest = decode_document(directory.joinpath("manifest.json").read_bytes())
    b = directory.joinpath("bars.parquet").read_bytes()
    manifest["files"]["bars.parquet"] = {"sha256": hashlib.sha256(b).hexdigest(), "bytes": len(b)}
    identity = hashlib.sha256(
        canonical({k: v for k, v in manifest.items() if k != "snapshot_id"})
    ).hexdigest()
    manifest["snapshot_id"] = identity
    directory.joinpath("manifest.json").write_bytes(canonical(manifest))
    directory.rename(pipeline.candidates.root / identity)
    with pytest.raises(DataLayerError, match="roundtrip"):
        pipeline.candidates.query(identity, req.window)
