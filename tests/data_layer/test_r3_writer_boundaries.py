"""The constructor and frozen-kernel captured files share writer inode safety."""

import io
import os
import shutil
from pathlib import Path

import pyarrow as pa
import pytest
from test_r3_writer_directory_race import peer_identity
from test_store import publish

from candle_audit.data_layer import CandidateStore, DataLayerError
from candle_audit.data_layer import store as implementation


def test_constructor_parent_swap_does_not_create_a_store_in_the_peer(tmp_path, bundle, monkeypatch):
    parent, saved, peer = (tmp_path / name for name in ("parent", "saved", "peer"))
    parent.mkdir()
    peer.mkdir()
    (peer / "marker").write_bytes(b"test-only benign peer")
    before = peer_identity(peer)
    real_mkdir = os.mkdir
    swapped = False
    effects = []

    def mkdir(path, *args, **kwargs):
        nonlocal swapped
        if Path(path).name == "new-store" and not swapped:
            parent.rename(saved)
            parent.symlink_to(peer, target_is_directory=True)
            swapped = True
        result = real_mkdir(path, *args, **kwargs)
        if peer_identity(peer) != before:
            effects.append(str(path))
        return result

    monkeypatch.setattr(implementation.os, "mkdir", mkdir)
    store = CandidateStore(parent / "new-store")
    with pytest.raises((DataLayerError, OSError)):
        publish(store, bundle)
    assert swapped and effects == []
    assert peer_identity(peer) == before


def test_frozen_oracles_reopen_captured_inode_after_semantic_directory_swap(
    tmp_path, bundle, monkeypatch
):
    store = CandidateStore(tmp_path / "store")
    peer, saved = tmp_path / "peer", tmp_path / "semantic-saved"
    peer.mkdir()
    (peer / "candidate.csv").write_bytes(b"different benign engineering bytes")
    (peer / "raw.json").write_bytes(b"different benign engineering bytes")
    before = peer_identity(peer)
    peer_inodes = {(p.stat().st_dev, p.stat().st_ino) for p in peer.iterdir()}
    real_mkdir, real_open = os.mkdir, io.open
    real_validate = implementation.candle_quality.validate
    target = None
    swapped = False
    active = False
    peer_reads = []

    def mkdir(path, *args, **kwargs):
        nonlocal target
        result = real_mkdir(path, *args, **kwargs)
        if Path(path).name.startswith("semantic-"):
            target = implementation.project_root() / ".tmp-data-layer" / Path(path).name
        return result

    def opened(*args, **kwargs):
        stream = real_open(*args, **kwargs)
        info = os.fstat(stream.fileno())
        if active and swapped and (info.st_dev, info.st_ino) in peer_inodes:
            peer_reads.append(str(args[0]))
        return stream

    def validate(*args, **kwargs):
        nonlocal swapped
        if not swapped:
            target.rename(saved)
            target.symlink_to(peer, target_is_directory=True)
            swapped = True
        return real_validate(*args, **kwargs)

    monkeypatch.setattr(implementation.os, "mkdir", mkdir)
    monkeypatch.setattr(io, "open", opened)
    monkeypatch.setattr(implementation.candle_quality, "validate", validate)
    try:
        active = True
        snapshot = publish(store, bundle)
        active = False
        assert snapshot.reports["quality"]["structural_status"] == "PASS"
        assert snapshot.reports["reconcile"]["comparison"] == "MATCH"
        assert peer_identity(peer) == before
        assert swapped and peer_reads == []
        assert list(saved.iterdir()) == []
    finally:
        if target is not None and target.is_symlink():
            target.unlink()
        if saved.exists():
            shutil.rmtree(saved)


def test_missing_snapshot_rejects_without_creating_a_directory(tmp_path, bundle):
    from candle_audit.data_layer import Window

    store = CandidateStore(tmp_path / "store")
    with pytest.raises(DataLayerError):
        store.query("0" * 64, Window.parse(bundle["start"], bundle["end"]))
    assert list(store.root.iterdir()) == []


def test_real_parquet_write_corruption_rejects_and_preserves_old_candidate(
    tmp_path, bundle, monkeypatch
):
    store = CandidateStore(tmp_path / "store")
    original = publish(store, bundle)
    before = peer_identity(store.root)
    real_write = implementation.pq.write_table

    def corrupted(table, output, **kwargs):
        changed = table.set_column(
            1, table.schema.field("open"), pa.array(["777"] * table.num_rows)
        )
        return real_write(changed, output, **kwargs)

    monkeypatch.setattr(implementation.pq, "write_table", corrupted)
    with pytest.raises(DataLayerError, match="Parquet roundtrip"):
        publish(store, bundle)
    assert peer_identity(store.root) == before
    assert list(store.root.glob(".incoming-*")) == []
    assert (original.path / "raw.json").read_bytes() == bundle["raw"]
