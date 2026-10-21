"""Writer races observe peer side effects before rejection, including later cleanup."""

import hashlib
import os
import shutil
from pathlib import Path

import pytest
from test_store import publish

from candle_audit.data_layer import CandidateStore, DataLayerError
from candle_audit.data_layer import store as implementation


def peer_identity(peer):
    return {
        str(p.relative_to(peer)): (
            p.stat().st_dev,
            p.stat().st_ino,
            hashlib.sha256(p.read_bytes()).hexdigest() if p.is_file() else None,
        )
        for p in peer.rglob("*")
    }


@pytest.mark.parametrize(
    "target,phase",
    [
        ("root", "anchor"),
        ("root", "staging"),
        ("root", "write"),
        ("stage", "write"),
        ("root", "parquet"),
        ("stage", "parquet"),
        ("root", "publish"),
        ("stage", "publish"),
        ("root-real", "publish"),
        ("stage-real", "publish"),
        ("root", "cleanup"),
        ("stage", "cleanup"),
    ],
)
def test_writer_swap_never_changes_peer_before_rejecting(
    tmp_path, bundle, monkeypatch, target, phase
):
    store = CandidateStore(tmp_path / "store")
    peer = tmp_path / "peer"
    peer.mkdir()
    (peer / "marker.bin").write_bytes(b"synthetic-engineering/test-only peer")
    saved_root, saved_stage = tmp_path / "saved-root", tmp_path / "saved-stage"
    stage = None
    stage_identity = None
    swapped = False
    preparing = False
    before = peer_identity(peer)
    peer_changes = []
    real_mkdir, real_open, real_rename = os.mkdir, os.open, os.rename
    real_write, real_verify = Path.write_bytes, implementation._verify_inputs
    real_controlled, real_parquet = implementation.controlled_path, implementation.pq.write_table

    def observe():
        if swapped and not preparing and peer_identity(peer) != before:
            peer_changes.append(sorted(peer_identity(peer)))

    def swap():
        nonlocal swapped, before, peer, preparing
        if swapped:
            return
        preparing = True
        if stage is not None:
            destination = peer / stage.name if target.startswith("root") else peer
            shutil.copytree(stage, destination, dirs_exist_ok=True)
        before = peer_identity(peer)
        swapped = True
        if target.startswith("root"):
            store.root.rename(saved_root)
            if target == "root-real":
                peer.rename(store.root)
                peer = store.root
            else:
                store.root.symlink_to(peer, target_is_directory=True)
        else:
            stage.rename(saved_stage)
            if target == "stage-real":
                shutil.copytree(peer, stage)
            else:
                stage.symlink_to(peer, target_is_directory=True)
        before = peer_identity(peer)
        preparing = False

    def mkdir(path, *args, **kwargs):
        nonlocal stage, stage_identity
        incoming = Path(path).name.startswith(".incoming-")
        if incoming and phase == "staging":
            swap()
        result = real_mkdir(path, *args, **kwargs)
        if incoming:
            stage = store.root / Path(path).name
            info = stage.stat()
            stage_identity = info.st_dev, info.st_ino
        observe()
        return result

    def opened(path, flags, *args, **kwargs):
        if Path(path).name == "candidate.csv" and flags & os.O_WRONLY and phase == "write":
            swap()
        result = real_open(path, flags, *args, **kwargs)
        observe()
        return result

    def written(path, content):
        if path.name == "candidate.csv" and path.parent == stage and phase == "write":
            swap()
        result = real_write(path, content)
        observe()
        return result

    def renamed(source, destination, *args, **kwargs):
        if Path(source).name.startswith(".incoming-") and phase == "publish":
            swap()
        result = real_rename(source, destination, *args, **kwargs)
        observe()
        return result

    def verified(*args, **kwargs):
        if phase == "cleanup":
            swap()
            raise DataLayerError("injected engineering validation failure")
        return real_verify(*args, **kwargs)

    def controlled(path):
        result = real_controlled(path)
        if path == store.root and phase == "anchor":
            swap()
        return result

    def parquet(*args, **kwargs):
        if phase == "parquet":
            swap()
        result = real_parquet(*args, **kwargs)
        observe()
        return result

    monkeypatch.setattr(implementation.os, "mkdir", mkdir)
    monkeypatch.setattr(implementation.os, "open", opened)
    monkeypatch.setattr(implementation.os, "rename", renamed)
    monkeypatch.setattr(Path, "write_bytes", written)
    monkeypatch.setattr(implementation, "_verify_inputs", verified)
    monkeypatch.setattr(implementation, "controlled_path", controlled)
    monkeypatch.setattr(implementation.pq, "write_table", parquet)
    try:
        result = publish(store, bundle)
    except (DataLayerError, OSError):
        result = None
    observe()
    assert swapped
    assert peer_changes == [], "peer changed before eventual rejection"
    assert peer_identity(peer) == before
    if result is not None:
        assert not store.root.is_symlink()
        assert not result.path.is_symlink()
        info = result.path.stat()
        assert (info.st_dev, info.st_ino) == stage_identity
        assert (result.path / "raw.json").read_bytes() == bundle["raw"]


def test_legitimate_writer_duplicate_and_failed_cleanup_remain_live(tmp_path, bundle, monkeypatch):
    store = CandidateStore(tmp_path / "store")
    original = publish(store, bundle)
    before = peer_identity(store.root)
    assert publish(store, bundle).snapshot_id == original.snapshot_id
    assert peer_identity(store.root) == before
    (original.path / "raw.json").write_bytes(b"corrupted test-only bytes")
    damaged = peer_identity(store.root)
    with pytest.raises(DataLayerError):
        publish(store, bundle)
    assert peer_identity(store.root) == damaged
    assert list(store.root.glob(".incoming-*")) == []
