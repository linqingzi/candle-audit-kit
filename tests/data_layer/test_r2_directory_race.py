"""Deterministic benign ancestor swaps must never redirect the reader to another inode."""

import os
import shutil

import pytest
from test_store import publish

from candle_audit.data_layer import CandidateStore, DataLayerError, Window
from candle_audit.data_layer import store as implementation


@pytest.mark.parametrize("entry", ["load", "query"])
def test_checked_snapshot_directory_swap_does_not_read_the_peer(
    tmp_path, bundle, monkeypatch, entry
):
    store = CandidateStore(tmp_path / "store")
    original = publish(store, bundle)
    target, saved, peer = original.path, tmp_path / "saved", tmp_path / "peer"
    shutil.copytree(target, peer)
    peer_inodes = {(p.stat().st_dev, p.stat().st_ino) for p in peer.iterdir()}
    peer_reads = []
    real_open, real_read = os.open, implementation.read_bounded
    swapped = False

    def inspect_open(path, flags, *args, **kwargs):
        fd = real_open(path, flags, *args, **kwargs)
        opened = os.fstat(fd)
        if (opened.st_dev, opened.st_ino) in peer_inodes:
            peer_reads.append(str(path))
        return fd

    def swap_before_first_leaf(path, limit, **kwargs):
        nonlocal swapped
        if path.parent == target and not swapped:
            target.rename(saved)
            target.symlink_to(peer, target_is_directory=True)
            swapped = True
        return real_read(path, limit, **kwargs)

    monkeypatch.setattr(implementation.os, "open", inspect_open)
    monkeypatch.setattr(implementation, "read_bounded", swap_before_first_leaf)
    try:
        if entry == "load":
            _, files = store._load(original.snapshot_id)
            assert files["raw.json"] == bundle["raw"]
        else:
            result = store.query(original.snapshot_id, Window.parse(bundle["start"], bundle["end"]))
            assert result.table.num_rows == 2
    except DataLayerError:
        pass
    assert swapped
    assert peer_reads == []
