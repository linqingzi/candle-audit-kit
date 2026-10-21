"""B-02: retain frozen semantic replay, document scratch, prove persistent bytes and cleanup."""

import hashlib
import os
from contextlib import contextmanager
from pathlib import Path

import pytest
from test_full_releases import built

from candle_audit.data_layer import DataLayerError
from candle_audit.data_layer import store as kernel_store


def persistent_bytes(root):
    return {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in root.rglob("*")
        if p.is_file()
    }


@pytest.mark.parametrize("fail", [False, True])
def test_reader_verification_scratch_is_bounded_cleaned_and_never_changes_pipeline(
    tmp_path, bundle, monkeypatch, fail
):
    store, release, req = built(tmp_path, bundle)
    before = persistent_bytes(store.collector.root)
    scratch = Path(__file__).resolve().parents[2] / ".tmp-data-layer"
    owned = []
    descriptors = set()
    observed = []
    original_directory = kernel_store._temporary_directory
    original_create = kernel_store._create_file

    @contextmanager
    def observe_directory(parent, prefix):
        with original_directory(parent, prefix) as temporary:
            assert prefix == "semantic-"
            owned.append(scratch / temporary.name)
            assert (os.fstat(parent).st_dev, os.fstat(parent).st_ino) == (
                scratch.stat().st_dev,
                scratch.stat().st_ino,
            )
            descriptors.add(temporary.descriptor)
            try:
                yield temporary
            finally:
                descriptors.remove(temporary.descriptor)

    def observe_file(name, descriptor):
        assert descriptor in descriptors
        assert name in ("candidate.csv", "raw.json")
        observed.append(name)
        if fail and name == "raw.json":
            raise OSError("injected verification scratch I/O failure")
        return original_create(name, descriptor)

    monkeypatch.setattr(kernel_store, "_temporary_directory", observe_directory)
    monkeypatch.setattr(kernel_store, "_create_file", observe_file)
    if fail:
        with pytest.raises(DataLayerError):
            store.reader().query(release.release_id, req.window)
    else:
        assert store.reader().query(release.release_id, req.window).table.num_rows == 4
    assert observed and set(observed) == {"candidate.csv", "raw.json"}
    assert persistent_bytes(store.collector.root) == before
    assert owned and all(not p.exists() for p in owned)


def test_interface_document_declares_writable_verification_scratch():
    doc = (Path(__file__).resolve().parents[2] / "docs/data_layer_full.md").read_text()
    assert "verification scratch" in doc
    assert ".tmp-data-layer/semantic-" in doc
    assert "read-only mount" in doc
