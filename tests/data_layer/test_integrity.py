"""Publication failures preserve old complete artifacts; identity binds actual inputs."""

import copy
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from test_store import publish


def tree_identity(path):
    return {p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in path.iterdir()}


@pytest.mark.parametrize("stage", ["parquet", "rename"])
def test_interrupted_publication_keeps_old_candidate_readable(tmp_path, bundle, monkeypatch, stage):
    from candle_audit.data_layer import CandidateStore, Window
    from candle_audit.data_layer import store as implementation

    store = CandidateStore(tmp_path / "store")
    first = publish(store, bundle)
    before = tree_identity(first.path)
    newer = copy.deepcopy(bundle)
    newer["metadata"]["version"] = "v2"

    def interrupt_parquet(table, path, **kwargs):
        path.write(b"PAR1-partial-write")
        raise OSError("synthetic interrupted parquet write")

    def interrupt_rename(source, destination, **kwargs):
        raise OSError("synthetic interrupted publish rename")

    if stage == "parquet":
        monkeypatch.setattr(pq, "write_table", interrupt_parquet)
    else:
        monkeypatch.setattr(implementation.os, "rename", interrupt_rename)
    with pytest.raises(OSError, match="synthetic interrupted"):
        publish(store, newer)
    assert tree_identity(first.path) == before
    assert [p.name for p in store.root.iterdir()] == [first.snapshot_id]
    result = store.query(first.snapshot_id, Window.parse(bundle["start"], bundle["end"]))
    assert [row["open"] for row in result.table.to_pylist()] == ["100.00", "101"]


@pytest.mark.parametrize("artifact", ["raw.json", "candidate.csv", "bars.parquet", "manifest.json"])
def test_modified_artifacts_are_rejected_even_with_the_same_row_shape(tmp_path, bundle, artifact):
    from candle_audit.data_layer import CandidateStore, DataLayerError, Window

    store = CandidateStore(tmp_path / "store")
    snapshot = publish(store, bundle)
    path = snapshot.path / artifact
    if artifact == "bars.parquet":
        table = pq.read_table(path)
        changed = table.set_column(1, table.schema.field("open"), pa.array(["100.1", "101"]))
        pq.write_table(changed, path, compression="zstd")
        manifest_path = snapshot.path / "manifest.json"
        manifest = json.loads(manifest_path.read_bytes())
        content = path.read_bytes()
        manifest["files"][artifact] = {
            "sha256": hashlib.sha256(content).hexdigest(),
            "bytes": len(content),
        }
        manifest_path.write_text(json.dumps(manifest))
    elif artifact == "manifest.json":
        manifest = json.loads(path.read_bytes())
        manifest["source_verification"] = "VERIFIED"
        path.write_text(json.dumps(manifest))
    else:
        path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(DataLayerError, match="integrity|identity"):
        store.query(snapshot.snapshot_id, Window.parse(bundle["start"], bundle["end"]))
    with pytest.raises(DataLayerError):
        publish(store, bundle)
    assert not list(store.root.glob(".incoming-*"))


def test_same_input_is_idempotent_but_content_and_source_changes_are_distinct(tmp_path, bundle):
    from candle_audit.data_layer import CandidateStore, Window

    store = CandidateStore(tmp_path / "store")
    first = publish(store, bundle)
    before = tree_identity(first.path)
    equivalent = copy.deepcopy(bundle)
    equivalent["start"] = "2026-01-01T01:00:00+01:00"
    equivalent["end"] = "2026-01-01T03:00:00+01:00"
    assert publish(store, equivalent).snapshot_id == first.snapshot_id
    assert tree_identity(first.path) == before
    source_change = copy.deepcopy(bundle)
    source_change["source"]["acquisition"] = "UNVERIFIED"
    second = publish(store, source_change)
    assert second.snapshot_id != first.snapshot_id
    content_change = copy.deepcopy(bundle)
    content_change["raw"] = bundle["raw"].replace(b'"100.00"', b'"100.000"')
    third = publish(store, content_change)
    assert len({first.snapshot_id, second.snapshot_id, third.snapshot_id}) == 3
    for snapshot in (first, second, third):
        assert (
            store.query(
                snapshot.snapshot_id, Window.parse(bundle["start"], bundle["end"])
            ).table.num_rows
            == 2
        )


@pytest.mark.parametrize("identity", ["../outside", "' OR 1=1 --", "A" * 64, "0" * 63])
def test_snapshot_identifiers_cannot_be_paths_or_sql(tmp_path, identity):
    from candle_audit.data_layer import CandidateStore, DataLayerError, Window

    with pytest.raises(DataLayerError, match="identity"):
        CandidateStore(tmp_path / "store").query(identity, Window(1, 2))


def test_snapshot_and_input_symlinks_cannot_escape_the_workspace(tmp_path, bundle):
    from candle_audit.data_layer import CandidateStore, DataLayerError, Window
    from candle_audit.data_layer.__main__ import load_input

    store = CandidateStore(tmp_path / "store")
    alias = store.root / ("0" * 64)
    alias.symlink_to(Path.cwd() / "docs", target_is_directory=True)
    with pytest.raises(DataLayerError, match="escape"):
        store.query(alias.name, Window.parse(bundle["start"], bundle["end"]))
    raw_alias = tmp_path / "private.json"
    raw_alias.symlink_to(Path.cwd() / "pyproject.toml")
    with pytest.raises(DataLayerError, match="controlled"):
        load_input(str(raw_alias), ".json", 65536)


def test_replaced_store_directory_is_refused_before_any_new_write(tmp_path, bundle):
    from candle_audit.data_layer import CandidateStore, DataLayerError

    store = CandidateStore(tmp_path / "store")
    saved = tmp_path / "saved"
    store.root.rename(saved)
    target = tmp_path / "other-controlled-directory"
    target.mkdir()
    store.root.symlink_to(target, target_is_directory=True)
    with pytest.raises(DataLayerError):
        publish(store, bundle)
    assert list(target.iterdir()) == []


def test_reader_binds_parquet_schema_to_raw_and_declarations_even_for_a_new_hash(tmp_path, bundle):
    from candle_audit.data_layer import CandidateStore, DataLayerError, Window
    from candle_audit.data_layer.store import canonical

    store = CandidateStore(tmp_path / "store")
    snapshot = publish(store, bundle)
    path = snapshot.path / "bars.parquet"
    table = pq.read_table(path)
    pq.write_table(
        table.rename_columns(["timestamp_ns", 'open"', "high", "low", "close", "qty"]), path
    )
    manifest_path = snapshot.path / "manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    content = path.read_bytes()
    manifest["files"]["bars.parquet"] = {
        "sha256": hashlib.sha256(content).hexdigest(),
        "bytes": len(content),
    }
    identity = {key: value for key, value in manifest.items() if key != "snapshot_id"}
    replacement = hashlib.sha256(canonical(identity)).hexdigest()
    manifest["snapshot_id"] = replacement
    manifest_path.write_bytes(canonical(manifest))
    snapshot.path.rename(store.root / replacement)
    with pytest.raises(DataLayerError, match="schema|roundtrip"):
        store.query(replacement, Window.parse(bundle["start"], bundle["end"]))


def test_parquet_row_budget_is_checked_before_decoding(tmp_path, bundle, monkeypatch):
    from candle_audit.data_layer import CandidateStore, DataLayerError, Window
    from candle_audit.data_layer.store import canonical

    store = CandidateStore(tmp_path / "store")
    snapshot = publish(store, bundle)
    path = snapshot.path / "bars.parquet"
    original = pq.read_table(path)
    large = pa.Table.from_pylist([original.to_pylist()[0]] * 10001, schema=original.schema)
    pq.write_table(large, path)
    manifest_path = snapshot.path / "manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    content = path.read_bytes()
    manifest["rows"] = 10001
    manifest["files"]["bars.parquet"] = {
        "sha256": hashlib.sha256(content).hexdigest(),
        "bytes": len(content),
    }
    identity = {key: value for key, value in manifest.items() if key != "snapshot_id"}
    replacement = hashlib.sha256(canonical(identity)).hexdigest()
    manifest["snapshot_id"] = replacement
    manifest_path.write_bytes(canonical(manifest))
    snapshot.path.rename(store.root / replacement)

    def unsafe_decode(*args, **kwargs):
        raise AssertionError("row budget must be checked in real Parquet metadata before decoding")

    monkeypatch.setattr(pq, "read_table", unsafe_decode)
    with pytest.raises(DataLayerError, match="row budget"):
        store.query(replacement, Window.parse(bundle["start"], bundle["end"]))


def test_parquet_expanded_byte_budget_is_checked_before_decoding(tmp_path, bundle, monkeypatch):
    from candle_audit.data_layer import CandidateStore, DataLayerError, Window
    from candle_audit.data_layer.store import canonical

    store = CandidateStore(tmp_path / "store")
    snapshot = publish(store, bundle)
    path = snapshot.path / "bars.parquet"
    original = pq.read_table(path)
    large = original.set_column(
        1,
        original.schema.field("open"),
        pa.array(["x" * (9 * 1024 * 1024), "y" * (9 * 1024 * 1024)]),
    )
    pq.write_table(large, path, compression="zstd")
    assert pq.ParquetFile(path).metadata.row_group(0).total_byte_size > 16 * 1024 * 1024
    manifest_path = snapshot.path / "manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    content = path.read_bytes()
    manifest["files"]["bars.parquet"] = {
        "sha256": hashlib.sha256(content).hexdigest(),
        "bytes": len(content),
    }
    identity = {key: value for key, value in manifest.items() if key != "snapshot_id"}
    replacement = hashlib.sha256(canonical(identity)).hexdigest()
    manifest["snapshot_id"] = replacement
    manifest_path.write_bytes(canonical(manifest))
    snapshot.path.rename(store.root / replacement)

    def unsafe_decode(*args, **kwargs):
        raise AssertionError("expanded byte budget must precede Parquet decoding")

    monkeypatch.setattr(pq, "read_table", unsafe_decode)
    with pytest.raises(DataLayerError, match="byte budget"):
        store.query(replacement, Window.parse(bundle["start"], bundle["end"]))
