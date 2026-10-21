"""Real coherent artifacts exercise each native and semantic reader rejection boundary."""

import hashlib
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from r2_helpers import resign
from test_r2_decoded_budget import three_hundred_rows
from test_store import publish

from candle_audit.data_layer import CandidateStore, DataLayerError, Window
from candle_audit.data_layer.store import canonical


def test_dictionary_indexes_are_budgeted_before_native_decode(tmp_path, bundle, monkeypatch):
    three_hundred_rows(bundle)
    store = CandidateStore(tmp_path / "store")
    snapshot = publish(store, bundle)
    table = pq.read_table(snapshot.path / "bars.parquet")
    values = ["x" * 55824 + f"{index:06}" for index in range(300)]
    hostile = table.set_column(1, table.schema.field("open"), pa.array(values))
    identity, directory = resign(snapshot, table=hostile)
    metadata = pq.ParquetFile(directory / "bars.parquet").metadata
    encoded = sum(metadata.row_group(i).total_byte_size for i in range(metadata.num_row_groups))
    assert encoded <= 16777216 < encoded + metadata.num_rows * metadata.num_columns * 16
    real_read = pq.read_table
    decoded = []

    def measured(*args, **kwargs):
        decoded.append(True)
        return real_read(*args, **kwargs)

    monkeypatch.setattr(pq, "read_table", measured)
    with pytest.raises(DataLayerError, match="dictionary decoding byte budget"):
        store.query(identity, Window.parse(bundle["start"], bundle["end"]))
    assert decoded == []
    assert (
        store.query(
            snapshot.snapshot_id, Window.parse(bundle["start"], bundle["end"])
        ).table.num_rows
        == 300
    )


def test_same_schema_and_row_count_still_require_exact_raw_values(tmp_path, bundle):
    store = CandidateStore(tmp_path / "store")
    snapshot = publish(store, bundle)
    table = pq.read_table(snapshot.path / "bars.parquet")
    hostile = table.set_column(1, table.schema.field("open"), pa.array(["101", "100.00"]))
    identity, _ = resign(snapshot, table=hostile)
    with pytest.raises(DataLayerError, match="roundtrip mismatch"):
        store.query(identity, Window.parse(bundle["start"], bundle["end"]))
    assert store.query(snapshot.snapshot_id, Window.parse(bundle["start"], bundle["end"])).table[
        "open"
    ].to_pylist() == ["100.00", "101"]


@pytest.mark.parametrize("window", [None, {"start_ns": 1767225600000000000}, {"x": 1}])
def test_signed_manifest_cannot_replace_the_exact_window_declaration(tmp_path, bundle, window):
    store = CandidateStore(tmp_path / "store")
    snapshot = publish(store, bundle)
    manifest = json.loads((snapshot.path / "manifest.json").read_bytes())
    manifest["window"] = window
    identity = {key: value for key, value in manifest.items() if key != "snapshot_id"}
    key = hashlib.sha256(canonical(identity)).hexdigest()
    manifest["snapshot_id"] = key
    bad_id, _ = resign(snapshot, {"manifest.json": canonical(manifest)})
    with pytest.raises(DataLayerError, match="window schema"):
        store.query(bad_id, Window.parse(bundle["start"], bundle["end"]))
    assert (
        store.query(
            snapshot.snapshot_id, Window.parse(bundle["start"], bundle["end"])
        ).table.num_rows
        == 2
    )


def test_physical_parquet_nullability_cannot_bypass_the_nonnull_schema_guard(tmp_path, bundle):
    store = CandidateStore(tmp_path / "store")
    snapshot = publish(store, bundle)
    table = pq.read_table(snapshot.path / "bars.parquet")
    physical = pq.ParquetFile(snapshot.path / "bars.parquet").schema
    assert all(physical.column(i).max_definition_level == 0 for i in range(len(table.column_names)))
    nulls = pa.array([None, "101"], type=pa.string())
    required = table.set_column(1, table.schema.field("open"), nulls)
    with pytest.raises(pa.ArrowInvalid, match="non-nullable.*null"):
        pq.write_table(required, tmp_path / "invalid-required.parquet")
    optional = table.set_column(1, pa.field("open", pa.string(), nullable=True), nulls)
    identity, directory = resign(snapshot, table=optional)
    metadata = pq.ParquetFile(directory / "bars.parquet").metadata
    assert metadata.schema.column(1).max_definition_level == 1
    assert metadata.schema.to_arrow_schema().field("open").nullable
    with pytest.raises(DataLayerError, match="schema mismatch"):
        store.query(identity, Window.parse(bundle["start"], bundle["end"]))
    assert (
        store.query(
            snapshot.snapshot_id, Window.parse(bundle["start"], bundle["end"])
        ).table.num_rows
        == 2
    )
