"""Actual DuckDB queries across an Arrow/Parquet bridge without ns/decimal casts."""

import copy
import hashlib
import json

import pyarrow as pa
import pytest
from test_store import publish


def test_duckdb_preserves_raw_decimal_text_and_ns(tmp_path, bundle):
    from candle_audit.data_layer.model import Window
    from candle_audit.data_layer.store import CandidateStore

    bundle = copy.deepcopy(bundle)
    bundle["candidate_csv"] = bundle["candidate_csv"].replace(
        b",100,102,99,101,2\n",
        b",100.000000000000000001,102,99,101,0.000000000000000001234567890\n",
    )
    native = json.loads(bundle["raw"])
    native["data"][0][1] = "100.000000000000000001"
    native["data"][0][6] = "0.000000000000000001234567890"
    bundle["raw"] = json.dumps(native).encode()
    bundle["metadata"]["content_sha256"] = hashlib.sha256(bundle["candidate_csv"]).hexdigest()
    store = CandidateStore(tmp_path / "store")
    snapshot = publish(store, bundle)
    result = store.query(snapshot.snapshot_id, Window(1767225600000000000, 1767225600000000001))
    assert result.table.to_pylist() == [
        {
            "timestamp_ns": 1767225600000000000,
            "open": "100.000000000000000001",
            "high": "102",
            "low": "99",
            "close": "101",
            "qty": "0.000000000000000001234567890",
        }
    ]
    assert result.table.schema.field("timestamp_ns").type == pa.int64()
    assert result.table.schema.field("qty").type == pa.string()
    assert result.as_dict()["source_verification"] == "UNVERIFIED"


def test_legal_empty_query_retains_the_complete_typed_schema(tmp_path, bundle):
    from candle_audit.data_layer.model import Window
    from candle_audit.data_layer.store import CandidateStore

    store = CandidateStore(tmp_path / "store")
    snapshot = publish(store, bundle)
    populated = store.query(snapshot.snapshot_id, Window.parse(bundle["start"], bundle["end"]))
    empty = store.query(snapshot.snapshot_id, Window(1767225600000000001, 1767229200000000000))
    assert empty.table.num_rows == 0
    assert empty.table.schema.equals(populated.table.schema, check_metadata=True)
    document = empty.as_dict()
    assert document["query_status"] == "EMPTY"
    assert document["rows"] == [] and len(document["schema"]) == 6
    assert document["metadata"]["source"] == "synthetic-engineering"
    assert document["source_verification"] == "UNVERIFIED"
    assert document["admission"] == "NOT_EVALUATED"
    assert document["reports"]["reconcile"]["comparison"] == "MATCH"


@pytest.mark.parametrize("fields", [["open; SELECT 1"], ["open", "open"], [], ["missing"]])
def test_query_field_allowlist_rejects_sql_and_invalid_projections(tmp_path, bundle, fields):
    from candle_audit.data_layer.model import DataLayerError, Window
    from candle_audit.data_layer.store import CandidateStore

    store = CandidateStore(tmp_path / "store")
    snapshot = publish(store, bundle)
    with pytest.raises(DataLayerError):
        store.query(
            snapshot.snapshot_id, Window.parse(bundle["start"], bundle["end"]), fields=fields
        )
