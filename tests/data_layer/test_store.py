"""Real candidate artifacts and independent retained-kernel diagnostics."""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


def publish(store, bundle):
    from candle_audit.data_layer.model import Window

    return store.ingest(
        bundle["candidate_csv"],
        bundle["metadata"],
        bundle["raw"],
        bundle["source"],
        Window.parse(bundle["start"], bundle["end"]),
        raw_time_unit="ms",
    )


def test_ingest_calls_both_kernels_and_writes_real_exact_parquet(tmp_path, bundle):
    from candle_audit.data_layer.store import CandidateStore

    snapshot = publish(CandidateStore(tmp_path / "store"), bundle)
    assert snapshot.reports["quality"]["structural_status"] == "PASS"
    assert snapshot.reports["reconcile"]["comparison"] == "MATCH"
    assert snapshot.reports["reconcile"]["coverage"]["status"] == "COMPLETE"
    assert snapshot.reports["reconcile"]["closedbar"]["status"] == "CLOSED"
    assert snapshot.reports["reconcile"]["source_verification"] == "UNVERIFIED"
    assert snapshot.reports["reconcile"]["admission"] == "NOT_EVALUATED"
    assert (snapshot.path / "raw.json").read_bytes() == bundle["raw"]
    assert (snapshot.path / "candidate.csv").read_bytes() == bundle["candidate_csv"]
    content = (snapshot.path / "bars.parquet").read_bytes()
    assert content[:4] == content[-4:] == b"PAR1"
    table = pq.read_table(snapshot.path / "bars.parquet")
    assert table.schema.field("timestamp_ns").type == pa.int64()
    assert table.schema.field("open").type == pa.string()
    assert table.to_pylist() == [
        {
            "timestamp_ns": 1767225600000000000,
            "open": "100.00",
            "high": "102",
            "low": "99",
            "close": "101",
            "qty": "2",
        },
        {
            "timestamp_ns": 1767229200000000000,
            "open": "101",
            "high": "103",
            "low": "100",
            "close": "102",
            "qty": "3",
        },
    ]


def test_store_root_cannot_be_an_arbitrary_project_directory():
    from candle_audit.data_layer.model import DataLayerError
    from candle_audit.data_layer.store import CandidateStore

    with pytest.raises(DataLayerError, match="controlled"):
        CandidateStore(Path.cwd() / "docs")
