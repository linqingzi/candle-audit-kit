"""The bounded dictionary counterexample cannot allocate a >16 MiB dense Arrow table."""

import hashlib
import json
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from r2_helpers import resign
from test_store import publish

from candle_audit.data_layer import CandidateStore, DataLayerError, Window


def three_hundred_rows(bundle, count=300):
    start = datetime(2026, 1, 1, tzinfo=UTC)
    csv = ["timestamp,open,high,low,close,qty"]
    raw = []
    for index in range(count):
        stamp = start + timedelta(hours=index)
        csv.append(stamp.isoformat() + ",100,102,99,101,2")
        raw.append(
            [
                str(1767225600000 + index * 3600000),
                "100.00",
                "102",
                "99",
                "101",
                "20",
                "2",
                "2010",
                "1",
            ]
        )
    bundle["candidate_csv"] = ("\n".join(csv) + "\n").encode()
    bundle["raw"] = json.dumps({"code": "0", "msg": "", "data": raw}).encode()
    bundle["end"] = (start + timedelta(hours=count)).isoformat()
    bundle["metadata"]["content_sha256"] = hashlib.sha256(bundle["candidate_csv"]).hexdigest()


@pytest.mark.parametrize("same_input_row_count", [False, True])
def test_dictionary_expansion_is_rejected_before_over_budget_materialization(
    tmp_path, bundle, monkeypatch, same_input_row_count
):
    if same_input_row_count:
        three_hundred_rows(bundle)
    store = CandidateStore(tmp_path / "store")
    original = publish(store, bundle)
    base = pq.read_table(original.path / "bars.parquet")
    rows = [base.to_pylist()[0]] * 300
    for row in rows:
        row["open"] = "x" * 65536
    hostile = pa.Table.from_pylist(rows, schema=base.schema)
    assert hostile.nbytes == 19671900
    identity, directory = resign(original, table=hostile)
    footer = pq.ParquetFile(directory / "bars.parquet").metadata
    assert footer.row_group(0).total_byte_size < 16 * 1024 * 1024
    assert (directory / "bars.parquet").stat().st_size < 10000
    actual = pq.read_table
    decoded_sizes = []

    def measured(*args, **kwargs):
        result = actual(*args, **kwargs)
        decoded_sizes.append(result.nbytes)
        return result

    monkeypatch.setattr(pq, "read_table", measured)
    with pytest.raises(DataLayerError):
        store.query(identity, Window.parse(bundle["start"], bundle["end"]))
    print(
        json.dumps(
            {
                "same_input_row_count": same_input_row_count,
                "counterexample_dense_bytes": hostile.nbytes,
                "encoded_footer_bytes": footer.row_group(0).total_byte_size,
                "parquet_file_bytes": (directory / "bars.parquet").stat().st_size,
                "native_decoded_sizes_before_rejection": decoded_sizes.copy(),
                "budget_bytes": 16777216,
            }
        )
    )
    assert all(size <= 16 * 1024 * 1024 for size in decoded_sizes)
    assert store.query(
        original.snapshot_id, Window.parse(bundle["start"], bundle["end"])
    ).table.num_rows == (300 if same_input_row_count else 2)
