"""A real CLI must terminate after rejecting a large dictionary cell."""

import json
import subprocess
import sys

import pyarrow as pa
import pyarrow.parquet as pq
from r2_helpers import resign
from test_store import publish

from candle_audit.data_layer import CandidateStore, Window


def test_real_query_cli_rejects_dictionary_cell_and_terminates(tmp_path, bundle):
    store = CandidateStore(tmp_path / "store")
    snapshot = publish(store, bundle)
    table = pq.read_table(snapshot.path / "bars.parquet")
    table = table.set_column(1, table.schema.field("open"), pa.array(["x" * 65536] * 2))
    identity, _ = resign(snapshot, table=table)
    command = [
        sys.executable,
        "-B",
        "-m",
        "candle_audit.data_layer",
        "query",
        "--store",
        str(store.root),
        "--snapshot",
        identity,
        "--start",
        bundle["start"],
        "--end",
        bundle["end"],
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=False, timeout=10)
    assert result.returncode == 1 and "Traceback" not in result.stderr
    document = json.loads(result.stdout)
    assert document["status"] == "REJECTED" and "budget" in document["error"]
    assert document["source_verification"] == "UNVERIFIED"
    assert document["admission"] == "NOT_EVALUATED"
    assert (
        store.query(
            snapshot.snapshot_id, Window.parse(bundle["start"], bundle["end"])
        ).table.num_rows
        == 2
    )
