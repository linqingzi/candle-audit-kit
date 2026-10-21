"""The character limit cannot be mistaken for a UTF-8 decoded byte limit."""

import gc
import hashlib
import json

import pyarrow as pa
import pyarrow.parquet as pq
from r2_helpers import resign
from test_r2_decoded_budget import three_hundred_rows
from test_store import publish

from candle_audit.data_layer import CandidateStore, DataLayerError, Window


def test_multi_byte_dictionary_cells_cannot_expand_past_the_dense_byte_budget(tmp_path, bundle):
    three_hundred_rows(bundle, 6200)
    csv = bundle["candidate_csv"].replace(b",qty\n", b",qty,contracts_qty,quote_qty\n")
    csv = csv.replace(b",99,101,2\n", b",99,101,2,20,2010\n")
    bundle["candidate_csv"] = csv
    bundle["metadata"]["content_sha256"] = hashlib.sha256(csv).hexdigest()
    bundle["metadata"]["volume"]["columns"].extend(
        [
            {
                "column": "contracts_qty",
                "unit": "contracts",
                "contract_size": 0.1,
                "contract_size_unit": "base",
            },
            {"column": "quote_qty", "unit": "quote"},
        ]
    )
    store = CandidateStore(tmp_path / "store")
    snapshot = publish(store, bundle)
    table = pq.read_table(snapshot.path / "bars.parquet")
    for index in range(1, 8):
        table = table.set_column(
            index, table.schema.field(index), pa.array(["\U0001d7d9" * 96] * 6200)
        )
    assert 16777216 < table.nbytes < 19671900
    key, _ = resign(snapshot, table=table)
    window = Window.parse(bundle["start"], bundle["end"])
    original_pool = pa.default_memory_pool()
    measured = pa.proxy_memory_pool(original_pool)

    def attempted_query():
        try:
            store.query(key, window)
        except DataLayerError as error:
            return str(error)
        return "ACCEPTED"

    pa.set_memory_pool(measured)
    try:
        error = attempted_query()
    finally:
        pa.set_memory_pool(original_pool)
    gc.collect()
    peak = measured.max_memory()
    print(
        json.dumps(
            {
                "counterexample_dense_bytes": table.nbytes,
                "native_arrow_query_peak_bytes": peak,
                "live_bytes_after_rejection": measured.bytes_allocated(),
                "budget_bytes": 16777216,
            }
        )
    )
    assert measured.bytes_allocated() == 0
    assert error != "ACCEPTED"
    assert peak <= 16777216, f"native Arrow peak {peak} exceeded budget before rejection"
    assert store.query(snapshot.snapshot_id, window).table.num_rows == 6200


def test_legitimate_unicode_decimal_text_retains_its_original_utf8_lexeme(tmp_path, bundle):
    unicode_two = "\U0001d7da"
    native = json.loads(bundle["raw"])
    native["data"][0][6] = unicode_two
    csv = bundle["candidate_csv"].replace(
        b",99,101,2\n", (",99,101," + unicode_two + "\n").encode()
    )
    bundle["candidate_csv"] = csv
    bundle["raw"] = json.dumps(native).encode()
    bundle["metadata"]["content_sha256"] = hashlib.sha256(csv).hexdigest()
    store = CandidateStore(tmp_path / "store")
    snapshot = publish(store, bundle)
    result = store.query(snapshot.snapshot_id, Window.parse(bundle["start"], bundle["end"]))
    assert result.table["qty"].to_pylist() == [unicode_two, "3"]
