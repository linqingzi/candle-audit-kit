"""Legacy diagnostic acceptance does not prove canonical column namespace compatibility."""

import copy
import hashlib

import pytest
from r2_helpers import original_reports, resign

from candle_audit.data_layer import CandidateStore, DataLayerError, Window
from candle_audit.data_layer.store import Snapshot, canonical


def test_fresh_reader_still_checks_canonical_namespace_after_both_original_kernels_pass(
    tmp_path, bundle
):
    window = Window.parse(bundle["start"], bundle["end"])
    store = CandidateStore(tmp_path / "candidates")
    original = store.ingest(
        bundle["candidate_csv"],
        bundle["metadata"],
        bundle["raw"],
        bundle["source"],
        window,
        raw_time_unit="ms",
    )
    assert store.query(original.snapshot_id, window).table["open"].to_pylist() == ["100.00", "101"]
    changed_csv = bundle["candidate_csv"].replace(b",qty\n", b",timestamp_ns\n")
    assert changed_csv != bundle["candidate_csv"]
    metadata = copy.deepcopy(bundle["metadata"])
    metadata["volume"]["columns"][0]["column"] = "timestamp_ns"
    metadata["content_sha256"] = hashlib.sha256(changed_csv).hexdigest()
    key, directory = resign(
        original, {"candidate.csv": changed_csv, "metadata.json": canonical(metadata)}
    )
    reports = original_reports(directory)
    assert reports["quality"]["structural_status"] == "PASS"
    assert reports["reconcile"]["comparison"] == "MATCH"
    identity, _ = resign(Snapshot(key, directory, reports), {"reports.json": canonical(reports)})
    # _load actually accepts the diagnostic bundle; query must reach the canonical conversion.
    store._load(identity)
    with pytest.raises(DataLayerError, match="invalid snapshot schema/roundtrip:.*collides"):
        store.query(identity, window)
    assert store.query(original.snapshot_id, window).table.num_rows == 2
