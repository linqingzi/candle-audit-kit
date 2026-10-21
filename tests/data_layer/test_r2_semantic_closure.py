"""Coherently re-signed snapshots still have to satisfy the original candle oracles."""

import hashlib
import json

import pytest
from r2_helpers import original_reports, resign
from test_store import publish

from candle_audit.data_layer import CandidateStore, DataLayerError, Window
from candle_audit.data_layer.store import Snapshot, canonical


@pytest.mark.parametrize(
    "case",
    [
        "unclosed",
        "bad_csv",
        "false_report",
        "duplicate_raw",
        "unaligned_raw",
        "extended_window",
        "wrong_market",
        "wrong_volume_asset",
    ],
)
@pytest.mark.parametrize("entry", ["query", "load"])
def test_every_reader_rejects_resigned_invalid_semantics(tmp_path, bundle, case, entry):
    store = CandidateStore(tmp_path / "store")
    snapshot = publish(store, bundle)
    native = json.loads(bundle["raw"])
    metadata = json.loads((snapshot.path / "metadata.json").read_bytes())
    manifest = json.loads((snapshot.path / "manifest.json").read_bytes())
    reports = json.loads((snapshot.path / "reports.json").read_bytes())
    changes = {}
    rebuild = False
    if case == "unclosed":
        native["data"][0][8] = "0"
        changes["raw.json"] = canonical(native)
    if case == "bad_csv":
        csv = bundle["candidate_csv"].replace(b",100,102,99,101,2", b",0,102,99,101,2")
        metadata["content_sha256"] = hashlib.sha256(csv).hexdigest()
        changes.update({"candidate.csv": csv, "metadata.json": canonical(metadata)})
        rebuild = True
    if case == "false_report":
        reports["quality"]["structural_status"] = "FAIL"
        changes["reports.json"] = canonical(reports)
    if case == "duplicate_raw":
        native["data"].append(native["data"][0].copy())
        changes["raw.json"] = canonical(native)
        rebuild = True
    if case == "unaligned_raw":
        native["data"][0][0] = "1767225600001"
        changes["raw.json"] = canonical(native)
        rebuild = True
    if case == "extended_window":
        manifest["window"]["end_ns"] = 1767236400000000000
        changes["manifest.json"] = canonical(manifest)
        rebuild = True
    if case == "wrong_market":
        metadata["market"] = "ETH-USDT-SWAP"
        changes["metadata.json"] = canonical(metadata)
        rebuild = True
    if case == "wrong_volume_asset":
        metadata["volume"]["base_asset"] = "ETH"
        changes["metadata.json"] = canonical(metadata)
        rebuild = True
    identity, _ = resign(snapshot, changes, rebuild=rebuild)
    with pytest.raises(DataLayerError):
        if entry == "load":
            store._load(identity)
        else:
            store.query(identity, Window.parse(bundle["start"], bundle["end"]))
    assert (
        store.query(
            snapshot.snapshot_id, Window.parse(bundle["start"], bundle["end"])
        ).table.num_rows
        == 2
    )


def test_resigned_legal_numeric_equivalence_and_nonbusiness_annotations_stay_readable(
    tmp_path, bundle
):
    store = CandidateStore(tmp_path / "store")
    original = publish(store, bundle)
    csv = bundle["candidate_csv"].replace(b",100,102,99,101,2", b",100.000,102,99,101,2")
    metadata = json.loads((original.path / "metadata.json").read_bytes())
    metadata["content_sha256"] = hashlib.sha256(csv).hexdigest()
    interim, path = resign(
        original, {"candidate.csv": csv, "metadata.json": canonical(metadata)}, rebuild=True
    )
    reports = original_reports(path)
    assert reports["reconcile"]["comparison"] == "MATCH"
    reports["annotations"] = {
        "kind": "synthetic-engineering/test-only",
        "local_path": str(tmp_path),
    }
    reports["quality"]["checks"]["metadata"]["detail"] = "nonbusiness temporary path annotation"
    identity, _ = resign(Snapshot(interim, path, reports), {"reports.json": canonical(reports)})
    result = store.query(identity, Window.parse(bundle["start"], bundle["end"]))
    assert result.table["open"].to_pylist() == ["100.00", "101"]
    assert result.reports["reconcile"]["comparison"] == "MATCH"
    assert result.reports["quality"]["structural_status"] == "PASS"
    assert result.as_dict()["source_verification"] == "UNVERIFIED"
