"""Root-cause boundary and recurrence checks for the complete candidate software."""

import copy
import json

import pytest
from test_full_collection import api as collection_api
from test_full_collection import collector, history, native, request
from test_full_consumers import cli
from test_full_reference import api as reference_api
from test_full_reference import rules
from test_full_releases import api as releases_api
from test_full_releases import built, signed_admission

from candle_audit.data_layer import DataLayerError, Window


@pytest.mark.parametrize("change", ["tick", "contract"])
def test_exact_reference_rule_violations_cannot_pass_normalization(bundle, change):
    row = native(1767225600000, 2)[-1]
    if change == "tick":
        rule = rules(bundle, tick_size="1")
    else:
        rule = rules(bundle)
        row[6] = "0.9"
    with pytest.raises(DataLayerError):
        reference_api().Candle.from_native(row, rule, acquired_ns=1770000000000000000)
    good = native(1767225600000, 2)[-1]
    assert (
        reference_api()
        .Candle.from_native(good, rules(bundle), acquired_ns=1770000000000000000)
        .base_qty
        == "1"
    )


def test_large_window_final_singleton_is_repartitioned_within_frozen_budget(tmp_path, bundle):
    module = collection_api()
    start = 1704067200000000000
    window = Window(start, start + 10001 * 3600 * 10**9)
    req = module.CollectionRequest(
        provider_id="history",
        rules=(rules(bundle, effective=window),),
        window=window,
        name="large-engineering",
        revision="r1",
        kind="synthetic",
        provenance="engineering/test-only",
        shard_rows=10000,
        page_rows=300,
        max_pages=100,
        max_total_bytes=16 * 1024 * 1024,
    )
    rows = native(start // 10**6, 10001)
    pages = [rows[i : i + 300] for i in range(0, len(rows), 300)]
    pipeline = collector(tmp_path)
    result = pipeline.collect(req, history(bundle, req, pages), sleep=lambda _: None)
    assert result.rows == 10001
    assert sorted(s["rows"] for s in result.shards) == [2, 9999]
    assert pipeline.read(result.collection_id).rows == 10001


def test_invalid_api_payload_is_retained_as_original_quarantine_receipt(tmp_path, bundle):
    module = collection_api()
    req = request(bundle, count=2)
    from candle_audit.data_layer.providers import ProviderRegistry

    registry = ProviderRegistry()
    raw = b'{"code":"51000","msg":"engineering failure","data":[]}'
    registry.register("history", module.HistoryOKXProvider(lambda url, method: raw))
    pipeline = collector(tmp_path)
    with pytest.raises(module.CollectionFailure):
        pipeline.collect(req, registry)
    state = pipeline.task(req.request_id)
    receipt = json.loads(pipeline.artifacts.get(state["last_attempt_receipt"]))
    assert pipeline.artifacts.get(receipt["raw_sha256"]) == raw


def test_release_code_lineage_contains_actual_replayable_source_bytes(tmp_path, bundle):
    store, release, req = built(tmp_path, bundle)
    for relative, sha in release.document["lineage"]["code"]["files"].items():
        assert store.artifacts.get(sha), relative
    changed = copy.deepcopy(release.document)
    changed["lineage"]["code"]["files"]["data_layer/releases.py"] = "0" * 64
    resealed = store.artifacts.put(releases_api().canonical(changed))
    with pytest.raises(DataLayerError):
        store.reader().query(resealed, req.window)


def test_signed_publication_evidence_enables_historical_visibility_without_backdating_acquisition(
    tmp_path, bundle
):
    store, release, req = built(tmp_path, bundle, historical=True)
    publication = {
        "schema": "candle-audit-publication-evidence-v1",
        "release_id": release.release_id,
        "rows": [
            {
                "instrument": "BTC-USDT-SWAP",
                "timeframe": "1h",
                "open_time_ns": req.window.start_ns + index * 3600 * 10**9,
                "published_ns": req.window.start_ns + (index + 1) * 3600 * 10**9,
            }
            for index in range(4)
        ],
    }
    admitted, _, acceptance, signature = signed_admission(store, release, publication=publication)
    admitted.admit(acceptance, signature)
    result = admitted.reader().query(
        release.release_id,
        req.window,
        mode="RESEARCH",
        as_of_ns=req.window.start_ns + 2 * 3600 * 10**9,
        knowledge="published",
    )
    assert result.table.num_rows == 2
    assert result.table["published_ns"].to_pylist() == [1767229200000000000, 1767232800000000000]
    assert min(result.table["acquired_ns"].to_pylist()) > req.window.end_ns
    assert result.as_dict()["visibility"]["version_observed_ns"] > req.window.end_ns
    limited = admitted.reader().query(
        release.release_id,
        req.window,
        mode="RESEARCH",
        as_of_ns=req.window.start_ns + 2 * 3600 * 10**9,
        knowledge="published",
        limit=2,
    )
    assert limited.table.equals(result.table, check_metadata=True)


def test_empty_report_states_absence_and_unavailable_declared_window(tmp_path, bundle):
    from candle_audit.data_layer.reporting import report_document

    store, release, req = built(tmp_path, bundle)
    query = store.reader().query(
        release.release_id, Window(req.window.end_ns, req.window.end_ns + 3600 * 10**9)
    )
    report = report_document(query, release)
    assert report["query"]["query_status"] == "EMPTY"
    assert report["query_gaps"], "an empty query cannot conceal its unavailable requested window"


def test_invalid_new_cli_root_is_typed_error_without_traceback():
    result, doc = cli("task", "--root", "/not-authorized", "--request-id", "0" * 64)
    assert result.returncode == 1 and doc["status"] == "REJECTED"


@pytest.mark.parametrize("packet", [{}, {"release_id": "bad"}, None, [], {"verdict": "PASS"}])
def test_untrusted_approval_packets_raise_typed_errors(tmp_path, bundle, packet):
    store, _, _ = built(tmp_path, bundle, historical=True)
    with pytest.raises(DataLayerError):
        store.record_review(packet, b"x" * 64)
    with pytest.raises(DataLayerError):
        store.admit(packet, b"x" * 64)


def test_recovery_rejects_identity_or_counter_tamper(tmp_path, bundle):
    module = collection_api()
    pipeline = collector(tmp_path)
    req = request(bundle, count=4)
    rows = native(1767225600000, 4)
    with pytest.raises(module.CollectionFailure):
        pipeline.collect(
            req,
            history(bundle, req, [rows[:2], OSError("interrupt")]),
            retries=0,
            sleep=lambda _: None,
        )
    state = pipeline.task(req.request_id)
    state["bytes"] += 1
    pipeline.artifacts.save_state(req.request_id, state)
    with pytest.raises(DataLayerError):
        pipeline.collect(req, history(bundle, req, [rows[2:]]), sleep=lambda _: None)


def test_malformed_saved_approval_is_typed_rejection(tmp_path, bundle):
    store, release, req = built(tmp_path, bundle, historical=True)
    malicious = store.artifacts.put(releases_api().canonical({"payload": {}, "signature": "00"}))
    store.artifacts.save_state(release.release_id, {"approval_id": malicious})
    with pytest.raises(DataLayerError):
        store.reader().query(release.release_id, req.window, mode="RESEARCH")


def test_missing_checkpoint_fields_are_typed_rejection(tmp_path, bundle):
    pipeline = collector(tmp_path)
    req = request(bundle, count=2)
    pipeline.artifacts.save_state(req.request_id, {"status": "COMPLETE"})
    with pytest.raises(DataLayerError):
        pipeline.collect(req, history(bundle, req, [native(1767225600000, 2)]))


def test_resealed_receipt_cannot_change_actual_byte_counters_or_authority(tmp_path, bundle):
    pipeline = collector(tmp_path)
    req = request(bundle, count=2)
    result = pipeline.collect(req, history(bundle, req, [native(1767225600000, 2)]))
    receipt = json.loads(pipeline.artifacts.get(result.document["page_head"]))
    for field, value in [("bytes", 0), ("source_verification", "VERIFIED")]:
        changed = copy.deepcopy(receipt)
        changed[field] = value
        changed_id = pipeline.artifacts.put(releases_api().canonical(changed))
        document = copy.deepcopy(result.document)
        document["page_head"] = changed_id
        resealed = pipeline.artifacts.put(releases_api().canonical(document))
        with pytest.raises(DataLayerError):
            pipeline.read(resealed)
    assert pipeline.read(result.collection_id).rows == 2
