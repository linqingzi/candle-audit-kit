"""Immutable release/reader/admission consumers; inputs and generated trust keys are test-only."""

import copy
import importlib
import json
import time

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from test_full_collection import collector, history, native, request

from candle_audit.data_layer import DataLayerError, Window


def api():
    assert importlib.util.find_spec("candle_audit.data_layer.releases"), "release lifecycle absent"
    return importlib.import_module("candle_audit.data_layer.releases")


def built(tmp_path, bundle, *, historical=False):
    pipeline = collector(tmp_path)
    req = request(bundle, count=4)
    if historical:
        changed = copy.deepcopy(bundle)
        changed["source"]["acquisition"] = "UNVERIFIED"
        req = request(changed, count=4, kind="historical")
    rows = native(1767225600000, 4)
    result = pipeline.collect(req, history(bundle, req, [rows[:2], rows[2:]]), sleep=lambda _: None)
    releases = api().ReleaseStore(pipeline)
    release = releases.create([result.collection_id], name="engineering", producer_id="collector")
    return releases, release, req


def test_collection_read_replays_without_mutations(tmp_path, bundle, monkeypatch):
    pipeline = collector(tmp_path)
    req = request(bundle, count=2)
    result = pipeline.collect(req, history(bundle, req, [native(1767225600000, 2)]))

    def forbidden(*args, **kwargs):
        raise AssertionError("reader must not mutate")

    monkeypatch.setattr(pipeline.artifacts, "put", forbidden)
    monkeypatch.setattr(pipeline.candidates, "ingest", forbidden)
    assert pipeline.read(result.collection_id).rows == 2


def test_release_reader_precise_parquet_duckdb_typed_empty_and_projection(tmp_path, bundle):
    releases, release, req = built(tmp_path, bundle)
    reader = releases.reader()
    result = reader.query(release.release_id, req.window)
    assert result.table.num_rows == 4
    assert result.table["open"].to_pylist() == ["100.000000000000000001"] * 4
    assert result.table["contracts_qty"].to_pylist() == ["10"] * 4
    empty = reader.query(release.release_id, Window(req.window.end_ns, req.window.end_ns + 1))
    assert empty.table.num_rows == 0
    assert empty.table.schema.equals(result.table.schema, check_metadata=True)
    selected = reader.query(
        release.release_id, req.window, fields=("instrument", "open_time_ns", "open")
    )
    assert selected.table.column_names == ["instrument", "open_time_ns", "open"]
    assert result.as_dict()["admission"] == "VERIFY"
    assert all(
        releases.artifacts.get(s["canonical_parquet_sha256"])[:4] == b"PAR1"
        for s in release.document["partitions"]
    )
    with pytest.raises(DataLayerError):
        reader.query(release.release_id, req.window, fields=("open;DROP TABLE x",))
    with pytest.raises(DataLayerError):
        reader.query(release.release_id, req.window, limit=3)


def test_multi_instrument_and_timeframe_request_set_is_explicit(tmp_path, bundle):
    module = api()
    pipeline = collector(tmp_path)
    req = request(bundle, count=2)
    one = pipeline.collect(req, history(bundle, req, [native(1767225600000, 2)]))
    other = copy.deepcopy(bundle)
    other["source"].update(base_asset="ETH", instId="ETH-USDT-SWAP")
    second = request(other, count=2)
    two = pipeline.collect(second, history(other, second, [native(1767225600000, 2)]))
    store = module.ReleaseStore(pipeline)
    release = store.create(
        [two.collection_id, one.collection_id], name="multi", producer_id="collector"
    )
    result = store.reader().query(release.release_id, req.window)
    assert sorted(set(result.table["instrument"].to_pylist())) == ["BTC-USDT-SWAP", "ETH-USDT-SWAP"]
    filtered = store.reader().query(release.release_id, req.window, instruments=("ETH-USDT-SWAP",))
    assert filtered.table.num_rows == 2
    assert store.catalog(release.release_id)["requested_collections"] == sorted(
        [one.collection_id, two.collection_id]
    )
    with pytest.raises(DataLayerError):
        store.create(
            [one.collection_id, one.collection_id], name="duplicate", producer_id="collector"
        )


def test_revision_retains_original_read_and_precise_change_report(tmp_path, bundle):
    module = api()
    releases, original, req = built(tmp_path, bundle)
    changed = request(bundle, count=4, revision="r2")
    rows = native(1767225600000, 4)
    rows[-1][1] = "100.000000000000000002"
    new = releases.collector.collect(
        changed, history(bundle, changed, [rows[:2], rows[2:]]), sleep=lambda _: None
    )
    revision = releases.create(
        [new.collection_id],
        name="revision",
        producer_id="collector",
        parent_id=original.release_id,
        reason="late source revision",
    )
    assert revision.document["parent_id"] == original.release_id
    diff = releases.reader().revisions(original.release_id, revision.release_id, req.window)
    assert diff["changed_fields"] == [
        {
            "instrument": "BTC-USDT-SWAP",
            "timeframe": "1h",
            "open_time_ns": 1767225600000000000,
            "field": "open",
            "before": "100.000000000000000001",
            "after": "100.000000000000000002",
        }
    ]
    assert (
        releases.reader().query(original.release_id, req.window).table["open"][0].as_py()
        == "100.000000000000000001"
    )
    assert module is not None


def test_asof_never_invents_historical_publication_and_cache_has_full_identity(tmp_path, bundle):
    module = api()
    releases, release, req = built(tmp_path, bundle)
    with pytest.raises(module.AsOfUnavailable):
        releases.reader().query(
            release.release_id, req.window, as_of_ns=time.time_ns(), knowledge="published"
        )
    with pytest.raises(module.AsOfUnavailable):
        releases.reader().query(
            release.release_id, req.window, as_of_ns=req.window.end_ns, knowledge="observed"
        )
    reader = releases.reader()
    one = reader.query(release.release_id, req.window, fields=("open",))
    two = reader.query(
        release.release_id, Window(req.window.start_ns + 1, req.window.end_ns), fields=("close",)
    )
    assert one.cache_key != two.cache_key
    assert two.table.num_rows == 3 and two.table.column_names == ["close"]


def signed_admission(releases, release, *, publication=None):
    module = api()
    reviewer, owner = Ed25519PrivateKey.generate(), Ed25519PrivateKey.generate()
    policy = module.TrustPolicy(
        {
            "auditor": module.Principal(reviewer.public_key().public_bytes_raw(), ("AUDITOR",)),
            "owner": module.Principal(owner.public_key().public_bytes_raw(), ("OWNER",)),
        }
    )
    store = module.ReleaseStore(releases.collector, trust_policy=policy)
    evidence = {
        key: store.artifacts.put(
            json.dumps(
                {
                    "release_id": release.release_id,
                    "engineering_test_only": True,
                    "evidence_kind": key,
                }
            ).encode()
        )
        for key in ("source", "full_window", "independent", "rules", "license")
    }
    if publication is not None:
        evidence["publication"] = store.artifacts.put(json.dumps(publication).encode())
    payload = store.review_payload(release.release_id, auditor_id="auditor", evidence=evidence)
    review = store.record_review(payload, reviewer.sign(module.canonical(payload)))
    acceptance = store.acceptance_payload(release.release_id, review, owner_id="owner")
    return store, review, acceptance, owner.sign(module.canonical(acceptance))


def test_external_signed_review_and_owner_acceptance_enable_exact_research_object(tmp_path, bundle):
    releases, release, req = built(tmp_path, bundle, historical=True)
    store, review, acceptance, signature = signed_admission(releases, release)
    with pytest.raises(DataLayerError):
        store.reader().query(release.release_id, req.window, mode="RESEARCH")
    approval = store.admit(acceptance, signature)
    result = store.reader().query(release.release_id, req.window, mode="RESEARCH")
    assert result.as_dict()["admission"] == "RESEARCH"
    assert result.as_dict()["approval_id"] == approval
    assert result.as_dict()["source_verification"] == "EXTERNALLY_ATTESTED"
    assert (
        store.review_payload(
            release.release_id, auditor_id="auditor", evidence=review["payload"]["evidence"]
        )["lineage_sha256"]
        == acceptance["lineage_sha256"]
    )


def test_synthetic_data_cannot_be_promoted_even_with_test_authority_signatures(tmp_path, bundle):
    releases, release, _ = built(tmp_path, bundle)
    with pytest.raises(DataLayerError):
        signed_admission(releases, release)


def test_forged_signature_or_changed_acceptance_never_admits(tmp_path, bundle):
    releases, release, req = built(tmp_path, bundle, historical=True)
    store, _, acceptance, signature = signed_admission(releases, release)
    with pytest.raises(DataLayerError):
        store.admit(acceptance, b"x" * 64)
    changed = copy.deepcopy(acceptance)
    changed["owner_id"] = "collector"
    with pytest.raises(DataLayerError):
        store.admit(changed, signature)
    with pytest.raises(DataLayerError):
        store.reader().query(release.release_id, req.window, mode="RESEARCH")
    store.admit(acceptance, signature)
    assert store.reader().query(release.release_id, req.window, mode="RESEARCH").table.num_rows == 4


def test_canonical_parquet_tamper_and_resealed_manifest_do_not_become_success(tmp_path, bundle):
    releases, release, req = built(tmp_path, bundle)
    doc = copy.deepcopy(release.document)
    doc["partitions"][0]["canonical_parquet_sha256"] = releases.artifacts.put(b"PAR1broken")
    resealed = releases.artifacts.put(api().canonical(doc))
    with pytest.raises(DataLayerError):
        releases.reader().query(resealed, req.window)
    assert releases.reader().query(release.release_id, req.window).table.num_rows == 4
