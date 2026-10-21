"""Input-bound recovery, incremental versions and explicit reconstruction/public transport."""

import copy
import json
import time
from email.message import Message

import pytest
from test_full_collection import api as collection_api
from test_full_collection import collector, history, native, request
from test_full_reference import rules
from test_full_releases import api as release_api
from test_full_releases import built

from candle_audit.data_layer import DataLayerError, Window
from candle_audit.data_layer.providers import ProviderRegistry


def test_same_request_changed_import_bytes_cannot_reuse_success(tmp_path, bundle):
    module = collection_api()
    req = request(bundle, count=2, provider="file")
    provider = module.FileImportProvider(((bundle["raw"], bundle["candidate_csv"]),))
    registry = ProviderRegistry()
    registry.register("file", provider)
    pipeline = collector(tmp_path)
    original = pipeline.collect(req, registry)
    assert pipeline.collect(req, registry).collection_id == original.collection_id
    altered = ProviderRegistry()
    altered.register(
        "file",
        module.FileImportProvider(
            ((bundle["raw"].replace(b"100.00", b"100.01"), bundle["candidate_csv"]),)
        ),
    )
    with pytest.raises(DataLayerError):
        pipeline.collect(req, altered)
    assert pipeline.read(original.collection_id).rows == 2


def test_all_failed_network_attempts_are_counted_across_resume(tmp_path, bundle):
    module = collection_api()
    req = request(bundle, count=2, max_pages=1)
    pipeline = collector(tmp_path)
    calls = []

    def interrupted(url, method):
        calls.append(url)
        raise OSError("engineering disconnect")

    registry = ProviderRegistry()
    registry.register("history", module.HistoryOKXProvider(interrupted))
    for _ in range(4):
        with pytest.raises(module.CollectionFailure):
            pipeline.collect(req, registry, retries=0, sleep=lambda _: None)
    assert len(calls) == 3
    assert pipeline.task(req.request_id)["attempts"] == 3


def test_incremental_extension_adds_requested_window_and_preserves_old_release(tmp_path, bundle):
    store, original, previous = built(tmp_path, bundle)
    module = collection_api()
    window = Window(previous.window.end_ns, previous.window.end_ns + 2 * 3600 * 10**9)
    incremental = module.CollectionRequest(
        provider_id="history",
        rules=(rules(bundle, effective=window),),
        window=window,
        name="incremental-test",
        revision="r2",
        kind="synthetic",
        provenance="engineering/test-only",
        page_rows=2,
    )
    collected = store.collector.collect(
        incremental, history(bundle, incremental, [native(window.start_ns // 10**6, 2)])
    )
    assert hasattr(store, "extend"), "incremental release union consumer absent"
    extended = store.extend(
        original.release_id, [collected.collection_id], name="extended", producer_id="collector"
    )
    whole = Window(previous.window.start_ns, window.end_ns)
    assert store.reader().query(extended.release_id, whole).table.num_rows == 6
    assert store.reader().query(original.release_id, whole).table.num_rows == 4
    with pytest.raises(DataLayerError):
        store.extend(
            original.release_id,
            original.document["requested_collections"],
            name="overlap",
            producer_id="collector",
        )


def test_multiple_timeframes_and_leap_day_are_actual_canonical_consumers(tmp_path, bundle):
    module = collection_api()
    pipeline = collector(tmp_path)
    ids = []
    for timeframe, step, bar in [("1m", 60 * 10**9, "1m"), ("1h", 3600 * 10**9, "1H")]:
        modified = copy.deepcopy(bundle)
        modified["source"]["bar"] = bar
        window = Window.parse("2024-02-29T23:00:00Z", "2024-03-01T01:00:00Z")
        rule = rules(modified, timeframe=timeframe, effective=window)
        req = module.CollectionRequest(
            provider_id="history",
            rules=(rule,),
            window=window,
            name="leap-control",
            revision="r1",
            kind="synthetic",
            provenance="engineering/test-only",
            page_rows=300,
        )
        rows = native(
            window.start_ns // 10**6, (window.end_ns - window.start_ns) // step, step // 10**6
        )
        ids.append(pipeline.collect(req, history(modified, req, [rows])).collection_id)
    store = release_api().ReleaseStore(pipeline)
    release = store.create(ids, name="leap", producer_id="collector")
    data = store.reader().query(release.release_id, window)
    assert data.table.num_rows == 122
    assert set(data.table["timeframe"].to_pylist()) == {"1m", "1h"}


def test_reconstruction_is_explicit_preserves_values_and_cannot_silently_publish(tmp_path, bundle):
    import importlib

    aggregation = importlib.import_module("candle_audit.data_layer.aggregation")
    store, release, req = built(tmp_path, bundle)
    result = store.reader().query(release.release_id, req.window)
    assert hasattr(aggregation, "declare_reconstruction"), "explicit reconstruction consumer absent"
    declared = aggregation.declare_reconstruction(
        result,
        (req.window.start_ns,),
        evidence=b"engineering declaration of pre-existing reconstructed input",
    )
    assert declared.table["open"].to_pylist() == result.table["open"].to_pylist()
    assert declared.table["state"][0].as_py() == "DECLARED_RECONSTRUCTION"
    assert declared.visibility["reconstruction_evidence_sha256"]
    aggregated = aggregation.aggregate_candles(declared, "4h", req.window)
    assert aggregated.report["status"] == "CONTAINS_RECONSTRUCTION"
    assert aggregated.table["state"][0].as_py() == "DECLARED_RECONSTRUCTION"
    with pytest.raises(DataLayerError):
        aggregation.declare_reconstruction(result, (req.window.end_ns,), evidence=b"evidence")


def test_public_transport_uses_exact_public_get_and_consumes_its_grant(bundle):
    module = collection_api()
    req = request(bundle, count=2)
    grant = module.DownloadGrant(
        req.request_id, 1, 8388608, time.time_ns() + 10**10, "PUBLIC_OKX_HISTORY_ONLY"
    )
    transport = module.PublicOKXTransport(req, grant)
    raw = json.dumps({"code": "0", "msg": "", "data": native(1767225600000, 2)}).encode()
    headers = Message()
    headers["Content-Type"] = "application/json"

    class Response:
        status = 200

        def __init__(self):
            self.headers = headers

        def read(self, limit):
            return raw[:limit]

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    class OfflineOpener:
        def open(self, request, timeout):
            assert request.get_method() == "GET" and timeout == 10
            assert not request.has_header("Authorization")
            return Response()

    transport._opener = OfflineOpener()
    url = "https://www.okx.com/api/v5/market/history-candles?instId=BTC-USDT-SWAP&bar=1H&before=1767225599999&after=1767232800000&limit=2"
    assert transport(url, "GET") == raw
    with pytest.raises(DataLayerError):
        transport(url, "GET")


def test_failed_frozen_quality_reports_are_retained_with_legal_positive_control(tmp_path, bundle):
    module = collection_api()
    pipeline = collector(tmp_path)
    req = request(bundle, count=2)
    rows = native(1767225600000, 2)
    for row in rows:
        row[5:8] = ["1e309", "1e308", "1e309"]
    with pytest.raises(module.CollectionFailure):
        pipeline.collect(req, history(bundle, req, [rows]))
    state = pipeline.task(req.request_id)
    assert state.get("quality_reports_sha256"), "original kernel reports must survive quarantine"
    reports = json.loads(pipeline.artifacts.get(state["quality_reports_sha256"]))
    assert reports["quality"]["structural_status"] == "FAIL"
    good_req = request(bundle, count=2, revision="legal-control")
    result = pipeline.collect(good_req, history(bundle, good_req, [native(1767225600000, 2)]))
    assert pipeline.read(result.collection_id).rows == 2


def test_import_csv_rows_are_bounded_before_materializing_invalid_records(bundle):
    import tracemalloc

    module = collection_api()
    req = request(bundle, count=2, provider="file")
    raw = json.loads(bundle["raw"])["data"]
    hostile = bundle["candidate_csv"].splitlines()[0] + b"\n" + b"\n" * 200000
    tracemalloc.start()
    try:
        with pytest.raises(DataLayerError):
            module._validate_import(hostile, raw, req)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert peak < 4 * 1024 * 1024
    module._validate_import(bundle["candidate_csv"], raw, req)


def test_retryable_public_error_body_survives_successful_recovery(tmp_path, bundle):
    import io
    from urllib.error import HTTPError

    module = collection_api()
    req = request(bundle, count=2)
    grant = module.DownloadGrant(
        req.request_id, 2, 8388608, time.time_ns() + 10**10, "PUBLIC_OKX_HISTORY_ONLY"
    )
    transport = module.PublicOKXTransport(req, grant)
    raw = json.dumps({"code": "0", "msg": "", "data": native(1767225600000, 2)}).encode()
    error = b'{"code":"50011","msg":"engineering rate limit","data":[]}'
    headers = Message()
    headers["Content-Type"] = "application/json"

    class Response:
        status = 200

        def __init__(self):
            self.headers = headers

        def read(self, limit):
            return raw[:limit]

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    class OfflineOpener:
        calls = 0

        def open(self, request, timeout):
            self.calls += 1
            if self.calls == 1:
                raise HTTPError(
                    request.full_url, 429, "engineering retry", headers, io.BytesIO(error)
                )
            return Response()

    transport._opener = OfflineOpener()
    registry = ProviderRegistry()
    registry.register("history", module.HistoryOKXProvider(transport))
    pipeline = collector(tmp_path)
    result = pipeline.collect(req, registry, retries=1, sleep=lambda _: None)
    attempts = pipeline._pages(pipeline.task(req.request_id)["attempt_head"], 60)
    assert len(attempts) == 2
    assert pipeline.artifacts.get(attempts[0]["outcome"]["raw_sha256"]) == error
    assert attempts[0]["outcome"]["status"] == "RETRYABLE_HTTP_ERROR"
    assert result.rows == 2


def test_native_rate_limit_code_is_retried_with_original_error_receipt(tmp_path, bundle):
    module = collection_api()
    req = request(bundle, count=2)
    error = b'{"code":"50011","msg":"engineering native rate limit","data":[]}'
    good = json.dumps({"code": "0", "msg": "", "data": native(1767225600000, 2)}).encode()
    pages = [error, good]
    registry = ProviderRegistry()
    registry.register("history", module.HistoryOKXProvider(lambda url, method: pages.pop(0)))
    pipeline = collector(tmp_path)
    result = pipeline.collect(req, registry, retries=1, sleep=lambda _: None)
    attempts = pipeline._pages(pipeline.task(req.request_id)["attempt_head"], 60)
    assert attempts[0]["outcome"]["status"] == "RETRYABLE_API_ERROR"
    assert pipeline.artifacts.get(attempts[0]["outcome"]["raw_sha256"]) == error
    assert result.rows == 2


def test_live_grant_attempt_budget_survives_transport_recreation(tmp_path, bundle):
    module = collection_api()
    req = request(bundle, count=2)
    grant = module.DownloadGrant(
        req.request_id, 1, 8388608, time.time_ns() + 10**10, "PUBLIC_OKX_HISTORY_ONLY"
    )
    calls = []

    class Interrupted:
        def open(self, request, timeout):
            calls.append(request.full_url)
            raise OSError("engineering disconnect")

    pipeline = collector(tmp_path)
    for _ in range(2):
        transport = module.PublicOKXTransport(req, grant)
        transport._opener = Interrupted()
        registry = ProviderRegistry()
        registry.register("history", module.HistoryOKXProvider(transport))
        with pytest.raises(module.CollectionFailure):
            pipeline.collect(req, registry, retries=0, sleep=lambda _: None)
    assert len(calls) == 1


def test_original_error_response_bytes_count_toward_complete_request_budget(tmp_path, bundle):
    module = collection_api()
    error = b'{"code":"50011","msg":"engineering rate limit","data":[]}'
    req = request(bundle, count=2, max_total_bytes=len(error))
    pages = [error, error, error]
    registry = ProviderRegistry()
    registry.register("history", module.HistoryOKXProvider(lambda url, method: pages.pop(0)))
    pipeline = collector(tmp_path)
    with pytest.raises(module.CollectionFailure):
        pipeline.collect(req, registry, retries=2, sleep=lambda _: None)
    assert len(pages) == 2
    assert pipeline.task(req.request_id)["captured_bytes"] == len(error)
