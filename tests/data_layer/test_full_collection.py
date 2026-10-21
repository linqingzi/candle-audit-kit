"""Shared import/history collection; network is blocked by the retained test fixture."""

import copy
import importlib
import json
from urllib.parse import parse_qs, urlsplit

import pytest
from test_full_reference import rules

from candle_audit.data_layer import DataLayerError, Window
from candle_audit.data_layer.providers import ProviderRegistry


def api():
    assert importlib.util.find_spec("candle_audit.data_layer.collection"), (
        "collection pipeline absent"
    )
    return importlib.import_module("candle_audit.data_layer.collection")


def native(start, count, step=3600000):
    return [
        [str(start + i * step), "100.000000000000000001", "102", "99", "101", "10", "1", "100", "1"]
        for i in range(count)
    ][::-1]


def request(bundle, count=6, provider="history", **kwargs):
    module = api()
    altered = copy.deepcopy(bundle)
    window = Window(1767225600000000000, 1767225600000000000 + count * 3600 * 10**9)
    rule = rules(altered, effective=window)
    values = dict(
        provider_id=provider,
        rules=(rule,),
        window=window,
        name="test-collection",
        revision="r1",
        kind="synthetic",
        provenance="engineering/test-only",
        shard_rows=2,
        page_rows=2,
        max_pages=20,
        max_total_bytes=8388608,
    )
    if provider == "file":
        values["volume_columns"] = ({"column": "qty", "unit": "base"},)
    values.update(kwargs)
    return module.CollectionRequest(**values)


def collector(tmp_path):
    return api().Collector(tmp_path / "pipeline")


def history(bundle, req, pages, calls=None):
    module = api()

    def transport(url, method):
        params = parse_qs(urlsplit(url).query)
        if calls is not None:
            calls.append(params)
        value = pages.pop(0)
        if isinstance(value, BaseException):
            raise value
        return json.dumps({"code": "0", "msg": "", "data": value}).encode()

    registry = ProviderRegistry()
    registry.register(req.provider_id, module.HistoryOKXProvider(transport))
    return registry


def test_paginated_history_preserves_receipts_exact_shards_and_full_union(tmp_path, bundle):
    module = api()
    req = request(bundle)
    rows = native(1767225600000, 6)
    calls = []
    pipeline = collector(tmp_path)
    result = pipeline.collect(
        req, history(bundle, req, [rows[:2], rows[2:4], rows[4:]], calls), sleep=lambda _: None
    )
    assert result.status == "COMPLETE" and result.rows == 6
    assert len(result.shards) == 3
    assert [x["after"] for x in calls] == [["1767247200000"], ["1767240000000"], ["1767232800000"]]
    receipts = pipeline.pages(result)
    assert len(receipts) == 3
    assert all(r["acquired_ns"] > 1767247200000000000 for r in receipts)
    assert all(pipeline.artifacts.get(r["raw_sha256"]) for r in receipts)
    data = pipeline.read(result.collection_id)
    assert data.rows == 6 and data.collection_id == result.collection_id
    assert data.quality["completeness"]["status"] == "PASS"
    assert data.quality["accuracy"]["status"] == "UNVERIFIED"
    assert data.quality["independent_granularity"]["status"] == "UNVERIFIED"
    assert data.request_id == req.request_id
    assert module.CollectionRequest.from_dict(req.as_dict()).request_id == req.request_id


def test_file_import_uses_shared_result_and_keeps_both_original_bytes(tmp_path, bundle):
    module = api()
    req = request(bundle, count=2, provider="file")
    registry = ProviderRegistry()
    registry.register(
        "file", module.FileImportProvider(((bundle["raw"], bundle["candidate_csv"]),))
    )
    pipeline = collector(tmp_path)
    result = pipeline.collect(req, registry)
    page = pipeline.pages(result)[0]
    assert pipeline.artifacts.get(page["raw_sha256"]) == bundle["raw"]
    assert pipeline.artifacts.get(page["csv_sha256"]) == bundle["candidate_csv"]
    assert pipeline.read(result.collection_id).rows == 2


def test_checkpoint_binds_all_request_parameters_and_recovers_without_refetch(tmp_path, bundle):
    module = api()
    req = request(bundle)
    rows = native(1767225600000, 6)
    pipeline = collector(tmp_path)
    first = history(bundle, req, [rows[:2], OSError("offline interruption")])
    with pytest.raises(module.CollectionFailure):
        pipeline.collect(req, first, retries=0, sleep=lambda _: None)
    state = pipeline.task(req.request_id)
    assert state["status"] == "FAILED" and state["rows"] == 2
    assert state["alert"]["code"] == "ACQUISITION_FAILED"
    calls = []
    result = pipeline.collect(
        req, history(bundle, req, [rows[2:4], rows[4:]], calls), sleep=lambda _: None
    )
    assert result.rows == 6 and len(calls) == 2
    assert pipeline.task(req.request_id)["status"] == "COMPLETE"
    changed = request(bundle, revision="r2")
    assert changed.request_id != req.request_id
    assert pipeline.task(changed.request_id) is None


@pytest.mark.parametrize("bad", ["empty", "repeated", "gap", "unclosed", "float", "conflict"])
def test_bad_pages_quarantine_without_complete_publication(tmp_path, bundle, bad):
    module = api()
    req = request(bundle, count=4)
    rows = native(1767225600000, 4)
    first, last = rows[:2], rows[2:]
    if bad == "empty":
        last = []
    elif bad == "repeated":
        last = first
    elif bad == "gap":
        last = last[:1]
    elif bad == "unclosed":
        last[0][8] = "0"
    elif bad == "float":
        last[0][1] = 100.0
    else:
        last.append(copy.deepcopy(last[0]))
        last[-1][1] = "99"
    pipeline = collector(tmp_path)
    with pytest.raises(module.CollectionFailure):
        pipeline.collect(req, history(bundle, req, [first, last, []]), sleep=lambda _: None)
    state = pipeline.task(req.request_id)
    assert state["status"] in ("QUARANTINED", "FAILED")
    assert state["collection_id"] is None
    assert state["alert"]["severity"] == "ERROR"


def test_retry_budget_rate_limit_and_request_budget_are_enforced(tmp_path, bundle):
    req = request(bundle, count=2)
    pauses = []
    result = collector(tmp_path).collect(
        req,
        history(bundle, req, [OSError("temporary"), native(1767225600000, 2)]),
        retries=1,
        sleep=pauses.append,
    )
    assert result.rows == 2 and sum(pauses) >= 0.1
    limited = request(bundle, max_pages=1)
    with pytest.raises(api().CollectionFailure):
        collector(tmp_path / "limited").collect(
            limited,
            history(bundle, limited, [native(1767225600000, 6)[:2]]),
            sleep=lambda _: None,
        )


def test_live_public_transport_requires_explicit_matching_bounded_grant(bundle):
    module = api()
    req = request(bundle)
    with pytest.raises(DataLayerError):
        module.PublicOKXTransport(req, None)
    grant = module.DownloadGrant(
        req.request_id, 2, 8388608, 9000000000000000000, "PUBLIC_OKX_HISTORY_ONLY"
    )
    transport = module.PublicOKXTransport(req, grant)
    for url in [
        "http://www.okx.com/api/v5/market/history-candles",
        "https://evil.test/",
        "https://www.okx.com/api/v5/account/balance",
    ]:
        with pytest.raises(DataLayerError):
            transport(url, "GET")
    assert transport.requests == 0


def test_partial_incremental_failure_does_not_destroy_prior_collection(tmp_path, bundle):
    pipeline = collector(tmp_path)
    req = request(bundle, count=2)
    result = pipeline.collect(req, history(bundle, req, [native(1767225600000, 2)]))
    next_request = request(bundle, revision="r2")
    with pytest.raises(api().CollectionFailure):
        pipeline.collect(next_request, history(bundle, next_request, [OSError("down")]), retries=0)
    assert pipeline.read(result.collection_id).rows == 2


def test_independent_import_csv_error_is_not_replaced_with_raw(tmp_path, bundle):
    module = api()
    req = request(bundle, count=2, provider="file")
    registry = ProviderRegistry()
    registry.register(
        "file",
        module.FileImportProvider(
            ((bundle["raw"], bundle["candidate_csv"].replace(b",100,", b",98,")),)
        ),
    )
    pipeline = collector(tmp_path)
    with pytest.raises(module.CollectionFailure):
        pipeline.collect(req, registry)
    assert pipeline.task(req.request_id)["status"] == "QUARANTINED"
