"""C01 consumers: synthetic-engineering/test-only, real native and storage boundaries."""

import copy
import hashlib
import importlib
import importlib.util
import json
import os
import tempfile
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from test_r2_properties import case

from candle_audit.data_layer import CandidateStore, DataLayerError, Window

ROOT = Path(__file__).resolve().parents[2]


def api():
    name = "candle_audit.data_layer.providers"
    assert importlib.util.find_spec(name) is not None, "C01 provider contract is missing"
    return importlib.import_module(name)


def request(bundle, provider_id="local"):
    return api().ProviderRequest(
        provider_id,
        bundle["metadata"],
        bundle["source"],
        Window.parse(bundle["start"], bundle["end"]),
        raw_time_unit="ms",
    )


def registry(bundle):
    module = api()
    result = module.ProviderRegistry()
    result.register("local", module.LocalCandidateProvider(bundle["candidate_csv"], bundle["raw"]))
    result.register(
        "okx-offline",
        module.OKXCaptureProvider(bundle["candidate_csv"], lambda url, method: bundle["raw"]),
    )
    return result


def product(bundle, base, kind, timeframe, seconds):
    result = copy.deepcopy(bundle)
    instrument = base + "-USDT" + ("-SWAP" if kind == "SWAP" else "")
    result["source"].update(
        base_asset=base, instType=kind, instId=instrument, bar={"1m": "1m", "1h": "1H"}[timeframe]
    )
    columns = [{"column": "base_qty", "unit": "base"}, {"column": "quote_qty", "unit": "quote"}]
    if kind == "SWAP":
        columns.append(
            {
                "column": "contracts_qty",
                "unit": "contracts",
                "contract_size": 0.1,
                "contract_size_unit": "base",
            }
        )
    start = datetime(2026, 1, 1, tzinfo=UTC)
    price = "100.000000000000000001"
    lines = ["timestamp,open,high,low,close," + ",".join(c["column"] for c in columns)]
    rows = []
    for index in range(2):
        lines.append(
            (start + timedelta(seconds=index * seconds)).isoformat()
            + f",{price},102,99,101,1.00,100.00"
            + (",10.00" if kind == "SWAP" else "")
        )
        rows.append(
            [
                str(1767225600000 + index * seconds * 1000),
                price,
                "102",
                "99",
                "101",
                "10.00" if kind == "SWAP" else "1.00",
                "1.00" if kind == "SWAP" else "100.00",
                "100.00",
                "1",
            ]
        )
    result["candidate_csv"] = ("\n".join(lines) + "\n").encode()
    result["raw"] = json.dumps({"code": "0", "msg": "", "data": rows[::-1]}).encode()
    result["metadata"].update(
        market=instrument,
        timeframe=timeframe,
        volume={"base_asset": base, "quote_asset": "USDT", "columns": columns},
        content_sha256=hashlib.sha256(result["candidate_csv"]).hexdigest(),
    )
    result["end"] = (start + timedelta(seconds=seconds * 2)).isoformat()
    return result


@pytest.mark.parametrize("provider_id", ["local", "okx-offline"])
@pytest.mark.parametrize("base,kind", [("BTC", "SPOT"), ("ETH", "SWAP")])
@pytest.mark.parametrize("timeframe,seconds", [("1m", 60), ("1h", 3600)])
def test_both_providers_feed_exact_native_parquet_duckdb_and_typed_empty(
    tmp_path, bundle, monkeypatch, provider_id, base, kind, timeframe, seconds
):
    module = api()
    from candle_audit import candle_quality, candle_reconcile
    from candle_audit.data_layer import store as implementation

    observed = []
    for target, name in [
        (candle_quality, "validate"),
        (candle_reconcile, "reconcile"),
        (implementation.pq, "write_table"),
        (implementation.duckdb, "connect"),
    ]:
        real = getattr(target, name)

        def observe(*args, _real=real, _name=name, **kwargs):
            observed.append(_name)
            return _real(*args, **kwargs)

        monkeypatch.setattr(target, name, observe)
    bundle = product(bundle, base, kind, timeframe, seconds)
    calls = []

    def transport(url, method):
        address = urlsplit(url)
        assert (address.scheme, address.netloc, address.path, method) == (
            "https",
            "www.okx.com",
            "/api/v5/market/history-candles",
            "GET",
        )
        assert parse_qs(address.query) == {
            "instId": [bundle["source"]["instId"]],
            "bar": [bundle["source"]["bar"]],
            "before": ["1767225599999"],
            "after": [str(1767225600000 + seconds * 2000)],
            "limit": ["2"],
        }
        calls.append(url)
        return bundle["raw"]

    providers = registry(bundle)
    if provider_id == "okx-offline":
        providers = module.ProviderRegistry()
        providers.register(
            provider_id, module.OKXCaptureProvider(bundle["candidate_csv"], transport)
        )
    store = CandidateStore(tmp_path / "store")
    expected_request = request(bundle, provider_id)
    result, snapshot = providers.ingest(expected_request, store)
    full = store.query(snapshot.snapshot_id, expected_request.window)
    rows = full.table.to_pylist()
    assert [r["timestamp_ns"] for r in rows] == [
        1767225600000000000,
        1767225600000000000 + seconds * 10**9,
    ]
    assert [r["open"] for r in rows] == ["100.000000000000000001"] * 2
    assert [r["base_qty"] for r in rows] == ["1.00"] * 2
    assert [r["quote_qty"] for r in rows] == ["100.00"] * 2
    if kind == "SWAP":
        assert [r["contracts_qty"] for r in rows] == ["10.00"] * 2
    empty = store.query(
        snapshot.snapshot_id,
        Window(
            expected_request.window.start_ns + 1, expected_request.window.start_ns + seconds * 10**9
        ),
    )
    assert empty.table.num_rows == 0
    assert empty.table.schema.equals(full.table.schema, check_metadata=True)
    assert empty.as_dict()["source_verification"] == "UNVERIFIED"
    assert empty.as_dict()["admission"] == "NOT_EVALUATED"
    assert set(observed) == {"validate", "reconcile", "write_table", "connect"}
    assert len(calls) == (1 if provider_id == "okx-offline" else 0)
    assert result.request == expected_request
    assert (
        result.candidate_csv
        == (snapshot.path / "candidate.csv").read_bytes()
        == bundle["candidate_csv"]
    )
    assert result.raw == (snapshot.path / "raw.json").read_bytes() == bundle["raw"]
    assert full.metadata == expected_request.metadata and full.source == expected_request.source
    receipt = result.receipt
    assert receipt["provider_id"] == provider_id
    assert receipt["request_id"] == expected_request.request_id
    assert receipt["request"] == expected_request.as_dict()
    assert receipt["candidate_sha256"] == hashlib.sha256(bundle["candidate_csv"]).hexdigest()
    assert receipt["raw_sha256"] == hashlib.sha256(bundle["raw"]).hexdigest()
    assert (
        receipt["source_verification"] == "UNVERIFIED" and receipt["admission"] == "NOT_EVALUATED"
    )
    assert snapshot.reports["quality"]["structural_status"] == "PASS"
    assert snapshot.reports["reconcile"]["comparison"] == "MATCH"
    again, replay = providers.ingest(expected_request, store)
    assert again.receipt == receipt and replay.snapshot_id == snapshot.snapshot_id


def test_request_and_result_survive_nested_caller_mutation(bundle, tmp_path):
    providers = registry(bundle)
    declared = request(bundle)
    original = declared.as_dict()
    identity = declared.request_id
    bundle["metadata"]["volume"]["columns"][0]["unit"] = "quote"
    bundle["source"]["instId"] = "ETH-USDT-SWAP"
    declared.metadata["volume"]["columns"].clear()
    declared.source.clear()
    declared.as_dict()["metadata"].clear()
    assert declared.request_id == identity and declared.as_dict() == original
    result, snapshot = providers.ingest(declared, CandidateStore(tmp_path / "store"))
    result.receipt["request"]["metadata"].clear()
    assert result.receipt["request"] == original
    with pytest.raises(FrozenInstanceError):
        declared.provider_id = "okx-offline"
    with pytest.raises(FrozenInstanceError):
        result.raw = b"changed"
    assert snapshot.reports["reconcile"]["comparison"] == "MATCH"


def test_unknown_duplicate_and_non_provider_registration_are_explicit_errors(bundle, tmp_path):
    module = api()
    providers = registry(bundle)
    store = CandidateStore(tmp_path / "store")
    with pytest.raises(DataLayerError, match="unknown provider"):
        providers.ingest(request(bundle, "missing"), store)
    with pytest.raises(DataLayerError, match="already registered"):
        providers.register("local", module.LocalCandidateProvider(b"bad", b"bad"))
    for invalid in ["import.module:load", object(), None]:
        with pytest.raises(DataLayerError):
            providers.register("third", invalid)
    assert not list(store.root.iterdir())
    _, snapshot = providers.ingest(request(bundle), store)
    assert store.query(snapshot.snapshot_id, request(bundle).window).table.num_rows == 2


@pytest.mark.parametrize("bad_id", ["", "a" * 65, "../local", "module:provider", "LOCAL", None, []])
def test_provider_ids_do_not_become_paths_or_import_expressions(bundle, bad_id):
    with pytest.raises(DataLayerError):
        request(bundle, bad_id)
    with pytest.raises(DataLayerError):
        api().ProviderRegistry().register(bad_id, api().LocalCandidateProvider(b"", b""))


@pytest.mark.parametrize(
    "change",
    [
        "venue",
        "bar",
        "market",
        "currency",
        "assets",
        "units",
        "raw-unit",
        "window",
        "timeframe",
        "acquisition",
        "metadata",
    ],
)
@pytest.mark.parametrize("provider_id", ["local", "okx-offline"])
def test_bad_declarations_reject_without_publishing_and_keep_legal_control(
    bundle, tmp_path, change, provider_id
):
    module = api()
    good = copy.deepcopy(bundle)
    unit, window = "ms", Window.parse(bundle["start"], bundle["end"])
    if change == "venue":
        bundle["source"]["venue"] = "OTHER"
    elif change == "bar":
        bundle["source"]["bar"] = "5m"
    elif change == "market":
        bundle["metadata"]["market"] = "ETH-USDT-SWAP"
    elif change == "currency":
        bundle["metadata"]["currency"] = "BTC"
    elif change == "assets":
        bundle["metadata"]["volume"]["base_asset"] = "ETH"
    elif change == "units":
        bundle["metadata"]["volume"]["columns"][0]["unit"] = "quote"
    elif change == "raw-unit":
        unit = "ns"
    elif change == "window":
        window = Window(window.start_ns + 1, window.end_ns)
    elif change == "timeframe":
        bundle["metadata"]["timeframe"] = []
    elif change == "acquisition":
        bundle["source"]["acquisition"] = "VERIFIED"
    else:
        bundle["metadata"]["extra"] = "not permitted"
    store = CandidateStore(tmp_path / "store")
    with pytest.raises(DataLayerError):
        declared = module.ProviderRequest(
            provider_id, bundle["metadata"], bundle["source"], window, raw_time_unit=unit
        )
        registry(bundle).ingest(declared, store)
    assert not list(store.root.iterdir())
    _, snapshot = registry(good).ingest(request(good, provider_id), store)
    assert store.query(snapshot.snapshot_id, request(good).window).table.num_rows == 2


@pytest.mark.parametrize("provider_id", ["local", "okx-offline"])
@pytest.mark.parametrize(
    "change", ["unclosed", "empty", "price", "wrong-window", "bad-json", "hash"]
)
def test_invalid_candidate_bytes_never_publish(bundle, tmp_path, change, provider_id):
    module = api()
    native = json.loads(bundle["raw"])
    if change == "unclosed":
        native["data"][0][8] = "0"
    elif change == "empty":
        native["data"] = []
    elif change == "price":
        native["data"][0][1] = "100.000000000000000001"
    elif change == "wrong-window":
        native["data"][0][0] = "1767232800000"
    bundle["raw"] = json.dumps(native).encode() if change != "bad-json" else b"{"
    if change == "hash":
        bundle["candidate_csv"] += b"\n"
    store = CandidateStore(tmp_path / "store")
    with pytest.raises(DataLayerError):
        registry(bundle).ingest(request(bundle, provider_id), store)
    assert not list(store.root.iterdir())
    if provider_id == "okx-offline":
        with pytest.raises(DataLayerError, match="network is disabled"):
            module.OKXCaptureProvider(bundle["candidate_csv"]).acquire(request(bundle, provider_id))


@pytest.mark.parametrize("field", ["candidate_csv", "raw"])
@pytest.mark.parametrize(
    "bad",
    [bytearray(b"mutable"), "text", b"x" * (8 * 1024 * 1024 + 1)],
    ids=["mutable", "text", "over-budget"],
)
def test_local_provider_captures_only_bounded_immutable_bytes(bundle, field, bad):
    bundle[field] = bad
    with pytest.raises(DataLayerError):
        api().LocalCandidateProvider(bundle["candidate_csv"], bundle["raw"])


def test_controlled_file_import_captures_once_and_rejects_unsafe_paths(bundle, tmp_path):
    module = api()
    csv_path, raw_path = tmp_path / "bars.csv", tmp_path / "raw.json"
    csv_path.write_bytes(bundle["candidate_csv"])
    raw_path.write_bytes(bundle["raw"])
    provider = module.LocalCandidateProvider.from_files(csv_path, raw_path)
    csv_path.write_bytes(b"changed after capture")
    result = provider.acquire(request(bundle))
    assert result.candidate_csv == bundle["candidate_csv"] and result.raw == bundle["raw"]
    fifo = tmp_path / "fifo"
    os.mkfifo(fifo)
    link = tmp_path / "linked"
    link.symlink_to(raw_path)
    for bad_path in [ROOT / "README.md", tmp_path / ".." / "raw.json", fifo, tmp_path]:
        with pytest.raises(DataLayerError):
            module.LocalCandidateProvider.from_files(csv_path, bad_path)
    # A controlled symlink to a regular in-domain file follows the existing controlled_path rule.
    linked = module.LocalCandidateProvider.from_files(raw_path, link)
    assert linked.raw == bundle["raw"]


def test_registry_rejects_provider_results_bound_to_another_request(bundle, tmp_path):
    module = api()
    expected = request(bundle)
    other = request(bundle, "other")

    class WrongRequest:
        def acquire(self, declared):
            return module.ProviderResult(other, bundle["candidate_csv"], bundle["raw"])

    providers = module.ProviderRegistry()
    providers.register("local", WrongRequest())
    store = CandidateStore(tmp_path / "store")
    with pytest.raises(DataLayerError, match="request"):
        providers.ingest(expected, store)
    assert not list(store.root.iterdir())


def test_same_shape_different_source_or_provider_has_distinct_request_receipt(bundle, tmp_path):
    module = api()
    providers = registry(bundle)
    first = request(bundle)
    switched = request(bundle, "okx-offline")
    bundle["source"].update(base_asset="ETH", instId="ETH-USDT-SWAP")
    bundle["metadata"]["market"] = "ETH-USDT-SWAP"
    bundle["metadata"]["volume"]["base_asset"] = "ETH"
    second = request(bundle)
    assert len({first.request_id, switched.request_id, second.request_id}) == 3
    store = CandidateStore(tmp_path / "store")
    one, snap1 = providers.ingest(first, store)
    two, snap2 = providers.ingest(second, store)
    assert one.receipt != two.receipt and snap1.snapshot_id != snap2.snapshot_id
    assert store.query(snap1.snapshot_id, first.window).source["base_asset"] == "BTC"
    assert store.query(snap2.snapshot_id, second.window).source["base_asset"] == "ETH"
    assert module.ProviderResult(first, one.candidate_csv, one.raw).receipt == one.receipt


def test_new_public_named_imports_preserve_legacy_wildcard_surface():
    module = api()
    package = importlib.import_module("candle_audit.data_layer")
    for name in [
        "ProviderRequest",
        "ProviderResult",
        "CandidateProvider",
        "ProviderRegistry",
        "LocalCandidateProvider",
        "OKXCaptureProvider",
    ]:
        assert getattr(package, name) is getattr(module, name)
        assert name in dir(package)


@settings(max_examples=1000, deadline=None, derandomize=True)
@given(
    quantity=st.integers(1, 10**8),
    hour=st.integers(0, 100000),
    provider_id=st.sampled_from(["local", "okx-offline"]),
)
def test_request_identity_and_exact_consumer_survive_nested_mutations(quantity, hour, provider_id):
    module = api()
    csv, metadata, raw, source, window, _ = case(quantity, hour)
    declared = module.ProviderRequest(provider_id, metadata, source, window, raw_time_unit="ms")
    original_id = declared.request_id
    metadata["volume"]["columns"][0]["unit"] = "quote"
    source["instId"] = "BTC-USDT-SWAP"
    assert declared.request_id == original_id
    providers = module.ProviderRegistry()
    provider = (
        module.LocalCandidateProvider(csv, raw)
        if provider_id == "local"
        else module.OKXCaptureProvider(csv, lambda url, method: raw)
    )
    providers.register(provider_id, provider)
    with tempfile.TemporaryDirectory(
        dir=ROOT / ".tmp-data-layer", prefix="provider-property-"
    ) as temp:
        store = CandidateStore(Path(temp))
        result, snapshot = providers.ingest(declared, store)
        full = store.query(snapshot.snapshot_id, window)
        assert full.table["base_qty"].to_pylist() == [str(quantity)] * 2
        assert full.table["open"].to_pylist() == ["100.00"] * 2
        assert full.source["instId"] == "ETH-USDT-SWAP"
        assert result.receipt["request_id"] == original_id
        assert result.candidate_csv == csv and result.raw == raw
        bad = module.ProviderRequest(
            provider_id, metadata, declared.source, window, raw_time_unit="ms"
        )
        assert bad.request_id != original_id
        with pytest.raises(DataLayerError):
            providers.ingest(bad, store)
        assert [p.name for p in store.root.iterdir()] == [snapshot.snapshot_id]
