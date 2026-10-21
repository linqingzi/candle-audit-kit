"""High-risk local semantic, report, source-unit and query contracts; real oracles and real IO."""

import hashlib
import json
import os
import shutil
import tempfile
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from r2_helpers import original_reports, resign

from candle_audit.data_layer import CandidateStore, DataLayerError, OfflineOKX, Window
from candle_audit.data_layer.store import canonical

ROOT = Path(__file__).resolve().parents[2]


def case(quantity, minute, kind="SWAP", units=("base",)):
    origin = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(hours=minute)
    stamp = 1767225600000 + minute * 3600000
    instrument = "ETH-USDT" + ("-SWAP" if kind == "SWAP" else "")
    source = {
        "venue": "OKX",
        "instId": instrument,
        "instType": kind,
        "base_asset": "ETH",
        "quote_asset": "USDT",
        "bar": "1H",
        "endpoint": "/api/v5/market/history-candles",
        "source_type": "trade",
        "acquisition": "SYNTHETIC",
    }
    columns = []
    quantities = {
        "base": str(quantity),
        "contracts": str(quantity * 10),
        "quote": str(quantity * 1000),
    }
    for unit in units:
        descriptor = {"column": unit + "_qty", "unit": unit}
        if unit == "contracts":
            descriptor.update(contract_size=0.1, contract_size_unit="base")
        columns.append(descriptor)
    csv = ["timestamp,open,high,low,close," + ",".join(d["column"] for d in columns)]
    rows = []
    for index in range(2):
        csv.append(
            (origin + timedelta(hours=index)).isoformat()
            + ",100,102,99,101,"
            + ",".join(quantities[d["unit"]] for d in columns)
        )
        rows.append(
            [
                str(stamp + index * 3600000),
                "100.00",
                "102",
                "99",
                "101",
                quantities["base" if kind == "SPOT" else "contracts"],
                quantities["quote" if kind == "SPOT" else "base"],
                quantities["quote"],
                "1",
            ]
        )
    content = ("\n".join(csv) + "\n").encode()
    metadata = {
        "schema": "candle-audit-data-version-v2",
        "name": "property-r2",
        "version": "v1",
        "source": "synthetic-engineering",
        "provenance": "synthetic-engineering/test-only",
        "market": instrument,
        "currency": "USDT",
        "timeframe": "1h",
        "timezone": "UTC",
        "kind": "synthetic",
        "content_sha256": hashlib.sha256(content).hexdigest(),
        "volume": {"base_asset": "ETH", "quote_asset": "USDT", "columns": columns},
    }
    raw = canonical({"code": "0", "msg": "", "data": rows[::-1]})
    window = Window.parse(origin.isoformat(), (origin + timedelta(hours=2)).isoformat())
    return content, metadata, raw, source, window, quantities


@settings(max_examples=1000, deadline=None, derandomize=True)
@given(
    quantity=st.integers(1, 10**8),
    hour=st.integers(0, 100000),
    failure=st.sampled_from(
        [
            "unclosed",
            "missing_confirm",
            "wrong_volume_asset",
            "wrong_market",
            "bad_csv",
            "false_report",
            "duplicate_raw",
            "unaligned_raw",
        ]
    ),
)
def test_resigned_invalid_local_semantics_never_become_reader_success(quantity, hour, failure):
    csv, metadata, raw, source, window, _ = case(quantity, hour)
    with tempfile.TemporaryDirectory(dir=ROOT / ".tmp-data-layer", prefix="property-r2-") as temp:
        store = CandidateStore(Path(temp))
        snapshot = store.ingest(csv, metadata, raw, source, window, raw_time_unit="ms")
        native = json.loads(raw)
        changes = {}
        if failure in ("unclosed", "missing_confirm", "duplicate_raw", "unaligned_raw"):
            if failure == "unclosed":
                native["data"][0][8] = "0"
            if failure == "missing_confirm":
                native["data"][0].pop()
            if failure == "duplicate_raw":
                native["data"].append(native["data"][0].copy())
            if failure == "unaligned_raw":
                native["data"][0][0] = str(int(native["data"][0][0]) + 1)
            changes["raw.json"] = canonical(native)
        if failure == "wrong_volume_asset":
            metadata["volume"]["base_asset"] = "BTC"
            changes["metadata.json"] = canonical(metadata)
        if failure == "wrong_market":
            metadata["market"] = "BTC-USDT-SWAP"
            changes["metadata.json"] = canonical(metadata)
        if failure == "bad_csv":
            bad = csv.replace(b",100,102,99,101,", b",0,102,99,101,", 1)
            metadata["content_sha256"] = hashlib.sha256(bad).hexdigest()
            changes.update({"candidate.csv": bad, "metadata.json": canonical(metadata)})
        if failure == "false_report":
            report = json.loads((snapshot.path / "reports.json").read_bytes())
            report["reconcile"]["closedbar"]["status"] = "UNCLOSED"
            changes["reports.json"] = canonical(report)
        bad_id, _ = resign(snapshot, changes, rebuild=True)
        with pytest.raises(DataLayerError):
            store.query(bad_id, window)
        assert store.query(snapshot.snapshot_id, window).table.num_rows == 2


@settings(max_examples=1000, deadline=None, derandomize=True)
@given(
    quantity=st.integers(1, 10**8),
    hour=st.integers(0, 100000),
    selection=st.sampled_from(
        [
            ("SPOT", ("base",)),
            ("SPOT", ("quote", "base")),
            ("SWAP", ("base",)),
            ("SWAP", ("contracts",)),
            ("SWAP", ("contracts", "quote", "base")),
        ]
    ),
)
def test_legitimate_units_raw_order_and_typed_half_open_queries_share_exact_values(
    quantity, hour, selection
):
    csv, metadata, raw, source, window, values = case(quantity, hour, *selection)
    with tempfile.TemporaryDirectory(dir=ROOT / ".tmp-data-layer", prefix="property-r2-") as temp:
        store = CandidateStore(Path(temp))
        snapshot = store.ingest(csv, metadata, raw, source, window, raw_time_unit="ms")
        result = store.query(snapshot.snapshot_id, window)
        rows = result.table.to_pylist()
        assert [row["timestamp_ns"] for row in rows] == [
            window.start_ns,
            window.start_ns + 3600000000000,
        ]
        assert [row["open"] for row in rows] == ["100.00", "100.00"]
        for unit in selection[1]:
            assert [row[unit + "_qty"] for row in rows] == [values[unit], values[unit]]
        empty = store.query(
            snapshot.snapshot_id, Window(window.start_ns + 1, window.start_ns + 3600000000000)
        )
        assert empty.table.num_rows == 0
        assert empty.table.schema.equals(result.table.schema, check_metadata=True)
        assert empty.as_dict()["source_verification"] == "UNVERIFIED"
        assert empty.as_dict()["admission"] == "NOT_EVALUATED"
        with pytest.raises(DataLayerError, match="row budget"):
            store.query(snapshot.snapshot_id, window, limit=1)


@settings(max_examples=1000, deadline=None, derandomize=True)
@given(
    quantity=st.integers(1, 10**8),
    hour=st.integers(0, 100000),
    leaf=st.sampled_from(["manifest.json", "raw.json", "candidate.csv", "reports.json"]),
)
def test_directory_inode_binding_survives_benign_swaps_without_reading_a_peer(quantity, hour, leaf):
    from candle_audit.data_layer import store as implementation

    csv, metadata, raw, source, window, _ = case(quantity, hour)
    with tempfile.TemporaryDirectory(dir=ROOT / ".tmp-data-layer", prefix="property-race-") as temp:
        home = Path(temp)
        store = CandidateStore(home / "store")
        snapshot = store.ingest(csv, metadata, raw, source, window, raw_time_unit="ms")
        peer, saved = home / "peer", home / "saved"
        shutil.copytree(snapshot.path, peer)
        peer_inodes = {(p.stat().st_dev, p.stat().st_ino) for p in peer.iterdir()}
        peer_reads = []
        real_open, real_read = os.open, implementation.read_bounded
        switched = False

        def opening(path, flags, *args, **kwargs):
            fd = real_open(path, flags, *args, **kwargs)
            info = os.fstat(fd)
            if (info.st_dev, info.st_ino) in peer_inodes:
                peer_reads.append(str(path))
            return fd

        def reading(path, limit, **kwargs):
            nonlocal switched
            if path.parent == snapshot.path and path.name == leaf and not switched:
                snapshot.path.rename(saved)
                snapshot.path.symlink_to(peer, target_is_directory=True)
                switched = True
            return real_read(path, limit, **kwargs)

        with (
            patch.object(implementation.os, "open", opening),
            patch.object(implementation, "read_bounded", reading),
        ):
            result = store.query(snapshot.snapshot_id, window)
            assert result.table.num_rows == 2
        assert switched and peer_reads == []


@settings(max_examples=1000, deadline=None, derandomize=True)
@given(quantity=st.integers(1, 10**8), hour=st.integers(0, 100000), excess=st.integers(97, 8192))
def test_dictionary_values_cannot_bypass_the_supported_decimal_cell_budget(quantity, hour, excess):
    csv, metadata, raw, source, window, _ = case(quantity, hour)
    with tempfile.TemporaryDirectory(
        dir=ROOT / ".tmp-data-layer", prefix="property-budget-"
    ) as temp:
        store = CandidateStore(Path(temp))
        snapshot = store.ingest(csv, metadata, raw, source, window, raw_time_unit="ms")
        table = pq.read_table(snapshot.path / "bars.parquet")
        corrupted = table.set_column(1, table.schema.field("open"), pa.array(["x" * excess] * 2))
        identity, _ = resign(snapshot, table=corrupted)
        with pytest.raises(DataLayerError, match="budget"):
            store.query(identity, window)
        assert store.query(snapshot.snapshot_id, window).table["open"].to_pylist() == [
            "100.00",
            "100.00",
        ]


@settings(max_examples=1000, deadline=None, derandomize=True)
@given(quantity=st.integers(1, 10**8), hour=st.integers(0, 100000), zeros=st.integers(1, 50))
def test_equivalent_csv_lexemes_and_descriptive_reports_do_not_change_raw_truth(
    quantity, hour, zeros
):
    csv, metadata, raw, source, window, _ = case(quantity, hour)
    with tempfile.TemporaryDirectory(
        dir=ROOT / ".tmp-data-layer", prefix="property-lexeme-"
    ) as temp:
        store = CandidateStore(Path(temp))
        snapshot = store.ingest(csv, metadata, raw, source, window, raw_time_unit="ms")
        equivalent = csv.replace(b",100,102,99,101,", b",100." + b"0" * zeros + b",102,99,101,")
        metadata["content_sha256"] = hashlib.sha256(equivalent).hexdigest()
        first, directory = resign(
            snapshot,
            {"candidate.csv": equivalent, "metadata.json": canonical(metadata)},
            rebuild=True,
        )
        reports = original_reports(directory)
        reports["notes"] = "synthetic explanation " + str(hour)
        reports["quality"]["detail"] = "a relocated temporary path is not price truth"
        identity, _ = resign(
            replace(snapshot, path=directory), {"reports.json": canonical(reports)}
        )
        result = store.query(identity, window)
        assert identity != first
        assert result.table["open"].to_pylist() == ["100.00", "100.00"]
        assert result.table["base_qty"].to_pylist() == [str(quantity), str(quantity)]
        assert result.reports["quality"]["structural_status"] == "PASS"
        assert result.reports["reconcile"]["closedbar"]["status"] == "CLOSED"
        assert result.as_dict()["source_verification"] == "UNVERIFIED"
        assert result.as_dict()["admission"] == "NOT_EVALUATED"


@settings(max_examples=1000, deadline=None, derandomize=True)
@given(quantity=st.integers(1, 10**8), hour=st.integers(0, 100000), missing=st.booleans())
def test_failed_publication_cleans_its_staging_and_preserves_previous_snapshot_bytes(
    quantity, hour, missing
):
    csv, metadata, raw, source, window, _ = case(quantity, hour)
    with tempfile.TemporaryDirectory(
        dir=ROOT / ".tmp-data-layer", prefix="property-atomic-"
    ) as temp:
        store = CandidateStore(Path(temp))
        snapshot = store.ingest(csv, metadata, raw, source, window, raw_time_unit="ms")
        before = {p.name: p.read_bytes() for p in snapshot.path.iterdir()}
        bad = json.loads(raw)
        if missing:
            bad["data"][0].pop()
        else:
            bad["data"][0][8] = "0"
        with pytest.raises(DataLayerError):
            store.ingest(csv, metadata, canonical(bad), source, window, raw_time_unit="ms")
        assert {p.name: p.read_bytes() for p in snapshot.path.iterdir()} == before
        assert [p.name for p in store.root.iterdir()] == [snapshot.snapshot_id]
        assert store.query(snapshot.snapshot_id, window).table.num_rows == 2


@settings(max_examples=1000, deadline=None, derandomize=True)
@given(quantity=st.integers(1, 10**8), hour=st.integers(0, 100000), base_length=st.integers(1, 230))
def test_public_source_bounds_are_shared_by_producer_and_reader(quantity, hour, base_length):
    csv, metadata, raw, source, window, _ = case(quantity, hour)
    base = "A" * base_length
    source.update(base_asset=base, instId=base + "-USDT-SWAP")
    metadata["market"] = source["instId"]
    # The unchanged v2 volume contract permits asset names of at most 32 characters.
    # Its v1 counterpart has no quantity columns and admits the full source string boundary.
    metadata["schema"] = "candle-audit-data-version-v1"
    del metadata["volume"]
    csv = ("\n".join(line.rsplit(",", 1)[0] for line in csv.decode().splitlines()) + "\n").encode()
    metadata["content_sha256"] = hashlib.sha256(csv).hexdigest()
    with tempfile.TemporaryDirectory(
        dir=ROOT / ".tmp-data-layer", prefix="property-source-"
    ) as temp:
        store = CandidateStore(Path(temp))
        snapshot = store.ingest(csv, metadata, raw, source, window, raw_time_unit="ms")
        result = store.query(snapshot.snapshot_id, window)
        assert result.source == source and result.table.num_rows == 2
        invalid = {**source, "base_asset": "A" * 241, "instId": "A" * 241 + "-USDT-SWAP"}
        with pytest.raises(DataLayerError, match="bounded source"):
            store.ingest(csv, metadata, raw, invalid, window, raw_time_unit="ms")
        assert [p.name for p in store.root.iterdir()] == [snapshot.snapshot_id]


@settings(max_examples=1000, deadline=None, derandomize=True)
@given(
    quantity=st.integers(1, 10**8),
    hour=st.integers(0, 100000),
    kind=st.sampled_from(["SPOT", "SWAP"]),
    error_code=st.integers(1, 10**8),
)
def test_native_sdk_offline_capture_preserves_bytes_units_and_rejects_error_envelopes(
    quantity, hour, kind, error_code
):
    _, _, raw, source, window, _ = case(quantity, hour, kind)
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
            "instId": [source["instId"]],
            "bar": ["1H"],
            "before": [str(window.start_ns // 1000000 - 1)],
            "after": [str(window.end_ns // 1000000)],
            "limit": ["2"],
        }
        calls.append(True)
        return raw

    capture = OfflineOKX(transport).capture(source, "1h", window, raw_time_unit="ms")
    assert calls == [True] and capture.raw == raw
    assert capture.quantity_units == {
        "vol": "base" if kind == "SPOT" else "contracts",
        "volCcy": "quote" if kind == "SPOT" else "base",
        "volCcyQuote": "quote",
    }
    assert capture.view_status == "NON_AUTHORITATIVE_FLOAT"
    assert capture.float_view == (
        (window.start_ns // 1000000 + 3600000, 100.0, 102.0, 99.0, 101.0, float(quantity)),
        (window.start_ns // 1000000, 100.0, 102.0, 99.0, 101.0, float(quantity)),
    )
    with pytest.raises(DataLayerError, match="network is disabled"):
        OfflineOKX().capture(source, "1h", window, raw_time_unit="ms")
    error = canonical({"code": str(error_code), "msg": "synthetic error", "data": []})
    with pytest.raises(DataLayerError, match="native response code"):
        OfflineOKX(lambda url, method: error).capture(source, "1h", window, raw_time_unit="ms")
