"""Formal >=1000 valid-example properties over actual serialization and query boundaries."""

import hashlib
import json
import tempfile
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

from hypothesis import example, given, settings
from hypothesis import strategies as st

from candle_audit.data_layer import CandidateStore, Window, timestamp_ns

ROOT = Path(__file__).resolve().parents[2]
START_NS = 1767225600000000000
END_NS = 1767232800000000000
SOURCE = {
    "venue": "OKX",
    "instId": "ETH-USDT-SWAP",
    "instType": "SWAP",
    "base_asset": "ETH",
    "quote_asset": "USDT",
    "bar": "1H",
    "endpoint": "/api/v5/market/history-candles",
    "source_type": "trade",
    "acquisition": "SYNTHETIC",
}


def make_case(price, quantity):
    csv = (
        "timestamp,open,high,low,close,qty\n"
        f"2026-01-01T00:00:00Z,{price},{price},{price},{price},{quantity}\n"
        f"2026-01-01T01:00:00Z,{price},{price},{price},{price},{quantity}\n"
    ).encode()
    raw = json.dumps(
        {
            "code": "0",
            "msg": "",
            "data": [
                ["1767225600000", price, price, price, price, "20", quantity, "2010", "1"],
                ["1767229200000", price, price, price, price, "30", quantity, "3045", "1"],
            ],
        }
    ).encode()
    metadata = {
        "schema": "candle-audit-data-version-v2",
        "name": "property-test",
        "version": "v1",
        "source": "synthetic-engineering",
        "provenance": "synthetic-engineering/test-only",
        "market": "ETH-USDT-SWAP",
        "currency": "USDT",
        "timeframe": "1h",
        "timezone": "UTC",
        "kind": "synthetic",
        "content_sha256": hashlib.sha256(csv).hexdigest(),
        "volume": {
            "base_asset": "ETH",
            "quote_asset": "USDT",
            "columns": [{"column": "qty", "unit": "base"}],
        },
    }
    return csv, metadata, raw


price_text = st.builds(
    lambda leading, integer, fraction: f"{leading}{integer}.{fraction}",
    st.integers(min_value=1, max_value=9),
    st.text("0123456789", max_size=70),
    st.text("0123456789", min_size=1, max_size=20),
)


@settings(max_examples=1000, deadline=None, derandomize=True)
@given(
    price=price_text,
    quantity=st.decimals(
        min_value=0, max_value=10**12, places=18, allow_nan=False, allow_infinity=False
    ).map(str),
)
@example(price="1" * 70 + "." + "2" * 20, quantity="0.000000000000000001")
def test_raw_csv_parquet_duckdb_text_roundtrip_and_half_open_query(price, quantity):
    csv, metadata, raw = make_case(price, quantity)
    with tempfile.TemporaryDirectory(dir=ROOT / ".tmp-data-layer", prefix="property-") as temp:
        store = CandidateStore(Path(temp))
        snapshot = store.ingest(
            csv, metadata, raw, SOURCE, Window(START_NS, END_NS), raw_time_unit="ms"
        )
        full = store.query(
            snapshot.snapshot_id, Window(START_NS, END_NS), fields=["timestamp_ns", "open", "qty"]
        )
        expected = [
            {"timestamp_ns": 1767225600000000000, "open": price, "qty": quantity},
            {"timestamp_ns": 1767229200000000000, "open": price, "qty": quantity},
        ]
        assert full.table.to_pylist() == expected
        right = store.query(
            snapshot.snapshot_id,
            Window(START_NS + 1, END_NS),
            fields=["timestamp_ns", "open", "qty"],
        )
        assert right.table.to_pylist() == expected[1:]
        empty = store.query(
            snapshot.snapshot_id,
            Window(START_NS + 1, 1767229200000000000),
            fields=["timestamp_ns", "open", "qty"],
        )
        assert empty.table.num_rows == 0
        assert empty.table.schema.equals(full.table.schema, check_metadata=True)
        assert (snapshot.path / "candidate.csv").read_bytes() == csv
        assert (snapshot.path / "raw.json").read_bytes() == raw
        assert empty.as_dict()["admission"] == "NOT_EVALUATED"


def independent_iso(ns, offset_minutes):
    seconds, remainder = divmod(ns, 1_000_000_000)
    instant = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(seconds=seconds)
    local = instant.astimezone(timezone(timedelta(minutes=offset_minutes)))
    return (
        local.strftime("%Y-%m-%dT%H:%M:%S")
        + f".{remainder:09d}"
        + local.strftime("%z")[:3]
        + ":"
        + local.strftime("%z")[3:]
    )


@settings(max_examples=1000, deadline=None, derandomize=True)
@given(
    ns=st.integers(min_value=946684800000000000, max_value=4102444800000000000),
    offset=st.integers(min_value=-720, max_value=840),
)
@example(ns=1709251199999999999, offset=840)
@example(ns=1767225599999999999, offset=-720)
def test_calendar_timezone_representation_retains_each_nanosecond(ns, offset):
    start, end = independent_iso(ns, offset), independent_iso(ns + 1, offset)
    assert timestamp_ns(start) == ns
    window = Window.parse(start, end)
    assert window.start_ns == ns and window.end_ns == ns + 1
    a, b = window.iso_bounds()
    assert Window.parse(a, b) == window
