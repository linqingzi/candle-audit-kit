"""Explicit synthetic-engineering/test-only inputs, with socket access blocked."""

import hashlib
import socket
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("engineering tests must not open network connections")

    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket.socket, "connect_ex", forbidden)


@pytest.fixture
def bundle():
    fixtures = Path(__file__).resolve().parents[2] / (
        "tests/fixtures/legacy"
    )
    csv = (fixtures / "bars.csv").read_bytes()
    return {
        "candidate_csv": csv,
        "raw": (fixtures / "raw.json").read_bytes(),
        "metadata": {
            "schema": "candle-audit-data-version-v2",
            "name": "m1-test",
            "version": "v1",
            "source": "synthetic-engineering",
            "provenance": "synthetic-engineering/test-only",
            "market": "BTC-USDT-SWAP",
            "currency": "USDT",
            "timeframe": "1h",
            "timezone": "UTC",
            "kind": "synthetic",
            "content_sha256": hashlib.sha256(csv).hexdigest(),
            "volume": {
                "base_asset": "BTC",
                "quote_asset": "USDT",
                "columns": [{"column": "qty", "unit": "base"}],
            },
        },
        "source": {
            "venue": "OKX",
            "instId": "BTC-USDT-SWAP",
            "instType": "SWAP",
            "base_asset": "BTC",
            "quote_asset": "USDT",
            "bar": "1H",
            "endpoint": "/api/v5/market/history-candles",
            "source_type": "trade",
            "acquisition": "SYNTHETIC",
        },
        "start": "2026-01-01T00:00:00Z",
        "end": "2026-01-01T02:00:00Z",
    }
