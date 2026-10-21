"""Exercise the real CCXT instance and native method below an offline transport."""

import importlib.metadata
from urllib.parse import parse_qs, urlsplit

import ccxt
import pytest


def test_default_ccxt_transport_refuses_network(bundle):
    from candle_audit.data_layer.adapter import OfflineOKX
    from candle_audit.data_layer.model import DataLayerError, Window

    adapter = OfflineOKX()
    with pytest.raises(DataLayerError, match="offline transport"):
        adapter.capture(
            bundle["source"], "1h", Window.parse(bundle["start"], bundle["end"]), raw_time_unit="ms"
        )


def test_real_ccxt_native_method_retains_bytes_and_labels_float_view(bundle):
    from candle_audit.data_layer.adapter import OfflineOKX
    from candle_audit.data_layer.model import Window

    def transport(url, method):
        address = urlsplit(url)
        assert address.scheme == "https" and address.netloc == "www.okx.com"
        assert address.path == "/api/v5/market/history-candles" and method == "GET"
        assert parse_qs(address.query) == {
            "instId": ["BTC-USDT-SWAP"],
            "bar": ["1H"],
            "before": ["1767225599999"],
            "after": ["1767232800000"],
            "limit": ["2"],
        }
        return bundle["raw"]

    adapter = OfflineOKX(transport)
    capture = adapter.capture(
        bundle["source"], "1h", Window.parse(bundle["start"], bundle["end"]), raw_time_unit="ms"
    )
    assert type(adapter.exchange) is ccxt.okx
    assert importlib.metadata.version("ccxt") == capture.ccxt_version == "4.5.64"
    assert capture.method == "publicGetMarketHistoryCandles"
    assert capture.raw == bundle["raw"]
    assert capture.view_status == "NON_AUTHORITATIVE_FLOAT"
    assert capture.float_view == (
        (1767225600000, 100.0, 102.0, 99.0, 101.0, 2.0),
        (1767229200000, 101.0, 103.0, 100.0, 102.0, 3.0),
    )
    assert capture.quantity_units == {"vol": "contracts", "volCcy": "base", "volCcyQuote": "quote"}


def test_adapter_rejects_a_nontext_timeframe_before_its_native_call(bundle):
    from candle_audit.data_layer import DataLayerError, OfflineOKX, Window

    with pytest.raises(DataLayerError):
        OfflineOKX().capture(bundle["source"], [], Window(1, 2), raw_time_unit="ms")
