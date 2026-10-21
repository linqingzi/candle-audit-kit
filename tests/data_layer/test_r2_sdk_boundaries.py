"""Real SDK transport boundaries retain denied addresses, methods, bodies and bounded JSON."""

import pytest

from candle_audit.data_layer import DataLayerError, OfflineOKX, Window


@pytest.mark.parametrize(
    "url,method,body",
    [
        ("http://www.okx.com/api/v5/market/history-candles", "GET", None),
        ("https://synthetic.invalid/api/v5/market/history-candles", "GET", None),
        ("https://www.okx.com/api/v5/public/time", "GET", None),
        ("https://www.okx.com/api/v5/market/history-candles", "POST", None),
        ("https://www.okx.com/api/v5/market/history-candles", "GET", "synthetic-body"),
    ],
)
def test_sdk_bound_fetch_refuses_unsupported_request_before_transport(bundle, url, method, body):
    calls = []

    def transport(url, method):
        calls.append((url, method))
        return bundle["raw"]

    adapter = OfflineOKX(transport)
    assert adapter.exchange.enableRateLimit is False
    with pytest.raises(DataLayerError, match="only offline public history"):
        adapter.exchange.fetch(url, method, body=body)
    assert calls == []
    capture = adapter.capture(
        bundle["source"], "1h", Window.parse(bundle["start"], bundle["end"]), raw_time_unit="ms"
    )
    assert len(calls) == 1 and capture.raw == bundle["raw"]


def test_other_real_sdk_public_method_is_refused_without_an_acquisition_call(bundle):
    calls = []

    def transport(url, method):
        calls.append((url, method))
        return bundle["raw"]

    adapter = OfflineOKX(transport)
    with pytest.raises(DataLayerError, match="only offline public history"):
        adapter.exchange.publicGetPublicTime()
    assert calls == []


@pytest.mark.parametrize(
    "raw",
    [
        b"[]",
        b'{"code":"0","data":{}}',
        b'{"code":"0","data":[]',
        b'{"code":"0","data":[0]}',
        b"x" * (8 * 1024 * 1024 + 1),
        bytearray(b"{}"),
    ],
)
def test_sdk_capture_rejects_non_native_shapes_and_transport_budgets(bundle, raw):
    with pytest.raises(DataLayerError):
        OfflineOKX(lambda url, method: raw).capture(
            bundle["source"], "1h", Window.parse(bundle["start"], bundle["end"]), raw_time_unit="ms"
        )


def test_nontext_sdk_option_keys_are_rejected_before_native_call(bundle):
    calls = []
    adapter = OfflineOKX(lambda url, method: calls.append(True) or bundle["raw"])
    adapter.exchange.options = {1: "synthetic"}
    with pytest.raises(DataLayerError, match="string-keyed mapping"):
        adapter.capture(
            bundle["source"], "1h", Window.parse(bundle["start"], bundle["end"]), raw_time_unit="ms"
        )
    assert calls == []
