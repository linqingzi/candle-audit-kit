"""The parent-confirmed native error envelope cannot be presented as a successful capture."""

import json

import pytest

from candle_audit.data_layer import DataLayerError, OfflineOKX, Window


@pytest.mark.parametrize("code", ["403", "500", 0, None])
def test_real_sdk_capture_rejects_native_error_codes(bundle, code):
    raw = json.dumps({"code": code, "msg": "synthetic error", "data": []}).encode()
    adapter = OfflineOKX(lambda url, method: raw)
    with pytest.raises(
        DataLayerError, match="native response code|invalid bounded native JSON response"
    ):
        adapter.capture(
            bundle["source"], "1h", Window.parse(bundle["start"], bundle["end"]), raw_time_unit="ms"
        )


def test_successful_empty_native_capture_retains_raw_bytes_without_admission(bundle):
    raw = b'{"code":"0","msg":"","data":[]}'
    capture = OfflineOKX(lambda url, method: raw).capture(
        bundle["source"], "1h", Window.parse(bundle["start"], bundle["end"]), raw_time_unit="ms"
    )
    assert capture.raw == raw
    assert capture.ccxt_version == "4.5.64"
    assert capture.method == "publicGetMarketHistoryCandles"
    assert capture.float_view == ()
