"""Truthful public application identity through the real, socket-blocked history path."""

import io
from email.message import Message

import pytest
from test_full_collection import request

from candle_audit.data_layer import DataLayerError
from candle_audit.data_layer.collection import DownloadGrant, HistoryOKXProvider, PublicOKXTransport


def test_public_history_identifies_tool_and_preserves_scoped_request_and_raw_bytes(
    bundle, monkeypatch
):
    req = request(bundle, count=2)
    grant = DownloadGrant(
        req.request_id, 1, req.max_total_bytes, 9000000000000000000, "PUBLIC_OKX_HISTORY_ONLY"
    )
    transport = PublicOKXTransport(req, grant)
    raw = bundle["raw"]
    captured = []

    class Response(io.BytesIO):
        status = 200

        def __init__(self):
            super().__init__(raw)
            self.headers = Message()
            self.headers["Content-Type"] = "application/json"

    def open_response(http_request, timeout):
        captured.append(http_request)
        assert timeout == 10
        return Response()

    monkeypatch.setattr(transport._opener, "open", open_response)
    page = HistoryOKXProvider(transport).acquire_page(req, req.window.end_ns, 0)

    assert len(captured) == 1
    http_request = captured[0]
    assert http_request.get_method() == "GET"
    assert http_request.data is None
    assert http_request.full_url == (
        "https://www.okx.com/api/v5/market/history-candles?"
        "instId=BTC-USDT-SWAP&bar=1H&before=1767225599999&after=1767232800000&limit=2"
    )
    headers = {name.lower(): value for name, value in http_request.header_items()}
    assert headers.get("user-agent") == "Candle Audit Kit-DataLayer/0.1.0"
    # The exact header set also excludes cookies, authorization and trading credentials.
    assert headers == {
        "accept": "application/json",
        "accept-encoding": "identity",
        "user-agent": "Candle Audit Kit-DataLayer/0.1.0",
    }
    assert page.raw == raw
    assert page.candidate_csv is None
    assert page.request["sdk_parse_status"] == "PARSED"
    assert page.request["response"]["status"] == 200
    assert transport.bytes == len(raw)
    assert transport.requests == 1
    with pytest.raises(DataLayerError, match="expired or exhausted"):
        transport(http_request.full_url, "GET")
    assert len(captured) == 1
