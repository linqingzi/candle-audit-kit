"""A real CCXT OKX native-method call with a permanently offline byte transport."""

import math
from collections.abc import Callable, MutableMapping
from dataclasses import dataclass
from typing import Any, cast
from urllib.parse import urlsplit

import ccxt

from candle_audit.candle_reconcile import MAX_BYTES, MAX_ROWS, strict_json

from .model import DataLayerError, Window, validate_source

Transport = Callable[[str, str], bytes]


@dataclass(frozen=True)
class Capture:
    raw: bytes
    ccxt_version: str
    method: str
    quantity_units: dict[str, str]
    view_status: str
    float_view: tuple[tuple[int | float, ...], ...] | None


class OfflineOKX:
    """The injected transport supplies test-only or previously acquired bytes, never a socket."""

    def __init__(self, transport: Transport | None = None) -> None:
        self.exchange = ccxt.okx({"enableRateLimit": False})
        self._transport = transport
        self._raw: bytes | None = None
        self.exchange.fetch = self._fetch

    def _fetch(
        self, url: str, method: str = "GET", headers: object = None, body: object = None
    ) -> dict[str, Any]:
        address = urlsplit(url)
        if (address.scheme, address.netloc, address.path, method) != (
            "https",
            "www.okx.com",
            "/api/v5/market/history-candles",
            "GET",
        ) or body is not None:
            raise DataLayerError("only offline public history candle transport is supported")
        if self._transport is None:
            raise DataLayerError("explicit offline transport required; network is disabled")
        content = self._transport(url, method)
        if not isinstance(content, bytes) or len(content) > MAX_BYTES:
            raise DataLayerError("transport requires immutable bytes within the 8 MiB budget")
        try:
            value = strict_json(content)
        except (ValueError, UnicodeError, RecursionError) as exc:
            raise DataLayerError("invalid bounded native JSON response") from exc
        if not isinstance(value, dict):
            raise DataLayerError("native response must be an object")
        if value.get("code") != "0":
            raise DataLayerError("native response code must declare success")
        self._raw = content
        return value

    def capture(
        self,
        source: dict[str, str],
        timeframe: str,
        window: Window,
        *,
        raw_time_unit: str,
        page_rows: int = 300,
    ) -> Capture:
        declaration = validate_source(source, timeframe, raw_time_unit)
        native_options = self.exchange.options
        if not isinstance(native_options, dict) or not all(
            isinstance(key, str) for key in native_options
        ):
            raise DataLayerError("native SDK options must be a string-keyed mapping")
        options = cast(MutableMapping[str, object], native_options)
        step = window.aligned_step(timeframe)
        if type(page_rows) is not int or not 1 <= page_rows <= 300:
            raise DataLayerError("page size requires an integer within 1..300")
        self._raw = None
        response = self.exchange.publicGetMarketHistoryCandles(
            {
                "instId": declaration["instId"],
                "bar": declaration["bar"],
                "before": str(window.start_ns // 1_000_000 - 1),
                "after": str(window.end_ns // 1_000_000),
                "limit": min(page_rows, (window.end_ns - window.start_ns) // step),
            }
        )
        rows = response.get("data")
        if not isinstance(rows, list) or len(rows) > MAX_ROWS:
            raise DataLayerError("native response row type/budget exceeded")
        options["defaultType"] = "spot" if declaration["instType"] == "SPOT" else "swap"
        parsed = [self.exchange.parse_ohlcv(row) for row in rows if isinstance(row, list)]
        representable = len(parsed) == len(rows) and all(
            len(row) == 6 and all(isinstance(v, (int, float)) and math.isfinite(v) for v in row)
            for row in parsed
        )
        if self._raw is None:
            raise DataLayerError("CCXT did not use the offline byte transport")
        return Capture(
            raw=self._raw,
            ccxt_version=str(ccxt.__version__),
            method="publicGetMarketHistoryCandles",
            quantity_units={
                "vol": "base" if declaration["instType"] == "SPOT" else "contracts",
                "volCcy": "quote" if declaration["instType"] == "SPOT" else "base",
                "volCcyQuote": "quote",
            },
            view_status="NON_AUTHORITATIVE_FLOAT" if representable else "NOT_REPRESENTABLE",
            float_view=tuple(tuple(row) for row in parsed) if representable else None,
        )
