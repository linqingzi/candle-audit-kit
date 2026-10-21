"""Explicit UTC nanosecond windows and the exact OKX declaration boundary."""

import re
from dataclasses import dataclass
from typing import Any, Self


class DataLayerError(ValueError):
    """A candidate or query does not satisfy the offline engineering contract."""


class RejectedCandidate(DataLayerError):
    """A failed ingest, retaining independent quality and reconcile diagnostics."""

    def __init__(self, reports: dict[str, Any]) -> None:
        super().__init__("candidate rejected; inspect independent quality/reconcile reports")
        self.reports = reports


def timestamp_ns(value: str | int) -> int:
    """Accept explicit integer ns or ISO instants with a timezone, without float conversion."""
    if type(value) is int:
        result = value
    elif isinstance(value, str) and re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})", value
    ):
        import pandas as pd

        try:
            result = pd.Timestamp(value).value
        except (ValueError, OverflowError) as exc:
            raise DataLayerError("timestamp outside supported integer ns/calendar range") from exc
    else:
        raise DataLayerError("explicit integer ns or timezone-aware ISO instant required")
    # pandas reserves the int64 minimum for NaT, so it cannot be an ISO UTC instant here.
    if not -(2**63) < result < 2**63:
        raise DataLayerError("timestamp outside signed int64 ns range")
    return result


@dataclass(frozen=True)
class Window:
    """UTC candle-opening interval [start_ns, end_ns); query bounds need not bar-align."""

    start_ns: int
    end_ns: int

    def __post_init__(self) -> None:
        if type(self.start_ns) is not int or type(self.end_ns) is not int:
            raise DataLayerError("Window fields require explicit integer nanoseconds")
        timestamp_ns(self.start_ns)
        timestamp_ns(self.end_ns)
        if self.start_ns >= self.end_ns:
            raise DataLayerError("increasing half-open window required")

    @classmethod
    def parse(cls, start: str | int, end: str | int) -> Self:
        return cls(timestamp_ns(start), timestamp_ns(end))

    def iso_bounds(self) -> tuple[str, str]:
        import pandas as pd

        return (
            pd.Timestamp(self.start_ns, unit="ns", tz="UTC").isoformat().replace("+00:00", "Z"),
            pd.Timestamp(self.end_ns, unit="ns", tz="UTC").isoformat().replace("+00:00", "Z"),
        )

    def aligned_step(self, timeframe: str) -> int:
        steps = {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "4h": 14400, "1d": 86400}
        if not isinstance(timeframe, str) or timeframe not in steps:
            raise DataLayerError("NOT_SUPPORTED timeframe")
        step = steps[timeframe] * 1_000_000_000
        if self.start_ns % step or self.end_ns % step:
            raise DataLayerError("ingest requires UTC epoch bar-aligned bounds")
        return step


def validate_source(source: object, timeframe: str, raw_time_unit: str) -> dict[str, str]:
    """An identity declaration is never evidence of market acquisition."""
    from candle_audit.candle_reconcile import BARS

    if not isinstance(timeframe, str) or timeframe not in BARS:
        raise DataLayerError("NOT_SUPPORTED timeframe")
    keys = {
        "venue",
        "instId",
        "instType",
        "base_asset",
        "quote_asset",
        "bar",
        "endpoint",
        "source_type",
        "acquisition",
    }
    if (
        not isinstance(source, dict)
        or set(source) != keys
        or not all(isinstance(v, str) and len(v) <= 240 for v in source.values())
    ):
        raise DataLayerError("exact bounded source declaration required")
    if raw_time_unit != "ms":
        raise DataLayerError("NOT_SUPPORTED raw time unit; explicit ms required")
    if (
        source["venue"] != "OKX"
        or source["instType"] not in ("SPOT", "SWAP")
        or (source["instType"] == "SWAP" and source["quote_asset"] != "USDT")
    ):
        raise DataLayerError("NOT_SUPPORTED venue/instrument; OKX SPOT or linear USDT SWAP only")
    base, quote = source["base_asset"], source["quote_asset"]
    suffix = "-SWAP" if source["instType"] == "SWAP" else ""
    if (
        not all(re.fullmatch(r"[A-Z0-9]+", a) for a in (base, quote))
        or base == quote
        or (source["instId"] != f"{base}-{quote}{suffix}")
    ):
        raise DataLayerError("instrument identity mismatch")
    if (
        source["bar"] != BARS.get(timeframe)
        or source["source_type"] != "trade"
        or (source["endpoint"] != "/api/v5/market/history-candles")
    ):
        raise DataLayerError("NOT_SUPPORTED bar/endpoint/source type")
    if source["acquisition"] not in ("SYNTHETIC", "UNVERIFIED"):
        raise DataLayerError("local acquisition cannot become VERIFIED")
    return dict(source)
