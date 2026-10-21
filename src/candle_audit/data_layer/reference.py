"""Exact candle objects and time-bounded instrument declarations, never source certification."""

import hashlib
from dataclasses import asdict, dataclass, field
from decimal import Decimal, InvalidOperation, localcontext
from typing import Any

from candle_audit.candle_reconcile import number

from .model import DataLayerError, Window, timestamp_ns, validate_source
from .store import canonical, decode_document


def exact_number(value: object) -> Decimal:
    try:
        result: Decimal = number(value)
        return result
    except (ValueError, InvalidOperation) as exc:
        raise DataLayerError("bounded exact decimal text required") from exc


@dataclass(frozen=True, init=False)
class InstrumentRules:
    timeframe: str
    effective: Window
    recorded_ns: int
    tick_size: str
    lot_size: str
    contract_size: str | None
    rule_evidence: bytes = field(repr=False)
    basis: str
    price_kind: str
    _source: bytes = field(repr=False)

    def __init__(
        self,
        *,
        source: object,
        timeframe: str,
        effective: Window,
        recorded_ns: int,
        tick_size: str,
        lot_size: str,
        contract_size: str | None,
        rule_evidence: bytes,
        basis: str = "DECLARED",
        price_kind: str = "trade",
    ) -> None:
        origin = validate_source(source, timeframe, "ms")
        if not isinstance(effective, Window):
            raise DataLayerError("explicit effective Window required")
        effective.aligned_step(timeframe)
        timestamp_ns(recorded_ns)
        if price_kind != "trade" or basis not in ("DECLARED", "OBSERVED"):
            raise DataLayerError("unknown price kind or reference basis")
        for value in (tick_size, lot_size):
            if exact_number(value) <= 0:
                raise DataLayerError("positive tick/lot decimal required")
        if origin["instType"] == "SWAP":
            if contract_size is None or exact_number(contract_size) <= 0:
                raise DataLayerError("explicit positive base contract size required")
        elif contract_size is not None:
            raise DataLayerError("SPOT cannot declare a contract size")
        if not isinstance(rule_evidence, bytes) or not 1 <= len(rule_evidence) <= 65536:
            raise DataLayerError("bounded nonempty reference evidence bytes required")
        for name, value in locals().copy().items():
            if name in self.__dataclass_fields__ and name != "_source":
                object.__setattr__(self, name, value)
        object.__setattr__(self, "_source", canonical(origin))

    @property
    def source(self) -> dict[str, str]:
        return decode_document(self._source)

    @property
    def quantity_units(self) -> dict[str, str]:
        return {
            "vol": "base" if self.source["instType"] == "SPOT" else "contracts",
            "volCcy": "quote" if self.source["instType"] == "SPOT" else "base",
            "volCcyQuote": "quote",
        }

    def as_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "timeframe": self.timeframe,
            "effective": {"start_ns": self.effective.start_ns, "end_ns": self.effective.end_ns},
            "recorded_ns": self.recorded_ns,
            "tick_size": self.tick_size,
            "lot_size": self.lot_size,
            "contract_size": self.contract_size,
            "rule_evidence_sha256": hashlib.sha256(self.rule_evidence).hexdigest(),
            "basis": self.basis,
            "price_kind": self.price_kind,
        }

    @property
    def rule_id(self) -> str:
        return hashlib.sha256(canonical(self.as_dict())).hexdigest()

    def lookup(self, opening_ns: int, *, strict_historical: bool = False) -> dict[str, Any]:
        timestamp_ns(opening_ns)
        if not self.effective.start_ns <= opening_ns < self.effective.end_ns:
            raise DataLayerError("instrument rules unavailable for effective time")
        if strict_historical:
            raise DataLayerError(
                "UNKNOWN historical rule truth: declaration requires independent audit"
            )
        return self.as_dict() | {"historical_truth": "UNVERIFIED"}


@dataclass(frozen=True)
class Candle:
    venue: str
    instrument: str
    market_kind: str
    price_kind: str
    base_asset: str
    quote_asset: str
    timeframe: str
    open_time_ns: int
    close_time_ns: int
    event_time_ns: int | None
    acquired_ns: int
    published_ns: int | None
    open: str
    high: str
    low: str
    close: str
    base_qty: str
    quote_qty: str
    contracts_qty: str | None
    state: str
    rule_id: str

    @classmethod
    def from_native(cls, row: object, rules: InstrumentRules, *, acquired_ns: int) -> "Candle":
        if not isinstance(row, list) or len(row) != 9 or not all(isinstance(x, str) for x in row):
            raise DataLayerError("exact native nine-string candle row required")
        ts = row[0]
        if len(ts) != 13 or not ts.isascii() or not ts.isdigit() or row[8] != "1":
            raise DataLayerError("closed native candle with explicit millisecond opening required")
        opening = int(ts) * 1_000_000
        rules.lookup(opening)
        step = rules.effective.aligned_step(rules.timeframe)
        timestamp_ns(acquired_ns)
        if (
            opening % step
            or opening + step > rules.effective.end_ns
            or acquired_ns < opening + step
        ):
            raise DataLayerError("unaligned/out-of-rule/unavailable closed candle time")
        values = [exact_number(x) for x in row[1:8]]
        o, h, low, c = values[:4]
        if (
            min(o, h, low, c) <= 0
            or h < max(o, low, c)
            or low > min(o, h, c)
            or min(values[4:]) < 0
        ):
            raise DataLayerError("invalid native OHLC or quantity")
        source = rules.source
        spot = source["instType"] == "SPOT"
        with localcontext() as context:
            context.prec = 4096
            tick, lot = exact_number(rules.tick_size), exact_number(rules.lot_size)
            if any(v % tick for v in values[:4]) or values[4] % lot:
                raise DataLayerError("native price/quantity violates declared tick/lot rule")
            if not spot and values[4] * exact_number(rules.contract_size) != values[5]:
                raise DataLayerError("native contracts/base quantity violates exact contract size")
            if spot and values[5] != values[6]:
                raise DataLayerError("native SPOT quote quantity fields disagree")
        return cls(
            venue=source["venue"],
            instrument=source["instId"],
            market_kind=source["instType"],
            price_kind=rules.price_kind,
            base_asset=source["base_asset"],
            quote_asset=source["quote_asset"],
            timeframe=rules.timeframe,
            open_time_ns=opening,
            close_time_ns=opening + step,
            event_time_ns=None,
            acquired_ns=acquired_ns,
            published_ns=None,
            open=row[1],
            high=row[2],
            low=row[3],
            close=row[4],
            base_qty=row[5] if spot else row[6],
            quote_qty=row[7],
            contracts_qty=None if spot else row[5],
            state="OBSERVED_ZERO" if all(x == 0 for x in values[4:]) else "OBSERVED",
            rule_id=rules.rule_id,
        )

    def decimal(self, field_name: str) -> Decimal:
        if field_name not in (
            "open",
            "high",
            "low",
            "close",
            "base_qty",
            "quote_qty",
            "contracts_qty",
        ):
            raise DataLayerError("explicit numeric candle field required")
        return exact_number(getattr(self, field_name))

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
