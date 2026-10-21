"""UTC complete-bucket OHLC and exact quantity aggregation, with explicit gap diagnostics."""

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal, localcontext
from typing import Any

import pyarrow as pa

from .model import DataLayerError, Window
from .reference import exact_number
from .releases import CANDLE_SCHEMA, DatasetResult, consumer_scope
from .store import canonical


@dataclass(frozen=True)
class AggregateResult:
    table: Any
    report: dict[str, Any]
    source_release_id: str


def declare_reconstruction(
    data: DatasetResult, openings_ns: Sequence[int], *, evidence: bytes
) -> DatasetResult:
    """Mark existing supplied rows as declared reconstruction; never create or interpolate a bar."""
    if (
        not isinstance(data, DatasetResult)
        or not data.table.schema.equals(CANDLE_SCHEMA, check_metadata=True)
        or not isinstance(openings_ns, (tuple, list))
        or not openings_ns
        or len(openings_ns) > 10000
        or any(type(v) is not int for v in openings_ns)
        or len(set(openings_ns)) != len(openings_ns)
        or not isinstance(evidence, bytes)
        or not 1 <= len(evidence) <= 65536
    ):
        raise DataLayerError(
            "explicit existing openings and bounded reconstruction evidence required"
        )
    rows = data.table.to_pylist()
    if not set(openings_ns) <= {r["open_time_ns"] for r in rows}:
        raise DataLayerError("reconstruction declaration cannot invent absent openings")
    marked = [
        r | {"state": "DECLARED_RECONSTRUCTION"} if r["open_time_ns"] in openings_ns else r
        for r in rows
    ]
    declaration = {
        "source_release_id": data.release_id,
        "openings_ns": list(openings_ns),
        "reconstruction_evidence_sha256": hashlib.sha256(evidence).hexdigest(),
    }
    return DatasetResult(
        pa.Table.from_pylist(marked, schema=CANDLE_SCHEMA),
        data.release_id,
        data.window,
        "VERIFY",
        hashlib.sha256(
            canonical({"query": data.cache_key, "declaration": declaration})
        ).hexdigest(),
        None,
        data.quality,
        data.visibility | declaration | {"reconstruction_truth": "DECLARED_UNVERIFIED"},
        data.selection,
    )


def aggregate_candles(
    data: DatasetResult, target_timeframe: str, window: Window
) -> AggregateResult:
    if not isinstance(data, DatasetResult) or not data.table.schema.equals(
        CANDLE_SCHEMA, check_metadata=True
    ):
        raise DataLayerError("complete typed canonical consumer output required")
    target_step = window.aligned_step(target_timeframe)
    if window.start_ns < data.window.start_ns or window.end_ns > data.window.end_ns:
        raise DataLayerError("aggregation window must stay inside the consumer input window")
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in data.table.to_pylist():
        if not window.start_ns <= row["open_time_ns"] < window.end_ns:
            continue
        groups.setdefault((row["instrument"], row["timeframe"]), []).append(row)
    scope = consumer_scope(data, window)
    for stream in scope["requested_streams"]:
        groups.setdefault((stream["instrument"], stream["timeframe"]), [])
    output, gaps, reconstructions = [], [], []
    instruments = [k[0] for k in groups]
    if len(instruments) != len(set(instruments)):
        raise DataLayerError("select one source timeframe per instrument before aggregation")
    for (instrument, source_timeframe), rows in sorted(groups.items()):
        source_step = window.aligned_step(source_timeframe)
        if target_step <= source_step or target_step % source_step:
            raise DataLayerError(
                "target must be an integer coarser multiple of the source timeframe"
            )
        points = {r["open_time_ns"]: r for r in rows}
        if len(points) != len(rows):
            raise DataLayerError("duplicate/conflicting source openings cannot aggregate")
        for opening in range(window.start_ns, window.end_ns, target_step):
            expected = range(opening, opening + target_step, source_step)
            missing = [t for t in expected if t not in points]
            if missing:
                gaps.append(
                    {
                        "instrument": instrument,
                        "source_timeframe": source_timeframe,
                        "bucket_start_ns": opening,
                        "status": "MISSING_COLLECTION",
                        "missing_openings_ns": missing,
                    }
                )
                continue
            bucket = [points[t] for t in expected]
            if any(
                r["close_time_ns"] != r["open_time_ns"] + source_step
                or r["acquired_ns"] < r["close_time_ns"]
                for r in bucket
            ):
                raise DataLayerError("unclosed or unavailable source bar cannot aggregate")
            if any(
                r["state"]
                not in (
                    "OBSERVED",
                    "OBSERVED_ZERO",
                    "AGGREGATED",
                    "AGGREGATED_ZERO",
                    "DECLARED_RECONSTRUCTION",
                )
                for r in bucket
            ):
                raise DataLayerError("unknown source state cannot aggregate")
            identity = (
                "venue",
                "instrument",
                "market_kind",
                "price_kind",
                "base_asset",
                "quote_asset",
            )
            if any(any(r[k] != bucket[0][k] for k in identity) for r in bucket):
                raise DataLayerError("cross-market/unit bucket cannot aggregate")
            if len({r["rule_id"] for r in bucket}) != 1:
                raise DataLayerError(
                    "mixed rule bucket cannot be represented by the canonical scalar rule_id"
                )
            bar = dict(bucket[0])
            bar.update(
                timeframe=target_timeframe,
                open_time_ns=opening,
                close_time_ns=opening + target_step,
                event_time_ns=None,
                acquired_ns=max(r["acquired_ns"] for r in bucket),
                published_ns=max(r["published_ns"] for r in bucket)
                if all(r["published_ns"] is not None for r in bucket)
                else None,
                high=max(bucket, key=lambda r: exact_number(r["high"]))["high"],
                low=min(bucket, key=lambda r: exact_number(r["low"]))["low"],
                close=bucket[-1]["close"],
            )
            with localcontext() as context:
                context.prec = 4096
                for name in ("base_qty", "quote_qty", "contracts_qty"):
                    quantities = [r[name] for r in bucket]
                    if all(q is None for q in quantities):
                        bar[name] = None
                    elif any(q is None for q in quantities):
                        raise DataLayerError(
                            "inconsistent contract quantity units in source bucket"
                        )
                    else:
                        bar[name] = str(sum((exact_number(q) for q in quantities), Decimal(0)))
                        if len(bar[name]) > 240:
                            raise DataLayerError("derived exact decimal cell budget exceeded")
            rebuilt = any(r["state"] == "DECLARED_RECONSTRUCTION" for r in bucket)
            bar["state"] = (
                "DECLARED_RECONSTRUCTION"
                if rebuilt
                else "AGGREGATED_ZERO"
                if all(Decimal(r["base_qty"]) == 0 and Decimal(r["quote_qty"]) == 0 for r in bucket)
                else "AGGREGATED"
            )
            if rebuilt:
                reconstructions.append({"instrument": instrument, "bucket_start_ns": opening})
            # Retain source rule identity without claiming an exchange-issued coarse bar.
            output.append(bar)
    status = (
        "INCOMPLETE"
        if gaps or scope["unmatched_instruments"] or scope["unmatched_timeframes"]
        else "CONTAINS_RECONSTRUCTION"
        if reconstructions
        else "COMPLETE"
        if output
        else "EMPTY"
    )
    return AggregateResult(
        pa.Table.from_pylist(output, schema=CANDLE_SCHEMA),
        {
            "status": status,
            "gaps": gaps,
            "reconstructions": reconstructions,
            "target_timeframe": target_timeframe,
            "quantity_method": "exact Decimal sum of explicitly denominated source quantities",
            "zero_basis": "observed zero quantity; never inferred from absent openings",
            "source_release_id": data.release_id,
            "scope": scope,
        },
        data.release_id,
    )


def audit_granularity(
    fine: DatasetResult, coarse: DatasetResult, target_timeframe: str, window: Window
) -> dict[str, Any]:
    """Compare actual coarse input to independently calculated complete fine buckets."""
    if (
        not isinstance(coarse, DatasetResult)
        or not coarse.table.schema.equals(CANDLE_SCHEMA, check_metadata=True)
        or window.start_ns < coarse.window.start_ns
        or window.end_ns > coarse.window.end_ns
    ):
        raise DataLayerError("full typed coarse consumer and covered window required")
    aggregated = aggregate_candles(fine, target_timeframe, window)
    left = {(r["instrument"], r["open_time_ns"]): r for r in aggregated.table.to_pylist()}
    supplied = [
        r for r in coarse.table.to_pylist() if window.start_ns <= r["open_time_ns"] < window.end_ns
    ]
    if any(r["timeframe"] != target_timeframe for r in supplied):
        raise DataLayerError("select the explicit coarse timeframe before audit")
    right = {(r["instrument"], r["open_time_ns"]): r for r in supplied}
    if len(right) != len(supplied):
        raise DataLayerError("duplicate coarse input cannot be audited")
    differences = []
    for key in sorted(left.keys() & right.keys()):
        for name in ("venue", "market_kind", "price_kind", "base_asset", "quote_asset"):
            if left[key][name] != right[key][name]:
                raise DataLayerError("cross-source instrument/quantity identity mismatch")
        for name in ("open", "high", "low", "close", "base_qty", "quote_qty", "contracts_qty"):
            a, b = left[key][name], right[key][name]
            if (a is None) != (b is None) or (a is not None and Decimal(a) != Decimal(b)):
                differences.append(
                    {
                        "instrument": key[0],
                        "open_time_ns": key[1],
                        "field": name,
                        "fine_aggregate": a,
                        "native_coarse": b,
                    }
                )
    reasons = []
    scopes = {"fine": aggregated.report["scope"], "coarse": consumer_scope(coarse, window)}
    for side, data in (("fine", fine), ("coarse", coarse)):
        scope = scopes[side]
        if (
            scope["unavailable_streams"]
            and scope["universe"] == "SELECTED"
            or (scope["unmatched_instruments"] or scope["unmatched_timeframes"])
        ):
            reasons.append({"side": side, "reason": "UNAVAILABLE_SELECTED_STREAMS", "scope": scope})
        states = sorted(
            {
                r["state"]
                for r in data.table.to_pylist()
                if window.start_ns <= r["open_time_ns"] < window.end_ns
            }
            - {"OBSERVED", "OBSERVED_ZERO"}
        )
        if states:
            reasons.append({"side": side, "reason": "NON_NATIVE_STATES", "states": states})
        if data.visibility.get("reconstruction_truth") is not None:
            reasons.append(
                {
                    "side": side,
                    "reason": "RECONSTRUCTION_DECLARATION",
                    "declaration": data.visibility["reconstruction_truth"],
                }
            )
    complete = aggregated.report["status"] == "COMPLETE" and not reasons
    comparison = (
        "NOT_COMPARABLE"
        if not complete
        else "MISMATCH"
        if differences or left.keys() != right.keys()
        else "MATCH"
    )
    return {
        "schema": "candle-audit-granularity-audit-v1",
        "comparison": comparison,
        "fine_release_id": fine.release_id,
        "coarse_release_id": coarse.release_id,
        "window": {"start_ns": window.start_ns, "end_ns": window.end_ns},
        "target_timeframe": target_timeframe,
        "differences": differences,
        "comparability_reasons": reasons,
        "missing_coarse": [list(k) for k in sorted(left.keys() - right.keys())],
        "extra_coarse": [list(k) for k in sorted(right.keys() - left.keys())],
        "source_independence": "NOT_ESTABLISHED",
        "market_accuracy": "UNVERIFIED",
        "aggregation": aggregated.report,
        "scope": scopes,
        "admission": "NOT_EVALUATED",
    }
