"""Exact v1/v2 metadata and the migrated kernels' existing float volume diagnostic."""

import copy
import math
import re
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from .registry import RegistryError

SCHEMA = "candle-audit-data-version-v1"
VOLUME_SCHEMA = "candle-audit-data-version-v2"
FIELDS = {
    "schema",
    "name",
    "version",
    "source",
    "provenance",
    "market",
    "currency",
    "timeframe",
    "timezone",
    "kind",
    "content_sha256",
}


def metadata(doc: object) -> dict[str, Any]:
    """Validate exact legacy v1/v2 fields and return an independent declaration."""
    expected = FIELDS
    if isinstance(doc, dict) and doc.get("schema") == VOLUME_SCHEMA and "volume" in doc:
        expected = FIELDS | {"volume"}
    if not isinstance(doc, dict) or set(doc) != expected:
        raise RegistryError("data metadata requires exactly: " + ", ".join(sorted(expected)))
    if doc["schema"] not in (SCHEMA, VOLUME_SCHEMA) or doc["kind"] not in (
        "historical",
        "synthetic",
    ):
        raise RegistryError("unknown data schema/kind")
    for key in FIELDS:
        if not isinstance(doc[key], str) or not doc[key].strip():
            raise RegistryError("metadata field must be nonempty text: " + key)
    for key in ("name", "version"):
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", doc[key]):
            raise RegistryError("unsafe version identity: " + key)
    if not re.fullmatch(r"[0-9a-f]{64}", doc["content_sha256"]):
        raise RegistryError("content_sha256 must be explicit lowercase SHA256")
    if doc["timeframe"] not in ("1m", "5m", "15m", "30m", "1h", "4h", "1d"):
        raise RegistryError("unsupported timeframe")
    try:
        ZoneInfo(doc["timezone"])
    except (ValueError, ZoneInfoNotFoundError) as exc:
        raise RegistryError("unknown timezone") from exc
    if doc["kind"] == "synthetic" and doc["source"] != "synthetic-engineering":
        raise RegistryError("synthetic source must declare synthetic-engineering")
    if doc["source"] == "synthetic-engineering" and doc["kind"] != "synthetic":
        raise RegistryError("synthetic engineering data cannot be historical")
    if "volume" in doc:
        volume_metadata(doc["volume"])
    return copy.deepcopy(doc)


def volume_metadata(volume: object) -> list[dict[str, Any]]:
    """Preserve the original explicit denomination and fixed contract-size rules."""
    if not isinstance(volume, dict) or set(volume) != {"base_asset", "quote_asset", "columns"}:
        raise RegistryError("volume requires base_asset,quote_asset,columns")
    for key in ("base_asset", "quote_asset"):
        if not isinstance(volume[key], str) or not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9_.-]{0,31}", volume[key]
        ):
            raise RegistryError("invalid volume asset: " + key)
    if volume["base_asset"] == volume["quote_asset"]:
        raise RegistryError("volume base and quote assets must differ")
    columns = volume["columns"]
    if not isinstance(columns, list) or not 1 <= len(columns) <= 3:
        raise RegistryError("volume requires one to three explicit columns")
    names: set[str] = set()
    units: set[str] = set()
    for column in columns:
        if not isinstance(column, dict):
            raise RegistryError("invalid volume column descriptor")
        unit = column.get("unit")
        expected = {"column", "unit"}
        if unit == "contracts":
            expected |= {"contract_size", "contract_size_unit"}
        if unit not in ("base", "quote", "contracts") or set(column) != expected:
            raise RegistryError("unknown volume unit or inappropriate contract metadata")
        name = column["column"]
        if (
            not isinstance(name, str)
            or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,63}", name)
            or (name in {"timestamp", "open", "high", "low", "close"})
        ):
            raise RegistryError("invalid volume column name")
        if name in names or unit in units:
            raise RegistryError("duplicate volume column/unit")
        names.add(name)
        units.add(unit)
        if unit == "contracts":
            size = column["contract_size"]
            try:
                valid = (
                    not isinstance(size, bool)
                    and isinstance(size, (int, float))
                    and math.isfinite(size)
                    and size > 0
                )
            except OverflowError:
                valid = False
            if not valid:
                raise RegistryError("contract_size must be a finite positive number")
            if column["contract_size_unit"] not in ("base", "quote"):
                raise RegistryError("contract_size_unit must be base or quote")
    return columns


def volume_columns(raw: pd.DataFrame, doc: dict[str, Any]) -> dict[str, NDArray[np.float64]]:
    """Legacy float diagnostic only; never used as storage or reconciliation truth."""
    extra: dict[str, NDArray[np.float64]] = {}
    descriptors = volume_metadata(doc["volume"])
    # The frozen kernels pass one already parsed row, containing exactly these columns.
    # Use scalar math for that shape; keep the original vector/extra-column path below.
    if raw.shape == (1, len(descriptors)) and list(raw.columns) == [
        d["column"] for d in descriptors
    ]:
        scalars: dict[str, float] = {}
        for descriptor, scalar in zip(descriptors, raw.to_numpy(dtype=float)[0], strict=True):
            value = float(scalar)
            if not math.isfinite(value) or value < 0:
                raise RegistryError(
                    "volume must be finite and nonnegative: " + descriptor["column"]
                )
            target = {"base": "volume", "quote": "quote_volume", "contracts": "volume_contracts"}[
                descriptor["unit"]
            ]
            scalars[target] = value
        for descriptor in descriptors:
            if descriptor["unit"] == "contracts":
                target = "volume" if descriptor["contract_size_unit"] == "base" else "quote_volume"
                converted = scalars["volume_contracts"] * descriptor["contract_size"]
                if not math.isfinite(converted):
                    raise RegistryError("contract volume conversion overflow")
                if target in scalars and abs(scalars[target] - converted) > 1e-12 * abs(converted):
                    raise RegistryError("reported volume disagrees with fixed contract conversion")
                scalars.setdefault(target, converted)
        return {name: np.array([value], dtype=np.float64) for name, value in scalars.items()}
    for descriptor in descriptors:
        values = raw[descriptor["column"]].to_numpy(dtype=float)
        if not np.isfinite(values).all() or (values < 0).any():
            raise RegistryError("volume must be finite and nonnegative: " + descriptor["column"])
        target = {"base": "volume", "quote": "quote_volume", "contracts": "volume_contracts"}[
            descriptor["unit"]
        ]
        extra[target] = values
    for descriptor in doc["volume"]["columns"]:
        if descriptor["unit"] == "contracts":
            target = "volume" if descriptor["contract_size_unit"] == "base" else "quote_volume"
            with np.errstate(over="ignore", invalid="ignore"):
                converted = extra["volume_contracts"] * descriptor["contract_size"]
            if not np.isfinite(converted).all():
                raise RegistryError("contract volume conversion overflow")
            if target in extra and not np.allclose(extra[target], converted, rtol=1e-12, atol=0):
                raise RegistryError("reported volume disagrees with fixed contract conversion")
            extra.setdefault(target, converted)
    return extra
