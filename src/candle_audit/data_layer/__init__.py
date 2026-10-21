"""Offline candidate ingestion and bounded exact-data queries."""

from importlib import import_module
from typing import TYPE_CHECKING

from .model import DataLayerError, RejectedCandidate, Window, timestamp_ns

if TYPE_CHECKING:
    from .adapter import Capture, OfflineOKX
    from .aggregation import aggregate_candles as aggregate_candles
    from .aggregation import audit_granularity as audit_granularity
    from .aggregation import declare_reconstruction as declare_reconstruction
    from .collection import CollectionRequest as CollectionRequest
    from .collection import Collector as Collector
    from .providers import (
        CandidateProvider as CandidateProvider,
    )
    from .providers import (
        LocalCandidateProvider as LocalCandidateProvider,
    )
    from .providers import (
        OKXCaptureProvider as OKXCaptureProvider,
    )
    from .providers import (
        ProviderRegistry as ProviderRegistry,
    )
    from .providers import (
        ProviderRequest as ProviderRequest,
    )
    from .providers import (
        ProviderResult as ProviderResult,
    )
    from .reference import Candle as Candle
    from .reference import InstrumentRules as InstrumentRules
    from .releases import DatasetReader as DatasetReader
    from .releases import ReleaseStore as ReleaseStore
    from .store import CandidateStore, QueryResult, Snapshot

__all__ = [
    "CandidateStore",
    "Capture",
    "DataLayerError",
    "OfflineOKX",
    "QueryResult",
    "RejectedCandidate",
    "Snapshot",
    "Window",
    "timestamp_ns",
]

_LAZY_EXPORTS = {
    "Capture": ".adapter",
    "OfflineOKX": ".adapter",
    "CandidateStore": ".store",
    "QueryResult": ".store",
    "Snapshot": ".store",
    "CandidateProvider": ".providers",
    "LocalCandidateProvider": ".providers",
    "OKXCaptureProvider": ".providers",
    "ProviderRegistry": ".providers",
    "ProviderRequest": ".providers",
    "ProviderResult": ".providers",
    "CollectionRequest": ".collection",
    "Collector": ".collection",
    "InstrumentRules": ".reference",
    "Candle": ".reference",
    "ReleaseStore": ".releases",
    "DatasetReader": ".releases",
    "aggregate_candles": ".aggregation",
    "audit_granularity": ".aggregation",
    "declare_reconstruction": ".aggregation",
}


def __getattr__(name: str) -> object:
    if name not in _LAZY_EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value: object = getattr(import_module(_LAZY_EXPORTS[name], __name__), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__) | set(_LAZY_EXPORTS))
