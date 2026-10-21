"""Explicit offline providers sharing the existing candidate admission boundary.

Providers are registered Python objects, never import strings. A receipt binds supplied
declarations to captured bytes; it does not authenticate their market origin.
"""

import hashlib
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, Self

from candle_audit.candle_reconcile import MAX_BYTES
from candle_audit.contract import data_versions
from candle_audit.contract.registry import RegistryError

from ._input import bounded, controlled_path, read_bounded
from .adapter import OfflineOKX, Transport
from .model import DataLayerError, Window, validate_source
from .store import CandidateStore, Snapshot, canonical, decode_document


def _provider_id(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", value):
        raise DataLayerError("provider_id requires 1..64 lowercase identifier characters")
    return value


@dataclass(frozen=True, init=False)
class ProviderRequest:
    """Validated declarations captured as bytes, with independent public dictionary views.

    Cross-declaration and input semantics are checked by the original quality kernels
    during ingest. Constructing a request alone does not admit a candidate.
    """

    provider_id: str
    window: Window
    raw_time_unit: str
    _metadata: bytes = field(repr=False)
    _source: bytes = field(repr=False)

    def __init__(
        self,
        provider_id: str,
        metadata: object,
        source: object,
        window: Window,
        *,
        raw_time_unit: str,
    ) -> None:
        identity = _provider_id(provider_id)
        document = decode_document(bounded(canonical(metadata), 65536, "metadata"))
        try:
            declaration = data_versions.metadata(document)
        except RegistryError as exc:
            raise DataLayerError("invalid exact data declaration") from exc
        origin = validate_source(
            decode_document(bounded(canonical(source), 65536, "source")),
            declaration["timeframe"],
            raw_time_unit,
        )
        if not isinstance(window, Window):
            raise DataLayerError("explicit Window required")
        window.aligned_step(declaration["timeframe"])
        object.__setattr__(self, "provider_id", identity)
        object.__setattr__(self, "window", window)
        object.__setattr__(self, "raw_time_unit", raw_time_unit)
        object.__setattr__(self, "_metadata", canonical(declaration))
        object.__setattr__(self, "_source", canonical(origin))

    @property
    def metadata(self) -> dict[str, Any]:
        return decode_document(self._metadata)

    @property
    def source(self) -> dict[str, Any]:
        return decode_document(self._source)

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema": "candle-audit-provider-request-v1",
            "provider_id": self.provider_id,
            "metadata": self.metadata,
            "source": self.source,
            "raw_time_unit": self.raw_time_unit,
            "window": {"start_ns": self.window.start_ns, "end_ns": self.window.end_ns},
        }

    @property
    def request_id(self) -> str:
        return hashlib.sha256(canonical(self.as_dict())).hexdigest()


@dataclass(frozen=True)
class ProviderResult:
    """Immutable original CSV/raw bytes bound to the request that selected their provider."""

    request: ProviderRequest
    candidate_csv: bytes = field(repr=False)
    raw: bytes = field(repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.request, ProviderRequest):
            raise DataLayerError("explicit ProviderRequest required")
        bounded(self.candidate_csv, MAX_BYTES, "candidate CSV")
        bounded(self.raw, MAX_BYTES, "native raw")
        if (
            hashlib.sha256(self.candidate_csv).hexdigest()
            != self.request.metadata["content_sha256"]
        ):
            raise DataLayerError("candidate CSV does not match request content_sha256")

    @property
    def receipt(self) -> dict[str, Any]:
        return {
            "schema": "candle-audit-provider-receipt-v1",
            "provider_id": self.request.provider_id,
            "request_id": self.request.request_id,
            "request": self.request.as_dict(),
            "candidate_sha256": hashlib.sha256(self.candidate_csv).hexdigest(),
            "raw_sha256": hashlib.sha256(self.raw).hexdigest(),
            "source_verification": "UNVERIFIED",
            "admission": "NOT_EVALUATED",
        }


class CandidateProvider(Protocol):
    """A trusted, explicitly registered offline object supplying one bounded candidate."""

    def acquire(self, request: ProviderRequest) -> ProviderResult: ...


class PageProvider(Protocol):
    """Explicit registered collection providers; no dynamic plugin loading."""

    def acquire_page(self, request: Any, cursor_ns: int, page_index: int) -> Any: ...


@dataclass(frozen=True)
class LocalCandidateProvider:
    """Import exact bytes once, including from the existing controlled file namespace."""

    candidate_csv: bytes = field(repr=False)
    raw: bytes = field(repr=False)

    def __post_init__(self) -> None:
        bounded(self.candidate_csv, MAX_BYTES, "candidate CSV")
        bounded(self.raw, MAX_BYTES, "native raw")

    @classmethod
    def from_files(cls, candidate_csv: Path, raw: Path) -> Self:
        paths = (controlled_path(candidate_csv), controlled_path(raw))
        try:
            return cls(*(read_bounded(path, MAX_BYTES) for path in paths))
        except OSError as exc:
            raise DataLayerError(
                "provider input is unavailable or not a safe regular file"
            ) from exc

    def acquire(self, request: ProviderRequest) -> ProviderResult:
        return ProviderResult(request, self.candidate_csv, self.raw)


@dataclass(frozen=True)
class OKXCaptureProvider:
    """Use the real native CCXT method with an explicit final offline byte transport.

    The CSV is supplied independently for reconciliation, never reconstructed from
    CCXT's non-authoritative float view. A fresh adapter isolates each capture.
    """

    candidate_csv: bytes = field(repr=False)
    transport: Transport | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        bounded(self.candidate_csv, MAX_BYTES, "candidate CSV")
        if self.transport is not None and not callable(self.transport):
            raise DataLayerError("an explicit callable offline transport is required")

    def acquire(self, request: ProviderRequest) -> ProviderResult:
        capture = OfflineOKX(self.transport).capture(
            request.source,
            request.metadata["timeframe"],
            request.window,
            raw_time_unit=request.raw_time_unit,
        )
        return ProviderResult(request, self.candidate_csv, capture.raw)


class ProviderRegistry:
    """Select explicit providers and publish only through CandidateStore's original gates."""

    def __init__(self) -> None:
        self._providers: dict[str, CandidateProvider | PageProvider] = {}

    def register(self, provider_id: str, provider: CandidateProvider | PageProvider) -> None:
        identity = _provider_id(provider_id)
        if identity in self._providers:
            raise DataLayerError("provider is already registered: " + identity)
        if not any(callable(getattr(provider, name, None)) for name in ("acquire", "acquire_page")):
            raise DataLayerError("an explicit provider object with acquire(request) is required")
        self._providers[identity] = provider

    def list_providers(self) -> tuple[str, ...]:
        """Return a stable, detached inventory of explicitly registered providers."""
        return tuple(sorted(self._providers))

    def select(self, provider_id: str) -> CandidateProvider | PageProvider:
        identity = _provider_id(provider_id)
        try:
            return self._providers[identity]
        except KeyError as exc:
            raise DataLayerError("unknown provider: " + identity) from exc

    def ingest(
        self, request: ProviderRequest, store: CandidateStore
    ) -> tuple[ProviderResult, Snapshot]:
        if not isinstance(request, ProviderRequest):
            raise DataLayerError("explicit ProviderRequest required")
        provider = self.select(request.provider_id)
        acquire = getattr(provider, "acquire", None)
        if not callable(acquire):
            raise DataLayerError("selected provider requires the collection interface")
        result = acquire(request)
        if not isinstance(result, ProviderResult) or result.request != request:
            raise DataLayerError("provider result must bind the selected request")
        snapshot = store.ingest(
            result.candidate_csv,
            request.metadata,
            result.raw,
            request.source,
            request.window,
            raw_time_unit=request.raw_time_unit,
        )
        return result, snapshot
