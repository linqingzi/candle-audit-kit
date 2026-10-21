"""Bounded paginated acquisition/import with original receipts, recovery and frozen shards."""

import csv
import hashlib
import io
import time
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

import ccxt

from candle_audit.candle_reconcile import MAX_BYTES, MAX_ROWS, strict_json, utc

from ._input import bounded, controlled_path
from .adapter import OfflineOKX, Transport
from .artifacts import Artifacts, digest, identifier
from .model import DataLayerError, RejectedCandidate, Window, timestamp_ns
from .providers import ProviderRegistry, _provider_id
from .reference import Candle, InstrumentRules, exact_number
from .store import CandidateStore, canonical, decode_document


@dataclass(frozen=True, init=False)
class CollectionRequest:
    provider_id: str
    rules: tuple[InstrumentRules, ...]
    window: Window
    name: str
    revision: str
    kind: str
    provenance: str
    shard_rows: int
    page_rows: int
    max_pages: int
    max_total_bytes: int
    _columns: bytes = field(repr=False)

    def __init__(
        self,
        *,
        provider_id: str,
        rules: tuple[InstrumentRules, ...],
        window: Window,
        name: str,
        revision: str,
        kind: str,
        provenance: str,
        shard_rows: int = MAX_ROWS,
        page_rows: int = 300,
        max_pages: int = 50000,
        max_total_bytes: int = 256 * 1024 * 1024,
        volume_columns: Sequence[dict[str, Any]] | None = None,
    ) -> None:
        _provider_id(provider_id)
        identifier(name)
        identifier(revision)
        if (
            not isinstance(rules, tuple)
            or not 1 <= len(rules) <= 64
            or not all(isinstance(r, InstrumentRules) for r in rules)
        ):
            raise DataLayerError("1..64 explicit instrument rules required")
        if not isinstance(window, Window):
            raise DataLayerError("explicit collection Window required")
        step = window.aligned_step(rules[0].timeframe)
        if (window.end_ns - window.start_ns) // step < 2:
            raise DataLayerError("preserved kernel boundary requires at least two bars")
        cursor = window.start_ns
        for rule in rules:
            if rule.source != rules[0].source or rule.timeframe != rules[0].timeframe:
                raise DataLayerError(
                    "one source/timeframe per collection; use releases for multiple streams"
                )
            if rule.effective.start_ns != cursor or (rule.effective.end_ns - cursor) // step < 2:
                raise DataLayerError(
                    "rules must cover contiguous effective windows with at least two bars"
                )
            cursor = rule.effective.end_ns
        if cursor != window.end_ns:
            raise DataLayerError("rules must cover the full requested window")
        for value, lower, upper in (
            (shard_rows, 2, MAX_ROWS),
            (page_rows, 1, 300),
            (max_pages, 1, 50000),
            (max_total_bytes, 1, 2**31),
        ):
            if type(value) is not int or not lower <= value <= upper:
                raise DataLayerError("collection resource budget outside supported bounds")
        if kind not in ("synthetic", "historical") or (
            rules[0].source["acquisition"] == "SYNTHETIC"
        ) != (kind == "synthetic"):
            raise DataLayerError("synthetic and historical declarations must agree")
        if not isinstance(provenance, str) or not 1 <= len(provenance) <= 240:
            raise DataLayerError("bounded explicit provenance required")
        columns = (
            [{"column": "base_qty", "unit": "base"}, {"column": "quote_qty", "unit": "quote"}]
            if volume_columns is None
            else list(volume_columns)
        )
        from candle_audit.contract.data_versions import volume_metadata
        from candle_audit.contract.registry import RegistryError

        try:
            volume_metadata(
                {
                    "base_asset": rules[0].source["base_asset"],
                    "quote_asset": rules[0].source["quote_asset"],
                    "columns": columns,
                }
            )
        except RegistryError as exc:
            raise DataLayerError("invalid explicit collection volume descriptors") from exc
        for column in columns:
            if column["unit"] == "contracts" and any(
                rule.source["instType"] != "SWAP"
                or column["contract_size_unit"] != "base"
                or exact_number(str(column["contract_size"])) != exact_number(rule.contract_size)
                for rule in rules
            ):
                raise DataLayerError(
                    "contract descriptor must match the exact base conversion of every rule"
                )
        for key in (
            "provider_id",
            "rules",
            "window",
            "name",
            "revision",
            "kind",
            "provenance",
            "shard_rows",
            "page_rows",
            "max_pages",
            "max_total_bytes",
        ):
            object.__setattr__(self, key, locals()[key])
        object.__setattr__(self, "_columns", canonical({"columns": columns}))
        bounded(canonical(self.as_dict()), 262144, "complete collection request")

    @property
    def source(self) -> dict[str, str]:
        return self.rules[0].source

    @property
    def timeframe(self) -> str:
        return self.rules[0].timeframe

    @property
    def columns(self) -> list[dict[str, Any]]:
        columns: list[dict[str, Any]] = decode_document(self._columns)["columns"]
        return columns

    def rule_at(self, opening_ns: int) -> InstrumentRules:
        for rule in self.rules:
            if rule.effective.start_ns <= opening_ns < rule.effective.end_ns:
                return rule
        raise DataLayerError("rules unavailable for candle opening")

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema": "candle-audit-collection-request-v1",
            "provider_id": self.provider_id,
            "rules": [
                r.as_dict() | {"rule_evidence_hex": r.rule_evidence.hex()} for r in self.rules
            ],
            "window": {"start_ns": self.window.start_ns, "end_ns": self.window.end_ns},
            "name": self.name,
            "revision": self.revision,
            "kind": self.kind,
            "provenance": self.provenance,
            "shard_rows": self.shard_rows,
            "page_rows": self.page_rows,
            "max_pages": self.max_pages,
            "max_total_bytes": self.max_total_bytes,
            "volume_columns": self.columns,
        }

    @classmethod
    def from_dict(cls, doc: object) -> "CollectionRequest":
        try:
            value = decode_document(bounded(canonical(doc), 262144, "collection request"))
            if value.pop("schema") != "candle-audit-collection-request-v1":
                raise DataLayerError("unknown collection request schema")
            parsed = []
            for item in value["rules"]:
                evidence = bytes.fromhex(item.pop("rule_evidence_hex"))
                if hashlib.sha256(evidence).hexdigest() != item.pop("rule_evidence_sha256"):
                    raise DataLayerError("reference evidence identity mismatch")
                item["effective"] = Window(**item["effective"])
                parsed.append(InstrumentRules(**item, rule_evidence=evidence))
            value["rules"] = tuple(parsed)
            value["window"] = Window(**value["window"])
            return cls(**value)
        except (KeyError, TypeError, ValueError, AttributeError) as exc:
            raise DataLayerError("invalid complete collection request") from exc

    @property
    def request_id(self) -> str:
        return hashlib.sha256(canonical(self.as_dict())).hexdigest()

    def metadata_for(self, content: bytes) -> dict[str, Any]:
        return {
            "schema": "candle-audit-data-version-v2",
            "name": self.name,
            "version": self.revision,
            "source": "synthetic-engineering"
            if self.kind == "synthetic"
            else "declared-public-import",
            "provenance": self.provenance,
            "market": self.source["instId"],
            "currency": self.source["quote_asset"],
            "timeframe": self.timeframe,
            "timezone": "UTC",
            "kind": self.kind,
            "content_sha256": hashlib.sha256(content).hexdigest(),
            "volume": {
                "base_asset": self.source["base_asset"],
                "quote_asset": self.source["quote_asset"],
                "columns": self.columns,
            },
        }


@dataclass(frozen=True)
class AcquiredPage:
    raw: bytes
    acquired_ns: int
    request: dict[str, Any]
    candidate_csv: bytes | None = None


@dataclass(frozen=True)
class HistoryOKXProvider:
    transport: Transport | None = field(default=None, repr=False)

    def acquire_page(
        self, request: CollectionRequest, cursor_ns: int, page_index: int
    ) -> AcquiredPage:
        if self.transport is None:
            raise DataLayerError("explicit public grant or offline byte transport required")
        transport = self.transport
        address: dict[str, Any] = {}
        captured: bytes | None = None

        def capture(url: str, method: str) -> bytes:
            nonlocal captured
            address.update(url=url, method=method)
            captured = bounded(transport(url, method), MAX_BYTES, "original response")
            return captured

        try:
            OfflineOKX(capture).capture(
                request.source,
                request.timeframe,
                Window(request.window.start_ns, cursor_ns),
                raw_time_unit="ms",
                page_rows=request.page_rows,
            )
            status = "PARSED"
        except (DataLayerError, ValueError, TypeError, KeyError, IndexError, ccxt.BaseError) as exc:
            if isinstance(exc, _PublicResponseError):
                captured = exc.raw
                address["response"] = exc.response
            if captured is None:
                raise
            status = "REJECTED"
        if captured is None:
            raise DataLayerError("native method did not capture original bytes")
        if status == "REJECTED":
            try:
                error_document = strict_json(captured)
            except (ValueError, UnicodeError, RecursionError):
                error_document = {}
            if isinstance(error_document, dict) and error_document.get("code") == "50011":
                raise _PublicTransportError(captured, {"api_code": "50011", "request": address})
        if isinstance(transport, PublicOKXTransport) and transport.last_response is not None:
            address["response"] = transport.last_response
        return AcquiredPage(
            captured,
            time.time_ns(),
            address
            | {
                "kind": "OKX_PUBLIC_HISTORY",
                "ccxt_version": str(ccxt.__version__),
                "sdk_parse_status": status,
                "origin_mode": "LIVE_PUBLIC_REQUEST"
                if isinstance(transport, PublicOKXTransport)
                else "SUPPLIED_OFFLINE_BYTES",
            },
        )


@dataclass(frozen=True)
class FileImportProvider:
    pages: tuple[tuple[bytes, bytes | None], ...] = field(repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.pages, tuple) or not 1 <= len(self.pages) <= 50000:
            raise DataLayerError("bounded captured file page tuple required")
        for raw, candidate in self.pages:
            bounded(raw, MAX_BYTES, "original imported raw")
            if candidate is not None:
                bounded(candidate, MAX_BYTES, "original imported CSV")

    def acquire_page(
        self, request: CollectionRequest, cursor_ns: int, page_index: int
    ) -> AcquiredPage:
        if page_index >= len(self.pages):
            raise DataLayerError("import files exhausted before full window")
        raw, candidate = self.pages[page_index]
        return AcquiredPage(
            raw, time.time_ns(), {"kind": "FILE_IMPORT", "page_index": page_index}, candidate
        )


@dataclass(frozen=True)
class DownloadGrant:
    request_id: str
    max_requests: int
    max_bytes: int
    expires_ns: int
    consent: str


class _PublicTransportError(OSError):
    def __init__(self, raw: bytes, response: dict[str, Any]) -> None:
        super().__init__("temporary public endpoint failure")
        self.raw = raw
        self.response = response


class _PublicResponseError(DataLayerError):
    def __init__(self, raw: bytes, response: dict[str, Any]) -> None:
        super().__init__("public endpoint response refused")
        self.raw = raw
        self.response = response


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(
        self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str
    ) -> None:
        return None


class PublicOKXTransport:
    """Public history only; construction requires an explicit request-bound commissioning grant.

    This API does not establish human authority. The calling Candle Audit Kit owner must supply
    an authorized grant; the CLI never constructs one without its explicit live flag.
    """

    def __init__(self, request: CollectionRequest, grant: DownloadGrant | None) -> None:
        if (
            not isinstance(grant, DownloadGrant)
            or grant.request_id != request.request_id
            or grant.consent != "PUBLIC_OKX_HISTORY_ONLY"
        ):
            raise DataLayerError("matching explicit public-download grant required")
        digest(grant.request_id)
        timestamp_ns(grant.expires_ns)
        if (
            type(grant.max_requests) is not int
            or not 1 <= grant.max_requests <= request.max_pages
            or type(grant.max_bytes) is not int
            or not 1 <= grant.max_bytes <= request.max_total_bytes
        ):
            raise DataLayerError("bounded public grant required")
        self.request, self.grant = request, grant
        self.requests, self.bytes = 0, 0
        self._last_request = 0.0
        self._opener = build_opener(ProxyHandler({}), _NoRedirect())
        self.last_response: dict[str, Any] | None = None

    def __call__(self, url: str, method: str) -> bytes:
        address = urlsplit(url)
        try:
            query = parse_qs(address.query, strict_parsing=True)
        except ValueError as exc:
            raise DataLayerError("malformed public query refused") from exc
        req = self.request
        valid = (address.scheme, address.netloc, address.path, method) == (
            "https",
            "www.okx.com",
            "/api/v5/market/history-candles",
            "GET",
        ) and not address.fragment
        try:
            valid = (
                valid
                and set(query) == {"instId", "bar", "before", "after", "limit"}
                and all(len(v) == 1 for v in query.values())
                and query["instId"] == [req.source["instId"]]
                and query["bar"] == [req.source["bar"]]
                and int(query["before"][0]) == req.window.start_ns // 1_000_000 - 1
                and req.window.start_ns // 1_000_000
                < int(query["after"][0])
                <= req.window.end_ns // 1_000_000
                and 1 <= int(query["limit"][0]) <= req.page_rows
            )
        except (ValueError, KeyError):
            valid = False
        if not valid:
            raise DataLayerError("public download address/parameters outside grant")
        if (
            time.time_ns() > self.grant.expires_ns
            or self.requests >= self.grant.max_requests
            or self.bytes >= self.grant.max_bytes
        ):
            raise DataLayerError("public download grant expired or exhausted")
        pause = 0.11 - (time.monotonic() - self._last_request)
        if pause > 0:
            time.sleep(pause)
        self._last_request = time.monotonic()
        self.requests += 1
        request = Request(
            url,
            headers={
                "Accept": "application/json",
                "Accept-Encoding": "identity",
                "User-Agent": "Candle Audit Kit-DataLayer/0.1.0",
            },
            method="GET",
        )
        try:
            with self._opener.open(request, timeout=10) as response:
                content = response.read(min(MAX_BYTES, self.grant.max_bytes - self.bytes) + 1)
                if len(content) > self.grant.max_bytes - self.bytes:
                    raise DataLayerError("public download total byte budget exceeded")
                bounded(content, MAX_BYTES, "public response")
                self.bytes += len(content)
                self.last_response = {
                    "status": response.status,
                    "headers": {
                        k: str(response.headers[k])[:240]
                        for k in ("Date", "Content-Type", "Content-Encoding")
                        if k in response.headers
                    },
                    "received_ns": time.time_ns(),
                }
                if (
                    response.status != 200
                    or response.headers.get_content_type() != "application/json"
                    or response.headers.get("Content-Encoding", "identity") != "identity"
                ):
                    raise _PublicResponseError(content, self.last_response)
                return bounded(content, MAX_BYTES, "public response")
        except HTTPError as exc:
            raw = bounded(
                exc.read(min(MAX_BYTES, self.grant.max_bytes - self.bytes) + 1),
                MAX_BYTES,
                "public HTTP error body",
            )
            if len(raw) > self.grant.max_bytes - self.bytes:
                raise DataLayerError("public error body total byte budget exceeded") from None
            self.bytes += len(raw)
            metadata = {
                "status": exc.code,
                "headers": {
                    k: str(exc.headers[k])[:240]
                    for k in ("Date", "Content-Type", "Content-Encoding")
                    if k in exc.headers
                },
                "received_ns": time.time_ns(),
            }
            exc.close()
            if exc.code in (429, 500, 502, 503, 504):
                raise _PublicTransportError(raw, metadata) from None
            raise _PublicResponseError(raw, metadata) from None
        except URLError:
            raise OSError("public endpoint unavailable") from None


class CollectionFailure(DataLayerError):
    def __init__(self, state: dict[str, Any]) -> None:
        super().__init__("collection incomplete; retained checkpoint and quarantine diagnostics")
        self.state = state


def _provider_identity(provider: object) -> str:
    doc: dict[str, Any] = {
        "implementation": type(provider).__module__ + "." + type(provider).__qualname__
    }
    if isinstance(provider, FileImportProvider):
        doc["inputs"] = [
            [
                hashlib.sha256(raw).hexdigest(),
                hashlib.sha256(candidate).hexdigest() if candidate is not None else None,
            ]
            for raw, candidate in provider.pages
        ]
    elif isinstance(provider, HistoryOKXProvider):
        doc["transport"] = (
            asdict(provider.transport.grant)
            if isinstance(provider.transport, PublicOKXTransport)
            else "EXPLICIT_OFFLINE_BYTES"
        )
    return hashlib.sha256(canonical(doc)).hexdigest()


@dataclass(frozen=True)
class CollectionResult:
    collection_id: str
    request_id: str
    status: str
    rows: int
    shards: tuple[dict[str, Any], ...]
    quality: dict[str, Any]
    document: dict[str, Any] = field(repr=False)


def _csv_content(rows: list[list[str]], request: CollectionRequest) -> bytes:
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n")
    columns = request.columns
    writer.writerow(["timestamp", "open", "high", "low", "close", *[d["column"] for d in columns]])
    indexes = {"base": 5 if request.source["instType"] == "SPOT" else 6, "quote": 7, "contracts": 5}
    import pandas as pd

    for row in sorted(rows, key=lambda r: int(r[0])):
        opening = (
            pd.Timestamp(int(row[0]) * 1_000_000, unit="ns", tz="UTC")
            .isoformat()
            .replace("+00:00", "Z")
        )
        writer.writerow([opening, *row[1:5], *[row[indexes[d["unit"]]] for d in columns]])
    return bounded(stream.getvalue().encode(), MAX_BYTES, "normalized shard CSV")


def _validate_import(content: bytes, rows: list[list[str]], request: CollectionRequest) -> None:
    try:
        expected = list(csv.reader(io.StringIO(_csv_content(rows, request).decode())))
        supplied = []
        for index, row in enumerate(
            csv.reader(io.StringIO(bounded(content, MAX_BYTES, "import CSV").decode()))
        ):
            if (
                index > len(rows)
                or len(row) != len(expected[0])
                or any(len(cell) > 96 for cell in row)
            ):
                raise DataLayerError("import CSV decoded row/cell budget exceeded")
            supplied.append(row)
        if not supplied or supplied[0] != expected[0] or len(supplied) != len(expected):
            raise DataLayerError("import CSV layout/count mismatch")
        for got, want in zip(supplied[1:], expected[1:], strict=True):
            if (
                len(got) != len(want)
                or utc(got[0]) != utc(want[0])
                or any(
                    exact_number(a) != exact_number(b)
                    for a, b in zip(got[1:], want[1:], strict=True)
                )
            ):
                raise DataLayerError("import CSV/raw field mismatch")
    except (ValueError, UnicodeError, csv.Error) as exc:
        raise DataLayerError("invalid original import CSV") from exc


class Collector:
    def __init__(self, root: Path) -> None:
        self.root = controlled_path(root)
        self.artifacts = Artifacts(self.root / "artifacts")
        self.candidates = CandidateStore(self.root / "candidates")

    def task(self, request_id: str) -> dict[str, Any] | None:
        identity = digest(request_id)
        state = self.artifacts.load_state(identity)
        required = {
            "schema",
            "request_id",
            "request",
            "status",
            "rows",
            "bytes",
            "page_count",
            "page_head",
            "cursor_ns",
            "collection_id",
            "alert",
            "updated_ns",
            "provider_identity",
            "attempts",
            "attempt_head",
            "retry_budget",
            "captured_bytes",
        }
        if state is not None:
            if (
                not required <= set(state)
                or state["schema"] != "candle-audit-collection-task-v1"
                or state["request_id"] != identity
                or state["status"] not in ("RUNNING", "FAILED", "QUARANTINED", "COMPLETE")
            ):
                raise DataLayerError("invalid saved task checkpoint schema/identity")
            if any(
                type(state[k]) is not int or state[k] < 0
                for k in ("rows", "bytes", "page_count", "attempts", "captured_bytes")
            ):
                raise DataLayerError("invalid saved task counters")
            timestamp_ns(state["updated_ns"])
            timestamp_ns(state["cursor_ns"])
        return state

    def _pages(self, head: str | None, budget: int) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        seen = set()
        while head is not None:
            digest(head)
            if head in seen or len(result) >= budget:
                raise DataLayerError("page chain cycle/budget exceeded")
            seen.add(head)
            page = decode_document(self.artifacts.get(head, limit=262144))
            if "previous" not in page:
                raise DataLayerError("invalid saved journal link")
            result.append(page)
            head = page["previous"]
        return result[::-1]

    def pages(self, result: CollectionResult) -> list[dict[str, Any]]:
        return self._pages(result.document["page_head"], result.document["request"]["max_pages"])

    def _captured_bytes(self, attempts: list[dict[str, Any]], budget: int) -> int:
        total = 0
        for attempt in attempts:
            if "outcome" not in attempt:
                # A crash after dispatch has an unknown response outcome. Reserve the
                # remaining byte budget rather than pretend the grant was not used.
                return budget
            outcome = attempt["outcome"]
            for name in ("raw_sha256", "csv_sha256"):
                if outcome.get(name) is not None:
                    total += len(self.artifacts.get(outcome[name]))
        return total

    def _rows(
        self, receipt: dict[str, Any], request: CollectionRequest, cursor: int, index: int
    ) -> list[list[str]]:
        if (
            receipt.get("schema") != "candle-audit-acquisition-receipt-v1"
            or receipt.get("source_verification") != "UNVERIFIED"
        ):
            raise DataLayerError("saved receipt cannot assert source authority")
        if (
            receipt["request_id"] != request.request_id
            or receipt["page_index"] != index
            or receipt["cursor_ns"] != cursor
        ):
            raise DataLayerError("page receipt/request/checkpoint identity mismatch")
        raw = self.artifacts.get(receipt["raw_sha256"])
        csv_bytes = (
            self.artifacts.get(receipt["csv_sha256"]) if receipt["csv_sha256"] is not None else None
        )
        actual_bytes = len(raw) + (len(csv_bytes) if csv_bytes is not None else 0)
        if type(receipt["bytes"]) is not int or receipt["bytes"] != actual_bytes:
            raise DataLayerError("saved receipt byte counter contradicts actual captured inputs")
        try:
            doc = strict_json(raw)
            if (
                not isinstance(doc, dict)
                or set(doc) != {"code", "msg", "data"}
                or doc["code"] != "0"
                or not isinstance(doc["msg"], str)
            ):
                raise DataLayerError("native page envelope mismatch")
            rows = doc["data"]
            if not isinstance(rows, list) or not 1 <= len(rows) <= request.page_rows:
                raise DataLayerError("empty/over-budget native page before full completion")
            # File order is not API pagination order. Retain the original bytes and
            # map their positions separately while walking the same descending cursor.
            if receipt["provider_request"]["kind"] == "FILE_IMPORT":
                rows = sorted(rows, key=lambda row: int(row[0]), reverse=True)
            step = request.window.aligned_step(request.timeframe)
            for row in rows:
                bar = Candle.from_native(
                    row,
                    request.rule_at(int(row[0]) * 1_000_000),
                    acquired_ns=receipt["acquired_ns"],
                )
                if bar.open_time_ns != cursor - step:
                    raise DataLayerError("gap/duplicate/conflict/non-progressing page opening")
                cursor = bar.open_time_ns
            if csv_bytes is not None:
                _validate_import(csv_bytes, rows, request)
            return rows
        except (ValueError, TypeError, KeyError, IndexError, RecursionError) as exc:
            raise DataLayerError("invalid native page semantics") from exc

    def collect(
        self,
        request: CollectionRequest,
        providers: ProviderRegistry,
        *,
        retries: int = 2,
        sleep: Callable[[float], None] = time.sleep,
    ) -> CollectionResult:
        if (
            not isinstance(request, CollectionRequest)
            or type(retries) is not int
            or not 0 <= retries <= 2
        ):
            raise DataLayerError("explicit collection request and bounded retries required")
        provider = providers.select(request.provider_id)
        provider_id = _provider_identity(provider)
        live = (
            provider.transport
            if isinstance(provider, HistoryOKXProvider)
            and isinstance(provider.transport, PublicOKXTransport)
            else None
        )
        attempt_budget = (
            min(request.max_pages * 3, live.grant.max_requests) if live else request.max_pages * 3
        )
        byte_budget = (
            min(request.max_total_bytes, live.grant.max_bytes) if live else request.max_total_bytes
        )
        acquire = getattr(provider, "acquire_page", None)
        if not callable(acquire):
            raise DataLayerError("selected provider has no paginated collection interface")
        with self.artifacts.lock(request.request_id):
            old = self.task(request.request_id)
            state: dict[str, Any] = {
                "schema": "candle-audit-collection-task-v1",
                "request_id": request.request_id,
                "request": request.as_dict(),
                "status": "RUNNING",
                "rows": 0,
                "bytes": 0,
                "page_count": 0,
                "page_head": None,
                "cursor_ns": request.window.end_ns,
                "collection_id": None,
                "alert": None,
                "updated_ns": time.time_ns(),
                "provider_identity": provider_id,
                "attempts": 0,
                "attempt_head": None,
                "retry_budget": 2,
                "captured_bytes": 0,
            }
            if old is not None:
                if (
                    old["request"] != request.as_dict()
                    or old["provider_identity"] != provider_id
                    or old["retry_budget"] != 2
                ):
                    raise DataLayerError("checkpoint complete request binding mismatch")
                attempts = self._pages(old["attempt_head"], request.max_pages * 3)
                for index, record in enumerate(attempts):
                    if (
                        record["request_id"] != request.request_id
                        or record["attempt_index"] != index
                        or record["provider_identity"] != provider_id
                        or not request.window.start_ns
                        < record["cursor_ns"]
                        <= request.window.end_ns
                    ):
                        raise DataLayerError("checkpoint attempt journal identity mismatch")
                if type(old["attempts"]) is not int or old["attempts"] != len(attempts):
                    raise DataLayerError("checkpoint attempt count mismatch")
                state.update(attempts=len(attempts), attempt_head=old["attempt_head"])
                state["captured_bytes"] = self._captured_bytes(attempts, byte_budget)
                if state["captured_bytes"] != old["captured_bytes"]:
                    raise DataLayerError(
                        "checkpoint captured-byte counter contradicts attempt receipts"
                    )
                for index, receipt in enumerate(self._pages(old["page_head"], request.max_pages)):
                    rows = self._rows(receipt, request, state["cursor_ns"], index)
                    state["rows"] += len(rows)
                    state["bytes"] += receipt["bytes"]
                    state["cursor_ns"] = int(rows[-1][0]) * 1_000_000
                    state["page_count"] += 1
                state["page_head"] = old["page_head"]
                if any(state[k] != old[k] for k in ("rows", "bytes", "cursor_ns", "page_count")):
                    raise DataLayerError("checkpoint counters contradict replayed original pages")
                if old["status"] == "COMPLETE":
                    if not isinstance(provider, FileImportProvider):
                        raise DataLayerError(
                            "completed acquisition is immutable; "
                            "use read(collection_id) or a new revision to reacquire"
                        )
                    return self.read(old["collection_id"])
            self.artifacts.save_state(request.request_id, state)
            if live is not None:
                live.requests = max(live.requests, state["attempts"])
                live.bytes = max(live.bytes, state["captured_bytes"])
            try:
                while state["cursor_ns"] > request.window.start_ns:
                    if state["page_count"] >= request.max_pages:
                        raise DataLayerError("page budget exhausted before full completion")
                    page = None
                    for attempt in range(retries + 1):
                        if state["attempts"] >= attempt_budget:
                            raise DataLayerError("persistent acquisition attempt budget exhausted")
                        if state["captured_bytes"] >= byte_budget:
                            raise DataLayerError(
                                "persistent captured-response byte budget exhausted"
                            )
                        if state["page_count"] or attempt:
                            sleep(max(0.11, min(2.0, 0.11 * 2**attempt)))
                        attempt_id = self.artifacts.put(
                            canonical(
                                {
                                    "schema": "candle-audit-acquisition-attempt-v1",
                                    "request_id": request.request_id,
                                    "attempt_index": state["attempts"],
                                    "cursor_ns": state["cursor_ns"],
                                    "provider_identity": provider_id,
                                    "started_ns": time.time_ns(),
                                    "previous": state["attempt_head"],
                                }
                            )
                        )
                        state.update(attempts=state["attempts"] + 1, attempt_head=attempt_id)
                        self.artifacts.save_state(request.request_id, state)
                        try:
                            page = acquire(request, state["cursor_ns"], state["page_count"])
                            break
                        except (OSError, TimeoutError, ccxt.NetworkError) as exc:
                            attempt_doc = decode_document(self.artifacts.get(attempt_id))
                            outcome: dict[str, Any] = {
                                "status": "ACQUISITION_FAILED",
                                "raw_sha256": None,
                                "finished_ns": time.time_ns(),
                            }
                            if isinstance(exc, _PublicTransportError):
                                state["captured_bytes"] += len(exc.raw)
                                outcome.update(
                                    status="RETRYABLE_API_ERROR"
                                    if "api_code" in exc.response
                                    else "RETRYABLE_HTTP_ERROR",
                                    raw_sha256=self.artifacts.put(exc.raw),
                                    response=exc.response,
                                )
                            state["attempt_head"] = self.artifacts.put(
                                canonical(attempt_doc | {"outcome": outcome})
                            )
                            self.artifacts.save_state(request.request_id, state)
                            if attempt == retries:
                                raise OSError("bounded acquisition attempts exhausted") from None
                    if not isinstance(page, AcquiredPage):
                        raise DataLayerError("provider requires an explicit acquired-page result")
                    timestamp_ns(page.acquired_ns)
                    size = len(bounded(page.raw, MAX_BYTES, "original page")) + (
                        len(page.candidate_csv) if page.candidate_csv is not None else 0
                    )
                    raw_sha = self.artifacts.put(page.raw)
                    csv_sha = (
                        self.artifacts.put(page.candidate_csv)
                        if page.candidate_csv is not None
                        else None
                    )
                    attempt_doc = decode_document(self.artifacts.get(state["attempt_head"]))
                    if not attempt_doc["started_ns"] <= page.acquired_ns <= time.time_ns():
                        raise DataLayerError(
                            "provider cannot backdate/future-date actual acquisition receipt"
                        )
                    state["attempt_head"] = self.artifacts.put(
                        canonical(
                            attempt_doc
                            | {
                                "outcome": {
                                    "status": "CAPTURED",
                                    "raw_sha256": raw_sha,
                                    "csv_sha256": csv_sha,
                                    "finished_ns": page.acquired_ns,
                                    "provider_request": page.request,
                                }
                            }
                        )
                    )
                    receipt = {
                        "schema": "candle-audit-acquisition-receipt-v1",
                        "request_id": request.request_id,
                        "page_index": state["page_count"],
                        "cursor_ns": state["cursor_ns"],
                        "raw_sha256": raw_sha,
                        "csv_sha256": csv_sha,
                        "bytes": size,
                        "acquired_ns": page.acquired_ns,
                        "provider_request": page.request,
                        "source_verification": "UNVERIFIED",
                        "previous": state["page_head"],
                        "attempt_id": state["attempt_head"],
                        "provider_identity": provider_id,
                    }
                    receipt_id = self.artifacts.put(
                        bounded(canonical(receipt), 262144, "page receipt")
                    )
                    state["last_attempt_receipt"] = receipt_id
                    state["captured_bytes"] += size
                    if state["captured_bytes"] > byte_budget:
                        raise DataLayerError("total acquisition byte budget exceeded")
                    rows = self._rows(receipt, request, state["cursor_ns"], state["page_count"])
                    state.update(
                        page_head=receipt_id,
                        cursor_ns=int(rows[-1][0]) * 1_000_000,
                        rows=state["rows"] + len(rows),
                        bytes=state["bytes"] + size,
                        page_count=state["page_count"] + 1,
                        updated_ns=time.time_ns(),
                    )
                    self.artifacts.save_state(request.request_id, state)
                result = self._materialize(request, state["page_head"])
                state.update(
                    status="COMPLETE",
                    collection_id=result.collection_id,
                    alert=None,
                    updated_ns=time.time_ns(),
                )
                self.artifacts.save_state(request.request_id, state)
                return result
            except (OSError, DataLayerError, ccxt.NetworkError) as exc:
                acquisition = isinstance(exc, (OSError, ccxt.NetworkError))
                state.update(
                    status="FAILED" if acquisition else "QUARANTINED",
                    collection_id=None,
                    alert={
                        "severity": "ERROR",
                        "code": "ACQUISITION_FAILED" if acquisition else "VALIDATION_FAILED",
                        "detail": "bounded collection failed; inspect retained artifacts",
                        "at_ns": time.time_ns(),
                    },
                    updated_ns=time.time_ns(),
                )
                if isinstance(exc, RejectedCandidate):
                    state["quality_reports_sha256"] = self.artifacts.put(
                        bounded(canonical(exc.reports), 262144, "failed kernel reports")
                    )
                state["failure_type"] = type(exc).__name__
                self.artifacts.save_state(request.request_id, state)
                raise CollectionFailure(state) from exc

    def _materialize(
        self, request: CollectionRequest, head: str, saved: dict[str, Any] | None = None
    ) -> CollectionResult:
        pages = self._pages(head, request.max_pages)
        cursor, count, total_bytes = request.window.end_ns, 0, 0
        buffers: list[tuple[list[str], str, int, int]] = []
        shards: list[dict[str, Any]] = []
        saved_shards = (
            {} if saved is None else {(s["start_ns"], s["end_ns"]): s for s in saved["shards"]}
        )
        if saved is not None and len(saved_shards) != len(saved["shards"]):
            raise DataLayerError("duplicate saved shard windows")

        def flush(values: list[tuple[list[str], str, int, int]]) -> None:
            if len(shards) >= 4096:
                raise DataLayerError("partition catalog budget exceeded")
            rows = [v[0] for v in values]
            if len(rows) > MAX_ROWS:
                flush(values[:-2])
                flush(values[-2:])
                return
            openings = [int(r[0]) * 1_000_000 for r in rows]
            window = Window(
                min(openings), max(openings) + request.window.aligned_step(request.timeframe)
            )
            raw_bytes = canonical({"code": "0", "msg": "", "data": rows})
            try:
                csv_bytes = _csv_content(rows, request)
                bounded(raw_bytes, MAX_BYTES, "derived raw shard")
            except DataLayerError:
                if len(values) < 4:
                    raise
                middle = len(values) // 2
                flush(values[:middle])
                flush(values[middle:])
                return
            mapping_bytes = canonical(
                {
                    "rows": [
                        {"page_sha256": v[1], "native_row": v[2], "acquired_ns": v[3]}
                        for v in values
                    ]
                }
            )
            if saved is None:
                snapshot = self.candidates.ingest(
                    csv_bytes,
                    request.metadata_for(csv_bytes),
                    raw_bytes,
                    request.source,
                    window,
                    raw_time_unit="ms",
                )
                snapshot_id = snapshot.snapshot_id
                mapping = self.artifacts.put(mapping_bytes)
            else:
                shard = saved_shards[(window.start_ns, window.end_ns)]
                snapshot_id = shard["snapshot_id"]
                captured = self.candidates._load(snapshot_id)
                _, files = captured
                self.candidates._query_loaded(snapshot_id, window, captured)
                if (
                    files["candidate.csv"] != csv_bytes
                    or files["raw.json"] != raw_bytes
                    or decode_document(files["metadata.json"]) != request.metadata_for(csv_bytes)
                    or decode_document(files["source.json"]) != request.source
                ):
                    raise DataLayerError("saved shard contradicts original page mapping")
                mapping = hashlib.sha256(mapping_bytes).hexdigest()
                if (
                    shard["mapping_sha256"] != mapping
                    or self.artifacts.get(mapping) != mapping_bytes
                ):
                    raise DataLayerError("saved original-row mapping mismatch")
            shards.append(
                {
                    "snapshot_id": snapshot_id,
                    "mapping_sha256": mapping,
                    "start_ns": window.start_ns,
                    "end_ns": window.end_ns,
                    "rows": len(rows),
                }
            )

        for index, page in enumerate(pages):
            rows = self._rows(page, request, cursor, index)
            total_bytes += page["bytes"]
            if total_bytes > request.max_total_bytes:
                raise DataLayerError("replayed collection exceeds complete-request byte budget")
            original_rows = strict_json(self.artifacts.get(page["raw_sha256"]))["data"]
            positions = {row[0]: i for i, row in enumerate(original_rows)}
            for row in rows:
                if (
                    buffers
                    and request.rule_at(int(buffers[-1][0][0]) * 1_000_000).rule_id
                    != request.rule_at(int(row[0]) * 1_000_000).rule_id
                ):
                    flush(buffers)
                    buffers = []
                buffers.append((row, page["raw_sha256"], positions[row[0]], page["acquired_ns"]))
                if len(buffers) >= request.shard_rows + 2:
                    flush(buffers[: request.shard_rows])
                    buffers = buffers[request.shard_rows :]
            count += len(rows)
            cursor = int(rows[-1][0]) * 1_000_000
        if cursor != request.window.start_ns:
            raise DataLayerError("collection full-window boundary incomplete")
        if buffers:
            flush(buffers)
        shards.sort(key=lambda s: s["start_ns"])
        quality = {
            key: {"status": "PASS"}
            for key in ("completeness", "uniqueness", "consistency", "validity")
        }
        quality.update(
            accuracy={
                "status": "UNVERIFIED",
                "basis": "raw field mapping is not independent market truth",
            },
            timeliness={
                "status": "UNKNOWN",
                "basis": "historical publication/delivery evidence unavailable",
            },
            independent_granularity={
                "status": "UNVERIFIED",
                "basis": "same endpoint repeat is not an independent source",
            },
        )
        doc = {
            "schema": "candle-audit-collection-v1",
            "request_id": request.request_id,
            "request": request.as_dict(),
            "status": "COMPLETE",
            "rows": count,
            "shards": shards,
            "quality": quality,
            "page_head": head,
            "source_verification": "UNVERIFIED",
            "admission": "VERIFY",
        }
        identity = (
            self.artifacts.put(canonical(doc))
            if saved is None
            else hashlib.sha256(canonical(doc)).hexdigest()
        )
        return CollectionResult(
            identity, request.request_id, "COMPLETE", count, tuple(shards), quality, doc
        )

    def read(self, collection_id: str) -> CollectionResult:
        doc = decode_document(self.artifacts.get(collection_id))
        try:
            request = CollectionRequest.from_dict(doc["request"])
            actual = self._materialize(request, doc["page_head"], saved=doc)
            if actual.collection_id != collection_id:
                raise DataLayerError(
                    "saved collection contradicts replayed original page/shard semantics"
                )
            return actual
        except (ValueError, KeyError, TypeError) as exc:
            raise DataLayerError("invalid complete collection") from exc
