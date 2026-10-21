"""Immutable dataset releases, signed external admission, and the unified read-only consumer."""

import hashlib
import importlib.metadata
import re
import time
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from candle_audit.candle_reconcile import MAX_BYTES, MAX_ROWS, strict_json

from ._input import bounded
from .artifacts import Artifacts, digest, identifier
from .collection import CollectionRequest, CollectionResult, Collector
from .model import DataLayerError, Window, timestamp_ns
from .reference import Candle
from .store import canonical, decode_document

_INTEGER_FIELDS = {"open_time_ns", "close_time_ns", "event_time_ns", "acquired_ns", "published_ns"}
_NULLABLE_FIELDS = {"event_time_ns", "published_ns", "contracts_qty"}
CANDLE_SCHEMA = pa.schema(
    [
        pa.field(
            name,
            pa.int64() if name in _INTEGER_FIELDS else pa.string(),
            nullable=name in _NULLABLE_FIELDS,
        )
        for name in Candle.__dataclass_fields__
    ],
    metadata={
        b"candle_audit": canonical(
            {
                "schema": "candle-audit-candle-v1",
                "time": "UTC integer ns",
                "window": "opening [start,end)",
                "price_unit": "quote/base",
                "base_qty": "base_asset",
                "quote_qty": "quote_asset",
                "contracts_qty": "contracts",
                "number_truth": "original native decimal text",
                "event_time": "UNKNOWN unless separately evidenced",
                "published_time": "UNKNOWN unless externally attested",
            }
        )
    },
)


def code_identity() -> dict[str, Any]:
    root = Path(__file__).resolve().parents[1]
    files = sorted(
        [
            *(root / "data_layer").glob("*.py"),
            *(root / "contract").glob("*.py"),
            root / "candle_quality.py",
            root / "candle_reconcile.py",
            root / "__init__.py",
        ]
    )
    return {
        "files": {
            p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in files
        },
        "dependencies": {
            n: importlib.metadata.version(n)
            for n in ("ccxt", "pyarrow", "duckdb", "pandas", "numpy", "cryptography")
        },
    }


def _freeze_code(artifacts: Artifacts) -> dict[str, Any]:
    identity = code_identity()
    root = Path(__file__).resolve().parents[1]
    for relative, sha in identity["files"].items():
        if artifacts.put((root / relative).read_bytes()) != sha:
            raise DataLayerError("source changed while freezing code identity")
    return identity


def _verify_code(artifacts: Artifacts, identity: object) -> None:
    if (
        not isinstance(identity, dict)
        or set(identity) != {"files", "dependencies"}
        or not isinstance(identity["files"], dict)
        or not 1 <= len(identity["files"]) <= 64
    ):
        raise DataLayerError("invalid frozen code/dependency lineage")
    for relative, sha in identity["files"].items():
        if not isinstance(relative, str) or not re.fullmatch(
            r"(?:(?:data_layer|contract)/)?[A-Za-z_][A-Za-z0-9_]*\.py", relative
        ):
            raise DataLayerError("invalid frozen source member")
        if not artifacts.get(digest(sha)):
            raise DataLayerError("missing frozen source bytes")
    actual = code_identity()["dependencies"]
    if identity["dependencies"] != actual:
        raise DataLayerError(
            "frozen dependency runtime mismatch; explicit replay environment required"
        )


def _packet(value: object, keys: set[str]) -> dict[str, Any]:
    document = decode_document(bounded(canonical(value), 262144, "signed packet"))
    if set(document) != keys:
        raise DataLayerError("exact signed packet schema required")
    return document


def _signature(value: object) -> bytes:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{128}", value):
        raise DataLayerError("exact detached Ed25519 signature encoding required")
    return bytes.fromhex(value)


@dataclass(frozen=True)
class Release:
    release_id: str
    document: dict[str, Any]


@dataclass(frozen=True)
class Principal:
    public_key: bytes = field(repr=False)
    roles: tuple[str, ...]

    def __post_init__(self) -> None:
        if (
            not isinstance(self.public_key, bytes)
            or len(self.public_key) != 32
            or not isinstance(self.roles, tuple)
            or not self.roles
            or set(self.roles) - {"AUDITOR", "OWNER"}
        ):
            raise DataLayerError("explicit Ed25519 principal key and allowed roles required")
        Ed25519PublicKey.from_public_bytes(self.public_key)


class TrustPolicy:
    """Owner-provisioned trust root; never populated from a dataset or approval packet."""

    def __init__(self, principals: Mapping[str, Principal]) -> None:
        if not isinstance(principals, Mapping) or not 1 <= len(principals) <= 64:
            raise DataLayerError("bounded explicit trust registry required")
        self._principals = {identifier(k): v for k, v in principals.items()}
        if not all(isinstance(p, Principal) for p in self._principals.values()):
            raise DataLayerError("explicit principals required")
        keys = [p.public_key for p in self._principals.values()]
        if len(keys) != len(set(keys)):
            raise DataLayerError("independent actors cannot share a signing key")
        self.policy_id = hashlib.sha256(
            canonical(
                {
                    k: {"public_key": v.public_key.hex(), "roles": v.roles}
                    for k, v in self._principals.items()
                }
            )
        ).hexdigest()

    def verify(self, principal: str, role: str, payload: object, signature: bytes) -> None:
        identifier(principal)
        actor = self._principals.get(principal)
        if (
            actor is None
            or role not in actor.roles
            or not isinstance(signature, bytes)
            or len(signature) != 64
        ):
            raise DataLayerError("unknown principal/role or invalid detached signature")
        try:
            Ed25519PublicKey.from_public_bytes(actor.public_key).verify(
                signature, canonical(payload)
            )
        except InvalidSignature as exc:
            raise DataLayerError("detached approval signature verification failed") from exc


class AsOfUnavailable(DataLayerError):
    """The requested visibility needs unavailable historical knowledge evidence."""


def canonical_table(
    collector: Collector, collection: CollectionResult, shard: dict[str, Any]
) -> pa.Table:
    request = CollectionRequest.from_dict(collection.document["request"])
    mapping = decode_document(collector.artifacts.get(shard["mapping_sha256"]))["rows"]
    if (
        not isinstance(mapping, list)
        or len(mapping) != shard["rows"]
        or not 2 <= len(mapping) <= MAX_ROWS
    ):
        raise DataLayerError("canonical mapping row budget/identity mismatch")
    raw_pages: dict[str, Any] = {}
    bars = []
    for item in mapping:
        sha = item["page_sha256"]
        if sha not in raw_pages:
            raw_pages[sha] = strict_json(collector.artifacts.get(sha))["data"]
        index = item["native_row"]
        if type(index) is not int or not 0 <= index < len(raw_pages[sha]):
            raise DataLayerError("original native row locator invalid")
        row = raw_pages[sha][index]
        rule = request.rule_at(int(row[0]) * 1_000_000)
        bars.append(Candle.from_native(row, rule, acquired_ns=item["acquired_ns"]).as_dict())
    bars.sort(key=lambda b: b["open_time_ns"])
    return pa.Table.from_pylist(bars, schema=CANDLE_SCHEMA)


def _read_canonical(content: bytes, expected: pa.Table) -> pa.Table:
    """Check encoded and dense budgets while retaining untrusted string dictionaries."""
    try:
        bounded(content, MAX_BYTES, "canonical Parquet")
        meta = pq.ParquetFile(pa.BufferReader(content)).metadata
        if (
            meta.num_rows != expected.num_rows
            or not 2 <= meta.num_rows <= MAX_ROWS
            or meta.num_row_groups > MAX_ROWS
            or not meta.schema.to_arrow_schema().equals(CANDLE_SCHEMA, check_metadata=True)
        ):
            raise DataLayerError("canonical Parquet schema/count mismatch")
        encoded = sum(meta.row_group(i).total_byte_size for i in range(meta.num_row_groups))
        if encoded + meta.num_rows * meta.num_columns * 16 > 2 * MAX_BYTES:
            raise DataLayerError("canonical Parquet encoded/decoding budget exceeded")
        names = [f.name for f in CANDLE_SCHEMA if pa.types.is_string(f.type)]
        table = pq.read_table(pa.BufferReader(content), read_dictionary=names, use_threads=False)
        dense = meta.num_rows * (len(_INTEGER_FIELDS) * 8 + len(CANDLE_SCHEMA))
        for name in names:
            column = table[name]
            dense += (meta.num_rows + 1) * 4
            for chunk in column.chunks:
                dictionary = chunk.dictionary if pa.types.is_dictionary(chunk.type) else chunk
                for value in dictionary:
                    text = value.as_py()
                    if text is not None and (not isinstance(text, str) or len(text) > 240):
                        raise DataLayerError(
                            "canonical string cell budget exceeded before expansion"
                        )
                for value in chunk:
                    text = value.as_py()
                    if text is not None:
                        dense += len(text.encode("utf-8"))
        if dense > 2 * MAX_BYTES:
            raise DataLayerError("canonical dense UTF-8 budget exceeded before expansion")
        result = table.cast(CANDLE_SCHEMA)
        if not result.equals(expected, check_metadata=True):
            raise DataLayerError("canonical Parquet contradicts original native-row semantics")
        return result
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, OSError) as exc:
        raise DataLayerError("invalid canonical Parquet") from exc


class ReleaseStore:
    def __init__(self, collector: Collector, *, trust_policy: TrustPolicy | None = None) -> None:
        self.collector = collector
        self.artifacts: Artifacts = collector.artifacts
        self.trust_policy = trust_policy

    def _collections(
        self, ids: Sequence[str]
    ) -> tuple[list[CollectionResult], list[dict[str, Any]]]:
        if (
            not isinstance(ids, (list, tuple))
            or not 1 <= len(ids) <= 4096
            or len(set(ids)) != len(ids)
        ):
            raise DataLayerError("explicit unique bounded collection set required")
        collections = [self.collector.read(digest(value)) for value in sorted(ids)]
        streams: list[dict[str, Any]] = []
        for collection in collections:
            req = CollectionRequest.from_dict(collection.document["request"])
            streams.append(
                {
                    "collection_id": collection.collection_id,
                    "request_id": req.request_id,
                    "source": req.source,
                    "timeframe": req.timeframe,
                    "price_kind": req.rules[0].price_kind,
                    "start_ns": req.window.start_ns,
                    "end_ns": req.window.end_ns,
                    "rows": collection.rows,
                    "rule_ids": [r.rule_id for r in req.rules],
                }
            )
        ordered = sorted(
            streams, key=lambda x: (x["source"]["instId"], x["timeframe"], x["start_ns"])
        )
        for left, right in zip(ordered, ordered[1:], strict=False):
            if (left["source"]["instId"], left["timeframe"]) == (
                right["source"]["instId"],
                right["timeframe"],
            ) and left["end_ns"] > right["start_ns"]:
                raise DataLayerError("overlapping collection windows need an explicit new revision")
        return collections, streams

    def create(
        self,
        collection_ids: Sequence[str],
        *,
        name: str,
        producer_id: str,
        parent_id: str | None = None,
        reason: str | None = None,
    ) -> Release:
        identifier(name)
        identifier(producer_id)
        if parent_id is not None:
            self.load(parent_id)
            if not isinstance(reason, str) or not 1 <= len(reason) <= 240:
                raise DataLayerError("bounded explicit revision reason required")
        elif reason is not None:
            raise DataLayerError("revision reason requires parent identity")
        collections, streams = self._collections(collection_ids)
        partitions = []
        for collection in collections:
            for shard in collection.shards:
                table = canonical_table(self.collector, collection, shard)
                out = pa.BufferOutputStream()
                pq.write_table(table, out, compression="zstd")
                content = bounded(out.getvalue().to_pybytes(), MAX_BYTES, "canonical Parquet")
                _read_canonical(content, table)
                partitions.append(
                    shard
                    | {
                        "collection_id": collection.collection_id,
                        "canonical_parquet_sha256": self.artifacts.put(content),
                    }
                )
        doc = {
            "schema": "candle-audit-dataset-release-v1",
            "name": name,
            "producer_id": producer_id,
            "created_ns": time.time_ns(),
            "parent_id": parent_id,
            "revision_reason": reason,
            "requested_collections": sorted(collection_ids),
            "streams": streams,
            "partitions": partitions,
            "lineage": {
                "code": _freeze_code(self.artifacts),
                "collections": {c.collection_id: c.document for c in collections},
            },
            "quality": {c.collection_id: c.quality for c in collections},
            "admission": "VERIFY",
            "source_verification": "UNVERIFIED",
            "publication_truth": "UNKNOWN",
        }
        identity = self.artifacts.put(canonical(doc))
        return Release(identity, doc)

    def extend(
        self, parent_id: str, new_collections: Sequence[str], *, name: str, producer_id: str
    ) -> Release:
        parent = self.load(parent_id)
        if (
            parent.document["schema"] != "candle-audit-dataset-release-v1"
            or not isinstance(new_collections, (list, tuple))
            or not new_collections
        ):
            raise DataLayerError(
                "incremental extension requires a native parent and explicit new windows"
            )
        return self.create(
            [*parent.document["requested_collections"], *new_collections],
            name=name,
            producer_id=producer_id,
            parent_id=parent_id,
            reason="explicit incremental window extension",
        )

    def load(self, release_id: str) -> Release:
        digest(release_id)
        try:
            doc = decode_document(self.artifacts.get(release_id))
            if doc.get("schema") == "candle-audit-derived-release-v1":
                return self._load_derived(release_id, doc)
            if (
                set(doc)
                != {
                    "schema",
                    "name",
                    "producer_id",
                    "created_ns",
                    "parent_id",
                    "revision_reason",
                    "requested_collections",
                    "streams",
                    "partitions",
                    "lineage",
                    "quality",
                    "admission",
                    "source_verification",
                    "publication_truth",
                }
                or doc["schema"] != "candle-audit-dataset-release-v1"
                or doc["admission"] != "VERIFY"
                or doc["source_verification"] != "UNVERIFIED"
                or doc["publication_truth"] != "UNKNOWN"
            ):
                raise DataLayerError("invalid candidate release declaration")
            identifier(doc["name"])
            identifier(doc["producer_id"])
            timestamp_ns(doc["created_ns"])
            if doc["parent_id"] is not None:
                digest(doc["parent_id"])
                if (
                    not isinstance(doc["revision_reason"], str)
                    or not 1 <= len(doc["revision_reason"]) <= 240
                ):
                    raise DataLayerError("invalid revision lineage")
            collections, streams = self._collections(doc["requested_collections"])
            if set(doc["lineage"]) != {"code", "collections"}:
                raise DataLayerError("invalid native lineage schema")
            _verify_code(self.artifacts, doc["lineage"]["code"])
            if (
                doc["streams"] != streams
                or doc["quality"] != {c.collection_id: c.quality for c in collections}
                or doc["lineage"]["collections"]
                != {c.collection_id: c.document for c in collections}
            ):
                raise DataLayerError("release lineage/quality contradicts actual collections")
            expected = [
                s | {"collection_id": c.collection_id} for c in collections for s in c.shards
            ]
            if len(expected) != len(doc["partitions"]):
                raise DataLayerError("release partition set mismatch")
            by_id = {c.collection_id: c for c in collections}
            for wanted, partition in zip(expected, doc["partitions"], strict=True):
                if {
                    k: v for k, v in partition.items() if k != "canonical_parquet_sha256"
                } != wanted:
                    raise DataLayerError("release full partition/window set mismatch")
                table = canonical_table(
                    self.collector, by_id[partition["collection_id"]], partition
                )
                _read_canonical(self.artifacts.get(partition["canonical_parquet_sha256"]), table)
                if (
                    table["acquired_ns"].to_pylist()
                    and max(table["acquired_ns"].to_pylist()) > doc["created_ns"]
                ):
                    raise DataLayerError("release predates actual acquisition")
            return Release(release_id, doc)
        except (ValueError, KeyError, TypeError, IndexError) as exc:
            raise DataLayerError("invalid release semantics") from exc

    def aggregate(
        self,
        source_release_id: str,
        target_timeframe: str,
        window: Window,
        *,
        name: str,
        producer_id: str,
        source_timeframes: Sequence[str] | None = None,
    ) -> Release:
        source = self.load(source_release_id)
        if source.document["schema"] != "candle-audit-dataset-release-v1":
            raise DataLayerError(
                "derive directly from a native release; nested derived chains are unsupported"
            )
        identifier(name)
        identifier(producer_id)
        result = self._aggregate_native(source, target_timeframe, window, source_timeframes)
        if result.report["status"] != "COMPLETE" or result.table.num_rows < 2:
            raise DataLayerError(
                "only complete, observed aggregate windows "
                "with at least two output bars can release"
            )
        out = pa.BufferOutputStream()
        pq.write_table(result.table, out, compression="zstd")
        content = out.getvalue().to_pybytes()
        _read_canonical(content, result.table)
        doc = {
            "schema": "candle-audit-derived-release-v1",
            "name": name,
            "producer_id": producer_id,
            "created_ns": time.time_ns(),
            "parent_id": source_release_id,
            "derivation": {
                "source_release_id": source_release_id,
                "target_timeframe": target_timeframe,
                "source_timeframes": list(source_timeframes)
                if source_timeframes is not None
                else None,
                "window": {"start_ns": window.start_ns, "end_ns": window.end_ns},
            },
            "canonical_parquet_sha256": self.artifacts.put(content),
            "report": result.report,
            "lineage": source.document["lineage"]
            | {"derivation_code": _freeze_code(self.artifacts)},
            "quality": source.document["quality"],
            "admission": "VERIFY",
            "source_verification": "UNVERIFIED",
        }
        return Release(self.artifacts.put(canonical(doc)), doc)

    def _aggregate_native(
        self,
        source: Release,
        target_timeframe: str,
        window: Window,
        source_timeframes: Sequence[str] | None,
    ) -> Any:
        """Keep one bounded native partition and one closed bucket, never the whole source."""
        from .aggregation import AggregateResult, aggregate_candles

        if not isinstance(window, Window) or (
            source_timeframes is not None
            and (
                not isinstance(source_timeframes, (tuple, list))
                or not source_timeframes
                or len(source_timeframes) > 64
                or not all(isinstance(t, str) for t in source_timeframes)
                or len(set(source_timeframes)) != len(source_timeframes)
            )
        ):
            raise DataLayerError(
                "explicit bounded aggregation window/timeframe selections required"
            )
        target_step = window.aligned_step(target_timeframe)
        selection = self.reader()._selection(source, window, None, source_timeframes)
        if selection["unmatched_timeframes"]:
            raise DataLayerError(
                "requested source timeframes absent from native release: "
                + ", ".join(selection["unmatched_timeframes"])
            )
        eligible = []
        excluded = [
            stream | {"reason": "SOURCE_TIMEFRAME_FILTER"}
            for stream in self.reader()._selection(source, window, None, None)["streams"]
            if source_timeframes is not None and stream["timeframe"] not in source_timeframes
        ]
        for stream in selection["streams"]:
            if any(
                c["start_ns"] < window.end_ns and c["end_ns"] > window.start_ns
                for c in stream["coverage"]
            ):
                eligible.append(stream)
            else:
                excluded.append(stream | {"reason": "OUTSIDE_TRANSFORM_WINDOW"})
        selection = selection | {"streams": eligible}
        streams = [(s["instrument"], s["timeframe"]) for s in eligible]
        if len({instrument for instrument, _ in streams}) != len(streams):
            raise DataLayerError("select one source timeframe per instrument before aggregation")
        count = len(streams) * ((window.end_ns - window.start_ns) // target_step)
        if count > MAX_ROWS:
            raise DataLayerError("derived output row budget exceeded")
        if count < 2:
            raise DataLayerError("complete aggregate release requires at least two output bars")
        output: list[dict[str, Any]] = []
        report: dict[str, Any] = {}
        assessed_counts: dict[tuple[str, str], int] = {}
        string_names = [f.name for f in CANDLE_SCHEMA if pa.types.is_string(f.type)]
        dense_bytes = len(string_names) * 4
        for instrument, timeframe in streams:
            source_step = window.aligned_step(timeframe)
            if target_step <= source_step or target_step % source_step:
                raise DataLayerError(
                    "target must be an integer coarser multiple of the source timeframe"
                )
            if target_step // source_step > MAX_ROWS:
                raise DataLayerError("closed bucket source row budget exceeded")
            rows = (
                row
                for table in self.reader()._tables(source, window, (instrument,), (timeframe,))
                for row in table.to_pylist()
                if window.start_ns <= row["open_time_ns"] < window.end_ns
            )
            row = next(rows, None)
            for opening in range(window.start_ns, window.end_ns, target_step):
                bucket = []
                while row is not None and row["open_time_ns"] < opening + target_step:
                    bucket.append(row)
                    if len(bucket) > MAX_ROWS:
                        raise DataLayerError("closed bucket source row budget exceeded")
                    row = next(rows, None)
                bucket_window = Window(opening, opening + target_step)
                data = DatasetResult(
                    pa.Table.from_pylist(bucket, schema=CANDLE_SCHEMA),
                    source.release_id,
                    bucket_window,
                    "VERIFY",
                    source.release_id,
                    None,
                    source.document["quality"],
                )
                result = aggregate_candles(data, target_timeframe, bucket_window)
                if result.report["status"] != "COMPLETE" or result.table.num_rows != 1:
                    raise DataLayerError("only complete, observed aggregate buckets can release")
                bar = result.table.to_pylist()[0]
                dense_bytes += len(_INTEGER_FIELDS) * 8 + len(CANDLE_SCHEMA) + len(string_names) * 4
                dense_bytes += sum(
                    len(bar[n].encode("utf-8")) for n in string_names if bar[n] is not None
                )
                if dense_bytes > 2 * MAX_BYTES:
                    raise DataLayerError("derived output dense UTF-8 budget exceeded")
                output.append(bar)
                report = result.report
                assessed_counts[(instrument, timeframe)] = assessed_counts.get(
                    (instrument, timeframe), 0
                ) + len(bucket)
        report = report | {
            "scope": stream_scope(selection, window, assessed_counts)
            | {"excluded_streams": excluded, "basis": "window-scoped native transformation"}
        }
        return AggregateResult(
            pa.Table.from_pylist(output, schema=CANDLE_SCHEMA), report, source.release_id
        )

    def _load_derived(self, release_id: str, doc: dict[str, Any]) -> Release:
        if (
            set(doc)
            != {
                "schema",
                "name",
                "producer_id",
                "created_ns",
                "parent_id",
                "derivation",
                "canonical_parquet_sha256",
                "report",
                "lineage",
                "quality",
                "admission",
                "source_verification",
            }
            or doc["admission"] != "VERIFY"
            or doc["source_verification"] != "UNVERIFIED"
        ):
            raise DataLayerError("invalid derived release schema")
        identifier(doc["name"])
        identifier(doc["producer_id"])
        timestamp_ns(doc["created_ns"])
        params = doc["derivation"]
        source_id = digest(params["source_release_id"])
        source_doc = decode_document(self.artifacts.get(source_id))
        if source_doc.get("schema") != "candle-audit-dataset-release-v1" or doc["parent_id"] != source_id:
            raise DataLayerError("derived parent must be a native dataset release")
        source = self.load(source_id)
        _verify_code(self.artifacts, doc["lineage"]["derivation_code"])
        window = Window(**params["window"])
        actual = self._aggregate_native(
            source, params["target_timeframe"], window, params["source_timeframes"]
        )
        # Original V1 producers stored the same replayable business report without scope.
        # Compare that exact format, while always replaying the current window-scoped source.
        expected_report = actual.report
        if "scope" not in doc["report"]:
            expected_report = {k: v for k, v in expected_report.items() if k != "scope"}
        if (
            expected_report != doc["report"]
            or actual.report["status"] != "COMPLETE"
            or doc["quality"] != source.document["quality"]
            or {k: v for k, v in doc["lineage"].items() if k != "derivation_code"}
            != source.document["lineage"]
            or doc["created_ns"] < source.document["created_ns"]
        ):
            raise DataLayerError("derived report/lineage contradicts retained source")
        _read_canonical(self.artifacts.get(doc["canonical_parquet_sha256"]), actual.table)
        return Release(release_id, doc)

    def catalog(self, release_id: str) -> dict[str, Any]:
        release = self.load(release_id)
        derived = release.document["schema"] == "candle-audit-derived-release-v1"
        keys = (
            ("derivation", "canonical_parquet_sha256", "report")
            if derived
            else (
                "requested_collections",
                "streams",
                "partitions",
            )
        )
        return {"release_id": release_id, "kind": "DERIVED" if derived else "NATIVE"} | {
            k: release.document[k]
            for k in (
                "schema",
                "lineage",
                "quality",
                "created_ns",
                "parent_id",
                *keys,
            )
        }

    def reader(self) -> "DatasetReader":
        return DatasetReader(self)

    def _policy(self) -> TrustPolicy:
        if not isinstance(self.trust_policy, TrustPolicy):
            raise DataLayerError("external trust root is not commissioned")
        return self.trust_policy

    def review_payload(
        self, release_id: str, *, auditor_id: str, evidence: Mapping[str, str]
    ) -> dict[str, Any]:
        release = self.load(release_id)
        if any(
            c["request"]["kind"] != "historical"
            for c in release.document["lineage"]["collections"].values()
        ):
            raise DataLayerError("synthetic engineering data cannot become RESEARCH")
        identifier(auditor_id)
        if auditor_id == release.document["producer_id"]:
            raise DataLayerError("producer cannot audit its own release")
        if not isinstance(evidence, Mapping) or set(evidence) not in (
            {"source", "full_window", "independent", "rules", "license"},
            {"source", "full_window", "independent", "rules", "license", "publication"},
        ):
            raise DataLayerError("complete external commissioning evidence required")
        for sha in evidence.values():
            if not self.artifacts.get(digest(sha)):
                raise DataLayerError("empty commissioning evidence")
        return {
            "schema": "candle-audit-external-review-v1",
            "release_id": release_id,
            "lineage_sha256": hashlib.sha256(canonical(release.document["lineage"])).hexdigest(),
            "auditor_id": auditor_id,
            "evidence": dict(evidence),
            "verdict": "EXTERNALLY_ATTESTED",
            "policy_id": self._policy().policy_id,
        }

    def record_review(self, payload: object, signature: bytes) -> dict[str, Any]:
        payload = _packet(
            payload,
            {
                "schema",
                "release_id",
                "lineage_sha256",
                "auditor_id",
                "evidence",
                "verdict",
                "policy_id",
            },
        )
        expected = self.review_payload(
            payload["release_id"], auditor_id=payload["auditor_id"], evidence=payload["evidence"]
        )
        if payload != expected:
            raise DataLayerError("review object/lineage/evidence mismatch")
        self._policy().verify(payload["auditor_id"], "AUDITOR", payload, signature)
        envelope = {"payload": payload, "signature": signature.hex()}
        sha = self.artifacts.put(canonical(envelope))
        return envelope | {"review_id": sha}

    def acceptance_payload(
        self, release_id: str, review: object, *, owner_id: str
    ) -> dict[str, Any]:
        if isinstance(review, dict) and set(review) == {"payload", "signature", "review_id"}:
            review = {k: review[k] for k in ("payload", "signature")}
        review = _packet(review, {"payload", "signature"})
        review["payload"] = _packet(
            review["payload"],
            {
                "schema",
                "release_id",
                "lineage_sha256",
                "auditor_id",
                "evidence",
                "verdict",
                "policy_id",
            },
        )
        release = self.load(release_id)
        expected = self.review_payload(
            release_id,
            auditor_id=review["payload"]["auditor_id"],
            evidence=review["payload"]["evidence"],
        )
        if expected != review["payload"]:
            raise DataLayerError("review does not bind accepted object")
        signature = _signature(review["signature"])
        self._policy().verify(expected["auditor_id"], "AUDITOR", expected, signature)
        envelope = {k: review[k] for k in ("payload", "signature")}
        review_id = hashlib.sha256(canonical(envelope)).hexdigest()
        if self.artifacts.get(review_id) != canonical(envelope):
            raise DataLayerError("review artifact mismatch")
        identifier(owner_id)
        if owner_id in (release.document["producer_id"], expected["auditor_id"]):
            raise DataLayerError("producer/auditor/owner must be distinct")
        return {
            "schema": "candle-audit-owner-acceptance-v1",
            "release_id": release_id,
            "lineage_sha256": expected["lineage_sha256"],
            "review_id": review_id,
            "owner_id": owner_id,
            "decision": "ACCEPT_RESEARCH",
            "policy_id": self._policy().policy_id,
        }

    def admit(self, acceptance: object, signature: bytes) -> str:
        acceptance = _packet(
            acceptance,
            {
                "schema",
                "release_id",
                "lineage_sha256",
                "review_id",
                "owner_id",
                "decision",
                "policy_id",
            },
        )
        review = decode_document(self.artifacts.get(acceptance["review_id"]))
        expected = self.acceptance_payload(
            acceptance["release_id"], review, owner_id=acceptance["owner_id"]
        )
        if acceptance != expected:
            raise DataLayerError("acceptance object/lineage/review mismatch")
        self._policy().verify(acceptance["owner_id"], "OWNER", acceptance, signature)
        sha = self.artifacts.put(canonical({"payload": acceptance, "signature": signature.hex()}))
        # Bind the pointer to the full release id; replay signed approval on every read.
        self.artifacts.save_state(acceptance["release_id"], {"approval_id": sha})
        return sha

    def approval(self, release_id: str) -> dict[str, Any]:
        pointer = self.artifacts.load_state(release_id)
        if pointer is None or set(pointer) != {"approval_id"}:
            raise DataLayerError("release is not admitted for RESEARCH")
        approval_id = pointer["approval_id"]
        envelope = _packet(
            decode_document(self.artifacts.get(approval_id)), {"payload", "signature"}
        )
        acceptance = _packet(
            envelope["payload"],
            {
                "schema",
                "release_id",
                "lineage_sha256",
                "review_id",
                "owner_id",
                "decision",
                "policy_id",
            },
        )
        review = decode_document(self.artifacts.get(acceptance["review_id"]))
        expected = self.acceptance_payload(release_id, review, owner_id=acceptance["owner_id"])
        if acceptance != expected:
            raise DataLayerError("saved approval object mismatch")
        self._policy().verify(
            acceptance["owner_id"], "OWNER", acceptance, _signature(envelope["signature"])
        )
        return {"approval_id": approval_id, "acceptance": acceptance, "review": review}


@dataclass(frozen=True)
class DatasetResult:
    table: Any
    release_id: str
    window: Window
    mode: str
    cache_key: str
    approval_id: str | None
    quality: dict[str, Any]
    visibility: dict[str, Any] = field(default_factory=dict)
    selection: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "release_id": self.release_id,
            "query_status": "EMPTY" if self.table.num_rows == 0 else "ROWS",
            "rows": self.table.to_pylist(),
            "schema": [
                {"name": f.name, "type": str(f.type), "nullable": f.nullable}
                for f in self.table.schema
            ],
            "admission": self.mode,
            "approval_id": self.approval_id,
            "cache_key": self.cache_key,
            "source_verification": "EXTERNALLY_ATTESTED"
            if self.mode == "RESEARCH"
            else "UNVERIFIED",
            "window": {"start_ns": self.window.start_ns, "end_ns": self.window.end_ns},
            "quality": self.quality,
            "visibility": self.visibility,
            "selection": self.selection,
        }


def stream_scope(
    selection: dict[str, Any], window: Window, counts: Mapping[tuple[str, str], int]
) -> dict[str, Any]:
    """Account for selected streams independently of which rows a consumer received."""
    known = "streams" in selection
    requested: list[dict[str, Any]] = (
        selection["streams"]
        if known
        else [{"instrument": i, "timeframe": t, "coverage": []} for i, t in sorted(counts)]
    )
    keys = {(s["instrument"], s["timeframe"]) for s in requested}
    if known and counts.keys() - keys:
        raise DataLayerError("consumer rows contradict the explicit selected stream universe")
    assessed, unavailable = [], []
    for stream in requested:
        key = stream["instrument"], stream["timeframe"]
        step = Window(0, 86400 * 10**9).aligned_step(key[1])
        expected = max(0, (window.end_ns - 1) // step - (window.start_ns + step - 1) // step + 1)
        count = counts.get(key, 0)
        item = stream | {"returned_count": count, "expected_count": expected}
        if count:
            assessed.append(item)
        if count < expected:
            unavailable.append(
                item | {"missing_count": expected - count, "market_gap_truth": "UNKNOWN"}
            )
    return {
        "universe": "SELECTED" if known else "UNKNOWN",
        "requested_window": {"start_ns": window.start_ns, "end_ns": window.end_ns},
        "requested_streams": requested,
        "assessed_streams": assessed,
        "unavailable_streams": unavailable,
        "unmatched_instruments": selection.get("unmatched_instruments", []),
        "unmatched_timeframes": selection.get("unmatched_timeframes", []),
        "basis": "selected input rows; no assertion of market completeness",
    }


def consumer_scope(data: DatasetResult, window: Window) -> dict[str, Any]:
    counts: dict[tuple[str, str], int] = {}
    for row in data.table.to_pylist():
        if window.start_ns <= row["open_time_ns"] < window.end_ns:
            key = row["instrument"], row["timeframe"]
            counts[key] = counts.get(key, 0) + 1
    return stream_scope(data.selection, window, counts)


class DatasetReader:
    def __init__(self, releases: ReleaseStore) -> None:
        self._releases = releases

    def _selection(
        self,
        release: Release,
        window: Window,
        instruments: Sequence[str] | None,
        timeframes: Sequence[str] | None,
    ) -> dict[str, Any]:
        streams: dict[tuple[str, str], list[dict[str, int]]] = {}
        if release.document["schema"] == "candle-audit-derived-release-v1":
            for table in self._tables(release, window, None, None):
                for row in table.to_pylist():
                    streams[(row["instrument"], row["timeframe"])] = [
                        release.document["derivation"]["window"]
                    ]
        else:
            for stream in release.document["streams"]:
                streams.setdefault((stream["source"]["instId"], stream["timeframe"]), []).append(
                    {"start_ns": stream["start_ns"], "end_ns": stream["end_ns"]}
                )
        selected: list[dict[str, Any]] = [
            {
                "instrument": key[0],
                "timeframe": key[1],
                "coverage": sorted(coverage, key=lambda c: c["start_ns"]),
            }
            for key, coverage in sorted(streams.items())
            if (instruments is None or key[0] in instruments)
            and (timeframes is None or key[1] in timeframes)
        ]
        return {
            "status": "RELEASE_STREAMS_SELECTED",
            "basis": "stored collection/derivation windows; market completeness is UNKNOWN",
            "instruments": list(instruments) if instruments is not None else None,
            "timeframes": list(timeframes) if timeframes is not None else None,
            "streams": selected,
            "unmatched_instruments": sorted(set(instruments or ()) - {k[0] for k in streams}),
            "unmatched_timeframes": sorted(set(timeframes or ()) - {k[1] for k in streams}),
        }

    def _tables(
        self,
        release: Release,
        window: Window,
        instruments: Sequence[str] | None,
        timeframes: Sequence[str] | None,
    ) -> Iterator[pa.Table]:
        if release.document["schema"] == "candle-audit-derived-release-v1":
            content = self._releases.artifacts.get(release.document["canonical_parquet_sha256"])
            # load() already replayed and bounded this exact immutable artifact.
            table = pq.read_table(pa.BufferReader(content), use_threads=False)
            if instruments is not None or timeframes is not None:
                rows = [
                    r
                    for r in table.to_pylist()
                    if (instruments is None or r["instrument"] in instruments)
                    and (timeframes is None or r["timeframe"] in timeframes)
                ]
                table = pa.Table.from_pylist(rows, schema=CANDLE_SCHEMA)
            yield table
            return
        stream_by_id = {s["collection_id"]: s for s in release.document["streams"]}
        for partition in sorted(
            release.document["partitions"],
            key=lambda p: (
                stream_by_id[p["collection_id"]]["source"]["instId"],
                stream_by_id[p["collection_id"]]["timeframe"],
                p["start_ns"],
            ),
        ):
            stream = stream_by_id[partition["collection_id"]]
            if (
                partition["end_ns"] <= window.start_ns
                or partition["start_ns"] >= window.end_ns
                or (instruments is not None and stream["source"]["instId"] not in instruments)
                or (timeframes is not None and stream["timeframe"] not in timeframes)
            ):
                continue
            collection_doc = release.document["lineage"]["collections"][partition["collection_id"]]
            collection = CollectionResult(
                partition["collection_id"],
                collection_doc["request_id"],
                "COMPLETE",
                collection_doc["rows"],
                tuple(collection_doc["shards"]),
                collection_doc["quality"],
                collection_doc,
            )
            expected = canonical_table(self._releases.collector, collection, partition)
            yield _read_canonical(
                self._releases.artifacts.get(partition["canonical_parquet_sha256"]), expected
            )

    def query(
        self,
        release_id: str,
        window: Window,
        *,
        instruments: Sequence[str] | None = None,
        timeframes: Sequence[str] | None = None,
        fields: Sequence[str] | None = None,
        limit: int = MAX_ROWS,
        mode: str = "VERIFY",
        as_of_ns: int | None = None,
        knowledge: str = "observed",
    ) -> DatasetResult:
        if (
            not isinstance(window, Window)
            or type(limit) is not int
            or not 1 <= limit <= MAX_ROWS
            or mode not in ("VERIFY", "RESEARCH")
            or knowledge not in ("observed", "published")
        ):
            raise DataLayerError("invalid typed reader request/budget/mode")
        for values in (instruments, timeframes, fields):
            if values is not None and (
                not isinstance(values, (tuple, list))
                or not values
                or len(values) > 64
                or not all(isinstance(v, str) for v in values)
                or len(set(values)) != len(values)
            ):
                raise DataLayerError("explicit unique bounded reader selections required")
        selected = CANDLE_SCHEMA.names if fields is None else list(fields)
        if any(f not in CANDLE_SCHEMA.names for f in selected):
            raise DataLayerError("fixed canonical projection allowlist required")
        release = self._releases.load(release_id)
        approval = self._releases.approval(release_id) if mode == "RESEARCH" else None
        if as_of_ns is not None:
            timestamp_ns(as_of_ns)
            if knowledge == "observed" and as_of_ns < release.document["created_ns"]:
                raise AsOfUnavailable(
                    "UNAVAILABLE: release did not exist at requested observed as-of"
                )
            if knowledge == "published" and (
                approval is None or "publication" not in approval["review"]["payload"]["evidence"]
            ):
                raise AsOfUnavailable(
                    "UNKNOWN: independently attested per-row publication evidence required"
                )
        elif knowledge == "published":
            raise DataLayerError("published visibility requires an explicit as-of instant")
        rows = []
        with duckdb.connect(
            ":memory:",
            config={
                "enable_external_access": False,
                "autoinstall_known_extensions": False,
                "autoload_known_extensions": False,
                "threads": 1,
                "memory_limit": "128MB",
            },
        ) as connection:
            for table in self._tables(release, window, instruments, timeframes):
                connection.register("bars", table)
                part = connection.execute(
                    "SELECT * FROM bars WHERE open_time_ns >= ? AND open_time_ns < ? "
                    "ORDER BY instrument,timeframe,open_time_ns LIMIT ?",
                    [
                        window.start_ns,
                        window.end_ns,
                        MAX_ROWS + 1 if knowledge == "published" else limit + 1,
                    ],
                ).to_arrow_table()
                visible = part.to_pylist()
                if knowledge == "published" and approval is not None and as_of_ns is not None:
                    visible = self._publication_rows(visible, release_id, approval, as_of_ns)
                rows.extend(visible)
                if len(rows) > limit:
                    raise DataLayerError(
                        "query row budget exceeded; narrow the window or stream set"
                    )
        rows.sort(key=lambda b: (b["instrument"], b["timeframe"], b["open_time_ns"]))
        table = pa.Table.from_pylist(rows, schema=CANDLE_SCHEMA).select(selected)
        query_identity = {
            "release_id": release_id,
            "lineage": release.document["lineage"],
            "window": [window.start_ns, window.end_ns],
            "instruments": instruments,
            "timeframes": timeframes,
            "fields": selected,
            "limit": limit,
            "mode": mode,
            "as_of_ns": as_of_ns,
            "knowledge": knowledge,
            "approval_id": approval["approval_id"] if approval else None,
        }
        # Expose the complete key for downstream caches; always revalidate this reader.
        key = hashlib.sha256(canonical(query_identity)).hexdigest()
        return DatasetResult(
            table,
            release_id,
            window,
            mode,
            key,
            approval["approval_id"] if approval else None,
            release.document["quality"],
            {
                "knowledge": knowledge,
                "as_of_ns": as_of_ns,
                "version_observed_ns": release.document["created_ns"],
                "historical_publication": "EXTERNALLY_ATTESTED"
                if knowledge == "published"
                else "UNKNOWN",
            },
            self._selection(release, window, instruments, timeframes),
        )

    def _publication_rows(
        self, rows: list[dict[str, Any]], release_id: str, approval: dict[str, Any], as_of_ns: int
    ) -> list[dict[str, Any]]:
        sha = approval["review"]["payload"]["evidence"]["publication"]
        doc = decode_document(self._releases.artifacts.get(sha))
        documents: Sequence[dict[str, Any]] | Iterator[dict[str, Any]] = [doc]
        if doc.get("schema") == "candle-audit-publication-manifest-v1":
            if (
                set(doc) != {"schema", "release_id", "row_shards"}
                or doc["release_id"] != release_id
                or not isinstance(doc["row_shards"], list)
                or not 1 <= len(doc["row_shards"]) <= 4096
            ):
                raise AsOfUnavailable("UNKNOWN: invalid attested publication manifest")

            def publication_documents() -> Iterator[dict[str, Any]]:
                total = 0
                for sha in doc["row_shards"]:
                    content = self._releases.artifacts.get(digest(sha))
                    total += len(content)
                    if total > 256 * 1024 * 1024:
                        raise AsOfUnavailable(
                            "UNKNOWN: publication evidence query byte budget exceeded"
                        )
                    yield decode_document(content)

            documents = publication_documents()
        wanted = {(r["instrument"], r["timeframe"], r["open_time_ns"]): r for r in rows}
        times: dict[tuple[str, str, int], int] = {}
        for document in documents:
            if (
                set(document) != {"schema", "release_id", "rows"}
                or document["schema"] != "candle-audit-publication-evidence-v1"
                or document["release_id"] != release_id
                or not isinstance(document["rows"], list)
                or len(document["rows"]) > MAX_ROWS
            ):
                raise AsOfUnavailable("UNKNOWN: invalid per-row publication evidence")
            for item in document["rows"]:
                if not isinstance(item, dict) or set(item) != {
                    "instrument",
                    "timeframe",
                    "open_time_ns",
                    "published_ns",
                }:
                    raise AsOfUnavailable("UNKNOWN: invalid publication locator")
                if not isinstance(item["instrument"], str) or not isinstance(
                    item["timeframe"], str
                ):
                    raise AsOfUnavailable("UNKNOWN: invalid publication instrument/timeframe")
                opening, published = (
                    timestamp_ns(item["open_time_ns"]),
                    timestamp_ns(item["published_ns"]),
                )
                key = (item["instrument"], item["timeframe"], opening)
                if key in wanted:
                    if (
                        key in times
                        or not wanted[key]["close_time_ns"]
                        <= published
                        <= wanted[key]["acquired_ns"]
                    ):
                        raise AsOfUnavailable(
                            "UNKNOWN: conflicting/unavailable publication evidence"
                        )
                    times[key] = published
        if set(times) != set(wanted):
            raise AsOfUnavailable("UNKNOWN: publication evidence does not cover requested rows")
        return [
            r | {"published_ns": times[key]} for key, r in wanted.items() if times[key] <= as_of_ns
        ]

    def revisions(self, before_id: str, after_id: str, window: Window) -> dict[str, Any]:
        after = self._releases.load(after_id)
        if after.document["parent_id"] != before_id:
            raise DataLayerError("revision must bind the explicit parent release")
        if after.document["schema"] == "candle-audit-derived-release-v1":
            return {
                "before_id": before_id,
                "after_id": after_id,
                "window": {"start_ns": window.start_ns, "end_ns": window.end_ns},
                "comparison": "NOT_APPLICABLE",
                "reason": "DERIVATION_NOT_REVISION",
            }
        before_rows = self.query(before_id, window).table.to_pylist()
        after_rows = self.query(after_id, window).table.to_pylist()
        before = self._releases.load(before_id)
        identity_fields = ["instrument", "timeframe", "open_time_ns"]
        observation_fields = ["event_time_ns", "acquired_ns", "published_ns"]
        compared_fields = [
            f for f in CANDLE_SCHEMA.names if f not in identity_fields + observation_fields
        ]
        declaration_fields = ["source", "volume_columns", "kind", "provenance"]

        def metadata(release: Release, *, reference: bool) -> dict[tuple[Any, ...], dict[str, Any]]:
            entries = {}
            for collection in release.document["lineage"]["collections"].values():
                req = collection["request"]
                values = req["rules"] if reference else [req]
                for item in values:
                    bounds = item["effective"] if reference else item["window"]
                    if bounds["end_ns"] <= window.start_ns or bounds["start_ns"] >= window.end_ns:
                        continue
                    source = item["source"] if reference else req["rules"][0]["source"]
                    timeframe = item["timeframe"] if reference else req["rules"][0]["timeframe"]
                    locator = (source["instId"], timeframe, bounds["start_ns"], bounds["end_ns"])
                    entries[locator] = (
                        {k: v for k, v in item.items() if k != "rule_evidence_hex"}
                        if reference
                        else {
                            "source": source,
                            **{k: req[k] for k in declaration_fields if k != "source"},
                        }
                    )
            return entries

        def metadata_changes(*, reference: bool) -> list[dict[str, Any]]:
            first, second = (
                metadata(before, reference=reference),
                metadata(after, reference=reference),
            )
            changes = []
            for point in sorted(first.keys() | second.keys()):
                left_value, right_value = first.get(point), second.get(point)
                if left_value != right_value:
                    fields = sorted(
                        k
                        for k in (left_value or {}).keys() | (right_value or {}).keys()
                        if (left_value or {}).get(k) != (right_value or {}).get(k)
                    )
                    changes.append(
                        {
                            "key": list(point),
                            "fields": fields,
                            "before": left_value,
                            "after": right_value,
                        }
                    )
            return changes

        def key(row: dict[str, Any]) -> tuple[str, str, int]:
            return row["instrument"], row["timeframe"], row["open_time_ns"]

        left, right = {key(r): r for r in before_rows}, {key(r): r for r in after_rows}
        changed = []
        for point in sorted(left.keys() & right.keys()):
            for name in compared_fields:
                if left[point][name] != right[point][name]:
                    changed.append(
                        {
                            "instrument": point[0],
                            "timeframe": point[1],
                            "open_time_ns": point[2],
                            "field": name,
                            "before": left[point][name],
                            "after": right[point][name],
                        }
                    )
        return {
            "before_id": before_id,
            "after_id": after_id,
            "window": {"start_ns": window.start_ns, "end_ns": window.end_ns},
            "identity_fields": identity_fields,
            "compared_fields": compared_fields,
            "non_compared_fields": observation_fields,
            "observation_policy": "event/acquisition/publication observations "
            "are not candle semantic changes",
            "reference_changes": metadata_changes(reference=True),
            "declaration_changes": metadata_changes(reference=False),
            "declaration_compared_fields": declaration_fields,
            "changed_fields": changed,
            "added": [list(k) for k in sorted(right.keys() - left.keys())],
            "removed": [list(k) for k in sorted(left.keys() - right.keys())],
        }
