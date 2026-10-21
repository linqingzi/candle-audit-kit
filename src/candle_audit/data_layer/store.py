"""Bounded engineering candidates: immutable source bytes, exact Parquet, atomic publication."""

import csv
import hashlib
import io
import json
import os
import re
import secrets
import stat
import sys
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from candle_audit import candle_quality, candle_reconcile
from candle_audit.candle_reconcile import MAX_BYTES, MAX_NUMBER, MAX_ROWS
from candle_audit.contract import data_versions
from candle_audit.contract.registry import RegistryError

from ._input import (
    _directory_descriptor,
    bounded,
    controlled_path,
    project_root,
)
from ._input import (
    read_bounded as read_bounded,
)
from .model import DataLayerError, RejectedCandidate, Window, validate_source

SCHEMA = "candle-audit-candidate-v1"
FILE_LIMITS = {
    "candidate.csv": MAX_BYTES,
    "raw.json": MAX_BYTES,
    "metadata.json": 65536,
    "source.json": 65536,
    "reports.json": 262144,
    "bars.parquet": 2 * MAX_BYTES,
}


def canonical(value: object) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        raise DataLayerError("bounded finite JSON declaration required") from exc


def decode_document(content: bytes) -> dict[str, Any]:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise DataLayerError("duplicate declaration JSON key")
            result[key] = value
        return result

    def invalid_constant(value: str) -> None:
        raise DataLayerError("nonfinite declaration JSON")

    try:
        value = json.loads(content, object_pairs_hook=pairs, parse_constant=invalid_constant)
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise DataLayerError("invalid declaration JSON") from exc
    if not isinstance(value, dict):
        raise DataLayerError("declaration must be a JSON object")
    return value


def _directory_matches(name: str, parent: int, descriptor: int) -> bool:
    try:
        current = os.stat(name, dir_fd=parent, follow_symlinks=False)
    except FileNotFoundError:
        return False
    held = os.fstat(descriptor)
    return stat.S_ISDIR(current.st_mode) and (current.st_dev, current.st_ino) == (
        held.st_dev,
        held.st_ino,
    )


def _create_file(name: str, descriptor: int) -> BinaryIO:
    return os.fdopen(
        os.open(
            name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            mode=0o600,
            dir_fd=descriptor,
        ),
        "wb",
    )


@contextmanager
def _temporary_directory(parent: int, prefix: str) -> Iterator["_TemporaryDirectory"]:
    name = prefix + secrets.token_hex(16)
    os.mkdir(name, mode=0o700, dir_fd=parent)
    with _directory_descriptor(Path(name), dir_fd=parent) as descriptor:
        temporary = _TemporaryDirectory(name, descriptor)
        try:
            yield temporary
        finally:
            # A renamed/replaced staging path cannot redirect deletion to another directory.
            if not temporary.retained:
                for leaf in os.listdir(descriptor):
                    os.unlink(leaf, dir_fd=descriptor)
                if _directory_matches(temporary.name, parent, descriptor):
                    os.rmdir(temporary.name, dir_fd=parent)


@dataclass
class _TemporaryDirectory:
    name: str
    descriptor: int
    retained: bool = False


def _assert_root(path: Path, descriptor: int) -> None:
    with _directory_descriptor(path) as current:
        held, found = os.fstat(descriptor), os.fstat(current)
        if (held.st_dev, held.st_ino) != (found.st_dev, found.st_ino):
            raise DataLayerError("controlled store directory was replaced")


def _kernel_path(descriptor: int) -> Path:
    """Frozen pathname readers reopen the held file inode with an independent offset."""
    if sys.platform == "darwin":
        held = os.fstat(descriptor)
        return Path(f"/.vol/{held.st_dev}/{held.st_ino}")
    return Path(f"/proc/self/fd/{descriptor}")


def _check_input_budget(candidate: bytes, raw: bytes) -> None:
    try:
        for index, row in enumerate(csv.reader(io.StringIO(candidate.decode("utf-8")))):
            if index > MAX_ROWS or len(row) > 8 or any(len(cell) > MAX_NUMBER for cell in row):
                raise DataLayerError("candidate row/cell budget exceeded")
        native = candle_reconcile.strict_json(raw)
        if (
            isinstance(native, dict)
            and isinstance(native.get("data"), list)
            and (len(native["data"]) > MAX_ROWS)
        ):
            raise DataLayerError("raw row budget exceeded")
    except (ValueError, UnicodeError, csv.Error, RecursionError) as exc:
        raise DataLayerError("invalid bounded CSV/native JSON input: " + str(exc)) from exc


def _table(raw: bytes, metadata: dict[str, Any], source: dict[str, Any], window: Window) -> Any:
    descriptors = metadata.get("volume", {}).get("columns", [])
    names = ["timestamp_ns", "open", "high", "low", "close"] + [d["column"] for d in descriptors]
    if len({name.casefold() for name in names}) != len(names):
        raise DataLayerError("volume column collides with normalized schema")
    declaration = {
        "metadata": metadata,
        "source": source,
        "source_verification": "UNVERIFIED",
        "admission": "NOT_EVALUATED",
        "time_truth": "UTC integer nanoseconds",
        "raw_time_unit": "ms",
        "number_truth": "native raw decimal text",
        "window": {"start_ns": window.start_ns, "end_ns": window.end_ns},
    }
    schema = pa.schema(
        [pa.field(names[0], pa.int64(), nullable=False)]
        + [pa.field(name, pa.string(), nullable=False) for name in names[1:]],
        metadata={b"candle_audit": canonical(declaration)},
    )
    indexes = {"base": 5 if source["instType"] == "SPOT" else 6, "quote": 7, "contracts": 5}
    rows = []
    for row in sorted(candle_reconcile.strict_json(raw)["data"], key=lambda r: int(r[0])):
        values = [
            int(row[0]) * 1_000_000,
            *row[1:5],
            *[row[indexes[d["unit"]]] for d in descriptors],
        ]
        rows.append(dict(zip(names, values, strict=True)))
    return pa.Table.from_pylist(rows, schema=schema)


def _verify_inputs(
    candidate: bytes, metadata: object, raw: bytes, source: object, window: Window
) -> tuple[dict[str, object], dict[str, str], dict[str, object]]:
    """Replay both frozen oracles on immutable captured bytes, without trusting saved reports."""
    try:
        declaration = data_versions.metadata(metadata)
    except RegistryError as exc:
        raise DataLayerError("invalid exact data declaration") from exc
    origin = validate_source(source, declaration["timeframe"], "ms")
    window.aligned_step(declaration["timeframe"])
    _check_input_budget(candidate, raw)
    with (
        _directory_descriptor(project_root()) as workspace,
        _directory_descriptor(Path(".tmp-data-layer"), dir_fd=workspace, create=True) as parent,
        _temporary_directory(parent, "semantic-") as temporary,
    ):
        descriptor = temporary.descriptor
        for name, content in (("candidate.csv", candidate), ("raw.json", raw)):
            with _create_file(name, descriptor) as stream:
                stream.write(content)
        start, end = window.iso_bounds()
        context = {
            key: origin[key] for key in ("venue", "instId", "instType", "base_asset", "quote_asset")
        }
        with (
            os.fdopen(
                os.open("candidate.csv", os.O_RDONLY | os.O_NOFOLLOW, dir_fd=descriptor), "rb"
            ) as csv_stream,
            os.fdopen(
                os.open("raw.json", os.O_RDONLY | os.O_NOFOLLOW, dir_fd=descriptor), "rb"
            ) as raw_stream,
        ):
            csv_path, raw_path = (
                _kernel_path(stream.fileno()) for stream in (csv_stream, raw_stream)
            )
            quality = candle_quality.validate(csv_path, declaration, context, start, end)
            reconcile = candle_reconcile.reconcile(
                csv_path, declaration, raw_path, origin, start, end
            )
    reports: dict[str, object] = {"quality": quality, "reconcile": reconcile}
    if (
        quality["structural_status"] != "PASS"
        or reconcile["comparison"] != "MATCH"
        or (
            reconcile["coverage"]["status"] != "COMPLETE"
            or reconcile["closedbar"]["status"] != "CLOSED"
        )
    ):
        raise RejectedCandidate(reports)
    return declaration, origin, reports


def _report_matches(saved: object, actual: object) -> bool:
    """Require every recomputed business value; descriptive details/extra notes are not gates."""
    if isinstance(actual, dict):
        return isinstance(saved, dict) and all(
            key in ("detail", "basis") or (key in saved and _report_matches(saved[key], value))
            for key, value in actual.items()
        )
    if isinstance(actual, list):
        return (
            isinstance(saved, list)
            and len(saved) == len(actual)
            and all(_report_matches(left, right) for left, right in zip(saved, actual, strict=True))
        )
    return type(saved) is type(actual) and saved == actual


def _read_parquet(content: bytes, expected: pa.Table, rows: object) -> pa.Table:
    """Keep byte-array columns dictionary encoded until their values satisfy the exact contract."""
    try:
        metadata = pq.ParquetFile(pa.BufferReader(content)).metadata
        if (
            type(rows) is not int
            or not 2 <= metadata.num_rows <= MAX_ROWS
            or (
                metadata.num_rows != rows
                or metadata.num_rows != expected.num_rows
                or metadata.num_row_groups > MAX_ROWS
            )
        ):
            raise DataLayerError("Parquet row budget/manifest mismatch before decoding")
        encoded = sum(metadata.row_group(i).total_byte_size for i in range(metadata.num_row_groups))
        if encoded > FILE_LIMITS["bars.parquet"]:
            raise DataLayerError("Parquet expanded byte budget exceeded before decoding")
        if not metadata.schema.to_arrow_schema().equals(expected.schema, check_metadata=True):
            raise DataLayerError("Parquet/raw declaration schema mismatch")
        # A repeated dictionary value must not be expanded once per row before it is checked.
        # Index/offset arrays add at most 16 bytes per row/column to the encoded representation.
        if encoded + metadata.num_rows * metadata.num_columns * 16 > FILE_LIMITS["bars.parquet"]:
            raise DataLayerError("Parquet dictionary decoding byte budget exceeded")
        encoded_table = pq.read_table(
            pa.BufferReader(content), read_dictionary=expected.column_names[1:], use_threads=False
        )
        for column in encoded_table.columns[1:]:
            for chunk in column.chunks:
                values = chunk.dictionary if pa.types.is_dictionary(chunk.type) else chunk
                for value in values:
                    text = value.as_py()
                    if not isinstance(text, str) or len(text) > MAX_NUMBER:
                        raise DataLayerError(
                            "Parquet decimal string budget exceeded before expansion"
                        )
        # The frozen decimal grammar also accepts Unicode digits: characters are not UTF-8 bytes.
        # Count actual dense offsets, bitmap reserve and string bytes while retaining dictionaries.
        dense_bytes = metadata.num_rows * 8
        for column in encoded_table.columns[1:]:
            for chunk in column.chunks:
                dense_bytes += (len(chunk) + 1) * 4 + (len(chunk) + 7) // 8
                for value in chunk:
                    text = value.as_py()
                    if not isinstance(text, str):
                        raise DataLayerError("Parquet requires nonnull decimal text")
                    dense_bytes += len(text.encode("utf-8"))
        if dense_bytes > FILE_LIMITS["bars.parquet"]:
            raise DataLayerError("Parquet dense UTF-8 byte budget exceeded before expansion")
        table = encoded_table.cast(expected.schema)
        if not table.equals(expected, check_metadata=True):
            raise DataLayerError("Parquet/raw/declaration roundtrip mismatch")
        return table
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, OSError) as exc:
        raise DataLayerError("invalid native Parquet metadata or data") from exc


@dataclass(frozen=True)
class Snapshot:
    snapshot_id: str
    path: Path
    reports: dict[str, Any]


@dataclass(frozen=True)
class QueryResult:
    table: Any
    snapshot_id: str
    window: Window
    metadata: dict[str, Any]
    source: dict[str, Any]
    reports: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "query_status": "EMPTY" if self.table.num_rows == 0 else "ROWS",
            "schema": [
                {"name": field.name, "type": str(field.type), "nullable": field.nullable}
                for field in self.table.schema
            ],
            "query_window": {
                "start_ns": self.window.start_ns,
                "end_ns": self.window.end_ns,
                "interval": "[start,end)",
            },
            "rows": self.table.to_pylist(),
            "metadata": self.metadata,
            "source": self.source,
            "source_verification": "UNVERIFIED",
            "admission": "NOT_EVALUATED",
            "reports": self.reports,
        }


class CandidateStore:
    """Candidates only. No research registry, cache, external database or acquisition proof."""

    def __init__(self, root: Path) -> None:
        self.root = controlled_path(root)
        workspace = project_root()
        with (
            _directory_descriptor(workspace) as descriptor,
            _directory_descriptor(self.root.relative_to(workspace), dir_fd=descriptor, create=True),
        ):
            pass

    def ingest(
        self,
        candidate_csv: bytes,
        metadata: dict[str, Any],
        raw: bytes,
        source: dict[str, Any],
        window: Window,
        *,
        raw_time_unit: str,
    ) -> Snapshot:
        if self.root.is_symlink() or controlled_path(self.root) != self.root:
            raise DataLayerError("controlled store directory was replaced")
        bounded(candidate_csv, MAX_BYTES, "candidate CSV")
        bounded(raw, MAX_BYTES, "native raw")
        if raw_time_unit != "ms":
            raise DataLayerError("NOT_SUPPORTED raw time unit; explicit ms required")
        mb = bounded(canonical(metadata), 65536, "metadata")
        sb = bounded(canonical(source), 65536, "source")
        metadata, source = decode_document(mb), decode_document(sb)
        source = validate_source(source, metadata.get("timeframe", ""), raw_time_unit)
        window.aligned_step(metadata.get("timeframe", ""))
        _check_input_budget(candidate_csv, raw)
        with (
            _directory_descriptor(self.root) as root_descriptor,
            _temporary_directory(root_descriptor, ".incoming-") as temporary,
        ):
            staging, descriptor = temporary.name, temporary.descriptor
            files = {
                "candidate.csv": candidate_csv,
                "raw.json": raw,
                "metadata.json": mb,
                "source.json": sb,
            }
            for name, content in files.items():
                with _create_file(name, descriptor) as stream:
                    stream.write(content)
            metadata, source, reports = _verify_inputs(candidate_csv, metadata, raw, source, window)
            table = _table(raw, metadata, source, window)
            with _create_file("bars.parquet", descriptor) as stream:
                pq.write_table(table, stream, compression="zstd")
            parquet = read_bounded(
                Path("bars.parquet"), FILE_LIMITS["bars.parquet"], dir_fd=descriptor
            )
            if not pq.read_table(pa.BufferReader(parquet)).equals(table, check_metadata=True):
                raise DataLayerError("Parquet roundtrip changed values/schema/metadata")
            with _create_file("reports.json", descriptor) as stream:
                stream.write(bounded(canonical(reports), 262144, "reports"))
            fingerprints = {}
            for name, limit in FILE_LIMITS.items():
                content = read_bounded(Path(name), limit, dir_fd=descriptor)
                fingerprints[name] = {
                    "sha256": hashlib.sha256(content).hexdigest(),
                    "bytes": len(content),
                }
            manifest = {
                "schema": SCHEMA,
                "rows": table.num_rows,
                "files": fingerprints,
                "window": {"start_ns": window.start_ns, "end_ns": window.end_ns},
                "source_verification": "UNVERIFIED",
                "admission": "NOT_EVALUATED",
            }
            snapshot_id = hashlib.sha256(canonical(manifest)).hexdigest()
            manifest["snapshot_id"] = snapshot_id
            with _create_file("manifest.json", descriptor) as stream:
                stream.write(canonical(manifest))
            final = self.root / snapshot_id
            _assert_root(self.root, root_descriptor)
            if not _directory_matches(staging, root_descriptor, descriptor):
                raise DataLayerError("controlled staging directory was replaced")
            try:
                os.stat(snapshot_id, dir_fd=root_descriptor, follow_symlinks=False)
            except FileNotFoundError:
                os.rename(
                    staging, snapshot_id, src_dir_fd=root_descriptor, dst_dir_fd=root_descriptor
                )
                temporary.name = snapshot_id
                if not _directory_matches(snapshot_id, root_descriptor, descriptor):
                    if stat.S_ISLNK(
                        os.stat(snapshot_id, dir_fd=root_descriptor, follow_symlinks=False).st_mode
                    ):
                        os.unlink(snapshot_id, dir_fd=root_descriptor)
                    raise DataLayerError(
                        "published directory does not match captured staging"
                    ) from None
            else:
                self._load(snapshot_id, dir_fd=root_descriptor)
            _assert_root(self.root, root_descriptor)
            temporary.retained = temporary.name == snapshot_id
            return Snapshot(snapshot_id, final, reports)

    def _load(
        self, snapshot_id: str, *, dir_fd: int | None = None
    ) -> tuple[dict[str, Any], dict[str, bytes]]:
        if not isinstance(snapshot_id, str) or not re.fullmatch(r"[0-9a-f]{64}", snapshot_id):
            raise DataLayerError("explicit lowercase SHA256 snapshot identity required")
        directory = self.root / snapshot_id
        if directory.is_symlink() or controlled_path(directory) != directory:
            raise DataLayerError("snapshot symlink/identity escape")
        try:
            with _directory_descriptor(
                directory if dir_fd is None else Path(snapshot_id), dir_fd=dir_fd
            ) as descriptor:
                manifest_bytes = read_bounded(directory / "manifest.json", 65536, dir_fd=descriptor)
                captured = {
                    name: read_bounded(directory / name, limit, dir_fd=descriptor)
                    for name, limit in FILE_LIMITS.items()
                }
            manifest = decode_document(manifest_bytes)
            if set(manifest) != {
                "schema",
                "rows",
                "files",
                "window",
                "source_verification",
                "admission",
                "snapshot_id",
            }:
                raise DataLayerError("invalid candidate manifest schema")
            identity = {k: v for k, v in manifest.items() if k != "snapshot_id"}
            if (
                manifest["schema"] != SCHEMA
                or manifest["snapshot_id"] != snapshot_id
                or (hashlib.sha256(canonical(identity)).hexdigest() != snapshot_id)
                or manifest["source_verification"] != "UNVERIFIED"
                or (manifest["admission"] != "NOT_EVALUATED")
                or not isinstance(manifest["files"], dict)
                or set(manifest["files"]) != set(FILE_LIMITS)
            ):
                raise DataLayerError("snapshot identity/content binding mismatch")
            files = {}
            for name in FILE_LIMITS:
                content = captured[name]
                expected = {"sha256": hashlib.sha256(content).hexdigest(), "bytes": len(content)}
                if manifest["files"][name] != expected:
                    raise DataLayerError("artifact integrity mismatch: " + name)
                files[name] = content
            declared_window = manifest["window"]
            if not isinstance(declared_window, dict) or set(declared_window) != {
                "start_ns",
                "end_ns",
            }:
                raise DataLayerError("invalid snapshot window schema")
            _, _, reports = _verify_inputs(
                files["candidate.csv"],
                decode_document(files["metadata.json"]),
                files["raw.json"],
                decode_document(files["source.json"]),
                Window(declared_window["start_ns"], declared_window["end_ns"]),
            )
            if not _report_matches(decode_document(files["reports.json"]), reports):
                raise DataLayerError("saved reports contradict actual captured input semantics")
            files["reports.json"] = canonical(reports)
            return manifest, files
        except OSError as exc:
            raise DataLayerError("snapshot is unavailable or contains unsafe files") from exc

    def query(
        self,
        snapshot_id: str,
        window: Window,
        *,
        fields: Sequence[str] | None = None,
        limit: int = MAX_ROWS,
    ) -> QueryResult:
        if type(limit) is not int or not 1 <= limit <= MAX_ROWS:
            raise DataLayerError("query row limit must be an integer within 1..10000")
        if fields is not None and not isinstance(fields, (list, tuple)):
            raise DataLayerError("query projection must be a list/tuple of allowed fields")
        return self._query_loaded(
            snapshot_id, window, self._load(snapshot_id), fields=fields, limit=limit
        )

    def _query_loaded(
        self,
        snapshot_id: str,
        window: Window,
        captured: tuple[dict[str, Any], dict[str, bytes]],
        *,
        fields: Sequence[str] | None = None,
        limit: int = MAX_ROWS,
    ) -> QueryResult:
        """Consume this call's authenticated immutable bundle; never retain it across reads."""
        manifest, files = captured
        try:
            metadata = data_versions.metadata(decode_document(files["metadata.json"]))
            source = validate_source(
                decode_document(files["source.json"]), metadata["timeframe"], "ms"
            )
            declared_window = manifest["window"]
            if not isinstance(declared_window, dict) or set(declared_window) != {
                "start_ns",
                "end_ns",
            }:
                raise DataLayerError("invalid snapshot window schema")
            input_window = Window(declared_window["start_ns"], declared_window["end_ns"])
            input_window.aligned_step(metadata["timeframe"])
            _check_input_budget(files["candidate.csv"], files["raw.json"])
            expected = _table(files["raw.json"], metadata, source, input_window)
            if metadata["content_sha256"] != (hashlib.sha256(files["candidate.csv"]).hexdigest()):
                raise DataLayerError("Parquet/raw/declaration schema or roundtrip mismatch")
        except (RegistryError, ValueError, KeyError, TypeError, IndexError, OverflowError) as exc:
            raise DataLayerError("invalid snapshot schema/roundtrip: " + str(exc)[:160]) from exc
        table = _read_parquet(files["bars.parquet"], expected, manifest["rows"])
        selected = list(table.column_names) if fields is None else list(fields)
        if not selected or not all(
            isinstance(f, str) and f in table.column_names for f in selected
        ):
            raise DataLayerError("query fields must come from the fixed candidate schema allowlist")
        if len(set(selected)) != len(selected):
            raise DataLayerError("query fields must be unique")
        # Parquet is read once from verified bounded bytes into Arrow. DuckDB gets only this
        # table: external files/extensions/network stay disabled throughout the query.
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
            connection.register("candidate", table)
            projection = ", ".join('"' + field + '"' for field in selected)
            result = connection.execute(
                f"SELECT {projection} FROM candidate WHERE timestamp_ns >= ? AND timestamp_ns < ? "
                "ORDER BY timestamp_ns LIMIT ?",
                [window.start_ns, window.end_ns, limit + 1],
            ).to_arrow_table()
        if result.num_rows > limit:
            raise DataLayerError("query row budget exceeded; request a narrower window")
        schema = pa.schema(
            [table.schema.field(f) for f in selected], metadata=table.schema.metadata
        )
        result = result.cast(schema)
        return QueryResult(
            result,
            snapshot_id,
            window,
            metadata,
            source,
            decode_document(files["reports.json"]),
        )
