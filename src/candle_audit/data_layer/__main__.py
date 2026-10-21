"""Formal Candle Audit Kit offline data-layer consumer; all input fixtures remain unverified candidates."""

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from ._input import controlled_path, project_root, read_bounded
from .model import DataLayerError, RejectedCandidate, Window


def load_input(value: str, suffix: str, limit: int) -> bytes:
    path = Path(value)
    if ".." in path.parts or path.suffix != suffix:
        raise DataLayerError("input path traversal/type refused")
    resolved = path.resolve()
    fixtures = project_root() / "tests/fixtures/legacy"
    if resolved not in (fixtures / "raw.json", fixtures / "bars.csv"):
        resolved = controlled_path(path)
    return read_bounded(resolved, limit)


def main(argv: Sequence[str] | None = None) -> int:
    requested = list(sys.argv[1:] if argv is None else argv)
    if requested and requested[0] == "demo":
        from .demo import main as demo_main

        return demo_main(requested[1:])
    if requested and requested[0] in {
        "providers",
        "collect",
        "release",
        "catalog",
        "read",
        "aggregate",
        "report",
        "task",
        "benchmark",
        "review-payload",
        "record-review",
        "acceptance-payload",
        "admit",
    }:
        from .service import main as whole_main

        return whole_main(requested)
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog="Offline quick start: demo --root data/demo. Whole-module commands: "
        "providers, collect, release, catalog, read, aggregate, "
        "report, task, benchmark, review-payload, record-review, acceptance-payload, admit.",
    )
    subcommands = parser.add_subparsers(dest="command", required=True)
    from candle_audit import __version__

    parser.add_argument("--version", action="version", version=f"Candle Audit Kit {__version__}")
    ingest = subcommands.add_parser(
        "ingest", help="offline CCXT bytes to a new engineering candidate"
    )
    query = subcommands.add_parser("query", help="bounded, read-only DuckDB candidate query")
    for command in (ingest, query):
        command.add_argument("--store", type=Path, required=True)
        command.add_argument("--start", required=True)
        command.add_argument("--end", required=True)
    for name in ("csv", "raw", "metadata", "source", "raw-time-unit"):
        ingest.add_argument("--" + name, required=True)
    query.add_argument("--snapshot", required=True)
    query.add_argument("--fields", nargs="+")
    query.add_argument("--limit", type=int, default=10000)
    args = parser.parse_args(argv)
    document: dict[str, object]
    try:
        if args.command == "query":
            window = Window.parse(args.start, args.end)

            from .store import CandidateStore

            store = CandidateStore(args.store)
            document = store.query(
                args.snapshot, window, fields=args.fields, limit=args.limit
            ).as_dict()
        else:
            csv = load_input(args.csv, ".csv", 8 * 1024 * 1024)
            raw = load_input(args.raw, ".json", 8 * 1024 * 1024)
            metadata_bytes = load_input(args.metadata, ".json", 65536)
            source_bytes = load_input(args.source, ".json", 65536)
            window = Window.parse(args.start, args.end)

            from .adapter import OfflineOKX
            from .store import CandidateStore, decode_document

            store = CandidateStore(args.store)
            metadata = decode_document(metadata_bytes)
            source = decode_document(source_bytes)
            capture = OfflineOKX(lambda url, method: raw).capture(
                source, metadata.get("timeframe", ""), window, raw_time_unit=args.raw_time_unit
            )
            snapshot = store.ingest(
                csv, metadata, capture.raw, source, window, raw_time_unit=args.raw_time_unit
            )
            document = {
                "status": "CANDIDATE_STORED",
                "snapshot_id": snapshot.snapshot_id,
                "source_verification": "UNVERIFIED",
                "admission": "NOT_EVALUATED",
                "reports": snapshot.reports,
                "capture": {
                    "ccxt_version": capture.ccxt_version,
                    "method": capture.method,
                    "view_status": capture.view_status,
                    "quantity_units": capture.quantity_units,
                },
            }
        print(json.dumps(document, sort_keys=True, allow_nan=False))
        return 0
    except (DataLayerError, OSError) as exc:
        document = {
            "status": "REJECTED",
            "error": str(exc)[:240],
            "source_verification": "UNVERIFIED",
            "admission": "NOT_EVALUATED",
        }
        if isinstance(exc, RejectedCandidate):
            document["reports"] = exc.reports
        print(json.dumps(document, sort_keys=True, allow_nan=False))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
