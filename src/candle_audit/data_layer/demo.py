"""Complete packaged synthetic example; never downloads data or creates credentials."""

import argparse
import json
from collections.abc import Sequence
from importlib.resources import files
from pathlib import Path

from .model import DataLayerError


def run_demo(root: Path, *, language: str = "zh-CN") -> dict[str, object]:
    from ._input import _directory_descriptor, controlled_path, read_bounded
    from .collection import CollectionRequest, Collector, FileImportProvider
    from .model import Window
    from .providers import ProviderRegistry
    from .reference import InstrumentRules
    from .releases import ReleaseStore
    from .reporting import validate_report_language, write_report
    from .store import CandidateStore, _create_file, canonical, decode_document

    validate_report_language(language)
    root = controlled_path(root)
    resources = files("candle_audit.data_layer").joinpath("_demo_data")
    inputs = {name: resources.joinpath(name).read_bytes()
              for name in ("bars.csv", "raw.json", "metadata.json", "source.json")}
    metadata = decode_document(inputs["metadata.json"])
    source = decode_document(inputs["source.json"])
    window = Window.parse("2026-01-01T00:00:00Z", "2026-01-01T08:00:00Z")
    rule = InstrumentRules(
        source=source, timeframe="1h", effective=window, recorded_ns=window.end_ns,
        tick_size="0.01", lot_size="1", contract_size="0.1",
        rule_evidence=b"synthetic-example-only; not an exchange rule or a market observation",
    )
    request = CollectionRequest(
        provider_id="file", rules=(rule,), window=window, name="synthetic-demo",
        revision="v1", kind="synthetic", provenance="synthetic-engineering/test-only",
        volume_columns=metadata["volume"]["columns"],
        shard_rows=4, max_pages=4, max_total_bytes=8388608,
    )
    inputs["request.json"] = canonical(request.as_dict())
    inputs["pages.json"] = canonical({"pages": [{
        "raw": str(root / "inputs/raw.json"), "csv": str(root / "inputs/bars.csv")
    }]})
    with _directory_descriptor(root / "inputs", create=True) as descriptor:
        for name, content in inputs.items():
            try:
                with _create_file(name, descriptor) as stream:
                    stream.write(content)
            except FileExistsError:
                if read_bounded(Path(name), 262144, dir_fd=descriptor) != content:
                    raise DataLayerError("demo refuses to overwrite changed input files") from None

    candidates = CandidateStore(root / "candidates")
    snapshot = candidates.ingest(
        inputs["bars.csv"], metadata, inputs["raw.json"], source, window, raw_time_unit="ms"
    )
    registry = ProviderRegistry()
    registry.register("file", FileImportProvider(((inputs["raw.json"], inputs["bars.csv"]),)))
    collector = Collector(root / "pipeline")
    collected = collector.collect(request, registry)
    releases = ReleaseStore(collector)
    release = releases.create([collected.collection_id], name="synthetic-demo", producer_id="example")
    data = releases.reader().query(release.release_id, window)
    derived = releases.aggregate(
        release.release_id, "4h", window, name="synthetic-4h", producer_id="example"
    )
    coarse = releases.reader().query(derived.release_id, window)
    report = write_report(root / "reports", data, release, language=language)
    return {
        "status": "SYNTHETIC_DEMO_COMPLETE", "network_used": False,
        "source_verification": "UNVERIFIED", "admission": "NOT_EVALUATED",
        "snapshot_id": snapshot.snapshot_id, "collection_id": collected.collection_id,
        "release_id": release.release_id, "derived_release_id": derived.release_id,
        "rows": data.table.num_rows, "aggregate_rows": coarse.table.num_rows,
        "report": report, "root": str(root),
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("data/demo"))
    parser.add_argument("--language", choices=("zh-CN", "en"), default="zh-CN",
                        help="HTML report language; JSON evidence is unchanged (default: zh-CN)")
    args = parser.parse_args(argv)
    try:
        result = run_demo(args.root, language=args.language)
    except (DataLayerError, OSError) as exc:
        print(json.dumps({"status": "REJECTED", "error": str(exc)[:240]}))
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0
