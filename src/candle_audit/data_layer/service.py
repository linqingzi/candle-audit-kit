"""Whole data-layer CLI; a user must explicitly commission any public network acquisition."""

import argparse
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from ._input import controlled_path, read_bounded
from .model import DataLayerError, Window


def _document(path: Path) -> dict[str, Any]:
    from .store import decode_document

    return decode_document(read_bounded(controlled_path(path), 262144))


def main(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("providers", help="列出本模块真实可用Provider实现")
    collect = commands.add_parser("collect", help="文件导入或明确授权的历史分页采集/断点续作")
    collect.add_argument("--request", type=Path, required=True)
    collect.add_argument("--pages", type=Path)
    collect.add_argument("--authorize-public-download", action="store_true")
    collect.add_argument("--grant", type=Path)
    collect.add_argument("--retries", type=int, default=2)
    release = commands.add_parser("release", help="冻结完整请求集合为不可变VERIFY版本")
    release.add_argument("--collections", nargs="+", required=True)
    release.add_argument("--name", required=True)
    release.add_argument("--producer", required=True)
    release.add_argument("--parent")
    release.add_argument("--reason")
    catalog = commands.add_parser("catalog", help="按原生/派生版本schema列出真实目录及血缘")
    read = commands.add_parser("read", help="唯一规范reader，精确文本与typed EMPTY")
    read.add_argument("--fields", nargs="+")
    read.add_argument("--instruments", nargs="+")
    read.add_argument("--timeframes", nargs="+")
    read.add_argument("--limit", type=int, default=10000)
    read.add_argument("--mode", choices=("VERIFY", "RESEARCH"), default="VERIFY")
    read.add_argument("--as-of")
    read.add_argument("--knowledge", choices=("observed", "published"), default="observed")
    aggregate = commands.add_parser("aggregate", help="完整UTC闭桶聚合，拒绝漏棒/插值")
    aggregate.add_argument("--timeframe", required=True)
    aggregate.add_argument("--source-timeframes", nargs="+")
    aggregate.add_argument("--name", required=True)
    aggregate.add_argument("--producer", required=True)
    report = commands.add_parser("report", help="Chinese/English quality, gaps, revisions and selected-input candles")
    report.add_argument("--output", type=Path, required=True)
    report.add_argument("--compare-parent", action="store_true")
    report.add_argument("--language", choices=("zh-CN", "en"), default="zh-CN",
                        help="HTML report language; JSON evidence is unchanged (default: zh-CN)")
    task = commands.add_parser("task", help="任务状态、失败告警、恢复入口")
    task.add_argument("--request-id", required=True)
    benchmark = commands.add_parser("benchmark", help="实际输入reader资源测量，无市场规模推断")
    benchmark.add_argument("--repeats", type=int, default=3)
    review = commands.add_parser("review-payload", help="给独立审证者生成精确对象签名材料")
    review.add_argument("--auditor", required=True)
    review.add_argument("--evidence", type=Path, required=True)
    record = commands.add_parser("record-review", help="验证外部审证者签名并保存原件")
    acceptance = commands.add_parser("acceptance-payload", help="给独立用户所有者生成接受材料")
    acceptance.add_argument("--review-id", required=True)
    acceptance.add_argument("--owner", required=True)
    admit = commands.add_parser("admit", help="验证用户签名后才准入研究版本")
    for command in (record, admit):
        command.add_argument("--payload", type=Path, required=True)
        command.add_argument("--signature", type=Path, required=True)
    for command in (
        collect,
        release,
        catalog,
        read,
        aggregate,
        report,
        task,
        benchmark,
        review,
        record,
        acceptance,
        admit,
    ):
        command.add_argument("--root", type=Path, required=True)
    for command in (read, aggregate, report, benchmark, review, acceptance, catalog):
        command.add_argument("--release", required=True)
    for command in (read, aggregate, report, benchmark):
        command.add_argument("--start", required=True)
        command.add_argument("--end", required=True)
    for command in (read, review, record, acceptance, admit):
        command.add_argument("--trust-policy", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "providers":
            from .collection import FileImportProvider, HistoryOKXProvider

            assert callable(FileImportProvider.acquire_page) and callable(
                HistoryOKXProvider.acquire_page
            )
            print(
                json.dumps(
                    {
                        "providers": ["file-import", "okx-history"],
                        "network": "explicit grant required",
                    }
                )
            )
            return 0
        root = controlled_path(args.root)
        window = Window.parse(args.start, args.end) if hasattr(args, "start") else None
        from .collection import (
            CollectionRequest,
            Collector,
            DownloadGrant,
            FileImportProvider,
            HistoryOKXProvider,
            PublicOKXTransport,
        )
        from .providers import ProviderRegistry
        from .releases import Principal, ReleaseStore, TrustPolicy

        pipeline = Collector(root)
        policy = None
        if getattr(args, "trust_policy", None) is not None:
            doc = _document(args.trust_policy)
            policy = TrustPolicy(
                {
                    k: Principal(bytes.fromhex(v["public_key"]), tuple(v["roles"]))
                    for k, v in doc["principals"].items()
                }
            )
        releases = ReleaseStore(pipeline, trust_policy=policy)
        output: dict[str, Any]
        if args.command == "collect":
            request = CollectionRequest.from_dict(_document(args.request))
            providers = ProviderRegistry()
            if args.pages is not None:
                if args.authorize_public_download or args.grant is not None:
                    raise DataLayerError(
                        "file import and live commissioning must be explicit separate selections"
                    )
                page_doc = _document(args.pages)
                captured = tuple(
                    (
                        read_bounded(controlled_path(Path(p["raw"])), 8 * 1024 * 1024),
                        read_bounded(controlled_path(Path(p["csv"])), 8 * 1024 * 1024)
                        if p.get("csv") is not None
                        else None,
                    )
                    for p in page_doc["pages"]
                )
                providers.register(request.provider_id, FileImportProvider(captured))
            elif args.authorize_public_download and args.grant is not None:
                grant = DownloadGrant(**_document(args.grant))
                providers.register(
                    request.provider_id, HistoryOKXProvider(PublicOKXTransport(request, grant))
                )
            else:
                raise DataLayerError("真实公开下载需要明确授权及匹配grant；也可提供文件pages")
            result = pipeline.collect(request, providers, retries=args.retries)
            output = {
                "status": result.status,
                "collection_id": result.collection_id,
                "rows": result.rows,
                "request_id": result.request_id,
                "quality": result.quality,
            }
        elif args.command == "release":
            created = releases.create(
                args.collections,
                name=args.name,
                producer_id=args.producer,
                parent_id=args.parent,
                reason=args.reason,
            )
            output = {"status": "VERIFY_RELEASE_CREATED", "release_id": created.release_id}
        elif args.command == "catalog":
            output = releases.catalog(args.release)
        elif args.command == "task":
            output = {
                "task": pipeline.task(args.request_id),
                "status": "FOUND" if pipeline.task(args.request_id) is not None else "UNKNOWN",
            }
        elif args.command == "review-payload":
            output = releases.review_payload(
                args.release, auditor_id=args.auditor, evidence=_document(args.evidence)
            )
        elif args.command == "record-review":
            output = releases.record_review(
                _document(args.payload), read_bounded(controlled_path(args.signature), 64)
            )
        elif args.command == "acceptance-payload":
            output = releases.acceptance_payload(
                args.release, _document_from_artifact(pipeline, args.review_id), owner_id=args.owner
            )
        elif args.command == "admit":
            output = {
                "status": "RESEARCH_ADMITTED",
                "approval_id": releases.admit(
                    _document(args.payload), read_bounded(controlled_path(args.signature), 64)
                ),
            }
        elif window is not None:
            if args.command == "read":
                output = (
                    releases.reader()
                    .query(
                        args.release,
                        window,
                        fields=args.fields,
                        instruments=args.instruments,
                        timeframes=args.timeframes,
                        limit=args.limit,
                        mode=args.mode,
                        as_of_ns=_instant(args.as_of),
                        knowledge=args.knowledge,
                    )
                    .as_dict()
                )
            elif args.command == "aggregate":
                created = releases.aggregate(
                    args.release,
                    args.timeframe,
                    window,
                    name=args.name,
                    producer_id=args.producer,
                    source_timeframes=args.source_timeframes,
                )
                output = {
                    "status": "DERIVED_VERIFY_RELEASE_CREATED",
                    "release_id": created.release_id,
                }
            elif args.command == "benchmark":
                from .reporting import measure_reader

                output = measure_reader(
                    releases.reader(), args.release, window, repeats=args.repeats
                )
            else:
                from .reporting import write_report

                released = releases.load(args.release)
                query = releases.reader().query(args.release, window)
                revision = (
                    releases.reader().revisions(
                        released.document["parent_id"], args.release, window
                    )
                    if args.compare_parent and released.document["parent_id"]
                    else None
                )
                output = write_report(controlled_path(args.output), query, released, revision,
                                      language=args.language)
        else:
            raise DataLayerError("unknown whole-module command")
        print(json.dumps(output, ensure_ascii=False, sort_keys=True, allow_nan=False))
        return 0
    except (DataLayerError, OSError, ValueError, TypeError, KeyError, IndexError) as exc:
        output = {"status": "REJECTED", "error": str(exc)[:240], "admission": "NOT_EVALUATED"}
        state = getattr(exc, "state", None)
        if state is not None:
            output["task"] = state
        print(json.dumps(output, ensure_ascii=False, sort_keys=True, allow_nan=False))
        return 1


def _document_from_artifact(pipeline: Any, identity: str) -> dict[str, Any]:
    from .store import decode_document

    return decode_document(pipeline.artifacts.get(identity))


def _instant(value: str | None) -> int | None:
    from .model import timestamp_ns

    return (
        None
        if value is None
        else timestamp_ns(int(value) if value.lstrip("-").isdigit() else value)
    )
