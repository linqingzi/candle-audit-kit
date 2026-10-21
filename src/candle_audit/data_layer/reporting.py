"""Chinese/English HTML views of unchanged quality evidence and actual-input candles."""

import html
import json
import resource
import statistics
import sys
import time
from decimal import Decimal, localcontext
from pathlib import Path
from typing import Any, Callable

from .artifacts import Artifacts
from .model import DataLayerError, Window
from .releases import CANDLE_SCHEMA, DatasetReader, DatasetResult, Release, consumer_scope
from .store import canonical


_ENGLISH_UI = {
    "工程合成测试数据": "Engineered synthetic test data",
    "历史数据声明 · 来源待独立审证": "Historical data declaration · provenance awaits independent review",
    "外部审证及用户接受的研究版本": "Externally reviewed and user-accepted research release",
    "本页使用所选输入，不插值、不补棒；原始精确文本可逐棒查看。":
        "This view uses the selected input without interpolation or filling candles; inspect each candle's exact original text.",
    "K线": "candles",
    "EMPTY · 当前窗口无可用棒；市场缺棒/无成交事实 UNKNOWN。不生成零棒。":
        "EMPTY · No candles available in this window; market-gap/no-trade facts remain UNKNOWN. No zero-value candles are generated.",
    "查看精确原值": "View exact original values",
    "所选输入K线": "Candles from the selected input",
    "最高": "High", "最低": "Low", "UTC 纳秒窗口": "UTC nanosecond window",
    "完整": "Completeness", "唯一": "Uniqueness", "一致": "Consistency",
    "及时": "Timeliness", "有效": "Validity", "准确": "Accuracy",
    "独立粒度证据": "Independent granularity evidence",
    "采集集合": "Collection", "质量项": "Quality dimension", "状态": "Status",
    "依据/未知项": "Evidence / unknowns",
    "EMPTY · 当前窗口无数据": "EMPTY · No data in this window",
    "保留规范schema；不推断无成交。": "The canonical schema is retained; no-trade facts are not inferred.",
    "数据质量与K线": "Data quality and candles", "数据层 M01": "Data layer M01",
    "质量与K线": "Quality and candles", "版本": "Release", "准入": "Mode",
    "棒": "candles", "商品/周期组合": "instrument/timeframe combinations",
    "商品与周期": "Instrument and timeframe", "空查询": "Empty query",
    "逐棒精确原值": "Exact original values per candle",
    "点击或用键盘选中K线查看OHLC、数量单位、取得时间和未知发布时间。":
        "Click or focus a candle with the keyboard to inspect OHLC, quantity units, capture time and unknown publication time.",
    "质量分项": "Quality dimensions", "缺口与修订": "Gaps and revisions",
    "精确文本表（最多显示1000行；JSON报告保留本次查询全部行）":
        "Exact-text table (up to 1000 displayed rows; the JSON report retains all rows in this query)",
    "商品": "Instrument", "周期": "Timeframe", "开": "Open", "高": "High",
    "低": "Low", "收": "Close",
    "历史发布时间缺证时保持UNKNOWN。\n工程结果不等于真实市场来源认证或独立验收通过。":
        "Historical publication time remains UNKNOWN without evidence.\nEngineering results do not certify market provenance or independent acceptance.",
}


def validate_report_language(language: str) -> None:
    if type(language) is not str or language not in ("zh-CN", "en"):
        raise DataLayerError("report language must be 'zh-CN' or 'en'")


def _report_text(language: str) -> Callable[[str], str]:
    validate_report_language(language)
    # Translate only our UI literals, never dataset names, exact values or evidence payloads.
    return _ENGLISH_UI.__getitem__ if language == "en" else lambda value: value


def report_document(
    result: DatasetResult, release: Release, revision: dict[str, Any] | None = None
) -> dict[str, Any]:
    if result.release_id != release.release_id or not result.table.schema.equals(
        CANDLE_SCHEMA, check_metadata=True
    ):
        raise DataLayerError("full canonical query of the selected release required for report")
    collections = release.document["lineage"]["collections"]
    synthetic = any(c["request"]["kind"] == "synthetic" for c in collections.values())
    gaps = []
    rows = result.table.to_pylist()
    selected_streams = result.selection.get("streams", [])
    streams = selected_streams or [
        {"instrument": i, "timeframe": t, "coverage": []}
        for i, t in sorted({(r["instrument"], r["timeframe"]) for r in rows})
    ]
    for stream in streams:
        instrument, timeframe = stream["instrument"], stream["timeframe"]
        points = sorted(
            r["open_time_ns"]
            for r in rows
            if (r["instrument"], r["timeframe"]) == (instrument, timeframe)
        )
        step = Window(0, 86400 * 10**9).aligned_step(timeframe)
        expected = max(
            0, (result.window.end_ns - 1) // step - (result.window.start_ns + step - 1) // step + 1
        )
        missing = expected - len(points)
        if missing:
            sample: list[int] = []
            cursor = (result.window.start_ns + step - 1) // step * step
            for point in [*points, result.window.end_ns]:
                sample.extend(
                    cursor + i * step
                    for i in range(
                        min(10 - len(sample), max(0, (point - cursor + step - 1) // step))
                    )
                )
                cursor = point + step
                if len(sample) >= 10:
                    break
            gaps.append(
                {
                    "instrument": instrument,
                    "timeframe": timeframe,
                    "missing_count": missing,
                    "returned_count": len(points),
                    "requested_window": {
                        "start_ns": result.window.start_ns,
                        "end_ns": result.window.end_ns,
                    },
                    "stored_coverage": stream["coverage"],
                    "market_gap_truth": "UNKNOWN",
                    "sample_openings_ns": sample,
                    "status": "UNAVAILABLE_IN_SELECTED_QUERY",
                    "basis": "query gaps are not evidence of no trades",
                }
            )
    if not rows:
        gaps.append(
            {
                "status": "EMPTY_QUERY",
                "requested_window": [result.window.start_ns, result.window.end_ns],
                "basis": "no rows in selected version/window; no-trade truth is UNKNOWN",
            }
        )
    return {
        "schema": "candle-audit-chinese-data-report-v1",
        "release_id": result.release_id,
        "label": "工程合成测试数据"
        if synthetic
        else "历史数据声明 · 来源待独立审证"
        if result.mode == "VERIFY"
        else "外部审证及用户接受的研究版本",
        "quality": result.quality,
        "selected_streams": selected_streams,
        "selection_status": result.selection.get("status", "UNKNOWN"),
        "scope": consumer_scope(result, result.window),
        "query_gaps": gaps,
        "revision": revision,
        "lineage": {
            "collection_ids": list(collections),
            "parent_id": release.document["parent_id"],
        },
        "query": result.as_dict(),
        "publication_truth": "UNKNOWN unless separately attested",
        "boundaries": "本页使用所选输入，不插值、不补棒；原始精确文本可逐棒查看。",
    }


def render_report(
    result: DatasetResult, release: Release, revision: dict[str, Any] | None = None,
    *, language: str = "zh-CN",
) -> bytes:
    text = _report_text(language)
    doc = report_document(result, release, revision)
    rows = result.table.to_pylist()
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(row["instrument"] + " / " + row["timeframe"], []).append(row)
    for stream in doc["selected_streams"]:
        groups.setdefault(stream["instrument"] + " / " + stream["timeframe"], [])
    panels = []
    for group_index, (key, bars) in enumerate(sorted(groups.items())):
        if not bars:
            panels.append(
                f'<section class="panel" data-panel="{group_index}" '
                f"{'hidden' if group_index else ''}><h2>{html.escape(key)} {text('K线')}</h2>"
                f"<p>{text('EMPTY · 当前窗口无可用棒；市场缺棒/无成交事实 UNKNOWN。不生成零棒。')}</p></section>"
            )
            continue
        low = min(Decimal(b["low"]) for b in bars)
        high = max(Decimal(b["high"]) for b in bars)
        shapes = []
        with localcontext() as context:
            context.prec = 4096
            span = high - low or Decimal(1)
            for bar in bars:
                x = 55 + int(
                    Decimal(bar["open_time_ns"] - result.window.start_ns)
                    * 990
                    / Decimal(result.window.end_ns - result.window.start_ns)
                )
                coords = {
                    k: 255 - int((Decimal(bar[k]) - low) * 210 / span)
                    for k in ("open", "high", "low", "close")
                }
                color = "#45bf9a" if Decimal(bar["close"]) >= Decimal(bar["open"]) else "#ed827e"
                detail = html.escape(json.dumps(bar, ensure_ascii=False, indent=2), quote=True)
                width = min(14, max(1, 800 // len(bars)))
                shapes.append(
                    f'<g tabindex="0" role="button" aria-label="{text("查看精确原值")}" '
                    f'data-detail="{detail}"><title>{detail}</title>'
                    f'<line x1="{x}" x2="{x}" y1="{coords["high"]}" '
                    f'y2="{coords["low"]}" stroke="{color}"/>'
                    f'<rect x="{x - width // 2}" y="{min(coords["open"], coords["close"])}" '
                    f'width="{width}" height="{max(1, abs(coords["open"] - coords["close"]))}" '
                    f'fill="{color}"/></g>'
                )
        panels.append(
            f'<section class="panel" data-panel="{group_index}" '
            f"{'hidden' if group_index else ''}><h2>{html.escape(key)} {text('K线')}</h2>"
            f'<svg viewBox="0 0 1100 310" role="img" aria-label="{text("所选输入K线")}">'
            '<text x="20" y="25" fill="#abb7ca">'
            f"{text('最高')} {html.escape(str(high))} · {text('最低')} {html.escape(str(low))}</text>"
            '<line x1="55" x2="1050" y1="265" y2="265" stroke="#324056"/>'
            f'{"".join(shapes)}<text x="55" y="292" fill="#abb7ca">'
            f"{text('UTC 纳秒窗口')} [{result.window.start_ns}, {result.window.end_ns})"
            "</text></svg></section>"
        )
    options = "".join(
        f'<option value="{i}">{html.escape(key)}</option>' for i, key in enumerate(sorted(groups))
    )
    quality_names = {
        "completeness": text("完整"),
        "uniqueness": text("唯一"),
        "consistency": text("一致"),
        "timeliness": text("及时"),
        "validity": text("有效"),
        "accuracy": text("准确"),
        "independent_granularity": text("独立粒度证据"),
    }
    diagnostics = []
    for collection_id, quality in result.quality.items():
        cells = "".join(
            f"<tr><td>{html.escape(str(quality_names.get(k, k)))}</td>"
            f"<td>{html.escape(str(v.get('status', 'UNKNOWN')))}</td>"
            f"<td>{html.escape(str(v.get('basis', '')))}</td></tr>"
            for k, v in quality.items()
        )
        diagnostics.append(
            f"<details open><summary>{text('采集集合')} {html.escape(collection_id[:16])}</summary>"
            f"<table><thead><tr><th>{text('质量项')}</th><th>{text('状态')}</th><th>{text('依据/未知项')}</th></tr></thead>"
            f"<tbody>{cells}</tbody></table></details>"
        )
    price_rows = "".join(
        "<tr>"
        + "".join(
            f"<td>{html.escape(str(row[k]))}</td>"
            for k in (
                "instrument",
                "timeframe",
                "open_time_ns",
                "open",
                "high",
                "low",
                "close",
                "base_qty",
                "quote_qty",
                "contracts_qty",
                "state",
            )
        )
        + "</tr>"
        for row in rows[:1000]
    )
    script = """document.querySelector('#stream').addEventListener('change', e => {
      document.querySelectorAll('[data-panel]').forEach(p => {
        p.hidden = p.dataset.panel !== e.target.value;
      });
    });
    document.querySelectorAll('[data-detail]').forEach(g => {
      const show = () => { document.querySelector('#detail').textContent = g.dataset.detail; };
      g.addEventListener('click', show);
      g.addEventListener('focus', show);
    });"""
    import base64
    import hashlib

    script_hash = base64.b64encode(hashlib.sha256(script.encode()).digest()).decode()
    empty_panel = (
        f"<section><h2>{text('EMPTY · 当前窗口无数据')}</h2><p>{text('保留规范schema；不推断无成交。')}</p></section>"
    )
    gap_text = html.escape(
        json.dumps(
            {
                "gaps": doc["query_gaps"],
                "revision": revision,
                "query_window": doc["query"]["window"],
                "selection": result.selection,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    footer = text(
        "历史发布时间缺证时保持UNKNOWN。\n"
        "工程结果不等于真实市场来源认证或独立验收通过。"
    )
    counts = f"{len(rows)} 棒 · {len(groups)} 商品/周期组合"
    if language == "en":
        counts = f"Candles: {len(rows)} · Instrument/timeframe combinations: {len(groups)}"
    content = f"""<!doctype html>
<html lang="{language}"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="Content-Security-Policy" content="default-src 'none';
style-src 'unsafe-inline'; script-src 'sha256-{script_hash}';
base-uri 'none'; form-action 'none'">
<title>Candle Audit Kit {text('数据质量与K线')}</title><style>
body{{margin:0;background:#0e1420;color:#e6ecf5;font:15px system-ui,sans-serif}}
main{{max-width:1180px;margin:40px auto;padding:0 24px}}
h1{{font-size:32px;margin-bottom:10px}} h2{{font-size:21px}}
p{{line-height:1.65;color:#abb7ca}}
.badge{{display:inline-block;padding:8px 12px;background:#47321b;
color:#ffe1a1;border-radius:6px}} code{{word-break:break-all}}
section,details{{background:#172132;border:1px solid #2b3a50;
border-radius:10px;padding:18px;margin:20px 0}} svg{{width:100%;height:auto}}
select{{padding:9px;background:#172132;color:#e6ecf5;
border:1px solid #657791;border-radius:4px}}
table{{border-collapse:collapse;width:100%;font-size:13px}}
th,td{{padding:9px;text-align:left;border-bottom:1px solid #324056}}
pre{{white-space:pre-wrap;word-break:break-word;line-height:1.5}}
.scroll{{overflow:auto}} [hidden]{{display:none}}
g:focus{{outline:2px solid #eee}} summary{{cursor:pointer}}
</style></head><body><main><p>Candle Audit Kit · {text('数据层 M01')}</p>
<h1>{html.escape(release.document["name"])} · {text('质量与K线')}</h1>
<div class="badge">{html.escape(text(doc["label"]))}</div>
<p>{text('版本')} <code>{result.release_id}</code><br>
{text('准入')} {result.mode} · {counts}</p>
<p>{html.escape(text(doc["boundaries"]))}</p>
<label for="stream">{text('商品与周期')} </label>
<select id="stream">{options or '<option>' + text('空查询') + '</option>'}</select>
{"".join(panels) or empty_panel}
<section><h2>{text('逐棒精确原值')}</h2><pre id="detail">
{text('点击或用键盘选中K线查看OHLC、数量单位、取得时间和未知发布时间。')}
</pre></section><h2>{text('质量分项')}</h2>{"".join(diagnostics)}
<section><h2>{text('缺口与修订')}</h2><pre>{gap_text}</pre></section>
<details><summary>{text('精确文本表（最多显示1000行；JSON报告保留本次查询全部行）')}</summary>
<div class="scroll"><table><thead><tr><th>{text('商品')}</th><th>{text('周期')}</th><th>UTC ns</th>
<th>{text('开')}</th><th>{text('高')}</th><th>{text('低')}</th><th>{text('收')}</th><th>base</th><th>quote</th>
<th>contracts</th><th>{text('状态')}</th></tr></thead><tbody>{price_rows}</tbody></table></div>
</details><p>{footer}</p>
</main><script>{script}</script></body></html>"""
    data = content.encode()
    if len(data) > 8 * 1024 * 1024:
        raise DataLayerError("report byte budget exceeded; narrow query window")
    return data


def write_report(
    output: Path, result: DatasetResult, release: Release, revision: dict[str, Any] | None = None,
    *, language: str = "zh-CN",
) -> dict[str, Any]:
    validate_report_language(language)
    artifacts = Artifacts(output)
    html_id = artifacts.put(render_report(result, release, revision, language=language))
    json_id = artifacts.put(canonical(report_document(result, release, revision)))
    return {
        "status": "REPORT_CREATED",
        "release_id": release.release_id,
        "html_path": str(artifacts.export(html_id, ".html")),
        "json_path": str(artifacts.export(json_id, ".json")),
        "html_sha256": html_id,
        "json_sha256": json_id,
    }


def measure_reader(
    reader: DatasetReader, release_id: str, window: Window, *, repeats: int = 3
) -> dict[str, Any]:
    if type(repeats) is not int or not 1 <= repeats <= 10:
        raise DataLayerError("bounded benchmark repeats within 1..10 required")
    seconds, result = [], None
    for _ in range(repeats):
        start = time.monotonic()
        result = reader.query(release_id, window)
        seconds.append(time.monotonic() - start)
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return {
        "schema": "candle-audit-reader-measurement-v1",
        "release_id": release_id,
        "rows": result.table.num_rows if result else 0,
        "seconds": seconds,
        "median_seconds": statistics.median(seconds),
        "peak_rss_bytes": usage.ru_maxrss if sys.platform == "darwin" else usage.ru_maxrss * 1024,
        "user_cpu_seconds": usage.ru_utime,
        "system_cpu_seconds": usage.ru_stime,
        "local_two_second_query_target": "PASS" if max(seconds) <= 2 else "FAIL",
        "market_scale_slo": "NOT_VERIFIED",
        "basis": "actual supplied offline input and current reader; no scale extrapolation",
    }
