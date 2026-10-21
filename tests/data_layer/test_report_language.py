"""HTML language is presentation only; exact evidence and existing guards remain intact."""

import base64
import copy
import hashlib
import html
import json
import re
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pytest
from test_full_releases import built
from test_whole_review_aggregation import HOUR, START, typed_bars

from candle_audit.data_layer import DataLayerError, Window
from candle_audit.data_layer.demo import main as demo_main
from candle_audit.data_layer.releases import CANDLE_SCHEMA, Release
from candle_audit.data_layer.reporting import render_report, report_document, write_report
from candle_audit.data_layer.service import main as service_main
from candle_audit.data_layer.store import canonical


def candle_details(document):
    return [json.loads(html.unescape(value))
            for value in re.findall(r'data-detail="([^"]+)"', document)]


def test_language_changes_ui_without_changing_exact_candles_evidence_or_script(tmp_path, bundle):
    store, release, req = built(tmp_path, bundle)
    result = store.reader().query(release.release_id, req.window)
    original = copy.deepcopy(report_document(result, release))
    chinese = render_report(result, release).decode()
    assert chinese == render_report(result, release, language="zh-CN").decode()
    english = render_report(result, release, language="en").decode()
    assert '<html lang="en">' in english and '<html lang="zh-CN">' in chinese
    assert "Engineered synthetic test data" in english and "工程合成测试数据" in chinese
    for label in ("Quality and candles", "Quality dimensions", "Completeness", "Accuracy",
                  "Instrument and timeframe", "Exact original values per candle",
                  "Gaps and revisions", "UNVERIFIED", "UNKNOWN"):
        assert label in english
    assert not re.search(r"[\u3400-\u9fff]", english), "synthetic English UI must not mix Chinese labels"
    assert candle_details(english) == candle_details(chinese) == result.table.to_pylist()
    assert "100.00" in english
    assert re.findall(r"<(?:line|rect) [^>]*>", english) == re.findall(r"<(?:line|rect) [^>]*>", chinese)
    scripts = [re.search(r"<script>(.*?)</script>", text, re.S).group(1)
               for text in (english, chinese)]
    assert scripts[0] == scripts[1]
    digest = base64.b64encode(hashlib.sha256(scripts[0].encode()).digest()).decode()
    assert all("script-src 'sha256-" + digest + "'" in text for text in (english, chinese))
    assert report_document(result, release) == original


def test_language_specific_html_retains_byte_identical_json_evidence(tmp_path, bundle):
    store, release, req = built(tmp_path, bundle)
    result = store.reader().query(release.release_id, req.window)
    chinese = write_report(tmp_path / "reports-zh", result, release)
    english = write_report(tmp_path / "reports-en", result, release, language="en")
    assert chinese["json_sha256"] == english["json_sha256"]
    assert Path(chinese["json_path"]).read_bytes() == Path(english["json_path"]).read_bytes()
    assert Path(english["json_path"]).read_bytes() == canonical(report_document(result, release))
    assert chinese["html_sha256"] != english["html_sha256"]
    assert Path(english["html_path"]).read_bytes() == render_report(result, release, language="en")


def test_english_empty_selection_preserves_unknown_gap_truth_and_creates_no_candles(tmp_path, bundle):
    store, release, req = built(tmp_path, bundle)
    result = store.reader().query(release.release_id, Window(req.window.end_ns, req.window.end_ns + HOUR))
    document = render_report(result, release, language="en").decode()
    assert "EMPTY" in document and "No zero-value candles are generated" in document
    assert "UNKNOWN" in document and not candle_details(document)
    assert report_document(result, release)["query_gaps"][0]["market_gap_truth"] == "UNKNOWN"


def test_english_ui_preserves_unicode_user_text_and_escapes_markup(tmp_path, bundle):
    store, release, req = built(tmp_path, bundle)
    result = store.reader().query(release.release_id, req.window)
    release.document["name"] = "我的資料 </script><img src=x onerror=alert(1)>"
    document = render_report(result, release, language="en").decode()
    assert "我的資料" in document and "&lt;/script&gt;" in document
    assert "<img src=x" not in document
    assert "Quality and candles" in document


@pytest.mark.parametrize("language", ["fr", "EN", "en<script>", None, True])
def test_unsupported_language_is_refused_before_output_writes(tmp_path, bundle, language):
    store, release, req = built(tmp_path, bundle)
    result = store.reader().query(release.release_id, req.window)
    output = tmp_path / "not-created"
    with pytest.raises(DataLayerError, match="report language"):
        write_report(output, result, release, language=language)
    assert not output.exists()


def test_demo_cli_generates_actual_english_report_offline(tmp_path, capsys):
    assert demo_main(["--root", str(tmp_path / "demo"), "--language", "en"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "SYNTHETIC_DEMO_COMPLETE"
    assert (result["rows"], result["aggregate_rows"], result["network_used"]) == (8, 2, False)
    assert result["source_verification"] == "UNVERIFIED" and result["admission"] == "NOT_EVALUATED"
    document = Path(result["report"]["html_path"]).read_text()
    assert "Engineered synthetic test data" in document and '<html lang="en">' in document


def test_report_cli_generates_english_from_the_selected_release(tmp_path, bundle, capsys):
    store, release, req = built(tmp_path, bundle)
    assert service_main([
        "report", "--root", str(tmp_path / "pipeline"), "--release", release.release_id,
        "--start", bundle["start"], "--end", bundle["end"],
        "--output", str(tmp_path / "report"), "--language", "en",
    ]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["release_id"] == release.release_id
    assert "Quality dimensions" in Path(output["html_path"]).read_text()


def test_english_report_retains_the_eight_mebibyte_size_guard():
    data = typed_bars([1] * 8)
    release = Release(data.release_id, {"name": "engineering", "lineage": {"collections": {}}, "parent_id": None})
    assert b'lang="en"' in render_report(data, release, language="en")
    rows = typed_bars([1] * 10000).table.to_pylist()
    for row in rows:
        row.update(open="1." + "0" * 90, high="2." + "0" * 90,
                   low="0." + "1" * 90, close="1." + "0" * 90)
    many = replace(data, table=pa.Table.from_pylist(rows, schema=CANDLE_SCHEMA),
                   window=Window(START, START + 10000 * HOUR))
    with pytest.raises(DataLayerError, match="report byte budget"):
        render_report(many, release, language="en")


@pytest.mark.parametrize("invalid", ["partial-schema", "wrong-release"])
def test_english_view_requires_the_same_canonical_release_input(tmp_path, bundle, invalid):
    store, release, req = built(tmp_path, bundle)
    result = store.reader().query(release.release_id, req.window)
    invalid_result = (replace(result, table=result.table.select(["close"]))
                      if invalid == "partial-schema" else replace(result, release_id="different"))
    with pytest.raises(DataLayerError, match="full canonical query"):
        render_report(invalid_result, release, language="en")


@pytest.mark.parametrize("command", ["demo", "report"])
def test_cli_rejects_unsupported_language_before_creating_files(tmp_path, command):
    root = tmp_path / "unused-root"
    if command == "demo":
        args = ["--root", str(root), "--language", "fr"]
        entry = demo_main
    else:
        args = ["report", "--root", str(root), "--release", "unused",
                "--start", "2026-01-01T00:00:00Z", "--end", "2026-01-01T01:00:00Z",
                "--output", str(tmp_path / "unused-output"), "--language", "fr"]
        entry = service_main
    with pytest.raises(SystemExit) as rejected:
        entry(args)
    assert rejected.value.code == 2
    assert not root.exists() and not (tmp_path / "unused-output").exists()
