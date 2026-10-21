"""A source-to-derived edge is not a same-grain historical revision."""

import json
from pathlib import Path

import pytest
from test_full_aggregation import inputs
from test_whole_review_streaming import MINUTE, assert_hour_oracle, minute_release

from candle_audit.data_layer import DataLayerError, Window
from candle_audit.data_layer.__main__ import main
from candle_audit.data_layer.reporting import report_document


@pytest.mark.parametrize("count", [120, 10020])
def test_long_derived_report_same_cli_input_only_parent_flag_changes(
    tmp_path, bundle, capsys, count
):
    store, source, req, _ = minute_release(tmp_path, bundle, count, 6002)
    derived = store.aggregate(
        source.release_id, "1h", req.window, name="hour", producer_id="collector"
    )
    result = store.reader().query(derived.release_id, req.window)
    assert_hour_oracle(result, count)
    revision = store.reader().revisions(source.release_id, derived.release_id, req.window)
    assert revision["comparison"] == "NOT_APPLICABLE"
    assert revision["reason"] == "DERIVATION_NOT_REVISION"
    assert "added" not in revision and "removed" not in revision
    assert report_document(result, derived, revision)["revision"] == revision
    start, end = req.window.iso_bounds()
    args = [
        "report",
        "--root",
        str(store.collector.root),
        "--release",
        derived.release_id,
        "--start",
        start,
        "--end",
        end,
        "--output",
        str(tmp_path / "reports"),
    ]
    for flag in ([], ["--compare-parent"]):
        assert main(args + flag) == 0
        output = json.loads(capsys.readouterr().out)
        assert output["status"] == "REPORT_CREATED"
        document = json.loads(Path(output["json_path"]).read_bytes())
        assert len(document["query"]["rows"]) == count // 60
        assert document["revision"] == (revision if flag else None)
    if count > 10000:
        with pytest.raises(DataLayerError, match="row budget"):
            store.reader().query(source.release_id, req.window)


def test_native_parent_revisions_and_empty_derived_reports_preserve_relation(tmp_path, bundle):
    store, source, req = inputs(tmp_path, bundle)
    revision = store.create(
        source.document["requested_collections"],
        name="revision",
        producer_id="collector",
        parent_id=source.release_id,
        reason="review",
    )
    compared = store.reader().revisions(source.release_id, revision.release_id, req.window)
    assert compared["added"] == compared["removed"] == compared["changed_fields"] == []
    assert "rule_id" in compared["compared_fields"]
    derived = store.aggregate(
        source.release_id, "4h", req.window, name="hour", producer_id="collector"
    )
    outside = Window(req.window.end_ns, req.window.end_ns + 8 * 60 * MINUTE)
    empty = store.reader().query(derived.release_id, outside)
    relation = store.reader().revisions(source.release_id, derived.release_id, outside)
    assert relation["reason"] == "DERIVATION_NOT_REVISION"
    assert report_document(empty, derived, relation)["query"]["query_status"] == "EMPTY"
    with pytest.raises(DataLayerError, match="explicit parent"):
        store.reader().revisions("1" * 64, derived.release_id, req.window)
