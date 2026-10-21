"""B-03: revisions disclose numerical, rule/reference and declaration comparison scopes."""

from test_full_collection import history, native, request
from test_full_reference import rules
from test_full_releases import built


def test_reference_only_revision_is_visible_with_exact_scope_and_immutable_parent(tmp_path, bundle):
    store, before, req = built(tmp_path, bundle)
    reference = rules(
        bundle, effective=req.window, basis="OBSERVED", rule_evidence=b"new reference"
    )
    revised = request(bundle, count=4, rules=(reference,), revision="r2", page_rows=300)
    c = store.collector.collect(
        revised,
        history(bundle, revised, [native(req.window.start_ns // 10**6, 4)]),
        sleep=lambda _: None,
    )
    after = store.create(
        [c.collection_id],
        name="reference-revision",
        producer_id="collector",
        parent_id=before.release_id,
        reason="reference evidence update",
    )
    original_bytes = store.artifacts.get(before.release_id)
    report = store.reader().revisions(before.release_id, after.release_id, req.window)
    assert {r["field"] for r in report["changed_fields"]} == {"rule_id"}
    assert len(report["changed_fields"]) == 4
    assert report["added"] == report["removed"] == []
    assert "rule_id" in report["compared_fields"] and "state" in report["compared_fields"]
    assert set(report["non_compared_fields"]) == {"event_time_ns", "acquired_ns", "published_ns"}
    assert report["identity_fields"] == ["instrument", "timeframe", "open_time_ns"]
    changes = report["reference_changes"]
    assert len(changes) == 1
    assert set(changes[0]["fields"]) == {"basis", "rule_evidence_sha256"}
    assert changes[0]["before"]["basis"] == "DECLARED"
    assert changes[0]["after"]["basis"] == "OBSERVED"
    assert report["declaration_changes"] == []
    assert store.artifacts.get(before.release_id) == original_bytes
    assert (
        store.reader().query(before.release_id, req.window).table["rule_id"][0].as_py()
        != reference.rule_id
    )


def test_volume_declaration_change_is_separate_from_price_and_quantity_changes(tmp_path, bundle):
    store, before, req = built(tmp_path, bundle)
    revised = request(
        bundle,
        count=4,
        revision="r2",
        page_rows=300,
        volume_columns=[{"column": "base", "unit": "base"}, {"column": "quote", "unit": "quote"}],
    )
    c = store.collector.collect(
        revised,
        history(bundle, revised, [native(req.window.start_ns // 10**6, 4)]),
        sleep=lambda _: None,
    )
    after = store.create(
        [c.collection_id],
        name="declaration",
        producer_id="collector",
        parent_id=before.release_id,
        reason="explicit column declaration",
    )
    report = store.reader().revisions(before.release_id, after.release_id, req.window)
    assert report["changed_fields"] == report["reference_changes"] == []
    assert len(report["declaration_changes"]) == 1
    assert report["declaration_changes"][0]["fields"] == ["volume_columns"]
    assert "volume_columns" in report["declaration_compared_fields"]


def test_zero_quantity_revision_also_reports_state_without_acquisition_price_changes(
    tmp_path, bundle
):
    store, before, req = built(tmp_path, bundle)
    revised = request(bundle, count=4, revision="r2", page_rows=300)
    rows = native(req.window.start_ns // 10**6, 4)
    rows[0][5:8] = ["0", "0", "0"]
    c = store.collector.collect(revised, history(bundle, revised, [rows]), sleep=lambda _: None)
    after = store.create(
        [c.collection_id],
        name="zero",
        producer_id="collector",
        parent_id=before.release_id,
        reason="observed zero revision",
    )
    report = store.reader().revisions(before.release_id, after.release_id, req.window)
    assert {r["field"] for r in report["changed_fields"]} == {
        "base_qty",
        "quote_qty",
        "contracts_qty",
        "state",
    }
    state = next(r for r in report["changed_fields"] if r["field"] == "state")
    assert (state["before"], state["after"]) == ("OBSERVED", "OBSERVED_ZERO")
    assert report["reference_changes"] == []
