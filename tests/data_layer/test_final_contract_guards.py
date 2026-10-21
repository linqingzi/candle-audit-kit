"""Single-premise guards proven necessary by four official native mutant survivors."""

from dataclasses import replace

import pytest
from test_whole_review_aggregation import HOUR, native_pair

from candle_audit.data_layer import DataLayerError, Window, aggregation, reporting
from candle_audit.data_layer.releases import Release


def test_aggregate_rejects_only_wrong_schema_metadata_after_valid_positive():
    fine, _ = native_pair()
    assert aggregation.aggregate_candles(fine, "4h", fine.window).report["status"] == "COMPLETE"
    wrong = replace(fine, table=fine.table.replace_schema_metadata({b"candle_audit": b"wrong"}))
    with pytest.raises(DataLayerError, match="typed canonical"):
        aggregation.aggregate_candles(wrong, "4h", fine.window)


def test_audit_rejects_only_coarse_window_not_covering_requested_end_after_match():
    fine, coarse = native_pair()
    assert aggregation.audit_granularity(fine, coarse, "4h", fine.window)["comparison"] == "MATCH"
    wrong = replace(coarse, window=Window(fine.window.start_ns, fine.window.end_ns - HOUR))
    with pytest.raises(DataLayerError, match="covered window"):
        aggregation.audit_granularity(fine, wrong, "4h", fine.window)


def test_audit_rejects_only_wrong_coarse_schema_metadata_after_match():
    fine, coarse = native_pair()
    assert aggregation.audit_granularity(fine, coarse, "4h", fine.window)["comparison"] == "MATCH"
    wrong = replace(coarse, table=coarse.table.replace_schema_metadata({b"candle_audit": b"wrong"}))
    with pytest.raises(DataLayerError, match="typed coarse"):
        aggregation.audit_granularity(fine, wrong, "4h", fine.window)


def test_report_rejects_only_wrong_release_identity_after_valid_positive():
    fine, _ = native_pair()
    release = Release(fine.release_id, {"lineage": {"collections": {}}, "parent_id": None})
    assert reporting.report_document(fine, release)["release_id"] == fine.release_id
    wrong = replace(fine, release_id="1" * 64)
    with pytest.raises(DataLayerError, match="selected release"):
        reporting.report_document(wrong, release)
