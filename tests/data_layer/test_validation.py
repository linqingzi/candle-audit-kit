"""Two-sided trust-boundary and retained diagnostic checks."""

import copy
import hashlib
import json

import pytest
from test_store import publish


def test_window_constructor_rejects_iso_text_in_integer_ns_fields():
    from candle_audit.data_layer.model import DataLayerError, Window

    with pytest.raises(DataLayerError):
        Window("2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z")


@pytest.mark.parametrize(
    "value",
    [
        True,
        1.0,
        -(2**63),
        2**63,
        "NaT",
        "2026-01-01",
        "2026-01-01T00:00:00",
        "2026-02-30T00:00:00Z",
        "2026-01-01T00:00:00.0000000001Z",
    ],
)
def test_invalid_nanoseconds_and_iso_inputs_are_contract_errors(value):
    from candle_audit.data_layer import DataLayerError, timestamp_ns

    with pytest.raises(DataLayerError):
        timestamp_ns(value)


def test_invalid_metadata_timeframe_is_a_contract_error(tmp_path, bundle):
    from candle_audit.data_layer.model import DataLayerError
    from candle_audit.data_layer.store import CandidateStore

    bundle["metadata"]["timeframe"] = []
    with pytest.raises(DataLayerError):
        publish(CandidateStore(tmp_path / "store"), bundle)


def test_query_invalid_projection_container_is_a_contract_error(tmp_path, bundle):
    from candle_audit.data_layer.model import DataLayerError, Window
    from candle_audit.data_layer.store import CandidateStore

    store = CandidateStore(tmp_path / "store")
    snapshot = publish(store, bundle)
    with pytest.raises(DataLayerError):
        store.query(snapshot.snapshot_id, Window.parse(bundle["start"], bundle["end"]), fields=1)


@pytest.mark.parametrize(
    "case",
    [
        "price",
        "nan",
        "inf",
        "empty-raw",
        "empty-csv",
        "raw-duplicate",
        "raw-conflict",
        "csv-duplicate",
        "seconds",
        "volume-unit",
        "venue",
        "missing-confirm",
        "unclosed",
        "api-error",
        "bar",
        "hash",
        "outside",
        "leading",
        "trailing",
        "long-number",
    ],
)
def test_bad_single_variable_candidates_never_publish(tmp_path, bundle, case):
    from candle_audit.data_layer.model import DataLayerError, RejectedCandidate
    from candle_audit.data_layer.store import CandidateStore

    bundle = copy.deepcopy(bundle)
    native = json.loads(bundle["raw"])
    if case == "price":
        native["data"][0][1] = "100.000000000000000001"
    if case == "nan":
        native["data"][0][1] = "NaN"
    if case == "inf":
        native["data"][0][1] = "Infinity"
    if case == "empty-raw":
        native["data"] = []
    if case == "empty-csv":
        bundle["candidate_csv"] = b"timestamp,open,high,low,close,qty\n"
    if case in ("raw-duplicate", "raw-conflict"):
        row = native["data"][0].copy()
        if case == "raw-conflict":
            row[1] = "100.1"
        native["data"].append(row)
    if case == "csv-duplicate":
        bundle["candidate_csv"] += bundle["candidate_csv"].splitlines()[1] + b"\n"
    if case == "seconds":
        native["data"][0][0] = "1767225600"
    if case == "volume-unit":
        bundle["metadata"]["volume"]["columns"][0]["unit"] = "guess"
    if case == "venue":
        bundle["source"]["venue"] = "OTHER"
    if case == "missing-confirm":
        native["data"][0].pop()
    if case == "unclosed":
        native["data"][0][8] = "0"
    if case == "api-error":
        native["code"] = "403"
    if case == "bar":
        bundle["source"]["bar"] = "4H"
    if case == "outside":
        bundle["end"] = "2026-01-01T01:00:00Z"
    if case == "leading":
        bundle["start"] = "2025-12-31T23:00:00Z"
    if case == "trailing":
        bundle["end"] = "2026-01-01T03:00:00Z"
    if case == "long-number":
        native["data"][0][1] = "1" * 97
    bundle["raw"] = json.dumps(native).encode()
    bundle["metadata"]["content_sha256"] = (
        "0" * 64 if case == "hash" else hashlib.sha256(bundle["candidate_csv"]).hexdigest()
    )
    store = CandidateStore(tmp_path / "store")
    with pytest.raises(DataLayerError) as failure:
        publish(store, bundle)
    assert list(store.root.iterdir()) == []
    if case in ("missing-confirm", "unclosed"):
        assert isinstance(failure.value, RejectedCandidate)
        report = failure.value.reports["reconcile"]
        assert report["comparison"] == "MATCH"
        assert report["closedbar"]["status"] == (
            "UNVERIFIED" if case == "missing-confirm" else "UNCLOSED"
        )
        assert report["coverage"]["status"] == "COMPLETE"
    if case in ("leading", "trailing"):
        counts = failure.value.reports["quality"]["requested_range"]
        assert counts[case + "_missing_bars"] == 1


@pytest.mark.parametrize("target", ["raw", "candidate_csv"])
def test_actual_eight_mib_budget_rejects_oversized_input(tmp_path, bundle, target):
    from candle_audit.data_layer.model import DataLayerError
    from candle_audit.data_layer.store import CandidateStore

    bundle[target] = b" " * (8 * 1024 * 1024 + 1)
    with pytest.raises(DataLayerError, match="byte budget"):
        publish(CandidateStore(tmp_path / "store"), bundle)


def test_actual_10001_rows_are_rejected_before_kernel_processing(tmp_path, bundle):
    from candle_audit.data_layer.model import DataLayerError
    from candle_audit.data_layer.store import CandidateStore

    header, first, _ = bundle["candidate_csv"].splitlines()
    bundle["candidate_csv"] = header + b"\n" + (first + b"\n") * 10001
    with pytest.raises(DataLayerError, match="row/cell budget"):
        publish(CandidateStore(tmp_path / "store"), bundle)


@pytest.mark.parametrize("unit", ["s", "ns", "", None])
def test_raw_time_unit_is_declared_not_guessed(tmp_path, bundle, unit):
    from candle_audit.data_layer.model import DataLayerError, Window
    from candle_audit.data_layer.store import CandidateStore

    with pytest.raises(DataLayerError, match="time unit"):
        CandidateStore(tmp_path / "store").ingest(
            bundle["candidate_csv"],
            bundle["metadata"],
            bundle["raw"],
            bundle["source"],
            Window.parse(bundle["start"], bundle["end"]),
            raw_time_unit=unit,
        )


@pytest.mark.parametrize("name", ["OPEN", "Timestamp_ns"])
def test_duckdb_case_insensitive_column_collisions_are_rejected_before_publication(
    tmp_path, bundle, name
):
    from candle_audit.data_layer import CandidateStore, DataLayerError

    bundle["candidate_csv"] = bundle["candidate_csv"].replace(
        b",qty\n", ("," + name + "\n").encode()
    )
    bundle["metadata"]["volume"]["columns"][0]["column"] = name
    bundle["metadata"]["content_sha256"] = hashlib.sha256(bundle["candidate_csv"]).hexdigest()
    store = CandidateStore(tmp_path / "store")
    with pytest.raises(DataLayerError, match="collides"):
        publish(store, bundle)
    assert list(store.root.iterdir()) == []
