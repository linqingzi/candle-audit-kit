"""Exercise the real CLI function as well as the retained subprocess consumers."""

import json

from test_consumer import arguments

from candle_audit.data_layer.__main__ import main


def test_cli_function_consumes_its_actual_store_and_reports(tmp_path, bundle, capsys):
    assert main(arguments(tmp_path, bundle)) == 0
    document = json.loads(capsys.readouterr().out)
    assert document["status"] == "CANDIDATE_STORED"
    assert document["reports"]["quality"]["structural_status"] == "PASS"
    assert document["reports"]["reconcile"]["closedbar"]["status"] == "CLOSED"
    assert document["capture"] == {
        "ccxt_version": "4.5.64",
        "method": "publicGetMarketHistoryCandles",
        "view_status": "NON_AUTHORITATIVE_FLOAT",
        "quantity_units": {"vol": "contracts", "volCcy": "base", "volCcyQuote": "quote"},
    }
    assert document["source_verification"] == "UNVERIFIED"
    assert document["admission"] == "NOT_EVALUATED"
    args = [
        "query",
        "--store",
        str(tmp_path / "store"),
        "--snapshot",
        document["snapshot_id"],
        "--start",
        bundle["start"],
        "--end",
        bundle["end"],
        "--fields",
        "timestamp_ns",
        "open",
    ]
    assert main(args) == 0
    query = json.loads(capsys.readouterr().out)
    assert query["query_status"] == "ROWS"
    assert query["rows"] == [
        {"timestamp_ns": 1767225600000000000, "open": "100.00"},
        {"timestamp_ns": 1767229200000000000, "open": "101"},
    ]
    assert [field["type"] for field in query["schema"]] == ["int64", "string"]
    assert main([*args, "--limit", "1"]) == 1
    rejected = json.loads(capsys.readouterr().out)
    assert rejected["status"] == "REJECTED" and "row budget" in rejected["error"]
    assert rejected["source_verification"] == "UNVERIFIED"
    assert rejected["admission"] == "NOT_EVALUATED"


def test_cli_function_semantic_rejection_keeps_original_kernel_reports(tmp_path, bundle, capsys):
    args = arguments(tmp_path, bundle)
    native = json.loads(bundle["raw"])
    native["data"][0][8] = "0"
    (tmp_path / "raw.json").write_text(json.dumps(native))
    assert main(args) == 1
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "REJECTED"
    assert result["reports"]["quality"]["structural_status"] == "PASS"
    assert result["reports"]["reconcile"]["comparison"] == "MATCH"
    assert result["reports"]["reconcile"]["closedbar"]["status"] == "UNCLOSED"
    assert not list((tmp_path / "store").iterdir())
