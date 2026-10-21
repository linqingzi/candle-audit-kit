"""The formal Candle Audit Kit CLI consumes the same candidate/API as the module reader."""

import json
import subprocess
import sys


def command(*args):
    return subprocess.run(
        [sys.executable, "-B", "-m", "candle_audit.data_layer", *args],
        capture_output=True,
        text=True,
        check=False,
    )


def arguments(tmp_path, bundle):
    for name, content in (
        ("bars.csv", bundle["candidate_csv"]),
        ("raw.json", bundle["raw"]),
        ("metadata.json", json.dumps(bundle["metadata"]).encode()),
        ("source.json", json.dumps(bundle["source"]).encode()),
    ):
        (tmp_path / name).write_bytes(content)
    return [
        "ingest",
        "--store",
        str(tmp_path / "store"),
        "--csv",
        str(tmp_path / "bars.csv"),
        "--raw",
        str(tmp_path / "raw.json"),
        "--metadata",
        str(tmp_path / "metadata.json"),
        "--source",
        str(tmp_path / "source.json"),
        "--start",
        bundle["start"],
        "--end",
        bundle["end"],
        "--raw-time-unit",
        "ms",
    ]


def test_cli_and_public_api_consume_nonempty_and_empty_candidates(tmp_path, bundle):
    from candle_audit.data_layer import CandidateStore, Window

    result = command(*arguments(tmp_path, bundle))
    assert result.returncode == 0, result.stderr
    document = json.loads(result.stdout)
    assert document["capture"]["ccxt_version"] == "4.5.64"
    assert document["capture"]["method"] == "publicGetMarketHistoryCandles"
    assert document["source_verification"] == "UNVERIFIED"
    base = ["query", "--store", str(tmp_path / "store"), "--snapshot", document["snapshot_id"]]
    nonempty = command(*base, "--start", bundle["start"], "--end", bundle["end"])
    assert nonempty.returncode == 0, nonempty.stderr
    cli = json.loads(nonempty.stdout)
    api = CandidateStore(tmp_path / "store").query(
        document["snapshot_id"], Window.parse(bundle["start"], bundle["end"])
    )
    assert cli == api.as_dict()
    assert [r["open"] for r in cli["rows"]] == ["100.00", "101"]
    empty = command(
        *base, "--start", "2026-01-01T00:00:00.000000001Z", "--end", "2026-01-01T01:00:00Z"
    )
    assert empty.returncode == 0, empty.stderr
    empty_doc = json.loads(empty.stdout)
    assert empty_doc["query_status"] == "EMPTY" and empty_doc["rows"] == []
    assert empty_doc["schema"] == cli["schema"]
    assert empty_doc["admission"] == "NOT_EVALUATED"


def test_cli_rejection_is_json_and_does_not_publish(tmp_path, bundle):
    args = arguments(tmp_path, bundle)
    native = json.loads(bundle["raw"])
    native["data"][0].pop()
    (tmp_path / "raw.json").write_text(json.dumps(native))
    result = command(*args)
    assert result.returncode == 1, result.stderr
    rejected = json.loads(result.stdout)
    assert rejected["status"] == "REJECTED"
    assert rejected["reports"]["reconcile"]["comparison"] == "MATCH"
    assert rejected["reports"]["reconcile"]["closedbar"]["status"] == "UNVERIFIED"
    assert not list((tmp_path / "store").iterdir())
