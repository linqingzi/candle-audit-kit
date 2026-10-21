"""Legitimate retained consumers, capacity boundaries and supported declarations."""

import calendar
import copy
import hashlib
import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta

import pytest
from test_consumer import arguments
from test_store import publish


@pytest.mark.parametrize("kind", ["SPOT", "SWAP"])
@pytest.mark.parametrize("schema", ["candle-audit-data-version-v1", "candle-audit-data-version-v2"])
def test_eth_spot_swap_and_both_exact_versions_are_real_consumers(tmp_path, bundle, kind, schema):
    from candle_audit.data_layer import CandidateStore, Window

    bundle = copy.deepcopy(bundle)
    instrument = "ETH-USDT" + ("-SWAP" if kind == "SWAP" else "")
    bundle["source"].update(instType=kind, instId=instrument, base_asset="ETH")
    bundle["metadata"].update(market=instrument, schema=schema)
    bundle["metadata"]["volume"]["base_asset"] = "ETH"
    if kind == "SPOT":
        native = json.loads(bundle["raw"])
        native["data"][0][5:8] = ["2", "2010", "2010"]
        native["data"][1][5:8] = ["3", "3045", "3045"]
        bundle["raw"] = json.dumps(native).encode()
    if schema.endswith("v1"):
        bundle["candidate_csv"] = (
            b"\n".join(
                b",".join(line.split(b",")[:5]) for line in bundle["candidate_csv"].splitlines()
            )
            + b"\n"
        )
        del bundle["metadata"]["volume"]
    bundle["metadata"]["content_sha256"] = hashlib.sha256(bundle["candidate_csv"]).hexdigest()
    store = CandidateStore(tmp_path / "store")
    snapshot = publish(store, bundle)
    query = store.query(snapshot.snapshot_id, Window.parse(bundle["start"], bundle["end"]))
    assert query.source["instId"] == instrument
    assert [r["open"] for r in query.table.to_pylist()] == ["100.00", "101"]
    assert query.table.num_columns == (5 if schema.endswith("v1") else 6)
    assert query.as_dict()["source_verification"] == "UNVERIFIED"


def test_contract_base_and_quote_native_columns_preserve_direct_units(tmp_path, bundle):
    from candle_audit.data_layer import CandidateStore, Window

    bundle["candidate_csv"] = (
        b"timestamp,open,high,low,close,contracts,base_qty,quote_qty\n"
        b"2026-01-01T00:00:00Z,100,102,99,101,20,2,2010\n"
        b"2026-01-01T01:00:00Z,101,103,100,102,30,3,3045\n"
    )
    bundle["metadata"]["volume"]["columns"] = [
        {
            "column": "contracts",
            "unit": "contracts",
            "contract_size": 0.1,
            "contract_size_unit": "base",
        },
        {"column": "base_qty", "unit": "base"},
        {"column": "quote_qty", "unit": "quote"},
    ]
    bundle["metadata"]["content_sha256"] = hashlib.sha256(bundle["candidate_csv"]).hexdigest()
    store = CandidateStore(tmp_path / "store")
    snapshot = publish(store, bundle)
    result = store.query(
        snapshot.snapshot_id,
        Window.parse(bundle["start"], bundle["end"]),
        fields=["contracts", "base_qty", "quote_qty"],
    )
    assert result.table.to_pylist() == [
        {"contracts": "20", "base_qty": "2", "quote_qty": "2010"},
        {"contracts": "30", "base_qty": "3", "quote_qty": "3045"},
    ]


def test_10000_valid_bars_are_accepted_at_the_actual_row_limit(tmp_path, bundle):
    from candle_audit.data_layer import CandidateStore, Window

    start = datetime(2024, 1, 1, tzinfo=UTC)
    csv = ["timestamp,open,high,low,close,qty"]
    native = []
    for i in range(10000):
        instant = start + timedelta(hours=i)
        csv.append(instant.isoformat().replace("+00:00", "Z") + ",100,102,99,101,2")
        native.append(
            [str(1704067200000 + i * 3600000), "100.00", "102", "99", "101", "20", "2", "2010", "1"]
        )
    bundle["start"] = "2024-01-01T00:00:00Z"
    bundle["end"] = (start + timedelta(hours=10000)).isoformat()
    bundle["candidate_csv"] = ("\n".join(csv) + "\n").encode()
    bundle["raw"] = json.dumps({"code": "0", "msg": "", "data": native}).encode()
    bundle["metadata"]["content_sha256"] = hashlib.sha256(bundle["candidate_csv"]).hexdigest()
    store = CandidateStore(tmp_path / "store")
    snapshot = publish(store, bundle)
    assert (
        store.query(
            snapshot.snapshot_id, Window.parse(bundle["start"], bundle["end"])
        ).table.num_rows
        == 10000
    )


def test_fifo_input_fails_promptly_instead_of_blocking_a_bounded_cli(tmp_path, bundle):
    import os

    from r2_instrumentation import run_fifo_cli

    args = arguments(tmp_path, bundle)
    (tmp_path / "raw.json").unlink()
    os.mkfifo(tmp_path / "raw.json")
    result = run_fifo_cli([sys.executable, "-B", "-m", "candle_audit.data_layer", *args])
    assert result.returncode == 1, result.stderr
    assert json.loads(result.stdout)["status"] == "REJECTED"


@pytest.mark.parametrize("origin", ["2024-02-29T00:00:00Z", "2025-12-31T00:00:00Z"])
def test_calendar_boundaries_agree_across_both_kernels_parquet_duckdb_and_cli(
    tmp_path, bundle, origin
):
    from candle_audit.data_layer import CandidateStore, Window

    start = datetime.fromisoformat(origin)
    middle, end = start + timedelta(days=1), start + timedelta(days=2)
    a = calendar.timegm(start.utctimetuple()) * 1_000_000_000
    m = calendar.timegm(middle.utctimetuple()) * 1_000_000_000
    bundle["start"], bundle["end"] = start.isoformat(), end.isoformat()
    bundle["metadata"]["timeframe"], bundle["source"]["bar"] = "1d", "1Dutc"
    bundle["candidate_csv"] = (
        "timestamp,open,high,low,close,qty\n"
        + start.isoformat()
        + ",100,102,99,101,2\n"
        + middle.isoformat()
        + ",101,103,100,102,3\n"
    ).encode()
    native = json.loads(bundle["raw"])
    native["data"][0][0], native["data"][1][0] = str(a // 1_000_000), str(m // 1_000_000)
    bundle["raw"] = json.dumps(native).encode()
    bundle["metadata"]["content_sha256"] = hashlib.sha256(bundle["candidate_csv"]).hexdigest()
    store = CandidateStore(tmp_path / "store")
    snapshot = publish(store, bundle)
    result = store.query(snapshot.snapshot_id, Window.parse(start.isoformat(), middle.isoformat()))
    assert [row["timestamp_ns"] for row in result.table.to_pylist()] == [a]
    cli = subprocess.run(
        [
            sys.executable,
            "-B",
            "-m",
            "candle_audit.data_layer",
            "query",
            "--store",
            str(store.root),
            "--snapshot",
            snapshot.snapshot_id,
            "--start",
            start.isoformat(),
            "--end",
            middle.isoformat(),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert cli.returncode == 0, cli.stderr
    assert json.loads(cli.stdout) == result.as_dict()


def test_96_character_decimal_and_nonrepresentable_float_view_keep_exact_truth(tmp_path, bundle):
    from candle_audit.data_layer import CandidateStore, OfflineOKX, Window

    price = "100." + "0" * 92
    quantity = "1e-999"
    native = json.loads(bundle["raw"])
    for row in native["data"]:
        row[1:5] = [price] * 4
        row[6] = quantity
    bundle["candidate_csv"] = (
        "timestamp,open,high,low,close,qty\n"
        + "\n".join(
            f"2026-01-01T0{i}:00:00Z,{price},{price},{price},{price},{quantity}" for i in range(2)
        )
        + "\n"
    ).encode()
    bundle["raw"] = json.dumps(native).encode()
    bundle["metadata"]["content_sha256"] = hashlib.sha256(bundle["candidate_csv"]).hexdigest()
    store = CandidateStore(tmp_path / "store")
    snapshot = publish(store, bundle)
    assert store.query(snapshot.snapshot_id, Window.parse(bundle["start"], bundle["end"])).table[
        "open"
    ].to_pylist() == [price, price]
    for row in native["data"]:
        row[1:5] = ["1e999"] * 4
    raw = json.dumps(native).encode()
    capture = OfflineOKX(lambda url, method: raw).capture(
        bundle["source"], "1h", Window.parse(bundle["start"], bundle["end"]), raw_time_unit="ms"
    )
    assert capture.view_status == "NOT_REPRESENTABLE" and capture.float_view is None
    assert capture.raw == raw
