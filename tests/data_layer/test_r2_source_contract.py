"""Public producer and its own reader share the approved narrow declaration boundary."""

import hashlib

import pytest
from test_store import publish

from candle_audit.data_layer import CandidateStore, DataLayerError, Window


def v1_long_asset(bundle, length):
    base = "A" * length
    instrument = base + "-USDT-SWAP"
    bundle["source"].update(base_asset=base, instId=instrument)
    bundle["metadata"].update(schema="candle-audit-data-version-v1", market=instrument)
    del bundle["metadata"]["volume"]
    bundle["candidate_csv"] = (
        b"\n".join(b",".join(line.split(b",")[:5]) for line in bundle["candidate_csv"].splitlines())
        + b"\n"
    )
    bundle["metadata"]["content_sha256"] = hashlib.sha256(bundle["candidate_csv"]).hexdigest()


def test_public_ingest_refuses_241_asset_251_instrument_before_publication(tmp_path, bundle):
    v1_long_asset(bundle, 241)
    store = CandidateStore(tmp_path / "store")
    with pytest.raises(DataLayerError, match="bounded source"):
        publish(store, bundle)
    assert list(store.root.iterdir()) == []


def test_allowed_240_instrument_boundary_is_consumed_by_its_own_reader(tmp_path, bundle):
    v1_long_asset(bundle, 230)
    store = CandidateStore(tmp_path / "store")
    snapshot = publish(store, bundle)
    result = store.query(snapshot.snapshot_id, Window.parse(bundle["start"], bundle["end"]))
    assert result.source["instId"] == "A" * 230 + "-USDT-SWAP"
    assert result.table.num_rows == 2
    assert result.table["open"].to_pylist() == ["100.00", "101"]
