"""Observe the actual native connection, including its denied local-file reader."""

import duckdb
import pytest
from test_store import publish

from candle_audit.data_layer import CandidateStore, Window


def test_native_duckdb_connection_keeps_all_resource_and_external_access_controls(
    tmp_path, bundle, monkeypatch
):
    store = CandidateStore(tmp_path / "store")
    snapshot = publish(store, bundle)
    local = tmp_path / "synthetic.csv"
    local.write_text("x\n1\n")
    native = duckdb.connect
    observed = []

    def checked(*args, **kwargs):
        connection = native(*args, **kwargs)
        settings = connection.execute(
            "SELECT current_setting('enable_external_access'), "
            "current_setting('autoinstall_known_extensions'), "
            "current_setting('autoload_known_extensions'), current_setting('threads')"
        ).fetchone()
        observed.append(settings)
        assert settings == (False, False, False, 1)
        assert kwargs["config"]["memory_limit"] == "128MB"
        with pytest.raises(duckdb.PermissionException, match="disabled"):
            connection.execute("SELECT * FROM read_csv(?)", [str(local)]).fetchall()
        return connection

    monkeypatch.setattr(duckdb, "connect", checked)
    result = store.query(snapshot.snapshot_id, Window.parse(bundle["start"], bundle["end"]))
    assert observed == [(False, False, False, 1)]
    assert result.table["open"].to_pylist() == ["100.00", "101"]
