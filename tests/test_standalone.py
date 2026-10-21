"""Independent workspaces and the real packaged synthetic consumer."""

from importlib.resources import files
from pathlib import Path

import pytest

from candle_audit.data_layer import CandidateStore, DataLayerError
from candle_audit.data_layer._input import controlled_path, project_root
from candle_audit.data_layer.demo import run_demo


def test_current_directory_workspace_needs_no_source_tree_or_project_file(tmp_path, monkeypatch):
    monkeypatch.delenv("CANDLE_AUDIT_WORKSPACE", raising=False)
    monkeypatch.chdir(tmp_path)
    assert not (tmp_path / "pyproject.toml").exists()
    assert project_root() == tmp_path
    store = CandidateStore(Path("data/local"))
    assert store.root == tmp_path / "data/local"
    assert store.root.is_dir()


@pytest.mark.parametrize("value", ["", ".", "../workspace", "/tmp/../tmp"])
def test_explicit_workspace_cannot_be_relative_empty_or_traversing(monkeypatch, value):
    monkeypatch.setenv("CANDLE_AUDIT_WORKSPACE", value)
    with pytest.raises(DataLayerError):
        project_root()


def test_existing_external_workspace_is_explicit_and_rejects_escape(tmp_path, monkeypatch):
    workspace = tmp_path / "workspace"
    external = tmp_path / "external"
    workspace.mkdir()
    external.mkdir()
    sentinel = external / "retained.txt"
    sentinel.write_text("retained")
    (workspace / "data").mkdir()
    (workspace / "data/escape").symlink_to(external, target_is_directory=True)
    monkeypatch.setenv("CANDLE_AUDIT_WORKSPACE", str(workspace))
    assert project_root() == workspace
    for path in [workspace / "data/escape/new", workspace / "data/../new", external / "new"]:
        with pytest.raises(DataLayerError):
            CandidateStore(path)
    assert list(external.iterdir()) == [sentinel]
    assert sentinel.read_text() == "retained"
    assert controlled_path(workspace / "data/valid") == workspace / "data/valid"


def test_explicit_workspace_symlink_is_refused(tmp_path, monkeypatch):
    target = tmp_path / "target"
    target.mkdir()
    link = tmp_path / "alias"
    link.symlink_to(target, target_is_directory=True)
    monkeypatch.setenv("CANDLE_AUDIT_WORKSPACE", str(link))
    with pytest.raises(DataLayerError):
        project_root()
    assert list(target.iterdir()) == []


def test_packaged_demo_runs_actual_pipeline_and_preserves_changed_inputs(tmp_path, monkeypatch):
    monkeypatch.setenv("CANDLE_AUDIT_WORKSPACE", str(tmp_path))
    result = run_demo(tmp_path / "data/demo")
    assert result["status"] == "SYNTHETIC_DEMO_COMPLETE"
    assert result["rows"] == 8 and result["aggregate_rows"] == 2
    assert result["network_used"] is False
    assert result["source_verification"] == "UNVERIFIED"
    html = Path(result["report"]["html_path"]).read_text()
    assert "工程合成测试数据" in html and "100.00" in html
    original = tmp_path / "data/demo/inputs/bars.csv"
    original.write_bytes(b"retained changed input")
    with pytest.raises(DataLayerError, match="refuses to overwrite"):
        run_demo(tmp_path / "data/demo")
    assert original.read_bytes() == b"retained changed input"


def test_packaged_demo_inputs_match_complete_source_examples():
    root = Path(__file__).resolve().parents[1]
    resources = files("candle_audit.data_layer").joinpath("_demo_data")
    for name in ("bars.csv", "raw.json", "metadata.json", "source.json"):
        assert resources.joinpath(name).read_bytes() == (root / "examples/synthetic" / name).read_bytes()
