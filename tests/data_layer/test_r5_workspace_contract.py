"""Real workspace and lexical path contracts with owned temporary inputs."""

from pathlib import Path

import pytest

from candle_audit.data_layer import CandidateStore, DataLayerError, _input


def test_real_workspace_supports_candidate_store(tmp_path: Path) -> None:
    workspace = Path(__file__).resolve().parents[2]
    candidate = tmp_path / "candidates"

    assert (workspace / "pyproject.toml").is_file()
    assert _input.project_root() == workspace
    assert _input.controlled_path(candidate) == candidate.resolve()
    assert not candidate.exists()

    store = CandidateStore(candidate)

    assert store.root == candidate.resolve()
    assert candidate.is_dir()
    assert list(candidate.iterdir()) == []
    assert set(tmp_path.iterdir()) == {candidate}


def test_missing_workspace_rejects_helper_and_public_store(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = Path(__file__).resolve().parents[2]

    assert not (tmp_path / "pyproject.toml").exists()
    # Explicit invalid workspaces fail closed and must not be created by a read/helper.
    with monkeypatch.context() as module_locator:
        module_locator.setenv("CANDLE_AUDIT_WORKSPACE", str(tmp_path / "missing"))
        reason = r"^workspace must exist and must not contain directory symlinks$"
        with pytest.raises(DataLayerError, match=reason):
            _input.project_root()
        with pytest.raises(DataLayerError, match=reason):
            CandidateStore(tmp_path / "candidates")

    assert _input.project_root() == workspace
    assert list(tmp_path.iterdir()) == []


def test_public_store_rejects_lexical_parent_before_creating_directories(tmp_path: Path) -> None:
    candidate = tmp_path / "candidates"
    lexical = tmp_path / "not-created" / ".." / "candidates"
    assert lexical.resolve() == candidate.resolve()
    assert list(tmp_path.iterdir()) == []

    with pytest.raises(DataLayerError, match=r"^path traversal is not a controlled path$"):
        CandidateStore(lexical)

    assert list(tmp_path.iterdir()) == []
    store = CandidateStore(candidate)
    assert store.root == candidate.resolve()
    assert candidate.is_dir()
    assert list(candidate.iterdir()) == []
    assert set(tmp_path.iterdir()) == {candidate}


def test_public_store_rejects_lexical_parent_with_existing_middle(tmp_path: Path) -> None:
    middle = tmp_path / "existing-middle"
    middle.mkdir()
    retained = middle / "retained.txt"
    retained.write_bytes(b"owned middle content")
    candidate = tmp_path / "candidates"
    lexical = middle / ".." / "candidates"
    before_layout = set(tmp_path.rglob("*"))
    before_content = retained.read_bytes()

    assert middle.is_dir()
    assert set(middle.iterdir()) == {retained}
    assert before_layout == {middle, retained}
    assert not candidate.exists()
    assert lexical.resolve() == candidate.resolve()

    with pytest.raises(DataLayerError, match=r"^path traversal is not a controlled path$"):
        CandidateStore(lexical)

    assert not candidate.exists()
    assert set(tmp_path.rglob("*")) == before_layout
    assert middle.is_dir()
    assert retained.read_bytes() == before_content

    store = CandidateStore(candidate)
    assert store.root == candidate.resolve()
    assert candidate.is_dir()
    assert list(candidate.iterdir()) == []
    assert set(tmp_path.rglob("*")) == before_layout | {candidate}
    assert middle.is_dir()
    assert retained.read_bytes() == before_content
