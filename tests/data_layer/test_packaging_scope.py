"""Native source archives contain the current canonical project, without internal copies."""

import json
import os
import subprocess
import sys
import tarfile
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[2]
ARCHIVE_ROOT = "candle_audit_kit-0.1.0rc1"
ROOT_FILES = {"README.md", "README.zh-TW.md", "pyproject.toml", ".gitignore",
              "CHANGELOG.md", "LICENSE", "LICENSE-STATUS.md", "PROVENANCE.md", "THIRD_PARTY_NOTICES.md",
              "RELEASE_STATUS.md", "requirements-test-lock.txt", "CHECKSUMS.sha256"}
FIXTURES = {
    "tests/fixtures/legacy/bars.csv",
    "tests/fixtures/legacy/raw.json",
}
INTERNAL_PARTS = {
    ".worktrees",
    ".hermes",
    ".tmp-data-layer",
    ".git",
    "runtime",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".hypothesis",
    ".cache",
    "cache",
}


def test_native_sdist_contains_exact_canonical_project_files(tmp_path: Path) -> None:
    output = tmp_path / "dist"
    argv = [
        sys.executable,
        "-B",
        "-m",
        "build",
        "--sdist",
        "--no-isolation",
        "--outdir",
        str(output),
    ]
    result = subprocess.run(argv, cwd=ROOT, capture_output=True, text=True, check=False)
    (tmp_path / "native-build.stdout.log").write_text(result.stdout)
    (tmp_path / "native-build.stderr.log").write_text(result.stderr)
    (tmp_path / "native-build.command.json").write_text(
        json.dumps(
            {
                "argv": argv,
                "cwd": str(ROOT),
                "exit_code": result.returncode,
                "environment": {
                    key: os.environ.get(key)
                    for key in (
                        "PYTHONPATH",
                        "PYTHONDONTWRITEBYTECODE",
                        "TMPDIR",
                        "TMP",
                        "TEMP",
                    )
                },
            },
            indent=2,
        )
        + "\n"
    )
    assert result.returncode == 0, result.stdout + result.stderr
    archives = list(output.glob("*.tar.gz"))
    assert len(archives) == 1

    expected = ROOT_FILES | FIXTURES
    for directory in ("src", "tests", "docs", "examples", "scripts", "licenses"):
        expected.update(
            path.relative_to(ROOT).as_posix()
            for path in (ROOT / directory).rglob("*")
            if path.is_file()
            and not (set(path.relative_to(ROOT).parts) & INTERNAL_PARTS)
            and path.name != ".DS_Store"
            and path.suffix not in {".pyc", ".pyo", ".pyd"}
        )

    with tarfile.open(archives[0]) as archive:
        members = archive.getmembers()
        names = [member.name for member in members]
        (tmp_path / "archive-members.json").write_text(
            json.dumps(
                [
                    {"name": member.name, "type": member.type.decode("ascii"), "bytes": member.size}
                    for member in members
                ],
                indent=2,
            )
            + "\n"
        )
        assert len(names) == len(set(names)), "duplicate archive members"
        invalid = []
        files = {}
        for member in members:
            path = PurePosixPath(member.name)
            if (
                path.is_absolute()
                or ".." in path.parts
                or "\\" in member.name
                or path.as_posix() != member.name
                or path.parts[0] != ARCHIVE_ROOT
                or not member.isfile()
            ):
                invalid.append(member.name)
            files["/".join(path.parts[1:])] = member
        assert not invalid, f"unsafe or nonregular archive members: {invalid}"
        unexpected = sorted(set(files) - expected - {"PKG-INFO"})
        assert not unexpected, f"non-canonical archive members: {unexpected}"
        assert set(files) == expected | {"PKG-INFO"}, "required canonical files missing"
        for name in expected:
            assert archive.extractfile(files[name]).read() == (ROOT / name).read_bytes(), name
        # Hatch bundles root Git exclusion rules and generates standard distribution metadata.
        metadata = archive.extractfile(files["PKG-INFO"]).read().decode()
        assert "Metadata-Version:" in metadata
        assert "Name: candle-audit-kit\n" in metadata
        assert "Version: 0.1.0rc1\n" in metadata
