"""Installed distributions must preserve the license texts promised by their notices."""

import subprocess
import sys
import zipfile
from pathlib import Path


def test_wheel_preserves_project_and_dependency_license_texts(tmp_path):
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, "-B", "-m", "build", "--wheel", "--no-isolation",
         "--outdir", str(tmp_path / "wheel")],
        cwd=root, capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    wheels = list((tmp_path / "wheel").glob("*.whl"))
    assert len(wheels) == 1
    retained = [root / name for name in
                ("LICENSE", "LICENSE-STATUS.md", "PROVENANCE.md", "THIRD_PARTY_NOTICES.md")]
    retained += [p for p in (root / "licenses").rglob("*") if p.is_file()]
    assert any("third-party" in p.parts for p in retained)
    with zipfile.ZipFile(wheels[0]) as wheel:
        metadata = wheel.read("candle_audit_kit-0.1.0rc2.dist-info/METADATA").decode()
        assert "License-Expression: MIT\n" in metadata
        for path in retained:
            member = ("candle_audit_kit-0.1.0rc2.dist-info/licenses/"
                      + path.relative_to(root).as_posix())
            assert member in wheel.namelist(), "missing license text: " + str(path.relative_to(root))
            assert wheel.read(member) == path.read_bytes()
