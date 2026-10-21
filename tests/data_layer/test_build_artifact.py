"""The source distribution must carry its actual contract, code and retained test inputs."""

import subprocess
import sys
import tarfile
from pathlib import Path


def test_sdist_carries_interface_documentation_and_all_six_retained_inputs(tmp_path):
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "-B", "-m", "build", "--no-isolation", "--outdir", str(tmp_path / "dist")],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    with tarfile.open(tmp_path / "dist/candle_audit_kit-0.1.0rc1.tar.gz") as archive:
        names = archive.getnames()
        assert "candle_audit_kit-0.1.0rc1/docs/data_layer_m1.md" in names
        for name in (
            "docs/data_layer_m1.md",
            "README.md",
            "src/candle_audit/candle_quality.py",
            "src/candle_audit/candle_reconcile.py",
            "tests/unit/test_candle_quality.py",
            "tests/unit/test_candle_reconcile.py",
            "tests/fixtures/legacy/raw.json",
            "tests/fixtures/legacy/bars.csv",
        ):
            assert (
                archive.extractfile("candle_audit_kit-0.1.0rc1/" + name).read() == (root / name).read_bytes()
            )
