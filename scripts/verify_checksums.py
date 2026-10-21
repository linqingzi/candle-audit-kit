"""Check the distributed source files against CHECKSUMS.sha256 (no downloads)."""

import hashlib
from pathlib import Path

root = Path(__file__).resolve().parents[1]
count = 0
for line in (root / "CHECKSUMS.sha256").read_text().splitlines():
    expected, name = line.split("  ", 1)
    relative = Path(name)
    if relative.is_absolute() or ".." in relative.parts:
        raise SystemExit("unsafe manifest member")
    path = root / relative
    if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(root):
        raise SystemExit("missing/unsafe member: " + name)
    if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        raise SystemExit("checksum mismatch: " + name)
    count += 1
print(f"Verified {count} source files")
