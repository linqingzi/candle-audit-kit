"""Controlled synthetic re-signing for the precise adjudicated reader counterexamples."""

import hashlib
import json
import shutil
import tempfile
from pathlib import Path

import pyarrow.parquet as pq

from candle_audit import candle_quality, candle_reconcile
from candle_audit.data_layer import Window
from candle_audit.data_layer.store import FILE_LIMITS, _table, canonical


def resign(snapshot, changes=None, *, rebuild=False, table=None):
    directory = Path(tempfile.mkdtemp(prefix=".test-r2-", dir=snapshot.path.parent))
    for path in snapshot.path.iterdir():
        assert not path.is_symlink()
        shutil.copyfile(path, directory / path.name)
    for name, value in (changes or {}).items():
        (directory / name).write_bytes(value)
    manifest = json.loads((directory / "manifest.json").read_bytes())
    metadata = json.loads((directory / "metadata.json").read_bytes())
    source = json.loads((directory / "source.json").read_bytes())
    if rebuild:
        window = Window(**manifest["window"])
        table = _table((directory / "raw.json").read_bytes(), metadata, source, window)
    if table is not None:
        pq.write_table(table, directory / "bars.parquet", compression="zstd")
        manifest["rows"] = table.num_rows
    for name in FILE_LIMITS:
        content = (directory / name).read_bytes()
        manifest["files"][name] = {
            "sha256": hashlib.sha256(content).hexdigest(),
            "bytes": len(content),
        }
    identity = {k: v for k, v in manifest.items() if k != "snapshot_id"}
    key = hashlib.sha256(canonical(identity)).hexdigest()
    manifest["snapshot_id"] = key
    (directory / "manifest.json").write_bytes(canonical(manifest))
    target = directory.parent / key
    directory.rename(target)
    return key, target


def original_reports(directory):
    metadata = json.loads((directory / "metadata.json").read_bytes())
    source = json.loads((directory / "source.json").read_bytes())
    manifest = json.loads((directory / "manifest.json").read_bytes())
    start, end = Window(**manifest["window"]).iso_bounds()
    context = {k: source[k] for k in ("venue", "instId", "instType", "base_asset", "quote_asset")}
    return {
        "quality": candle_quality.validate(
            directory / "candidate.csv", metadata, context, start, end
        ),
        "reconcile": candle_reconcile.reconcile(
            directory / "candidate.csv", metadata, directory / "raw.json", source, start, end
        ),
    }
