"""Bounded immutable artifacts and atomically replaced task checkpoints in controlled roots."""

import fcntl
import hashlib
import os
import re
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from candle_audit.candle_reconcile import MAX_BYTES

from ._input import _directory_descriptor, bounded, controlled_path, read_bounded
from .model import DataLayerError
from .store import _assert_root, _create_file, _temporary_directory, canonical, decode_document


def identifier(value: object) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}", value):
        raise DataLayerError("bounded artifact identifier required")
    return value


def digest(value: object) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise DataLayerError("lowercase SHA256 artifact identity required")
    return value


class Artifacts:
    def __init__(self, root: Path) -> None:
        self.root = controlled_path(root)
        with _directory_descriptor(self.root, create=True):
            pass

    def put(self, content: bytes) -> str:
        bounded(content, MAX_BYTES, "artifact")
        identity = hashlib.sha256(content).hexdigest()
        try:
            with (
                _directory_descriptor(self.root) as parent,
                _temporary_directory(parent, ".blob-") as temp,
            ):
                with _create_file("content", temp.descriptor) as out:
                    out.write(content)
                    out.flush()
                    os.fsync(out.fileno())
                _assert_root(self.root, parent)
                try:
                    os.link(
                        "content",
                        identity,
                        src_dir_fd=temp.descriptor,
                        dst_dir_fd=parent,
                        follow_symlinks=False,
                    )
                except FileExistsError:
                    if read_bounded(Path(identity), MAX_BYTES, dir_fd=parent) != content:
                        raise DataLayerError(
                            "existing artifact contradicts content identity"
                        ) from None
                os.fsync(parent)
                _assert_root(self.root, parent)
        except OSError as exc:
            raise DataLayerError("immutable artifact publication failed") from exc
        return identity

    def get(self, identity: str, *, limit: int = MAX_BYTES) -> bytes:
        digest(identity)
        try:
            with _directory_descriptor(self.root) as parent:
                content = read_bounded(Path(identity), min(limit, MAX_BYTES), dir_fd=parent)
                _assert_root(self.root, parent)
            if hashlib.sha256(content).hexdigest() != identity:
                raise DataLayerError("artifact integrity mismatch")
            return content
        except OSError as exc:
            raise DataLayerError("artifact unavailable or unsafe") from exc

    def export(self, identity: str, suffix: str) -> Path:
        """Give immutable bytes a browser/document suffix inside the same held output root."""
        if suffix not in (".html", ".json", ".parquet"):
            raise DataLayerError("unsupported artifact export type")
        content = self.get(identity)
        name = identity + suffix
        try:
            with _directory_descriptor(self.root) as parent:
                try:
                    os.link(
                        identity, name, src_dir_fd=parent, dst_dir_fd=parent, follow_symlinks=False
                    )
                except FileExistsError:
                    if read_bounded(Path(name), MAX_BYTES, dir_fd=parent) != content:
                        raise DataLayerError("export conflicts with immutable content") from None
                _assert_root(self.root, parent)
        except OSError as exc:
            raise DataLayerError("artifact export unavailable or unsafe") from exc
        return self.root / name

    def save_state(self, key: str, document: object) -> None:
        name = "state-" + identifier(key) + ".json"
        content = bounded(canonical(document), 262144, "task checkpoint")
        try:
            with (
                _directory_descriptor(self.root) as parent,
                _temporary_directory(parent, ".state-") as temp,
            ):
                with _create_file("content", temp.descriptor) as out:
                    out.write(content)
                    out.flush()
                    os.fsync(out.fileno())
                _assert_root(self.root, parent)
                os.replace("content", name, src_dir_fd=temp.descriptor, dst_dir_fd=parent)
                os.fsync(parent)
                _assert_root(self.root, parent)
        except OSError as exc:
            raise DataLayerError(
                "atomic checkpoint publication failed; "
                "prior state retained if replace did not occur"
            ) from exc

    def load_state(self, key: str) -> dict[str, Any] | None:
        name = "state-" + identifier(key) + ".json"
        try:
            with _directory_descriptor(self.root) as parent:
                try:
                    content = read_bounded(Path(name), 262144, dir_fd=parent)
                except FileNotFoundError:
                    return None
                _assert_root(self.root, parent)
            return decode_document(content)
        except OSError as exc:
            raise DataLayerError("checkpoint unavailable or unsafe") from exc

    @contextmanager
    def lock(self, key: str) -> Iterator[None]:
        name = "lock-" + identifier(key)
        try:
            with _directory_descriptor(self.root) as parent:
                fd = os.open(
                    name,
                    os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK,
                    0o600,
                    dir_fd=parent,
                )
                try:
                    if not stat.S_ISREG(os.fstat(fd).st_mode):
                        raise DataLayerError("task lock must be a regular file")
                    try:
                        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except BlockingIOError as exc:
                        raise DataLayerError("task already running") from exc
                    _assert_root(self.root, parent)
                    yield
                    _assert_root(self.root, parent)
                finally:
                    os.close(fd)
        except OSError as exc:
            raise DataLayerError("task lock unavailable") from exc
