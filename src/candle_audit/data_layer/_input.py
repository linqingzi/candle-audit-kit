"""Shared bounded input reads before optional data-engine imports."""

import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from .model import DataLayerError


def project_root() -> Path:
    """Return the caller's workspace, independently of installed package ancestry.

    Only data/ and .tmp-data-layer/ inside this existing directory are permitted.
    An explicit workspace must be absolute and cannot contain traversal or symlinks.
    No directory is created by this function.
    """
    configured = os.environ.get("CANDLE_AUDIT_WORKSPACE")
    if configured is not None:
        root = Path(configured)
        if not configured or not root.is_absolute() or ".." in root.parts:
            raise DataLayerError("workspace must be an existing absolute directory without traversal")
    else:
        root = Path.cwd()
    if not root.is_dir() or root.absolute() != root.resolve():
        raise DataLayerError("workspace must exist and must not contain directory symlinks")
    return root


def controlled_path(path: Path) -> Path:
    """Resolve before opening; reject traversal and symlink escapes from candidate output roots."""
    if ".." in path.parts:
        raise DataLayerError("path traversal is not a controlled path")
    root = project_root()
    resolved = path.resolve()
    allowed = (root / ".tmp-data-layer", root / "data")
    if not any(resolved.is_relative_to(parent) for parent in allowed):
        raise DataLayerError("path must stay inside controlled candidate directories")
    return resolved


def bounded(content: object, limit: int, name: str) -> bytes:
    if not isinstance(content, bytes) or len(content) > limit:
        raise DataLayerError(f"{name} requires immutable bytes within its byte budget")
    return content


@contextmanager
def _directory_descriptor(
    path: Path, *, dir_fd: int | None = None, create: bool = False
) -> Iterator[int]:
    """Anchor every directory component; later relative opens keep that exact inode."""
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    descriptor = os.open("/", flags) if dir_fd is None else os.dup(dir_fd)
    components = path.absolute().parts[1:] if dir_fd is None else path.parts
    try:
        for component in components:
            try:
                child = os.open(component, flags, dir_fd=descriptor)
            except FileNotFoundError:
                if not create:
                    raise
                os.mkdir(component, mode=0o700, dir_fd=descriptor)
                child = os.open(component, flags, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        yield descriptor
    finally:
        os.close(descriptor)


def read_bounded(path: Path, limit: int, *, dir_fd: int | None = None) -> bytes:
    """Do not follow a file symlink; never allocate an unbounded source/artifact."""
    if dir_fd is None:
        with _directory_descriptor(path.parent) as descriptor:
            return read_bounded(path, limit, dir_fd=descriptor)
    with os.fdopen(
        os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=dir_fd), "rb"
    ) as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise DataLayerError("only regular input/artifact files are supported")
        content = stream.read(limit + 1)
    return bounded(content, limit, path.name)
