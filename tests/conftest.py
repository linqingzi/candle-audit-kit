"""Independent owned workspaces; preserve path restrictions and disable network in tests."""

import os
import socket
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = ROOT / ".tmp-data-layer"


def pytest_configure(config):
    SCRATCH.mkdir(exist_ok=True)
    os.environ["CANDLE_AUDIT_WORKSPACE"] = str(ROOT)
    os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
    os.environ["PYTHONPATH"] = os.pathsep.join(
        filter(None, (str(ROOT / "src"), os.environ.get("PYTHONPATH", "")))
    )
    os.environ["PIP_NO_INDEX"] = "1"
    os.environ["PIP_DISABLE_PIP_VERSION_CHECK"] = "1"
    if config.option.basetemp is None:
        config.option.basetemp = str(SCRATCH / "pytest-owned")


def pytest_collection_modifyitems(items):
    for item in items:
        if hasattr(item.obj, "hypothesis"):
            item.add_marker(pytest.mark.property)


@pytest.fixture(autouse=True)
def deny_test_network(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("offline tests must not open network connections")

    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket.socket, "connect_ex", forbidden)
