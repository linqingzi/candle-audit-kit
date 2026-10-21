"""An independently observed write invariant, with >=1000 real generated valid inputs."""

import os
import tempfile
from contextlib import suppress
from pathlib import Path
from unittest.mock import patch

from hypothesis import given, settings
from hypothesis import strategies as st
from test_r2_properties import case
from test_r3_writer_directory_race import peer_identity

from candle_audit.data_layer import CandidateStore, DataLayerError
from candle_audit.data_layer import store as implementation

ROOT = Path(__file__).resolve().parents[2]


@settings(max_examples=1000, deadline=None, derandomize=True)
@given(quantity=st.integers(1, 10**8), hour=st.integers(0, 100000))
def test_writer_root_inode_binding_never_creates_peer_artifacts(quantity, hour):
    csv, metadata, raw, source, window, _ = case(quantity, hour)
    with tempfile.TemporaryDirectory(
        dir=ROOT / ".tmp-data-layer", prefix="writer-property-"
    ) as temp:
        home = Path(temp)
        store = CandidateStore(home / "store")
        peer, saved = home / "peer", home / "saved"
        peer.mkdir()
        (peer / "marker").write_bytes(b"synthetic-engineering/test-only")
        before = peer_identity(peer)
        real_mkdir, real_open = os.mkdir, os.open
        swapped = False
        effects = []

        def observe():
            if peer_identity(peer) != before:
                effects.append(sorted(peer_identity(peer)))

        def mkdir(path, *args, **kwargs):
            nonlocal swapped
            if Path(path).name.startswith(".incoming-") and not swapped:
                store.root.rename(saved)
                store.root.symlink_to(peer, target_is_directory=True)
                swapped = True
            result = real_mkdir(path, *args, **kwargs)
            observe()
            return result

        def opened(*args, **kwargs):
            result = real_open(*args, **kwargs)
            observe()
            return result

        with (
            patch.object(implementation.os, "mkdir", mkdir),
            patch.object(implementation.os, "open", opened),
            suppress(DataLayerError, OSError),
        ):
            store.ingest(csv, metadata, raw, source, window, raw_time_unit="ms")
        assert swapped and effects == []
        assert peer_identity(peer) == before
        assert list(saved.glob(".incoming-*")) == []
