"""The approved official SDK/security pins must agree with installed metadata and actual calls."""

import importlib.metadata

import ccxt


def test_approved_sdk_and_multidict_are_installed_without_conflicting_metadata():
    assert importlib.metadata.version("ccxt") == ccxt.__version__ == "4.5.64"
    assert importlib.metadata.version("multidict") == "6.9.1"
    requirements = importlib.metadata.requires("ccxt")
    assert requirements is not None
    assert not any("multidict==6.7.1" in requirement for requirement in requirements)
    assert importlib.metadata.version("pyarrow") == "25.0.0"
    assert importlib.metadata.version("duckdb") == "1.5.6"
    assert importlib.metadata.version("pandas") == "3.0.3"
    assert importlib.metadata.version("numpy") == "2.4.6"
