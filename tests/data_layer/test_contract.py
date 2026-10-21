"""Regression tests for the minimal migrated-kernel support interface."""

import hashlib


def test_exact_v2_metadata_support_preserves_the_declaration():
    from candle_audit.contract.data_versions import metadata

    declaration = {
        "schema": "candle-audit-data-version-v2",
        "name": "contract-test",
        "version": "v1",
        "source": "synthetic-engineering",
        "provenance": "synthetic-engineering/test-only",
        "market": "BTC-USDT-SWAP",
        "currency": "USDT",
        "timeframe": "1h",
        "timezone": "UTC",
        "kind": "synthetic",
        "content_sha256": hashlib.sha256(b"test-only").hexdigest(),
        "volume": {
            "base_asset": "BTC",
            "quote_asset": "USDT",
            "columns": [{"column": "qty", "unit": "base"}],
        },
    }
    result = metadata(declaration)
    assert result == declaration
    result["volume"]["columns"][0]["unit"] = "quote"
    assert declaration["volume"]["columns"][0]["unit"] == "base"
