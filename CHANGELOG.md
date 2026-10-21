# Changelog

## 0.1.0rc2 — 2026-10-21 (prerelease)

- Add an install-free static entry for reading the real English/Chinese synthetic reports,
  followed by a versioned ZIP and local demo instructions. The page accepts no visitor files.
- Align the package version, candidate download, bilingual quick start and packaging checks.

- Include optional English HTML views through `demo --language en` and `report --language en`.
  Chinese remains the default; machine-readable JSON evidence, exact data and validation rules
  are unchanged. The existing 0.1.0rc1 release/tag/archive is not replaced.
- Replace the English README's Chinese report image with an actual generated English report;
  retain the original Chinese image and disclose capture/reproduction details.

## 0.1.0rc1 — 2026-10-21

- Standalone `candle-audit-kit` distribution, `candle_audit` imports and `candle-audit` command.
- Caller-owned workspace, preserving bounded file reads, traversal/symlink checks, fixed query
  fields, exact decimals, immutable data and verification scratch cleanup.
- Six declared runtime dependencies; exact isolated validation environment lock.
- Complete packaged synthetic example, bilingual quick start and standalone test setup.
- Removed unrelated application configuration, hidden metadata and internal evidence references.
- Adopted MIT for project material after the maintainer confirmed development under their
  direction with Codex and authorized MIT. Dependency copyright/terms remain unchanged.
  This initial standalone release does not certify market data.
- Both source and wheel distributions retain the original dependency license texts; a wheel
  contents test verifies every promised license file byte-for-byte.
- Three unavailable pre-standalone historical-artifact cases are explicitly skipped; added
  three real current-format byte-preserving relocation controls, without fabricating history.

This pre-release uses independent schema names. It does not promise compatibility with private
application snapshots or persisted datasets created by them.
