# Feedback and first contributions

Start with the [offline demo](../README.md#quick-start). [Open an issue](https://github.com/linqingzi/candle-audit-kit/issues/new) for a reproducible problem or a small proposal; [existing issues](https://github.com/linqingzi/candle-audit-kit/issues) and [pull requests](https://github.com/linqingzi/candle-audit-kit/pulls) show actual activity.

## Report a problem

Include your OS and Python version, the command you ran, expected versus actual output, and a minimal synthetic input if needed. Replace private paths with generic placeholders. Do not attach API keys, accounts, restricted market data or your complete research workspace. A screenshot of the synthetic report is useful; a copied error message is often easier to search.

## Small, currently unclaimed tasks

These are proposed contributions, not claims of existing issues or users. Check the issue list and describe your scope before starting.

1. **English report glossary.** The generated HTML report currently uses Chinese labels. Add an English guide mapping its seven quality rows and `PASS`, `UNVERIFIED`, `UNKNOWN` and `VERIFY` states to the real eight-candle demo. Acceptance: link each explanation to the existing report/JSON fields; keep missing market evidence explicit; no runtime or admission changes required.
2. **Document one reproducible Linux validation.** The published acceptance evidence covers macOS arm64. Run the locked synthetic example and test suite on one supported Linux/CPython combination, recording exact versions and passed/failed/skipped counts. Acceptance: share commands and sanitized results, including failures; do not mark an untested platform as verified or weaken any sample count or path protection.

The first task is documentation only. The second adds evidence, not an assumption of platform support. Neither task involves trading strategies, private integrations, real data downloads or credentials.

## Before a pull request

Use an isolated environment. The locked setup and test/build commands are in [the README](../README.md#reproduce-tests-and-packaging). Keep all 1000-example property settings and explicit historical-artifact skips. If you change a distributed source file, regenerate its entry in `CHECKSUMS.sha256` and run `python -B scripts/verify_checksums.py`; include new canonical source files as well. Preserve [MIT](../LICENSE), [third-party notices](../THIRD_PARTY_NOTICES.md) and original license texts.

Prefer one focused change, explain its behavior and validation, and include only synthetic fixtures. No contributor license agreement is added by this project. Reading and running the tool requires no service account; opening a GitHub issue or pull request uses a GitHub account.
