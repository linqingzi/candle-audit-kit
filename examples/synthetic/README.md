# Offline synthetic example

All values and contract rules are invented engineering inputs, not real market observations.
The four supplied files are complete inputs for candidate ingestion. The packaged `candle-audit
demo --root data/demo` command consumes identical bytes and additionally writes complete
collection `request.json` and `pages.json`, stores eight candles, aggregates two 4h candles
and produces an offline report. See the root README for a clean-workspace walkthrough.

Time window: 2026-01-01T00:00:00Z to 2026-01-01T08:00:00Z, [start,end).
Rules: declared synthetic tick 0.01, contract lot 1, base/contract 0.1.
No accounts, secrets, real acquisition or external trust identities are included.
