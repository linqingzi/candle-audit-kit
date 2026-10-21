# Candidate ingestion and bounded queries

The public `candle_audit.data_layer` API accepts exact metadata/source declarations, original
CSV/raw immutable bytes and an explicit `Window`. It supports OKX SPOT and linear USDT SWAP
trade candles. It rejects unsupported venues, guessed units, inverse contracts and mark/index.

Create a clean caller-owned workspace and run `candle-audit demo --root data/demo`. This
creates all four ingestion inputs plus a complete collection request and offline pages list.

```sh
candle-audit ingest --store data/manual-candidates \
  --csv data/demo/inputs/bars.csv --raw data/demo/inputs/raw.json \
  --metadata data/demo/inputs/metadata.json --source data/demo/inputs/source.json \
  --start 2026-01-01T00:00:00Z --end 2026-01-01T08:00:00Z --raw-time-unit ms
candle-audit query --store data/manual-candidates --snapshot SNAPSHOT_ID \
  --start 2026-01-01T00:00:00Z --end 2026-01-01T01:00:00Z
```

Replace SNAPSHOT_ID with the actual ID printed by ingest. The query returns the first bar.
Changing the start to 00:00:00.000000001Z gives a typed empty query for that one-hour window.

```python
from pathlib import Path
from candle_audit.data_layer import CandidateStore, Window
store = CandidateStore(Path("data/manual-candidates"))
# result = store.query(actual_snapshot_id, Window.parse(start_utc, end_utc))
```

The workspace is CANDLE_AUDIT_WORKSPACE or the current directory. It need not contain a
pyproject file. Only data/ and .tmp-data-layer/ are controlled data roots. Input traversal,
symlink escapes, FIFO/nonregular files, oversized files and arbitrary SQL remain refused.
Only the two bundled legacy fixtures have the retained narrow CLI fixture exception.

Original decimal text and bytes remain authoritative; SDK floats are labeled non-authoritative.
Source hashes and matching bytes do not authenticate an exchange. Candidate outputs remain
UNVERIFIED / NOT_EVALUATED. Query row/byte budgets, schema, source identity and semantic replay
are checked on every read. Metadata/source keys and quantity units are explicit.
