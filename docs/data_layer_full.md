# Dataset collections, releases and reports

Pipeline: InstrumentRules → CollectionRequest → ProviderRegistry/Collector → VERIFY collection
→ ReleaseStore → DatasetReader. The complete offline demo exercises those real objects,
canonical Parquet, queries, aggregation and reports on eight invented candles.

```sh
candle-audit demo --root data/demo --language en
candle-audit collect --root data/imported --request data/demo/inputs/request.json --pages data/demo/inputs/pages.json
# Use the actual returned collection/release identifiers for subsequent commands.
candle-audit release --help
candle-audit read --help
candle-audit aggregate --help
candle-audit report --help
```

`request.json` is CollectionRequest.as_dict(); rules include their original synthetic evidence
bytes. `pages.json` identifies bounded files within the caller workspace, not remote URLs.
Only explicit trusted provider objects are registered; there is no dynamic import/eval plugin.

## Exact values, time and revisions

UTC integer-ns opening windows are [start,end). Prices and quantities retain decimal text;
base, quote and contract units are distinct. Arrow/Parquet schema and raw/CSV correspondence
are rechecked. DuckDB is in-memory with external files/network/extensions disabled, fixed
projection fields and parameterized windows. Reader results have a 10000-row budget.

Collection preserves pages, original bytes, row locations, attempts and checkpoints. Empty,
unclosed, repeated, conflicting or missing pages do not become COMPLETE. Retry/page/shard/byte
budgets remain explicit. Versions and revision lineage are immutable. Only complete closed
aggregation buckets are emitted; absent bars are never silently filled or treated as zero.
Mixed rules inside a bucket are refused; reconstruction is explicitly labeled and does not
become an observed market fact. as-of publication visibility without evidence is unavailable.

## Filesystem contract

The caller workspace is its current directory or explicit CANDLE_AUDIT_WORKSPACE, independent
of module ancestry or any other project configuration. Inputs/outputs are confined to data/
and .tmp-data-layer/. Directory FD/inode binding, no-follow opens, bounded regular files,
atomic writes and cleanup remain enforced.

The reader's read-only contract means dataset/version bytes do not change. Semantic replay
requires bounded writable **verification scratch** at `.tmp-data-layer/semantic-<random>/`
within this workspace. Temporary CSV/raw files are removed on success and failure. A
read-only mount is not supported for this workspace; reading must not skip the original
validation kernels to avoid the temporary writes.

## External trust and network boundaries

VERIFY does not certify market truth. Research admission requires externally configured
independent signer public keys, roles and evidence. Producer, auditor and owner cannot be the
same signer. Synthetic data cannot enter RESEARCH. No real identities or credentials are
provided, and a caller's PASS field or hash is not an approval. Data licenses require external
permission; the program cannot infer them from a successful comparison.

Public downloading remains separate and opt-in: an explicit request-bound expiring grant,
request/byte limits and --authorize-public-download are required. Only the HTTPS public OKX
history-candles GET endpoint is allowed; redirects, proxies, private endpoints and account
headers are refused. Offline tests mock the HTTP boundary. This candidate does not establish
real historical availability or redistribution permission.

JSON/HTML reports use the selected canonical rows, escape dynamic text, preserve empty stream
selection and distinguish data gaps from unknown market truth. HTML has no remote resource
requests. Deployment, performance at market scale, public adoption and independent market
certification are not claimed. See limitations.md and the release gate.

Starting with 0.1.0rc2, `demo` and `report` accept `--language en` for an English HTML view or
`--language zh-CN` for the original default Chinese view. Only project-owned interface labels
are translated: exact candle values, user dataset names, statuses, JSON evidence and validation
rules are retained. The JSON schema name remains unchanged for compatibility. This is new
presentation support after the existing 0.1.0rc1 release; that release's archive is unchanged.

The public static preview links to saved synthetic HTML outputs only. It is separate from
these local commands and accepts no visitor files. Download the matching candidate archive
from the release linked in the README to reproduce a new local run.
