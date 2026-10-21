# Supported scope and limits

- Native identity: OKX SPOT or linear USDT SWAP, trade candles only. Base/quote identity is
  explicit; there is no fixed asset and no guessed unit conversion.
- Timeframes: 1m, 5m, 15m, 30m, 1h, 4h and UTC 1d. Integer nanoseconds and half-open opening
  windows preserve time precision. Unsupported calendars/venues/types fail explicitly.
- Prices and quantities remain decimal text. The optional SDK float view is non-authoritative.
- Individual native inputs are bounded at 8 MiB / 10000 rows, at least two bars. Queries return
  at most 10000 rows and reject overflow instead of silently truncating. Collection has explicit
  page, shard, byte and retry budgets. Aggregation requires complete closed buckets and does not
  interpolate, forward-fill or infer zero-volume bars from missing data.
- Dataset hashes, declarations and offline matching do not authenticate a real market source.
  Synthetic engineering data cannot become a research-approved dataset. Optional approval
  features require externally supplied independent principals and Ed25519 signatures; this
  package supplies no real signing identities or credentials.
- Reads preserve dataset/version contents but need bounded writable verification scratch.
  Atomic file publication does not claim full power-loss recovery or distributed scheduling.
- POSIX file descriptors, no-follow opens, locks and OS-specific held-inode paths are used.
  macOS/Linux are the intended platforms; Windows is unsupported. Each OS/runtime combination
  still needs its own acceptance run. No performance claim at market scale is made.
- Public history download requires an explicit request-bound grant with limits and expiry.
  Its only allowed endpoint is the HTTPS public OKX history-candles GET endpoint. Redirects,
  proxies, private endpoints and account headers are refused. Offline tests use fake responses;
  actual public downloads, historical availability and redistribution rights are not verified.
- Project MIT permission was confirmed by the maintainer. Dependencies keep their original
  terms and copyright notices. This does not establish permission to redistribute real market
  datasets. No exchange affiliation or grant selection is claimed.
