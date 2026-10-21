# Candle Audit Kit

Exact-decimal candle validation, original-response reconciliation and reproducible local datasets.

[繁體中文起步說明](README.zh-TW.md)

**0.1.0rc1 is a local pre-release prepared under the MIT license.** The maintainer confirmed
that the inherited project code, tests and examples were developed under their direction with
Codex and authorized MIT publication. Third-party dependencies retain their original terms.
This is the first public pre-release. See [LICENSE](LICENSE),
[PROVENANCE.md](PROVENANCE.md) and [third-party notices](THIRD_PARTY_NOTICES.md).

## What it does

- Compare declared OKX trade-candle CSV data with original native JSON responses.
- Check exact identity, time windows, gaps, duplicates, closed bars and quantity units.
- Keep original decimal strings, immutable snapshots, Parquet data, revisions and source hashes.
- Query verified local data with bounded DuckDB queries; aggregate complete UTC buckets.
- Produce self-contained Chinese HTML/JSON quality reports without remote resources.

It does not place orders, use accounts, train models or certify that supplied data came from a
real exchange. Supported inputs are OKX SPOT and linear USDT SWAP trade candles. Other venues,
mark/index candles and inverse contracts are rejected. See [limitations](docs/limitations.md).

## Install

Requires CPython 3.11–3.14 and macOS or Linux. The exact validated environment is recorded in
`requirements-test-lock.txt`; no Windows support is claimed. The native wheels may impose a
newer minimum OS version. All six direct runtime dependencies are explicitly pinned.

From an extracted source archive:

```sh
python3.11 -m venv .venv
. .venv/bin/activate
python -m pip install --index-url https://pypi.org/simple -r requirements-test-lock.txt
python -m pip install --no-build-isolation --no-deps .
python -m pip check
candle-audit --help
```

The first install downloads packages from PyPI. All examples and tests below operate on
synthetic local inputs. The lock includes testing/build tools; a runtime-only installation is
`python -m pip install .`, which installs the six runtime dependencies and their transitives.
Do not install into a global Python environment.

## Run the complete offline example

```sh
mkdir demo-workspace
cd demo-workspace
candle-audit demo --root data/demo
```

Expected result: `SYNTHETIC_DEMO_COMPLETE`, `rows: 8`, `aggregate_rows: 2`,
`network_used: false`, `source_verification: UNVERIFIED`, `admission: NOT_EVALUATED`.
The result gives the exact HTML/JSON report paths. Open the HTML file in a browser.

The demo writes complete CSV/JSON/metadata/source/request/page inputs under `data/demo/inputs`,
ingests a candidate, creates a native release, queries it, derives two 4-hour bars and renders
a report. Prices and contract rules are invented engineering inputs. Changed demo input files
are never silently overwritten. Source examples are also provided in `examples/synthetic/`.

## Workspace and commands

The workspace is your current directory, independent of the installed package location.
Alternatively set `CANDLE_AUDIT_WORKSPACE` to an existing absolute directory without symlinks.
No project file, other repository, or external service is required.

Only `data/` and `.tmp-data-layer/` inside that workspace are allowed for user inputs/outputs.
Copy your authorized inputs into `data/` first. Traversal, escapes and unsafe file types remain
rejected. Reads leave dataset versions unchanged but use bounded temporary verification files
in `.tmp-data-layer/`; the workspace must be writable. Do not point it at a production system.

```sh
candle-audit providers
candle-audit ingest --help
candle-audit collect --help
candle-audit read --help
candle-audit report --help
python -m candle_audit.data_layer --help
```

Use the demo's `release_id` with `read` or `report`. See [the interface](docs/data_layer_full.md)
and [candidate ingestion](docs/data_layer_m1.md) for complete parameter examples.

## Run tests and build distributions

From the extracted source root, with the isolated environment activated:

```sh
python -B -m pytest -q
python -m build --no-isolation
```

Tests use synthetic fixtures and block socket connections. Property tests retain their
1000-example settings and can take substantial time. For a fast development check only use
`python -B -m pytest -q -m 'not property'`; it is not the full acceptance suite.
The test harness creates its own temporary paths inside `.tmp-data-layer/`, without widening
the production path allowlist. No credentials or real market downloads are required.
Three inherited historical-artifact compatibility cases are explicitly skipped because their
pre-standalone artifacts were not supplied. Three new relocation controls verify the current
standalone format; they do not pretend to establish compatibility with missing old artifacts.

## License and support status

Dependency notices and their original license texts are in `THIRD_PARTY_NOTICES.md` and
`licenses/third-party/`. The maintainer confirmation covers project material; dependency rights remain separate. No third-party binaries, account data or private configuration are bundled.
There is no claim of public adoption, external certification, production readiness or
eligibility/selection for any grant program. See `RELEASE_STATUS.md` and `docs/validation.md` for the release status and validation limits.
