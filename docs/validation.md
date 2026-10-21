# Validation and release scope

Version: 0.1.0rc2 candidate. The initial 0.1.0rc1 evidence below remains a historical baseline;
the candidate's release notes and linked CI identify its final run and artifact checks.
No prior public adoption, download totals,
external customers or long-running public maintenance history are claimed.

## Initial 0.1.0rc1 local evidence

- Complete synthetic functional suite: 662 passed, 0 failed, 3 skipped; 857.29 seconds.
- All 21 property tests retained max_examples=1000; no claim of 21000 distinct inputs is made.
- Three pre-standalone historical-artifact cases were skipped because the original artifacts
  were not supplied. They do not establish backward compatibility. Three independent
  current-format relocation controls actually copy datasets and check bytes/query semantics.
- Runtime source consists of 21 Python files. Their bytes were unchanged when MIT licensing
  and packaging metadata were finalized.
- The final MIT archive was separately extracted, installed into a new isolated environment
  and passed 15 targeted workspace, demo, relocation, source/wheel and license-retention tests.
- pip check, command/module entry points, command help, documented ingestion/query/empty-query/
  collection, and a real offline demo all succeeded. Demo: 8 native candles, 2 aggregated 4h
  candles, HTML/JSON reports. No credentials, actual market download or trading account.
- Project metadata is MIT; 24 original dependency license texts remain unchanged in source and
  wheel distributions. Their terms are not replaced by the project license.

The validated local combination was macOS arm64 / CPython 3.11.15 with the pinned environment.
Public CI runs the same synthetic test suite on GitHub's standard macOS runner. Its actual
current result is recorded in GitHub Actions; configuration alone is not a passing result.

The public CI workflow has contents:read, does not persist checkout credentials, uses no
repository secrets, and uses only GitHub-hosted standard runners. No real market data is
requested. Test/build package downloads come from official PyPI.

Other OS/Python combinations, public-download availability, real data redistribution rights
and market-scale performance are not independently validated. The retained pandas daily-bar
deprecation warnings are visible in test output. A successful hash/compare does not certify
market provenance. See limitations.md.

Original application history, private backups and local acceptance logs are not published.
This source tree starts with a new independent Git history; project/source permission was
confirmed by the maintainer. Program eligibility or public publication does not guarantee
selection for any grant.
