# Third-party notices

Runtime packages are installed separately from official PyPI; no dependency binaries are
bundled in this source archive. The project material uses MIT after maintainer confirmation; dependencies keep their own terms.

| Direct dependency | Version | License information from the installed wheel |
| --- | --- | --- |
| CCXT | 4.5.64 | MIT |
| DuckDB | 1.5.6 | MIT |
| PyArrow | 25.0.0 | Apache-2.0; retain NOTICE and bundled component terms |
| pandas | 3.0.3 | BSD; see the original LICENSE for full terms and components |
| NumPy | 2.4.6 | BSD-3-Clause and bundled 0BSD/MIT/Zlib/CC0-1.0 component terms |
| cryptography | 50.0.2 | Apache-2.0 OR BSD-3-Clause; see all bundled license files |

Original license and attribution texts copied from those exact installed wheel distributions
are in `licenses/third-party/`. `licenses/dependency-license-metadata.json` records their
PyPI version URLs and retained texts. Preserve these texts when repackaging this candidate.
Do not replace their copyright holders with the new project's name.

Transitive dependencies and test/build tools are locked in `requirements-test-lock.txt` and
remain separate installations under their own terms. Their distribution license metadata is
recorded in `licenses/environment-license-metadata.json`. The project MIT text in `LICENSE` follows https://opensource.org/license/mit.
It does not replace or relicense any dependency license or copyright notice.
