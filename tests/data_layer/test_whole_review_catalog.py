"""B-01: both actual release schemas have usable, explicitly typed catalogs."""

import json

import pytest
from test_full_aggregation import inputs
from test_full_consumers import cli


@pytest.mark.parametrize("derived", [False, True])
def test_native_and_derived_catalogs_preserve_real_schema_and_lineage(tmp_path, bundle, derived):
    store, release, req = inputs(tmp_path, bundle)
    if derived:
        release = store.aggregate(
            release.release_id, "4h", req.window, name="catalog-derived", producer_id="collector"
        )
    catalog = store.catalog(release.release_id)
    assert catalog["schema"] == release.document["schema"]
    assert catalog["kind"] == ("DERIVED" if derived else "NATIVE")
    assert catalog["release_id"] == release.release_id
    assert catalog["quality"] == release.document["quality"]
    assert catalog["lineage"] == release.document["lineage"]
    if derived:
        assert catalog["derivation"] == release.document["derivation"]
        assert catalog["canonical_parquet_sha256"] == release.document["canonical_parquet_sha256"]
        assert not {"requested_collections", "streams", "partitions"} & catalog.keys()
        assert catalog["parent_id"] == release.document["derivation"]["source_release_id"]
    else:
        for name in ("requested_collections", "streams", "partitions"):
            assert catalog[name] == release.document[name]
    assert store.reader().query(release.release_id, req.window).table.num_rows == (
        2 if derived else 8
    )
    process, document = cli(
        "catalog", "--root", store.collector.root, "--release", release.release_id
    )
    assert process.returncode == 0, process.stdout + process.stderr
    assert document == json.loads(json.dumps(catalog))
