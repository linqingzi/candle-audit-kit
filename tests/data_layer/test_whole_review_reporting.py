"""B-04: selected streams with no returned bars remain visible as unavailable, never zero bars."""

import copy

import pytest
from test_full_collection import collector, history, native, request
from test_full_reference import rules

from candle_audit.data_layer import Window
from candle_audit.data_layer.aggregation import declare_reconstruction
from candle_audit.data_layer.releases import ReleaseStore
from candle_audit.data_layer.reporting import render_report, report_document

START = 1767225600000000000
HOUR = 3600 * 10**9


def separated_release(tmp_path, bundle, *, multi_tf=False):
    pipeline = collector(tmp_path)
    ids = []
    specs = [("BTC", "1h", "1H", 0, HOUR), ("ETH", "1h", "1H", 4 * HOUR, HOUR)]
    if multi_tf:
        specs.append(("BTC", "1m", "1m", 0, 60 * 10**9))
    for asset, tf, bar, offset, step in specs:
        changed = copy.deepcopy(bundle)
        changed["source"].update(base_asset=asset, instId=f"{asset}-USDT-SWAP", bar=bar)
        changed["metadata"]["timeframe"] = tf
        window = Window(START + offset, START + offset + 2 * step)
        reference = rules(changed, timeframe=tf, effective=window)
        req = request(changed, count=2, rules=(reference,), window=window)
        rows = native(window.start_ns // 10**6, 2, step // 10**6)
        ids.append(pipeline.collect(req, history(changed, req, [rows])).collection_id)
    store = ReleaseStore(pipeline)
    return store, store.create(ids, name="separate", producer_id="collector")


@pytest.mark.parametrize(
    "instruments,expected,gap_assets,returned",
    [
        (None, {"BTC-USDT-SWAP", "ETH-USDT-SWAP"}, {"ETH-USDT-SWAP"}, 2),
        (("BTC-USDT-SWAP",), {"BTC-USDT-SWAP"}, set(), 2),
        (("ETH-USDT-SWAP",), {"ETH-USDT-SWAP"}, {"ETH-USDT-SWAP"}, 0),
    ],
)
def test_default_all_and_explicit_filters_use_selected_universe(
    tmp_path, bundle, instruments, expected, gap_assets, returned
):
    store, release = separated_release(tmp_path, bundle)
    window = Window(START, START + 2 * HOUR)
    data = store.reader().query(release.release_id, window, instruments=instruments)
    doc = report_document(data, release)
    gaps = [g for g in doc["query_gaps"] if "instrument" in g]
    assert {g["instrument"] for g in gaps} == gap_assets
    assert {s["instrument"] for s in doc["selected_streams"]} == expected
    assert data.table.num_rows == returned
    assert doc["query"]["selection"]["instruments"] == (list(instruments) if instruments else None)
    for gap in gaps:
        assert gap["missing_count"] == 2 and gap["returned_count"] == 0
        assert gap["status"] == "UNAVAILABLE_IN_SELECTED_QUERY"
        assert gap["market_gap_truth"] == "UNKNOWN"
        assert gap["requested_window"] == {"start_ns": START, "end_ns": START + 2 * HOUR}
        assert gap["stored_coverage"] == [
            {"start_ns": START + 4 * HOUR, "end_ns": START + 6 * HOUR}
        ]
    html = render_report(data, release).decode()
    for instrument in expected:
        assert instrument in html
    assert 'data-detail="' not in html if returned == 0 else 'data-detail="' in html
    if data.table.num_rows:
        marked = declare_reconstruction(data, [START], evidence=b"engineering-only")
        assert marked.selection == data.selection


def test_whole_empty_and_multiple_timeframes_disclose_each_selected_stream(tmp_path, bundle):
    store, release = separated_release(tmp_path, bundle, multi_tf=True)
    window = Window(START + 8 * HOUR, START + 10 * HOUR)
    result = store.reader().query(release.release_id, window)
    doc = report_document(result, release)
    assert result.table.num_rows == 0
    assert len(doc["selected_streams"]) == 3
    gaps = [g for g in doc["query_gaps"] if "instrument" in g]
    assert sorted(g["missing_count"] for g in gaps) == [2, 2, 120]
    html = render_report(result, release).decode()
    assert html.count('data-panel="') == 3
    assert 'data-detail="' not in html
    filtered = store.reader().query(release.release_id, window, timeframes=("1m",))
    assert len(report_document(filtered, release)["selected_streams"]) == 1


def test_derived_empty_report_uses_target_stream_coverage(tmp_path, bundle):
    from test_full_aggregation import inputs

    store, release, req = inputs(tmp_path, bundle)
    derived = store.aggregate(
        release.release_id, "4h", req.window, name="derived", producer_id="collector"
    )
    window = Window(req.window.end_ns, req.window.end_ns + 8 * HOUR)
    result = store.reader().query(derived.release_id, window)
    doc = report_document(result, derived)
    assert result.table.num_rows == 0
    assert [(s["instrument"], s["timeframe"]) for s in doc["selected_streams"]] == [
        ("BTC-USDT-SWAP", "4h")
    ]
    gap = next(g for g in doc["query_gaps"] if "instrument" in g)
    assert gap["missing_count"] == 2
    assert gap["stored_coverage"] == [
        {"start_ns": req.window.start_ns, "end_ns": req.window.end_ns}
    ]
