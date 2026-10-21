"""Independent fixed synthetic values; never market authentication."""
import copy
import hashlib
import json
from pathlib import Path

import pytest

from candle_audit.candle_reconcile import reconcile

FIXTURES = Path(__file__).resolve().parents[2] / 'tests/fixtures/legacy'
START, END = '2026-01-01T00:00:00Z', '2026-01-01T02:00:00Z'
SOURCE = dict(venue='OKX', instId='BTC-USDT-SWAP', instType='SWAP', base_asset='BTC',
              quote_asset='USDT', bar='1H', endpoint='/api/v5/market/history-candles',
              source_type='trade', acquisition='SYNTHETIC')


def fixture(tmp_path):
    csv = tmp_path/'bars.csv'
    raw = tmp_path/'raw.json'
    csv.write_bytes((FIXTURES/'bars.csv').read_bytes())
    raw.write_bytes((FIXTURES/'raw.json').read_bytes())
    metadata = dict(schema='candle-audit-data-version-v2', name='reconcile-test', version='v1',
                    source='synthetic-engineering', provenance='independent fixed synthetic values',
                    market='BTC-USDT-SWAP', currency='USDT', timeframe='1h', timezone='UTC',
                    kind='synthetic', content_sha256=hashlib.sha256(csv.read_bytes()).hexdigest(),
                    volume=dict(base_asset='BTC', quote_asset='USDT',
                                columns=[dict(column='qty', unit='base')]))
    return csv, raw, metadata, copy.deepcopy(SOURCE)


def run(f):
    csv, raw, meta, source = f
    return reconcile(csv, meta, raw, source, START, END)


def test_normal_readonly(tmp_path):
    f = fixture(tmp_path)
    before = [p.read_bytes() for p in f[:2]]
    r = run(f)
    assert r['comparison'] == 'MATCH'
    assert r['coverage']['status'] == 'COMPLETE'
    assert r['closedbar']['status'] == 'CLOSED'
    assert r['provenance'] == 'SYNTHETIC'
    assert r['source_verification'] == 'UNVERIFIED'
    assert r['admission'] == 'NOT_EVALUATED'
    assert before == [p.read_bytes() for p in f[:2]]


@pytest.mark.parametrize('case,expected', [
    ('price','MISMATCH'), ('rounding','MISMATCH'), ('volume','MISMATCH'),
    ('missing','MISMATCH'), ('extra','MISMATCH'), ('duplicate','MISMATCH'),
    ('conflict','MISMATCH'), ('seconds','NOT_COMPARABLE'), ('instrument','NOT_COMPARABLE'),
    ('bar','NOT_COMPARABLE'), ('market','NOT_COMPARABLE'), ('unit','NOT_COMPARABLE'),
    ('unclosed','MATCH'), ('confirm','MATCH'), ('api','NOT_COMPARABLE'),
    ('truncated','NOT_COMPARABLE'), ('nan','NOT_COMPARABLE'), ('source','NOT_COMPARABLE'),
    ('venue','NOT_COMPARABLE'), ('inverse','NOT_COMPARABLE'), ('nested','NOT_COMPARABLE'),
    ('longnumber','NOT_COMPARABLE'), ('badconfirm','NOT_COMPARABLE')])
def test_fixed_mutations(tmp_path, case, expected):
    f = fixture(tmp_path)
    csv, raw, m, s = f
    d = json.loads(raw.read_text())
    if case == 'price': d['data'][0][1] = '100.1'
    elif case == 'rounding': d['data'][0][1] = '100.000000000000000001'
    elif case == 'volume': d['data'][0][6] = '2.01'
    elif case == 'missing': d['data'].pop()
    elif case == 'extra':
        row = d['data'][1].copy(); row[0] = '1767232800000'; d['data'].append(row)
    elif case in ('duplicate','conflict'):
        row = d['data'][0].copy()
        if case == 'conflict': row[1] = '100.1'
        d['data'].append(row)
    elif case == 'seconds': d['data'][0][0] = '1767225600'
    elif case == 'instrument': s['instId'] = 'ETH-USDT-SWAP'
    elif case == 'bar': s['bar'] = '4H'
    elif case == 'market': m['market'] = 'ETH-USDT-SWAP'
    elif case == 'unit': m['volume']['columns'][0]['unit'] = 'unknown'
    elif case == 'unclosed': d['data'][0][8] = '0'
    elif case == 'confirm': d['data'][0].pop()
    elif case == 'api': d['code'] = '403'
    elif case == 'nan': d['data'][0][1] = 'NaN'
    elif case == 'source': s['source_type'] = 'mark'
    elif case == 'venue': s['venue'] = 'OTHER'
    elif case == 'inverse': s['quote_asset'] = 'USD'
    elif case == 'nested': d['data'] = [[[[[[[[[]]]]]]]]]
    elif case == 'longnumber': d['data'][0][1] = '1'*97
    elif case == 'badconfirm': d['data'][0][8] = '2'
    raw.write_text('{' if case == 'truncated' else json.dumps(d))
    r = run(f)
    assert r['comparison'] == expected, r
    if case == 'missing': assert r['coverage']['status'] == 'INCOMPLETE'
    if case == 'conflict': assert r['coverage']['raw_conflicts'] == 1
    if case == 'unclosed': assert r['closedbar']['status'] == 'UNCLOSED'
    if case == 'confirm': assert r['closedbar']['missing_confirm'] == 1


@pytest.mark.parametrize('unit,values', [('quote', ('2010','3045')), ('contracts', ('20','30'))])
def test_direct_units(tmp_path, unit, values):
    csv, raw, m, s = f = fixture(tmp_path)
    text = csv.read_text().replace(',101,2\n', ',101,'+values[0]+'\n').replace(',102,3\n', ',102,'+values[1]+'\n')
    csv.write_text(text)
    m['content_sha256'] = hashlib.sha256(csv.read_bytes()).hexdigest()
    m['volume']['columns'] = [dict(column='qty', unit=unit)]
    if unit == 'contracts': m['volume']['columns'][0].update(contract_size=0.1, contract_size_unit='base')
    assert run(f)['comparison'] == 'MATCH'


def test_spot_and_v1(tmp_path):
    csv, raw, m, s = f = fixture(tmp_path)
    s.update(instId='BTC-USDT', instType='SPOT'); m['market'] = 'BTC-USDT'
    d = json.loads(raw.read_text())
    d['data'][0][5:8] = ['2','2010','2010']
    d['data'][1][5:8] = ['3','3045','3045']
    raw.write_text(json.dumps(d))
    assert run(f)['comparison'] == 'MATCH'
    csv.write_text('\n'.join(','.join(row.split(',')[:5]) for row in csv.read_text().splitlines())+'\n')
    m.update(schema='candle-audit-data-version-v1', content_sha256=hashlib.sha256(csv.read_bytes()).hexdigest())
    del m['volume']
    assert run(f)['comparison'] == 'MATCH'


def test_bounds_and_partial(tmp_path, monkeypatch):
    f = fixture(tmp_path)
    r = reconcile(f[0], f[2], f[1], f[3], START, '2026-01-01T03:00:00Z')
    assert r['coverage']['status'] == 'INCOMPLETE'
    assert r['comparison'] == 'NOT_COMPARABLE'
    monkeypatch.setattr('candle_audit.candle_reconcile.MAX_ROWS', 1)
    assert run(f)['comparison'] == 'NOT_COMPARABLE'


@pytest.mark.parametrize('value', [None, [], 'bad', 1])
def test_declaration_types(tmp_path, value):
    csv, raw, m, s = fixture(tmp_path)
    for meta, source in ((value, s), (m, value)):
        r = reconcile(csv, meta, raw, source, START, END)
        assert r['comparison'] == 'NOT_COMPARABLE'
        assert r['admission'] == 'NOT_EVALUATED'


def test_empty_and_partial_raw_diagnostics(tmp_path):
    f = fixture(tmp_path)
    d = json.loads(f[1].read_text())
    d['data'].append(['bad'])
    f[1].write_text(json.dumps(d))
    r = run(f)
    assert r['raw_diagnostics']['rows_processed'] == 2
    assert r['comparison'] == 'NOT_COMPARABLE'
    d['data'] = []
    f[1].write_text(json.dumps(d))
    r = run(f)
    assert r['closedbar']['status'] == 'UNVERIFIED'
    assert r['comparison'] == 'MISMATCH'


def test_candidate_duplicate_and_alignment(tmp_path):
    f = fixture(tmp_path)
    text = f[0].read_text()
    f[0].write_text(text + text.splitlines()[1]+'\n')
    f[2]['content_sha256'] = hashlib.sha256(f[0].read_bytes()).hexdigest()
    r = run(f)
    assert r['coverage']['candidate_duplicates'] == 1
    assert r['coverage']['candidate_conflicts'] == 0
    assert len(r['coverage']['candidate_duplicate_samples_ms']) == 1
    f[0].write_text(text + text.splitlines()[1].replace('00:00:00','00:01:00')+'\n')
    f[2]['content_sha256'] = hashlib.sha256(f[0].read_bytes()).hexdigest()
    r = run(f)
    assert r['coverage']['candidate_missing'] == 0
    assert r['comparison'] == 'NOT_COMPARABLE'


def test_cli_schema_and_hashes(tmp_path):
    import os
    import subprocess
    import sys
    csv, raw, m, s = fixture(tmp_path)
    mp, sp = tmp_path/'meta.json', tmp_path/'source.json'
    mp.write_text(json.dumps(m)); sp.write_text(json.dumps(s))
    args = [sys.executable, '-m', 'candle_audit.candle_reconcile', '--csv', str(csv),
            '--raw', str(raw), '--metadata', str(mp), '--source', str(sp),
            '--start', START, '--end', END]
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE='1')
    good = subprocess.run(args, capture_output=True, text=True, env=env)
    assert good.returncode == 0, good.stderr
    report = json.loads(good.stdout)
    assert report['requested_range']['start_utc'] == START
    assert report['actual_range']['raw']['valid_unique_count'] == 2
    assert report['declarations']['source'] == s
    assert report['hashes']['candidate'] == hashlib.sha256(csv.read_bytes()).hexdigest()
    assert report['hashes']['raw'] == hashlib.sha256(raw.read_bytes()).hexdigest()
    assert report['hashes']['metadata'] == hashlib.sha256(mp.read_bytes()).hexdigest()
    assert report['hashes']['source'] == hashlib.sha256(sp.read_bytes()).hexdigest()
    mp.write_text('null')
    bad = subprocess.run(args, capture_output=True, text=True, env=env)
    assert bad.returncode == 1
    assert set(json.loads(bad.stdout)) >= {'schema','comparison','coverage','closedbar','provenance','hashes','admission'}
    mp.write_text('{')
    bad = subprocess.run(args, capture_output=True, text=True, env=env)
    assert bad.returncode == 1
    failed = json.loads(bad.stdout)
    assert failed['closedbar']['status'] == 'UNVERIFIED'
    assert failed['declarations']['status'] == 'UNKNOWN'
    assert failed['requested_range']['status'] == 'UNKNOWN'
    assert failed['actual_range']['candidate']['valid_unique_count'] is None
    assert failed['hashes']['metadata'] == hashlib.sha256(mp.read_bytes()).hexdigest()
    assert failed['hashes']['source'] == hashlib.sha256(sp.read_bytes()).hexdigest()


def test_json_limits(tmp_path):
    f = fixture(tmp_path)
    for content in ('{"code":"0","code":"0","msg":"","data":[]}',
                    '{"code":"0","msg":"","data":[NaN]}',
                    '{"code":"0","msg":"","data":[1]}',
                    '{"code":"0","msg":"'+('x'*4097)+'","data":[]}'):
        f[1].write_text(content)
        assert run(f)['comparison'] == 'NOT_COMPARABLE'
    f[1].write_bytes(b' ' * (8*1024*1024+1))
    assert run(f)['errors'] == ['input byte limit exceeded']


@pytest.mark.parametrize('volume', [None, [], 'bad', {'columns':None}])
def test_invalid_volume_shape(tmp_path, volume):
    f = fixture(tmp_path)
    f[2]['volume'] = volume
    assert run(f)['comparison'] == 'NOT_COMPARABLE'


@pytest.mark.parametrize('schema', ['candle-audit-data-version-v1', 'candle-audit-data-version-v2'])
@pytest.mark.parametrize('market_kind', ['SPOT', 'SWAP'])
def test_product_declarations(tmp_path, schema, market_kind):
    csv, raw, m, s = f = fixture(tmp_path)
    if market_kind == 'SPOT':
        s.update(instId='BTC-USDT', instType='SPOT')
        m['market'] = s['instId']
        d = json.loads(raw.read_text())
        d['data'][0][5:8] = ['2', '2010', '2010']
        d['data'][1][5:8] = ['3', '3045', '3045']
        raw.write_text(json.dumps(d))
    if schema.endswith('v1'):
        csv.write_text('\n'.join(','.join(r.split(',')[:5]) for r in csv.read_text().splitlines())+'\n')
        m.update(schema=schema, content_sha256=hashlib.sha256(csv.read_bytes()).hexdigest())
        del m['volume']
    r = run(f)
    assert r['comparison'] == 'MATCH'
    assert r['declarations']['status'] == 'VALIDATED'
    assert r['declarations']['source'] == s
    md = r['declarations']['metadata']
    assert md == {**{k: m[k] for k in ('schema','source','provenance','market','kind','timeframe','timezone')},
                  'verification': 'UNVERIFIED', 'truncated_fields': []}
    candidate = r['declarations']['candidate_identity']
    assert candidate == dict(status='UNVERIFIED',
        basis='validated supplied context declaration; not authentication',
        venue='OKX', instId=s['instId'], market_kind=market_kind, bar='1H',
        time_basis='UTC candle opening ISO timestamp')
    identity = r['declarations']['raw_identity']
    assert identity == dict(status='UNVERIFIED',
        basis='supplied request context; raw arrays lack instrument/bar identity',
        venue='OKX', instId=s['instId'], market_kind=market_kind, bar='1H',
        time_basis='UTC candle opening milliseconds')
    assert r['requested_range'] == dict(status='VALIDATED', start_utc=START, end_utc=END,
        interval='[start,end)', time_basis='UTC candle opening')
    for name in ('candidate', 'raw'):
        actual = r['actual_range'][name]
        assert actual['status'] == 'OBSERVED'
        assert actual['min_utc'] == START
        assert actual['max_utc'] == '2026-01-01T01:00:00Z'
        assert actual['valid_unique_count'] == actual['in_requested_range_count'] == 2
        assert r['hashes'][name] == hashlib.sha256(f[0 if name == 'candidate' else 1].read_bytes()).hexdigest()


@pytest.mark.parametrize('case', ['source','venue','instrument','range','metadata','file'])
def test_product_early_failure_schema(tmp_path, case):
    csv, raw, m, s = fixture(tmp_path)
    end = END
    if case == 'source': s['source_type'] = 'mark'
    if case == 'venue': s['venue'] = 'OTHER'
    if case == 'instrument': s['instId'] = 'ETH-USDT-SWAP'
    if case == 'range': end = START
    if case == 'metadata': m['schema'] = 'unsupported'
    if case == 'file': csv.unlink()
    r = reconcile(csv, m, raw, s, START, end)
    assert r['comparison'] == 'NOT_COMPARABLE'
    assert r['declarations']['status'] == 'UNKNOWN'
    assert r['declarations']['source'] is None
    assert r['declarations']['raw_identity']['status'] == 'UNVERIFIED'
    assert r['declarations']['raw_identity']['instId'] is None
    assert set(r['hashes']) == {'candidate','raw','metadata','source'}
    assert set(r['actual_range']) == {'candidate','raw'}
    if case in ('range','metadata','file','source'):
        assert r['requested_range']['status'] == 'UNKNOWN'
        assert r['requested_range']['start_utc'] is None
    for actual in r['actual_range'].values():
        assert set(actual) >= {'status','min_utc','max_utc','valid_unique_count','in_requested_range_count'}


def test_product_partial_coverage(tmp_path):
    csv, raw, m, s = fixture(tmp_path)
    r = reconcile(csv, m, raw, s, START, '2026-01-01T03:00:00Z')
    assert r['requested_range']['end_utc'] == '2026-01-01T03:00:00Z'
    assert r['coverage']['status'] == 'INCOMPLETE'
    assert r['coverage']['candidate_missing'] == r['coverage']['raw_missing'] == 1
    for actual in r['actual_range'].values():
        assert actual['valid_unique_count'] == actual['in_requested_range_count'] == 2
        assert actual['min_utc'] == START
        assert actual['max_utc'] == '2026-01-01T01:00:00Z'
    assert r['comparison'] == 'NOT_COMPARABLE'


def test_product_bounded_metadata_summary(tmp_path):
    f = fixture(tmp_path)
    # Exercise free-text historical declarations on a synthetic acquisition fixture.
    f[2]['kind'] = 'historical'
    f[2]['source'] = 's' * 10000
    f[2]['provenance'] = 'p' * 10000
    r = run(f)
    assert r['comparison'] == 'MATCH'
    md = r['declarations']['metadata']
    assert md['source'] == 's' * 240
    assert md['provenance'] == 'p' * 240
    assert md['truncated_fields'] == ['source', 'provenance']
    assert r['provenance'] == 'SYNTHETIC'
    assert len(json.dumps(r)) < 6000
