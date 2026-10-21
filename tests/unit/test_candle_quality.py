"""Fixed offline synthetic diagnostics; no registry writes or engine execution."""
import copy
import hashlib
import json
import subprocess
import sys

import pytest

from candle_audit.candle_quality import validate

CONTEXT = dict(venue='OKX', instId='BTC-USDT-SWAP', instType='SWAP', base_asset='BTC', quote_asset='USDT')
ROWS = [['2026-01-01T00:00:00Z', '100', '102', '99', '101', '2'],
        ['2026-01-01T01:00:00Z', '101', '103', '100', '102', '3'],
        ['2026-01-01T02:00:00Z', '102', '104', '101', '103', '4']]


def fixture(tmp_path, rows=None, header=None, **updates):
    p = tmp_path/'bars.csv'
    p.write_text('\n'.join(','.join(r) for r in [header or ['timestamp', 'open', 'high', 'low', 'close', 'qty'], *(ROWS if rows is None else rows)])+'\n')
    doc = dict(schema='candle-audit-data-version-v2', name='quality-test', version='v1',
               source='synthetic-engineering', provenance='fixed synthetic test only',
               market='BTC-USDT-SWAP', currency='USDT', timeframe='1h', timezone='UTC',
               kind='synthetic', content_sha256=hashlib.sha256(p.read_bytes()).hexdigest(),
               volume=dict(base_asset='BTC', quote_asset='USDT', columns=[dict(column='qty', unit='base')]))
    doc.update(updates)
    return p, doc


def test_normal_and_v1(tmp_path):
    p, d = fixture(tmp_path)
    before = p.read_bytes()
    r = validate(p, d, CONTEXT)
    assert r['structural_status'] == 'PASS'
    assert r['source_consistency'] == 'UNVERIFIED'
    assert r['admission'] == 'NOT_EVALUATED'
    assert p.read_bytes() == before
    p, d = fixture(tmp_path, [r[:5] for r in ROWS], ['timestamp','open','high','low','close'], schema='candle-audit-data-version-v1')
    del d['volume']
    assert validate(p, d, CONTEXT)['structural_status'] == 'PASS'


@pytest.mark.parametrize('case,check', [
    ('missing_column','missing_columns'), ('columns_order','column_layout'),
    ('cell','missing_cells'), ('width','row_width'), ('empty','minimum_bars'),
    ('gap','missing_bars'), ('duplicate','duplicates'), ('reverse','order'),
    ('naive','timezone_missing'), ('offset','timezone_mismatch'), ('nat','invalid_timestamp'),
    ('alignment','period_alignment'), ('spacing','timeframe_spacing'),
    ('nan','nonfinite'), ('inf','nonfinite'), ('text','nonnumeric'),
    ('zero_price','ohlc_positivity'), ('high','ohlc_logic'), ('low','ohlc_logic'),
    ('negative','negative_volume')])
def test_checks(tmp_path, case, check):
    rows = copy.deepcopy(ROWS)
    header = None
    if case == 'missing_column':
        rows = [r[:4]+r[5:] for r in rows]; header = ['timestamp','open','high','low','qty']
    elif case == 'columns_order': header = ['timestamp','high','open','low','close','qty']
    elif case == 'cell': rows[1][2] = ''
    elif case == 'width': rows[1].pop()
    elif case == 'empty': rows = []
    elif case == 'gap': rows.pop(1)
    elif case == 'duplicate': rows[1] = rows[0].copy()
    elif case == 'reverse': rows.reverse()
    elif case == 'naive': rows[1][0] = '2026-01-01T01:00:00'
    elif case == 'offset': rows[1][0] = '2026-01-01T01:00:00+01:00'
    elif case == 'nat': rows[1][0] = 'NaT'
    elif case in ('alignment','spacing'): rows[1][0] = '2026-01-01T01:01:00Z'
    elif case in ('nan','inf','text'): rows[1][1] = {'nan':'nan','inf':'inf','text':'bad'}[case]
    elif case == 'zero_price': rows[1][1] = '0'
    elif case == 'high': rows[1][2] = '1'
    elif case == 'low': rows[1][3] = '200'
    elif case == 'negative': rows[1][5] = '-1'
    p, d = fixture(tmp_path, rows, header)
    r = validate(p, d, CONTEXT)
    assert check in r['errors']
    assert r['source_consistency'] == 'UNVERIFIED'


def test_warnings_only(tmp_path):
    rows = copy.deepcopy(ROWS)
    rows[1][1:5] = ['200','203','199','202']
    rows[1][5] = '0'
    p, d = fixture(tmp_path, rows)
    r = validate(p, d, CONTEXT)
    assert r['structural_status'] == 'PASS'
    assert set(r['warnings']) == {'zero_volume','price_jump'}


@pytest.mark.parametrize('case', ['extra_metadata','matrix','hash','timezone','timeframe','unit','context','market','spot_contract','contracts'])
def test_identity_units(tmp_path, case):
    p, d = fixture(tmp_path)
    c = CONTEXT.copy()
    if case == 'extra_metadata': d['instId'] = c['instId']
    elif case == 'matrix': d['schema'] = 'candle-audit-xs-matrix-v1'
    elif case == 'hash': d['content_sha256'] = '0'*64
    elif case == 'timezone': d['timezone'] = 'No/Such_Zone'
    elif case == 'timeframe': d['timeframe'] = '2h'
    elif case == 'unit': d['volume']['columns'][0]['unit'] = 'unknown'
    elif case == 'context': c['instId'] = 'BTC-USDT'
    elif case == 'market': d['market'] = 'BTC-USD-SWAP'
    elif case == 'spot_contract':
        c.update(instId='BTC-USDT', instType='SPOT'); d['market'] = 'BTC-USDT'
        d['volume']['columns'][0].update(unit='contracts', contract_size=1, contract_size_unit='base')
    elif case == 'contracts':
        d['volume']['columns'] = [dict(column='qty',unit='contracts',contract_size=1e308,contract_size_unit='base')]
    assert validate(p, d, c)['structural_status'] == 'FAIL'


def test_cli(tmp_path):
    p, d = fixture(tmp_path)
    meta, ctx = tmp_path/'meta.json', tmp_path/'ctx.json'
    meta.write_text(json.dumps(d)); ctx.write_text(json.dumps(CONTEXT))
    cmd = [sys.executable, '-B', '-m', 'candle_audit.candle_quality', '--csv', str(p), '--metadata', str(meta), '--okx-context', str(ctx)]
    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)['source_consistency'] == 'UNVERIFIED'
    p.write_text('broken\n')
    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 1
    assert json.loads(result.stdout)['structural_status'] == 'FAIL'


def test_fixed_contract_consistency(tmp_path):
    rows = [r + [str(float(r[-1])*0.5)] for r in copy.deepcopy(ROWS)]
    p, d = fixture(tmp_path, rows, ['timestamp','open','high','low','close','qty','base_qty'])
    d['volume']['columns'] = [dict(column='qty',unit='contracts',contract_size=.5,contract_size_unit='base'), dict(column='base_qty',unit='base')]
    assert validate(p, d, CONTEXT)['structural_status'] == 'PASS'
    rows[1][-1] = '999'
    p, fresh = fixture(tmp_path, rows, ['timestamp','open','high','low','close','qty','base_qty'])
    d['content_sha256'] = fresh['content_sha256']
    assert 'volume_units' in validate(p, d, CONTEXT)['errors']


def test_spot_quote_volume_and_bad_context(tmp_path):
    p, d = fixture(tmp_path)
    c = CONTEXT.copy(); c.update(instType='SPOT', instId='BTC-USDT')
    d['market'] = c['instId']; d['volume']['columns'][0]['unit'] = 'quote'
    assert validate(p, d, c)['structural_status'] == 'PASS'
    for change in ({'venue':'OTHER'}, {'instType':'FUTURES'}, {'quote_asset':'USD'}, {'extra':1}, {'base_asset':'btc'}):
        bad = dict(c, **change)
        assert 'okx_context' in validate(p, d, bad)['errors']
    d['volume']['base_asset'] = 'ETH'
    assert 'okx_context' in validate(p, d, c)['errors']


def test_missing_file_and_nonfinite_volume(tmp_path):
    p, d = fixture(tmp_path)
    assert 'csv_read' in validate(tmp_path/'absent.csv', d, CONTEXT)['errors']
    rows = copy.deepcopy(ROWS); rows[1][-1] = 'inf'
    p, d = fixture(tmp_path, rows)
    r = validate(p, d, CONTEXT)
    assert {'nonfinite','volume_units'} <= set(r['errors'])


def test_v2_without_volume(tmp_path):
    p, d = fixture(tmp_path, [r[:5] for r in ROWS], ['timestamp','open','high','low','close'])
    del d['volume']
    assert validate(p, d, CONTEXT)['structural_status'] == 'PASS'


@pytest.mark.parametrize('bad_timestamp', ['NaT', '2026-01-01T01:00:00', '2026-01-01T01:00:00+01:00'])
def test_invalid_timestamp_breaks_derived_neighbors(tmp_path, bad_timestamp):
    rows = copy.deepcopy(ROWS)
    rows[1][0] = bad_timestamp
    rows[1][1:5] = ['200','203','199','202']
    rows[2][0] = '2026-01-01T00:01:00Z'
    p, d = fixture(tmp_path, rows)
    checks = validate(p, d, CONTEXT)['checks']
    assert checks['timeframe_spacing']['count'] == 0
    assert checks['order']['count'] == 0
    assert checks['price_jump']['count'] == 0
    assert checks['missing_bars']['status'] == 'UNVERIFIED'
    assert checks['period_alignment']['row_positions'] == [3]


def test_samples_are_bounded_original_data_rows(tmp_path):
    rows = [ROWS[0].copy() for _ in range(15)]
    for row in rows:
        row[1] = '-1'
    p, d = fixture(tmp_path, rows)
    checks = validate(p, d, CONTEXT)['checks']
    assert checks['ohlc_positivity']['count'] == 15
    assert checks['ohlc_positivity']['row_positions'] == list(range(1,11))
    assert checks['duplicates']['count'] == 14
    assert checks['duplicates']['row_positions'] == list(range(2,12))


@pytest.mark.parametrize('case', ['metadata', 'negative_volume'])
def test_registry_errors_remain_cli_json(tmp_path, case):
    rows = copy.deepcopy(ROWS)
    if case == 'negative_volume': rows[1][-1] = '-1'
    p, d = fixture(tmp_path, rows)
    if case == 'metadata': d['unknown'] = 'forbidden'
    meta, ctx = tmp_path/'meta.json', tmp_path/'ctx.json'
    meta.write_text(json.dumps(d)); ctx.write_text(json.dumps(CONTEXT))
    result = subprocess.run([sys.executable, '-B', '-m', 'candle_audit.candle_quality',
                             '--csv', str(p), '--metadata', str(meta), '--okx-context', str(ctx)],
                            capture_output=True, text=True)
    assert result.returncode == 1
    report = json.loads(result.stdout)
    assert case in report['errors']
    assert 'Traceback' not in result.stderr
    assert report['source_consistency'] == 'UNVERIFIED'


@pytest.mark.parametrize('contracts', [False, True])
def test_report_attaches_validated_declarations(tmp_path, contracts):
    p, d = fixture(tmp_path)
    if contracts:
        d['volume']['columns'][0].update(unit='contracts', contract_size=.5,
                                        contract_size_unit='base')
    r = validate(p, d, CONTEXT)
    assert r['declarations'] == {'source_verification': 'UNVERIFIED',
                                  'metadata': d, 'okx_context': CONTEXT}
    assert r['declarations']['metadata']['market'] == r['declarations']['okx_context']['instId']
    assert r['source_consistency'] == 'UNVERIFIED'
    assert r['admission'] == 'NOT_EVALUATED'
    # Only validated context is attached, even when the metadata itself is valid.
    invalid = dict(CONTEXT, instId='BTC-USDT')
    failed = validate(p, d, invalid)
    assert failed['declarations']['okx_context'] is None
    assert failed['declarations']['metadata'] == d
    d['extra'] = 'invalid'
    assert 'declarations' not in validate(p, d, CONTEXT)


@pytest.mark.parametrize('field,value,diagnostic', [(2,'NaN','nonfinite'), (2,'inf','nonfinite'),
                                                   (5,'-1','negative_volume'), (3,'0','ohlc_positivity')])
def test_mixed_bad_fields_are_independent(tmp_path, field, value, diagnostic):
    rows = copy.deepcopy(ROWS)
    rows[1][1] = 'bad'
    rows[1][field] = value
    p, d = fixture(tmp_path, rows)
    checks = validate(p, d, CONTEXT)['checks']
    assert checks[diagnostic]['count'] == 1
    assert checks[diagnostic]['row_positions'] == [2]
    assert checks['ohlc_logic']['status'] == 'UNVERIFIED'
    assert checks['ohlc_logic']['undecidable_count'] == 1
    assert checks['ohlc_logic']['undecidable_row_positions'] == [2]
    assert checks['volume_units']['status'] == ('FAIL' if field == 5 else 'PASS')


def test_partial_ohlc_failure_and_close_review(tmp_path):
    rows = copy.deepcopy(ROWS)
    rows[1][1:5] = ['bad','90','100','200']
    p, d = fixture(tmp_path, rows)
    checks = validate(p, d, CONTEXT)['checks']
    assert checks['ohlc_logic']['status'] == 'FAIL'
    assert checks['ohlc_logic']['count'] == 1
    assert checks['ohlc_logic']['row_positions'] == [2]
    assert checks['ohlc_logic']['undecidable_count'] == 1
    assert checks['price_jump']['count'] == 2


def test_bad_price_does_not_skip_fixed_units(tmp_path):
    rows = [r + [str(float(r[-1])*.5)] for r in copy.deepcopy(ROWS)]
    rows[1][1] = 'bad'
    rows[1][-1] = '999'
    p, d = fixture(tmp_path, rows, ['timestamp','open','high','low','close','qty','base_qty'])
    d['volume']['columns'] = [dict(column='qty',unit='contracts',contract_size=.5,contract_size_unit='base'),
                              dict(column='base_qty',unit='base')]
    check = validate(p, d, CONTEXT)['checks']['volume_units']
    assert check['status'] == 'FAIL'
    assert check['count'] == 1
    assert check['row_positions'] == [2]


def test_bad_volume_does_not_hide_other_conversion_overflow(tmp_path):
    rows = [r + ['bad'] for r in copy.deepcopy(ROWS)]
    p, d = fixture(tmp_path, rows, ['timestamp','open','high','low','close','qty','quote_qty'])
    d['volume']['columns'] = [dict(column='qty',unit='contracts',contract_size=1e308,contract_size_unit='base'),
                              dict(column='quote_qty',unit='quote')]
    check = validate(p, d, CONTEXT)['checks']['volume_units']
    assert check['status'] == 'FAIL'
    assert 'overflow' in str(check['detail'])
    assert check['row_positions'] == [1,2,3]


@pytest.mark.parametrize('value', ['NaN','inf','-inf'])
def test_bad_price_with_nonfinite_volume(tmp_path, value):
    rows = copy.deepcopy(ROWS)
    rows[1][1], rows[1][5] = 'bad', value
    p, d = fixture(tmp_path, rows)
    report = validate(p, d, CONTEXT)
    assert report['checks']['nonfinite']['count'] == 1
    assert report['checks']['nonfinite']['row_positions'] == [2]
    assert report['checks']['volume_units']['status'] == 'FAIL'
    assert report['checks']['volume_units']['row_positions'] == [2]
    assert report['checks']['ohlc_logic']['status'] == 'UNVERIFIED'


def test_independent_counts_and_undecidable_samples_are_bounded(tmp_path):
    rows = [r.copy() for r in [ROWS[0]]*17]
    for row in rows:
        row[1:6] = ['bad','NaN','bad','-1','-1']
    p, d = fixture(tmp_path, rows)
    checks = validate(p, d, CONTEXT)['checks']
    assert checks['nonnumeric']['count'] == 34
    assert checks['nonfinite']['count'] == 17
    assert checks['negative_volume']['count'] == 17
    assert checks['ohlc_positivity']['count'] == 17
    assert checks['ohlc_logic']['status'] == 'UNVERIFIED'
    assert checks['ohlc_logic']['undecidable_count'] == 17
    assert checks['ohlc_logic']['undecidable_row_positions'] == list(range(1,11))
    assert checks['volume_units']['count'] == 17
    assert checks['volume_units']['row_positions'] == list(range(1,11))


START = '2026-01-01T00:00:00Z'
END = '2026-01-01T05:00:00+00:00'


@pytest.mark.parametrize('start,end', [
    (START, None), (None, END), (END, START), (START, START),
    ('2026-01-01', END), ('2026-01-01T00:00:00', END),
    ('2026-01-01T00:00:00+01:00', END), ('NaT', END),
    ('2026-01-01T00:01:00Z', END), (START, '2026-01-01T05:00:00.000000001Z'),
    ('2026-02-30T00:00:00Z', END), (123, END),
])
def test_range_arguments(tmp_path, start, end):
    p, d = fixture(tmp_path)
    r = validate(p, d, CONTEXT, start, end)
    assert r['structural_status'] == 'FAIL'
    assert r['checks']['requested_range_arguments']['detail']
    assert r['requested_range']['status'] == 'UNVERIFIED'


@pytest.mark.parametrize('period,seconds', [('1m',60),('5m',300),('15m',900),
                                          ('30m',1800),('1h',3600),('4h',14400),('1d',86400)])
def test_range_fixed_periods(tmp_path, period, seconds):
    from datetime import datetime, timedelta, timezone
    origin = datetime(2026,1,1,tzinfo=timezone.utc)
    rows = copy.deepcopy(ROWS[:2])
    for i, row in enumerate(rows): row[0] = (origin+timedelta(seconds=i*seconds)).isoformat()
    p, d = fixture(tmp_path, rows, timeframe=period)
    end = (origin+timedelta(seconds=2*seconds)).isoformat()
    r = validate(p, d, CONTEXT, START, end)
    assert r['structural_status'] == 'PASS'
    assert r['requested_range']['expected_bars'] == 2
    assert r['requested_range']['valid_unique_bars'] == 2
    assert r['requested_range']['missing_bars'] == 0


@pytest.mark.parametrize('indices,counts,status', [
    ([], (0,5,0,0,0,5), 'ALL_MISSING'),
    ([2], (1,4,2,0,2,0), 'INCOMPLETE'),
    ([1,3], (2,3,1,1,1,0), 'INCOMPLETE'),
    ([0,1,2,3,4], (5,0,0,0,0,0), 'COMPLETE'),
    ([5,6], (0,5,0,0,0,5), 'ALL_MISSING'),
])
def test_range_exact_partition(tmp_path, indices, counts, status):
    rows = []
    for i in indices:
        row = ROWS[0].copy(); row[0] = f'2026-01-01T{i:02}:00:00Z'; rows.append(row)
    p, d = fixture(tmp_path, rows)
    r = validate(p, d, CONTEXT, START, END)
    q = r['requested_range']
    keys = ['valid_unique_bars','missing_bars','leading_missing_bars',
            'internal_missing_bars','trailing_missing_bars','full_range_missing_bars']
    assert tuple(q[k] for k in keys) == counts
    assert q['status'] == status
    expected = [f'2026-01-01T{i:02}:00:00+00:00' for i in range(5) if i not in indices]
    assert q['missing_sample_utc'] == expected
    assert q['out_of_range_rows'] == sum(i >= 5 for i in indices)
    assert sum(q[k] for k in keys[2:]) == q['missing_bars']
    assert r['source_consistency'] == 'UNVERIFIED' and r['admission'] == 'NOT_EVALUATED'


def test_range_invalid_duplicate_unordered_and_outside(tmp_path):
    stamps = ['2026-01-01T03:00:00Z', START, START, 'NaT',
              '2026-01-01T01:30:00Z', END, '2025-12-31T23:00:00Z',
              '2026-01-01T01:00:00', '2026-01-01T01:00:00+01:00']
    rows = []
    for stamp in stamps:
        row = ROWS[0].copy(); row[0] = stamp; rows.append(row)
    p, d = fixture(tmp_path, rows)
    r = validate(p, d, CONTEXT, START, END); q = r['requested_range']
    assert q['valid_unique_bars'] == 2 and q['missing_bars'] == 3
    assert (q['leading_missing_bars'],q['internal_missing_bars'],q['trailing_missing_bars']) == (0,2,1)
    assert q['invalid_timestamp_row_positions'] == [4,8,9]
    assert q['unaligned_timestamp_row_positions'] == [5]
    assert q['out_of_range_row_positions'] == [6,7]
    assert r['checks']['missing_bars']['status'] == 'UNVERIFIED'
    assert r['checks']['requested_out_of_range']['count'] == 2


@pytest.mark.parametrize('case', ['metadata','period','header','width','missing_file'])
def test_range_early_failure(tmp_path, case):
    p, d = fixture(tmp_path)
    if case == 'metadata': d['extra'] = 1
    if case == 'period': d['timeframe'] = '2h'
    if case == 'header': p, d = fixture(tmp_path, header=['bad','open','high','low','close','qty'])
    if case == 'width': p, d = fixture(tmp_path, [ROWS[0][:-1]])
    if case == 'missing_file': p = tmp_path/'missing.csv'
    r = validate(p, d, CONTEXT, START, END)
    assert r['structural_status'] == 'FAIL'
    assert r['requested_range'] == {'status':'UNVERIFIED'}


def test_range_large_arithmetic_and_legacy_identity(tmp_path):
    p, d = fixture(tmp_path, [], timeframe='1m')
    r = validate(p, d, CONTEXT, '1970-01-01T00:00:00Z', '2200-01-01T00:00:00Z')
    from datetime import datetime, timezone
    expected = int((datetime(2200,1,1,tzinfo=timezone.utc)-datetime(1970,1,1,tzinfo=timezone.utc)).total_seconds())//60
    q = r['requested_range']
    assert q['expected_bars'] == q['missing_bars'] == q['full_range_missing_bars'] == expected
    assert len(q['missing_sample_utc']) == 10
    assert 'requested_range' not in validate(p, d, CONTEXT)
    assert validate(p, d, CONTEXT) == validate(p, d, CONTEXT, None, None)


def test_range_cli(tmp_path):
    p, d = fixture(tmp_path)
    meta, ctx = tmp_path/'metadata.json', tmp_path/'context.json'
    meta.write_text(json.dumps(d)); ctx.write_text(json.dumps(CONTEXT))
    cmd = [sys.executable,'-B','-m','candle_audit.candle_quality','--csv',str(p),
           '--metadata',str(meta),'--okx-context',str(ctx)]
    for flags, code, status in [(['--start',START,'--end','2026-01-01T03:00:00Z'],0,'COMPLETE'),
                                (['--start',START,'--end',END],1,'INCOMPLETE'),
                                (['--start',START],1,'UNVERIFIED')]:
        result = subprocess.run(cmd+flags,capture_output=True,text=True)
        assert result.returncode == code, result.stderr
        assert json.loads(result.stdout)['requested_range']['status'] == status


@pytest.mark.parametrize('prices,logic,positive,nonfinite,nonnumeric,undecidable', [
    (['17.125','18.249999999999999999','16','18.25'],1,0,0,0,0),
    (['1e1000','2e1000','1e999','1.5e1000'],0,0,0,0,0),
    (['1e-1000','2e-1000','1e-1001','1.5e-1000'],0,0,0,0,0),
    (['1','1','0.999999999999999999999999999999999999','1'],0,0,0,0,0),
    (['-0','1','-0e-1000','0'],0,3,0,0,0),
    (['1','NaN','0.5','1'],0,0,1,0,1),
    (['sNaN','Infinity','-Infinity','1'],0,0,3,0,1),
    (['bad','0.999999999999999999','0','1'],1,1,0,1,1),
    (['bad','1','0.5','1'],0,0,0,1,1),
    (['1.825e1','18.250000000000000000','1.7E+1','18.25'],0,0,0,0,0),
    (['1e-1000','9.999999999999999999e-1001','1e-1001','1e-1000'],1,0,0,0,0),
    (['1e1000','9.999999999999999999e999','1e999','1e1000'],1,0,0,0,0),
])
def test_raw_decimal_ohlc(tmp_path, prices, logic, positive, nonfinite, nonnumeric, undecidable):
    # Fixed expectations above are hand specified, independent of implementation.
    rows = copy.deepcopy(ROWS)
    rows[1][1:5] = prices
    p, d = fixture(tmp_path, rows)
    r = validate(p, d, CONTEXT)
    for key, count in [('ohlc_logic',logic),('ohlc_positivity',positive),
                       ('nonfinite',nonfinite),('nonnumeric',nonnumeric)]:
        assert r['checks'][key]['count'] == count
        assert r['checks'][key]['row_positions'] == ([2] if count else [])
    assert r['checks']['ohlc_logic']['undecidable_count'] == undecidable
    assert r['checks']['ohlc_logic']['status'] == ('FAIL' if logic else 'UNVERIFIED' if undecidable else 'PASS')
    assert r['structural_status'] == ('FAIL' if logic or positive or nonfinite or nonnumeric else 'PASS')
    assert r['checks']['volume_units']['status'] == 'PASS'
    assert r['source_consistency'] == 'UNVERIFIED' and r['admission'] == 'NOT_EVALUATED'
