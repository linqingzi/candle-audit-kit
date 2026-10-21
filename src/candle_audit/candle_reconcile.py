"""Bounded offline comparison of declared OKX trade candles; never admission."""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import re
from decimal import Decimal
from pathlib import Path

import pandas as pd

from candle_audit.candle_quality import validate
from candle_audit.contract import data_versions
from candle_audit.contract.registry import RegistryError

MAX_BYTES = 8 * 1024 * 1024
MAX_ROWS = 10000
MAX_NUMBER = 96
BARS = {'1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m',
        '1h': '1H', '4h': '4H', '1d': '1Dutc'}


def read(path, limit=MAX_BYTES):
    """Bound allocation even if a file grows after opening."""
    with Path(path).open('rb') as stream:
        value = stream.read(limit + 1)
    if len(value) > limit:
        raise ValueError('input byte limit exceeded')
    return value


def number(value):
    if not isinstance(value, str) or len(value) > MAX_NUMBER or not re.fullmatch(
            r'[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d{1,3})?', value):
        raise ValueError('bounded decimal string required')
    result = Decimal(value)
    if not result.is_finite():
        raise ValueError('finite decimal required')
    return result


def strict_json(content):
    # Bound lexical complexity before the JSON parser allocates containers.
    text = content.decode('utf-8')
    depth = 0
    tokens = 0
    for token in re.finditer(r'"(?:[^"\\]|\\.)*"|[\[\]{}:,]|[^\s\[\]{}:,]+', text):
        value = token.group()
        tokens += 1
        if len(value) > 4096 or tokens > MAX_ROWS * 32 + 100:
            raise ValueError('JSON token limit exceeded')
        if value in ('[', '{'):
            depth += 1
            if depth > 6:
                raise ValueError('JSON nesting limit exceeded')
        elif value in (']', '}'):
            depth -= 1
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('duplicate JSON key')
            result[key] = value
        return result
    return json.loads(text, object_pairs_hook=pairs,
                      parse_constant=lambda _: (_ for _ in ()).throw(ValueError('illegal JSON constant')),
                      parse_int=lambda _: (_ for _ in ()).throw(ValueError('JSON numbers must be strings')),
                      parse_float=lambda _: (_ for _ in ()).throw(ValueError('JSON numbers must be strings')))


def utc(value):
    if not isinstance(value, str) or not re.fullmatch(
            r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|\+00:00)', value):
        raise ValueError('strict UTC opening timestamp required')
    return pd.Timestamp(value).value


def unknown_identity():
    return dict(status='UNVERIFIED', basis='UNKNOWN', venue=None, instId=None,
                market_kind=None, bar=None, time_basis=None)


def opening_coverage():
    return dict(status='UNKNOWN', min_utc=None, max_utc=None, valid_unique_count=None,
                in_requested_range_count=None,
                basis='distinct valid UTC epoch-aligned opening timestamps; not OHLC validity')


def record_openings(report, name, points, start, end, step):
    valid = {t for t in points if t % step == 0}
    report['actual_range'][name] = dict(
        status='OBSERVED', min_utc=iso_utc(min(valid)) if valid else None,
        max_utc=iso_utc(max(valid)) if valid else None, valid_unique_count=len(valid),
        in_requested_range_count=sum(start <= t < end for t in valid),
        basis=opening_coverage()['basis'])


def iso_utc(value):
    return pd.Timestamp(value, unit='ns', tz='UTC').isoformat().replace('+00:00', 'Z')


def empty_report():
    return dict(schema="candle-audit-offline-candle-reconcile-v1", comparison="NOT_COMPARABLE",
                coverage={"status": "UNVERIFIED"}, closedbar={"status": "UNVERIFIED"},
                provenance="UNVERIFIED", source_verification="UNVERIFIED",
                admission="NOT_EVALUATED", errors=[], hashes={k: None for k in ('candidate', 'raw', 'metadata', 'source')},
                declarations=dict(status='UNKNOWN', metadata=None, source=None, source_truncated_fields=[],
                                  source_verification='UNVERIFIED',
                                  candidate_identity=unknown_identity(), raw_identity=unknown_identity()),
                requested_range=dict(status='UNKNOWN', start_utc=None, end_utc=None,
                                     interval='[start,end)', time_basis='UTC candle opening'),
                actual_range=dict(candidate=opening_coverage(), raw=opening_coverage()))


def reconcile(csv_path, metadata, raw_path, source, start, end):
    """Source is an explicit declaration, not proof of acquisition or instrument truth.

    No multiplier conversion: compare contracts directly with vol, base directly
    with SPOT vol / SWAP volCcy, and quote directly with volCcyQuote.
    """
    report = empty_report()
    try:
        cb = read(csv_path)
        report['hashes']['candidate'] = hashlib.sha256(cb).hexdigest()
        rb = read(raw_path)
        report['hashes']['raw'] = hashlib.sha256(rb).hexdigest()
        if not isinstance(metadata, dict) or not isinstance(source, dict):
            raise ValueError('metadata and source must be objects')
        if metadata.get('kind') == 'synthetic' or source.get('acquisition') == 'SYNTHETIC':
            report['provenance'] = 'SYNTHETIC'
        if metadata.get('schema') not in (data_versions.SCHEMA, data_versions.VOLUME_SCHEMA):
            raise ValueError('only v1/v2 candidates supported')
        metadata = data_versions.metadata(metadata)
        report['declarations']['metadata'] = {
            k: metadata[k][:240] for k in
            ('schema', 'source', 'provenance', 'market', 'kind', 'timeframe', 'timezone')}
        report['declarations']['metadata'].update(
            verification='UNVERIFIED',
            truncated_fields=[k for k, v in report['declarations']['metadata'].items()
                              if len(metadata[k]) > len(v)])
        keys = {'venue', 'instId', 'instType', 'base_asset', 'quote_asset',
                'bar', 'endpoint', 'source_type', 'acquisition'}
        if set(source) != keys or source['source_type'] != 'trade' or source['endpoint'] != '/api/v5/market/history-candles':
            raise ValueError('exact history trade candle source declaration required')
        if source['acquisition'] not in ('UNVERIFIED', 'SYNTHETIC'):
            raise ValueError('acquisition cannot be locally VERIFIED')
        if source['bar'] != BARS.get(metadata.get('timeframe')):
            raise ValueError('bar/timeframe mismatch (daily requires 1Dutc)')
        a, b = utc(start), utc(end)
        step = pd.Timedelta(metadata['timeframe']).value
        if a >= b or a % step or b % step:
            raise ValueError('aligned increasing [start,end) required')
        report['requested_range'].update(status='VALIDATED', start_utc=iso_utc(a), end_utc=iso_utc(b))
        # Stream CSV rows into a bounded list before invoking the existing diagnostic.
        rows = []
        for row in csv.reader(io.StringIO(cb.decode('utf-8'))):
            if len(rows) >= MAX_ROWS + 1 or len(row) > 8 or any(len(x) > MAX_NUMBER for x in row):
                raise ValueError('candidate row/cell limit exceeded')
            rows.append(row)
        context = {k: source[k] for k in ('venue','instId','instType','base_asset','quote_asset')}
        quality = validate(csv_path, metadata, context, start=start, end=end)
        if quality.get('declarations', {}).get('okx_context') is not None:
            report['declarations'].update(
                status='VALIDATED', source={k: v[:240] for k, v in source.items()},
                source_truncated_fields=[k for k, v in source.items() if len(v) > 240])
            report['declarations']['candidate_identity'] = dict(
                status='UNVERIFIED', basis='validated supplied context declaration; not authentication',
                venue=source['venue'], instId=source['instId'][:240], market_kind=source['instType'],
                bar=source['bar'], time_basis='UTC candle opening ISO timestamp')
            report['declarations']['raw_identity'] = dict(
                status='UNVERIFIED', basis='supplied request context; raw arrays lack instrument/bar identity',
                venue=source['venue'], instId=source['instId'][:240], market_kind=source['instType'],
                bar=source['bar'], time_basis='UTC candle opening milliseconds')
        # The validator rereads read-only input: detect concurrent byte changes.
        if read(csv_path) != cb:
            raise ValueError('candidate changed during validation')
        report['candidate_quality'] = {'status': quality['structural_status'], 'errors': quality['errors']}
        width = 5 + len(metadata.get('volume', {}).get('columns', []))
        if not rows or any(len(row) != width for row in rows):
            raise ValueError('candidate row width invalid')
        data = strict_json(rb)
        if not isinstance(data, dict) or set(data) != {'code', 'msg', 'data'} or data['code'] != '0' or not isinstance(data['msg'], str):
            raise ValueError('invalid/API error response envelope')
        raw = data['data']
        if not isinstance(raw, list) or len(raw) > MAX_ROWS:
            raise ValueError('raw row limit/type')
        points, duplicates, conflicts = {}, 0, 0
        unclosed, missing_confirm = 0, 0
        raw_duplicate_samples = []
        report['raw_diagnostics'] = {'rows_processed': 0, 'duplicates': 0, 'conflicts': 0,
                                     'unclosed': 0, 'missing_confirm': 0}
        for row in raw:
            if not isinstance(row, list) or len(row) not in (8, 9):
                raise ValueError('raw row requires eight fields plus optional confirm')
            ts = row[0]
            if not isinstance(ts, str) or not re.fullmatch(r'\d{13}', ts):
                raise ValueError('raw opening requires 13-digit UTC milliseconds; seconds refused')
            t = int(ts) * 1000000
            if t % step:
                raise ValueError('raw opening not bar aligned')
            nums = tuple(number(x) for x in row[1:8])
            o, h, l, c = nums[:4]
            if min(o,h,l,c) <= 0 or h < max(o,l,c) or l > min(o,h,c) or min(nums[4:]) < 0:
                raise ValueError('invalid raw OHLC/volume')
            confirm = row[8] if len(row) == 9 else None
            if confirm not in (None, '0', '1'):
                raise ValueError('invalid confirm')
            missing_confirm += confirm is None
            unclosed += confirm == '0'
            if t in points:
                duplicates += 1
                if len(raw_duplicate_samples) < 10:
                    raw_duplicate_samples.append(str(t//1000000))
                conflicts += points[t] != (nums, confirm)
            else:
                points[t] = (nums, confirm)
            report['raw_diagnostics'].update(rows_processed=report['raw_diagnostics']['rows_processed']+1,
                                             duplicates=duplicates, conflicts=conflicts,
                                             unclosed=unclosed, missing_confirm=missing_confirm)
        record_openings(report, 'raw', points, a, b, step)
        report['closedbar'] = {'status': 'UNVERIFIED' if missing_confirm or not raw else ('UNCLOSED' if unclosed else 'CLOSED'),
                               'unclosed': unclosed, 'missing_confirm': missing_confirm}
        candidates = {}
        candidate_duplicates, candidate_conflicts = 0, 0
        duplicate_samples = []
        for row in rows[1:]:
            t = utc(row[0])
            nums = tuple(number(x) for x in row[1:])
            if t in candidates:
                candidate_duplicates += 1
                candidate_conflicts += candidates[t] != nums
                if len(duplicate_samples) < 10:
                    duplicate_samples.append(str(t//1000000))
            else:
                candidates[t] = nums
        record_openings(report, 'candidate', candidates, a, b, step)
        cp, rp = set(candidates), set(points)
        expected = (b-a)//step
        ci, ri = {t for t in cp if a <= t < b and t % step == 0}, {t for t in rp if a <= t < b and t % step == 0}
        report['coverage'] = dict(status='COMPLETE' if len(ci) == len(ri) == expected else 'INCOMPLETE',
                                  expected=expected, candidate_missing=expected-len(ci), raw_missing=expected-len(ri),
                                  candidate_outside=sum(not a <= t < b for t in cp), raw_outside=sum(not a <= t < b for t in rp),
                                  candidate_unaligned=sum(t % step != 0 for t in cp),
                                  missing_raw=len(cp-rp), extra_raw=len(rp-cp),
                                  raw_duplicates=duplicates, raw_conflicts=conflicts,
                                  raw_duplicate_samples_ms=raw_duplicate_samples,
                                  candidate_duplicates=candidate_duplicates, candidate_conflicts=candidate_conflicts,
                                  candidate_duplicate_samples_ms=duplicate_samples,
                                  missing_raw_samples_ms=[str(t//1000000) for t in sorted(cp-rp)[:10]],
                                  extra_raw_samples_ms=[str(t//1000000) for t in sorted(rp-cp)[:10]])
        mismatches = 0
        samples = []
        descriptors = metadata.get('volume', {}).get('columns', [])
        indexes = {'base': 4 if source['instType'] == 'SPOT' else 5, 'quote': 6, 'contracts': 4}
        for t in sorted(cp & rp):
            actual = candidates[t]
            reference = points[t][0]
            selected = reference[:4] + tuple(reference[indexes[d['unit']]] for d in descriptors)
            for i, (x, y) in enumerate(zip(actual, selected)):
                if x != y:
                    mismatches += 1
                    if len(samples) < 10:
                        samples.append({'opening_ms': str(t//1000000), 'column': rows[0][i+1],
                                        'candidate': str(x), 'raw': str(y)})
        report['differences'] = {'count': mismatches, 'samples': samples}
        if quality['structural_status'] != 'PASS':
            report['errors'].append('candidate structural failure')
        # Structural failures never become a comparable success. Missing/extra and
        # duplicate raw rows are explicit mismatches, separate from coverage/closure.
        report['comparison'] = ('NOT_COMPARABLE' if report['errors'] else
                                'MISMATCH' if mismatches or cp != rp or duplicates else 'MATCH')
    except (RegistryError, OSError, ValueError, TypeError, KeyError, OverflowError, csv.Error, RecursionError) as exc:
        report['errors'].append(str(exc)[:240])
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for arg in ('csv', 'metadata', 'raw', 'source', 'start', 'end'):
        parser.add_argument('--' + arg, required=True)
    args = parser.parse_args()
    result = empty_report()
    try:
        # Declaration documents have their own byte bounds; standard JSON permits
        # the existing contract_size numeric metadata representation.
        mb = read(args.metadata, 65536)
        result['hashes']['metadata'] = hashlib.sha256(mb).hexdigest()
        sb = read(args.source, 65536)
        result['hashes']['source'] = hashlib.sha256(sb).hexdigest()
        metadata = json.loads(mb)
        source = json.loads(sb)
        result = reconcile(args.csv, metadata, args.raw, source, args.start, args.end)
        result['hashes'].update(metadata=hashlib.sha256(mb).hexdigest(), source=hashlib.sha256(sb).hexdigest())
    except (OSError, ValueError, RecursionError) as exc:
        result['errors'].append(str(exc)[:240])
    print(json.dumps(result, sort_keys=True))
    return 0 if result['comparison'] == 'MATCH' else 1


if __name__ == '__main__':
    raise SystemExit(main())
