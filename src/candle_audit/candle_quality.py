"""Offline diagnostics for exact version-v1/v2 CSVs; never an admission gate."""
from __future__ import annotations

import argparse
import csv
from decimal import Decimal, InvalidOperation
import hashlib
import io
import json
import math
import re
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from candle_audit.contract import data_versions
from candle_audit.contract.registry import RegistryError


def validate(csv_path, doc, context, start=None, end=None):
    """Read bytes once, report independent diagnostics without repairing input.

    Context is an owner declaration, not verified exchange instrument metadata.
    Supported markets are SPOT and linear USDT SWAP only.
    """
    report = {'schema': 'candle-audit-local-candle-quality-v1', 'source_consistency': 'UNVERIFIED',
              'admission': 'NOT_EVALUATED', 'errors': [], 'warnings': [], 'checks': {}}

    def check(name, count=0, detail=None, warning=False, positions=None):
        entry = {'status': ('REVIEW' if warning else 'FAIL') if count else 'PASS',
                 'count': int(count)}
        if positions is not None:
            entry['row_positions'] = positions[:10]
        if detail is not None:
            entry['detail'] = detail
        report['checks'][name] = entry
        if count:
            report['warnings' if warning else 'errors'].append(name)

    def finish():
        report['structural_status'] = 'FAIL' if report['errors'] else 'PASS'
        return report

    requested = start is not None or end is not None
    if requested:
        report['requested_range'] = {'status': 'UNVERIFIED'}
    try:
        if not isinstance(doc, dict) or doc.get('schema') not in (data_versions.SCHEMA, data_versions.VOLUME_SCHEMA):
            raise ValueError('only exact version v1/v2 metadata supported')
        doc = data_versions.metadata(doc)
        report['declarations'] = {
            'source_verification': 'UNVERIFIED',
            'metadata': doc,
            'okx_context': None,
        }
        check('metadata')
    except (RegistryError, ValueError, TypeError, KeyError, OverflowError) as exc:
        check('metadata', 1, str(exc))
        return finish()
    bounds = None
    if requested:
        report['requested_range'] = {'status': 'UNVERIFIED'}
        try:
            if start is None or end is None:
                raise ValueError('start and end must be supplied together')
            parsed = []
            for value in (start, end):
                if not isinstance(value, str) or not re.fullmatch(
                        r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|\+00:00)', value):
                    raise ValueError('strict ISO UTC timestamp with Z or +00:00 required')
                parsed.append(pd.Timestamp(value).value)
            step = pd.Timedelta(doc['timeframe']).value
            a, b = parsed
            if a >= b or a % step or b % step:
                raise ValueError('range must be increasing and UTC epoch period aligned')
            bounds = (a, b, step)
            check('requested_range_arguments')
        except (ValueError, TypeError, OverflowError) as exc:
            check('requested_range_arguments', 1, str(exc))
            return finish()
    try:
        if not isinstance(context, dict) or set(context) != {'venue', 'instId', 'instType', 'base_asset', 'quote_asset'}:
            raise ValueError('exact OKX context keys required')
        base, quote = context['base_asset'], context['quote_asset']
        if not all(isinstance(x, str) and re.fullmatch(r'[A-Z0-9]+', x) for x in (base, quote)) or base == quote:
            raise ValueError('explicit distinct uppercase assets required')
        kind = context['instType']
        suffix = '' if kind == 'SPOT' else '-SWAP'
        if context['venue'] != 'OKX' or kind not in ('SPOT', 'SWAP') or (kind == 'SWAP' and quote != 'USDT'):
            raise ValueError('only OKX SPOT or linear USDT SWAP supported')
        if context['instId'] != f'{base}-{quote}{suffix}' or doc['market'] != context['instId'] or doc['currency'] != quote:
            raise ValueError('exact instId/market/quote identity mismatch')
        if 'volume' in doc:
            v = doc['volume']
            if v['base_asset'] != base or v['quote_asset'] != quote:
                raise ValueError('volume assets disagree with instrument')
            if kind == 'SPOT' and any(c['unit'] == 'contracts' for c in v['columns']):
                raise ValueError('SPOT cannot declare contracts')
        report['declarations']['okx_context'] = dict(context)
        check('okx_context')
    except (ValueError, TypeError, KeyError) as exc:
        check('okx_context', 1, str(exc))
    try:
        content = Path(csv_path).read_bytes()
        report['content_sha256'] = hashlib.sha256(content).hexdigest()
        check('content_hash', report['content_sha256'] != doc['content_sha256'])
        rows = list(csv.reader(io.StringIO(content.decode('utf-8'))))
    except (OSError, UnicodeError, csv.Error) as exc:
        check('csv_read', 1, str(exc))
        return finish()
    expected = ['timestamp', 'open', 'high', 'low', 'close'] + [c['column'] for c in doc.get('volume', {}).get('columns', [])]
    header = rows[0] if rows else []
    check('missing_columns', len(set(expected) - set(header)), sorted(set(expected) - set(header)))
    check('column_layout', header != expected, 'exact ordered version columns required')
    body = rows[1:]
    report['bars'] = len(body)
    check('minimum_bars', len(body) < 2)
    check('row_width', sum(len(r) != len(header) for r in body),
          positions=[i for i, r in enumerate(body, 1) if len(r) != len(header)])
    check('missing_cells', sum(not c.strip() for r in body for c in r) + sum(max(0, len(header)-len(r)) for r in body),
          positions=[i for i, r in enumerate(body, 1)
                     if len(r) < len(header) or any(not c.strip() for c in r)])
    if header != expected or any(len(r) != len(header) for r in body):
        return finish()
    # Preserve original row positions: invalid timestamps never join neighbors.
    times = []
    timestamp_issues = {name: [] for name in
                        ('invalid_timestamp', 'timezone_missing', 'timezone_mismatch')}
    for position, row in enumerate(body, 1):
        value = None
        try:
            t = pd.Timestamp(row[0])
            if pd.isna(t):
                raise ValueError('NaT')
            if t.tzinfo is None:
                timestamp_issues['timezone_missing'].append(position)
            elif t.utcoffset() != t.tz_convert(ZoneInfo(doc['timezone'])).utcoffset():
                timestamp_issues['timezone_mismatch'].append(position)
            else:
                value = t.tz_convert('UTC').value
        except (ValueError, TypeError, OverflowError):
            timestamp_issues['invalid_timestamp'].append(position)
        times.append(value)
    for name, positions in timestamp_issues.items():
        check(name, len(positions), positions=positions)
    seen, duplicates = set(), []
    for position, t in enumerate(times, 1):
        if t is not None:
            if t in seen:
                duplicates.append(position)
            seen.add(t)
    check('duplicates', len(duplicates), positions=duplicates)
    pairs = [(i + 2, a, b) for i, (a, b) in enumerate(zip(times, times[1:]))
             if a is not None and b is not None]
    descending = [position for position, a, b in pairs if b < a]
    check('order', len(descending), positions=descending)
    step = pd.Timedelta(doc['timeframe']).value
    misaligned = [i for i, t in enumerate(times, 1) if t is not None and t % step]
    check('period_alignment', len(misaligned), positions=misaligned)
    spacing = [position for position, a, b in pairs if (b-a) % step]
    check('timeframe_spacing', len(spacing), positions=spacing)
    ready = all(t is not None for t in times) and not duplicates and not descending and not misaligned
    if ready:
        gaps = [position for position, a, b in pairs if b-a > step]
        check('missing_bars', sum((b-a)//step-1 for _, a, b in pairs), positions=gaps)
    else:
        report['checks']['missing_bars'] = {'status': 'UNVERIFIED', 'count': None}
    if bounds is not None:
        a, b, step = bounds
        points = sorted({t for t in times if t is not None and t % step == 0 and a <= t < b})
        outside = [i for i, t in enumerate(times, 1) if t is not None and not a <= t < b]
        invalid = [i for i, t in enumerate(times, 1) if t is None]
        expected_count = (b-a)//step
        missing = expected_count - len(points)
        leading = (points[0]-a)//step if points else 0
        trailing = (b-step-points[-1])//step if points else 0
        internal = missing-leading-trailing if points else 0
        sample = []
        cursor = a
        for t in points:
            take = min(10-len(sample), (t-cursor)//step)
            sample.extend(pd.Timestamp(cursor+i*step, tz='UTC').isoformat() for i in range(take))
            cursor = t+step
            if len(sample) == 10:
                break
        if len(sample) < 10:
            take = min(10-len(sample), (b-cursor)//step)
            sample.extend(pd.Timestamp(cursor+i*step, tz='UTC').isoformat() for i in range(take))
        report['requested_range'] = {
            'status': 'ALL_MISSING' if not points else ('INCOMPLETE' if missing else 'COMPLETE'),
            'start_utc': pd.Timestamp(a, tz='UTC').isoformat(),
            'end_utc_exclusive': pd.Timestamp(b, tz='UTC').isoformat(),
            'expected_bars': expected_count, 'valid_unique_bars': len(points),
            'missing_bars': missing, 'leading_missing_bars': leading,
            'internal_missing_bars': internal, 'trailing_missing_bars': trailing,
            'full_range_missing_bars': missing if not points else 0,
            'missing_sample_utc': sample,
            'out_of_range_rows': len(outside), 'out_of_range_row_positions': outside[:10],
            'invalid_timestamp_rows': len(invalid), 'invalid_timestamp_row_positions': invalid[:10],
            'unaligned_timestamp_rows': len(misaligned), 'unaligned_timestamp_row_positions': misaligned[:10],
        }
        check('requested_missing_bars', missing)
        check('requested_out_of_range', len(outside), positions=outside)
    issue_positions = {name: [] for name in ('nonnumeric', 'nonfinite', 'ohlc_positivity',
                       'ohlc_logic', 'negative_volume', 'zero_volume', 'price_jump')}
    counts = dict.fromkeys(issue_positions, 0)
    undecidable = []
    unit_positions, unit_details = [], []
    previous = None
    for position, row in enumerate(body, 1):
        values = []
        for index, cell in enumerate(row[1:]):
            try:
                values.append(Decimal(cell) if index < 4 else float(cell))
            except (ValueError, InvalidOperation):
                values.append(None)
        finite = lambda v: v is not None and (v.is_finite() if isinstance(v, Decimal) else math.isfinite(v))
        o, h, l, c = values[:4]
        comparisons = [(h, o, 'high'), (h, c, 'high'),
                       (l, o, 'low'), (l, c, 'low'), (h, l, 'high')]
        available = [(a, b, kind) for a, b, kind in comparisons if finite(a) and finite(b)]
        if len(available) != len(comparisons):
            undecidable.append(position)
        adjacent = (position > 1 and times[position-1] is not None
                    and times[position-2] is not None
                    and times[position-1] - times[position-2] == step
                    and times[position-1] % step == 0)
        # Keep the existing binary-float jump REVIEW heuristic separate from exact
        # OHLC validity. Volume conversion also retains its existing float policy.
        review_close = float(c) if finite(c) else None
        found = {
            'nonnumeric': sum(v is None for v in values),
            'nonfinite': sum(v is not None and not finite(v) for v in values),
            'ohlc_positivity': sum(finite(v) and v <= 0 for v in values[:4]),
            'ohlc_logic': int(any(a < b if kind == 'high' else a > b
                                  for a, b, kind in available)),
            'negative_volume': sum(finite(v) and v < 0 for v in values[4:]),
            'zero_volume': sum(v == 0 for v in values[4:]),
            'price_jump': int(bool(adjacent and previous is not None and previous > 0
                                   and review_close is not None and math.isfinite(review_close)
                                   and review_close > 0 and abs(review_close/previous-1) > .20)),
        }
        previous = review_close if review_close is not None and math.isfinite(review_close) else None
        for name, count in found.items():
            counts[name] += count
            if count:
                issue_positions[name].append(position)
        if 'volume' in doc:
            # Validate each descriptor independently, then each decidable fixed-size
            # relation. A bad unrelated field cannot suppress another unit failure.
            descriptors = doc['volume']['columns']
            by_column = dict(zip(expected[5:], values[4:]))
            groups = [[d] for d in descriptors]
            for d in descriptors:
                if d['unit'] == 'contracts':
                    groups += [[d, target] for target in descriptors
                               if target['unit'] == d['contract_size_unit']]
            failures = []
            for group in groups:
                if any(by_column[d['column']] is None for d in group):
                    failures.append('nonnumeric volume: ' + ','.join(
                        d['column'] for d in group if by_column[d['column']] is None))
                    continue
                subset = dict(doc, volume=dict(doc['volume'], columns=group))
                try:
                    frame = pd.DataFrame({d['column']: [by_column[d['column']]] for d in group})
                    data_versions.volume_columns(frame, subset)
                except (RegistryError, ValueError, TypeError, KeyError, OverflowError) as exc:
                    failures.append(str(exc))
            if failures:
                unit_positions.append(position)
                for detail in failures:
                    if detail not in unit_details:
                        unit_details.append(detail)
    for name, count in counts.items():
        check(name, count, warning=name in ('zero_volume', 'price_jump'),
              positions=issue_positions[name])
    logic_check = report['checks']['ohlc_logic']
    logic_check['undecidable_count'] = len(undecidable)
    logic_check['undecidable_row_positions'] = undecidable[:10]
    if undecidable and not counts['ohlc_logic']:
        logic_check['status'] = 'UNVERIFIED'
    if 'volume' in doc:
        check('volume_units', len(unit_positions), unit_details or None, positions=unit_positions)
    return finish()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--csv', required=True)
    parser.add_argument('--metadata', required=True)
    parser.add_argument('--okx-context', required=True)
    parser.add_argument('--start')
    parser.add_argument('--end')
    args = parser.parse_args(argv)
    try:
        report = validate(args.csv, json.loads(Path(args.metadata).read_text()),
                          json.loads(Path(args.okx_context).read_text()), args.start, args.end)
    except (OSError, ValueError, TypeError) as exc:
        report = {'structural_status': 'FAIL', 'source_consistency': 'UNVERIFIED',
                  'admission': 'NOT_EVALUATED', 'errors': ['input_read'], 'detail': str(exc)}
    print(json.dumps(report, sort_keys=True, allow_nan=False))
    return 1 if report['structural_status'] == 'FAIL' else 0


if __name__ == '__main__':
    raise SystemExit(main())
