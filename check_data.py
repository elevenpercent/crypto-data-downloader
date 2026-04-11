#!/usr/bin/env python3
"""
check_data.py — Data quality validation tool for crypto-data-downloader

Usage:
  python check_data.py --file AllData/btcusdt/btcusdt_2023.zip
  python check_data.py --file AllData/btcusdt/btcusdt_2014.zip
  python check_data.py --merge-check --symbol btcusdt --year 2014
  python check_data.py --api-research
"""

import argparse
import csv
import io
import os
import sys
import zipfile
from collections import defaultdict
from datetime import datetime, timezone

# Import exchange fetchers and helpers from the main downloader
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from download_crypto_data import (
    _bitfinex_month, _bitstamp_month, _BFX_PAIRS, _BSTAMP_PAIRS, normalize,
)

# ── constants ─────────────────────────────────────────────────────────────────
_CANDLE_MS   = 60_000   # 1 minute in milliseconds
_TOL_PCT     = 0.001    # 0.1% price tolerance for OHLC range checks
_MAX_SPIKE   = 0.20     # 20% max candle-to-candle close change
_BIAS_THRESH = 0.001    # 0.1% systematic bias threshold for merge check

# ── display helpers ───────────────────────────────────────────────────────────

def _ts_str(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')


def _div(char='═', width=72):
    print(char * width)


def _load_csv_from_zip(zip_path: str) -> list:
    """Open a zip, find the CSV, skip the header, return raw string-tuple rows."""
    with zipfile.ZipFile(zip_path, 'r') as zf:
        csv_name = next((n for n in zf.namelist() if n.endswith('.csv')), None)
        if csv_name is None:
            raise ValueError(f'No CSV found inside {zip_path}')
        with zf.open(csv_name) as f:
            reader = csv.reader(io.TextIOWrapper(f, 'utf-8'))
            next(reader, None)          # skip header
            return [row for row in reader if row]


# ══════════════════════════════════════════════════════════════════════════════
# PART 1 — Candle Quality Checker
# ══════════════════════════════════════════════════════════════════════════════

def check_candles(zip_path: str) -> None:
    _div()
    print('  CANDLE QUALITY REPORT')
    print(f'  File : {zip_path}')
    _div()

    raw = _load_csv_from_zip(zip_path)
    print(f'\n  Total candles in file : {len(raw):,}\n')

    if not raw:
        print('  [!] Empty file — nothing to check.\n')
        _div()
        return

    # ── parse rows ────────────────────────────────────────────────────────────
    candles = []
    parse_errors = 0
    for row in raw:
        try:
            candles.append((
                int(row[0]),           # ts
                float(row[1]),         # open
                float(row[2]),         # high
                float(row[3]),         # low
                float(row[4]),         # close
                row[5].strip() if len(row) > 5 else '',   # volume (keep as str for null check)
            ))
        except (ValueError, IndexError):
            parse_errors += 1

    if parse_errors:
        print(f'  [!] Skipped {parse_errors:,} unparseable rows\n')

    # ── failure buckets ───────────────────────────────────────────────────────
    fail_hl       = []   # 1. high < low
    fail_open     = []   # 2. open outside H/L ± tolerance
    fail_close    = []   # 3. close outside H/L ± tolerance
    fail_vol_zero = []   # 4. volume == 0
    fail_vol_null = []   # 5. volume null/empty/non-numeric
    fail_dup      = []   # 6. duplicate timestamp
    fail_gap      = []   # 7a. gap > 60 000 ms  (missing candles)
    fail_overlap  = []   # 7b. gap 0 < x < 60 000 ms  (overlap)
    fail_spike    = []   # 8. close-to-close change > 20%

    seen_ts    = {}
    prev_ts    = None
    prev_close = None
    volumes    = []

    for ts, o, h, l, c, vol_str in candles:
        tol = max(h, 1e-9) * _TOL_PCT    # 0.1% of price

        # 1. High >= Low
        if h < l:
            fail_hl.append((ts, o, h, l, c, vol_str))

        # 2. Open within H/L  (±0.1% price tolerance covers float rounding)
        if not (l - tol <= o <= h + tol):
            fail_open.append((ts, o, h, l, c, vol_str))

        # 3. Close within H/L
        if not (l - tol <= c <= h + tol):
            fail_close.append((ts, o, h, l, c, vol_str))

        # 4 & 5. Volume
        if vol_str == '':
            fail_vol_null.append((ts, o, h, l, c, vol_str))
        else:
            try:
                vf = float(vol_str)
                volumes.append(vf)
                if vf == 0.0:
                    fail_vol_zero.append((ts, o, h, l, c, vol_str))
            except ValueError:
                fail_vol_null.append((ts, o, h, l, c, vol_str))

        # 6. Duplicate timestamps
        if ts in seen_ts:
            fail_dup.append((ts, o, h, l, c, vol_str))
        else:
            seen_ts[ts] = True

        # 7. Timestamp spacing
        if prev_ts is not None:
            diff = ts - prev_ts
            if diff > _CANDLE_MS:
                fail_gap.append((ts, o, h, l, c, vol_str, diff))
            elif 0 < diff < _CANDLE_MS:
                fail_overlap.append((ts, o, h, l, c, vol_str, diff))

        # 8. Price spike
        if prev_close is not None and prev_close != 0:
            change = abs(c - prev_close) / prev_close
            if change > _MAX_SPIKE:
                fail_spike.append((ts, o, h, l, c, vol_str, change, prev_close))

        prev_ts    = ts
        prev_close = c

    # total missing minutes across all gap events
    total_missing = sum((row[6] // _CANDLE_MS) - 1 for row in fail_gap)

    # ── report helpers ────────────────────────────────────────────────────────
    W = 48   # label column width

    def _row(ts, o, h, l, c, vol):
        return f'       {_ts_str(ts)}  O={o}  H={h}  L={l}  C={c}  V={vol}'

    def _check(label, failures, extra_fn=None):
        ok     = not failures
        status = 'PASS' if ok else f'FAIL  ({len(failures):,} issues)'
        print(f'  {"✓" if ok else "✗"}  {label:<{W}} {status}')
        if not ok:
            show = failures[:3]
            for row in show:
                print(_row(*row[:6]))
            if extra_fn:
                for row in show:
                    extra_fn(row)

    _div('─')
    print('  CHECK RESULTS')
    _div('─')

    _check('1. High >= Low',                     fail_hl)
    _check('2. Open within High/Low (±0.1%)',    fail_open)
    _check('3. Close within High/Low (±0.1%)',   fail_close)
    _check('4. Volume > 0',                      fail_vol_zero)
    _check('5. Volume not null/empty',           fail_vol_null)
    _check('6. No duplicate timestamps',         fail_dup)

    # 7a: gap (needs extra detail line)
    ok = not fail_gap
    print(f'  {"✓" if ok else "✗"}  {"7a. No missing candles (gap > 60 s)":<{W}} '
          f'{"PASS" if ok else f"FAIL  ({len(fail_gap):,} gaps)"}')
    if fail_gap:
        for row in fail_gap[:3]:
            ts, o, h, l, c, vol, diff = row
            missing = (diff // _CANDLE_MS) - 1
            print(f'       {_ts_str(ts)}  gap={diff//1000}s  (~{missing:,} missing candle(s))')

    # 7b: overlap
    ok = not fail_overlap
    print(f'  {"✓" if ok else "✗"}  {"7b. No overlapping candles (gap < 60 s)":<{W}} '
          f'{"PASS" if ok else f"FAIL  ({len(fail_overlap):,} overlaps)"}')
    if fail_overlap:
        for row in fail_overlap[:3]:
            ts, o, h, l, c, vol, diff = row
            print(f'       {_ts_str(ts)}  actual gap = {diff} ms  (expected 60 000 ms)')

    # 8: price spike
    ok = not fail_spike
    print(f'  {"✓" if ok else "✗"}  {"8. No price spike > 20% per candle":<{W}} '
          f'{"PASS" if ok else f"FAIL  ({len(fail_spike):,} spikes)"}')
    if fail_spike:
        for row in fail_spike[:3]:
            ts, o, h, l, c, vol, chg, pc = row
            print(f'       {_ts_str(ts)}  O={o}  C={c}  prev_close={pc}  change={chg:.2%}')

    # ── statistics ────────────────────────────────────────────────────────────
    print()
    _div('─')
    print('  STATISTICS')
    _div('─')
    if volumes:
        print(f'  Volume  min  : {min(volumes):.8f}')
        print(f'  Volume  max  : {max(volumes):.8f}')
        print(f'  Volume  avg  : {sum(volumes) / len(volumes):.8f}')
    else:
        print('  Volume  : no valid volume data')
    print(f'  Zero-volume candles   : {len(fail_vol_zero):,}')
    print(f'  Total missing minutes : {total_missing:,}')
    print(f'  Total gap events      : {len(fail_gap):,}')
    print(f'  Total overlap events  : {len(fail_overlap):,}')

    # ── score ─────────────────────────────────────────────────────────────────
    all_checks  = [fail_hl, fail_open, fail_close, fail_vol_zero, fail_vol_null,
                   fail_dup, fail_gap, fail_overlap, fail_spike]
    n_passed    = sum(1 for f in all_checks if not f)
    score       = n_passed / len(all_checks) * 100

    print()
    _div('─')
    print(f'  OVERALL DATA QUALITY SCORE : {score:.1f}%'
          f'  ({n_passed}/{len(all_checks)} checks passed)')
    _div()
    print()


# ══════════════════════════════════════════════════════════════════════════════
# PART 2 — Merge Bias Checker
# ══════════════════════════════════════════════════════════════════════════════

def check_merge_bias(symbol: str, year: int) -> None:
    sym = normalize(symbol)

    _div()
    print('  MERGE BIAS CHECKER')
    print(f'  Symbol : {sym.upper()}  |  Year : {year}  |  Analysis period : January')
    _div()

    if year >= 2017:
        print(f'\n  [!] Merge bias check targets pre-2017 dual-source data.')
        print(f'      Year {year} uses Binance as primary — no Bitfinex/Bitstamp overlap.')
        _div()
        return

    has_bfx    = sym in _BFX_PAIRS
    has_bstamp = sym in _BSTAMP_PAIRS

    if not has_bfx and not has_bstamp:
        print(f'\n  [!] {sym.upper()} is not supported by Bitfinex or Bitstamp.')
        _div()
        return

    sources = []
    if has_bfx:    sources.append('bitfinex')
    if has_bstamp: sources.append('bitstamp')

    if len(sources) < 2:
        print(f'\n  [!] Only one source available ({sources[0]}) — need 2 to compare.')
        _div()
        return

    print(f'\n  Downloading January {year} independently from: {" and ".join(sources)}\n')

    raw: dict[str, dict[int, tuple]] = {}   # src -> {ts_ms -> (o, h, l, c)}

    for src in sources:
        print(f'  ── {src.upper()} ──')
        try:
            if src == 'bitfinex':
                rows = _bitfinex_month(sym, year, 1)
            else:
                rows = _bitstamp_month(sym, year, 1)
            raw[src] = {int(r[0]): (float(r[1]), float(r[2]), float(r[3]), float(r[4]))
                        for r in rows}
            print(f'  Fetched {len(rows):,} candles from {src}\n')
        except Exception as exc:
            print(f'  [!] {src} failed: {exc}\n')
            raw[src] = {}

    src_a, src_b = sources[0], sources[1]
    data_a, data_b = raw[src_a], raw[src_b]

    if not data_a or not data_b:
        print('  [!] One or both sources returned no data — cannot compare.')
        _div()
        return

    common_ts = sorted(set(data_a) & set(data_b))

    print(f'  Candles in {src_a:<10}: {len(data_a):,}')
    print(f'  Candles in {src_b:<10}: {len(data_b):,}')
    print(f'  Common timestamps   : {len(common_ts):,}')

    if not common_ts:
        print('\n  [!] No common timestamps — cannot compare sources.')
        _div()
        return

    # ── price comparison ──────────────────────────────────────────────────────
    diffs        = []       # (close_a - close_b) / mid_price
    hour_diff    = defaultdict(list)   # hour -> list of diffs

    for ts in common_ts:
        oa, ha, la, ca = data_a[ts]
        ob, hb, lb, cb = data_b[ts]
        mid   = (ca + cb) / 2 or 1e-9
        d     = (ca - cb) / mid
        diffs.append(d)
        dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        hour_diff[dt.hour].append(d)

    avg_diff     = sum(diffs) / len(diffs)
    abs_avg_diff = sum(abs(d) for d in diffs) / len(diffs)
    pct_a_higher = sum(1 for d in diffs if d > 0) / len(diffs)

    dominant  = src_a if avg_diff > 0 else src_b
    dominated = src_b if avg_diff > 0 else src_a

    print()
    _div('─')
    print(f'  PRICE COMPARISON  ({len(common_ts):,} overlapping candles)')
    _div('─')
    print(f'  Average diff ({src_a} − {src_b}) / mid   : {avg_diff*100:+.4f}%')
    print(f'  Average absolute diff               : {abs_avg_diff*100:.4f}%')
    print(f'  Max diff                            : {max(diffs)*100:+.4f}%')
    print(f'  Min diff                            : {min(diffs)*100:+.4f}%')
    print(f'  Candles where {src_a} is higher       : {pct_a_higher*100:.1f}%')

    # ── hourly dominance ──────────────────────────────────────────────────────
    biased_hours = []
    for h in range(24):
        hd = hour_diff.get(h)
        if not hd:
            continue
        avg_h = sum(hd) / len(hd)
        if abs(avg_h) > _BIAS_THRESH:
            biased_hours.append((h, avg_h, len(hd)))

    print()
    _div('─')
    print('  HOURLY DOMINANCE PATTERN')
    _div('─')
    if biased_hours:
        print(f'  Hours with systematic >0.1% price difference:')
        for h, avg_h, cnt in sorted(biased_hours):
            src_hi = src_a if avg_h > 0 else src_b
            print(f'    {h:02d}:00 UTC  {src_hi} averages {abs(avg_h)*100:.3f}% higher '
                  f'({cnt} candles)')
    else:
        print('  No hourly block shows a systematic >0.1% price difference.')

    # ── source-switch discontinuities ─────────────────────────────────────────
    only_a = set(data_a) - set(data_b)
    only_b = set(data_b) - set(data_a)

    print()
    _div('─')
    print('  SOURCE-SWITCH DISCONTINUITY CHECK')
    _div('─')
    print(f'  Candles exclusive to {src_a:<10}: {len(only_a):,}')
    print(f'  Candles exclusive to {src_b:<10}: {len(only_b):,}')

    discontinuities = []
    # For exclusive candles check if the next candle is from the other source
    for ts in sorted(only_a)[:200]:
        next_ts = ts + _CANDLE_MS
        if next_ts in data_b and next_ts not in data_a:
            ca_excl   = data_a[ts][3]           # close of last exclusive A candle
            ob_next   = data_b[next_ts][0]       # open of first B candle
            mid       = (ca_excl + ob_next) / 2 or 1e-9
            jump      = abs(ca_excl - ob_next) / mid
            if jump > _BIAS_THRESH:
                discontinuities.append((ts, src_a, src_b, jump))
    for ts in sorted(only_b)[:200]:
        next_ts = ts + _CANDLE_MS
        if next_ts in data_a and next_ts not in data_b:
            cb_excl  = data_b[ts][3]
            oa_next  = data_a[next_ts][0]
            mid      = (cb_excl + oa_next) / 2 or 1e-9
            jump     = abs(cb_excl - oa_next) / mid
            if jump > _BIAS_THRESH:
                discontinuities.append((ts, src_b, src_a, jump))

    if discontinuities:
        print(f'  Price discontinuities at source boundaries: {len(discontinuities)}')
        for ts, leaving, entering, jump in discontinuities[:3]:
            print(f'    {_ts_str(ts)}  {leaving}→{entering}  {jump*100:.3f}% price jump')
    else:
        print('  No significant price discontinuities at source switch points.')

    # ── verdict ───────────────────────────────────────────────────────────────
    warnings = []
    if abs(avg_diff) > _BIAS_THRESH:
        warnings.append(
            f'{dominant} prices are systematically {abs(avg_diff)*100:.3f}% higher '
            f'than {dominated} on average'
        )
    if len(biased_hours) >= 4:
        hrs = ', '.join(f'{h:02d}:00' for h, _, _ in sorted(biased_hours)[:5])
        warnings.append(f'systematic hourly bias detected at UTC hours: {hrs}...')
    if len(discontinuities) > 5:
        warnings.append(
            f'{len(discontinuities)} price discontinuities found at source switch points'
        )

    print()
    _div('─')
    if warnings:
        print('  WARNING — potential bias found:')
        for w in warnings:
            print(f'    • {w}')
    else:
        print('  SAFE — no exploitable merge patterns detected')
        print(f'  (avg diff {avg_diff*100:+.4f}%, abs avg {abs_avg_diff*100:.4f}% — '
              f'both below 0.1% threshold)')
    _div()
    print()


# ══════════════════════════════════════════════════════════════════════════════
# PART 3 — API Token Research
# ══════════════════════════════════════════════════════════════════════════════

_APIS = [
    {
        'name':    'CoinAPI',
        'url':     'coinapi.io',
        'rate':    '100 req/day (free)',
        'history': 'BTC from ~2010; most majors full depth',
        'min_int': '1-minute  ✓',
        'quality': 'Good — multi-exchange normalized OHLCV, widely used',
        'vs_ours': '300+ exchanges; strong pre-2017 complement; low free quota',
    },
    {
        'name':    'CryptoCompare',
        'url':     'min-api.cryptocompare.com',
        'rate':    '~100 000 calls/month free (~3 300/day)',
        'history': 'BTC from 2010; most alts from 2017+',
        'min_int': '1-minute  ✓',
        'quality': 'Moderate — aggregated VWAP, some exchange cherry-picking',
        'vs_ours': 'Broad but aggregated; not raw exchange data; best free quota',
    },
    {
        'name':    'Tardis.dev',
        'url':     'tardis.dev',
        'rate':    'No free tier — pay-per-GB (from ~$0.10/GB)',
        'history': '2018+ for most top-40 exchanges; tick data',
        'min_int': 'Tick + 1-minute  ✓',
        'quality': 'Excellent — institutional, raw tick replay, very high fidelity',
        'vs_ours': 'Superior quality 2018+; no pre-2018; expensive for bulk history',
    },
    {
        'name':    'Kaiko',
        'url':     'kaiko.com',
        'rate':    'No free tier — demo/sandbox only',
        'history': 'BTC from 2013 on Bitstamp/Bitfinex; tick data',
        'min_int': 'Tick + 1-minute  ✓',
        'quality': 'Excellent — institutional, curated, used by Bloomberg/Reuters',
        'vs_ours': 'Best pre-2017 alternative; 100+ exchanges; cost-prohibitive personal use',
    },
    {
        'name':    'Messari',
        'url':     'messari.io',
        'rate':    '20 req/min free (basic endpoints only)',
        'history': 'Daily OHLCV only on free; on-chain / fundamentals focus',
        'min_int': '1-minute  ✗  (daily granularity on free tier)',
        'quality': 'Good for fundamentals; not suited to 1-min trading data',
        'vs_ours': 'Wrong tool for OHLCV; useful for on-chain metrics and asset profiles',
    },
    {
        'name':    'Amberdata',
        'url':     'amberdata.io',
        'rate':    '100 req/day free',
        'history': 'CeFi majors from 2018+; DeFi from protocol launch',
        'min_int': '1-minute  ✓',
        'quality': 'Good — CeFi + DeFi unified, institutional quality',
        'vs_ours': 'Adds DeFi/on-chain layer; limited pre-2018; small free quota',
    },
    {
        'name':    'Polygon.io (crypto)',
        'url':     'polygon.io',
        'rate':    'Unlimited basic (15-min delayed on free)',
        'history': 'BTC/ETH from ~2015 (Coinbase, Kraken, etc.)',
        'min_int': '1-minute  ✓  (second-bar on paid)',
        'quality': 'Good — clean REST + WebSocket API, growing coverage',
        'vs_ours': 'Best free quota; US-regulated exchanges only; fills our Coinbase gap',
    },
]


def print_api_research() -> None:
    _div()
    print('  FREE TOKEN-BASED CRYPTO API COMPARISON TABLE')
    _div()

    fields = [
        ('Free tier rate limits',   'rate'),
        ('Historical data depth',   'history'),
        ('Min interval (1m avail)', 'min_int'),
        ('Data quality reputation', 'quality'),
        ('Coverage vs our sources', 'vs_ours'),
    ]

    for api in _APIS:
        bar = '─' * max(0, 61 - len(api['name']))
        print(f'\n  ┌─ {api["name"].upper()} ({api["url"]}) {bar}┐')
        for label, key in fields:
            val = api[key]
            # word-wrap at 53 chars
            if len(val) <= 53:
                print(f'  │  {label:<26} : {val}')
            else:
                words, lines, cur = val.split(), [], ''
                for w in words:
                    if len(cur) + len(w) + 1 <= 53:
                        cur = (cur + ' ' + w).lstrip()
                    else:
                        if cur:
                            lines.append(cur)
                        cur = w
                if cur:
                    lines.append(cur)
                print(f'  │  {label:<26} : {lines[0]}')
                for extra in lines[1:]:
                    print(f'  │  {" " * 28}  {extra}')
        print(f'  └' + '─' * 68 + '┘')

    print()
    _div('─')
    print('  RECOMMENDATIONS FOR THIS PROJECT')
    _div('─')
    recs = [
        ('Best free complement',     'Polygon.io — unlimited calls, fills Coinbase/Kraken gap'),
        ('Best 1-min data quality',  'Tardis.dev — if budget allows; unmatched tick fidelity'),
        ('Best legacy pre-2017',     'Kaiko — raw Bitfinex/Bitstamp history; paid only'),
        ('Best broad free coverage', 'CryptoCompare — 100k calls/month, many exchanges'),
        ('Avoid for OHLCV work',     'Messari — fundamentals tool, not a price feed'),
    ]
    for label, rec in recs:
        print(f'  {label:<32} : {rec}')
    _div()
    print()


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Data quality validation for crypto-data-downloader',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument('--file',
                        metavar='ZIP',
                        help='Path to a zip file to quality-check')
    parser.add_argument('--merge-check',
                        action='store_true',
                        help='Run merge bias analysis (requires --symbol and --year)')
    parser.add_argument('--symbol',
                        metavar='SYM',
                        help='Trading pair, e.g. btcusdt  (used with --merge-check)')
    parser.add_argument('--year',
                        type=int,
                        metavar='YYYY',
                        help='Year to analyse  (used with --merge-check)')
    parser.add_argument('--api-research',
                        action='store_true',
                        help='Print the free API comparison table')

    args = parser.parse_args()

    if not any([args.file, args.merge_check, args.api_research]):
        parser.print_help()
        sys.exit(0)

    if args.file:
        check_candles(args.file)

    if args.merge_check:
        if not args.symbol or not args.year:
            parser.error('--merge-check requires --symbol and --year')
        check_merge_bias(args.symbol, args.year)

    if args.api_research:
        print_api_research()


if __name__ == '__main__':
    main()
