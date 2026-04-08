#!/usr/bin/env python3
"""
Download historical 1-minute OHLCV crypto data.

Source selection:
  year >= 2017 : Binance Vision  (primary)
  year <  2017 : Bitfinex → Kraken → Bitstamp  (merged, deduped)

Output: AllData/{symbol}/{symbol}_{year}.zip
CSV   : timestamp(ms), open, high, low, close, volume
"""

import argparse
import calendar
import csv
import io
import json
import os
import ssl
import sys
import time
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timezone


# ── SSL fix for Mac / Python 3.14 ─────────────────────────────────────────────

def _make_ssl_ctx():
    try:
        import certifi
        return ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        pass
    for path in ('/etc/ssl/cert.pem', '/etc/ssl/certs/ca-certificates.crt'):
        if os.path.exists(path):
            ctx = ssl.create_default_context()
            ctx.load_verify_locations(path)
            return ctx
    print("WARNING: SSL verification disabled (pip install certifi to fix)", file=sys.stderr)
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


_SSL = _make_ssl_ctx()


# ── HTTP helpers ───────────────────────────────────────────────────────────────

def _get_bytes(url, params=None, show_progress=False, desc=''):
    if params:
        url += '?' + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={'User-Agent': 'crypto-downloader/1.0'})
    with urllib.request.urlopen(req, context=_SSL, timeout=60) as resp:
        total = int(resp.headers.get('Content-Length', 0) or 0)
        if show_progress and total:
            chunks, done = [], 0
            while chunk := resp.read(65536):
                chunks.append(chunk)
                done += len(chunk)
                _mb_bar(done, total, desc)
            sys.stdout.write('\n')
            sys.stdout.flush()
            return b''.join(chunks)
        return resp.read()


def _get_json(url, params=None, retries=3):
    for attempt in range(retries):
        try:
            return json.loads(_get_bytes(url, params=params))
        except Exception:
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)


# ── Progress bars ──────────────────────────────────────────────────────────────

def _mb_bar(done, total, desc='', width=35):
    pct = done / total
    bar = '#' * int(width * pct) + '-' * (width - int(width * pct))
    sys.stdout.write(f'\r  [{bar}] {pct*100:.0f}%  {done/1e6:.1f}/{total/1e6:.1f} MB  {desc}')
    sys.stdout.flush()


def _pct_bar(done, total, desc='', width=35):
    if not total:
        return
    pct = min(done / total, 1.0)
    bar = '#' * int(width * pct) + '-' * (width - int(width * pct))
    sys.stdout.write(f'\r  [{bar}] {pct*100:.0f}%  {desc}')
    sys.stdout.flush()


# ── Month boundary helpers ─────────────────────────────────────────────────────

def _month_bounds_ms(year, month):
    """(start_ms, end_ms) for the given year/month, UTC, exclusive end."""
    s = int(datetime(year, month, 1, tzinfo=timezone.utc).timestamp()) * 1000
    nm, ny = month % 12 + 1, year + (1 if month == 12 else 0)
    e = int(datetime(ny, nm, 1, tzinfo=timezone.utc).timestamp()) * 1000
    return s, e


def _expected_candles(year, month):
    return calendar.monthrange(year, month)[1] * 24 * 60


# ── Binance Vision ─────────────────────────────────────────────────────────────

_BV_BASE = 'https://data.binance.vision/data/spot/monthly/klines'


def _binance_month(symbol, year, month):
    sym = symbol.upper()
    ym  = f'{year}-{month:02d}'
    url = f'{_BV_BASE}/{sym}/1m/{sym}-1m-{ym}.zip'
    raw = _get_bytes(url, show_progress=True, desc=ym)
    rows = []
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        with zf.open(zf.namelist()[0]) as f:
            for row in csv.reader(io.TextIOWrapper(f, encoding='utf-8')):
                if len(row) >= 6:
                    try:
                        rows.append((int(row[0]), row[1], row[2], row[3], row[4], row[5]))
                    except (ValueError, IndexError):
                        pass
    return rows


# ── Bitfinex ───────────────────────────────────────────────────────────────────
# API response order: [ts_ms, open, close, high, low, volume]
# Output order:       (ts_ms, open, high,  low,  close, volume)

_BFX_PAIRS = {
    'btcusdt': 'tBTCUSD',
    'ethusdt': 'tETHUSD',
    'ltcusdt': 'tLTCUSD',
    'xrpusdt': 'tXRPUSD',
}


def _bitfinex_month(symbol, year, month):
    pair = _BFX_PAIRS.get(symbol.lower())
    if not pair:
        raise ValueError(f'No Bitfinex mapping for {symbol}')

    start_ms, end_ms = _month_bounds_ms(year, month)
    expected = _expected_candles(year, month)
    ym  = f'{year}-{month:02d}'
    url = f'https://api-pub.bitfinex.com/v2/candles/trade:1m:{pair}/hist'

    rows, cur = [], start_ms
    while cur < end_ms:
        data = _get_json(url, params={'limit': 10000, 'start': cur,
                                      'end': end_ms - 1, 'sort': 1})
        # Error response looks like ["error", code, "msg"]
        if not data or not isinstance(data, list) or (data and data[0] == 'error'):
            break
        if not isinstance(data[0], list):
            break  # unexpected format

        added = 0
        for c in data:
            ts = int(c[0])
            if ts >= end_ms:
                break
            # Remap: api=[ts,open,close,high,low,vol] → out=(ts,open,high,low,close,vol)
            rows.append((ts, str(c[1]), str(c[3]), str(c[4]), str(c[2]), str(c[5])))
            added += 1

        _pct_bar(len(rows), expected, desc=f'{ym}  {len(rows):,} candles')
        last_ts = int(data[-1][0])
        if last_ts <= cur or added < len(data):
            break
        cur = last_ts + 1           # advance 1 ms past last candle
        time.sleep(0.5)

    sys.stdout.write('\n')
    sys.stdout.flush()
    return rows


# ── Kraken ─────────────────────────────────────────────────────────────────────
# API response order: [time_sec, open, high, low, close, vwap, volume, count]
# Output order:       (ts_ms,   open, high, low, close,        volume)

_KRAKEN_PAIRS = {
    'btcusdt': 'XBTUSD',
    'ethusdt': 'ETHUSD',
    'ltcusdt': 'LTCUSD',
    'xrpusdt': 'XRPUSD',
}


def _kraken_month(symbol, year, month):
    pair = _KRAKEN_PAIRS.get(symbol.lower())
    if not pair:
        raise ValueError(f'No Kraken mapping for {symbol}')

    start_ms, end_ms = _month_bounds_ms(year, month)
    start_sec, end_sec = start_ms // 1000, end_ms // 1000
    expected = _expected_candles(year, month)
    ym  = f'{year}-{month:02d}'
    url = 'https://api.kraken.com/0/public/OHLC'

    rows, cur_sec = [], start_sec
    while True:
        data = _get_json(url, params={'pair': pair, 'interval': 1, 'since': cur_sec})
        if data.get('error'):
            raise RuntimeError(f"Kraken: {data['error']}")

        result     = data.get('result', {})
        candle_key = next((k for k in result if k != 'last'), None)
        if not candle_key:
            break

        candles  = result[candle_key]
        last_ret = int(result.get('last', 0))

        for c in candles:
            ts = int(c[0])
            if ts < start_sec or ts >= end_sec:
                continue
            # [time, open, high, low, close, vwap, volume, count]
            rows.append((ts * 1000, str(c[1]), str(c[2]), str(c[3]), str(c[4]), str(c[6])))

        _pct_bar(len(rows), expected, desc=f'{ym}  {len(rows):,} candles')

        if not candles or last_ret <= cur_sec or last_ret >= end_sec:
            break
        cur_sec = last_ret
        time.sleep(1.0)

    sys.stdout.write('\n')
    sys.stdout.flush()
    return rows


# ── Bitstamp ───────────────────────────────────────────────────────────────────
# Note: omit 'end' param — when supplied, Bitstamp returns the *last* N candles
# before end_ts rather than the *first* N after start.

_BSTAMP_PAIRS = {
    'btcusdt': 'btcusd',
    'btcusd':  'btcusd',
    'btceur':  'btceur',
}


def _bitstamp_month(symbol, year, month):
    pair = _BSTAMP_PAIRS.get(symbol.lower())
    if not pair:
        raise ValueError(f'No Bitstamp mapping for {symbol}')

    start_ms, end_ms = _month_bounds_ms(year, month)
    start_sec, end_sec = start_ms // 1000, end_ms // 1000
    expected = _expected_candles(year, month)
    ym  = f'{year}-{month:02d}'
    url = f'https://www.bitstamp.net/api/v2/ohlc/{pair}/'

    rows, cur = [], start_sec
    while cur < end_sec:
        data    = _get_json(url, params={'step': 60, 'limit': 1000, 'start': cur})
        candles = data.get('data', {}).get('ohlc', [])
        if not candles:
            break
        added = 0
        for c in candles:
            ts = int(c['timestamp'])
            if ts >= end_sec:
                break
            rows.append((ts * 1000, c['open'], c['high'], c['low'], c['close'], c['volume']))
            added += 1
        _pct_bar(len(rows), expected, desc=f'{ym}  {len(rows):,} candles')
        last = int(candles[-1]['timestamp'])
        if last <= cur or added < len(candles):
            break
        cur = last + 60
        time.sleep(0.15)

    sys.stdout.write('\n')
    sys.stdout.flush()
    return rows


# ── Source registry ────────────────────────────────────────────────────────────

SOURCES = {
    'binance':  {'fn': _binance_month,  'pairs': None,
                 'since': 2017},
    'bitfinex': {'fn': _bitfinex_month, 'pairs': set(_BFX_PAIRS),
                 'since': 2013},
    'kraken':   {'fn': _kraken_month,   'pairs': set(_KRAKEN_PAIRS),
                 'since': 2013},
    'bitstamp': {'fn': _bitstamp_month, 'pairs': set(_BSTAMP_PAIRS),
                 'since': 2011},
}

_LEGACY_ORDER = ('bitfinex', 'kraken', 'bitstamp')   # priority for pre-2017


def _plan_sources(symbol, year, force=None):
    """Return ordered list of source names to use for this symbol/year."""
    if force:
        return [force]
    if year >= 2017:
        return ['binance']
    candidates = []
    sym = symbol.lower()
    for name in _LEGACY_ORDER:
        meta = SOURCES[name]
        if year < meta['since']:
            continue
        if meta['pairs'] and sym not in meta['pairs']:
            continue
        candidates.append(name)
    return candidates


# ── Gap detection ──────────────────────────────────────────────────────────────

def _check_gaps(rows, year, month):
    if len(rows) < 2:
        return
    ts_list = sorted(r[0] for r in rows)
    gaps, missing = 0, 0
    for a, b in zip(ts_list, ts_list[1:]):
        diff = (b - a) // 60_000
        if diff > 1:
            gaps   += 1
            missing += diff - 1
    if gaps:
        print(f'  Warning: {missing:,} missing candle(s) across {gaps} gap(s) '
              f'in {year}-{month:02d}')


# ── Orchestration ──────────────────────────────────────────────────────────────

_FULL_THRESHOLD = 0.99   # skip remaining sources once this fraction is reached


def download_year(symbol, year, force_source=None):
    sources = _plan_sources(symbol, year, force=force_source)

    if not sources:
        sys.exit(f'No data source available for {symbol.upper()} in {year}.\n'
                 f'Supported legacy symbols: '
                 + ', '.join(sorted(set(_BFX_PAIRS) | set(_KRAKEN_PAIRS) | set(_BSTAMP_PAIRS))))

    label = ' + '.join(sources)
    print(f'Source  : {label}')
    print(f'Symbol  : {symbol.upper()}')
    print(f'Year    : {year}')
    print()

    all_rows = []

    for month in range(1, 13):
        ym       = f'{year}-{month:02d}'
        expected = _expected_candles(year, month)
        print(f'[{month:02d}/12] {ym}  (expect ~{expected:,} candles)')

        month_rows = []
        seen_ts    = set()

        for src in sources:
            try:
                rows = SOURCES[src]['fn'](symbol, year, month)
            except Exception as e:
                print(f'  {src}: skipped — {e}')
                continue

            # Merge without duplicating timestamps already seen
            added = 0
            for r in rows:
                if r[0] not in seen_ts:
                    seen_ts.add(r[0])
                    month_rows.append(r)
                    added += 1

            coverage = len(month_rows) / expected if expected else 1
            print(f'  {src}: {len(rows):,} candles  '
                  f'(+{added:,} new, {coverage*100:.1f}% coverage)')

            # Short-circuit: skip remaining sources if data is essentially complete
            if coverage >= _FULL_THRESHOLD and src != sources[-1]:
                remaining = sources[sources.index(src) + 1:]
                print(f'  Coverage ≥{_FULL_THRESHOLD*100:.0f}% — skipping {", ".join(remaining)}')
                break

        month_rows.sort(key=lambda r: r[0])
        _check_gaps(month_rows, year, month)
        all_rows.extend(month_rows)
        print()

    return all_rows


# ── Output ─────────────────────────────────────────────────────────────────────

def save(symbol, year, rows):
    if not rows:
        print('No data to save.')
        return

    rows.sort(key=lambda r: r[0])
    seen, deduped = set(), []
    for r in rows:
        if r[0] not in seen:
            seen.add(r[0])
            deduped.append(r)

    out_dir  = os.path.join('AllData', symbol.lower())
    os.makedirs(out_dir, exist_ok=True)
    zip_path = os.path.join(out_dir, f'{symbol.lower()}_{year}.zip')
    csv_name = f'{symbol.lower()}_{year}.csv'

    buf = io.StringIO()
    w   = csv.writer(buf)
    w.writerow(['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    w.writerows(deduped)

    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(csv_name, buf.getvalue())

    print(f'Saved  : {zip_path}')
    print(f'Candles: {len(deduped):,}')


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download 1-minute crypto OHLCV data')
    parser.add_argument('--symbol', required=True, help='Trading pair, e.g. btcusdt')
    parser.add_argument('--year',   required=True, type=int, help='Year, e.g. 2023')
    parser.add_argument('--source', choices=list(SOURCES), default=None,
                        help='Force a specific exchange (bypasses auto-selection)')
    args = parser.parse_args()

    rows = download_year(args.symbol.lower(), args.year, force_source=args.source)
    save(args.symbol.lower(), args.year, rows)
