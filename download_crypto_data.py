#!/usr/bin/env python3
"""
Download historical 1-minute OHLCV crypto data.
  2017-present : Binance Vision  (data.binance.vision)
  2011-2016    : Bitstamp API    (BTC pairs only)

Output: AllData/{symbol}/{symbol}_{year}.zip
CSV   : timestamp, open, high, low, close, volume
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
    # 1. certifi (most reliable cross-platform)
    try:
        import certifi
        return ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        pass
    # 2. macOS system bundle
    for path in ('/etc/ssl/cert.pem', '/etc/ssl/certs/ca-certificates.crt'):
        if os.path.exists(path):
            ctx = ssl.create_default_context()
            ctx.load_verify_locations(path)
            return ctx
    # 3. Last resort – disable verification
    print("WARNING: SSL certificate verification disabled (pip install certifi to fix)",
          file=sys.stderr)
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


def _get_json(url, params=None):
    return json.loads(_get_bytes(url, params=params))


# ── Progress bars ──────────────────────────────────────────────────────────────

def _mb_bar(done, total, desc='', width=35):
    pct = done / total
    bar = '#' * int(width * pct) + '-' * (width - int(width * pct))
    sys.stdout.write(f'\r  [{bar}] {pct*100:.0f}%  {done/1e6:.1f}/{total/1e6:.1f} MB  {desc}')
    sys.stdout.flush()


def _pct_bar(done, total, desc='', width=35):
    if total == 0:
        return
    pct = min(done / total, 1.0)
    bar = '#' * int(width * pct) + '-' * (width - int(width * pct))
    sys.stdout.write(f'\r  [{bar}] {pct*100:.0f}%  {desc}')
    sys.stdout.flush()


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


# ── Bitstamp ───────────────────────────────────────────────────────────────────

def _bstamp_pair(symbol):
    s = symbol.lower()
    if s.endswith('usdt'):
        return s[:-1]          # btcusdt → btcusd
    return s


def _bitstamp_month(symbol, year, month):
    pair = _bstamp_pair(symbol)
    url  = f'https://www.bitstamp.net/api/v2/ohlc/{pair}/'

    start_ts = int(datetime(year, month, 1, tzinfo=timezone.utc).timestamp())
    next_m   = month % 12 + 1
    next_y   = year + (1 if month == 12 else 0)
    end_ts   = int(datetime(next_y, next_m, 1, tzinfo=timezone.utc).timestamp())

    expected = calendar.monthrange(year, month)[1] * 24 * 60
    ym       = f'{year}-{month:02d}'
    rows     = []
    cur      = start_ts
    limit    = 1000

    while cur < end_ts:
        # Note: omit 'end' — when supplied, Bitstamp returns the *last* N
        # candles before end_ts rather than the *first* N after start.
        data    = _get_json(url, params={'step': 60, 'limit': limit, 'start': cur})
        candles = data.get('data', {}).get('ohlc', [])
        if not candles:
            break
        added = 0
        for c in candles:
            ts = int(c['timestamp'])
            if ts >= end_ts:
                break        # past end of month — stop processing this page
            rows.append((ts * 1000,                           # → ms
                         c['open'], c['high'], c['low'], c['close'], c['volume']))
            added += 1
        _pct_bar(len(rows), expected, desc=f'{ym}  {len(rows):,} candles')
        last = int(candles[-1]['timestamp'])
        if last <= cur or added < len(candles):
            break          # no progress, or last page hit end_ts boundary
        cur = last + 60
        time.sleep(0.15)

    sys.stdout.write('\n')
    sys.stdout.flush()
    return rows


# ── Gap detection ──────────────────────────────────────────────────────────────

def _check_gaps(rows, year, month):
    if len(rows) < 2:
        return
    ts_sorted = sorted(r[0] for r in rows)
    gap_count  = 0
    gap_candles = 0
    for a, b in zip(ts_sorted, ts_sorted[1:]):
        diff_min = (b - a) // 60_000
        if diff_min > 1:
            gap_count  += 1
            gap_candles += diff_min - 1
    if gap_count:
        print(f'  Warning: {gap_candles:,} missing candle(s) across '
              f'{gap_count} gap(s) in {year}-{month:02d}')


# ── Orchestration ──────────────────────────────────────────────────────────────

_BINANCE_SINCE = 2017
_BITSTAMP_SYMS = {'btcusd', 'btcusdt', 'btceur'}


def download_year(symbol, year):
    use_binance  = year >= _BINANCE_SINCE
    use_bitstamp = (not use_binance) and symbol.lower() in _BITSTAMP_SYMS

    if not use_binance and not use_bitstamp:
        sys.exit(
            f'No data source for {symbol.upper()} in {year}.\n'
            f'Bitstamp fallback covers BTC pairs (btcusdt/btcusd/btceur) for 2011-2016.'
        )

    source = 'Binance Vision' if use_binance else 'Bitstamp'
    print(f'Source  : {source}')
    print(f'Symbol  : {symbol.upper()}')
    print(f'Year    : {year}')
    print()

    all_rows = []

    for month in range(1, 13):
        ym = f'{year}-{month:02d}'
        print(f'[{month:02d}/12] {ym}')
        try:
            if use_binance:
                rows = _binance_month(symbol, year, month)
            else:
                rows = _bitstamp_month(symbol, year, month)
            _check_gaps(rows, year, month)
            print(f'  {len(rows):,} candles')
            all_rows.extend(rows)
        except Exception as e:
            print(f'  Skipped: {e}')
        print()

    return all_rows


# ── Output ─────────────────────────────────────────────────────────────────────

def save(symbol, year, rows):
    if not rows:
        print('No data to save.')
        return

    # Sort + deduplicate by timestamp
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
    args = parser.parse_args()

    rows = download_year(args.symbol.lower(), args.year)
    save(args.symbol.lower(), args.year, rows)
