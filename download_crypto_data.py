#!/usr/bin/env python3
"""
Download historical 1-minute OHLCV crypto data.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SOURCES  (tried in priority order, merged with deduplication)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  >= 2019 : Binance Vision → OKX → Bybit
  2017–18 : Binance Vision → OKX
  2013–16 : Bitfinex → Kraken → Bitstamp
  2011–12 : Bitstamp

Once coverage reaches ≥99% for a month, remaining sources are skipped.
Gaps are reported after each month.

  * Bybit spot: geo-blocked from some regions (403), skipped gracefully
  * Coinbase: now requires auth — not included
  * Poloniex new API: no 1-min historical depth — not included

  python download_crypto_data.py --symbol btcusdt --year 2023
  python download_crypto_data.py --symbol btcusdt --year 2014

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DATE RANGE  (mutually exclusive with --year)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  --start "2013-01-02 09:45"  --end "2013-12-20 16:00"
  --start 2013-01-02          --end 2013-12-20   (00:00 UTC default)

Only the months needed are fetched.  Candles are trimmed to the exact
start/end timestamps (inclusive).  Max range: 10 years.
Output: AllData/{symbol}/{symbol}_{start-date}_{end-date}.zip

  python download_crypto_data.py --symbol btcusdt \\
      --start "2023-03-15 09:45" --end "2023-06-20 16:00"

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Accept symbol as: btcusdt / BTC/USDT / btc-usdt / BTCUSDT
Output: AllData/{symbol}/{symbol}_{year}.zip  or  ..._{start}_{end}.zip
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


def normalize(raw):
    return raw.lower().replace('/', '').replace('-', '').replace('_', '').replace(' ', '')


def _month_bounds_ms(year, month):
    s = int(datetime(year, month, 1, tzinfo=timezone.utc).timestamp()) * 1000
    nm, ny = month % 12 + 1, year + (1 if month == 12 else 0)
    e = int(datetime(ny, nm, 1, tzinfo=timezone.utc).timestamp()) * 1000
    return s, e


def _expected_candles(year, month):
    return calendar.monthrange(year, month)[1] * 24 * 60


def _parse_datetime(s):
    for fmt in ('%Y-%m-%d %H:%M', '%Y-%m-%d'):
        try:
            return datetime.strptime(s.strip(), fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    raise ValueError(
        f'Cannot parse date: {s!r}  (use YYYY-MM-DD or "YYYY-MM-DD HH:MM")'
    )


def _months_in_range(start_dt, end_dt):
    result = []
    y, m = start_dt.year, start_dt.month
    while (y, m) <= (end_dt.year, end_dt.month):
        result.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return result


_BFX_PAIRS = {
    'btcusdt': 'tBTCUSD',  'ethusdt': 'tETHUSD',  'ltcusdt': 'tLTCUSD',
    'xrpusdt': 'tXRPUSD',  'xmrusdt': 'tXMRUSD',  'dashusdt': 'tDSHUSD',
    'etcusdt': 'tETCUSD',  'zecusdt': 'tZECUSD',  'bchusdt': 'tBCHUSD',
    'eosusd':  'tEOSUSD',  'eosusdt': 'tEOSUSD',  'neousdt': 'tNEOUSD',
    'omgusdt': 'tOMGUSD',  'iotausdt': 'tIOTUSD', 'trxusdt': 'tTRXUSD',
    'linkusdt': 'tLINKUSD','algousdt': 'tALGUSD', 'repusdt': 'tREPUSD',
}

_KRAKEN_PAIRS = {
    'btcusdt':  'XBTUSD',  'ethusdt':  'ETHUSD',  'ltcusdt':  'LTCUSD',
    'xrpusdt':  'XRPUSD',  'xmrusdt':  'XMRUSD',  'dashusdt': 'DASHUSD',
    'etcusdt':  'ETCUSD',  'zecusdt':  'ZECUSD',  'bchusdt':  'BCHUSD',
    'adausdt':  'ADAUSD',  'solusdt':  'SOLUSD',  'dogeusdt': 'XDGUSD',
    'xlmusdt':  'XLMUSD',  'atomusdt': 'ATOMUSD', 'dotusdt':  'DOTUSD',
    'linkusdt': 'LINKUSD', 'uniusdt':  'UNIUSD',  'maticusdt':'MATICUSD',
    'avaxusdt': 'AVAXUSD', 'nearusdt': 'NEARUSD', 'algousdt': 'ALGOUSD',
    'trxusdt':  'TRXUSD',  'filusdt':  'FILUSD',  'aaveusd':  'AAVEUSD',
}

_BSTAMP_PAIRS = {
    'btcusdt': 'btcusd', 'btcusd': 'btcusd', 'btceur': 'btceur',
    'ethusdt': 'ethusd', 'ltcusdt': 'ltcusd', 'xrpusdt': 'xrpusd',
    'bchusdt': 'bchusd', 'linkusdt': 'linkusd',
}


def _okx_sym(symbol):
    s = symbol.lower()
    for q in ('usdt', 'usd', 'btc', 'eth'):
        if s.endswith(q):
            return f'{s[:-len(q)].upper()}-{q.upper()}'
    return None


def _bybit_sym(symbol):
    return symbol.upper()


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


def _okx_month(symbol, year, month):
    inst_id = _okx_sym(symbol)
    if not inst_id:
        raise ValueError(f'Cannot auto-map {symbol} to OKX format')

    start_ms, end_ms = _month_bounds_ms(year, month)
    expected = _expected_candles(year, month)
    ym  = f'{year}-{month:02d}'
    url = 'https://www.okx.com/api/v5/market/history-candles'

    rows, seen = [], set()
    cursor = end_ms

    while cursor > start_ms:
        data = _get_json(url, params={'instId': inst_id, 'bar': '1m',
                                      'after': cursor, 'limit': 100})
        if data.get('code') != '0':
            raise RuntimeError(f"OKX: {data.get('msg')}")
        candles = data.get('data', [])
        if not candles:
            break
        for c in candles:
            ts = int(c[0])
            if start_ms <= ts < end_ms and ts not in seen:
                seen.add(ts)
                rows.append((ts, str(c[1]), str(c[2]), str(c[3]), str(c[4]), str(c[5])))
        _pct_bar(len(rows), expected, desc=f'{ym}  {len(rows):,} candles')
        oldest = int(candles[-1][0])
        if oldest <= start_ms:
            break
        cursor = oldest
        time.sleep(0.12)

    sys.stdout.write('\n')
    sys.stdout.flush()
    return rows


def _bybit_month(symbol, year, month):
    sym = _bybit_sym(symbol)
    start_ms, end_ms = _month_bounds_ms(year, month)
    expected = _expected_candles(year, month)
    ym  = f'{year}-{month:02d}'
    url = 'https://api.bybit.com/v5/market/kline'

    rows, seen = [], set()
    step = 1000 * 60_000
    cur  = start_ms

    while cur < end_ms:
        win_end = min(cur + step, end_ms)
        data = _get_json(url, params={'category': 'spot', 'symbol': sym,
                                      'interval': 1, 'start': cur,
                                      'end': win_end, 'limit': 1000})
        if data.get('retCode') != 0:
            raise RuntimeError(f"Bybit: {data.get('retMsg')}")
        for c in data.get('result', {}).get('list', []):
            ts = int(c[0])
            if start_ms <= ts < end_ms and ts not in seen:
                seen.add(ts)
                rows.append((ts, str(c[1]), str(c[2]), str(c[3]), str(c[4]), str(c[5])))
        _pct_bar(len(rows), expected, desc=f'{ym}  {len(rows):,} candles')
        cur = win_end
        if cur < end_ms:
            time.sleep(0.1)

    sys.stdout.write('\n')
    sys.stdout.flush()
    return rows


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
        if not data or not isinstance(data, list) or (data and data[0] == 'error'):
            break
        if not isinstance(data[0], (list, tuple)):
            break
        added = 0
        for c in data:
            ts = int(c[0])
            if ts >= end_ms:
                break
            rows.append((ts, str(c[1]), str(c[3]), str(c[4]), str(c[2]), str(c[5])))
            added += 1
        _pct_bar(len(rows), expected, desc=f'{ym}  {len(rows):,} candles')
        last_ts = int(data[-1][0])
        if last_ts <= cur or added < len(data):
            break
        cur = last_ts + 1
        time.sleep(0.5)

    sys.stdout.write('\n')
    sys.stdout.flush()
    return rows


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
            if start_sec <= ts < end_sec:
                rows.append((ts * 1000, str(c[1]), str(c[2]), str(c[3]),
                             str(c[4]), str(c[6])))
        _pct_bar(len(rows), expected, desc=f'{ym}  {len(rows):,} candles')
        if not candles or last_ret <= cur_sec or last_ret >= end_sec:
            break
        cur_sec = last_ret
        time.sleep(0.5)

    sys.stdout.write('\n')
    sys.stdout.flush()
    return rows


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
            rows.append((ts * 1000, c['open'], c['high'], c['low'],
                         c['close'], c['volume']))
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


SOURCES = {
    'binance':  {'fn': _binance_month,  'pairs': None,          'since': 2017, 'auto': True},
    'okx':      {'fn': _okx_month,      'pairs': None,          'since': 2019, 'auto': True},
    'bybit':    {'fn': _bybit_month,    'pairs': None,          'since': 2021, 'auto': True},
    'bitfinex': {'fn': _bitfinex_month, 'pairs': _BFX_PAIRS,   'since': 2013},
    'kraken':   {'fn': _kraken_month,   'pairs': _KRAKEN_PAIRS, 'since': 2013},
    'bitstamp': {'fn': _bitstamp_month, 'pairs': _BSTAMP_PAIRS, 'since': 2011},
}

_MODERN_ORDER  = ('binance', 'okx', 'bybit')
_LEGACY_ORDER  = ('bitfinex', 'kraken', 'bitstamp')
_FULL_THRESHOLD = 0.99


def _supports(name, symbol):
    meta = SOURCES[name]
    if meta.get('auto'):
        return True
    return symbol.lower() in (meta.get('pairs') or {})


def _month_sources(symbol, year, force=None):
    if force:
        return [force]
    order = _MODERN_ORDER if year >= 2017 else _LEGACY_ORDER
    return [
        name for name in order
        if year >= SOURCES[name]['since'] and _supports(name, symbol)
    ]


def _check_gaps(rows, year, month):
    if len(rows) < 2:
        return
    ts_list = sorted(r[0] for r in rows)
    gaps, missing = 0, 0
    for a, b in zip(ts_list, ts_list[1:]):
        diff = (b - a) // 60_000
        if diff > 1:
            gaps    += 1
            missing += diff - 1
    if gaps:
        print(f'  Warning: {missing:,} missing candle(s) across {gaps} gap(s) '
              f'in {year}-{month:02d}')


def download_year(symbol, year_months, start_ms=None, end_ms=None, force_source=None):
    print(f'Mode    : multi-source merge')
    print(f'Symbol  : {symbol.upper()}')
    print()

    all_rows = []
    total    = len(year_months)

    for i, (year, month) in enumerate(year_months, 1):
        sources = _month_sources(symbol, year, force=force_source)
        if not sources:
            print(f'[{i:02d}/{total:02d}] {year}-{month:02d}  — no sources available, skipping')
            print()
            continue

        ym       = f'{year}-{month:02d}'
        expected = _expected_candles(year, month)
        print(f'[{i:02d}/{total:02d}] {ym}  sources={"+".join(sources)}  '
              f'(expect ~{expected:,} candles)')

        month_rows, seen_ts = [], set()

        for src in sources:
            try:
                rows = SOURCES[src]['fn'](symbol, year, month)
            except Exception as e:
                print(f'  {src}: skipped — {e}')
                continue

            new_rows = []
            for r in rows:
                if r[0] not in seen_ts:
                    seen_ts.add(r[0])
                    new_rows.append(r)
            month_rows.extend(new_rows)
            added = len(new_rows)

            coverage = len(month_rows) / expected if expected else 1
            print(f'  {src}: {len(rows):,} candles  '
                  f'(+{added:,} new  {coverage*100:.1f}% coverage)')

            if coverage >= _FULL_THRESHOLD and src != sources[-1]:
                rest = sources[sources.index(src) + 1:]
                print(f'  Coverage ≥{_FULL_THRESHOLD*100:.0f}% — skipping {", ".join(rest)}')
                break

        month_rows.sort(key=lambda r: r[0])
        _check_gaps(month_rows, year, month)

        coverage_final = len(month_rows) / expected if expected else 1
        if coverage_final < 0.80 and month_rows:
            print(f'  Warning: low coverage — only {coverage_final*100:.1f}% of expected candles')

        all_rows.extend(month_rows)
        print()

    if start_ms is not None:
        all_rows = [r for r in all_rows if r[0] >= start_ms]
    if end_ms is not None:
        all_rows = [r for r in all_rows if r[0] <= end_ms]

    return all_rows


def cmd_list_pairs():
    print('Supported exchanges and pairs')
    print('=' * 60)

    entries = [
        ('BINANCE VISION', '2017–present',
         'All Binance spot pairs via monthly data dumps\n'
         '    e.g. btcusdt, ethusdt, solusdt, bnbusdt, dogeusdt, + 400 more'),
        ('OKX', '2019–present',
         'Auto-maps any *usdt/*usd pair\n'
         '    e.g. btcusdt, ethusdt, solusdt, dogeusdt, avaxusdt, ...'),
        ('BYBIT SPOT', '2021–present',
         'Auto-maps any *usdt pair  [geo-blocked from some regions]\n'
         '    e.g. btcusdt, ethusdt, solusdt, bnbusdt, ...'),
        ('BITFINEX', '2013–present',
         ', '.join(sorted(_BFX_PAIRS.keys()))),
        ('KRAKEN', '2013–present',
         ', '.join(sorted(_KRAKEN_PAIRS.keys()))),
        ('BITSTAMP', '2011–present',
         ', '.join(sorted(set(_BSTAMP_PAIRS.keys()) - {'btcusd', 'btceur'}))),
    ]

    for name, span, detail in entries:
        print(f'\n  {name}  ({span})')
        for line in detail.splitlines():
            print(f'    {line}')

    print()
    print('Notes:')
    print('  - Coinbase Exchange API now requires authentication — not supported')
    print('  - Poloniex new API has no 1-minute historical depth — not supported')
    print('  - DOGE/USDT did not exist on major exchanges before 2020')
    print('  - ETH/USD pairs generally start late 2015 / early 2016')
    print()
    print('Usage examples:')
    print('  python download_crypto_data.py --symbol btcusdt --year 2023')
    print('  python download_crypto_data.py --symbol BTC/USDT --year 2014')
    print()
    print('  # Date range')
    print('  python download_crypto_data.py --symbol btcusdt \\')
    print('      --start "2023-03-15 09:45" --end "2023-06-20 16:00"')
    print('  python download_crypto_data.py --symbol btcusdt \\')
    print('      --start "2014-01-02" --end "2014-04-30"')
    print()
    print('  # Force a specific exchange')
    print('  python download_crypto_data.py --symbol ethusdt --year 2016 --source bitfinex')


def save(symbol, label, rows):
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
    zip_path = os.path.join(out_dir, f'{symbol.lower()}_{label}.zip')
    csv_name = f'{symbol.lower()}_{label}.csv'

    buf = io.StringIO()
    w   = csv.writer(buf)
    w.writerow(['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    w.writerows(deduped)

    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(csv_name, buf.getvalue())

    print(f'Saved  : {zip_path}')
    print(f'Candles: {len(deduped):,}')
    if deduped:
        def _fmt(ms):
            return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        print(f'First  : {_fmt(deduped[0][0])}')
        print(f'Last   : {_fmt(deduped[-1][0])}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Download 1-minute crypto OHLCV data (multi-source merge)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--symbol',     help='Trading pair, e.g. btcusdt or BTC/USDT')
    parser.add_argument('--year',       type=int, help='Full calendar year, e.g. 2023')
    parser.add_argument('--start',      metavar='DATE',
                        help='Range start: "2013-01-02" or "2013-01-02 09:45"')
    parser.add_argument('--end',        metavar='DATE',
                        help='Range end:   "2013-12-20" or "2013-12-20 16:00"')
    parser.add_argument('--source',     choices=list(SOURCES), default=None,
                        help='Force a specific exchange (overrides auto-selection)')
    parser.add_argument('--list-pairs', action='store_true',
                        help='Show all supported pairs and exit')
    args = parser.parse_args()

    if args.list_pairs:
        cmd_list_pairs()
        sys.exit(0)

    if not args.symbol:
        parser.error('--symbol is required (or use --list-pairs)')

    if args.year and (args.start or args.end):
        parser.error('--year and --start/--end are mutually exclusive')
    if bool(args.start) != bool(args.end):
        parser.error('--start and --end must be used together')
    if not args.year and not args.start:
        parser.error('one of --year or --start/--end is required')

    sym = normalize(args.symbol)

    if args.year:
        year_months = [(args.year, m) for m in range(1, 13)]
        start_ms = end_ms = None
        label    = str(args.year)
    else:
        try:
            start_dt = _parse_datetime(args.start)
            end_dt   = _parse_datetime(args.end)
        except ValueError as e:
            parser.error(str(e))

        if start_dt >= end_dt:
            parser.error('--start must be before --end')
        if (end_dt - start_dt).days > 10 * 365:
            parser.error('Date range exceeds the 10-year maximum')

        year_months = _months_in_range(start_dt, end_dt)
        start_ms    = int(start_dt.timestamp()) * 1000
        end_ms      = int(end_dt.timestamp()) * 1000
        label       = f'{start_dt.strftime("%Y-%m-%d")}_{end_dt.strftime("%Y-%m-%d")}'

    rows = download_year(sym, year_months,
                         start_ms=start_ms, end_ms=end_ms,
                         force_source=args.source)

    save(sym, label, rows)
