#!/usr/bin/env python3
"""
visualize_data.py — Terminal + HTML candlestick chart visualizer

Usage:
  python visualize_data.py --file AllData/btcusdt/btcusdt_2023.zip
  python visualize_data.py --file AllData/btcusdt/btcusdt_2014.zip
"""

import argparse
import csv
import io
import json
import os
import sys
import webbrowser
import zipfile
from datetime import datetime, timezone

# ── shared helpers ─────────────────────────────────────────────────────────────

def _ts_str(ts_ms: int, fmt: str = '%Y-%m-%d %H:%M') -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime(fmt)


def _load_candles(zip_path: str) -> list:
    """Return list of (ts_ms, open, high, low, close, volume) as floats."""
    with zipfile.ZipFile(zip_path, 'r') as zf:
        csv_name = next((n for n in zf.namelist() if n.endswith('.csv')), None)
        if csv_name is None:
            raise ValueError(f'No CSV found inside {zip_path}')
        with zf.open(csv_name) as f:
            reader = csv.reader(io.TextIOWrapper(f, 'utf-8'))
            next(reader, None)
            out = []
            for row in reader:
                if not row:
                    continue
                try:
                    out.append((
                        int(row[0]),
                        float(row[1]),
                        float(row[2]),
                        float(row[3]),
                        float(row[4]),
                        float(row[5]) if len(row) > 5 and row[5].strip() else 0.0,
                    ))
                except (ValueError, IndexError):
                    pass
    return out


def _symbol_label(zip_path: str) -> tuple:
    """Return (SYMBOL, label) parsed from the filename."""
    base  = os.path.splitext(os.path.basename(zip_path))[0]
    parts = base.split('_', 1)
    sym   = parts[0].upper()
    lbl   = parts[1] if len(parts) > 1 else ''
    return sym, lbl


def _price_fmt(price: float) -> str:
    if price >= 10_000:
        return f'{price:,.0f}'
    if price >= 100:
        return f'{price:.2f}'
    if price >= 1:
        return f'{price:.4f}'
    return f'{price:.6f}'


# ══════════════════════════════════════════════════════════════════════════════
# PART 1 — ASCII terminal candlestick chart
# ══════════════════════════════════════════════════════════════════════════════

_GRN  = '\033[92m'
_RED  = '\033[91m'
_GRY  = '\033[90m'
_CYN  = '\033[96m'
_WHT  = '\033[97m'
_DIM  = '\033[2m'
_BLD  = '\033[1m'
_RST  = '\033[0m'

_CHART_H    = 30    # price chart rows
_VOL_H      = 7     # volume sub-chart rows
_PRICE_LBL  = 11    # chars for left price label (not counting the separator)
_N_CANDLES  = 50    # how many candles to show


def render_ascii(candles: list, zip_path: str) -> None:
    """Render the last _N_CANDLES candles as an ASCII chart in the terminal."""
    view = candles[-_N_CANDLES:]
    if not view:
        print('  [!] No candles to display.')
        return

    symbol, label = _symbol_label(zip_path)
    n   = len(view)
    ts  = [c[0] for c in view]
    O   = [c[1] for c in view]
    H   = [c[2] for c in view]
    L   = [c[3] for c in view]
    C   = [c[4] for c in view]
    V   = [c[5] for c in view]

    p_min = min(L)
    p_max = max(H)
    p_rng = p_max - p_min or 1.0
    v_max = max(V) or 1.0

    def p2r(price: float) -> int:
        """Map price → row index (0 = top = highest price)."""
        return int(((p_max - price) / p_rng) * (_CHART_H - 1))

    def v2h(vol: float) -> int:
        return max(1, round((vol / v_max) * _VOL_H))

    # ── build price grid (n columns, _CHART_H rows) ────────────────────────
    grid   = [[' '] * n for _ in range(_CHART_H)]
    colors = []

    for i, (o, h, l, c, v) in enumerate(zip(O, H, L, C, V)):
        col  = _GRN if c >= o else _RED
        colors.append(col)

        r_hi   = p2r(h)
        r_lo   = p2r(l)
        r_btop = p2r(max(o, c))
        r_bbot = p2r(min(o, c))

        for r in range(r_hi, r_lo + 1):
            if r_btop <= r <= r_bbot:
                grid[r][i] = '█' if r_btop < r_bbot else '─'   # doji → dash
            else:
                grid[r][i] = '│'

    # ── price labels (5 evenly spaced) ────────────────────────────────────
    label_rows = {
        0:               p_max,
        _CHART_H // 4:   p_max - p_rng * 0.25,
        _CHART_H // 2:   p_max - p_rng * 0.50,
        3*_CHART_H // 4: p_max - p_rng * 0.75,
        _CHART_H - 1:    p_min,
    }

    # ── header ─────────────────────────────────────────────────────────────
    first_dt = _ts_str(ts[0],  '%Y-%m-%d %H:%M UTC')
    last_dt  = _ts_str(ts[-1], '%Y-%m-%d %H:%M UTC')
    print()
    print(f'{_BLD}{_CYN}  {symbol}  {label}{_RST}   {_DIM}{first_dt} → {last_dt}  '
          f'({n} candles){_RST}')
    print()

    # ── price chart rows ───────────────────────────────────────────────────
    sep = f'{_GRY}│{_RST}'
    for r in range(_CHART_H):
        lbl = _price_fmt(label_rows[r]) if r in label_rows else ''
        line = [f'{_DIM}{lbl:>{_PRICE_LBL}}{_RST} {sep}']
        for i in range(n):
            ch = grid[r][i]
            if ch != ' ':
                line.append(f'{colors[i]}{ch}{_RST} ')
            else:
                line.append('  ')
        print(''.join(line))

    # ── x-axis ─────────────────────────────────────────────────────────────
    print(' ' * (_PRICE_LBL + 1) + f' {_GRY}└' + '─' * (n * 2) + _RST)

    # ── timestamps (every ~10 candles) ────────────────────────────────────
    tick_step = max(1, n // 5)
    ticks     = list(range(0, n, tick_step))
    if n - 1 not in ticks:
        ticks.append(n - 1)

    # Build a fixed-width character buffer for the timestamp row
    # Extra 14 chars so a label at the last column never gets clipped
    ts_buf = [' '] * (n * 2 + 14)
    for i in ticks:
        label_str = _ts_str(ts[i], '%m-%d %H:%M')
        start = i * 2
        for j, ch in enumerate(label_str):
            pos = start + j
            if pos < len(ts_buf):
                ts_buf[pos] = ch

    print(' ' * (_PRICE_LBL + 3) + _DIM + ''.join(ts_buf).rstrip() + _RST)
    print()

    # ── volume sub-chart ───────────────────────────────────────────────────
    print(' ' * (_PRICE_LBL + 3) + f'{_DIM}Volume{_RST}')

    vol_grid = [[' '] * n for _ in range(_VOL_H)]
    for i, v in enumerate(V):
        bar_h = v2h(v)
        for r in range(_VOL_H - bar_h, _VOL_H):
            vol_grid[r][i] = '▪'

    for r in range(_VOL_H):
        line = [' ' * (_PRICE_LBL + 3)]
        for i in range(n):
            ch = vol_grid[r][i]
            if ch != ' ':
                line.append(f'{colors[i]}{ch}{_RST} ')
            else:
                line.append('  ')
        print(''.join(line))

    # ── summary stats below chart ──────────────────────────────────────────
    last_c = C[-1]
    prev_c = C[-2] if len(C) > 1 else C[-1]
    chg    = (last_c - prev_c) / prev_c * 100 if prev_c else 0
    chg_col = _GRN if chg >= 0 else _RED

    print()
    print(f'  {_DIM}Last{_RST}  {_BLD}{_price_fmt(last_c)}{_RST}  '
          f'{chg_col}{chg:+.2f}%{_RST}   '
          f'{_DIM}H{_RST} {_price_fmt(max(H))}  '
          f'{_DIM}L{_RST} {_price_fmt(min(L))}  '
          f'{_DIM}Vol{_RST} {sum(V):,.2f}')
    print()


# ══════════════════════════════════════════════════════════════════════════════
# PART 2 — Interactive HTML chart
# ══════════════════════════════════════════════════════════════════════════════

_HTML_OUT = 'chart_output.html'


def generate_html(candles: list, zip_path: str) -> str:
    """Build a standalone HTML file with a full interactive TradingView chart."""
    symbol, label = _symbol_label(zip_path)
    n_candles  = len(candles)
    first_date = _ts_str(candles[0][0],  '%Y-%m-%d') if candles else ''
    last_date  = _ts_str(candles[-1][0], '%Y-%m-%d') if candles else ''

    # Serialise candle + volume data as compact JSON
    # lightweight-charts uses Unix seconds (not ms) for `time`
    c_rows = []
    v_rows = []
    for ts, o, h, l, c, vol in candles:
        t = ts // 1000
        c_rows.append(f'{{"time":{t},"open":{o},"high":{h},"low":{l},"close":{c}}}')
        col = '#00ff88' if c >= o else '#ff3d5a'
        v_rows.append(f'{{"time":{t},"value":{vol},"color":"{col}"}}')

    candle_json = '[' + ','.join(c_rows) + ']'
    vol_json    = '[' + ','.join(v_rows) + ']'

    # Some stats for the info panel
    closes     = [c[4] for c in candles]
    highs      = [c[2] for c in candles]
    lows       = [c[3] for c in candles]
    volumes    = [c[5] for c in candles]
    all_time_h = max(highs)
    all_time_l = min(lows)
    avg_vol    = sum(volumes) / len(volumes) if volumes else 0

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{symbol} {label} — Candlestick Chart</title>
  <script src="https://unpkg.com/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js"></script>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

    body {{
      background: #0a0e14;
      color: #c9d1d9;
      font-family: 'SF Mono', 'Fira Code', 'Cascadia Code', 'Consolas', monospace;
      font-size: 13px;
      display: flex;
      flex-direction: column;
      height: 100vh;
      overflow: hidden;
    }}

    /* ── header bar ─────────────────────────────────────────── */
    #header {{
      display: flex;
      align-items: center;
      gap: 24px;
      padding: 10px 20px;
      border-bottom: 1px solid #1a1e28;
      flex-shrink: 0;
      background: #0d1117;
    }}

    #symbol {{
      font-size: 20px;
      font-weight: 700;
      color: #00ff88;
      letter-spacing: 1px;
    }}

    #label-tag {{
      background: #1a2030;
      color: #8b949e;
      padding: 2px 10px;
      border-radius: 4px;
      font-size: 12px;
      letter-spacing: 0.5px;
    }}

    .stat {{
      display: flex;
      flex-direction: column;
      gap: 1px;
    }}
    .stat-key {{
      font-size: 10px;
      color: #484f58;
      text-transform: uppercase;
      letter-spacing: 0.8px;
    }}
    .stat-val {{
      font-size: 13px;
      color: #c9d1d9;
    }}

    #spacer {{ flex: 1; }}

    /* ── hover tooltip ──────────────────────────────────────── */
    #tooltip {{
      position: absolute;
      top: 56px;
      left: 20px;
      background: rgba(13,17,23,0.92);
      border: 1px solid #1a1e28;
      border-radius: 6px;
      padding: 8px 14px;
      pointer-events: none;
      display: none;
      z-index: 100;
      line-height: 1.8;
      min-width: 220px;
    }}
    #tooltip .tt-row {{ display: flex; justify-content: space-between; gap: 16px; }}
    #tooltip .tt-key {{ color: #484f58; font-size: 11px; text-transform: uppercase; letter-spacing: 0.6px; }}
    #tooltip .tt-val {{ color: #c9d1d9; font-size: 12px; }}
    #tooltip .tt-date {{ color: #8b949e; font-size: 11px; margin-bottom: 4px; }}

    /* ── chart container ────────────────────────────────────── */
    #chart-wrap {{
      flex: 1;
      min-height: 0;
      position: relative;
    }}

    #chart {{
      width: 100%;
      height: 100%;
    }}

    /* ── footer ─────────────────────────────────────────────── */
    #footer {{
      padding: 5px 20px;
      border-top: 1px solid #1a1e28;
      color: #484f58;
      font-size: 11px;
      flex-shrink: 0;
      display: flex;
      gap: 20px;
      background: #0d1117;
    }}
  </style>
</head>
<body>

<div id="header">
  <span id="symbol">{symbol}</span>
  <span id="label-tag">{label}</span>

  <div class="stat">
    <span class="stat-key">Candles</span>
    <span class="stat-val">{n_candles:,}</span>
  </div>
  <div class="stat">
    <span class="stat-key">Range</span>
    <span class="stat-val">{first_date} → {last_date}</span>
  </div>
  <div class="stat">
    <span class="stat-key">All-time High</span>
    <span class="stat-val" style="color:#00ff88">{_price_fmt(all_time_h)}</span>
  </div>
  <div class="stat">
    <span class="stat-key">All-time Low</span>
    <span class="stat-val" style="color:#ff3d5a">{_price_fmt(all_time_l)}</span>
  </div>
  <div class="stat">
    <span class="stat-key">Avg Volume / min</span>
    <span class="stat-val">{avg_vol:,.4f}</span>
  </div>

  <div id="spacer"></div>

  <div class="stat" style="text-align:right">
    <span class="stat-key">Source</span>
    <span class="stat-val" style="color:#484f58">crypto-data-downloader</span>
  </div>
</div>

<div id="tooltip">
  <div class="tt-date" id="tt-date"></div>
  <div class="tt-row"><span class="tt-key">Open</span>  <span class="tt-val" id="tt-o"></span></div>
  <div class="tt-row"><span class="tt-key">High</span>  <span class="tt-val" id="tt-h" style="color:#00ff88"></span></div>
  <div class="tt-row"><span class="tt-key">Low</span>   <span class="tt-val" id="tt-l" style="color:#ff3d5a"></span></div>
  <div class="tt-row"><span class="tt-key">Close</span> <span class="tt-val" id="tt-c"></span></div>
  <div class="tt-row"><span class="tt-key">Volume</span><span class="tt-val" id="tt-v" style="color:#8b949e"></span></div>
</div>

<div id="chart-wrap">
  <div id="chart"></div>
</div>

<div id="footer">
  <span>Scroll to zoom &nbsp;·&nbsp; Drag to pan &nbsp;·&nbsp; Hover for OHLCV &nbsp;·&nbsp; Double-click to reset view</span>
  <span style="margin-left:auto">1-minute OHLCV &nbsp;·&nbsp; All times UTC</span>
</div>

<script>
(function () {{
  // ── data ──────────────────────────────────────────────────────────────────
  const candleData = {candle_json};
  const volumeData = {vol_json};

  // ── chart setup ───────────────────────────────────────────────────────────
  const wrap  = document.getElementById('chart-wrap');
  const chart = LightweightCharts.createChart(document.getElementById('chart'), {{
    width:  wrap.clientWidth,
    height: wrap.clientHeight,
    layout: {{
      background: {{ type: 'solid', color: '#0a0e14' }},
      textColor:  '#8b949e',
      fontFamily: "'SF Mono','Fira Code','Cascadia Code',Consolas,monospace",
      fontSize:   11,
    }},
    grid: {{
      vertLines: {{ color: '#13171f' }},
      horzLines: {{ color: '#13171f' }},
    }},
    crosshair: {{
      mode: LightweightCharts.CrosshairMode.Normal,
      vertLine: {{ color: '#2a2e3a', labelBackgroundColor: '#1a1e28' }},
      horzLine: {{ color: '#2a2e3a', labelBackgroundColor: '#1a1e28' }},
    }},
    rightPriceScale: {{
      borderColor: '#1a1e28',
      scaleMargins: {{ top: 0.05, bottom: 0.25 }},
    }},
    timeScale: {{
      borderColor:    '#1a1e28',
      timeVisible:    true,
      secondsVisible: false,
      barSpacing:     3,
      minBarSpacing:  0.5,
    }},
    handleScroll:  {{ mouseWheel: true, pressedMouseMove: true, horzTouchDrag: true }},
    handleScale:   {{ mouseWheel: true, pinch: true, axisPressedMouseMove: true }},
  }});

  // ── candle series ─────────────────────────────────────────────────────────
  const candleSeries = chart.addCandlestickSeries({{
    upColor:       '#00ff88',
    downColor:     '#ff3d5a',
    borderVisible: false,
    wickUpColor:   '#00b85c',
    wickDownColor: '#cc2e47',
  }});
  candleSeries.setData(candleData);

  // ── volume series ─────────────────────────────────────────────────────────
  const volSeries = chart.addHistogramSeries({{
    priceFormat:    {{ type: 'volume' }},
    priceScaleId:   'vol',
    lastValueVisible: false,
    priceLineVisible: false,
  }});
  chart.priceScale('vol').applyOptions({{
    scaleMargins: {{ top: 0.78, bottom: 0.00 }},
  }});
  volSeries.setData(volumeData);

  // ── fit all data on load ──────────────────────────────────────────────────
  chart.timeScale().fitContent();

  // ── responsive resize ────────────────────────────────────────────────────
  const ro = new ResizeObserver(() => {{
    chart.applyOptions({{ width: wrap.clientWidth, height: wrap.clientHeight }});
  }});
  ro.observe(wrap);

  // ── hover tooltip ─────────────────────────────────────────────────────────
  const tooltip = document.getElementById('tooltip');
  const ttDate  = document.getElementById('tt-date');
  const ttO = document.getElementById('tt-o');
  const ttH = document.getElementById('tt-h');
  const ttL = document.getElementById('tt-l');
  const ttC = document.getElementById('tt-c');
  const ttV = document.getElementById('tt-v');

  // Build a fast lookup map: time_seconds → volume
  const volMap = new Map();
  for (const v of volumeData) volMap.set(v.time, v.value);

  function fmtPrice(p) {{
    if (p === undefined || p === null) return '—';
    if (p >= 10000) return p.toLocaleString('en-US', {{maximumFractionDigits: 0}});
    if (p >= 100)   return p.toFixed(2);
    if (p >= 1)     return p.toFixed(4);
    return p.toFixed(6);
  }}
  function fmtVol(v) {{
    if (v === undefined || v === null) return '—';
    if (v >= 1e6) return (v/1e6).toFixed(2) + 'M';
    if (v >= 1e3) return (v/1e3).toFixed(2) + 'K';
    return v.toFixed(4);
  }}
  function fmtTime(t) {{
    const d = new Date(t * 1000);
    return d.toISOString().replace('T', ' ').slice(0, 16) + ' UTC';
  }}

  chart.subscribeCrosshairMove(param => {{
    if (!param.time || !param.seriesData) {{
      tooltip.style.display = 'none';
      return;
    }}
    const bar = param.seriesData.get(candleSeries);
    if (!bar) {{ tooltip.style.display = 'none'; return; }}

    const vol = volMap.get(param.time) ?? 0;
    const isUp = bar.close >= bar.open;

    ttDate.textContent = fmtTime(param.time);
    ttO.textContent    = fmtPrice(bar.open);
    ttH.textContent    = fmtPrice(bar.high);
    ttL.textContent    = fmtPrice(bar.low);
    ttC.textContent    = fmtPrice(bar.close);
    ttC.style.color    = isUp ? '#00ff88' : '#ff3d5a';
    ttV.textContent    = fmtVol(vol);

    tooltip.style.display = 'block';
  }});

  // ── double-click to reset view ────────────────────────────────────────────
  document.getElementById('chart').addEventListener('dblclick', () => {{
    chart.timeScale().fitContent();
  }});
}})();
</script>
</body>
</html>"""
    return html


def write_html(candles: list, zip_path: str) -> str:
    html = generate_html(candles, zip_path)
    out_path = os.path.join(os.path.dirname(os.path.abspath(zip_path)),
                            '..', '..', _HTML_OUT)
    out_path = os.path.normpath(out_path)
    # Always write next to this script for easy access
    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), _HTML_OUT)
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(html)
    return out_path


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Terminal + HTML candlestick chart for crypto-data-downloader',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument('--file', required=True, metavar='ZIP',
                        help='Path to a zip file, e.g. AllData/btcusdt/btcusdt_2023.zip')
    args = parser.parse_args()

    if not os.path.isfile(args.file):
        print(f'[!] File not found: {args.file}', file=sys.stderr)
        sys.exit(1)

    symbol, label = _symbol_label(args.file)
    print(f'\n  Loading {args.file} …', end='', flush=True)
    candles = _load_candles(args.file)
    print(f'  {len(candles):,} candles loaded.')

    # ── Part 1: ASCII chart ────────────────────────────────────────────────
    render_ascii(candles, args.file)

    # ── Part 2: HTML chart ─────────────────────────────────────────────────
    print(f'  Generating HTML chart …', end='', flush=True)
    out_path = write_html(candles, args.file)
    print(f'  done.')
    print(f'  Saved → {out_path}')
    print()

    # Open in browser
    url = 'file://' + out_path
    print(f'  Opening {url}')
    webbrowser.open(url)
    print()


if __name__ == '__main__':
    main()
