# crypto-data-downloader

Download historical 1-minute OHLCV candle data for any crypto pair, from 2011 to present. Automatically merges multiple exchange sources for maximum coverage.

---

## What it does

- Downloads 1-minute open/high/low/close/volume candles for a given symbol and year (or date range)
- Tries all available sources in priority order, merges and deduplicates by timestamp
- Once any source reaches 99% coverage for a month, remaining sources are skipped
- Detects and warns on gaps in the data
- Saves everything as a compressed CSV inside a zip file

---

## Installation

No pip installs required. Uses Python 3 standard library only.

```bash
git clone https://github.com/elevenpercent/crypto-data-downloader.git
cd crypto-data-downloader
python3 download_crypto_data.py --help
```

Requires Python 3.10+ (uses walrus operator `:=`).

> **Mac SSL note:** If you see SSL certificate errors, run:
> ```bash
> pip install certifi
> ```
> The script will pick it up automatically. No code changes needed.

---

## Usage

```bash
# Download BTC/USDT for a full year
python3 download_crypto_data.py --symbol btcusdt --year 2023

# Accepts any common symbol format
python3 download_crypto_data.py --symbol BTC/USDT --year 2023
python3 download_crypto_data.py --symbol btc-usdt --year 2014
python3 download_crypto_data.py --symbol BTC_USDT --year 2019

# Pre-2017 pairs (uses Bitfinex, Kraken, Bitstamp)
python3 download_crypto_data.py --symbol ethusdt --year 2016
python3 download_crypto_data.py --symbol ltcusdt --year 2014

# Date range (exact start/end timestamps, inclusive)
python3 download_crypto_data.py --symbol btcusdt --start "2023-03-15 09:45" --end "2023-06-20 16:00"
python3 download_crypto_data.py --symbol btcusdt --start 2014-01-01 --end 2014-06-30

# Force a specific exchange
python3 download_crypto_data.py --symbol btcusdt --year 2015 --source bitstamp
python3 download_crypto_data.py --symbol ethusdt --year 2016 --source bitfinex

# List all supported pairs and exchanges
python3 download_crypto_data.py --list-pairs
```

### Flags

| Flag | Description |
|------|-------------|
| `--symbol` | Trading pair in any format: `btcusdt`, `BTC/USDT`, `btc-usdt`, `BTC_USDT` |
| `--year` | Four-digit year, e.g. `2023`. Mutually exclusive with `--start`/`--end` |
| `--start` | Range start: `2013-01-02` or `"2013-01-02 09:45"` (UTC) |
| `--end` | Range end: `2013-12-20` or `"2013-12-20 16:00"` (UTC) |
| `--source` | Force a specific exchange: `binance`, `okx`, `bybit`, `bitfinex`, `kraken`, `bitstamp` |
| `--list-pairs` | Print all supported pairs per exchange and exit |

---

## Output format

```
AllData/{symbol}/{symbol}_{year}.zip
AllData/{symbol}/{symbol}_{start-date}_{end-date}.zip
```

Each zip contains a single CSV file with these columns:

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | integer | Unix milliseconds (UTC) |
| `open` | float | Open price |
| `high` | float | High price |
| `low` | float | Low price |
| `close` | float | Close price |
| `volume` | float | Volume in base currency |

Example (`btcusdt_2023.csv`):
```
timestamp,open,high,low,close,volume
1672531200000,16541.77,16544.76,16538.45,16543.67,83.08
1672531260000,16543.04,16544.41,16538.48,16539.31,80.45
1672531320000,16539.31,16541.17,16534.52,16536.43,62.90
```

---

## Supported exchanges and date ranges

| Exchange | Available from | Pairs |
|----------|---------------|-------|
| **Binance Vision** | 2017 | All Binance spot pairs (400+) |
| **OKX** | 2019 | All USDT/USD spot pairs |
| **Bybit** | 2021 | All USDT spot pairs |
| **Bitfinex** | 2013 | BTC, ETH, LTC, XRP, XMR, DASH, ETC, ZEC, BCH, EOS, NEO, OMG, IOTA, TRX, LINK, ALGO |
| **Kraken** | 2013 | BTC, ETH, LTC, XRP, XMR, DASH, ETC, ZEC, BCH, ADA, SOL, DOGE, XLM, ATOM, DOT, LINK, UNI, MATIC, AVAX, NEAR, ALGO, TRX |
| **Bitstamp** | 2011 | BTC, ETH, LTC, XRP, BCH, LINK |

### Source selection logic

Sources are tried in priority order for each month. Once any source hits ≥99% coverage, the rest are skipped.

| Era | Sources tried |
|-----|--------------|
| 2021+ | Binance Vision → OKX → Bybit |
| 2019–2020 | Binance Vision → OKX |
| 2017–2018 | Binance Vision |
| 2013–2016 | Bitfinex → Kraken → Bitstamp |
| 2011–2012 | Bitstamp |

---

## Known limitations

**Coinbase Exchange** — Their public candles API now requires authentication. Not supported.

**Bybit** — Geo-blocked from US IP addresses (returns 403). Skipped gracefully if unavailable.

**Poloniex** — Their new API has no 1-minute historical data depth. Not supported.

**OKX** — Historical 1-minute data only goes back to January 2019.

**DOGE/USDT before 2020** — DOGE was only traded against BTC on early exchanges. Expect 0 candles for `dogeusdt` before ~2020.

**ETH before March 2016** — ETH/USD pairs had very sparse trading through early 2016. Expect low coverage and gap warnings.

**Kraken rate limits** — Kraken's public API rate-limits aggressively for pre-2017 data. Bitfinex and Bitstamp are usually sufficient for BTC coverage.

**Gap warnings are informational** — A "missing candles" warning means no trades occurred in that minute on the source exchange. This is normal for illiquid pairs and early years.

---

## Verified test results

| Command | Sources used | Candles |
|---------|-------------|---------|
| `btcusdt --year 2023` | Binance only (≥99% every month) | 525,520 |
| `btcusdt --year 2014` | Bitfinex + Bitstamp (merged to 100%) | 525,600 |
| `btcusdt --start "2023-03-15 09:45" --end "2023-06-20 16:00"` | Binance only | 139,976 |
| `ethusdt --year 2016` | Bitfinex + Bitstamp | 85,818 |
| `ltcusdt --year 2013` | Bitfinex only | 18,645 |
| `dogeusdt --year 2015` | — | 0 (correct — DOGE/USDT didn't exist) |
