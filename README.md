# crypto-data-downloader

Download historical 1-minute OHLCV candle data for any crypto pair, from 2011 to present. Merges multiple exchange sources automatically for maximum coverage.

---

## What it does

- Downloads 1-minute open/high/low/close/volume candles for a given symbol and year
- Pulls from the best available source for that time period (Binance for recent data, legacy exchanges for pre-2017)
- When multiple sources are available, merges and deduplicates by timestamp
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
| `--year` | Four-digit year, e.g. `2023` |
| `--source` | Force a specific exchange: `binance`, `okx`, `bybit`, `bitfinex`, `kraken`, `bitstamp` |
| `--list-pairs` | Print all supported pairs per exchange and exit |

---

## Output format

```
AllData/{symbol}/{symbol}_{year}.zip
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

The script automatically picks the best source(s) for each symbol + year:

- **2021+** — Binance → OKX → Bybit
- **2019–2020** — Binance → OKX
- **2017–2018** — Binance only
- **2013–2016** — Bitfinex → Kraken → Bitstamp

Once any single source reaches 99% coverage for a month, the remaining sources are skipped. When multiple sources are used, rows are merged and deduplicated by timestamp.

---

## Known limitations

**Coinbase Exchange** — Their public candles API now requires authentication. Not supported.

**Bybit** — Geo-blocked from US IP addresses (returns 403). Skipped gracefully with a warning if unavailable. Users outside the US may have access.

**Poloniex** — Their new API has no 1-minute historical data depth. Not supported despite having a large altcoin history from 2014–2016.

**OKX** — Historical 1-minute data only goes back to January 2019, not 2018 as their documentation suggests.

**DOGE/USDT before 2020** — DOGE was only traded against BTC on early exchanges, not USD/USDT. The script will return 0 candles for `dogeusdt` before ~2020, which is correct.

**ETH before March 2016** — ETH/USD pairs didn't exist on major exchanges until mid-2015, and had extremely sparse trading through early 2016. Expect low coverage and many gap warnings for `ethusdt` before mid-2016.

**LTC/BTC/XRP in 2013** — Coverage is very sparse. Exchanges were young, liquidity was thin, and many minutes had no trades.

**Kraken rate limits** — Kraken's public API rate-limits aggressively. For full years of pre-2017 data, Kraken may return `EGeneral:Too many requests` for some months and be skipped. Bitfinex and Bitstamp are usually sufficient for BTC coverage.

**Gap warnings are informational** — A "missing candles" warning means no trades occurred in that minute on the source exchange. This is normal for illiquid pairs and early years — it is not a data error.

---

## Verified test results

| Command | Candles | Notes |
|---------|---------|-------|
| `btcusdt --year 2023` | 525,520 | 100% coverage all months |
| `btcusdt --year 2014` | 525,600 | 100% coverage, Bitfinex + Bitstamp merged |
| `btcusdt --year 2015` | 525,600 | 100% coverage, Bitfinex + Bitstamp merged |
| `ethusdt --year 2016` | 85,818 | Sparse but historically accurate |
| `ltcusdt --year 2013` | 18,645 | Very early LTC data, Bitfinex only |
| `dogeusdt --year 2015` | 0 | Correct — DOGE/USDT didn't exist |
