# CryptoDataProviders

**Cryptocurrency data source unified interface library**

CryptoDataProviders It is a Python library focused on cryptocurrency market data acquisition, providing unified interface access to multiple exchanges. Supports real-time data capture and batch download of historical data.

## 馃搵 characteristic

- **Multiple data sources support**
  - Binance REST API (Spot, perpetual contract, mark price, index price)
  - Binance Bulk Download (Historical data batch download)
  
- **Flexible data acquisition**
  - Supports multiple time granularities (1m, 5m, 15m, 1h, 4h, 1d, etc.)
  - Generator mode, memory efficient
  - Support progress display
  - Automatic batch control

- **Easy to integrate**
  - Clear API design
  - Complete type hints
  - Detailed error log
  - Agent support

## 馃摝 Install

```bash
pip install -r requirements.txt
```

## 馃殌 quick start

### 1. Binance REST API Get data

```python
from providers.binance_api.market_api import fetch_klines
from datetime import datetime, timezone
import pandas as pd

# Configure proxy (optional)
proxy = {
    "http": "http://127.0.0.1:7890",
    "https": "http://127.0.0.1:7890"
}

# Get the 1-hour K-line of Bitcoin perpetual contract
start_time = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
end_time = datetime(2025, 1, 10, 0, 0, 0, tzinfo=timezone.utc)

data_frames = []
for batch_df in fetch_klines(
    symbol="BTCUSDT",
    interval="1h",
    start_time=start_time,
    end_time=end_time,
    kline_type="swap",  # Perpetual contract
    batch_size=1000,
    progress=True,
    proxy=proxy
):
    data_frames.append(batch_df)

# Merge all batch data
df = pd.concat(data_frames, ignore_index=True)
print(df.head())
```


```python
from datetime import datetime, timezone

    exchange="binance",
    kline_type="swap",
    proxies=proxy
)

# Get data
start_time = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
end_time = datetime(2025, 1, 10, 0, 0, 0, tzinfo=timezone.utc)

data_frames = []
for batch_df in fetcher.fetch_klines(
    symbol="BTC/USDT:USDT",
    interval="1h",
    start_time=start_time,
    end_time=end_time,
    batch_size=1000,
    progress=True
):
    data_frames.append(batch_df)

df = pd.concat(data_frames, ignore_index=True)
```

### 3. Binance Historical data batch download

```python
from providers.binance_bulk.bulk_fetcher import BinanceBulkFetcher
from datetime import datetime, timezone
import pandas as pd

# Initialize batch scraper
fetcher = BinanceBulkFetcher(
    asset_type="um",  # USDT-M Perpetual contract
    data_type="klines",
    kline_interval="1h",
    proxy=proxy
)

# Download data for a specific month
start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
end_date = datetime(2024, 1, 31, tzinfo=timezone.utc)

data_frames = []
for batch_df in fetcher.fetch_range(
    symbol="BTCUSDT",
    start_date=start_date,
    end_date=end_date,
    period_type="daily",  # or"monthly"
    progress=True
):
    data_frames.append(batch_df)

df = pd.concat(data_frames, ignore_index=True)
```

### 4. Get list of trading pairs

```python
from utils.trading_pairs import get_trading_pairs

# Get Binance USDT Perpetual Contract Trading Pairs
trading_pairs = get_trading_pairs(
    exchange="binance",
    quote_currency="USDT",
    market_type="swap",
    proxy=proxy
)

print(f"turn up{len(trading_pairs)} trading pairs")
print(trading_pairs[:10])  # Show top 10
```

## 馃摎 API document

### Binance REST API

#### `fetch_klines(...)`

Get candlestick data from Binance REST API.

**parameter:**
- `symbol` (str): Trading pair symbol, such as"BTCUSDT"
- `interval` (str): time interval, such as"1h", "4h", "1d"
- `start_time` (datetime|str|int): start time
- `end_time` (datetime|str|int): end time
- `kline_type` (str): KLine type
  - `"spot"`: Spot goods
  - `"swap"`: Perpetual contract
  - `"mark"`: mark price
  - `"index"`: index price
- `batch_size` (int): The amount of data requested per time, default 1000
- `progress` (bool): Whether to display a progress bar
- `proxy` (dict): Agent configuration

**return:**
- Generator[pd.DataFrame]: Generator that yields one DataFrame batch at a time

**Data column: **
- `open_time`: Opening time (datetime)
- `open`: Opening price (float)
- `high`: Highest price (float)
- `low`: lowest price (float)
- `close`: closing price (float)
- `volume`: Volume (float)
- `close_time`: Closing time (datetime)
- `quote_volume`: Turnover (float)
- `trades`: Number of transactions (int)
- `taker_buy_volume`: Active buying volume (float)
- `taker_buy_quote_volume`: Active buying transaction volume (float)




**Initialization parameters: **
- `exchange` (str): Exchange name, such as"binance", "okx", "bybit"
- `kline_type` (str|None): KLine type, defaults to the exchange default type
- `proxies` (dict|str|None): Agent configuration

**method:**
- `fetch_klines(...)`: Get K-line data
- `get_available_symbols()`: Get the list of available trading pairs

### Binance Bulk Download

#### `BinanceBulkFetcher`

Batch download historical data from Binance S3 bucket.

**Initialization parameters: **
- `asset_type` (str): Asset type
  - `"spot"`: Spot goods
  - `"um"`: USDT-M Perpetual contract
  - `"cm"`: COIN-M Perpetual contract
- `data_type` (str): data type, such as"klines", "aggTrades"
- `kline_interval` (str): Kline spacing (when data_type="klines" hour)
- `proxy` (dict|None): Agent configuration

**method:**
- `fetch_range(...)`: Download data for a specified time range
- `fetch_single(...)`: Download a single file

## 馃敡 Configuration

exist`config.py` Modify the global configuration in:

```python
# Agent configuration
PROXY = {
    "http": "http://127.0.0.1:7890",
    "https": "http://127.0.0.1:7890"
}

# Default batch size
DEFAULT_BATCH_SIZE = 1000

# Request timeout
REQUEST_TIMEOUT = 30
```

## 馃搧 Project structure

```
CryptoDataProviders/
鈹溾攢鈹€ config.py                 # Global configuration
鈹溾攢鈹€ requirements.txt          # Dependency package
鈹溾攢鈹€ README.md                 # Project documentation
鈹溾攢鈹€ __init__.py              # Package initialization
鈹溾攢鈹€ providers/               # data source provider
鈹?  鈹溾攢鈹€ binance_api/         # Binance REST API
鈹?  鈹?  鈹溾攢鈹€ market_api.py    # KLine data acquisition
鈹?  鈹?  鈹溾攢鈹€ cons.py          # constant definition
鈹?  鈹?  鈹斺攢鈹€ utils.py         # Utility function
鈹?  鈹溾攢鈹€ binance_bulk/        # Binance Batch download
鈹?  鈹?  鈹溾攢鈹€ bulk_fetcher.py  # Batch data acquisition
鈹?  鈹?  鈹溾攢鈹€ downloader.py    # Downloader
鈹?  鈹?  鈹斺攢鈹€ exceptions.py    # Exception definition
鈹?      鈹溾攢鈹€ fetcher.py       # data scraper
鈹?      鈹溾攢鈹€ config.py        # Configuration
鈹?      鈹斺攢鈹€ utils.py         # Utility function
鈹溾攢鈹€ utils/                   # general tools
鈹?  鈹溾攢鈹€ common.py            # General functions (time analysis, progress tracking, etc.)
鈹?  鈹斺攢鈹€ trading_pairs.py     # Get trading pair
鈹斺攢鈹€ examples/                # Usage example
    鈹溾攢鈹€ example_binance_api.py
    鈹斺攢鈹€ example_bulk_download.py
```

## 馃幆 Usage scenarios

1. **Quantitative strategy backtesting**: Quickly obtain historical data for strategy backtesting
2. **Data Analysis**: Obtain multi-trading pair data for market analysis
3. **Real-time monitoring**: regularly capture the latest data for monitoring
4. **Database construction**: Download historical data in batches to establish a local database

## 鈿狅笍 Things to note

1. **APIRestrictions**: Comply with the exchange鈥檚 API call frequency limits
2. **Proxy configuration**: If you cannot access the exchange directly, you need to configure a proxy
3. **Error handling**: All errors will be logged to`errors/errors.json`
4. **Memory management**: Use generator mode to avoid loading large amounts of data at once

## 馃 contribute

Issues and Pull Requests are welcome!

## 馃搫 license

MIT License

## 馃摦 Contact information

If you have questions or suggestions, please contact us via Issues.
