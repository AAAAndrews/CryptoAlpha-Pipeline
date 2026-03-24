# CryptoDataProviders Quick start guide

## Installation steps

### 1. Install dependency packages

```bash
cd CryptoDataProviders
pip install -r requirements.txt
```

### 2. Verify installation

Run the test script to verify that the project is working properly:

```bash
python test_project.py
```

If all tests pass, the project is ready!

## Basic use

### Scenario 1: Get the latest K-line data (recommended for daily updates)

Get real-time or recent data using **Binance REST API**:

```python
from providers.binance_api.market_api import fetch_klines
from datetime import datetime, timezone
import pandas as pd

# Get the BTC 1-hour K-line for the last 3 days
start_time = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
end_time = datetime(2025, 1, 3, 0, 0, 0, tzinfo=timezone.utc)

data_frames = []
for batch_df in fetch_klines(
    symbol="BTCUSDT",
    interval="1h",
    start_time=start_time,
    end_time=end_time,
    kline_type="swap",  # Perpetual contract
    progress=True
):
    data_frames.append(batch_df)

df = pd.concat(data_frames, ignore_index=True)
print(df.head())
```

### Scenario 2: Batch download of historical data (recommended for establishing database)

Use **Binance Bulk Download** to quickly download large amounts of historical data:

```python
from providers.binance_bulk.bulk_fetcher import BinanceBulkFetcher
from datetime import datetime, timezone
import pandas as pd

# Initialize batch scraper
fetcher = BinanceBulkFetcher(
    asset_type="um",  # USDT-M Perpetual contract
    data_type="klines",
    kline_interval="1h"
)

# Download data for the entire month of 2024
start_date = datetime(2024, 12, 1, tzinfo=timezone.utc)
end_date = datetime(2024, 12, 31, tzinfo=timezone.utc)

data_frames = []
for batch_df in fetcher.fetch_range(
    symbol="BTCUSDT",
    start_date=start_date,
    end_date=end_date,
    period_type="monthly",  # Download by month
    progress=True
):
    data_frames.append(batch_df)

df = pd.concat(data_frames, ignore_index=True)
print(f"Downloaded{len(df)} piece of data")
```

### Scenario 3: Multi-exchange data acquisition

Support multiple exchanges using **CCXT**:

```python
from providers.ccxt_api.fetcher import CCXTKlineFetcher
from datetime import datetime, timezone
import pandas as pd

# Initialize OKX grabber
fetcher = CCXTKlineFetcher(
    exchange="okx",  # can be"binance", "okx", "bybit" wait
    kline_type="swap"
)

# Get data
start_time = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
end_time = datetime(2025, 1, 2, 0, 0, 0, tzinfo=timezone.utc)

data_frames = []
for batch_df in fetcher.fetch_klines(
    symbol="BTC/USDT:USDT",
    interval="1h",
    start_time=start_time,
    end_time=end_time,
    progress=True
):
    data_frames.append(batch_df)

df = pd.concat(data_frames, ignore_index=True)
```

### Scenario 4: Get the trading pair list

```python
from utils.trading_pairs import get_trading_pairs

# Get all Binance USDT perpetual contracts
pairs = get_trading_pairs(
    exchange="binance",
    quote_currency="USDT",
    market_type="swap"
)

print(f"share{len(pairs)} trading pairs")
print(pairs[:10])  # Show top 10
```

## Agent configuration

If you need to use a proxy to access the exchange API, go to`config.py` Medium configuration:

```python
PROXY = {
    "http": "http://127.0.0.1:7890",
    "https": "http://127.0.0.1:7890"
}
```

Then pass it in when calling the function:

```python
fetch_klines(
    symbol="BTCUSDT",
    interval="1h",
    proxy=PROXY,  # Incoming proxy configuration
    ...
)
```

## Run the sample script

The project provides a complete sample script:

```bash
# Example 1: Binance REST API
python examples/example_binance_api.py

# Example 2: CCXT multi-exchange
python examples/example_ccxt.py

# Example 3: Batch download
python examples/example_bulk_download.py

# Example 4: Get trading pair
python examples/example_trading_pairs.py
```

## FAQ

### 1. Network connection issues

If you encounter a network timeout or connection error:
- Configure the proxy (see above)
- Check firewall settings
- Try changing the network environment

### 2. API limit

Binance API There are frequency restrictions:
- REST API: There is a request limit per minute
- use`batch_size` Parameters control the number of requests per time
- Add appropriate delay time

### 3. missing data

Some historical data may not exist:
- Newly launched trading pairs may not have complete historical data
- Bulk Download There is a delay in data update
- Check if the trading pair is correct

## Data format

The DataFrame returned by all data sources contains the following columns:

| List| type| illustrate|
|------|------|------|
| open_time | datetime | opening time|
| open | float | opening price|
| high | float | highest price|
| low | float | lowest price|
| close | float | closing price|
| volume | float | Volume|
| close_time | datetime | closing time|
| quote_volume | float | Turnover|
| trades | int | Number of transactions|
| taker_buy_volume | float | Active buying volume|
| taker_buy_quote_volume | float | Active buying turnover|

## Next step

1. Check out [README.md](README.md) for complete API documentation
2. explore`examples/` More examples in directory
3. Customize data acquisition logic according to your needs

## Technical support

If you encounter problems:
1. View error log`errors/errors.json`
2. run`test_project.py` Check environment
3. View project documentation and sample code
