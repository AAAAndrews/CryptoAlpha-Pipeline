# CryptoDataProviders Project description

## Project introduction

`CryptoDataProviders` is from`CryptoDB_feather` An independent data source module separated from the project. It focuses on providing cryptocurrency market data acquisition capabilities and supports multiple data sources and exchanges.

## Separate content

### Detached module

1. **providers/** - data source provider
   - `binance_api/` - Binance REST API interface
   - `binance_bulk/` - Binance Batch historical data download

2. **utils/** - tool module
   - `common.py` - General tool functions (time analysis, progress tracking, error log)
   - `trading_pairs.py` - Trading pair acquisition tool

3. **Configuration and documentation**
   - `config.py` - Global configuration
   - `requirements.txt` - Dependency package list
   - `README.md` - Detailed documentation
   - `QUICKSTART.md` - Quick start guide
   - `test_project.py` - Project test script

4. **examples/** - Usage example
   - `example_binance_api.py` - Binance API Usage example
   - `example_bulk_download.py` - Batch download example
   - `example_trading_pairs.py` - Trading pair acquisition example

### Unseparated modules (remain in the original project)

- `core/` - Core database management module
  - `storage.py` - Feather storage layer
  - `db_manager.py` - Database manager
  - `bulk_manager.py` - Batch update management
  - `reader.py` - Data reading interface

- `scripts/` - Database maintenance script
  - `update_api.py` - API incremental update
  - `update_bulk.py` - Batch update
  - `cleanup_fake_data.py` - Data cleaning

## Project features

### Advantages

1. **Independence**: Completely independent of the database storage layer and can be used independently
2. **Flexibility**: supports multiple data sources and can be selected according to needs
3. **Ease of use**: clear API design, rich sample code
4. **Extensible**: easy to add new exchanges and data sources

### Applicable scenarios

1. **Data Acquisition**: Simply need to obtain cryptocurrency market data
2. **Multiple data sources**: Need to compare data from multiple exchanges
3. **Independent project**: Want to use the data acquisition function in other projects
4. **Learn Research**: Learn how to acquire and process cryptocurrency data

## Usage

### quick start

```bash
# 1. Install dependencies
cd CryptoDataProviders
pip install -r requirements.txt

# 2. Run tests
python test_project.py

# 3. View example
python examples/example_binance_api.py
```

### Basic usage

```python
# Binance REST API
from providers.binance_api.market_api import fetch_klines

for batch_df in fetch_klines(
    symbol="BTCUSDT",
    interval="1h",
    start_time="2025-01-01",
    end_time="2025-01-03",
    kline_type="swap"
):
    print(batch_df)


for batch_df in fetcher.fetch_klines(...):
    print(batch_df)
```

## Relationship to original project

### CryptoDB_feather锛坥riginal project)

- **Positioning**: Complete database system
- **Function**: Data acquisition + storage + management + reading
- **Applicable**: Scenarios where local database needs to be built

### CryptoDataProviders锛坣ew project)

- **Positioning**: Pure data acquisition library
- **Function**: Data acquisition only
- **Applicable**: Only need to obtain data and process the stored scenario by yourself

### collaborative use

Both projects can be used together:

```python
# 1. Get data using CryptoDataProviders
from CryptoDataProviders.providers.binance_api.market_api import fetch_klines

# 2. Use CryptoDB_feather to store data
from CryptoDB_feather.core.storage import save_to_feather
```

## Technical architecture

```
CryptoDataProviders/
鈹?鈹溾攢鈹€ providers/           # data source layer
鈹?  鈹溾攢鈹€ binance_api/    # Binance Native API
鈹?  鈹溾攢鈹€ binance_bulk/   # Binance Batch download
鈹?鈹溾攢鈹€ utils/              # tool layer
鈹?  鈹溾攢鈹€ common.py       # general tools
鈹?  鈹斺攢鈹€ trading_pairs.py # Trading pair management
鈹?鈹溾攢鈹€ examples/           # Sample layer
鈹?  鈹斺攢鈹€ ...
鈹?鈹斺攢鈹€ config.py           # Configuration layer
```

## test status

鉁?All module import tests passed
鉁?All dependent packages have been installed
鉁?Basic function test passed

## Follow-up development

### Features that can be added

1. **More exchanges**: Add support for OKX, Bybit, Huobi, etc.
2. **More data types**: Support depth data, transaction data, etc.
3. **Data Cache**: Add local caching mechanism
4. **Asynchronous support**: Add asynchronous data acquisition
5. **Error retry**: Enhanced error handling and automatic retries

### Improvement suggestions

1. Add unit tests
2. Add performance test
3. Improve error handling
4. Add logging system
5. Support configuration files

## license

MIT License

## Version history

- **v1.0.0** (2026-01-14)
  - Detached from CryptoDB_feather project
  - Complete basic functions and documentation
  - Pass all tests

## Contact information

If you have questions or suggestions, please contact via GitHub Issues.
