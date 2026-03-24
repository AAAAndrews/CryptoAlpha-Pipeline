# Database maintenance script

This directory contains scripts for maintaining and updating the CryptoDB_feather database. These scripts have been adapted to use **CryptoDataProviders** as the data source interface.

## 📁 script list

### 1. update_api.py - REST API incremental update

**Purpose**: Use Binance REST API for daily incremental updates, suitable for synchronizing the latest hours or days of data.

**Features**:
- ✅ Quickly update recent data
- ✅ Automatically detect the latest local timestamp
- ✅ Fetch only missing data segments
- ✅ Suitable for daily scheduled tasks

**How to use**:
```bash
cd scripts
python update_api.py
```

**Configuration**:
```python
# Modify these parameters in the script
update_params = {
    "interval_list": ["1h"],        # time interval
    "kline_type_list": ["swap"],    # KLine type
    "proxy": PROXY,                 # Agent configuration
    "progress": True                # show progress
}
```

**Applicable scenarios**:
- Update the database regularly every day
- Synchronize data of recent days
- Use when network conditions are good

---

### 2. update_bulk.py - Batch historical data download

**Purpose**: Use Binance S3 batch download, suitable for establishing a database for the first time or synchronizing historical data in a long range.

**Features**:
- ✅ Quickly download large amounts of historical data
- ✅ Support multi-thread concurrency
- ✅ Get compressed files directly from S3
- ✅ Suitable for one-time batch import

**How to use**:
```bash
cd scripts
python update_bulk.py
```

**Configuration**:
```python
# Modify these parameters in the script
update_params = {
    "interval_list": ["1h"],        # time interval
    "kline_type_list": ["swap"],    # KLine type
    "proxy": PROXY,                 # Agent configuration
    "max_workers": 16,              # Number of concurrent threads
    "batch_size": 100,              # batch size
    "single_threaded": False        # Multi-threaded mode
}
```

**Applicable scenarios**:
- Create database for the first time
- Download large amounts of historical data (months/years)
- Rebuild or complete the database

---

### 3. cleanup_fake_data.py - Clean up fake data

**Purpose**: Identify and clean up false data at the end of delisted trading pairs (OHLC is equal and trading volume is 0).

**Features**:
- ✅ Automatically identify delisted trading pairs
- ✅ Clean up the false K-line data at the end
- ✅ Support drill mode (dry run)
- ✅ Multi-threaded concurrent processing

**How to use**:
```bash
cd scripts

# Practice mode (recommended to run first)
python cleanup_fake_data.py

# Actual cleaning (need to modify dry_run in the script=False）
```

**Configuration**:
```python
run_cleanup(
    kline_type="swap",   # 'swap' or'spot'
    interval="1h",       # time interval
    dry_run=True,        # True=Walkthrough, False=Actual cleanup
    max_workers=10       # Number of concurrent threads
)
```

**Applicable scenarios**:
- Clean the database regularly
- Remove invalid data for delisted currencies
- Free up storage space

**Note**:
- ⚠️ It is recommended to use the drill mode for the first run
- ⚠️ Make sure you have a backup before doing the actual cleanup
- ⚠️ Only clear trading pairs that have been confirmed to be delisted

---

## 🔧 Dependencies

These scripts depend on the following modules:

### CryptoDataProviders（data source)
- `utils.trading_pairs` - Get list of trading pairs

### CryptoDB_feather（storage layer)
- `core.db_manager` - REST API Data management
- `core.bulk_manager` - Batch download management
- `config` - Configuration (proxy, path, etc.)

## 📊 Workflow

### Typical database maintenance process:

```
1. Create database for the first time
   └─> Run update_bulk.py (batch download historical data)

2. Routine maintenance
   └─> Run update_api.py every day (incremental update)

3. Clean regularly
   └─> Run cleanup_fake_data.py every month (clean up delisted currencies)
```

### Update strategy comparison

| characteristic| update_api.py | update_bulk.py |
|------|---------------|----------------|
| data source| REST API | S3 Bulk Download |
| speed| medium| quick|
| Scope of application| recent data| Full historical data|
| APIlimit| There is a frequency limit| Unlimited|
| Network requirements| Stablize| Requires good bandwidth|
| Recommended scenarios| daily updates| Establishing a database for the first time|

## 🚀 quick start

### First time use

1. **Configure environment**
   ```bash
   # Make sure dependencies are installed
   pip install -r CryptoDataProviders/requirements.txt
   pip install -r CryptoDB_feather/requirements.txt  # if any
   ```

2. **Check configuration**
   ```python
   # Set in CryptoDB_feather/config.py
   DB_ROOT_PATH = "your database path"
   PROXY = {"http": "...", "https": "..."}  # If you need an agent
   ```

3. **Run batch download**
   ```bash
   cd scripts
   python update_bulk.py
   ```

4. **Set up scheduled tasks** (optional)
   ```bash
   # Linux/Mac (crontab)
   0 2 * * * cd /path/to/scripts && python update_api.py

   # Windows (task scheduler)
   Create a task and run update_api.py at 2 am every day
   ```

### Routine maintenance

```bash
# Updated once a day
cd scripts
python update_api.py

# Clean up once a month (drill first)
python cleanup_fake_data.py
```

## 🔍 Troubleshooting

### FAQ

1. **ImportError: No module named 'xxx'**
   - Solution: Make sure the project path is correct, the script will automatically add the necessary paths

2. **Network connection failed**
   - Resolution: Check proxy configuration to ensure PROXY is set correctly

3. **Data update failed**
   - Check the error log:`DB_ROOT_PATH/errors/errors.json`
   - Check if the trading pair is still trading

4. **File permission error**
   - Make sure you have read and write permissions on the database directory

### error log

All errors will be logged to:
```
DB_ROOT_PATH/errors/errors.json
```

View recent errors:
```bash
cat DB_ROOT_PATH/errors/errors.json | tail -20
```

## 📝 Custom configuration

### Modify trading pair filter

By default, all USDT perpetual contracts are obtained. If you need to modify:

```python
# Find this line in the script and modify it
trading_pairs_list = get_trading_pairs(
    exchange="binance",
    quote_currency="USDT",     # Change to"BUSD", "BTC" wait
    market_type="swap",        # Change to"spot" wait
    proxy=PROXY
)

# Or manually specify the trading pair
trading_pairs_list = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
```

### Modify time interval

```python
update_params.update({
    "interval_list": ["1h", "4h", "1d"],  # Update multiple intervals simultaneously
    ...
})
```

### Modify K-line type

```python
update_params.update({
    "kline_type_list": ["spot", "swap", "mark"],  # many types
    ...
})
```

## 🎯 best practices

1. **First time database creation**: use`update_bulk.py`
2. **Routine maintenance**: Use`update_api.py`
3. **Periodic Cleanup**: Run once a month`cleanup_fake_data.py`
4. **Backup data**: Be sure to back up the database before cleaning
5. **Monitoring logs**: Check error logs regularly
6. **Reasonable concurrency**: Adjust according to network conditions`max_workers`

## 📞 Technical support

In case of problems:
1. View comments within the script
2. Check error log
3. Refer to the documentation for CryptoDataProviders and CryptoDB_feather

---

**Version**: 1.0.0
**Update date**: 2026-01-14
**Maintenance**: Use CryptoDataProviders data source interface
