# CryptoDB_feather Development documentation

> **🔄 The project has been restructured (2026-01-14)**: The data source function has been separated into [`CryptoDataProviders`](../CryptoDataProviders/) project. This project now focuses on the storage layer. See [REFACTORING_SUMMARY.md](./REFACTORING_SUMMARY.md) for details

## 1. Project overview

`CryptoDB_feather` It is a high-performance K-line data **storage layer** specially designed for cryptocurrency quantitative trading. It utilizes the Apache Feather (Arrow) format to store data, which is extremely efficient in terms of read and write speed, storage space, and Pandas integration.

### Core features:
- **High-performance storage**: Using the Feather format, read and write much faster than CSV or SQL databases.
- **Hybrid update mode**:
  - **REST API**：pass`CryptoDataProviders` Get recent data for incremental updates.
  - **Bulk Download**：pass`CryptoDataProviders` Batch download historical data from Binance S3.
- **Thread safety**: built-in file system lock, supports multi-threaded concurrent updates, and can automatically synchronize metadata to`dbinfo.json`。
- **Data source independent**: Pass`CryptoDataProviders` Unified interface supports multiple data sources.

### Architectural relationship:

```
┌─────────────────────────────────────────────────────┐
│              scripts/ (Maintenance script)│
│  - update_api.py (Incremental updates)│
│  - update_bulk.py (Batch download)│
│  - cleanup_fake_data.py (cleanup)│
└──────────────┬──────────────────┬───────────────────┘
               │                  │
               ▼                  ▼
┌──────────────────────┐   ┌────────────────────────┐
│ CryptoDataProviders  │   │  CryptoDB_feather      │
│  (data source layer)│   │  (storage layer)│
│                      │   │                        │
│ - Binance API       │◄──┤ - db_manager.py        │
│ - CCXT API          │   │ - bulk_manager.py      │
│ - Binance Bulk      │   │ - storage.py           │
│ - Trading Pairs     │   │ - reader.py            │
└──────────────────────┘   └────────────────────────┘
```

---

## 2. Directory structure

```text
CryptoDB_feather/
├── core/                      # Storage layer core module
│   ├── storage.py             # Feather File reading and writing, deduplication and path synchronization
│   ├── bulk_manager.py        # Batch historical data download management
│   ├── db_manager.py          # REST API Incremental update management
│   ├── reader.py              # Data reading interface
│   └── __init__.py            # Export core functionality
├── config.py                  # Global configuration (DB path, proxy, etc.)
├── DEVELOPMENT.md             # This document
├── REFACTORING_SUMMARY.md     # Refactoring summary
└── research_nb.ipynb          # research notebook
```

**Notice**:`providers/`, `utils/`, `scripts/` Migrated, see [REFACTORING_SUMMARY.md](./REFACTORING_SUMMARY.md) for details

---

## 3. Core module description

### 3.1 storage layer (`core/storage.py`)

**Responsibilities**: Reading, writing, deduplication and metadata management of Feather files.

#### Main functions:

- **`get_synced_filepath`**: 
  - **enter**:`db_root_path`, `exchange`, `symbol`, `kline_type`, `interval`
  - **Output**:`file_path` (str)
  - **Purpose**: thread-safely allocate directories on disk and`dbinfo.json` The "logical-physical" path mapping in the file is synchronized to ensure the traceability of data files.

### 3.2 Incremental Update Manager (`core/db_manager.py`)
- **`run_ccxt_updater`**: Updated with CCXT engine.
- **`run_binance_rest_updater`**: Updated using Binance’s dedicated REST interface, supported`spot`, `swap`, `mark`, `index` and other K-lines.
- **Logic**: Automatically detect the latest local OHLCV, capture only missing data segments, and automatically merge and remove duplicates.

### 3.3 Data reading interface (`core/reader.py`)
- **`read_symbol_klines`**: Loading single currency data is supported`start_time` and`end_time` filter.
- **`load_multi_klines`**: Concurrently load data of multiple currencies and return a long-format DataFrame, which is very suitable for multi-factor analysis or cross-sectional strategy backtesting.

---

## 4. Development Guide

### 4.1 Environmental preparation
```bash
pip install pandas pyarrow requests ccxt rich tqdm
```

### 4.2 Basic configuration
exist`config.py` Medium settings`DB_ROOT_PATH`：
```python
DB_ROOT_PATH = "C:/Your/Data/Path"
```

### 4.3 Common operating procedures

#### A. Establishing a database for the first time (large-scale historical data)
run`scripts/update_bulk.py`，Configure a longer number of concurrent threads.

#### B. Daily maintenance incremental updates
run`scripts/update_api.py`，It quickly checks the local status and supplements the latest K-line.

#### C. Data analysis reading
```python
from core.reader import load_multi_klines

df = load_multi_klines(
    db_root_path=DB_ROOT_PATH,
    exchange="binance",
    symbols=["BTCUSDT", "ETHUSDT"],
    interval="1h",
    start_time="2025-01-01"
)
```

---

## 5. Design philosophy: Why choose Feather?
- **Zero-cost loading**: Feather files are memory-mapped files on disk. There is almost no need to recalculate the data type when loading, and they are instantly converted into Pandas objects.
- **Native multi-architecture support**: Feather is powered by Apache Arrow, which means you can access this data in the same structure in Python, R or C++.
- **Compactness**: Compared to CSV, Feather provides a better compression ratio and supports concurrent reading.

---

## 6. Maintenance and error handling
All failed crawling tasks will be automatically recorded`db_root_path/errors/errors.json`。
If the task needs to be retried, the system will dynamically generate`retry_pairs.json`，This facilitates the subsequent execution of targeted patches.

---
*Document version: v1.1 (Last Updated: 2026-01-13)*
