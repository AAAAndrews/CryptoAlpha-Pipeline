# CryptoTradingSystem_allin1

Integrated cryptocurrency research and data-engineering workspace for factor research, market-data ingestion, and storage maintenance.

## Documentation Center

Use this section as the single entry point for all project documentation.

### Root

- [README.md](README.md): Repository-level overview, architecture, and navigation

### CoinMarketCap Scraper

- [CoinMarketCap_scraper/README.md](CoinMarketCap_scraper/README.md): CMC historical market-cap scraping module

### CryptoDataProviders

- [CryptoDataProviders/README.md](CryptoDataProviders/README.md): Main API and usage for all data providers
- [CryptoDataProviders/QUICKSTART.md](CryptoDataProviders/QUICKSTART.md): Fast setup and first-run steps
- [CryptoDataProviders/PROJECT_INFO.md](CryptoDataProviders/PROJECT_INFO.md): Project positioning and structure notes
- [CryptoDataProviders/SUMMARY.md](CryptoDataProviders/SUMMARY.md): High-level implementation summary

### CryptoDB_feather

- [CryptoDB_feather/README.md](CryptoDB_feather/README.md): Storage-layer overview and API usage
- [CryptoDB_feather/DEVELOPMENT.md](CryptoDB_feather/DEVELOPMENT.md): Development notes and engineering conventions
- [CryptoDB_feather/REFACTORING_SUMMARY.md](CryptoDB_feather/REFACTORING_SUMMARY.md): Refactor details and architecture changes
- [CryptoDB_feather/CLEANUP_REPORT.md](CryptoDB_feather/CLEANUP_REPORT.md): Cleanup results and migration outcomes

### Scripts

- [scripts/README.md](scripts/README.md): End-to-end maintenance scripts guide
- [scripts/QUICKREF.md](scripts/QUICKREF.md): Command cheat sheet
- [scripts/IMPORT_GUIDE.md](scripts/IMPORT_GUIDE.md): Import-path conventions
- [scripts/MIGRATION_SUMMARY.md](scripts/MIGRATION_SUMMARY.md): Script migration details

## Module Guide

### 1. CoinMarketCap_scraper

Purpose:
- Crawl historical CoinMarketCap market-cap data through authenticated internal API flows.

Core responsibilities:
- Manage anti-bot/session credentials via Selenium-assisted bootstrap
- Call CMC internal endpoints and parse JSON payloads
- Execute concurrent date-range scraping and persist results

Key files:
- `CoinMarketCap_scraper/main.py`: module entry point
- `CoinMarketCap_scraper/session_manager.py`: authentication/session management
- `CoinMarketCap_scraper/api_client.py`: HTTP client and response parsing
- `CoinMarketCap_scraper/scraper_engine.py`: multi-thread orchestration

Typical usage:
- Best suited for historical market-cap backfill and periodic refresh jobs.

### 2. Cross_Section_Factor

Purpose:
- Research and mine alpha expressions with cross-sectional and time-series methods.

Core responsibilities:
- Factor-expression generation and transformation
- GP/DEAP-based evolution experiments
- Fitness evaluation and stress testing
- Integration helpers for WorldQuant-style workflows

Key subpackages:
- `Cross_Section_Factor/deap_alpha/`: custom DEAP components, operators, and fitness metrics
- `Cross_Section_Factor/factorset/`: factor sets and transform utilities
- `Cross_Section_Factor/llm_alpha/`: LLM-assisted alpha workflow experiments
- `Cross_Section_Factor/worldquant_utils/`: WorldQuant API/client wrappers

Typical usage:
- Used in research loops where candidate factors are generated, evaluated, and iteratively improved.

### 3. CryptoDataProviders

Purpose:

Core responsibilities:
- Normalize access patterns for multiple data sources
- Support generator/batch retrieval for large-range time windows
- Encapsulate exchange-specific logic and error handling
- Provide symbol-list discovery utilities

Key files and folders:
- `CryptoDataProviders/providers/binance_api/`: REST endpoint adapters
- `CryptoDataProviders/providers/binance_bulk/`: S3 bulk historical downloader
- `CryptoDataProviders/utils/trading_pairs.py`: trading-pair discovery helpers
- `CryptoDataProviders/config.py`: global provider configuration

Typical usage:
- Called directly in research scripts or indirectly by storage-layer updaters.

### 4. CryptoDB_feather

Purpose:
- Act as the local storage layer for kline data and provide consistent read/write/update interfaces.

Core responsibilities:
- Feather-based data persistence
- Incremental update orchestration (REST and bulk modes)
- Thread-safe storage metadata management
- Unified read APIs for single or multi-symbol retrieval

Key files:
- `CryptoDB_feather/core/storage.py`: path resolution and file IO primitives
- `CryptoDB_feather/core/bulk_manager.py`: bulk download orchestration
- `CryptoDB_feather/core/reader.py`: query/read interface for stored data
- `CryptoDB_feather/config.py`: DB root path and runtime configuration

Typical usage:
- Maintains local market-data lake consumed by factor research pipelines.

### 5. scripts

Purpose:
- Provide operational entry scripts for routine maintenance and migration tasks.

Core responsibilities:
- Daily API incremental updates
- Historical bulk backfill
- Suspicious tail-data cleanup for delisted symbols
- Environment and dependency verification

Key scripts:
- `scripts/update_api.py`: daily incremental update workflow
- `scripts/update_bulk.py`: historical backfill workflow
- `scripts/cleanup_fake_data.py`: dry-run/real cleanup workflow
- `scripts/test_scripts.py`: integration smoke test

Typical usage:
- Run manually or via scheduler (cron/task scheduler) for production-like maintenance.

## Architecture Flow

The repository follows a layered model:

1. `CryptoDataProviders` acquires raw market data.
2. `CryptoDB_feather` persists and serves structured local data.
3. `Cross_Section_Factor` consumes stored data for factor research.
4. `scripts` orchestrate operational update and maintenance workflows.
5. `CoinMarketCap_scraper` provides additional external market-cap data pipelines.

## Quick Start

1. Create and activate a Python environment.
2. Install dependencies in each module where needed:
	- `CoinMarketCap_scraper/requirements.txt`
	- `CryptoDataProviders/requirements.txt`
3. Set proxy and data-root configuration in module config files.
4. Run `scripts/test_scripts.py` to verify imports and environment health.
5. Bootstrap data with `scripts/update_bulk.py`, then maintain with `scripts/update_api.py`.

## Documentation Maintenance Rules

- Add new documentation links to the Documentation Center in this file.
- Keep module-specific implementation details in each module's own README.
- Use this root README as the navigation and architecture index.

## License

No license is currently declared in this repository.
