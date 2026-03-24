# CMC Historical Scraper V2

This is a refactored CoinMarketCap historical market capitalization data crawler.

## Project structure
- `session_manager.py`: Use Selenium to obtain authentication information (Cookies and Headers).
- `api_client.py`: Encapsulates requests to CMC’s internal API and JSON parsing logic.
- `scraper_engine.py`: Multi-threaded crawling engine, responsible for scheduling dates and saving data.
- `main.py`: Project entrance.

## How to use

### Install dependencies
```bash
pip install -r requirements.txt
```

### Run the crawler
```bash
python scripts/cmc_mkt_value_scraper/main.py --mode scrape --threads 8 --limit 10
```

## Features
1. **Efficient**: Compared with Selenium crawling HTML, directly calling internal API is more than 10 times faster.
2. **Complete**: parses all fields in JSON, including multi-currency quotes (USD, BTC, etc.).
3. **Robust**: supports multi-threading and has error handling and retry mechanisms. Even if Selenium fails to obtain verification information, it will try to use the default headers to continue crawling.
4. **Flexible**: Customizable proxy, concurrency number and crawling range.
