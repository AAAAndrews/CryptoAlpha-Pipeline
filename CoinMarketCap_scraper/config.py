import os

# Basic configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(BASE_DIR)), "data", "cmc_snapshots")
# Operation control
RUN_MODE = "scrape"
LIMIT = None         # The limit on the number of crawled dates, None means crawling all
DEFAULT_THREADS = 8
DEFAULT_PROXY = "http://127.0.0.1:7890"

# CMC API Configuration
CMC_API_URL = "https://api.coinmarketcap.com/data-api/v3/cryptocurrency/listings/historical"
CMC_HISTORICAL_URL = "https://coinmarketcap.com/historical/"

# Crawler settings
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY = 2

# Selenium set up
CHROME_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
HEADLESS_MODE = True
