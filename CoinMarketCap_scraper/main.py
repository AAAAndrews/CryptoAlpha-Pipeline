import logging
import sys
import os

# Add the project root directory to sys.path to ensure the scripts module can be imported correctly
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from scripts.cmc_mkt_value_scraper.scraper_engine import ScraperEngine
from scripts.cmc_mkt_value_scraper.config import (
    DEFAULT_PROXY, 
    DEFAULT_THREADS, 
    RUN_MODE, 
    LIMIT, 
    DEFAULT_OUTPUT_DIR
)

def setup_logging():
    # Use absolute paths to ensure the log file location is fixed
    base_dir = os.path.dirname(os.path.abspath(__file__))
    log_file = os.path.join(base_dir, "scraper_v2.log")
    
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # Clear existing handlers
    if root_logger.hasHandlers():
        root_logger.handlers.clear()
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(threadName)s] %(message)s')
    
    # file processor
    fh = logging.FileHandler(log_file, encoding='utf-8', mode='a')
    fh.setFormatter(formatter)
    root_logger.addHandler(fh)
    
    # console processor
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    root_logger.addHandler(sh)

def start_scraper(mode=RUN_MODE, limit=LIMIT, threads=DEFAULT_THREADS, proxy=DEFAULT_PROXY, output=DEFAULT_OUTPUT_DIR, custom_dates=None):
    """
    Pythonic style startup function, which can be called directly by other scripts
    """
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info(f"CMC Crawler starts [mode:{mode}, Thread:{threads}, limit:{limit}]")

    if mode == "scrape":
        engine = ScraperEngine(proxy=proxy, max_threads=threads, output_dir=output)
        engine.run(limit=limit, custom_dates=custom_dates)
    else:
        logger.error(f"Unknown operating mode:{mode}")

if __name__ == "__main__":
    # All parameters are set explicitly in the script, and the configuration in config.py is used by default
    start_scraper(
        mode=RUN_MODE,
        limit=LIMIT,
        threads=DEFAULT_THREADS,
        proxy=DEFAULT_PROXY,
        output=DEFAULT_OUTPUT_DIR,
        custom_dates=None  # If you need to specify a date, you can pass in a list here, for example ["2024-01-01", "2024-01-07"]
    )
