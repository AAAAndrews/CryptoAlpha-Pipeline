import os
import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from scripts.cmc_mkt_value_scraper.session_manager import SessionManager
from scripts.cmc_mkt_value_scraper.api_client import APIClient
from scripts.cmc_mkt_value_scraper.utils import ensure_dir
from scripts.cmc_mkt_value_scraper.config import DEFAULT_PROXY, DEFAULT_OUTPUT_DIR, DEFAULT_THREADS, CMC_HISTORICAL_URL
import cloudscraper
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

class ScraperEngine:
    def __init__(self, proxy=DEFAULT_PROXY, output_dir=None, max_threads=DEFAULT_THREADS):
        self.proxy = proxy
        self.output_dir = output_dir or DEFAULT_OUTPUT_DIR
        self.max_threads = max_threads
        self.session_mgr = SessionManager(proxy=proxy)
        
        ensure_dir(self.output_dir)

    def get_historical_dates(self):
        """Get all historical snapshot dates"""
        scraper = cloudscraper.create_scraper()
        proxy_dict = {"http": self.proxy, "https": self.proxy} if self.proxy else None
        logger.info("Getting historical snapshot list...")
        try:
            response = scraper.get(CMC_HISTORICAL_URL, proxies=proxy_dict, timeout=20)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                links = soup.find_all('a', href=True)
                # Extract the date part, for example /historical/20220403/ -> 2022-04-03
                dates = []
                for l in links:
                    href = l['href']
                    if '/historical/20' in href:
                        date_str = href.strip('/').split('/')[-1]
                        if len(date_str) == 8:
                            formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
                            dates.append(formatted_date)
                
                dates = sorted(list(set(dates)), reverse=True)
                logger.info(f"successfully found{len(dates)} historical date.")
                return dates
            else:
                logger.error(f"Failed to obtain the list, status code:{response.status_code}")
                return []
        except Exception as e:
            logger.error(f"Error while getting list of dates:{e}")
            return []

    def run(self, limit=None, custom_dates=None):
        # 1. Get verification information
        cookies, headers = self.session_mgr.get_auth_info()
        if not cookies or not headers:
            logger.warning("Unable to obtain verification information through Selenium, will try to use cloudscraper to crawl directly...")
            cookies, headers = {}, {}

        # 2. Get list of dates
        if custom_dates:
            dates = custom_dates
            logger.info(f"Using a custom date list, total{len(dates)} date.")
        else:
            dates = self.get_historical_dates()

        if limit:
            dates = dates[:limit]

        # 3. Multi-thread crawling
        client = APIClient(cookies, headers, proxy=self.proxy)
        
        logger.info(f"Start multi-thread crawling, number of threads:{self.max_threads}")
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            future_to_date = {executor.submit(self.scrape_date, client, date): date for date in dates}
            for future in as_completed(future_to_date):
                date = future_to_date[future]
                try:
                    success = future.result()
                    if success:
                        logger.info(f"date{date} Processing completed.")
                    else:
                        logger.error(f"date{date} Processing failed.")
                except Exception as e:
                    logger.error(f"Processing date{date} An uncaught exception occurs when:{e}")

    def scrape_date(self, client: APIClient, date: str):
        """Crawling tasks for a single date"""
        output_path = os.path.join(self.output_dir, f"cmc_historical_{date.replace('-', '')}.csv")
        if os.path.exists(output_path):
            logger.info(f"File already exists, skip:{output_path}")
            return True

        try:
            df = client.fetch_all_for_date(date)
            if not df.empty:
                df.to_csv(output_path, index=False, encoding='utf-8-sig')
                return True
            return False
        except Exception as e:
            logger.error(f"Crawl date{date} Error:{e}")
            return False
