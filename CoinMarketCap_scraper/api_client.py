import requests
import logging
import time
import pandas as pd
import cloudscraper
from typing import Dict, List, Optional, Any
from scripts.cmc_mkt_value_scraper.config import CMC_API_URL, REQUEST_TIMEOUT, MAX_RETRIES, RETRY_DELAY

logger = logging.getLogger(__name__)

class APIClient:
    def __init__(self, cookies: Optional[Dict[str, str]] = None, headers: Optional[Dict[str, str]] = None, proxy: str = "http://127.0.0.1:7890"):
        self.cookies = cookies or {}
        self.headers = headers or {}
        self.proxies = {
            "http": proxy,
            "https": proxy
        }
        self.session = cloudscraper.create_scraper()
        if self.cookies:
            self.session.cookies.update(self.cookies)
        if self.headers:
            self.session.headers.update(self.headers)
        self.session.proxies.update(self.proxies)

    def fetch_page(self, date: str, start: int = 1, limit: int = 200) -> Optional[Dict[str, Any]]:
        """
        Get single page data with retry mechanism
        """
        params = {
            "convertId": "825",# 2781 :usd 825:usdt 1:btc
            "date": date,
            "limit": limit,
            "start": start
        }
        
        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.get(CMC_API_URL, params=params, timeout=REQUEST_TIMEOUT)
                if response.status_code == 200:
                    return response.json()
                else:
                    logger.error(f"API Request failed [{response.status_code}] (try{attempt+1}/{MAX_RETRIES}): {response.text[:200]}")
            except Exception as e:
                logger.error(f"Request exception (try{attempt+1}/{MAX_RETRIES}): {e}")
            
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
        
        return None

    def parse_data(self, json_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Parse JSON data, retain all fields, and flatten nested structures
        """
        if not json_data or 'data' not in json_data:
            return []
        
        items = json_data['data']
        if not isinstance(items, list):
            return []
            
        parsed_results = []
        
        for item in items:
            flat_item = {}
            
            # Simple flat logic
            for k, v in item.items():
                if k == 'quotes':
                    # Process quotation information, usually including USD, BTC, etc.
                    for quote in v:
                        q_name = quote.get('name', 'unknown').upper()
                        for qk, qv in quote.items():
                            if qk != 'name':
                                flat_item[f'quote_{q_name}_{qk}'] = qv
                elif isinstance(v, dict):
                    # Handle other nested dictionaries
                    for sub_k, sub_v in v.items():
                        flat_item[f"{k}_{sub_k}"] = sub_v
                elif isinstance(v, list):
                    # Process the list (convert to string and retain)
                    flat_item[k] = str(v)
                else:
                    flat_item[k] = v
                    
            parsed_results.append(flat_item)
            
        return parsed_results

    def fetch_all_for_date(self, date: str, max_items: int =1e6) -> pd.DataFrame:
        """
        Get all data for a specific date (paginated)
        """
        all_data = []
        start = 1
        limit = 200
        
        while start <= max_items:
            logger.info(f"Getting{date} data, start={start}...")
            json_res = self.fetch_page(date, start, limit)
            if not json_res:
                break
            
            page_data = self.parse_data(json_res)
            if not page_data:
                break
                
            all_data.extend(page_data)
            
            # If the returned data is less than the limit, it means the end has been reached.
            if len(page_data) < limit:
                break
            
            start += limit
            
        return pd.DataFrame(all_data)
        #     start += limit
        #     time.sleep(0.5) # polite delay
            
        # return pd.DataFrame(all_data)
