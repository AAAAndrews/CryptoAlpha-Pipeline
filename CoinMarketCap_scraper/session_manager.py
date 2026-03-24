import os
import time
import logging
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from scripts.cmc_mkt_value_scraper.config import DEFAULT_PROXY, CHROME_USER_AGENT, HEADLESS_MODE, CMC_HISTORICAL_URL

logger = logging.getLogger(__name__)

class SessionManager:
    def __init__(self, proxy=DEFAULT_PROXY):
        self.proxy = proxy
        self._setup_env()

    def _setup_env(self):
        if self.proxy:
            os.environ['http_proxy'] = self.proxy
            os.environ['https_proxy'] = self.proxy
            os.environ['WDM_PROXY'] = self.proxy
        os.environ['no_proxy'] = "localhost,127.0.0.1"
        os.environ['WDM_SSL_VERIFY'] = '0'

    def _get_driver(self):
        """Initialize Selenium driver (using undetected-chromedriver)"""
        import undetected_chromedriver as uc
        
        options = uc.ChromeOptions()
        chrome_path = os.path.join(os.environ.get('LOCALAPPDATA', ''), "Google/Chrome/Application/chrome.exe")
        if os.path.exists(chrome_path):
            options.binary_location = chrome_path
        
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        if self.proxy:
            options.add_argument(f'--proxy-server={self.proxy}')
        if HEADLESS_MODE:
            options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('window-size=1920x1080')
        options.add_argument(f'user-agent={CHROME_USER_AGENT}')
        
        # Disable logging output
        options.add_argument('--log-level=3')

        driver = uc.Chrome(options=options, version_main=131)
        return driver

    def get_auth_info(self, target_url=CMC_HISTORICAL_URL):
        """
        Visit the target page and obtain Cookies and Headers
        """
        logger.info(f"Retrieving verification information via Selenium:{target_url}")
        driver = None
        try:
            driver = self._get_driver()
            driver.get(target_url)
            # Shorten the wait time, or wait for a more general element
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(3) 
            
            cookies = driver.get_cookies()
            user_agent = driver.execute_script("return navigator.userAgent")
            
            session_cookies = {cookie['name']: cookie['value'] for cookie in cookies}
            headers = {
                "User-Agent": user_agent,
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Origin": "https://coinmarketcap.com",
                "Referer": target_url,
                "Platform": "web",
                "X-Request-Id": str(int(time.time() * 1000))
            }
            
            logger.info(f"successfully obtained{len(session_cookies)} Cookies")
            return session_cookies, headers
        except Exception as e:
            logger.error(f"Selenium Failed to obtain verification information:{e}")
            return None, None
        finally:
            if driver:
                driver.quit()
