# marketdata_api.py
from .cons import *
from datetime import datetime, timedelta
import requests
import requests.exceptions
import pandas as pd
from tqdm import tqdm
from .utils import _format_data
from utils.common import parse_time, log_error_to_json
from utils.retry import retry_with_backoff
import time
import json
import os


def log_error(symbol: str, interval: str, kline_type: str, error_msg: str):
    """Log error information for failed operations in market_api."""
    error_dir = os.path.join(os.path.dirname(__file__), "errors")
    error_entry = {
        "symbol": symbol,
        "interval": interval,
        "kline_type": kline_type,
        "error_message": str(error_msg)
    }
    log_error_to_json(error_dir, error_entry)


@retry_with_backoff(
    max_retries=2,
    base_delay=5.0,
    max_delay=30.0,
    exponential_base=5.0,
    jitter=True,
    retryable_exceptions=(requests.exceptions.RequestException,),
)
def _request_kline_batch(klines_url, params, headers, proxy):
    """
    发送单次K线数据HTTP请求 / Send a single HTTP request for kline data.

    由 retry_with_backoff 装饰器提供自动重试，遇到网络异常或特定状态码时自动退避重试。
    Automatic retry is provided by the retry_with_backoff decorator — backs off on
    network errors or specific HTTP status codes.

    参数 / Parameters:
        klines_url: K线API端点 / Kline API endpoint URL.
        params: 请求参数字典 / Request parameters dict.
        headers: 请求头 / Request headers.
        proxy: 代理配置 / Proxy configuration dict.

    返回 / Returns:
        list: 原始K线数据列表 / Raw kline data list from API.
    """
    # 服务端时间校验 / Server time verification
    time_res = requests.get(
        "https://api.binance.com/api/v3/time",
        headers=headers, timeout=10, proxies=proxy,
    )
    server_time = time_res.json()['serverTime']

    # 请求K线数据 / Request kline data
    response = requests.get(
        klines_url, params=params, headers=headers,
        timeout=30, proxies=proxy,
    )

    # 特定状态码检查：触发重试 / Specific status codes that trigger retry
    if response.status_code == 502:
        raise requests.exceptions.HTTPError("502 Bad Gateway")
    if response.status_code == 429:
        raise requests.exceptions.HTTPError("429 Too Many Requests - rate limited")

    response.raise_for_status()
    return response.json()


def fetch_klines(symbol, interval, start_time=None, end_time=None, limit=None, progress=True, batch_size=1000, kline_type="index",proxy:dict=None):
    """
    Loop through Binance REST API to capture K-line data. Supports spot, perpetual, index and mark prices.
    Use generator mode to yield data in batches.
    
    parameter:
        symbol (str): trading pair.
        interval (str): interval.
        start_time (Any): Start time (supports multiple formats).
        end_time (Any): end time.
        limit (Optional[int]): Total quantity limit.
        progress (bool): Whether to display the tqdm progress bar.
        batch_size (int): Maximum amount per request (API limits are typically 1000 or 1500).
        kline_type (str): Kline type ('spot', 'swap', 'mark', 'index')。
        proxy (dict): Agent Dictionary.
        
    return:
        Generator[pd.DataFrame]: Generate one batch of cleaned DataFrames at a time.
    """
    kline_type = kline_type.lower()
    assert kline_type in ["spot","swap","mark","index"]

    kline_type_dct = {
        "spot":spot_klines_url,
        "swap":derivatives_swap_klines_url,
        "mark":derivatives_mark_price_url,
        "index":derivatives_price_index_url
    }

    klines_url = kline_type_dct[kline_type]
    # Parameter preprocessing
    params = {
        'symbol': symbol,
        "pair":symbol,
        'interval': interval,
        'limit': batch_size  # Use batch_size as limit per request
    }
    if kline_type == "spot" or kline_type == "mark":
        params.pop('pair', None)  # Spot K-line does not require pair parameters
    elif kline_type == "swap":
        params["contractType"] = "PERPETUAL"  # Perpetual contract K-line needs to specify the contract type
        params.pop('symbol', None)  # Continuous and exponential K-lines do not require the symbol parameter
    else:
        params.pop('symbol', None)  # Continuous and exponential K-lines do not require the symbol parameter
    
    # Time conversion (make sure end_time contains the complete K line)
    now = datetime.utcnow()
    if end_time:
        end_ms = parse_time(end_time)
    else:
        # The default end time is the start time of the current complete K line
        end_ms = parse_time(now - timedelta(minutes=1))
    
    if start_time:
        start_ms = parse_time(start_time)
    else:
        start_ms = None

    # initialize variables
    total_collected = 0
    headers = {'Accept-Encoding': 'gzip'}

    # Progress bar initialization
    pbar = tqdm(total=limit, disable=not progress, 
               desc=f"Fetching {symbol} {interval}", unit='kline')
    
    try:
        while True:
            # Set time parameters
            if start_ms:
                params['startTime'] = start_ms
            params['endTime'] = end_ms

            # 使用带重试装饰器的请求函数 / Use decorated request function with retry
            try:
                data = _request_kline_batch(klines_url, params, headers, proxy)
            except requests.exceptions.RequestException as e:
                log_error(symbol, interval, kline_type, f"请求失败，已达最大重试次数: {str(e)}")
                break

            if not data:
                break

            # Processing time boundaries
            last_ts = data[-1][0]
            
            # Check if end_time limit is exceeded
            if last_ts > end_ms:
                data = [d for d in data if d[0] <= end_ms]
                if not data:
                    break
            
            # Format and yield data batches
            batch_df = _format_data(data, timeformat=False)
            yield batch_df

            collected = len(data)
            total_collected += collected
            
            # Update progress bar
            if limit:
                pbar.update(collected)
                if total_collected >= limit:
                    break
            else:
                pbar.update(collected)

            # Check if there is any follow-up data
            if len(data) < batch_size:
                break
                
            # Update the starting time of the next request (use the closing time of the last K line + 1ms)
            start_ms = data[-1][6] + 1
            
            # Terminate when end_time is reached
            if start_ms > end_ms:
                break
                
            # Speed ​​limit control
            time.sleep(0.2)
    
    except Exception as e:
        log_error(symbol, interval, kline_type, f"An error occurred during data acquisition:{str(e)}")
        print(f"An error occurred during data acquisition:{str(e)}")

    finally:
        pbar.close()

#Test data connectivity
def test_data_connection():
    response = requests.get(test_url)
    if response.status_code == 200:
        print("Data connection successful")
    else:
        print("Data connection failed")



