import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Union
from datetime import datetime
from CryptoDataProviders.utils.common import parse_time, build_kline_filepath

def read_symbol_klines(
    db_root_path: str,
    exchange: str,
    symbol: str,
    kline_type: str,
    interval: str,
    start_time: Optional[Union[int, datetime]] = None,
    end_time: Optional[Union[int, datetime]] = None,
) -> pd.DataFrame:
    """
    Read the Feather file for the specified trading pair and filter based on time range.
    
    parameter:
        db_root_path (str): Database storage root path.
        exchange (str): Exchanges (such as'binance'）。
        symbol (str): Trading pairs (such as'BTCUSDT'）。
        kline_type (str): KLine type (swap, spot, etc.).
        interval (str): KLine period (1h, 1d, etc.).
        start_time (Optional[Union[int, datetime]]): Start time (supports millisecond stamp or datetime object).
        end_time (Optional[Union[int, datetime]]): end time.
        
    return:
        pd.DataFrame: Data frame containing candlestick data. Returns an empty DataFrame if the file does not exist.
    """
    file_path = build_kline_filepath(db_root_path, exchange, symbol, kline_type, interval)
    
    if not os.path.exists(file_path):
        return pd.DataFrame()
    
    try:
        df = pd.read_feather(file_path)
        if df.empty:
            return df
        
        # Add symbol column
        df['symbol'] = symbol
        
        # time filter
        if start_time is not None:
            start_ts = parse_time(start_time)
            df = df[df['timestamp'] >= start_ts]
            
        if end_time is not None:
            end_ts = parse_time(end_time)
            df = df[df['timestamp'] <= end_ts]
            
        return df
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return pd.DataFrame()

def load_multi_klines(
    db_root_path: str,
    exchange: str,
    symbols: Optional[List[str]] = None,
    kline_type: str = "swap",
    interval: str = "1h",
    start_time: Optional[Union[int, datetime]] = None,
    end_time: Optional[Union[int, datetime]] = None,
    num_workers: int = 8
) -> pd.DataFrame:
    """
    Concurrently read the K-line data of multiple trading pairs and merge them into a long format (Long Format) data frame.
    
    parameter:
        db_root_path (str): Database root path.
        exchange (str): Exchange.
        symbols (Optional[List[str]]): List of trading pairs. If None, scan all subdirectories under the exchange directory as trading pairs.
        kline_type (str): KLine type.
        interval (str): cycle.
        start_time (Optional[Union[int, datetime]]): Start time filtering.
        end_time (Optional[Union[int, datetime]]): End time filter.
        num_workers (int): Number of threads to read in parallel.
        
    return:
        pd.DataFrame: All data merged and sorted by time and transaction pairs.
    """
    if symbols is None:
        exchange_path = os.path.join(db_root_path, exchange)
        if os.path.exists(exchange_path):
            symbols = [d for d in os.listdir(exchange_path) if os.path.isdir(os.path.join(exchange_path, d))]
        else:
            return pd.DataFrame()

    all_dfs = []
    
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        future_to_symbol = {
            executor.submit(
                read_symbol_klines, 
                db_root_path, 
                exchange, 
                symbol, 
                kline_type, 
                interval, 
                start_time, 
                end_time
            ): symbol for symbol in symbols
        }
        
        for future in as_completed(future_to_symbol):
            symbol = future_to_symbol[future]
            try:
                df = future.result()
                if not df.empty:
                    all_dfs.append(df)
            except Exception as exc:
                print(f'{symbol} generated an exception: {exc}')
                
    if not all_dfs:
        return pd.DataFrame()
    
    # Merge all dataframes
    combined_df = pd.concat(all_dfs, ignore_index=True)
    
    # Sort by time
    if 'timestamp' in combined_df.columns:
        combined_df.sort_values(['timestamp', 'symbol'], inplace=True)
        combined_df.reset_index(drop=True, inplace=True)
        
    return combined_df
