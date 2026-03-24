# utils.py
import pandas as pd
from datetime import datetime
from utils.common import parse_time

def _format_data(raw_data, timeformat=False):
    """
    Convert raw list data returned from the Binance REST API into a formatted Pandas DataFrame.
    
    parameter:
        raw_data (list): API The original 2D list returned.
        timeformat (bool): Whether to convert timestamps to datetime objects (default retains original millisecond stamps).
        
    return:
        pd.DataFrame: DataFrame with type conversion and column naming.
    """
    columns = [
        'open_time', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_volume', 'trades',
        'taker_buy_base', 'taker_buy_quote',"ignore"
    ]
    
    df = pd.DataFrame(raw_data, columns=columns)
    
    # Convert data type
    numeric_cols = ['open', 'high', 'low', 'close', 'volume', 
                   'quote_volume', 'taker_buy_base', 'taker_buy_quote']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    df['trades'] = pd.to_numeric(df['trades'], errors='coerce').fillna(0).astype(int)
    df['ignore'] = pd.to_numeric(df['ignore'], errors='coerce').fillna(0).astype(int)
    
    # time formatting
    if timeformat:
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
        df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
    
    return df