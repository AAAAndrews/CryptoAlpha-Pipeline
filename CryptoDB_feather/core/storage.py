import os
import json
import threading
import pandas as pd
from typing import Optional, Dict, Any
from CryptoDataProviders.utils.common import build_kline_filepath

_dbinfo_lock = threading.Lock()

def get_synced_filepath(db_root_path: str, exchange: str, symbol: str, kline_type: str, interval: str) -> str:
    """
    Thread-safely confirms the K-line storage path and synchronizes its metadata into dbinfo.json.
    
    parameter:
        db_root_path (str): Database root directory.
        exchange (str): Exchange name.
        symbol (str): trading pair.
        kline_type (str): K Line type.
        interval (str): time interval.
        
    return:
        str: Normalized file paths.
    """
    file_path = build_kline_filepath(db_root_path, exchange, symbol, kline_type, interval)
    dbinfo_path = os.path.join(db_root_path, "infomation", "dbinfo.json")

    with _dbinfo_lock:
        if not os.path.exists(dbinfo_path):
            os.makedirs(os.path.dirname(dbinfo_path), exist_ok=True)
            with open(dbinfo_path, 'w', encoding='utf-8') as f:
                json.dump({}, f)
        
        try:
            with open(dbinfo_path, 'r', encoding='utf-8') as f:
                dbinfo = json.load(f)
        except (json.JSONDecodeError, IOError):
            dbinfo = {}

        # Ensure nested structures exist and synchronize paths
        d = dbinfo.setdefault(exchange, {}).setdefault(symbol, {}).setdefault(kline_type, {})
        
        if d.get(interval) != file_path:
            d[interval] = file_path
            # Make sure the directory where the data files are located exists
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(dbinfo_path, 'w', encoding='utf-8') as f:
                json.dump(dbinfo, f, indent=4, ensure_ascii=False)
        else:
            # Make sure the physical directory exists even if the path is logged
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
                
    return file_path

def read_feather(file_path: str) -> pd.DataFrame:
    """
    Safely read Feather files. If the file does not exist or the read fails, an empty DataFrame is returned.
    
    parameter:
        file_path (str): File path.
        
    return:
        pd.DataFrame: The data read.
    """
    if os.path.exists(file_path):
        try:
            return pd.read_feather(file_path)
        except Exception as e:
            print(f"Error reading feather file {file_path}: {e}")
            return pd.DataFrame()
    return pd.DataFrame()

def write_feather(file_path: str, df: pd.DataFrame):
    """
    Save the DataFrame as a Feather file and make sure the target directory exists.
    
    parameter:
        file_path (str): Save path.
        df (pd.DataFrame): Data to be saved.
    """
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    df.to_feather(file_path)

def upsert_klines(file_path: str, new_df: pd.DataFrame):
    """
    Merge new candlestick data into existing Feather files (including deduplication and sorting).
    
    parameter:
        file_path (str): Target file path.
        new_df (pd.DataFrame): Newly captured K-line data.
    """
    if new_df.empty:
        return

    original_df = read_feather(file_path)
    
    # Define subset for deduplication (exclude audit columns)
    dedup_cols = [c for c in new_df.columns if c not in ('created_at', 'updated_at')]
    
    if not original_df.empty:
        combined_df = pd.concat([original_df, new_df]).drop_duplicates(subset=dedup_cols)
    else:
        combined_df = new_df
        
    combined_df = combined_df.sort_values("timestamp").reset_index(drop=True)
    write_feather(file_path, combined_df)
