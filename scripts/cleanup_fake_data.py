"""
Script: Clean up false data of delisted trading pairs in the database.
Purpose: Identify and clean up false data at the end (OHLC is equal and volume is 0), especially for delisted trading pairs.

rely:
- CryptoDataProviders: Get list of active trading pairs
- CryptoDB_feather.config: Database configuration
"""
import os
import sys
import json
import pandas as pd
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional

# Add project root directory to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Add CryptoDataProviders and CryptoDB_feather to path to support internal relative imports
for submodule in ["CryptoDataProviders", "CryptoDB_feather"]:
    submodule_path = os.path.join(project_root, submodule)
    if submodule_path not in sys.path:
        sys.path.insert(0, submodule_path)

# Import CryptoDataProviders data source interface
from CryptoDataProviders.utils.trading_pairs import get_trading_pairs

# Import CryptoDB_feather configuration
from CryptoDB_feather.config import DB_ROOT_PATH, PROXY


def get_active_symbols(kline_type: str = "swap", proxy: Optional[dict] = None) -> List[str]:
    """
    Use CryptoDataProviders to get the list of trading pairs currently online on Binance
    
    parameter:
        kline_type: Kline type ('spot' or'swap')
        proxy: Agent configuration
        
    return:
        List of active trading pairs
    """
    try:
        market_type = kline_type if kline_type in ["spot", "swap"] else "swap"
        symbols = get_trading_pairs(
            exchange="binance",
            quote_currency="USDT",
            market_type=market_type,
            proxy=proxy
        )
        return symbols
    except Exception as e:
        print(f"Failed to obtain trading pairs using CryptoDataProviders:{e}")
        print("Try an alternative method...")
        return get_active_symbols_fallback(kline_type, proxy)


def get_active_symbols_fallback(kline_type: str, proxy: Optional[dict] = None) -> List[str]:
    """Alternate method: Get the list of trading pairs directly from the Binance API"""
    from CryptoDataProviders.providers.binance_api.cons import spot_exchange_info_url, derivatives_exchange_info_url
    
    url = spot_exchange_info_url if kline_type == "spot" else derivatives_exchange_info_url
    try:
        response = requests.get(url, proxies=proxy, timeout=10)
        data = response.json()
        if kline_type == "spot":
            return [s['symbol'] for s in data['symbols'] if s['status'] == 'TRADING']
        else:
            # For derivatives, check if status is TRADING
            return [s['symbol'] for s in data['symbols'] 
                    if s.get('status') == 'TRADING' or s.get('contractStatus') == 'TRADING']
    except Exception as e:
        print(f"The alternative method also fails:{e}")
        return []


def identify_fake_data_end(df: pd.DataFrame) -> int:
    """
    identify the end of"false"data.
    Fake data characteristics: OHLC is equal and the trading volume is 0.
    
    parameter:
        df: KLine DataDataFrame
        
    return:
        The index of the last valid data. If all data is false, -1 is returned.
    """
    if df.empty:
        return -1
    
    # Check if required column exists
    required_cols = ['open', 'high', 'low', 'close', 'volume']
    if not all(col in df.columns for col in required_cols):
        print(f"  Warning: Required columns are missing, available columns:{df.columns.tolist()}")
        return len(df) - 1
    
    # Identifying fake data: OHLC is equal and volume is 0
    is_fake = (
        (df['open'] == df['high']) & 
        (df['high'] == df['low']) & 
        (df['low'] == df['close']) & 
        (df['volume'] == 0)
    )
    
    # Find the first index that is not false data from back to front
    idx = len(df) - 1
    while idx >= 0 and is_fake.iloc[idx]:
        idx -= 1
    
    return idx


def process_symbol_cleanup(
    db_root_path: str, 
    exchange_id: str, 
    symbol: str, 
    kline_type: str, 
    interval: str,
    active_symbols: List[str],
    dry_run: bool = False
) -> Optional[str]:
    """
    Cleaning logic for handling individual trading pairs
    
    parameter:
        db_root_path: Database root path
        exchange_id: Exchange ID
        symbol: trading pair symbol
        kline_type: KLine type
        interval: time interval
        active_symbols: List of active trading pairs
        dry_run: Whether it is in drill mode (without actually deleting data)
        
    return:
        Operation result description, if there is no operation, return None
    """
    file_path = os.path.join(
        db_root_path, exchange_id, symbol, kline_type, interval, "klines.feather"
    )
    
    if not os.path.exists(file_path):
        return None

    try:
        df = pd.read_feather(file_path)
        if df.empty:
            return None

        last_valid_idx = identify_fake_data_end(df)
        
        # If there is false data at the end
        if last_valid_idx < len(df) - 1:
            is_active = symbol in active_symbols
            fake_count = len(df) - 1 - last_valid_idx
            
            # Decision: whether to truncate data
            should_truncate = False
            reason = ""
            
            if not is_active:
                # Not in the transaction list, confirmed to have been removed from the shelves
                should_truncate = True
                reason = "Removed"
            else:
                # In active list but with fake data at the end
                # May be extremely low liquidity or temporary maintenance, retain data to prevent accidental deletion
                should_truncate = False
                reason = "Still trading, data retained"
            
            if should_truncate:
                action = "【drill] will" if dry_run else "already"
                new_df = df.iloc[:last_valid_idx + 1].copy()
                
                if not dry_run:
                    new_df.to_feather(file_path)
                
                return (f"{action}clean up{symbol} ({reason}): "
                       f"{len(df)} -> {len(new_df)} row (remove{fake_count} false data)")
            else:
                return f"jump over{symbol} ({reason}): There is at the end{fake_count} Suspicious data, but not processed yet"
        
        return None
        
    except Exception as e:
        if "Not an Arrow file" in str(e) or "feather" in str(e).lower():
            return f"❌ {symbol}: File corruption -{e}。It is recommended to delete manually."
        return f"❌ {symbol}: Handling error -{e}"


def run_cleanup(
    kline_type: str = "swap", 
    interval: str = "1h",
    dry_run: bool = True,
    max_workers: int = 10
):
    """
    Perform data cleansing
    
    parameter:
        kline_type: Kline type ('spot' or'swap')
        interval: time interval
        dry_run: Whether it is practice mode (True=without actually deleting)
        max_workers: Number of concurrent threads
    """
    exchange_id = "binance"
    
    print("=" * 70)
    print(f"Database Cleaning Tool -{exchange_id.upper()} {kline_type.upper()} {interval}")
    print("=" * 70)
    
    if dry_run:
        print("⚠️  Currently in drill mode, the data will not be actually modified.")
        print("   For actual cleaning, set dry_run=False")
    else:
        print("⚠️  WARNING: The database file will actually be modified!")
        print("   It is recommended to run the drill mode first (dry_run=True）See the impact")
    
    print("\n" + "-" * 70)
    
    # 1. Get the current online trading pair
    print(f"\n[1/4] Getting list of active trading pairs...")
    active_symbols = get_active_symbols(kline_type, PROXY)
    
    if not active_symbols:
        print("❌ Error: Unable to get list of active trading pairs.")
        print("   To prevent accidental deletion of data, the cleaning operation is suspended.")
        return
    
    print(f"✓ turn up{len(active_symbols)} active trading pairs")
    
    # 2. Scan local directory
    print(f"\n[2/4] Scanning local database...")
    exchange_path = os.path.join(DB_ROOT_PATH, exchange_id)
    
    if not os.path.exists(exchange_path):
        print(f"❌ Path does not exist:{exchange_path}")
        return

    all_symbols = [
        d for d in os.listdir(exchange_path) 
        if os.path.isdir(os.path.join(exchange_path, d))
    ]
    print(f"✓ turn up{len(all_symbols)} local trading pairs")
    
    # 3. parallel processing
    print(f"\n[3/4] Processing data... (Number of concurrency:{max_workers})")
    results = []
    processed = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                process_symbol_cleanup, 
                DB_ROOT_PATH, 
                exchange_id, 
                sym, 
                kline_type, 
                interval, 
                active_symbols,
                dry_run
            ): sym
            for sym in all_symbols
        }
        
        for future in as_completed(futures):
            processed += 1
            if processed % 100 == 0:
                print(f"  Processed:{processed}/{len(all_symbols)}")
            
            res = future.result()
            if res:
                results.append(res)
                print(f"  {res}")

    # 4. Output summary
    print(f"\n[4/4] Cleanup completed")
    print("-" * 70)
    print(f"\nSummarize:")
    print(f"  Scan trading pairs:{len(all_symbols)}")
    print(f"  Number of active trading pairs:{len(active_symbols)}")
    print(f"  Need to deal with:{len(results)}")
    
    if dry_run:
        print(f"\n⚠️  This is the result of walkthrough mode, no data was actually modified")
        print(f"   For actual cleaning, set dry_run=False")
    else:
        print(f"\n✓ The data has actually been modified")

    # 返回清理统计 / Return cleanup stats for pipeline summary
    return {
        "scanned": len(all_symbols),
        "active": len(active_symbols),
        "cleaned": len(results),
    }


def main():
    """main function"""
    # Walkthrough mode: first see what data will be cleaned
    print("\nIt is recommended to use the drill mode for the first run:\n")
    run_cleanup(
        kline_type="swap",  # 'swap' or'spot'
        interval="1h",      # '1m', '5m', '1h', '4h', '1d' wait
        dry_run=True,       # True=Practice mode, False=Actual cleanup
        max_workers=10      # Number of concurrent threads
    )
    
    # Uncomment below to perform the actual cleanup (do this with caution!)
    # print("\n\n")
    # response = input("Are you sure you want to actually clean the data? (yes/no):")
    # if response.lower() == 'yes':
    #     run_cleanup(
    #         kline_type="swap",
    #         interval="1h",
    #         dry_run=False,
    #         max_workers=10
    #     )
    # else:
    #     print("Actual cleanup canceled")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nUser interrupts and exits the program...")
    except Exception as e:
        print(f"\nmistake:{e}")
        import traceback
        traceback.print_exc()
