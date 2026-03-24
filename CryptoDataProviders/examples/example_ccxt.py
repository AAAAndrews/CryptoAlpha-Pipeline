"""
Example 2: Using CCXT multi-exchange interface to obtain data
"""
from datetime import datetime, timezone
import pandas as pd
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from providers.ccxt_api.fetcher import CCXTKlineFetcher

# Proxy configuration (if no proxy is required, set to None)
PROXY = {
    "http": "http://127.0.0.1:7890",
    "https": "http://127.0.0.1:7890"
}


def example_binance_ccxt():
    """Get data from Binance using CCXT"""
    print("=" * 50)
    print("Example 1: Using CCXT to get data from Binance")
    print("=" * 50)
    
    # Initialize the crawler
    fetcher = CCXTKlineFetcher(
        exchange="binance",
        kline_type="swap",
        proxies=None  # If you need a proxy, use PROXY
    )
    
    start_time = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end_time = datetime(2025, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
    
    # Get data
    data_frames = []
    for batch_df in fetcher.fetch_klines(
        symbol="BTC/USDT:USDT",  # CCXT format trading pair
        interval="1h",
        start_time=start_time,
        end_time=end_time,
        batch_size=1000,
        progress=True
    ):
        data_frames.append(batch_df)
    
    df = pd.concat(data_frames, ignore_index=True)
    
    print(f"\nGet{len(df)} piece of data")
    print("\nFirst 5 pieces of data:")
    print(df.head())


def example_okx_ccxt():
    """Get data from OKX using CCXT"""
    print("\n" + "=" * 50)
    print("Example 2: Using CCXT to get data from OKX")
    print("=" * 50)
    
    try:
        # Initialize the crawler
        fetcher = CCXTKlineFetcher(
            exchange="okx",
            kline_type="swap",
            proxies=None
        )
        
        start_time = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        
        # Get data
        data_frames = []
        for batch_df in fetcher.fetch_klines(
            symbol="BTC/USDT:USDT",
            interval="1h",
            start_time=start_time,
            end_time=end_time,
            batch_size=100,  # OKX A maximum of 100 items can be returned each time
            progress=True
        ):
            data_frames.append(batch_df)
        
        df = pd.concat(data_frames, ignore_index=True)
        
        print(f"\nGet{len(df)} piece of data")
        print("\nFirst 5 pieces of data:")
        print(df.head())
        
    except Exception as e:
        print(f"mistake:{e}")
        print("NOTE: Some exchanges may require API keys or proxy access")


def example_multi_exchange():
    """Compare data from multiple exchanges"""
    print("\n" + "=" * 50)
    print("Example 3: Compare BTC prices on multiple exchanges")
    print("=" * 50)
    
    exchanges = ["binance", "okx"]
    start_time = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end_time = datetime(2025, 1, 1, 6, 0, 0, tzinfo=timezone.utc)
    
    results = {}
    
    for exchange_name in exchanges:
        try:
            print(f"\nfrom{exchange_name} Get data...")
            
            fetcher = CCXTKlineFetcher(
                exchange=exchange_name,
                kline_type="swap",
                proxies=None
            )
            
            data_frames = []
            for batch_df in fetcher.fetch_klines(
                symbol="BTC/USDT:USDT",
                interval="1h",
                start_time=start_time,
                end_time=end_time,
                batch_size=100,
                progress=False
            ):
                data_frames.append(batch_df)
            
            df = pd.concat(data_frames, ignore_index=True)
            results[exchange_name] = df
            
            print(f"  - Get{len(df)} piece of data")
            print(f"  - Average closing price:{df['close'].mean():.2f}")
            
        except Exception as e:
            print(f"  - mistake:{e}")
    
    # Compare prices
    if len(results) >= 2:
        print("\nPrice comparison:")
        for i, (exchange, df) in enumerate(results.items()):
            print(f"\n{exchange.upper()}:")
            print(df[['open_time', 'close']].head())


def example_get_symbols():
    """Get the trading pairs supported by the exchange"""
    print("\n" + "=" * 50)
    print("Example 4: Get the trading pairs supported by the exchange")
    print("=" * 50)
    
    try:
        fetcher = CCXTKlineFetcher(
            exchange="binance",
            kline_type="swap",
            proxies=None
        )
        
        symbols = fetcher.get_available_symbols()
        
        print(f"\nBinance There are total perpetual contracts{len(symbols)} trading pairs")
        print("\nTop 20 trading pairs:")
        for symbol in symbols[:20]:
            print(f"  - {symbol}")
            
    except Exception as e:
        print(f"mistake:{e}")


if __name__ == "__main__":
    # Run all examples
    try:
        example_binance_ccxt()
        example_okx_ccxt()
        example_multi_exchange()
        example_get_symbols()
        
        print("\n" + "=" * 50)
        print("All examples run complete!")
        print("=" * 50)
        
    except Exception as e:
        print(f"\nmistake:{e}")
        import traceback
        traceback.print_exc()
