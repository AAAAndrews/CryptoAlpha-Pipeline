"""
Example 1: Use Binance REST API to obtain K-line data
"""
from datetime import datetime, timezone
import pandas as pd
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from providers.binance_api.market_api import fetch_klines

# Proxy configuration (if no proxy is required, set to None)
PROXY = {
    "http": "http://127.0.0.1:7890",
    "https": "http://127.0.0.1:7890"
}

def example_fetch_spot_klines():
    """Get spot K-line data"""
    print("=" * 50)
    print("Example 1: Get BTC spot 1-hour K-line data")
    print("=" * 50)
    
    start_time = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end_time = datetime(2025, 1, 3, 0, 0, 0, tzinfo=timezone.utc)
    
    data_frames = []
    for batch_df in fetch_klines(
        symbol="BTCUSDT",
        interval="1h",
        start_time=start_time,
        end_time=end_time,
        kline_type="spot",  # Spot goods
        batch_size=1000,
        progress=True,
        proxy=None  # If you need a proxy, use PROXY
    ):
        data_frames.append(batch_df)
    
    # Merge all batch data
    df = pd.concat(data_frames, ignore_index=True)
    
    print(f"\nGet{len(df)} piece of data")
    print("\nFirst 5 pieces of data:")
    print(df.head())
    print("\nData column:")
    print(df.columns.tolist())
    print("\nData information:")
    print(df.info())


def example_fetch_swap_klines():
    """Get perpetual contract K-line data"""
    print("\n" + "=" * 50)
    print("Example 2: Get ETH perpetual contract 1-hour K-line data")
    print("=" * 50)
    
    start_time = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end_time = datetime(2025, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
    
    data_frames = []
    for batch_df in fetch_klines(
        symbol="ETHUSDT",
        interval="1h",
        start_time=start_time,
        end_time=end_time,
        kline_type="swap",  # Perpetual contract
        batch_size=1000,
        progress=True,
        proxy=None
    ):
        data_frames.append(batch_df)
    
    df = pd.concat(data_frames, ignore_index=True)
    
    print(f"\nGet{len(df)} piece of data")
    print("\nFirst 5 pieces of data:")
    print(df.head())


def example_fetch_mark_price():
    """Get mark price data"""
    print("\n" + "=" * 50)
    print("Example 3: Get BTC token price data")
    print("=" * 50)
    
    start_time = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    
    data_frames = []
    for batch_df in fetch_klines(
        symbol="BTCUSDT",
        interval="1h",
        start_time=start_time,
        end_time=end_time,
        kline_type="mark",  # mark price
        batch_size=1000,
        progress=True,
        proxy=None
    ):
        data_frames.append(batch_df)
    
    df = pd.concat(data_frames, ignore_index=True)
    
    print(f"\nGet{len(df)} piece of data")
    print("\nTag price data:")
    print(df[['open_time', 'open', 'high', 'low', 'close']].head(10))


def example_multiple_symbols():
    """Get data for multiple trading pairs"""
    print("\n" + "=" * 50)
    print("Example 4: Get multiple trading pair data in batches")
    print("=" * 50)
    
    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
    start_time = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end_time = datetime(2025, 1, 1, 6, 0, 0, tzinfo=timezone.utc)
    
    all_data = {}
    
    for symbol in symbols:
        print(f"\nget{symbol} data...")
        data_frames = []
        for batch_df in fetch_klines(
            symbol=symbol,
            interval="1h",
            start_time=start_time,
            end_time=end_time,
            kline_type="swap",
            batch_size=1000,
            progress=False,
            proxy=None
        ):
            data_frames.append(batch_df)
        
        df = pd.concat(data_frames, ignore_index=True)
        all_data[symbol] = df
        print(f"  - Get{len(df)} piece of data")
    
    print("\nOverview of data for all trading pairs:")
    for symbol, df in all_data.items():
        print(f"\n{symbol}:")
        print(f"  Time range:{df['open_time'].min()} arrive{df['open_time'].max()}")
        print(f"  Price range:{df['close'].min():.2f} - {df['close'].max():.2f}")
        print(f"  Total trading volume:{df['volume'].sum():.2f}")


if __name__ == "__main__":
    # Run all examples
    try:
        example_fetch_spot_klines()
        example_fetch_swap_klines()
        example_fetch_mark_price()
        example_multiple_symbols()
        
        print("\n" + "=" * 50)
        print("All examples run complete!")
        print("=" * 50)
        
    except Exception as e:
        print(f"\nmistake:{e}")
        import traceback
        traceback.print_exc()
