"""
Example 3: Use Binance Bulk Download to batch download historical data
"""
from datetime import datetime, timezone
import pandas as pd
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from providers.binance_bulk.bulk_fetcher import BinanceBulkFetcher

# Proxy configuration (if no proxy is required, set to None)
PROXY = {
    "http": "http://127.0.0.1:7890",
    "https": "http://127.0.0.1:7890"
}


def example_daily_data():
    """Download daily level historical data"""
    print("=" * 50)
    print("Example 1: Download BTC daily level historical data")
    print("=" * 50)
    
    # Initialize batch scraper
    fetcher = BinanceBulkFetcher(
        asset_type="um",  # USDT-M Perpetual contract
        data_type="klines",
        kline_interval="1h",
        proxy=None  # If you need a proxy, use PROXY
    )
    
    # Download data for the last few days
    start_date = datetime(2024, 12, 1, tzinfo=timezone.utc)
    end_date = datetime(2024, 12, 3, tzinfo=timezone.utc)
    
    print(f"\nDownload time range:{start_date.date()} arrive{end_date.date()}")
    
    data_frames = []
    for batch_df in fetcher.fetch_range(
        symbol="BTCUSDT",
        start_date=start_date,
        end_date=end_date,
        period_type="daily",
        progress=True
    ):
        data_frames.append(batch_df)
    
    if data_frames:
        df = pd.concat(data_frames, ignore_index=True)
        
        print(f"\nGet{len(df)} piece of data")
        print("\nFirst 5 pieces of data:")
        print(df.head())
        print("\nThe last 5 pieces of data:")
        print(df.tail())
        print("\nData information:")
        print(df.info())
    else:
        print("\nNo data obtained")


def example_monthly_data():
    """Download monthly historical data"""
    print("\n" + "=" * 50)
    print("Example 2: Download ETH monthly level historical data")
    print("=" * 50)
    
    # Initialize batch scraper
    fetcher = BinanceBulkFetcher(
        asset_type="um",
        data_type="klines",
        kline_interval="1h",
        proxy=None
    )
    
    # Download data for certain months in 2024
    start_date = datetime(2024, 10, 1, tzinfo=timezone.utc)
    end_date = datetime(2024, 11, 30, tzinfo=timezone.utc)
    
    print(f"\nDownload time range:{start_date.date()} arrive{end_date.date()}")
    
    data_frames = []
    for batch_df in fetcher.fetch_range(
        symbol="ETHUSDT",
        start_date=start_date,
        end_date=end_date,
        period_type="monthly",
        progress=True
    ):
        data_frames.append(batch_df)
    
    if data_frames:
        df = pd.concat(data_frames, ignore_index=True)
        
        print(f"\nGet{len(df)} piece of data")
        print(f"Time range:{df['open_time'].min()} arrive{df['open_time'].max()}")
        print(f"Data days:{(df['open_time'].max() - df['open_time'].min()).days}")
    else:
        print("\nNo data obtained")


def example_different_intervals():
    """Download data at different time intervals"""
    print("\n" + "=" * 50)
    print("Example 3: Download data at different time intervals")
    print("=" * 50)
    
    intervals = ["15m", "1h", "4h"]
    start_date = datetime(2024, 12, 1, tzinfo=timezone.utc)
    end_date = datetime(2024, 12, 1, tzinfo=timezone.utc)
    
    for interval in intervals:
        print(f"\n--- {interval} Interval ---")
        
        fetcher = BinanceBulkFetcher(
            asset_type="um",
            data_type="klines",
            kline_interval=interval,
            proxy=None
        )
        
        data_frames = []
        for batch_df in fetcher.fetch_range(
            symbol="BTCUSDT",
            start_date=start_date,
            end_date=end_date,
            period_type="daily",
            progress=False
        ):
            data_frames.append(batch_df)
        
        if data_frames:
            df = pd.concat(data_frames, ignore_index=True)
            print(f"  - Get{len(df)} piece of data")
        else:
            print(f"  - No data obtained")


def example_spot_data():
    """Download spot data"""
    print("\n" + "=" * 50)
    print("Example 4: Download spot historical data")
    print("=" * 50)
    
    # Initialize batch grabber - spot
    fetcher = BinanceBulkFetcher(
        asset_type="spot",  # Spot goods
        data_type="klines",
        kline_interval="1h",
        proxy=None
    )
    
    start_date = datetime(2024, 12, 1, tzinfo=timezone.utc)
    end_date = datetime(2024, 12, 2, tzinfo=timezone.utc)
    
    print(f"\nDownload spot data:{start_date.date()} arrive{end_date.date()}")
    
    data_frames = []
    for batch_df in fetcher.fetch_range(
        symbol="BTCUSDT",
        start_date=start_date,
        end_date=end_date,
        period_type="daily",
        progress=True
    ):
        data_frames.append(batch_df)
    
    if data_frames:
        df = pd.concat(data_frames, ignore_index=True)
        
        print(f"\nGet{len(df)} piece of data")
        print("\nData overview:")
        print(df[['open_time', 'open', 'high', 'low', 'close', 'volume']].head(10))
    else:
        print("\nNo data obtained")


def example_multiple_symbols():
    """Download multiple trading pairs in batches"""
    print("\n" + "=" * 50)
    print("Example 5: Batch download of multiple trading pairs data")
    print("=" * 50)
    
    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
    start_date = datetime(2024, 12, 1, tzinfo=timezone.utc)
    end_date = datetime(2024, 12, 1, tzinfo=timezone.utc)
    
    fetcher = BinanceBulkFetcher(
        asset_type="um",
        data_type="klines",
        kline_interval="1h",
        proxy=None
    )
    
    all_data = {}
    
    for symbol in symbols:
        print(f"\ndownload{symbol}...")
        
        data_frames = []
        for batch_df in fetcher.fetch_range(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            period_type="daily",
            progress=False
        ):
            data_frames.append(batch_df)
        
        if data_frames:
            df = pd.concat(data_frames, ignore_index=True)
            all_data[symbol] = df
            print(f"  - Get{len(df)} piece of data")
    
    print("\nOverview of data for all trading pairs:")
    for symbol, df in all_data.items():
        print(f"\n{symbol}:")
        print(f"  - Data volume:{len(df)}")
        print(f"  - Price range:{df['close'].min():.2f} - {df['close'].max():.2f}")
        print(f"  - Total trading volume:{df['volume'].sum():.2f}")


if __name__ == "__main__":
    # Run all examples
    try:
        example_daily_data()
        example_monthly_data()
        example_different_intervals()
        example_spot_data()
        example_multiple_symbols()
        
        print("\n" + "=" * 50)
        print("All examples run complete!")
        print("=" * 50)
        print("\nThings to note:")
        print("1. Bulk Download Requires access to Binance S3 bucket")
        print("2. If you encounter network problems, configure a proxy")
        print("3. Historical data files are large and may take some time to download.")
        
    except Exception as e:
        print(f"\nmistake:{e}")
        import traceback
        traceback.print_exc()
