"""
Example 4: Get list of trading pairs
"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.trading_pairs import get_trading_pairs

# Proxy configuration (if no proxy is required, set to None)
PROXY = {
    "http": "http://127.0.0.1:7890",
    "https": "http://127.0.0.1:7890"
}


def example_binance_usdt_pairs():
    """Get Binance USDT Perpetual Contract Trading Pairs"""
    print("=" * 50)
    print("Example 1: Get Binance USDT Perpetual Contract Trading Pair")
    print("=" * 50)
    
    try:
        trading_pairs = get_trading_pairs(
            exchange="binance",
            quote_currency="USDT",
            market_type="swap",
            proxy=None  # If you need a proxy, use PROXY
        )
        
        print(f"\nturn up{len(trading_pairs)} USDT perpetual contract trading pairs")
        print("\nTop 20 trading pairs:")
        for i, pair in enumerate(trading_pairs[:20], 1):
            print(f"  {i}. {pair}")
            
    except Exception as e:
        print(f"mistake:{e}")


def example_binance_spot_pairs():
    """Get Binance spot trading pairs"""
    print("\n" + "=" * 50)
    print("Example 2: Get Binance spot trading pairs")
    print("=" * 50)
    
    try:
        trading_pairs = get_trading_pairs(
            exchange="binance",
            quote_currency="USDT",
            market_type="spot",
            proxy=None
        )
        
        print(f"\nturn up{len(trading_pairs)} spot trading pairs")
        print("\nTop 20 trading pairs:")
        for i, pair in enumerate(trading_pairs[:20], 1):
            print(f"  {i}. {pair}")
            
    except Exception as e:
        print(f"mistake:{e}")


def example_filter_pairs():
    """Filter for specific trading pairs"""
    print("\n" + "=" * 50)
    print("Example 3: Filter trading pairs containing specific keywords")
    print("=" * 50)
    
    try:
        # Get all USDT perpetual contracts
        all_pairs = get_trading_pairs(
            exchange="binance",
            quote_currency="USDT",
            market_type="swap",
            proxy=None
        )
        
        # filter contains"BTC", "ETH", "BNB" trading pair
        keywords = ["BTC", "ETH", "BNB"]
        filtered_pairs = [pair for pair in all_pairs if any(kw in pair for kw in keywords)]
        
        print(f"\nInclude{keywords} There are trading pairs{len(filtered_pairs)} indivual:")
        for pair in filtered_pairs:
            print(f"  - {pair}")
            
    except Exception as e:
        print(f"mistake:{e}")


def example_compare_markets():
    """Compare the number of trading pairs in different markets"""
    print("\n" + "=" * 50)
    print("Example 4: Comparing the number of trading pairs in different markets")
    print("=" * 50)
    
    market_types = ["spot", "swap"]
    
    for market_type in market_types:
        try:
            pairs = get_trading_pairs(
                exchange="binance",
                quote_currency="USDT",
                market_type=market_type,
                proxy=None
            )
            
            print(f"\n{market_type.upper()} market:{len(pairs)} trading pairs")
            
        except Exception as e:
            print(f"\n{market_type.upper()} Market: Failed to get -{e}")


def example_save_to_file():
    """Save trading pair list to file"""
    print("\n" + "=" * 50)
    print("Example 5: Save trading pair list to file")
    print("=" * 50)
    
    try:
        trading_pairs = get_trading_pairs(
            exchange="binance",
            quote_currency="USDT",
            market_type="swap",
            proxy=None
        )
        
        # save to file
        output_file = "binance_usdt_swap_pairs.txt"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"# Binance USDT Perpetual Contract Trading Pair\n")
            f.write(f"# total:{len(trading_pairs)} indivual\n")
            f.write(f"# Generation time:{__import__('datetime').datetime.now()}\n\n")
            for pair in trading_pairs:
                f.write(f"{pair}\n")
        
        print(f"\nList of trading pairs saved to:{output_file}")
        print(f"total:{len(trading_pairs)} trading pairs")
        
    except Exception as e:
        print(f"mistake:{e}")


if __name__ == "__main__":
    # Run all examples
    try:
        example_binance_usdt_pairs()
        example_binance_spot_pairs()
        example_filter_pairs()
        example_compare_markets()
        example_save_to_file()
        
        print("\n" + "=" * 50)
        print("All examples run complete!")
        print("=" * 50)
        
    except Exception as e:
        print(f"\nmistake:{e}")
        import traceback
        traceback.print_exc()
