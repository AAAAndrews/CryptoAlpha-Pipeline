"""
Script: Use Binance official S3 bucket to batch download historical data.
Purpose: Suitable for establishing a database for the first time or for historical data that needs to be synchronized over a long time range (such as several years).

rely:
- CryptoDataProviders: Data source interface
- CryptoDB_feather.core: storage layer
"""
import sys
import os
from datetime import datetime, timezone
from copy import deepcopy

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

# Import the CryptoDB_feather storage layer and configuration
from CryptoDB_feather.core.bulk_manager import run_bulk_updater
from CryptoDB_feather.config import PROXY, DEFAULT_BINANCE_PARAMS


def main():
    """Main function: execute batch download"""
    print("=" * 60)
    print("Database Bulk Download - Using Binance S3 Bulk Download")
    print("=" * 60)
    
    # Use CryptoDataProviders to get a list of trading pairs
    print("\nRetrieving trading pair list...")
    trading_pairs_list = get_trading_pairs(
        exchange="binance",
        quote_currency="USDT",
        market_type="swap",
        proxy=PROXY
    )
    print(f"turn up{len(trading_pairs_list)} trading pairs")
    
    # Configure update parameters
    update_params = deepcopy(DEFAULT_BINANCE_PARAMS)
    update_params.update({
        "symbol_list": trading_pairs_list,
        "interval_list": ["1h"],  # Update 1 hour K line
        "kline_type_list": ["swap"],  # Perpetual contract
        "proxy": PROXY,
        "max_workers": 16,  # Number of concurrent threads
        "batch_size": 100,  # batch size
        "single_threaded": False,  # Enable multithreading mode
    })
    
    # Perform batch download
    print("\nStart batch download...")
    print(f"Number of trading pairs:{len(update_params['symbol_list'])}")
    print(f"time interval:{update_params['interval_list']}")
    print(f"KLine type:{update_params['kline_type_list']}")
    print(f"Concurrent threads:{update_params['max_workers']}")
    print("-" * 60)

    run_bulk_updater(**update_params)

    print("\n" + "=" * 60)
    print("Batch download completed!")
    print("=" * 60)

    # 返回下载统计 / Return download stats for pipeline summary
    return {
        "pairs": len(trading_pairs_list),
        "intervals": update_params["interval_list"],
        "kline_type": update_params["kline_type_list"],
    }


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nUser interrupts and exits the program...")
    except Exception as e:
        print(f"\nmistake:{e}")
        import traceback
        traceback.print_exc()
