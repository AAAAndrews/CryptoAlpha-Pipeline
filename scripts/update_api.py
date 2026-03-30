"""
Script: Use Binance REST API for incremental updates.
Purpose: Suitable for daily maintenance, synchronizing the K-line data of the latest hours/days.

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
from CryptoDB_feather.core.db_manager import run_binance_rest_updater
from CryptoDB_feather.config import PROXY, DEFAULT_BINANCE_PARAMS

PROXY = None

def main():
    """Main function: perform incremental update"""
    print("=" * 60)
    print("Incremental database updates - using Binance REST API")
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
        "progress": True
    })
    
    # perform update
    print("\nStart updating data...")
    print(f"Number of trading pairs:{len(update_params['symbol_list'])}")
    print(f"time interval:{update_params['interval_list']}")
    print(f"KLine type:{update_params['kline_type_list']}")
    print("-" * 60)
    
    run_binance_rest_updater(**update_params)
    
    print("\n" + "=" * 60)
    print("Update complete!")
    print("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nUser interrupts and exits the program...")
    except Exception as e:
        print(f"\nmistake:{e}")
        import traceback
        traceback.print_exc()
