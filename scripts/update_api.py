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
from CryptoDataProviders.utils.trading_pairs import validate_active_trading_pairs

# Import the CryptoDB_feather storage layer and configuration
from CryptoDB_feather.core.db_manager import run_binance_rest_updater
from CryptoDB_feather.config import DB_ROOT_PATH, PROXY, DEFAULT_BINANCE_PARAMS

PROXY = None

def main():
    """Main function: perform incremental update"""
    print("=" * 60)
    print("Incremental database updates - using Binance REST API")
    print("=" * 60)

    # 使用活跃交易对校验工具获取有效交易对列表 / Use active trading pair validator to get valid pairs
    print("\nValidating active trading pairs...")
    active_pairs, local_pairs, valid_pairs = validate_active_trading_pairs(
        db_root_path=DB_ROOT_PATH,
        exchange="binance",
        proxy=PROXY
    )
    print(f"API active pairs: {len(active_pairs)}")
    print(f"Local DB pairs: {len(local_pairs)}")
    print(f"Valid pairs (active AND local): {len(valid_pairs)}")

    # Configure update parameters
    update_params = deepcopy(DEFAULT_BINANCE_PARAMS)
    update_params.update({
        "symbol_list": list(valid_pairs),  # 仅更新有效交易对 / Only update valid pairs
        "interval_list": ["1h"],  # Update 1 hour K line / 更新1小时K线
        "kline_type_list": ["swap"],  # Perpetual contract / 永续合约
        "proxy": PROXY,
        "progress": True
    })

    # perform update / 执行更新
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
