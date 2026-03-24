"""
Test script to verify that scripts work correctly.

This script will:
1. Check whether dependencies can be imported
2. Test CryptoDataProviders interface
3. Test CryptoDB_feather integration
4. Validate key configuration values
"""
import os
import sys

# Add project root to sys.path.
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Add submodule roots to support internal relative imports.
for submodule in ["CryptoDataProviders", "CryptoDB_feather"]:
    submodule_path = os.path.join(project_root, submodule)
    if submodule_path not in sys.path:
        sys.path.insert(0, submodule_path)


def test_imports():
    """Test module imports."""
    print("=" * 60)
    print("Test 1: module imports")
    print("=" * 60)

    try:
        from CryptoDataProviders.utils.trading_pairs import get_trading_pairs  # noqa: F401
        print("OK: imported CryptoDataProviders.utils.trading_pairs")

        from CryptoDB_feather.core.db_manager import run_binance_rest_updater  # noqa: F401
        print("OK: imported CryptoDB_feather.core.db_manager")

        from CryptoDB_feather.core.bulk_manager import run_bulk_updater  # noqa: F401
        print("OK: imported CryptoDB_feather.core.bulk_manager")

        from CryptoDB_feather.config import DB_ROOT_PATH, DEFAULT_BINANCE_PARAMS, PROXY  # noqa: F401
        print("OK: imported CryptoDB_feather.config")

        print("\nAll import checks passed")
        return True

    except Exception as exc:
        print(f"\nFAILED: import check error: {exc}")
        import traceback

        traceback.print_exc()
        return False


def test_trading_pairs():
    """Test fetching trading pairs."""
    print("\n" + "=" * 60)
    print("Test 2: fetch trading pairs")
    print("=" * 60)

    try:
        from CryptoDataProviders.utils.trading_pairs import get_trading_pairs

        print("\nFetching Binance USDT perpetual symbols...")
        pairs = get_trading_pairs(
            exchange="binance",
            quote_currency="USDT",
            market_type="swap",
            proxy=None,
        )

        print(f"OK: fetched {len(pairs)} symbols")
        print("\nFirst 10 symbols:")
        for index, pair in enumerate(pairs[:10], 1):
            print(f"  {index}. {pair}")

        print("\nTrading-pair check passed")
        return True

    except Exception as exc:
        print(f"\nFAILED: trading-pair fetch error: {exc}")
        print("Note: this may be a network issue; check proxy settings")
        return False


def test_config():
    """Test configuration values."""
    print("\n" + "=" * 60)
    print("Test 3: configuration")
    print("=" * 60)

    try:
        from CryptoDB_feather.config import DB_ROOT_PATH, DEFAULT_BINANCE_PARAMS, PROXY

        print(f"\nDB_ROOT_PATH: {DB_ROOT_PATH}")
        print(f"PROXY: {PROXY if PROXY else 'not set'}")
        print(f"DEFAULT_BINANCE_PARAMS: {DEFAULT_BINANCE_PARAMS}")

        if os.path.exists(DB_ROOT_PATH):
            print("OK: database path exists")
        else:
            print("WARNING: database path does not exist yet; it may be created on first run")

        print("\nConfiguration check passed")
        return True

    except Exception as exc:
        print(f"\nFAILED: configuration check error: {exc}")
        return False


def test_scripts_exists():
    """Test whether required script files exist."""
    print("\n" + "=" * 60)
    print("Test 4: required script files")
    print("=" * 60)

    scripts_dir = os.path.join(project_root, "scripts")
    required_scripts = [
        "update_api.py",
        "update_bulk.py",
        "cleanup_fake_data.py",
        "README.md",
    ]

    all_exist = True
    for script in required_scripts:
        script_path = os.path.join(scripts_dir, script)
        if os.path.exists(script_path):
            print(f"OK: {script} exists")
        else:
            print(f"FAILED: {script} is missing")
            all_exist = False

    if all_exist:
        print("\nAll required script files exist")

    return all_exist


def main():
    """Run all tests."""
    print("\n" + "=" * 60)
    print("Scripts Functional Test")
    print("=" * 60 + "\n")

    results = []

    results.append(("Module imports", test_imports()))
    results.append(("Trading-pair fetch", test_trading_pairs()))
    results.append(("Configuration", test_config()))
    results.append(("Script files", test_scripts_exists()))

    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)

    for test_name, passed in results:
        status = "PASS" if passed else "FAIL"
        print(f"{test_name:20s}: {status}")

    all_passed = all(passed for _, passed in results)

    if all_passed:
        print("\n" + "=" * 60)
        print("All tests passed. scripts are ready to use.")
        print("=" * 60)
        print("\nNext steps:")
        print("1. Read scripts/README.md for detailed usage")
        print("2. Run the update script you need")
        print("3. For first-time setup, run update_bulk.py")
    else:
        print("\n" + "=" * 60)
        print("Some tests failed. Check the error messages above.")
        print("=" * 60)
        print("\nCommon issues:")
        print("1. Import failure: verify project path")
        print("2. Trading-pair failure: verify network and proxy")
        print("3. Config failure: verify CryptoDB_feather/config.py")

    return all_passed


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
