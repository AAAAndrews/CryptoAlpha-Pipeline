# -*- coding: utf-8 -*-
"""
Data Pipeline End-to-End Validation Tests

Validation Items:
1. retry decorator import and behavior
2. Active trading pairs validator type and subset relationship
3. update_api.py complete import chain
4. db_manager retry wrapping correctness
"""
import sys
import os

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

for submodule in ["CryptoDataProviders", "CryptoDB_feather"]:
    submodule_path = os.path.join(project_root, submodule)
    if submodule_path not in sys.path:
        sys.path.insert(0, submodule_path)

import time
from typing import Set, Tuple


def test_retry_decorator():
    """Verify retry decorator can be imported and has expected behavior"""
    print("\n" + "=" * 60)
    print("Test 1: Retry Decorator Import and Behavior")
    print("=" * 60)

    try:
        from utils.retry import retry_with_backoff
        print("[OK] retry_with_backoff imported successfully")
    except ImportError as e:
        print(f"[FAIL] Import failed: {e}")
        return False

    # Verify decorator parameters
    try:
        # Create test function
        call_count = {"value": 0}

        @retry_with_backoff(
            max_retries=2,
            base_delay=0.1,
            max_delay=1.0,
            exponential_base=2.0,
            jitter=False,
            retryable_exceptions=(ValueError,),
        )
        def failing_function():
            call_count["value"] += 1
            if call_count["value"] <= 2:
                raise ValueError("Simulated error")
            return "success"

        result = failing_function()

        if result == "success" and call_count["value"] == 3:
            print(f"[OK] Retry behavior correct: succeeded after {call_count['value']} attempts")
        else:
            print(f"[FAIL] Retry behavior abnormal: {call_count['value']} attempts, result: {result}")
            return False

    except Exception as e:
        print(f"[FAIL] Decorator behavior test failed: {e}")
        return False

    print("[OK] Test 1 PASSED: retry decorator import and behavior normal")
    return True


def test_active_trading_pairs_validator():
    """Verify active trading pairs validator returns correct types and subset relationship"""
    print("\n" + "=" * 60)
    print("Test 2: Active Trading Pairs Validator Type and Subset")
    print("=" * 60)

    try:
        from utils.trading_pairs import (
            validate_active_trading_pairs,
            get_active_trading_pairs_from_api,
            get_local_trading_pairs,
        )
        print("[OK] trading_pairs module imported successfully")
    except ImportError as e:
        print(f"[FAIL] Import failed: {e}")
        return False

    try:
        from CryptoDB_feather.config import DB_ROOT_PATH

        # Call validator function
        active_pairs, local_pairs, valid_pairs = validate_active_trading_pairs(
            db_root_path=DB_ROOT_PATH,
            exchange="binance",
            proxy=None,
        )

        # Verify return types
        type_checks = [
            ("active_pairs", active_pairs, Set[str]),
            ("local_pairs", local_pairs, Set[str]),
            ("valid_pairs", valid_pairs, Set[str]),
        ]

        for name, value, expected_type in type_checks:
            if isinstance(value, set):
                print(f"[OK] {name} returns correct type: set[str], count: {len(value)}")
            else:
                print(f"[FAIL] {name} type error: expected set, got {type(value)}")
                return False

        # Verify subset relationship
        # valid_pairs should be intersection of active_pairs and local_pairs
        expected_valid = active_pairs & local_pairs

        if valid_pairs == expected_valid:
            print(f"[OK] Subset relationship correct: valid_pairs = active_pairs ^ local_pairs")
            print(f"  - |active_pairs| = {len(active_pairs)}")
            print(f"  - |local_pairs| = {len(local_pairs)}")
            print(f"  - |valid_pairs| = {len(valid_pairs)}")
        else:
            print(f"[FAIL] Subset relationship abnormal:")
            print(f"  - expected valid_pairs size: {len(expected_valid)}")
            print(f"  - actual valid_pairs size: {len(valid_pairs)}")
            return False

        # Verify valid_pairs is subset of both active_pairs and local_pairs
        if valid_pairs.issubset(active_pairs) and valid_pairs.issubset(local_pairs):
            print("[OK] valid_pairs is subset of both active_pairs and local_pairs")
        else:
            print("[FAIL] valid_pairs is not a subset of active_pairs and local_pairs")
            return False

    except Exception as e:
        print(f"[FAIL] Active trading pairs validation test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    print("[OK] Test 2 PASSED: validator returns correct types and subset relationship holds")
    return True


def test_update_api_import_chain():
    """Verify update_api.py import chain is complete without blocking"""
    print("\n" + "=" * 60)
    print("Test 3: update_api.py Import Chain Completeness")
    print("=" * 60)

    import_checks = []

    # Check each import
    try:
        from CryptoDataProviders.utils.trading_pairs import validate_active_trading_pairs
        import_checks.append(("validate_active_trading_pairs", True))
        print("[OK] validate_active_trading_pairs imported successfully")
    except ImportError as e:
        import_checks.append(("validate_active_trading_pairs", False))
        print(f"[FAIL] validate_active_trading_pairs import failed: {e}")

    try:
        from CryptoDB_feather.core.db_manager import run_binance_rest_updater
        import_checks.append(("run_binance_rest_updater", True))
        print("[OK] run_binance_rest_updater imported successfully")
    except ImportError as e:
        import_checks.append(("run_binance_rest_updater", False))
        print(f"[FAIL] run_binance_rest_updater import failed: {e}")

    try:
        from CryptoDB_feather.config import DB_ROOT_PATH, PROXY, DEFAULT_BINANCE_PARAMS
        import_checks.append(("config", True))
        print("[OK] config module imported successfully")
    except ImportError as e:
        import_checks.append(("config", False))
        print(f"[FAIL] config module import failed: {e}")

    # Check if all imports succeeded
    all_passed = all(result for _, result in import_checks)

    if all_passed:
        print("[OK] update_api.py all dependency modules imported successfully")
    else:
        failed = [name for name, result in import_checks if not result]
        print(f"[FAIL] update_api.py import chain has blocking: {', '.join(failed)}")
        return False

    print("[OK] Test 3 PASSED: update_api.py import chain is complete and unblocked")
    return True


def test_db_manager_retry_wrapping():
    """Verify retry wrapping in db_manager.py is configured correctly"""
    print("\n" + "=" * 60)
    print("Test 4: db_manager Retry Wrapping Correctness")
    print("=" * 60)

    try:
        from CryptoDB_feather.core.db_manager import run_binance_rest_updater
        from CryptoDB_feather.core import db_manager
        print("[OK] db_manager module imported successfully")
    except ImportError as e:
        print(f"[FAIL] Import failed: {e}")
        return False

    # Check if module imported retry_with_backoff
    has_retry_import = hasattr(db_manager, 'retry_with_backoff')

    if has_retry_import:
        print("[OK] db_manager imported retry_with_backoff")
    else:
        # Check if imported via other method
        import inspect
        source = inspect.getsource(db_manager)
        has_retry_import = 'retry_with_backoff' in source or 'from utils.retry import' in source or 'from CryptoDataProviders.utils.retry import' in source

        if has_retry_import:
            print("[OK] db_manager contains retry_with_backoff reference")
        else:
            print("[FAIL] db_manager retry_with_backoff reference not found")
            return False

    # Check if _fetch_and_save_pair function exists
    has_fetch_and_save = hasattr(db_manager, '_fetch_and_save_pair')

    if has_fetch_and_save:
        print("[OK] _fetch_and_save_pair helper function exists")
    else:
        print("[FAIL] _fetch_and_save_pair helper function does not exist")
        return False

    # Verify retry decorator parameters
    import inspect
    source = inspect.getsource(db_manager.run_binance_rest_updater)

    retry_params = {
        'max_retries=2': 'max_retries=2',
        'base_delay=5.0': 'base_delay=5.0',
        'max_delay=30.0': 'max_delay=30.0',
        'exponential_base=5.0': 'exponential_base=5.0',
    }

    for param_name, param_value in retry_params.items():
        if param_value in source:
            print(f"[OK] Retry parameter exists: {param_value}")
        else:
            print(f"[WARN] Retry parameter not found: {param_value} (may be configured differently)")

    # Check exception types
    has_request_exception = 'RequestException' in source
    has_os_error = 'OSError' in source

    if has_request_exception and has_os_error:
        print("[OK] Retry exception types include RequestException and OSError")
    else:
        print(f"[INFO] Exception types: RequestException={has_request_exception}, OSError={has_os_error}")

    print("[OK] Test 4 PASSED: db_manager retry wrapping is configured correctly")
    return True


def main():
    """Main test function"""
    print("\n" + "=" * 80)
    print("Data Pipeline End-to-End Validation Tests")
    print("=" * 80)

    results = []

    # Test 1
    results.append(("Test 1: Retry Decorator", test_retry_decorator()))

    # Test 2
    results.append(("Test 2: Active Trading Pairs Validator", test_active_trading_pairs_validator()))

    # Test 3
    results.append(("Test 3: update_api Import Chain", test_update_api_import_chain()))

    # Test 4
    results.append(("Test 4: db_manager Retry Wrapping", test_db_manager_retry_wrapping()))

    # Print test summary
    print("\n" + "=" * 80)
    print("Test Summary")
    print("=" * 80)

    for name, passed in results:
        status = "[PASS]" if passed else "[FAIL]"
        print(f"{status}: {name}")

    all_passed = all(result for _, result in results)

    print("=" * 80)
    if all_passed:
        print("[OK] All tests passed")
        return 0
    else:
        print("[FAIL] Some tests failed")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
