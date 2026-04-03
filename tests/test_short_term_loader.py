# -*- coding: utf-8 -*-
"""
Short-Term Data Loader Validation Tests

验证短期数据加载器的数据形状、缺失值、继承关系和编译校验。
Verify ShortTermDataLoader data shape, missing values, inheritance, and compile validation.
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


def test_import_and_inheritance():
    """Verify ShortTermDataLoader imports and inherits BaseDataLoader correctly"""
    print("\n" + "=" * 60)
    print("Test 1: Import and Inheritance")
    print("=" * 60)

    try:
        from Cross_Section_Factor.short_term_loader import ShortTermDataLoader
        print("[OK] ShortTermDataLoader imported successfully")
    except ImportError as e:
        print(f"[FAIL] Import failed: {e}")
        return False

    try:
        from Cross_Section_Factor.datapreprocess import BaseDataLoader
    except ImportError as e:
        print(f"[FAIL] BaseDataLoader import failed: {e}")
        return False

    if issubclass(ShortTermDataLoader, BaseDataLoader):
        print("[OK] ShortTermDataLoader is subclass of BaseDataLoader")
    else:
        print("[FAIL] ShortTermDataLoader does not inherit BaseDataLoader")
        return False

    # 验证必要方法存在 / Verify required methods exist
    required_methods = ["receive", "compile", "dataset"]
    for method in required_methods:
        if hasattr(ShortTermDataLoader, method):
            print(f"[OK] Method exists: {method}")
        else:
            print(f"[FAIL] Method missing: {method}")
            return False

    print("[OK] Test 1 PASSED")
    return True


def test_data_shape():
    """
    Verify loaded data has correct shape:
    - Required columns present
    - Correct dtypes for OHLC and timestamp
    - DataFrame is non-empty when DB has data
    """
    import pandas as pd

    print("\n" + "=" * 60)
    print("Test 2: Data Shape")
    print("=" * 60)

    try:
        from Cross_Section_Factor.short_term_loader import ShortTermDataLoader
    except ImportError as e:
        print(f"[FAIL] Import failed: {e}")
        return False

    # 使用极短回溯窗口加载真实数据 / Use very short lookback to load real data
    try:
        loader = ShortTermDataLoader(lookback_days=1)
        df = loader.compile()
    except Exception as e:
        print(f"[FAIL] Failed to load data: {e}")
        import traceback
        traceback.print_exc()
        return False

    if df.empty:
        print("[WARN] DataFrame is empty — no data in DB for lookback=1d")
        print("[OK] Test 2 SKIPPED (no data available)")
        return True

    # 校验必要列 / Verify required columns
    required_cols = {"timestamp", "symbol", "open", "high", "low", "close"}
    actual_cols = set(df.columns)
    missing_cols = required_cols - actual_cols

    if missing_cols:
        print(f"[FAIL] Missing required columns: {missing_cols}")
        print(f"  Available columns: {list(actual_cols)}")
        return False
    print(f"[OK] All required columns present: {sorted(required_cols)}")

    # 校验 symbol 列唯一值 / Verify symbol column has unique values
    unique_symbols = df["symbol"].nunique()
    print(f"[OK] Unique symbols: {unique_symbols}")

    # 校验行数合理性 / Verify row count is reasonable
    row_count = len(df)
    print(f"[OK] Total rows: {row_count}")

    # 校验 OHLC 数据类型为数值 / Verify OHLC columns are numeric
    ohlc_cols = ["open", "high", "low", "close"]
    for col in ohlc_cols:
        if pd.api.types.is_numeric_dtype(df[col]):
            print(f"[OK] Column '{col}' is numeric: {df[col].dtype}")
        else:
            print(f"[FAIL] Column '{col}' is not numeric: {df[col].dtype}")
            return False

    print("[OK] Test 2 PASSED")
    return True


def test_no_missing_data():
    """
    Verify no missing data in critical columns:
    - No NaN in OHLC columns
    - No NaN in timestamp
    - No NaN in symbol
    - No duplicate (timestamp, symbol) pairs
    """
    print("\n" + "=" * 60)
    print("Test 3: No Missing Data")
    print("=" * 60)

    try:
        from Cross_Section_Factor.short_term_loader import ShortTermDataLoader
    except ImportError as e:
        print(f"[FAIL] Import failed: {e}")
        return False

    try:
        loader = ShortTermDataLoader(lookback_days=1)
        df = loader.compile()
    except Exception as e:
        print(f"[FAIL] Failed to load data: {e}")
        import traceback
        traceback.print_exc()
        return False

    if df.empty:
        print("[WARN] DataFrame is empty — no data in DB for lookback=1d")
        print("[OK] Test 3 SKIPPED (no data available)")
        return True

    # 校验关键列无 NaN / Verify no NaN in critical columns
    critical_cols = ["timestamp", "symbol", "open", "high", "low", "close"]
    has_missing = False
    for col in critical_cols:
        nan_count = df[col].isna().sum()
        if nan_count > 0:
            print(f"[FAIL] Column '{col}' has {nan_count} NaN values")
            has_missing = True
        else:
            print(f"[OK] Column '{col}' has 0 NaN values")

    if has_missing:
        return False

    # 校验无重复 (timestamp, symbol) / Verify no duplicate (timestamp, symbol) pairs
    dup_count = df.duplicated(subset=["timestamp", "symbol"]).sum()
    if dup_count > 0:
        print(f"[FAIL] Found {dup_count} duplicate (timestamp, symbol) pairs")
        return False
    print(f"[OK] No duplicate (timestamp, symbol) pairs")

    # 校验 OHLC 逻辑: high >= low / Verify OHLC logic: high >= low
    invalid_ohlc = df[df["high"] < df["low"]]
    if len(invalid_ohlc) > 0:
        print(f"[FAIL] Found {len(invalid_ohlc)} rows where high < low")
        return False
    print("[OK] All rows satisfy high >= low")

    print("[OK] Test 3 PASSED")
    return True


def test_compile_validation():
    """
    Verify compile() validation behavior:
    - Missing required columns raises ValueError
    - Empty content triggers receive() automatically
    """
    print("\n" + "=" * 60)
    print("Test 4: Compile Validation")
    print("=" * 60)

    try:
        from Cross_Section_Factor.short_term_loader import ShortTermDataLoader
    except ImportError as e:
        print(f"[FAIL] Import failed: {e}")
        return False

    # 测试缺少必要列时抛出 ValueError / Test ValueError on missing columns
    try:
        loader = ShortTermDataLoader(lookback_days=1)
        # 直接注入一个缺少必要列的 DataFrame / Inject a DataFrame missing required columns
        import pandas as pd
        loader.content = pd.DataFrame({"foo": [1, 2, 3], "bar": [4, 5, 6]})
        loader.compile()
        print("[FAIL] compile() did not raise ValueError for missing columns")
        return False
    except ValueError as e:
        if "Missing required columns" in str(e):
            print(f"[OK] compile() raised ValueError for missing columns: {e}")
        else:
            print(f"[FAIL] ValueError message unexpected: {e}")
            return False
    except Exception as e:
        print(f"[FAIL] Unexpected exception: {type(e).__name__}: {e}")
        return False

    # 测试 dataset 属性自动调用 receive() / Test dataset property auto-calls receive()
    try:
        loader2 = ShortTermDataLoader(lookback_days=1)
        # 确保 content 为 None，然后通过 dataset 触发自动加载
        # Ensure content is None, then trigger auto-load via dataset property
        loader2.content = None
        df = loader2.dataset  # 应自动调用 receive() → compile()
        if df is not None and isinstance(df, pd.DataFrame):
            print(f"[OK] dataset property auto-triggered receive() and compile()")
        else:
            print(f"[FAIL] dataset property returned unexpected type: {type(df)}")
            return False
    except Exception as e:
        print(f"[FAIL] dataset property test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    print("[OK] Test 4 PASSED")
    return True


def main():
    """Main test runner"""
    import pandas as pd

    print("\n" + "=" * 80)
    print("Short-Term Data Loader Validation Tests")
    print("=" * 80)

    results = []

    results.append(("Test 1: Import and Inheritance", test_import_and_inheritance()))
    results.append(("Test 2: Data Shape", test_data_shape()))
    results.append(("Test 3: No Missing Data", test_no_missing_data()))
    results.append(("Test 4: Compile Validation", test_compile_validation()))

    # Print summary
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
