"""
Task 20 验证测试: calc_short_only_curve
Validate: shape, type, start value, no NaN, edge cases, parameter validation.
"""

import sys
import traceback
import numpy as np
import pandas as pd

# 确保项目根目录在 sys.path / ensure project root on sys.path
sys.path.insert(0, "f:/MyCryptoTrading/CryptoAlpha/CryptoAlpha-Pipeline")

passed = 0
failed = 0


def check(name: str, condition: bool):
    global passed, failed
    if condition:
        passed += 1
        print(f"  [PASS] {name}")
    else:
        failed += 1
        print(f"  [FAIL] {name}")


def make_factor_returns(n_dates=100, n_symbols=20, seed=42):
    """生成测试用的因子和收益率数据 / Generate synthetic factor and returns data."""
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2024-01-01", periods=n_dates, freq="D")
    symbols = [f"S{i:03d}" for i in range(n_symbols)]
    idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])
    factor = pd.Series(rng.standard_normal(len(idx)), index=idx)
    returns = pd.Series(rng.standard_normal(len(idx)) * 0.02, index=idx)
    return factor, returns


def make_good_factor(n_dates=100, n_symbols=20, seed=99):
    """
    生成"好"因子：因子值与未来收益负相关（做空最低组应有正收益）。
    Generate a "good" factor: factor negatively correlated with future returns
    (shorting bottom group should yield positive returns).
    """
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2024-01-01", periods=n_dates, freq="D")
    symbols = [f"S{i:03d}" for i in range(n_symbols)]
    idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])
    # 因子值高 → 收益高，因子值低 → 收益低 → 做空低组应有正收益
    # High factor → high returns, low factor → low returns → shorting low group yields positive
    factor = pd.Series(rng.standard_normal(len(idx)), index=idx)
    noise = pd.Series(rng.standard_normal(len(idx)) * 0.005, index=idx)
    returns = factor * 0.03 + noise
    return factor, returns


print("=" * 60)
print("Task 20 验证: calc_short_only_curve")
print("=" * 60)

# --- 1. Import ---
print("\n[1] Import 测试")
try:
    from FactorAnalysis.portfolio import calc_short_only_curve
    check("import OK", True)
except Exception as e:
    check(f"import failed: {e}", False)
    traceback.print_exc()

# --- 2. Public export ---
print("\n[2] Public export 测试")
try:
    import FactorAnalysis
    check("calc_short_only_curve in __all__", "calc_short_only_curve" in FactorAnalysis.__all__)
    check("module-level import OK", hasattr(FactorAnalysis, "calc_short_only_curve"))
except Exception as e:
    check(f"public export failed: {e}", False)

# --- 3. Basic shape & type ---
print("\n[3] Shape & type 测试")
factor, returns = make_factor_returns()
try:
    curve = calc_short_only_curve(factor, returns, n_groups=5, bottom_k=1)
    check("returns pd.Series", isinstance(curve, pd.Series))
    check("shape matches dates", len(curve) == 100)
    check("index is DatetimeIndex", isinstance(curve.index, pd.DatetimeIndex))
    check("dtype is float", curve.dtype in [np.float64, float])
except Exception as e:
    check(f"shape/type failed: {e}", False)
    traceback.print_exc()

# --- 4. Start value ---
print("\n[4] Start value 测试")
try:
    check("start value == 1.0", curve.iloc[0] == 1.0)
except Exception as e:
    check(f"start value check failed: {e}", False)

# --- 5. No NaN ---
print("\n[5] No NaN 测试")
try:
    check("no NaN in curve", not curve.isna().any())
    check("all finite", np.isfinite(curve).all())
except Exception as e:
    check(f"no-NaN check failed: {e}", False)

# --- 6. bottom_k=2 ---
print("\n[6] bottom_k=2 变体测试")
try:
    curve2 = calc_short_only_curve(factor, returns, n_groups=5, bottom_k=2)
    check("bottom_k=2 returns pd.Series", isinstance(curve2, pd.Series))
    check("bottom_k=2 shape", len(curve2) == 100)
    check("bottom_k=2 no NaN", not curve2.isna().any())
    check("bottom_k=2 start == 1.0", curve2.iloc[0] == 1.0)
except Exception as e:
    check(f"bottom_k=2 failed: {e}", False)
    traceback.print_exc()

# --- 7. bottom_k=5 (all groups) ---
print("\n[7] bottom_k=5 (全部组) 测试")
try:
    curve5 = calc_short_only_curve(factor, returns, n_groups=5, bottom_k=5)
    check("bottom_k=5 returns pd.Series", isinstance(curve5, pd.Series))
    check("bottom_k=5 no NaN", not curve5.isna().any())
    check("bottom_k=5 start == 1.0", curve5.iloc[0] == 1.0)
except Exception as e:
    check(f"bottom_k=5 failed: {e}", False)
    traceback.print_exc()

# --- 8. ValueError on invalid bottom_k ---
print("\n[8] 参数校验测试")
try:
    calc_short_only_curve(factor, returns, n_groups=5, bottom_k=0)
    check("bottom_k=0 raises ValueError", False)
except ValueError:
    check("bottom_k=0 raises ValueError", True)
except Exception as e:
    check(f"bottom_k=0 wrong exception: {e}", False)

try:
    calc_short_only_curve(factor, returns, n_groups=5, bottom_k=6)
    check("bottom_k=6 raises ValueError", False)
except ValueError:
    check("bottom_k=6 raises ValueError", True)
except Exception as e:
    check(f"bottom_k=6 wrong exception: {e}", False)

# --- 9. Good factor → short-only curve positive ---
print("\n[9] 好因子做空收益测试")
good_factor, good_returns = make_good_factor()
try:
    short_curve = calc_short_only_curve(good_factor, good_returns, n_groups=5, bottom_k=1)
    check("short curve final > 1.0", short_curve.iloc[-1] > 1.0)
except Exception as e:
    check(f"good factor short failed: {e}", False)
    traceback.print_exc()

# --- 10. All-NaN factor → flat curve ---
print("\n[10] 全 NaN 因子测试")
nan_factor = pd.Series(np.nan, index=factor.index, dtype=np.float64)
try:
    nan_curve = calc_short_only_curve(nan_factor, returns, n_groups=5, bottom_k=1)
    check("all-NaN curve is flat 1.0", (nan_curve == 1.0).all())
except Exception as e:
    check(f"all-NaN failed: {e}", False)
    traceback.print_exc()

# --- 11. Single symbol ---
print("\n[11] 单标的边界测试")
dates = pd.date_range("2024-01-01", periods=20, freq="D")
single_idx = pd.MultiIndex.from_product([dates, ["ONLY"]], names=["timestamp", "symbol"])
single_factor = pd.Series(np.arange(20, dtype=np.float64), index=single_idx)
single_returns = pd.Series(np.random.default_rng(0).standard_normal(20) * 0.01, index=single_idx)
try:
    single_curve = calc_short_only_curve(single_factor, single_returns, n_groups=2, bottom_k=1)
    check("single symbol curve OK", isinstance(single_curve, pd.Series) and len(single_curve) == 20)
    check("single symbol no NaN", not single_curve.isna().any())
except Exception as e:
    check(f"single symbol failed: {e}", False)
    traceback.print_exc()

# --- 12. Short vs Long symmetry check ---
print("\n[12] 多空对称性检查")
# 用 bottom_k=1 做空 vs top_k=1 做多，在随机因子上两者应该不同
try:
    long_curve = calc_short_only_curve.__module__ and __import__(
        "FactorAnalysis.portfolio", fromlist=["calc_long_only_curve"]
    ).calc_long_only_curve(factor, returns, n_groups=5, top_k=1)
    short_curve = calc_short_only_curve(factor, returns, n_groups=5, bottom_k=1)
    # 随机因子上两者不应完全相同
    check("short != long (random factor)", not np.allclose(long_curve.values, short_curve.values))
except Exception as e:
    check(f"symmetry check failed: {e}", False)
    traceback.print_exc()

print("\n" + "=" * 60)
print(f"结果: {passed} passed, {failed} failed (total {passed + failed})")
print("=" * 60)
sys.exit(0 if failed == 0 else 1)
