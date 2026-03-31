"""Task 21 验证: calc_top_bottom_curve 多空对冲净值曲线 / Task 21 verification: top-bottom hedged equity curve"""

import numpy as np
import pandas as pd
from FactorAnalysis import calc_top_bottom_curve, calc_long_only_curve, calc_short_only_curve

passed = 0
failed = 0

def check(name, condition):
    global passed, failed
    if condition:
        passed += 1
        print(f"  PASS: {name}")
    else:
        failed += 1
        print(f"  FAIL: {name}")


# --- 构造合成数据 / build synthetic data ---
np.random.seed(42)
dates = pd.date_range("2024-01-01", periods=60, freq="D")
symbols = ["BTC", "ETH", "SOL", "DOGE", "AVAX", "ADA", "DOT", "MATIC"]
n = len(dates) * len(symbols)
idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])

# 因子值：带趋势的随机值 / factor values: random with trend
factor_values = np.random.randn(n) * 0.1
factor = pd.Series(factor_values, index=idx, name="factor")

# 收益率：与因子有正相关 / returns: correlated with factor
returns_values = factor_values * 0.05 + np.random.randn(n) * 0.01
returns = pd.Series(returns_values, index=idx, name="returns")

print("=== 1. Import & Export ===")
check("import calc_top_bottom_curve", callable(calc_top_bottom_curve))
from FactorAnalysis import __all__
check("__all__ contains calc_top_bottom_curve", "calc_top_bottom_curve" in __all__)
# module-level import
from FactorAnalysis.portfolio import calc_top_bottom_curve as _ctbc
check("module-level import OK", _ctbc is calc_top_bottom_curve)

print("\n=== 2. Basic Shape & Type ===")
curve = calc_top_bottom_curve(factor, returns, n_groups=4, top_k=1, bottom_k=1)
check("returns pd.Series", isinstance(curve, pd.Series))
check("index is DatetimeIndex", isinstance(curve.index, pd.DatetimeIndex))
check("length == 60", len(curve) == 60)
check("dtype is float", curve.dtype in [np.float64, float])
check("start value == 1.0", curve.iloc[0] == 1.0)
check("no NaN", curve.isna().sum() == 0)
check("all finite", np.all(np.isfinite(curve)))

print("\n=== 3. Positive Factor → Hedged Curve > 1.0 ===")
# 因子与收益正相关，多空对冲应盈利 / correlated factor → hedged should profit
check("hedged curve ends > 1.0", curve.iloc[-1] > 1.0)

print("\n=== 4. Hedge > Long and Hedge > Short (positive factor) ===")
long_curve = calc_long_only_curve(factor, returns, n_groups=4, top_k=1)
short_curve = calc_short_only_curve(factor, returns, n_groups=4, bottom_k=1)
# 正相关因子 → 多空对冲应优于纯多和纯空 / positive factor → hedge > long & > short
check("hedge final > long final", curve.iloc[-1] >= long_curve.iloc[-1])
check("hedge final > short final", curve.iloc[-1] >= short_curve.iloc[-1])

print("\n=== 5. top_k / bottom_k Variants ===")
c1 = calc_top_bottom_curve(factor, returns, n_groups=4, top_k=2, bottom_k=1)
check("top_k=2, bottom_k=1 OK", len(c1) == 60 and c1.iloc[0] == 1.0)

c2 = calc_top_bottom_curve(factor, returns, n_groups=4, top_k=1, bottom_k=2)
check("top_k=1, bottom_k=2 OK", len(c2) == 60 and c2.iloc[0] == 1.0)

c3 = calc_top_bottom_curve(factor, returns, n_groups=5, top_k=1, bottom_k=1)
check("n_groups=5, top_k=1, bottom_k=1 OK", len(c3) == 60 and c3.iloc[0] == 1.0)

print("\n=== 6. ValueError on Invalid Params ===")
try:
    calc_top_bottom_curve(factor, returns, n_groups=4, top_k=0, bottom_k=1)
    check("top_k=0 raises ValueError", False)
except ValueError:
    check("top_k=0 raises ValueError", True)

try:
    calc_top_bottom_curve(factor, returns, n_groups=4, top_k=1, bottom_k=0)
    check("bottom_k=0 raises ValueError", False)
except ValueError:
    check("bottom_k=0 raises ValueError", True)

try:
    calc_top_bottom_curve(factor, returns, n_groups=4, top_k=3, bottom_k=2)
    check("top_k+bottom_k > n_groups raises ValueError", False)
except ValueError:
    check("top_k+bottom_k > n_groups raises ValueError", True)

print("\n=== 7. All-NaN Factor → Flat Curve ===")
nan_factor = pd.Series(np.nan, index=factor.index)
flat = calc_top_bottom_curve(nan_factor, returns, n_groups=4, top_k=1, bottom_k=1)
check("all-NaN → flat curve 1.0", (flat == 1.0).all())

print("\n=== 8. Single-Symbol Edge Case ===")
single_idx = pd.MultiIndex.from_product(
    [pd.date_range("2024-01-01", periods=10, freq="D"), ["BTC"]],
    names=["timestamp", "symbol"],
)
single_factor = pd.Series(np.random.randn(10), index=single_idx)
single_returns = pd.Series(np.random.randn(10) * 0.01, index=single_idx)
single_curve = calc_top_bottom_curve(single_factor, single_returns, n_groups=2, top_k=1, bottom_k=1)
check("single-symbol curve OK", len(single_curve) == 10 and single_curve.iloc[0] == 1.0)
check("single-symbol no NaN", single_curve.isna().sum() == 0)

print(f"\n{'='*40}")
print(f"Total: {passed} passed, {failed} failed")
if failed > 0:
    raise SystemExit(1)
