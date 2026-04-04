"""
FactorLib 集成验证测试 / FactorLib integration validation tests

验证 BaseFactor 抽象类、Alpha1/Alpha2 因子、registry 注册表和公共导出。
Validates BaseFactor ABC, Alpha1/Alpha2 factors, registry, and public exports.
"""

import os
import sys

# 将项目根目录添加到 sys.path / add project root to sys.path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import warnings

import numpy as np
import pandas as pd

passed = 0
failed = 0


def check(name: str, condition: bool) -> None:
    """断言辅助 / assertion helper"""
    global passed, failed
    if condition:
        passed += 1
        print(f"  [PASS] {name}")
    else:
        failed += 1
        print(f"  [FAIL] {name}")


# ---------------------------------------------------------------------------
# 构造合成数据 / build synthetic data
# ---------------------------------------------------------------------------
np.random.seed(42)
n_rows = 100
symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
timestamps = pd.date_range("2025-01-01", periods=n_rows, freq="1h")

rows = []
for sym in symbols:
    base_price = {"BTCUSDT": 50000, "ETHUSDT": 3000, "SOLUSDT": 150}[sym]
    prices = base_price * (1 + np.random.randn(n_rows).cumsum() * 0.002)
    for i, ts in enumerate(timestamps):
        close = round(prices[i], 2)
        rows.append({
            "timestamp": ts,
            "symbol": sym,
            "open": round(close * (1 + np.random.randn() * 0.001), 2),
            "high": round(close * (1 + abs(np.random.randn()) * 0.005), 2),
            "low": round(close * (1 - abs(np.random.randn()) * 0.005), 2),
            "close": close,
        })

data = pd.DataFrame(rows)

# ---------------------------------------------------------------------------
# 1. BaseFactor 抽象类不可实例化 / BaseFactor ABC not instantiable
# ---------------------------------------------------------------------------
print("\n--- BaseFactor ABC ---")

try:
    from FactorLib.base import BaseFactor
    check("BaseFactor import OK", True)
except Exception as e:
    check("BaseFactor import OK", False)

try:
    BaseFactor()  # type: ignore[abstract]
    check("BaseFactor 不可实例化", False)
except TypeError:
    check("BaseFactor 不可实例化", True)


# ---------------------------------------------------------------------------
# 2. Alpha1 动量因子 / Alpha1 Momentum Factor
# ---------------------------------------------------------------------------
print("\n--- Alpha1 Momentum ---")

from FactorLib.alpha_momentum import AlphaMomentum

mom = AlphaMomentum(lookback=10)
check("AlphaMomentum 继承 BaseFactor", isinstance(mom, BaseFactor))
check("AlphaMomentum repr 包含 lookback", "lookback=10" in repr(mom))

mom_result = mom.calculate(data)
check("AlphaMomentum 返回 pd.Series", isinstance(mom_result, pd.Series))
check("AlphaMomentum Series 长度 = 数据长度", len(mom_result) == len(data))
check("AlphaMomentum Series name 正确", mom_result.name == "AlphaMomentum(lookback=10)")

# 前 10 个 timestamp（每个 symbol）应为 NaN
nan_count = mom_result.isna().sum()
expected_nan = 10 * len(symbols)  # lookback × symbols
check(f"AlphaMomentum NaN 数量 ({nan_count}) = lookback × symbols ({expected_nan})",
      nan_count == expected_nan)

# 非空值数值合理性 / non-null values are reasonable
non_null = mom_result.dropna()
check("AlphaMomentum 非空值数量 > 0", len(non_null) > 0)
check("AlphaMomentum 非空值无 inf", np.isfinite(non_null).all())


# ---------------------------------------------------------------------------
# 3. Alpha2 波动率因子 / Alpha2 Volatility Factor
# ---------------------------------------------------------------------------
print("\n--- Alpha2 Volatility ---")

from FactorLib.alpha_volatility import AlphaVolatility

vol = AlphaVolatility(lookback=20)
check("AlphaVolatility 继承 BaseFactor", isinstance(vol, BaseFactor))
check("AlphaVolatility repr 包含 lookback", "lookback=20" in repr(vol))

vol_result = vol.calculate(data)
check("AlphaVolatility 返回 pd.Series", isinstance(vol_result, pd.Series))
check("AlphaVolatility Series 长度 = 数据长度", len(vol_result) == len(data))
check("AlphaVolatility Series name 正确", vol_result.name == "AlphaVolatility(lookback=20)")

# 前 20 个 timestamp（每个 symbol）应为 NaN（pct_change 产生 1 个 NaN + rolling 需要 19 个额外值）
# pct_change → 1 NaN, rolling(min_periods=lookback) → lookback-1 个额外 NaN, 共 lookback 个 NaN per symbol
vol_nan_count = vol_result.isna().sum()
expected_vol_nan = 20 * len(symbols)  # lookback × symbols
check(f"AlphaVolatility NaN 数量 ({vol_nan_count}) = lookback × symbols ({expected_vol_nan})",
      vol_nan_count == expected_vol_nan)

# 波动率非负 / volatility is non-negative
vol_non_null = vol_result.dropna()
check("AlphaVolatility 非空值数量 > 0", len(vol_non_null) > 0)
check("AlphaVolatility 非空值无 inf", np.isfinite(vol_non_null).all())
check("AlphaVolatility 值全部 >= 0", (vol_non_null >= 0).all())


# ---------------------------------------------------------------------------
# 4. Registry 注册表 / Registry
# ---------------------------------------------------------------------------
print("\n--- Registry ---")

from FactorLib.registry import register, list_factors, get, clear

# 清空后开始测试 / clear before testing
clear()
check("清空注册表后为空", list_factors() == [])

# 注册 AlphaMomentum
register(AlphaMomentum)
check("注册 AlphaMomentum 后列表长度 = 1", len(list_factors()) == 1)
check("列表包含 'AlphaMomentum'", "AlphaMomentum" in list_factors())

# 注册 AlphaVolatility
register(AlphaVolatility)
check("注册 AlphaVolatility 后列表长度 = 2", len(list_factors()) == 2)
check("列表包含 'AlphaVolatility'", "AlphaVolatility" in list_factors())

# get 获取
cls = get("AlphaMomentum")
check("get('AlphaMomentum') 返回正确类", cls is AlphaMomentum)
check("get('AlphaMomentum') 可实例化并计算", isinstance(cls(lookback=5).calculate(data), pd.Series))

# get 不存在返回 None
check("get('NonExistent') 返回 None", get("NonExistent") is None)

# 重复注册警告
with warnings.catch_warnings(record=True) as w:
    warnings.simplefilter("always")
    register(AlphaMomentum)
    check("重复注册产生 UserWarning", len(w) > 0 and issubclass(w[0].category, UserWarning))

# 非 BaseFactor 子类注册抛出 TypeError
try:
    register(int)  # type: ignore[arg-type]
    check("注册非 BaseFactor 抛出 TypeError", False)
except TypeError:
    check("注册非 BaseFactor 抛出 TypeError", True)

# 清理 / cleanup
clear()
check("clear 后列表为空", list_factors() == [])


# ---------------------------------------------------------------------------
# 5. __init__.py 公共导出 / Public exports from __init__.py
# ---------------------------------------------------------------------------
print("\n--- __init__.py Exports ---")

import FactorLib

# 检查所有导出名称都存在于模块中 / check all exported names exist in module
expected_exports = [
    "BaseFactor", "AlphaMomentum", "AlphaPriceRange", "AlphaVolatility",
    "register", "list_factors", "get", "clear",
]
for name in expected_exports:
    check(f"FactorLib.{name} 存在", hasattr(FactorLib, name))

# 检查 __all__ 列表完整 / check __all__ list is complete
check("__all__ 包含所有导出", set(FactorLib.__all__) == set(expected_exports))


# ---------------------------------------------------------------------------
# 结果汇总 / summary
# ---------------------------------------------------------------------------
print(f"\n{'='*50}")
print(f"  总计: {passed + failed}  通过: {passed}  失败: {failed}")
print(f"{'='*50}")

if __name__ == "__main__":
    if failed > 0:
        raise SystemExit(1)
