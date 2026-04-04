"""
Task 9 测试 — calc_ic 向量化数值一致性
Vectorized calc_ic numerical consistency test.

对比向量化 calc_ic 与逐截面 groupby.apply 参考实现，
验证 6 种 mock 场景 + 极端信号 + NaN 数据 + 小数据集的数值一致性。
Compare vectorized calc_ic vs per-cross-section groupby.apply reference,
verifying 6 mock scenarios + extreme signals + NaN + small dataset consistency.
"""

import sys
import numpy as np
import pandas as pd

sys.path.insert(0, ".")

from FactorAnalysis.metrics import calc_ic
from tests.mutual_components.conftest_perf import (
    iter_scenarios,
    assert_series_close,
    assert_scalar_close,
)

PASS = 0
FAIL = 0


def check(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  [PASS] {name}")
    else:
        FAIL += 1
        print(f"  [FAIL] {name} — {detail}")


def calc_ic_reference(factor: pd.Series, returns: pd.Series) -> pd.Series:
    """
    逐截面 groupby.apply 参考实现（优化前逻辑）/ Reference implementation (pre-optimization logic).

    用于对比向量化实现的数值一致性。
    For comparing numerical consistency with vectorized implementation.
    """
    df = pd.DataFrame({"factor": factor, "returns": returns})

    def _pearson(g: pd.DataFrame) -> float:
        if len(g) < 2:
            return np.nan
        f = g["factor"]
        r = g["returns"]
        mask = f.notna() & r.notna() & np.isfinite(f) & np.isfinite(r)
        if mask.sum() < 2:
            return np.nan
        return f[mask].corr(r[mask])

    return df.groupby(level=0).apply(_pearson)


def test_6_scenarios_ic_consistency():
    """6 种 mock 场景 × IC Series diff < 1e-10"""
    print("\n=== 6 Scenarios IC Consistency ===")
    for sid, factor, returns in iter_scenarios():
        ic_new = calc_ic(factor, returns)
        ic_ref = calc_ic_reference(factor, returns)
        assert_series_close(ic_new, ic_ref, tol=1e-10, label=f"{sid}_ic")
        check(f"{sid}: IC Series diff < 1e-10", True)


def test_extreme_ic_1():
    """极端信号 IC=1.0（完美线性正相关）"""
    print("\n=== Extreme Signal: IC=1.0 ===")
    rng = np.random.default_rng(42)
    n_days, n_symbols = 50, 30
    timestamps = pd.date_range("2025-01-01", periods=n_days, freq="D")
    symbols = [f"S_{i}" for i in range(n_symbols)]
    idx = pd.MultiIndex.from_product([timestamps, symbols], names=["timestamp", "symbol"])

    factor = pd.Series(rng.standard_normal(n_days * n_symbols), index=idx)
    # 完美线性正相关 / perfect positive linear correlation
    returns = factor * 2.0 + 1.0

    ic_new = calc_ic(factor, returns)
    ic_ref = calc_ic_reference(factor, returns)
    assert_series_close(ic_new, ic_ref, tol=1e-10, label="ic_1.0")

    valid_ic = ic_new.dropna()
    check("IC=1.0: mean IC ≈ 1.0", abs(valid_ic.mean() - 1.0) < 1e-6,
          f"mean={valid_ic.mean():.10f}")


def test_extreme_ic_neg1():
    """极端信号 IC=-1.0（完美线性负相关）"""
    print("\n=== Extreme Signal: IC=-1.0 ===")
    rng = np.random.default_rng(77)
    n_days, n_symbols = 50, 30
    timestamps = pd.date_range("2025-01-01", periods=n_days, freq="D")
    symbols = [f"S_{i}" for i in range(n_symbols)]
    idx = pd.MultiIndex.from_product([timestamps, symbols], names=["timestamp", "symbol"])

    factor = pd.Series(rng.standard_normal(n_days * n_symbols), index=idx)
    # 完美线性负相关 / perfect negative linear correlation
    returns = -factor * 3.0 + 0.5

    ic_new = calc_ic(factor, returns)
    ic_ref = calc_ic_reference(factor, returns)
    assert_series_close(ic_new, ic_ref, tol=1e-10, label="ic_neg1.0")

    valid_ic = ic_new.dropna()
    check("IC=-1.0: mean IC ≈ -1.0", abs(valid_ic.mean() + 1.0) < 1e-6,
          f"mean={valid_ic.mean():.10f}")


def test_with_nan_data():
    """含 NaN 数据的 IC 一致性"""
    print("\n=== NaN Data Handling ===")
    # 手动构建含 NaN 的数据 / manually build data with NaN
    rng = np.random.default_rng(123)
    n_days, n_symbols = 30, 20
    timestamps = pd.date_range("2025-01-01", periods=n_days, freq="D")
    symbols = [f"S_{i}" for i in range(n_symbols)]
    idx = pd.MultiIndex.from_product([timestamps, symbols], names=["timestamp", "symbol"])

    f_vals = rng.standard_normal(n_days * n_symbols)
    r_vals = f_vals * 0.5 + rng.standard_normal(n_days * n_symbols) * 0.866

    # 高比例 NaN 注入 / high NaN fraction injection
    nan_mask = rng.random(n_days * n_symbols) < 0.3
    f_vals[nan_mask] = np.nan
    # 部分独立 NaN（仅 returns 为 NaN）/ some independent NaN (only returns)
    r_nan_mask = rng.random(n_days * n_symbols) < 0.1
    r_vals[r_nan_mask] = np.nan

    factor = pd.Series(f_vals, index=idx)
    returns = pd.Series(r_vals, index=idx)

    ic_new = calc_ic(factor, returns)
    ic_ref = calc_ic_reference(factor, returns)
    assert_series_close(ic_new, ic_ref, tol=1e-10, label="nan_data")
    check("High NaN fraction: IC diff < 1e-10", True)

    # 全 NaN 输入 / all NaN input
    f_all_nan = pd.Series(np.nan, index=idx)
    r_all_nan = pd.Series(np.nan, index=idx)
    ic_nan_new = calc_ic(f_all_nan, r_all_nan)
    ic_nan_ref = calc_ic_reference(f_all_nan, r_all_nan)
    assert_series_close(ic_nan_new, ic_nan_ref, tol=1e-10, label="all_nan")
    check("All NaN input: IC all NaN, consistent", True)


def test_edge_cases():
    """边界情况：小数据、单资产、常数因子"""
    print("\n=== Edge Cases ===")

    # 单时间截面单资产 / single time, single asset
    idx1 = pd.MultiIndex.from_tuples(
        [(pd.Timestamp("2025-01-01"), "A")],
        names=["timestamp", "symbol"],
    )
    f1 = pd.Series([1.0], index=idx1)
    r1 = pd.Series([0.5], index=idx1)

    ic_new1 = calc_ic(f1, r1)
    ic_ref1 = calc_ic_reference(f1, r1)
    assert_series_close(ic_new1, ic_ref1, tol=1e-10, label="single_asset")
    check("Single asset: NaN IC, consistent", True)

    # 常数因子（方差为零） / constant factor (zero variance)
    idx2 = pd.MultiIndex.from_product(
        [pd.date_range("2025-01-01", periods=5), ["A", "B"]],
        names=["timestamp", "symbol"],
    )
    f_const = pd.Series(1.0, index=idx2)
    r_var = pd.Series(np.arange(10, dtype=float), index=idx2)

    ic_new_const = calc_ic(f_const, r_var)
    ic_ref_const = calc_ic_reference(f_const, r_var)
    assert_series_close(ic_new_const, ic_ref_const, tol=1e-10, label="const_factor")
    check("Constant factor: NaN IC, consistent", True)

    # 仅两个资产 / only two assets
    idx3 = pd.MultiIndex.from_product(
        [pd.date_range("2025-01-01", periods=3), ["A", "B"]],
        names=["timestamp", "symbol"],
    )
    f2 = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], index=idx3)
    r2 = pd.Series([2.0, 4.0, 6.0, 8.0, 10.0, 12.0], index=idx3)

    ic_new2 = calc_ic(f2, r2)
    ic_ref2 = calc_ic_reference(f2, r2)
    assert_series_close(ic_new2, ic_ref2, tol=1e-10, label="two_assets")
    check("Two assets: IC consistent", True)


def test_return_type_and_shape():
    """返回类型和形状一致性"""
    print("\n=== Return Type & Shape ===")
    for sid, factor, returns in iter_scenarios():
        ic_new = calc_ic(factor, returns)
        ic_ref = calc_ic_reference(factor, returns)

        check(f"{sid}: returns pd.Series", isinstance(ic_new, pd.Series))
        check(f"{sid}: length matches", len(ic_new) == len(ic_ref))
        check(f"{sid}: index type is timestamp", isinstance(ic_new.index, pd.DatetimeIndex))


def test_no_regression_existing():
    """既有 test_metrics_ic 测试用例不回归"""
    print("\n=== No Regression ===")

    rng = np.random.default_rng(42)
    n_days, n_symbols = 100, 50
    timestamps = pd.date_range("2025-01-01", periods=n_days, freq="D")
    symbols = [f"SYM_{i:03d}" for i in range(n_symbols)]
    idx = pd.MultiIndex.from_product([timestamps, symbols], names=["timestamp", "symbol"])

    factor_values = rng.standard_normal((n_days, n_symbols))
    nan_mask = rng.random((n_days, n_symbols)) < 0.02
    factor_values[nan_mask] = np.nan
    noise = rng.standard_normal((n_days, n_symbols)) * 0.8
    returns_values = factor_values * 0.3 + noise
    returns_values[nan_mask] = np.nan

    factor = pd.Series(factor_values.ravel(), index=idx, name="factor")
    returns = pd.Series(returns_values.ravel(), index=idx, name="returns")

    ic_series = calc_ic(factor, returns)
    valid_ic = ic_series.dropna()

    check("IC returns pd.Series", isinstance(ic_series, pd.Series))
    check("IC length == n_days", len(ic_series) == 100, f"got {len(ic_series)}")
    check("All IC in [-1, 1]", valid_ic.between(-1, 1).all(),
          f"min={valid_ic.min():.4f}, max={valid_ic.max():.4f}")
    check("Mean IC > 0 (positive corr injected)", valid_ic.mean() > 0,
          f"mean={valid_ic.mean():.4f}")
    check("Not all NaN", len(valid_ic) > 0)


if __name__ == "__main__":
    test_6_scenarios_ic_consistency()
    test_extreme_ic_1()
    test_extreme_ic_neg1()
    test_with_nan_data()
    test_edge_cases()
    test_return_type_and_shape()
    test_no_regression_existing()

    print(f"\n{'=' * 40}")
    print(f"Results: {PASS} PASSED, {FAIL} FAILED")
    print(f"{'=' * 40}")

    if FAIL > 0:
        sys.exit(1)
