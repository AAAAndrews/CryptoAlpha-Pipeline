"""
Task 10 测试 — calc_rank_ic 向量化数值一致性
Vectorized calc_rank_ic numerical consistency test.

对比向量化 calc_rank_ic 与逐截面 groupby.apply 参考实现，
验证 6 种 mock 场景 + 极端信号 + NaN 数据 + 边界情况的数值一致性。
Compare vectorized calc_rank_ic vs per-cross-section groupby.apply reference,
verifying 6 mock scenarios + extreme signals + NaN + edge cases consistency.
"""

import sys
import numpy as np
import pandas as pd

sys.path.insert(0, ".")

from FactorAnalysis.metrics import calc_rank_ic
from tests.mutual_components.conftest_perf import (
    iter_scenarios,
    assert_series_close,
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


def calc_rank_ic_reference(factor: pd.Series, returns: pd.Series) -> pd.Series:
    """
    逐截面 groupby.apply 参考实现（优化前逻辑）/ Reference implementation (pre-optimization logic).

    用于对比向量化实现的数值一致性。
    For comparing numerical consistency with vectorized implementation.
    """
    df = pd.DataFrame({"factor": factor, "returns": returns})

    def _spearman(g: pd.DataFrame) -> float:
        if len(g) < 2:
            return np.nan
        f = g["factor"]
        r = g["returns"]
        mask = f.notna() & r.notna() & np.isfinite(f) & np.isfinite(r)
        if mask.sum() < 2:
            return np.nan
        return f[mask].corr(r[mask], method="spearman")

    return df.groupby(level=0).apply(_spearman)


def test_6_scenarios_rank_ic_consistency():
    """6 种 mock 场景 × Rank IC Series diff < 1e-10"""
    print("\n=== 6 Scenarios Rank IC Consistency ===")
    for sid, factor, returns in iter_scenarios():
        ric_new = calc_rank_ic(factor, returns)
        ric_ref = calc_rank_ic_reference(factor, returns)
        assert_series_close(ric_new, ric_ref, tol=1e-10, label=f"{sid}_rank_ic")
        check(f"{sid}: Rank IC Series diff < 1e-10", True)


def test_extreme_rank_ic_1():
    """极端信号 Rank IC=1.0（完美单调正相关）"""
    print("\n=== Extreme Signal: Rank IC=1.0 ===")
    rng = np.random.default_rng(42)
    n_days, n_symbols = 50, 30
    timestamps = pd.date_range("2025-01-01", periods=n_days, freq="D")
    symbols = [f"S_{i}" for i in range(n_symbols)]
    idx = pd.MultiIndex.from_product([timestamps, symbols], names=["timestamp", "symbol"])

    factor = pd.Series(rng.standard_normal(n_days * n_symbols), index=idx)
    # 完美单调正相关 / perfect monotonic positive correlation
    returns = factor * 2.0 + 1.0

    ric_new = calc_rank_ic(factor, returns)
    ric_ref = calc_rank_ic_reference(factor, returns)
    assert_series_close(ric_new, ric_ref, tol=1e-10, label="ric_1.0")

    valid_ric = ric_new.dropna()
    check("Rank IC=1.0: mean ≈ 1.0", abs(valid_ric.mean() - 1.0) < 1e-6,
          f"mean={valid_ric.mean():.10f}")


def test_extreme_rank_ic_neg1():
    """极端信号 Rank IC=-1.0（完美单调负相关）"""
    print("\n=== Extreme Signal: Rank IC=-1.0 ===")
    rng = np.random.default_rng(77)
    n_days, n_symbols = 50, 30
    timestamps = pd.date_range("2025-01-01", periods=n_days, freq="D")
    symbols = [f"S_{i}" for i in range(n_symbols)]
    idx = pd.MultiIndex.from_product([timestamps, symbols], names=["timestamp", "symbol"])

    factor = pd.Series(rng.standard_normal(n_days * n_symbols), index=idx)
    # 完美单调负相关 / perfect monotonic negative correlation
    returns = -factor * 3.0 + 0.5

    ric_new = calc_rank_ic(factor, returns)
    ric_ref = calc_rank_ic_reference(factor, returns)
    assert_series_close(ric_new, ric_ref, tol=1e-10, label="ric_neg1.0")

    valid_ric = ric_new.dropna()
    check("Rank IC=-1.0: mean ≈ -1.0", abs(valid_ric.mean() + 1.0) < 1e-6,
          f"mean={valid_ric.mean():.10f}")


def test_with_nan_data():
    """含 NaN 数据的 Rank IC 一致性"""
    print("\n=== NaN Data Handling ===")
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

    ric_new = calc_rank_ic(factor, returns)
    ric_ref = calc_rank_ic_reference(factor, returns)
    assert_series_close(ric_new, ric_ref, tol=1e-10, label="nan_data")
    check("High NaN fraction: Rank IC diff < 1e-10", True)

    # 全 NaN 输入 / all NaN input
    f_all_nan = pd.Series(np.nan, index=idx)
    r_all_nan = pd.Series(np.nan, index=idx)
    ric_nan_new = calc_rank_ic(f_all_nan, r_all_nan)
    ric_nan_ref = calc_rank_ic_reference(f_all_nan, r_all_nan)
    assert_series_close(ric_nan_new, ric_nan_ref, tol=1e-10, label="all_nan")
    check("All NaN input: Rank IC all NaN, consistent", True)


def test_edge_cases():
    """边界情况：小数据、单资产、常数因子、含平值（ties）"""
    print("\n=== Edge Cases ===")

    # 单时间截面单资产 / single time, single asset
    idx1 = pd.MultiIndex.from_tuples(
        [(pd.Timestamp("2025-01-01"), "A")],
        names=["timestamp", "symbol"],
    )
    f1 = pd.Series([1.0], index=idx1)
    r1 = pd.Series([0.5], index=idx1)

    ric_new1 = calc_rank_ic(f1, r1)
    ric_ref1 = calc_rank_ic_reference(f1, r1)
    assert_series_close(ric_new1, ric_ref1, tol=1e-10, label="single_asset")
    check("Single asset: NaN Rank IC, consistent", True)

    # 常数因子（方差为零） / constant factor (zero variance)
    idx2 = pd.MultiIndex.from_product(
        [pd.date_range("2025-01-01", periods=5), ["A", "B"]],
        names=["timestamp", "symbol"],
    )
    f_const = pd.Series(1.0, index=idx2)
    r_var = pd.Series(np.arange(10, dtype=float), index=idx2)

    ric_new_const = calc_rank_ic(f_const, r_var)
    ric_ref_const = calc_rank_ic_reference(f_const, r_var)
    assert_series_close(ric_new_const, ric_ref_const, tol=1e-10, label="const_factor")
    check("Constant factor: NaN Rank IC, consistent", True)

    # 仅两个资产 / only two assets
    idx3 = pd.MultiIndex.from_product(
        [pd.date_range("2025-01-01", periods=3), ["A", "B"]],
        names=["timestamp", "symbol"],
    )
    f2 = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], index=idx3)
    r2 = pd.Series([2.0, 4.0, 6.0, 8.0, 10.0, 12.0], index=idx3)

    ric_new2 = calc_rank_ic(f2, r2)
    ric_ref2 = calc_rank_ic_reference(f2, r2)
    assert_series_close(ric_new2, ric_ref2, tol=1e-10, label="two_assets")
    check("Two assets: Rank IC consistent", True)

    # 含大量平值（ties） / data with many ties
    idx4 = pd.MultiIndex.from_product(
        [pd.date_range("2025-01-01", periods=5), [f"S{i}" for i in range(10)]],
        names=["timestamp", "symbol"],
    )
    # 大量重复值 / many duplicate values
    f_ties = pd.Series(np.repeat([1.0, 2.0, 3.0, 4.0, 5.0], 10), index=idx4)
    r_ties = pd.Series(np.repeat([5.0, 4.0, 3.0, 2.0, 1.0], 10), index=idx4)

    ric_new_ties = calc_rank_ic(f_ties, r_ties)
    ric_ref_ties = calc_rank_ic_reference(f_ties, r_ties)
    assert_series_close(ric_new_ties, ric_ref_ties, tol=1e-10, label="ties")
    check("Ties: Rank IC consistent", True)


def test_return_type_and_shape():
    """返回类型和形状一致性"""
    print("\n=== Return Type & Shape ===")
    for sid, factor, returns in iter_scenarios():
        ric_new = calc_rank_ic(factor, returns)
        ric_ref = calc_rank_ic_reference(factor, returns)

        check(f"{sid}: returns pd.Series", isinstance(ric_new, pd.Series))
        check(f"{sid}: length matches", len(ric_new) == len(ric_ref))
        check(f"{sid}: index type is timestamp", isinstance(ric_new.index, pd.DatetimeIndex))


def test_no_regression_existing():
    """既有 test_calc_rank_ic 测试用例不回归"""
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

    rank_ic_series = calc_rank_ic(factor, returns)
    valid_ric = rank_ic_series.dropna()

    check("Rank IC returns pd.Series", isinstance(rank_ic_series, pd.Series))
    check("Rank IC length == n_days", len(rank_ic_series) == 100, f"got {len(rank_ic_series)}")
    check("All Rank IC in [-1, 1]", valid_ric.between(-1, 1).all(),
          f"min={valid_ric.min():.4f}, max={valid_ric.max():.4f}")
    check("Mean Rank IC > 0 (positive corr injected)", valid_ric.mean() > 0,
          f"mean={valid_ric.mean():.4f}")
    check("Not all NaN", len(valid_ric) > 0)


if __name__ == "__main__":
    test_6_scenarios_rank_ic_consistency()
    test_extreme_rank_ic_1()
    test_extreme_rank_ic_neg1()
    test_with_nan_data()
    test_edge_cases()
    test_return_type_and_shape()
    test_no_regression_existing()

    print(f"\n{'=' * 40}")
    print(f"Results: {PASS} PASSED, {FAIL} FAILED")
    print(f"{'=' * 40}")

    if FAIL > 0:
        sys.exit(1)
