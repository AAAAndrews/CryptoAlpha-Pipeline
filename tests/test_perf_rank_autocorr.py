"""
Task 12 测试 — calc_rank_autocorr 向量化数值一致性
Vectorized calc_rank_autocorr numerical consistency test.

对比向量化 calc_rank_autocorr 与逐截面 xs+corr 参考实现，
验证 6 种 mock 场景 + 极端信号 + NaN 数据 + 小数据集 + lag 参数的数值一致性。
Compare vectorized calc_rank_autocorr vs per-cross-section xs+corr reference,
verifying 6 mock scenarios + extreme signals + NaN + small dataset + lag consistency.
"""

import sys
import numpy as np
import pandas as pd

sys.path.insert(0, ".")

from FactorAnalysis.turnover import calc_rank_autocorr
from tests.mutual_components.conftest_perf import (
    SCENARIOS,
    make_synthetic_data,
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


def check_series_close(name, a, b, tol=1e-10):
    """包装 assert_series_close 为 check 模式 / Wrap assert_series_close as check."""
    try:
        assert_series_close(a, b, tol=tol, label=name)
        check(name, True)
    except AssertionError as e:
        check(name, False, str(e))


def calc_rank_autocorr_reference(factor: pd.Series, lag: int = 1) -> pd.Series:
    """
    逐截面 xs+corr 参考实现（优化前逻辑）/ Reference implementation (pre-optimization logic).

    对每个时间截面排名后，逐截面取 rank 向量，与滞后 lag 期的 rank 向量做 Pearson 相关。
    Rank per cross-section, then per-timestamp Pearson between current and lagged rank vectors.
    """
    # 横截面排名 / cross-sectional ranking
    ranks = factor.groupby(level=0, group_keys=False).rank()

    timestamps = sorted(ranks.index.get_level_values(0).unique())

    if len(timestamps) <= lag:
        return pd.Series(np.nan, index=timestamps, dtype=np.float64)

    results = {}
    for i in range(len(timestamps)):
        if i < lag:
            results[timestamps[i]] = np.nan
            continue
        # 当前截面和滞后截面的 rank 向量 / current and lagged rank vectors
        curr = ranks.xs(timestamps[i], level=0).dropna()
        prev = ranks.xs(timestamps[i - lag], level=0).dropna()
        # 两者共同资产 / common assets
        common = curr.index.intersection(prev.index)
        if len(common) < 2:
            results[timestamps[i]] = np.nan
            continue
        c = curr.loc[common].values
        p = prev.loc[common].values
        # Pearson 相关 / Pearson correlation
        r = np.corrcoef(c, p)[0, 1]
        results[timestamps[i]] = r

    return pd.Series(results, dtype=np.float64)


# ── 1. 6 种 mock 场景一致性 / 6 mock scenarios consistency ─────────────

def test_6_scenarios_consistency():
    """6 种 mock 场景 × rank_autocorr Series diff < 1e-10."""
    print("\n=== 6 scenarios consistency ===")
    for sid, factor, _ in iter_scenarios():
        vec = calc_rank_autocorr(factor)
        ref = calc_rank_autocorr_reference(factor)
        check_series_close(f"{sid}: vectorized vs reference", vec, ref)


# ── 2. 极端信号：稳定因子 autocorr=1.0 / Extreme: stable factor ────────

def test_stable_factor_autocorr_1():
    """因子值不变 → 排名不变 → autocorr = 1.0."""
    print("\n=== stable factor autocorr=1.0 ===")
    n_days, n_syms = 20, 30
    timestamps = pd.date_range("2025-01-01", periods=n_days, freq="D")
    symbols = [f"S{i:03d}" for i in range(n_syms)]
    # 每期因子值相同 → 排名相同 → autocorr=1.0
    # same factor each period → same ranks → autocorr=1.0
    vals = np.tile(np.arange(n_syms, dtype=float), (n_days, 1))
    idx = pd.MultiIndex.from_product([timestamps, symbols], names=["timestamp", "symbol"])
    factor = pd.Series(vals.ravel(), index=idx)

    vec = calc_rank_autocorr(factor)
    ref = calc_rank_autocorr_reference(factor)
    check_series_close("stable: vectorized vs reference", vec, ref)
    assert_scalar_close(vec.mean(), 1.0, tol=1e-10, label="stable mean")
    check("stable mean ≈ 1.0", True)


# ── 3. 极端信号：振荡因子 autocorr=-1.0 / Extreme: oscillating ────────

def test_oscillating_factor_autocorr_neg1():
    """奇偶期排名完全反转 → autocorr = -1.0."""
    print("\n=== oscillating factor autocorr=-1.0 ===")
    n_days, n_syms = 6, 10
    timestamps = pd.date_range("2025-01-01", periods=n_days, freq="D")
    symbols = [f"S{i:03d}" for i in range(n_syms)]
    vals = np.zeros((n_days, n_syms))
    for i in range(n_days):
        if i % 2 == 0:
            vals[i] = np.arange(n_syms, dtype=float)
        else:
            vals[i] = np.arange(n_syms - 1, -1, -1, dtype=float)
    idx = pd.MultiIndex.from_product([timestamps, symbols], names=["timestamp", "symbol"])
    factor = pd.Series(vals.ravel(), index=idx)

    vec = calc_rank_autocorr(factor)
    ref = calc_rank_autocorr_reference(factor)
    check_series_close("oscillating: vectorized vs reference", vec, ref)
    assert_scalar_close(vec.mean(), -1.0, tol=1e-10, label="oscillating mean")
    check("oscillating mean ≈ -1.0", True)


# ── 4. NaN 数据处理 / NaN handling ────────────────────────────────────

def test_high_nan_consistency():
    """高比例 NaN (10%) × rank_autocorr 一致性."""
    print("\n=== high NaN consistency ===")
    factor, _ = make_synthetic_data(**SCENARIOS["with_nan"])

    vec = calc_rank_autocorr(factor)
    ref = calc_rank_autocorr_reference(factor)
    check_series_close("with_nan: vectorized vs reference", vec, ref)


def test_all_nan_input():
    """全 NaN 输入 → 全 NaN 输出."""
    print("\n=== all NaN input ===")
    timestamps = pd.date_range("2025-01-01", periods=10, freq="D")
    symbols = [f"S{i:03d}" for i in range(5)]
    idx = pd.MultiIndex.from_product([timestamps, symbols], names=["timestamp", "symbol"])
    factor = pd.Series(np.nan, index=idx)

    vec = calc_rank_autocorr(factor)
    ref = calc_rank_autocorr_reference(factor)
    check_series_close("all_nan: vectorized vs reference", vec, ref)
    check("all_nan: all NaN output", vec.isna().all())


# ── 5. 边界情况 / Edge cases ─────────────────────────────────────────

def test_minimal_data():
    """最小数据 (2 期 × 3 资产)."""
    print("\n=== minimal data ===")
    timestamps = pd.date_range("2025-01-01", periods=2, freq="D")
    symbols = ["A", "B", "C"]
    idx = pd.MultiIndex.from_product([timestamps, symbols], names=["timestamp", "symbol"])
    factor = pd.Series([1.0, 2.0, 3.0, 3.0, 1.0, 2.0], index=idx)

    vec = calc_rank_autocorr(factor)
    ref = calc_rank_autocorr_reference(factor)
    check_series_close("minimal: vectorized vs reference", vec, ref)


def test_single_period():
    """单期数据 → 全 NaN."""
    print("\n=== single period ===")
    timestamps = pd.date_range("2025-01-01", periods=1, freq="D")
    symbols = ["A", "B", "C"]
    idx = pd.MultiIndex.from_product([timestamps, symbols], names=["timestamp", "symbol"])
    factor = pd.Series([1.0, 2.0, 3.0], index=idx)

    vec = calc_rank_autocorr(factor)
    check("single period: all NaN", vec.isna().all())


def test_single_asset():
    """单资产 → 无法计算排名相关 → NaN."""
    print("\n=== single asset ===")
    timestamps = pd.date_range("2025-01-01", periods=5, freq="D")
    symbols = ["A"]
    idx = pd.MultiIndex.from_product([timestamps, symbols], names=["timestamp", "symbol"])
    factor = pd.Series(np.arange(5, dtype=float), index=idx)

    vec = calc_rank_autocorr(factor)
    check("single asset: all NaN", vec.isna().all())


# ── 6. lag 参数 / lag parameter ────────────────────────────────────────

def test_lag_2():
    """lag=2 时前两期为 NaN，其余正确计算."""
    print("\n=== lag=2 ===")
    factor, _ = make_synthetic_data(**SCENARIOS["basic"])

    vec = calc_rank_autocorr(factor, lag=2)
    ref = calc_rank_autocorr_reference(factor, lag=2)
    check_series_close("lag=2: vectorized vs reference", vec, ref)
    check("lag=2: first 2 NaN", vec.iloc[:2].isna().all())


def test_lag_5():
    """lag=5 时前五期为 NaN."""
    print("\n=== lag=5 ===")
    factor, _ = make_synthetic_data(**SCENARIOS["large"])

    vec = calc_rank_autocorr(factor, lag=5)
    ref = calc_rank_autocorr_reference(factor, lag=5)
    check_series_close("lag=5: vectorized vs reference", vec, ref)
    check("lag=5: first 5 NaN", vec.iloc[:5].isna().all())


# ── 7. 返回类型和形状 / Return type and shape ────────────────────────

def test_return_type_and_shape():
    """6 种场景 × 返回类型和形状."""
    print("\n=== return type and shape ===")
    for sid, factor, _ in iter_scenarios():
        result = calc_rank_autocorr(factor)
        check(f"{sid}: isinstance Series", isinstance(result, pd.Series))
        check(f"{sid}: dtype float", result.dtype in (np.float64, float))
        check(f"{sid}: first is NaN", np.isnan(result.iloc[0]))


# ── 8. 既有回归测试 / Existing regression tests ───────────────────────

def test_existing_regression():
    """test_task12_turnover 的关键断言回归 / Key assertions from test_task12_turnover."""
    print("\n=== existing regression ===")

    # 稳定因子 autocorr=1.0
    ts = pd.date_range("2025-01-01", periods=5, freq="D")
    syms = ["A", "B", "C", "D", "E"]
    vals = np.tile([1.0, 2.0, 3.0, 4.0, 5.0], (5, 1))
    idx = pd.MultiIndex.from_product([ts, syms], names=["timestamp", "symbol"])
    factor = pd.Series(vals.ravel(), index=idx)
    result = calc_rank_autocorr(factor)
    valid = result.dropna().values
    check("stable factor autocorr=1.0", np.allclose(valid, 1.0))

    # 振荡因子 autocorr=-1.0
    ts2 = pd.date_range("2025-01-01", periods=6, freq="D")
    vals_odd = [1.0, 2.0, 3.0, 4.0, 5.0]
    vals_even = [5.0, 4.0, 3.0, 2.0, 1.0]
    vals2 = [vals_odd if i % 2 == 0 else vals_even for i in range(6)]
    idx2 = pd.MultiIndex.from_product([ts2, syms], names=["timestamp", "symbol"])
    factor2 = pd.Series(np.array(vals2).ravel(), index=idx2)
    result2 = calc_rank_autocorr(factor2)
    valid2 = result2.dropna().values
    check("oscillating factor autocorr=-1.0", np.allclose(valid2, -1.0))

    # 值域 [-1, 1]
    rng = np.random.default_rng(42)
    ts3 = pd.date_range("2025-01-01", periods=20, freq="D")
    syms3 = [f"S{i}" for i in range(30)]
    vals3 = rng.standard_normal((20, 30))
    idx3 = pd.MultiIndex.from_product([ts3, syms3], names=["timestamp", "symbol"])
    factor3 = pd.Series(vals3.ravel(), index=idx3)
    result3 = calc_rank_autocorr(factor3)
    valid3 = result3.dropna().values
    check("value range [-1, 1]", (valid3 >= -1.0).all() and (valid3 <= 1.0).all())

    # 参数校验
    empty = pd.Series([], dtype=np.float64)
    try:
        calc_rank_autocorr(empty)
        check("empty raises ValueError", False)
    except ValueError:
        check("empty raises ValueError", True)
    try:
        calc_rank_autocorr(factor3, lag=0)
        check("lag=0 raises ValueError", False)
    except ValueError:
        check("lag=0 raises ValueError", True)
    try:
        calc_rank_autocorr([1, 2, 3])
        check("non-Series raises TypeError", False)
    except TypeError:
        check("non-Series raises TypeError", True)


# ── Main ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    test_6_scenarios_consistency()
    test_stable_factor_autocorr_1()
    test_oscillating_factor_autocorr_neg1()
    test_high_nan_consistency()
    test_all_nan_input()
    test_minimal_data()
    test_single_period()
    test_single_asset()
    test_lag_2()
    test_lag_5()
    test_return_type_and_shape()
    test_existing_regression()

    print(f"\n{'='*50}")
    print(f"Total: {PASS + FAIL} checks, {PASS} passed, {FAIL} failed")
    if FAIL > 0:
        print("RESULT: FAIL")
        sys.exit(1)
    else:
        print("RESULT: ALL PASSED")
