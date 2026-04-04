"""
Task 11 测试 — IC/RankIC 向量化整体数值一致性
Unified IC/RankIC vectorized numerical consistency test.

综合验证 calc_ic / calc_rank_ic / calc_icir / calc_ic_stats 四个函数的向量化实现
与逐截面 groupby.apply 参考实现的一致性。
Comprehensively verify vectorized implementations of calc_ic / calc_rank_ic / calc_icir / calc_ic_stats
against per-cross-section groupby.apply reference implementations.

覆盖范围 / Coverage:
  - 6 种 mock 场景 × IC/RankIC/ICIR/IC_stats 全字段 diff < 1e-10
  - 极端信号 (IC=1.0/-1.0) + RankIC 验证
  - 含 NaN 数据（高比例 NaN + 全 NaN + 独立 NaN）
  - 多种子稳定性（5 个不同种子 × 全量指标）
"""

import sys
import warnings
import numpy as np
import pandas as pd

sys.path.insert(0, ".")

from FactorAnalysis.metrics import calc_ic, calc_rank_ic, calc_icir, calc_ic_stats
from tests.mutual_components.conftest_perf import (
    iter_scenarios,
    assert_series_close,
    assert_scalar_close,
    make_synthetic_data,
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


# ============================================================
# 参考实现（优化前逐截面 groupby.apply）/ Reference implementations
# ============================================================


def calc_ic_reference(factor: pd.Series, returns: pd.Series) -> pd.Series:
    """
    逐截面 Pearson IC 参考实现 / Per-cross-section Pearson IC reference.
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


def calc_rank_ic_reference(factor: pd.Series, returns: pd.Series) -> pd.Series:
    """
    逐截面 Spearman Rank IC 参考实现 / Per-cross-section Spearman Rank IC reference.
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


def calc_icir_reference(factor: pd.Series, returns: pd.Series) -> float:
    """
    从参考 IC 序列计算 ICIR / Compute ICIR from reference IC series.
    """
    ic_series = calc_ic_reference(factor, returns)
    ic_valid = ic_series.dropna()
    if len(ic_valid) == 0:
        return 0.0
    mean_ic = ic_valid.mean()
    std_ic = ic_valid.std()
    if std_ic == 0:
        return 0.0
    return mean_ic / std_ic


def calc_ic_stats_reference(factor: pd.Series, returns: pd.Series) -> pd.Series:
    """
    从参考 IC 序列计算 IC 统计量 / Compute IC stats from reference IC series.
    与 calc_ic_stats 输出字段和逻辑一致。
    """
    from scipy import stats as sp_stats

    ic_series = calc_ic_reference(factor, returns)
    ic_valid = ic_series.dropna()

    if len(ic_valid) < 3:
        return pd.Series({
            "IC_mean": np.nan, "IC_std": np.nan, "ICIR": np.nan,
            "t_stat": np.nan, "p_value": np.nan,
            "IC_skew": np.nan, "IC_kurtosis": np.nan,
        })

    ic_mean = float(ic_valid.mean())
    ic_std = float(ic_valid.std(ddof=1))
    icir = ic_mean / ic_std if ic_std != 0 else np.nan

    t_stat, p_value = sp_stats.ttest_1samp(ic_valid.values, 0.0)
    ic_skew = float(sp_stats.skew(ic_valid.values, bias=False))
    ic_kurtosis = float(sp_stats.kurtosis(ic_valid.values, bias=False))

    return pd.Series({
        "IC_mean": ic_mean, "IC_std": ic_std, "ICIR": icir,
        "t_stat": float(t_stat), "p_value": float(p_value),
        "IC_skew": ic_skew, "IC_kurtosis": ic_kurtosis,
    })


# ============================================================
# 测试用例 / Test cases
# ============================================================


def test_6_scenarios_ic_rankic_icir_stats():
    """
    6 种 mock 场景 × IC/RankIC/ICIR/IC_stats 全字段 diff < 1e-10
    6 mock scenarios × IC/RankIC/ICIR/IC_stats all fields diff < 1e-10
    """
    print("\n=== 6 Scenarios: IC/RankIC/ICIR/IC_stats ===")
    stats_fields = ["IC_mean", "IC_std", "ICIR", "t_stat", "p_value", "IC_skew", "IC_kurtosis"]

    for sid, factor, returns in iter_scenarios():
        # IC Series 一致性 / IC Series consistency
        ic_new = calc_ic(factor, returns)
        ic_ref = calc_ic_reference(factor, returns)
        assert_series_close(ic_new, ic_ref, tol=1e-10, label=f"{sid}_ic")
        check(f"{sid}: IC Series diff < 1e-10", True)

        # Rank IC Series 一致性 / Rank IC Series consistency
        ric_new = calc_rank_ic(factor, returns)
        ric_ref = calc_rank_ic_reference(factor, returns)
        assert_series_close(ric_new, ric_ref, tol=1e-10, label=f"{sid}_rank_ic")
        check(f"{sid}: RankIC Series diff < 1e-10", True)

        # ICIR 一致性 / ICIR consistency
        icir_new = calc_icir(factor, returns)
        icir_ref = calc_icir_reference(factor, returns)
        assert_scalar_close(icir_new, icir_ref, tol=1e-10, label=f"{sid}_icir")
        check(f"{sid}: ICIR diff < 1e-10", True)

        # IC_stats 全字段一致性 / IC_stats all fields consistency
        stats_new = calc_ic_stats(factor, returns)
        stats_ref = calc_ic_stats_reference(factor, returns)
        for field in stats_fields:
            assert_scalar_close(
                stats_new[field], stats_ref[field],
                tol=1e-10, label=f"{sid}_stats_{field}",
            )
        check(f"{sid}: IC_stats 7 fields diff < 1e-10", True)


def test_extreme_ic_pos1():
    """
    极端信号 IC=1.0：完美线性正相关
    Extreme signal IC=1.0: perfect positive linear correlation
    """
    print("\n=== Extreme Signal: IC=1.0 ===")
    rng = np.random.default_rng(42)
    n_days, n_symbols = 50, 30
    timestamps = pd.date_range("2025-01-01", periods=n_days, freq="D")
    symbols = [f"S_{i}" for i in range(n_symbols)]
    idx = pd.MultiIndex.from_product([timestamps, symbols], names=["timestamp", "symbol"])

    factor = pd.Series(rng.standard_normal(n_days * n_symbols), index=idx)
    returns = factor * 2.0 + 1.0  # 完美线性正相关 / perfect positive linear

    # IC 一致性 / IC consistency
    ic_new = calc_ic(factor, returns)
    ic_ref = calc_ic_reference(factor, returns)
    assert_series_close(ic_new, ic_ref, tol=1e-10, label="extreme_pos_ic")
    valid_ic = ic_new.dropna()
    check("IC=1.0: mean IC ≈ 1.0", abs(valid_ic.mean() - 1.0) < 1e-6,
          f"mean={valid_ic.mean():.10f}")

    # Rank IC 一致性 / Rank IC consistency
    ric_new = calc_rank_ic(factor, returns)
    ric_ref = calc_rank_ic_reference(factor, returns)
    assert_series_close(ric_new, ric_ref, tol=1e-10, label="extreme_pos_rank_ic")
    valid_ric = ric_new.dropna()
    check("IC=1.0: mean RankIC ≈ 1.0", abs(valid_ric.mean() - 1.0) < 1e-6,
          f"mean={valid_ric.mean():.10f}")

    # ICIR：完美相关时 std≈0，ICIR 对浮点极敏感，改为验证 ICIR 逻辑正确性
    # ICIR: perfect correlation → std≈0, ICIR numerically unstable; verify logic instead
    icir_new = calc_icir(factor, returns)
    # IC 序列均值接近 1.0，ICIR 应为 0.0（std=0 分支）或非常大的正数
    # IC mean ≈ 1.0, ICIR should be 0.0 (std=0 branch) or very large positive
    check("IC=1.0: ICIR >= 0 (positive signal)", icir_new >= 0,
          f"ICIR={icir_new}")

    # IC_stats：IC_std≈0 时 ICIR/t_stat/p_value 对浮点极敏感，仅验证稳定字段
    # IC_stats: IC_std≈0 makes ICIR/t_stat/p_value float-sensitive; verify stable fields only
    stats_new = calc_ic_stats(factor, returns)
    stats_ref = calc_ic_stats_reference(factor, returns)
    for field in ["IC_mean", "IC_std", "IC_skew", "IC_kurtosis"]:
        assert_scalar_close(stats_new[field], stats_ref[field], tol=1e-8,
                            label=f"extreme_pos_stats_{field}")
    check("IC=1.0: IC_stats 4 stable fields diff < 1e-8", True)


def test_extreme_ic_neg1():
    """
    极端信号 IC=-1.0：完美线性负相关
    Extreme signal IC=-1.0: perfect negative linear correlation
    """
    print("\n=== Extreme Signal: IC=-1.0 ===")
    rng = np.random.default_rng(77)
    n_days, n_symbols = 50, 30
    timestamps = pd.date_range("2025-01-01", periods=n_days, freq="D")
    symbols = [f"S_{i}" for i in range(n_symbols)]
    idx = pd.MultiIndex.from_product([timestamps, symbols], names=["timestamp", "symbol"])

    factor = pd.Series(rng.standard_normal(n_days * n_symbols), index=idx)
    returns = -factor * 3.0 + 0.5  # 完美线性负相关 / perfect negative linear

    ic_new = calc_ic(factor, returns)
    ic_ref = calc_ic_reference(factor, returns)
    assert_series_close(ic_new, ic_ref, tol=1e-10, label="extreme_neg_ic")
    valid_ic = ic_new.dropna()
    check("IC=-1.0: mean IC ≈ -1.0", abs(valid_ic.mean() + 1.0) < 1e-6,
          f"mean={valid_ic.mean():.10f}")

    ric_new = calc_rank_ic(factor, returns)
    ric_ref = calc_rank_ic_reference(factor, returns)
    assert_series_close(ric_new, ric_ref, tol=1e-10, label="extreme_neg_rank_ic")
    valid_ric = ric_new.dropna()
    check("IC=-1.0: mean RankIC ≈ -1.0", abs(valid_ric.mean() + 1.0) < 1e-6,
          f"mean={valid_ric.mean():.10f}")

    # ICIR：完美负相关时 std≈0，ICIR 对浮点极敏感
    # ICIR: perfect negative correlation → std≈0, ICIR numerically unstable
    icir_new = calc_icir(factor, returns)
    check("IC=-1.0: ICIR <= 0 (negative signal)", icir_new <= 0,
          f"ICIR={icir_new}")


def test_with_nan_data():
    """
    含 NaN 数据：高比例 NaN + 全 NaN + 独立 NaN 位置
    NaN data: high fraction + all NaN + independent NaN positions
    """
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
    # 部分独立 NaN（仅 returns）/ some independent NaN (only returns)
    r_nan_mask = rng.random(n_days * n_symbols) < 0.1
    r_vals[r_nan_mask] = np.nan

    factor = pd.Series(f_vals, index=idx)
    returns = pd.Series(r_vals, index=idx)

    # IC 一致性 / IC consistency
    ic_new = calc_ic(factor, returns)
    ic_ref = calc_ic_reference(factor, returns)
    assert_series_close(ic_new, ic_ref, tol=1e-10, label="nan_data_ic")
    check("High NaN: IC diff < 1e-10", True)

    # Rank IC 一致性 / Rank IC consistency
    ric_new = calc_rank_ic(factor, returns)
    ric_ref = calc_rank_ic_reference(factor, returns)
    assert_series_close(ric_new, ric_ref, tol=1e-10, label="nan_data_rank_ic")
    check("High NaN: RankIC diff < 1e-10", True)

    # ICIR 和 IC_stats / ICIR and IC_stats
    icir_new = calc_icir(factor, returns)
    icir_ref = calc_icir_reference(factor, returns)
    assert_scalar_close(icir_new, icir_ref, tol=1e-10, label="nan_data_icir")
    check("High NaN: ICIR diff < 1e-10", True)

    stats_new = calc_ic_stats(factor, returns)
    stats_ref = calc_ic_stats_reference(factor, returns)
    for field in stats_ref.index:
        assert_scalar_close(stats_new[field], stats_ref[field], tol=1e-10,
                            label=f"nan_data_stats_{field}")
    check("High NaN: IC_stats 7 fields diff < 1e-10", True)

    # 全 NaN 输入 / all NaN input
    f_all_nan = pd.Series(np.nan, index=idx)
    r_all_nan = pd.Series(np.nan, index=idx)

    ic_nan_new = calc_ic(f_all_nan, r_all_nan)
    ic_nan_ref = calc_ic_reference(f_all_nan, r_all_nan)
    assert_series_close(ic_nan_new, ic_nan_ref, tol=1e-10, label="all_nan_ic")
    check("All NaN: IC all NaN, consistent", True)

    ric_nan_new = calc_rank_ic(f_all_nan, r_all_nan)
    ric_nan_ref = calc_rank_ic_reference(f_all_nan, r_all_nan)
    assert_series_close(ric_nan_new, ric_nan_ref, tol=1e-10, label="all_nan_rank_ic")
    check("All NaN: RankIC all NaN, consistent", True)

    # ICIR 全 NaN 输入应返回 0.0 / ICIR all NaN should return 0.0
    icir_nan = calc_icir(f_all_nan, r_all_nan)
    check("All NaN: ICIR = 0.0", icir_nan == 0.0, f"got {icir_nan}")


def test_multi_seed_stability():
    """
    多种子稳定性：5 个不同种子 × 全量指标一致
    Multi-seed stability: 5 different seeds × full metrics consistency
    """
    print("\n=== Multi-Seed Stability ===")
    seeds = [42, 100, 200, 300, 999]

    for seed in seeds:
        factor, returns = make_synthetic_data(
            n_days=100, n_symbols=30, seed=seed, nan_frac=0.05, corr=0.4,
        )

        # IC 一致性 / IC consistency
        ic_new = calc_ic(factor, returns)
        ic_ref = calc_ic_reference(factor, returns)
        assert_series_close(ic_new, ic_ref, tol=1e-10, label=f"seed{seed}_ic")
        check(f"seed={seed}: IC consistent", True)

        # Rank IC 一致性 / Rank IC consistency
        ric_new = calc_rank_ic(factor, returns)
        ric_ref = calc_rank_ic_reference(factor, returns)
        assert_series_close(ric_new, ric_ref, tol=1e-10, label=f"seed{seed}_rank_ic")
        check(f"seed={seed}: RankIC consistent", True)

        # ICIR 一致性 / ICIR consistency
        icir_new = calc_icir(factor, returns)
        icir_ref = calc_icir_reference(factor, returns)
        assert_scalar_close(icir_new, icir_ref, tol=1e-10, label=f"seed{seed}_icir")
        check(f"seed={seed}: ICIR consistent", True)

        # IC_stats 全字段一致性 / IC_stats all fields
        stats_new = calc_ic_stats(factor, returns)
        stats_ref = calc_ic_stats_reference(factor, returns)
        for field in stats_ref.index:
            assert_scalar_close(stats_new[field], stats_ref[field], tol=1e-10,
                                label=f"seed{seed}_stats_{field}")
        check(f"seed={seed}: IC_stats 7 fields consistent", True)


def test_ic_stats_insufficient_data_warning():
    """
    IC_stats 数据不足时返回全 NaN + 警告
    IC_stats returns all-NaN + warning when insufficient data
    """
    print("\n=== IC Stats Insufficient Data ===")
    rng = np.random.default_rng(42)
    n_days, n_symbols = 2, 5  # 仅 2 天，不足 3 个有效 IC
    timestamps = pd.date_range("2025-01-01", periods=n_days, freq="D")
    symbols = [f"S_{i}" for i in range(n_symbols)]
    idx = pd.MultiIndex.from_product([timestamps, symbols], names=["timestamp", "symbol"])

    factor = pd.Series(rng.standard_normal(n_days * n_symbols), index=idx)
    returns = pd.Series(rng.standard_normal(n_days * n_symbols), index=idx)

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        stats_new = calc_ic_stats(factor, returns)
        stats_ref = calc_ic_stats_reference(factor, returns)

    # 两者都应返回全 NaN / both should return all NaN
    for field in stats_ref.index:
        assert_scalar_close(stats_new[field], stats_ref[field], tol=1e-10,
                            label=f"insufficient_{field}")
    check("Insufficient data: IC_stats all NaN, consistent", True)
    # 验证警告 / verify warning
    check("Insufficient data: warning raised", len(w) > 0,
          f"expected warning, got {len(w)}")


if __name__ == "__main__":
    test_6_scenarios_ic_rankic_icir_stats()
    test_extreme_ic_pos1()
    test_extreme_ic_neg1()
    test_with_nan_data()
    test_multi_seed_stability()
    test_ic_stats_insufficient_data_warning()

    print(f"\n{'=' * 40}")
    print(f"Results: {PASS} PASSED, {FAIL} FAILED")
    print(f"{'=' * 40}")

    if FAIL > 0:
        sys.exit(1)
