"""
Task 17 验证测试 — FactorAnalysis/metrics.py IC/RankIC/ICIR
验证 pandas 实现的正确性、边界条件和返回类型。
"""

import sys
import numpy as np
import pandas as pd

sys.path.insert(0, ".")

from FactorAnalysis.metrics import calc_ic, calc_rank_ic, calc_icir

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


def make_synthetic_data(n_days=100, n_symbols=50, seed=42):
    """
    生成合成因子值和前向收益率，带一定相关性。
    Generate synthetic factor values and forward returns with some correlation.
    """
    rng = np.random.default_rng(seed)
    timestamps = pd.date_range("2025-01-01", periods=n_days, freq="D")
    symbols = [f"SYM_{i:03d}" for i in range(n_symbols)]

    # 因子值：带趋势的随机值 / Factor values: random with trend
    factor_values = rng.standard_normal((n_days, n_symbols))
    # 注入一些 NaN / inject some NaN
    nan_mask = rng.random((n_days, n_symbols)) < 0.02
    factor_values[nan_mask] = np.nan

    # 前向收益率：与因子值有弱正相关 / Forward returns: weak positive correlation with factor
    noise = rng.standard_normal((n_days, n_symbols)) * 0.8
    returns = factor_values * 0.3 + noise
    # 同步注入 NaN / sync NaN injection
    returns[nan_mask] = np.nan

    # 构建 MultiIndex Series / Build MultiIndex Series
    idx = pd.MultiIndex.from_product([timestamps, symbols], names=["timestamp", "symbol"])
    factor_s = pd.Series(factor_values.ravel(), index=idx, name="factor")
    returns_s = pd.Series(returns.ravel(), index=idx, name="returns")

    return factor_s, returns_s, timestamps


def test_import():
    """测试模块导入 / Test module import"""
    print("\n=== Import Test ===")
    check("calc_ic is callable", callable(calc_ic))
    check("calc_rank_ic is callable", callable(calc_rank_ic))
    check("calc_icir is callable", callable(calc_icir))


def test_calc_ic():
    """测试 Pearson IC 计算 / Test Pearson IC calculation"""
    print("\n=== calc_ic Test ===")
    factor, returns, timestamps = make_synthetic_data()

    ic_series = calc_ic(factor, returns)

    # 返回类型 / return type
    check("calc_ic returns pd.Series", isinstance(ic_series, pd.Series))

    # 长度应等于时间戳数量 / length should equal number of timestamps
    check("IC series length == n_days", len(ic_series) == 100,
          f"got {len(ic_series)}")

    # IC 值应在 [-1, 1] 范围内 / IC values should be in [-1, 1]
    valid_ic = ic_series.dropna()
    check("All IC values in [-1, 1]",
          valid_ic.between(-1, 1).all(),
          f"min={valid_ic.min():.4f}, max={valid_ic.max():.4f}")

    # 由于注入了正相关，平均 IC 应为正 / due to positive correlation, mean IC should be positive
    mean_ic = valid_ic.mean()
    check("Mean IC > 0 (positive correlation injected)",
          mean_ic > 0, f"mean_ic={mean_ic:.4f}")

    # 不应全为 NaN / should not be all NaN
    check("Not all NaN", valid_ic.shape[0] > 0)


def test_calc_rank_ic():
    """测试 Spearman Rank IC 计算 / Test Spearman Rank IC calculation"""
    print("\n=== calc_rank_ic Test ===")
    factor, returns, timestamps = make_synthetic_data()

    rank_ic_series = calc_rank_ic(factor, returns)

    # 返回类型 / return type
    check("calc_rank_ic returns pd.Series", isinstance(rank_ic_series, pd.Series))

    # 长度 / length
    check("Rank IC series length == n_days", len(rank_ic_series) == 100,
          f"got {len(rank_ic_series)}")

    # 范围 / range
    valid_ric = rank_ic_series.dropna()
    check("All Rank IC values in [-1, 1]",
          valid_ric.between(-1, 1).all(),
          f"min={valid_ric.min():.4f}, max={valid_ric.max():.4f}")

    # 平均值应为正 / mean should be positive
    mean_ric = valid_ric.mean()
    check("Mean Rank IC > 0", mean_ric > 0, f"mean_ric={mean_ric:.4f}")

    # Rank IC 通常比 Pearson IC 更稳定 / Rank IC is usually more stable
    check("Not all NaN", valid_ric.shape[0] > 0)


def test_calc_icir():
    """测试 ICIR 计算 / Test ICIR calculation"""
    print("\n=== calc_icir Test ===")
    factor, returns, timestamps = make_synthetic_data()

    icir_val = calc_icir(factor, returns)

    # 返回类型 / return type
    check("calc_icir returns float", isinstance(icir_val, (float, np.floating)))

    # ICIR 应为有限值 / ICIR should be finite
    check("ICIR is finite", np.isfinite(icir_val), f"got {icir_val}")

    # 由于有正相关注入，ICIR 应为正 / due to positive correlation, ICIR should be positive
    check("ICIR > 0", icir_val > 0, f"icir={icir_val:.4f}")


def test_perfect_correlation():
    """测试完美相关场景 / Test perfect correlation scenario"""
    print("\n=== Perfect Correlation Test ===")
    rng = np.random.default_rng(99)
    n_days, n_symbols = 50, 30
    timestamps = pd.date_range("2025-01-01", periods=n_days, freq="D")
    symbols = [f"S_{i}" for i in range(n_symbols)]

    idx = pd.MultiIndex.from_product([timestamps, symbols], names=["timestamp", "symbol"])
    factor = pd.Series(rng.standard_normal(n_days * n_symbols), index=idx)
    # 完美正相关 / perfect positive correlation
    returns = factor * 2.0 + 1.0

    ic_series = calc_ic(factor, returns)
    rank_ic_series = calc_rank_ic(factor, returns)

    valid_ic = ic_series.dropna()
    valid_ric = rank_ic_series.dropna()

    check("Perfect linear IC ≈ 1.0",
          abs(valid_ic.mean() - 1.0) < 1e-6,
          f"mean_ic={valid_ic.mean():.8f}")

    check("Perfect rank IC ≈ 1.0",
          abs(valid_ric.mean() - 1.0) < 1e-6,
          f"mean_ric={valid_ric.mean():.8f}")

    icir_val = calc_icir(factor, returns)
    check("Perfect correlation ICIR is large",
          icir_val > 10,
          f"icir={icir_val:.2f}")


def test_no_correlation():
    """测试无相关场景 / Test no correlation scenario"""
    print("\n=== No Correlation Test ===")
    rng = np.random.default_rng(77)
    n_days, n_symbols = 100, 50
    timestamps = pd.date_range("2025-01-01", periods=n_days, freq="D")
    symbols = [f"S_{i}" for i in range(n_symbols)]

    idx = pd.MultiIndex.from_product([timestamps, symbols], names=["timestamp", "symbol"])
    factor = pd.Series(rng.standard_normal(n_days * n_symbols), index=idx)
    returns = pd.Series(rng.standard_normal(n_days * n_symbols), index=idx)

    ic_series = calc_ic(factor, returns)
    valid_ic = ic_series.dropna()

    # 无相关时 IC 均值应接近 0 / with no correlation, mean IC should be near 0
    check("No correlation: |mean IC| < 0.1",
          abs(valid_ic.mean()) < 0.1,
          f"mean_ic={valid_ic.mean():.4f}")

    icir_val = calc_icir(factor, returns)
    check("No correlation: |ICIR| < 1.0",
          abs(icir_val) < 1.0,
          f"icir={icir_val:.4f}")


def test_edge_cases():
    """测试边界条件 / Test edge cases"""
    print("\n=== Edge Cases Test ===")

    # 单个时间截面，单资产 / single time, single asset
    idx = pd.MultiIndex.from_tuples(
        [(pd.Timestamp("2025-01-01"), "A")],
        names=["timestamp", "symbol"]
    )
    f1 = pd.Series([1.0], index=idx)
    r1 = pd.Series([0.5], index=idx)

    ic1 = calc_ic(f1, r1)
    check("Single asset returns NaN IC", len(ic1) == 1 and pd.isna(ic1.iloc[0]),
          f"got {ic1}")

    # 全 NaN 输入 / all NaN input
    idx2 = pd.MultiIndex.from_product(
        [pd.date_range("2025-01-01", periods=5), ["A", "B"]],
        names=["timestamp", "symbol"]
    )
    f_nan = pd.Series(np.nan, index=idx2)
    r_nan = pd.Series(np.nan, index=idx2)

    ic_nan = calc_ic(f_nan, r_nan)
    check("All NaN → all NaN IC", ic_nan.dropna().empty)

    icir_nan = calc_icir(f_nan, r_nan)
    check("All NaN → ICIR = 0.0", icir_nan == 0.0, f"got {icir_nan}")

    # 常数因子值（方差为 0）/ constant factor (zero variance)
    f_const = pd.Series(1.0, index=idx2)
    r_var = pd.Series(np.arange(10, dtype=float), index=idx2)
    ic_const = calc_ic(f_const, r_var)
    check("Constant factor → NaN IC (zero variance)",
          ic_const.dropna().empty or all(np.isnan(v) for v in ic_const),
          f"got {ic_const.tolist()}")


def test_public_exports():
    """测试 FactorAnalysis 公共导出 / Test public exports"""
    print("\n=== Public Exports Test ===")
    import FactorAnalysis
    check("calc_ic in module", hasattr(FactorAnalysis, "calc_ic"))
    check("calc_rank_ic in module", hasattr(FactorAnalysis, "calc_rank_ic"))
    check("calc_icir in module", hasattr(FactorAnalysis, "calc_icir"))
    check("__all__ contains calc_ic", "calc_ic" in FactorAnalysis.__all__)
    check("__all__ contains calc_rank_ic", "calc_rank_ic" in FactorAnalysis.__all__)
    check("__all__ contains calc_icir", "calc_icir" in FactorAnalysis.__all__)


if __name__ == "__main__":
    test_import()
    test_calc_ic()
    test_calc_rank_ic()
    test_calc_icir()
    test_perfect_correlation()
    test_no_correlation()
    test_edge_cases()
    test_public_exports()

    print(f"\n{'='*40}")
    print(f"Results: {PASS} PASSED, {FAIL} FAILED")
    print(f"{'='*40}")

    if FAIL > 0:
        sys.exit(1)
