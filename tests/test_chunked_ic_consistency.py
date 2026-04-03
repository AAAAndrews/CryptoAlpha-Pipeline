"""
验证测试 — Task 9: 分块 IC/IR 数值一致性验证
Chunked IC/IR numerical consistency verification.

验证分块计算与全量计算的 IC_mean / IC_std / ICIR 差异 < 1e-8，
使用多种子、多种 chunk_size、多种数据规模进行稳定性验证。
Verify that chunked vs full calculation differences for IC_mean, IC_std, ICIR
are within 1e-8 tolerance, using multiple seeds, chunk sizes, and data scales.
"""

import sys
import traceback

import numpy as np
import pandas as pd

checks = 0


def ok(label: str, condition: bool):
    global checks
    checks += 1
    status = "PASS" if condition else "FAIL"
    print(f"  [{status}] {label}")
    if not condition:
        traceback.print_stack()


def make_synthetic(n_dates=120, n_symbols=50, seed=42, nan_ratio=0.0):
    """生成合成因子和收益率 / Generate synthetic factor and returns."""
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2025-01-01", periods=n_dates, freq="B")
    symbols = [f"S{i:03d}" for i in range(n_symbols)]
    idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])

    true_signal = rng.standard_normal((n_dates, n_symbols))
    noise = rng.standard_normal((n_dates, n_symbols)) * 0.5
    factor_values = true_signal + noise
    returns_values = true_signal * 0.02 + rng.standard_normal((n_dates, n_symbols)) * 0.03

    # 注入 NaN / inject NaN values
    if nan_ratio > 0:
        nan_mask = rng.random((n_dates, n_symbols)) < nan_ratio
        factor_values[nan_mask] = np.nan

    factor = pd.Series(factor_values.ravel(), index=idx, dtype=np.float64)
    returns = pd.Series(returns_values.ravel(), index=idx, dtype=np.float64)
    return factor, returns


def assert_ic_consistency(factor, returns, chunk_size, tol=1e-8, label_prefix=""):
    """
    断言分块 vs 全量 IC 指标一致性 / Assert chunked vs full IC metrics consistency.

    验证 IC 序列、RankIC 序列、ICIR、IC_mean、IC_std、IC_stats 全部字段。
    Verify IC series, RankIC series, ICIR, IC_mean, IC_std, and all IC_stats fields.
    """
    ev_full = FactorEvaluator(factor, returns, chunk_size=None)
    ev_full.run_metrics()

    ev_chunked = FactorEvaluator(factor, returns, chunk_size=chunk_size)
    ev_chunked.run_metrics()

    # IC 序列逐值对比 / per-value IC series comparison
    ic_diff = np.max(np.abs(ev_chunked.ic.values - ev_full.ic.values))
    ok(f"{label_prefix} ic series max diff < {tol} (got {ic_diff:.2e})", ic_diff < tol)

    # RankIC 序列逐值对比 / per-value RankIC series comparison
    ric_diff = np.max(np.abs(ev_chunked.rank_ic.values - ev_full.rank_ic.values))
    ok(f"{label_prefix} rank_ic series max diff < {tol} (got {ric_diff:.2e})", ric_diff < tol)

    # IC 索引一致性 / IC index consistency
    ok(f"{label_prefix} ic index matches", ev_chunked.ic.index.equals(ev_full.ic.index))
    ok(f"{label_prefix} rank_ic index matches", ev_chunked.rank_ic.index.equals(ev_full.rank_ic.index))

    # ICIR 一致性 / ICIR consistency
    icir_diff = abs(ev_chunked.icir - ev_full.icir)
    ok(f"{label_prefix} icir diff < {tol} (got {icir_diff:.2e})", icir_diff < tol)

    # IC_mean / IC_std 一致性 / IC_mean / IC_std consistency
    full_ic_mean = ev_full.ic.mean()
    full_ic_std = ev_full.ic.std()
    chunked_ic_mean = ev_chunked.ic.mean()
    chunked_ic_std = ev_chunked.ic.std()
    ok(f"{label_prefix} ic_mean diff < {tol} (got {abs(chunked_ic_mean - full_ic_mean):.2e})",
       abs(chunked_ic_mean - full_ic_mean) < tol)
    ok(f"{label_prefix} ic_std diff < {tol} (got {abs(chunked_ic_std - full_ic_std):.2e})",
       abs(chunked_ic_std - full_ic_std) < tol)

    # IC_stats 全字段对比 / IC_stats all-field comparison
    stats_fields = ["IC_mean", "IC_std", "ICIR", "t_stat", "p_value", "IC_skew", "IC_kurtosis"]
    for field in stats_fields:
        diff = abs(float(ev_chunked.ic_stats[field]) - float(ev_full.ic_stats[field]))
        ok(f"{label_prefix} ic_stats[{field}] diff < {tol} (got {diff:.2e})", diff < tol)

    # RankIC 均值 / 标准差一致性 / RankIC mean/std consistency
    ric_mean_diff = abs(ev_chunked.rank_ic.mean() - ev_full.rank_ic.mean())
    ric_std_diff = abs(ev_chunked.rank_ic.std() - ev_full.rank_ic.std())
    ok(f"{label_prefix} rank_ic_mean diff < {tol} (got {ric_mean_diff:.2e})", ric_mean_diff < tol)
    ok(f"{label_prefix} rank_ic_std diff < {tol} (got {ric_std_diff:.2e})", ric_std_diff < tol)


try:
    from FactorAnalysis import FactorEvaluator

    print("=== Task 9: 分块 IC/IR 数值一致性验证 ===\n")

    # -------------------------------------------------------
    # 1. 多种子稳定性：不同随机种子下分块 vs 全量一致
    # Multi-seed stability: chunked vs full consistent across seeds
    # -------------------------------------------------------
    print("1. Multi-seed stability")
    seeds = [42, 123, 999, 7, 2024, 31415, 27182, 16180, 10000, 54321]
    for seed in seeds:
        factor, returns = make_synthetic(n_dates=120, n_symbols=50, seed=seed)
        assert_ic_consistency(factor, returns, chunk_size=30, label_prefix=f"seed={seed}")

    # -------------------------------------------------------
    # 2. 多 chunk_size 覆盖：从小块到接近全量
    # Multiple chunk sizes: from small to near-full
    # -------------------------------------------------------
    print("\n2. Multiple chunk sizes")
    chunk_sizes = [1, 2, 5, 10, 15, 20, 30, 40, 50, 60, 80, 100, 119, 120, 150]
    factor, returns = make_synthetic(n_dates=120, n_symbols=50, seed=42)
    for cs in chunk_sizes:
        assert_ic_consistency(factor, returns, chunk_size=cs, label_prefix=f"chunk_size={cs}")

    # -------------------------------------------------------
    # 3. 多数据规模：不同时间截面数和交易对数
    # Multiple data scales: different timestamp and symbol counts
    # -------------------------------------------------------
    print("\n3. Multiple data scales")
    scales = [
        (30, 10, 5),
        (60, 20, 10),
        (120, 50, 30),
        (200, 80, 40),
        (300, 100, 50),
        (500, 30, 100),
    ]
    for n_dates, n_symbols, cs in scales:
        factor, returns = make_synthetic(n_dates=n_dates, n_symbols=n_symbols, seed=42)
        assert_ic_consistency(factor, returns, chunk_size=cs,
                              label_prefix=f"{n_dates}x{n_symbols},cs={cs}")

    # -------------------------------------------------------
    # 4. 含 NaN 数据一致性
    # NaN data consistency
    # -------------------------------------------------------
    print("\n4. NaN data consistency")
    nan_ratios = [0.01, 0.05, 0.1, 0.2]
    for nr in nan_ratios:
        factor, returns = make_synthetic(n_dates=120, n_symbols=50, seed=42, nan_ratio=nr)
        assert_ic_consistency(factor, returns, chunk_size=30,
                              label_prefix=f"nan_ratio={nr}")

    # -------------------------------------------------------
    # 5. NaN + 多种子组合
    # NaN + multi-seed combinations
    # -------------------------------------------------------
    print("\n5. NaN + multi-seed combinations")
    for seed in [42, 123, 999, 31415]:
        factor, returns = make_synthetic(n_dates=120, n_symbols=50, seed=seed, nan_ratio=0.05)
        assert_ic_consistency(factor, returns, chunk_size=25,
                              label_prefix=f"nan=5%,seed={seed}")

    # -------------------------------------------------------
    # 6. 极端信号强度：强信号 / 弱信号
    # Extreme signal strength: strong / weak signal
    # -------------------------------------------------------
    print("\n6. Extreme signal strength")
    rng = np.random.default_rng(42)
    dates = pd.date_range("2025-01-01", periods=120, freq="B")
    symbols = [f"S{i:03d}" for i in range(50)]
    idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])

    # 强信号 / strong signal
    true_signal = rng.standard_normal((120, 50))
    strong_factor = pd.Series((true_signal + rng.standard_normal((120, 50)) * 0.1).ravel(),
                               index=idx, dtype=np.float64)
    strong_returns = pd.Series((true_signal * 0.1 + rng.standard_normal((120, 50)) * 0.005).ravel(),
                                index=idx, dtype=np.float64)
    assert_ic_consistency(strong_factor, strong_returns, chunk_size=30,
                          label_prefix="strong_signal")

    # 弱信号 / weak signal (near-random)
    weak_factor = pd.Series(rng.standard_normal(120 * 50), index=idx, dtype=np.float64)
    weak_returns = pd.Series(rng.standard_normal(120 * 50) * 0.01, index=idx, dtype=np.float64)
    assert_ic_consistency(weak_factor, weak_returns, chunk_size=30,
                          label_prefix="weak_signal")

    # -------------------------------------------------------
    # 7. chunk_size > n_dates（退化为全量）
    # chunk_size > n_dates (degrades to full mode)
    # -------------------------------------------------------
    print("\n7. chunk_size > n_dates (full-mode degradation)")
    factor, returns = make_synthetic(n_dates=50, n_symbols=30, seed=42)
    assert_ic_consistency(factor, returns, chunk_size=200,
                          label_prefix="cs>n_dates")

    # -------------------------------------------------------
    # 8. 边界：极少量时间截面
    # Edge: very few timestamps
    # -------------------------------------------------------
    print("\n8. Edge: few timestamps")
    factor, returns = make_synthetic(n_dates=5, n_symbols=20, seed=42)
    assert_ic_consistency(factor, returns, chunk_size=2,
                          label_prefix="5_dates,cs=2")

    factor, returns = make_synthetic(n_dates=3, n_symbols=20, seed=42)
    assert_ic_consistency(factor, returns, chunk_size=1,
                          label_prefix="3_dates,cs=1")

    # -------------------------------------------------------
    # 9. IC 序列长度一致性验证
    # IC series length consistency
    # -------------------------------------------------------
    print("\n9. IC series length consistency")
    for n_dates, cs in [(120, 30), (100, 17), (200, 73), (50, 7)]:
        factor, returns = make_synthetic(n_dates=n_dates, n_symbols=40, seed=42)
        ev_full = FactorEvaluator(factor, returns, chunk_size=None)
        ev_full.run_metrics()
        ev_chunked = FactorEvaluator(factor, returns, chunk_size=cs)
        ev_chunked.run_metrics()
        ok(f"{n_dates}d,cs={cs}: ic length matches ({len(ev_chunked.ic)} vs {len(ev_full.ic)})",
           len(ev_chunked.ic) == len(ev_full.ic))
        ok(f"{n_dates}d,cs={cs}: rank_ic length matches",
           len(ev_chunked.rank_ic) == len(ev_full.rank_ic))

    # -------------------------------------------------------
    # 10. IC 序列排序一致性（索引顺序）
    # IC series index order consistency
    # -------------------------------------------------------
    print("\n10. IC series index order consistency")
    factor, returns = make_synthetic(n_dates=200, n_symbols=60, seed=123)
    for cs in [13, 37, 77]:
        ev_full = FactorEvaluator(factor, returns, chunk_size=None)
        ev_full.run_metrics()
        ev_chunked = FactorEvaluator(factor, returns, chunk_size=cs)
        ev_chunked.run_metrics()
        ok(f"cs={cs}: ic index identical", ev_chunked.ic.index.equals(ev_full.ic.index))
        ok(f"cs={cs}: rank_ic index identical", ev_chunked.rank_ic.index.equals(ev_full.rank_ic.index))

    print(f"\n=== Task 9: {checks} checks passed ===")

except Exception as e:
    print(f"\nFATAL: {e}")
    traceback.print_exc()
    sys.exit(1)
