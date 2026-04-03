"""
验证测试 — Task 3: run_metrics() IC/IR 分块计算
run_metrics() IC/IR chunked computation — numerical consistency verification.
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


def make_synthetic(n_dates=120, n_symbols=50, seed=42):
    """生成合成因子和收益率 / Generate synthetic factor and returns."""
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2025-01-01", periods=n_dates, freq="B")
    symbols = [f"S{i:03d}" for i in range(n_symbols)]
    idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])

    true_signal = rng.standard_normal((n_dates, n_symbols))
    noise = rng.standard_normal((n_dates, n_symbols)) * 0.5
    factor_values = true_signal + noise
    returns_values = true_signal * 0.02 + rng.standard_normal((n_dates, n_symbols)) * 0.03

    factor = pd.Series(factor_values.ravel(), index=idx, dtype=np.float64)
    returns = pd.Series(returns_values.ravel(), index=idx, dtype=np.float64)
    return factor, returns


try:
    print("=== Task 3: run_metrics() IC/IR 分块计算 ===\n")

    from FactorAnalysis import FactorEvaluator

    # -------------------------------------------------------
    # 1. 基本功能：分块 IC 计算不报错 / basic: chunked IC computation works
    # -------------------------------------------------------
    print("1. Basic chunked IC computation")
    factor, returns = make_synthetic(n_dates=120, n_symbols=50, seed=42)
    ev_chunked = FactorEvaluator(factor, returns, chunk_size=30)
    ev_chunked.run_metrics()
    ok("chunked ic is pd.Series", isinstance(ev_chunked.ic, pd.Series))
    ok("chunked ic has 120 timestamps", len(ev_chunked.ic) == 120)
    ok("chunked ic no NaN", ev_chunked.ic.notna().all())
    ok("chunked rank_ic is pd.Series", isinstance(ev_chunked.rank_ic, pd.Series))
    ok("chunked rank_ic has 120 timestamps", len(ev_chunked.rank_ic) == 120)
    ok("chunked rank_ic no NaN", ev_chunked.rank_ic.notna().all())
    ok("chunked icir is float", isinstance(ev_chunked.icir, float))
    ok("chunked icir is finite", np.isfinite(ev_chunked.icir))
    ok("chunked ic_stats is pd.Series", isinstance(ev_chunked.ic_stats, pd.Series))
    ok("chunked ic_stats has IC_mean", "IC_mean" in ev_chunked.ic_stats.index)

    # -------------------------------------------------------
    # 2. 数值一致性：分块 vs 全量 IC 序列 / IC series consistency
    # -------------------------------------------------------
    print("\n2. Numerical consistency: chunked vs full IC series")
    ev_full = FactorEvaluator(factor, returns, chunk_size=None)
    ev_full.run_metrics()
    # IC 序列应完全一致（同一时间戳、同一截面数据） / IC series should be identical
    ok("ic values match (max diff < 1e-15)",
       np.max(np.abs(ev_chunked.ic.values - ev_full.ic.values)) < 1e-15)
    ok("rank_ic values match (max diff < 1e-15)",
       np.max(np.abs(ev_chunked.rank_ic.values - ev_full.rank_ic.values)) < 1e-15)
    # IC 索引一致 / IC index consistent
    ok("ic index matches", ev_chunked.ic.index.equals(ev_full.ic.index))
    ok("rank_ic index matches", ev_chunked.rank_ic.index.equals(ev_full.rank_ic.index))

    # -------------------------------------------------------
    # 3. 数值一致性：ICIR / ICIR consistency
    # -------------------------------------------------------
    print("\n3. Numerical consistency: ICIR")
    ok("ICIR match (diff < 1e-10)", abs(ev_chunked.icir - ev_full.icir) < 1e-10)

    # -------------------------------------------------------
    # 4. 数值一致性：IC 统计量 / IC stats consistency
    # -------------------------------------------------------
    print("\n4. Numerical consistency: IC stats")
    for field in ["IC_mean", "IC_std", "ICIR", "t_stat", "p_value", "IC_skew", "IC_kurtosis"]:
        diff = abs(ev_chunked.ic_stats[field] - ev_full.ic_stats[field])
        ok(f"ic_stats[{field}] match (diff={diff:.2e})", diff < 1e-8)

    # -------------------------------------------------------
    # 5. 不同 chunk_size 一致性 / consistency across chunk sizes
    # -------------------------------------------------------
    print("\n5. Consistency across different chunk_size values")
    for cs in [10, 20, 50, 60, 119]:
        ev_cs = FactorEvaluator(factor, returns, chunk_size=cs)
        ev_cs.run_metrics()
        ic_diff = np.max(np.abs(ev_cs.ic.values - ev_full.ic.values))
        rank_diff = np.max(np.abs(ev_cs.rank_ic.values - ev_full.rank_ic.values))
        icir_diff = abs(ev_cs.icir - ev_full.icir)
        ok(f"chunk_size={cs}: ic match ({ic_diff:.2e})", ic_diff < 1e-15)
        ok(f"chunk_size={cs}: rank_ic match ({rank_diff:.2e})", rank_diff < 1e-15)
        ok(f"chunk_size={cs}: icir match ({icir_diff:.2e})", icir_diff < 1e-10)

    # -------------------------------------------------------
    # 6. chunk_size > 数据量时退化为全量 / chunk_size >= n_dates falls back to full
    # -------------------------------------------------------
    print("\n6. chunk_size >= n_dates: single chunk fallback")
    ev_large = FactorEvaluator(factor, returns, chunk_size=200)
    ev_large.run_metrics()
    ok("large chunk_size: ic match", np.max(np.abs(ev_large.ic.values - ev_full.ic.values)) < 1e-15)
    ok("large chunk_size: icir match", abs(ev_large.icir - ev_full.icir) < 1e-10)
    ok("large chunk_size: ic_stats match",
       abs(ev_large.ic_stats["IC_mean"] - ev_full.ic_stats["IC_mean"]) < 1e-8)

    # -------------------------------------------------------
    # 7. 多种子稳定性 / multi-seed stability
    # -------------------------------------------------------
    print("\n7. Multi-seed stability")
    for seed in [0, 7, 99, 12345]:
        f, r = make_synthetic(n_dates=100, n_symbols=40, seed=seed)
        ev_f = FactorEvaluator(f, r, chunk_size=None)
        ev_c = FactorEvaluator(f, r, chunk_size=25)
        ev_f.run_metrics()
        ev_c.run_metrics()
        ic_diff = np.max(np.abs(ev_f.ic.values - ev_c.ic.values))
        ok(f"seed={seed}: ic match ({ic_diff:.2e})", ic_diff < 1e-15)
        ok(f"seed={seed}: icir match (diff={abs(ev_f.icir - ev_c.icir):.2e})",
           abs(ev_f.icir - ev_c.icir) < 1e-10)

    # -------------------------------------------------------
    # 8. 边界：含 NaN 因子值 / edge: factor with NaN values
    # -------------------------------------------------------
    print("\n8. Edge: factor with NaN values")
    rng = np.random.default_rng(42)
    f_nan = factor.copy()
    nan_mask = rng.random(len(f_nan)) < 0.1
    f_nan[nan_mask] = np.nan

    ev_f_nan = FactorEvaluator(f_nan, returns, chunk_size=None)
    ev_c_nan = FactorEvaluator(f_nan, returns, chunk_size=30)
    ev_f_nan.run_metrics()
    ev_c_nan.run_metrics()
    # NaN 后部分时间截面可能产生 NaN IC，但非 NaN 值应一致
    # some timestamps may yield NaN IC after injecting NaN, but non-NaN values should match
    both_valid = ev_f_nan.ic.notna() & ev_c_nan.ic.notna()
    if both_valid.sum() > 0:
        diff = np.max(np.abs(ev_f_nan.ic[both_valid].values - ev_c_nan.ic[both_valid].values))
        ok(f"NaN factor: valid IC values match (diff={diff:.2e})", diff < 1e-15)
    else:
        ok("NaN factor: no valid IC (skipped)", True)
    ok("NaN factor: icir both finite or both zero",
       (np.isfinite(ev_f_nan.icir) and np.isfinite(ev_c_nan.icir))
       or (ev_f_nan.icir == 0.0 and ev_c_nan.icir == 0.0))

    # -------------------------------------------------------
    # 9. 边界：少量数据 / edge: small dataset
    # -------------------------------------------------------
    print("\n9. Edge: small dataset (10 dates, 20 symbols)")
    f_small, r_small = make_synthetic(n_dates=10, n_symbols=20, seed=42)
    ev_fs = FactorEvaluator(f_small, r_small, chunk_size=None)
    ev_cs = FactorEvaluator(f_small, r_small, chunk_size=5)
    ev_fs.run_metrics()
    ev_cs.run_metrics()
    ok("small: ic computed", ev_cs.ic is not None)
    ok("small: ic same length", len(ev_cs.ic) == len(ev_fs.ic))
    if len(ev_cs.ic.dropna()) >= 2:
        ok("small: ic values match",
           np.max(np.abs(ev_cs.ic.dropna().values - ev_fs.ic.dropna().values)) < 1e-15)

    # -------------------------------------------------------
    # 10. 向后兼容：chunk_size=None 行为不变 / backward compat
    # -------------------------------------------------------
    print("\n10. Backward compatibility: chunk_size=None unchanged")
    ev_compat = FactorEvaluator(factor, returns)
    ev_compat.run_metrics()
    ok("compat: ic identical", ev_compat.ic.equals(ev_full.ic))
    ok("compat: rank_ic identical", ev_compat.rank_ic.equals(ev_full.rank_ic))
    ok("compat: icir identical", ev_compat.icir == ev_full.icir)
    for field in ["IC_mean", "IC_std", "ICIR", "t_stat", "p_value"]:
        ok(f"compat: ic_stats[{field}] identical",
           ev_compat.ic_stats[field] == ev_full.ic_stats[field])

    print(f"\n{'='*40}")
    print(f"Total: {checks} checks")

except Exception as e:
    print(f"\nFATAL: {e}")
    traceback.print_exc()
    sys.exit(1)
