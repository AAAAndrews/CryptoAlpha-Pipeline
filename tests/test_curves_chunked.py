"""
验证测试 — Task 5: run_curves() 分块计算
run_curves() chunked computation — numerical consistency verification.
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
    print("=== Task 5: run_curves() 分块计算 ===\n")

    from FactorAnalysis import FactorEvaluator

    # -------------------------------------------------------
    # 1. 基本功能：分块净值曲线计算不报错 / basic: chunked curves work
    # -------------------------------------------------------
    print("1. Basic chunked curves computation")
    factor, returns = make_synthetic(n_dates=120, n_symbols=50, seed=42)
    ev_chunked = FactorEvaluator(factor, returns, chunk_size=30)
    ev_chunked.run_curves()
    ok("chunked long_curve is pd.Series", isinstance(ev_chunked.long_curve, pd.Series))
    ok("chunked short_curve is pd.Series", isinstance(ev_chunked.short_curve, pd.Series))
    ok("chunked hedge_curve is pd.Series", isinstance(ev_chunked.hedge_curve, pd.Series))
    ok("chunked long_curve starts at 1.0", abs(ev_chunked.long_curve.iloc[0] - 1.0) < 1e-12)
    ok("chunked short_curve starts at 1.0", abs(ev_chunked.short_curve.iloc[0] - 1.0) < 1e-12)
    ok("chunked hedge_curve starts at 1.0", abs(ev_chunked.hedge_curve.iloc[0] - 1.0) < 1e-12)
    ok("chunked long_curve has 120 timestamps", len(ev_chunked.long_curve) == 120)
    ok("chunked short_curve has 120 timestamps", len(ev_chunked.short_curve) == 120)
    ok("chunked hedge_curve has 120 timestamps", len(ev_chunked.hedge_curve) == 120)

    # -------------------------------------------------------
    # 2. 数值一致性：分块 vs 全量净值曲线 / curve consistency
    # -------------------------------------------------------
    print("\n2. Numerical consistency: chunked vs full curves")
    ev_full = FactorEvaluator(factor, returns, chunk_size=None)
    ev_full.run_curves()
    # long_curve
    long_diff = np.max(np.abs(ev_chunked.long_curve.values - ev_full.long_curve.values))
    ok(f"long_curve match (max diff={long_diff:.2e})", long_diff < 1e-10)
    # short_curve
    short_diff = np.max(np.abs(ev_chunked.short_curve.values - ev_full.short_curve.values))
    ok(f"short_curve match (max diff={short_diff:.2e})", short_diff < 1e-10)
    # hedge_curve
    hedge_diff = np.max(np.abs(ev_chunked.hedge_curve.values - ev_full.hedge_curve.values))
    ok(f"hedge_curve match (max diff={hedge_diff:.2e})", hedge_diff < 1e-10)
    # timestamps
    ok("long_curve index matches", ev_chunked.long_curve.index.equals(ev_full.long_curve.index))
    ok("short_curve index matches", ev_chunked.short_curve.index.equals(ev_full.short_curve.index))
    ok("hedge_curve index matches", ev_chunked.hedge_curve.index.equals(ev_full.hedge_curve.index))

    # -------------------------------------------------------
    # 3. 成本扣除后曲线一致性 / cost-adjusted curve consistency
    # -------------------------------------------------------
    print("\n3. Cost-adjusted curve consistency")
    ok("hedge_curve_after_cost computed", ev_chunked.hedge_curve_after_cost is not None)
    cost_diff = np.max(np.abs(
        ev_chunked.hedge_curve_after_cost.values - ev_full.hedge_curve_after_cost.values
    ))
    ok(f"hedge_curve_after_cost match (max diff={cost_diff:.2e})", cost_diff < 1e-10)

    # -------------------------------------------------------
    # 4. 绩效比率一致性 / performance ratios consistency
    # -------------------------------------------------------
    print("\n4. Performance ratios consistency")
    ok("sharpe computed", ev_chunked.sharpe is not None)
    ok("calmar computed", ev_chunked.calmar is not None)
    ok("sortino computed", ev_chunked.sortino is not None)
    ok(f"sharpe match (diff={abs(ev_chunked.sharpe - ev_full.sharpe):.2e})",
       abs(ev_chunked.sharpe - ev_full.sharpe) < 1e-8)
    ok(f"calmar match (diff={abs(ev_chunked.calmar - ev_full.calmar):.2e})",
       abs(ev_chunked.calmar - ev_full.calmar) < 1e-8)
    ok(f"sortino match (diff={abs(ev_chunked.sortino - ev_full.sortino):.2e})",
       abs(ev_chunked.sortino - ev_full.sortino) < 1e-8)
    ok(f"sharpe_after_cost match (diff={abs(ev_chunked.sharpe_after_cost - ev_full.sharpe_after_cost):.2e})",
       abs(ev_chunked.sharpe_after_cost - ev_full.sharpe_after_cost) < 1e-8)

    # -------------------------------------------------------
    # 5. 不同 chunk_size 一致性 / consistency across chunk sizes
    # -------------------------------------------------------
    print("\n5. Consistency across different chunk_size values")
    for cs in [10, 20, 50, 60, 119]:
        ev_cs = FactorEvaluator(factor, returns, chunk_size=cs)
        ev_cs.run_curves()
        h_diff = np.max(np.abs(ev_cs.hedge_curve.values - ev_full.hedge_curve.values))
        ok(f"chunk_size={cs}: hedge_curve match (diff={h_diff:.2e})", h_diff < 1e-10)

    # -------------------------------------------------------
    # 6. chunk_size > 数据量时退化为全量 / chunk_size >= n_dates falls back
    # -------------------------------------------------------
    print("\n6. chunk_size >= n_dates: single chunk fallback")
    ev_large = FactorEvaluator(factor, returns, chunk_size=200)
    ev_large.run_curves()
    l_diff = np.max(np.abs(ev_large.long_curve.values - ev_full.long_curve.values))
    ok("large chunk_size: curves match", l_diff < 1e-12)

    # -------------------------------------------------------
    # 7. 多种子稳定性 / multi-seed stability
    # -------------------------------------------------------
    print("\n7. Multi-seed stability")
    for seed in [0, 7, 99, 12345]:
        f, r = make_synthetic(n_dates=100, n_symbols=40, seed=seed)
        ev_f = FactorEvaluator(f, r, chunk_size=None)
        ev_c = FactorEvaluator(f, r, chunk_size=25)
        ev_f.run_curves()
        ev_c.run_curves()
        diff = np.max(np.abs(ev_c.hedge_curve.values - ev_f.hedge_curve.values))
        ok(f"seed={seed}: hedge_curve match (diff={diff:.2e})", diff < 1e-10)

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
    ev_f_nan.run_curves()
    ev_c_nan.run_curves()
    h_nan_diff = np.max(np.abs(ev_c_nan.hedge_curve.values - ev_f_nan.hedge_curve.values))
    ok(f"NaN factor: hedge_curve match (diff={h_nan_diff:.2e})", h_nan_diff < 1e-10)
    ok("NaN factor: same length",
       len(ev_c_nan.hedge_curve) == len(ev_f_nan.hedge_curve))

    # -------------------------------------------------------
    # 9. 边界：少量数据 / edge: small dataset
    # -------------------------------------------------------
    print("\n9. Edge: small dataset (10 dates, 20 symbols)")
    f_small, r_small = make_synthetic(n_dates=10, n_symbols=20, seed=42)
    ev_fs = FactorEvaluator(f_small, r_small, chunk_size=None)
    ev_cs = FactorEvaluator(f_small, r_small, chunk_size=5)
    ev_fs.run_curves()
    ev_cs.run_curves()
    ok("small: curves computed", ev_cs.hedge_curve is not None)
    ok("small: same length", len(ev_cs.hedge_curve) == len(ev_fs.hedge_curve))
    s_diff = np.max(np.abs(ev_cs.hedge_curve.values - ev_fs.hedge_curve.values))
    ok(f"small: hedge_curve match (diff={s_diff:.2e})", s_diff < 1e-10)

    # -------------------------------------------------------
    # 10. 边界：chunk_size=1 / edge: chunk_size=1
    # -------------------------------------------------------
    print("\n10. Edge: chunk_size=1 (one timestamp per chunk)")
    ev_c1 = FactorEvaluator(factor, returns, chunk_size=1)
    ev_c1.run_curves()
    ok("chunk_size=1: curves computed", ev_c1.hedge_curve is not None)
    ok("chunk_size=1: same length",
       len(ev_c1.hedge_curve) == len(ev_full.hedge_curve))
    c1_diff = np.max(np.abs(ev_c1.hedge_curve.values - ev_full.hedge_curve.values))
    ok(f"chunk_size=1: hedge_curve match (diff={c1_diff:.2e})", c1_diff < 1e-10)

    # -------------------------------------------------------
    # 11. 向后兼容：chunk_size=None 行为不变 / backward compat
    # -------------------------------------------------------
    print("\n11. Backward compatibility: chunk_size=None unchanged")
    ev_compat = FactorEvaluator(factor, returns)
    ev_compat.run_curves()
    ok("compat: long_curve identical", ev_compat.long_curve.equals(ev_full.long_curve))
    ok("compat: short_curve identical", ev_compat.short_curve.equals(ev_full.short_curve))
    ok("compat: hedge_curve identical", ev_compat.hedge_curve.equals(ev_full.hedge_curve))
    ok("compat: hedge_curve_after_cost identical",
       ev_compat.hedge_curve_after_cost.equals(ev_full.hedge_curve_after_cost))

    # -------------------------------------------------------
    # 12. 与 run_metrics + run_grouping 联合运行 / combined operations
    # -------------------------------------------------------
    print("\n12. Combined: run_metrics + run_grouping + run_curves")
    ev_combo = FactorEvaluator(factor, returns, chunk_size=30)
    ev_combo.run_metrics().run_grouping().run_curves()
    ok("combo: ic computed", ev_combo.ic is not None)
    ok("combo: group_labels computed", ev_combo.group_labels is not None)
    ok("combo: hedge_curve computed", ev_combo.hedge_curve is not None)
    combo_diff = np.max(np.abs(ev_combo.hedge_curve.values - ev_full.hedge_curve.values))
    ok(f"combo: hedge_curve match (diff={combo_diff:.2e})", combo_diff < 1e-10)

    # -------------------------------------------------------
    # 13. 不同 top_k / bottom_k 组合 / different top/bottom params
    # -------------------------------------------------------
    print("\n13. Different top_k / bottom_k in chunked mode")
    for top, bot in [(1, 1), (2, 1), (1, 2), (2, 2)]:
        ev_p_f = FactorEvaluator(factor, returns, top_k=top, bottom_k=bot, chunk_size=None)
        ev_p_c = FactorEvaluator(factor, returns, top_k=top, bottom_k=bot, chunk_size=30)
        ev_p_f.run_curves()
        ev_p_c.run_curves()
        diff = np.max(np.abs(ev_p_c.hedge_curve.values - ev_p_f.hedge_curve.values))
        ok(f"top_k={top}, bottom_k={bot}: match (diff={diff:.2e})", diff < 1e-10)

    # -------------------------------------------------------
    # 14. 不同 n_groups / different n_groups
    # -------------------------------------------------------
    print("\n14. Different n_groups in chunked mode")
    for ng in [2, 3, 4, 10]:
        ev_ng_f = FactorEvaluator(factor, returns, n_groups=ng, chunk_size=None)
        ev_ng_c = FactorEvaluator(factor, returns, n_groups=ng, chunk_size=30)
        ev_ng_f.run_curves()
        ev_ng_c.run_curves()
        diff = np.max(np.abs(ev_ng_c.hedge_curve.values - ev_ng_f.hedge_curve.values))
        ok(f"n_groups={ng}: hedge_curve match (diff={diff:.2e})", diff < 1e-10)

    # -------------------------------------------------------
    # 15. 不同 cost_rate / different cost_rate
    # -------------------------------------------------------
    print("\n15. Different cost_rate in chunked mode")
    for cr in [0.0, 0.0005, 0.002, 0.005]:
        ev_cr_f = FactorEvaluator(factor, returns, cost_rate=cr, chunk_size=None)
        ev_cr_c = FactorEvaluator(factor, returns, cost_rate=cr, chunk_size=30)
        ev_cr_f.run_curves()
        ev_cr_c.run_curves()
        diff = np.max(np.abs(ev_cr_c.hedge_curve_after_cost.values - ev_cr_f.hedge_curve_after_cost.values))
        ok(f"cost_rate={cr}: after_cost match (diff={diff:.2e})", diff < 1e-10)

    # -------------------------------------------------------
    # 16. _merge_raw_curves 边界：空列表 / edge: empty list
    # -------------------------------------------------------
    print("\n16. Edge: _merge_raw_curves with empty list")
    from FactorAnalysis.evaluator import _merge_raw_curves
    empty_result = _merge_raw_curves([])
    ok("empty list returns empty Series", len(empty_result) == 0)
    ok("empty list dtype is float", empty_result.dtype == np.float64)

    # -------------------------------------------------------
    # 17. _merge_raw_curves 单块 / single chunk
    # -------------------------------------------------------
    print("\n17. Edge: _merge_raw_curves with single chunk")
    single_curve = pd.Series([1.02, 1.05, 1.03], index=pd.date_range("2025-01-01", periods=3, freq="B"))
    merged_single = _merge_raw_curves([single_curve])
    ok("single chunk: unchanged", merged_single.equals(single_curve))

    # -------------------------------------------------------
    # 18. _merge_raw_curves 连续性验证 / continuity check
    # -------------------------------------------------------
    print("\n18. _merge_raw_curves continuity verification")
    c1 = pd.Series([1.02, 1.05, 1.03], index=pd.date_range("2025-01-01", periods=3, freq="B"))
    c2 = pd.Series([0.99, 1.01, 1.04], index=pd.date_range("2025-01-06", periods=3, freq="B"))
    merged = _merge_raw_curves([c1, c2])
    ok("merged length = 6", len(merged) == 6)
    # 第一个块保持不变 / first chunk unchanged
    ok("first chunk values preserved",
       np.allclose(merged.values[:3], c1.values))
    # 第二个块缩放：c2 * c1[-1] = c2 * 1.03
    scale = c1.iloc[-1]
    ok("second chunk scaled correctly",
       np.allclose(merged.values[3:], c2.values * scale))

    print(f"\n{'='*40}")
    print(f"Total: {checks} checks")

except Exception as e:
    print(f"\nFATAL: {e}")
    traceback.print_exc()
    sys.exit(1)
