"""
验证测试 — Task 7: run_neutralize() 分块计算
run_neutralize() chunked computation — numerical consistency verification.

覆盖范围:
- 基本功能：分块中性化曲线计算 / Basic chunked neutralized curve
- 数值一致性：分块 vs 全量 / Numerical consistency (chunked vs full)
- 不同 chunk_size / demeaned / group_adjust / 多种子 / NaN / 小数据集 / chunk_size=1
- 向后兼容 / Backward compatibility
- generate_report 中 neutralize 板块 / report neutralize section
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
    print("=== Task 7: run_neutralize() 分块计算 ===\n")

    from FactorAnalysis import FactorEvaluator

    # -------------------------------------------------------
    # 1. 基本功能：分块中性化曲线计算不报错 / basic: chunked neutralize works
    # -------------------------------------------------------
    print("1. Basic chunked neutralized curve computation")
    factor, returns = make_synthetic(n_dates=120, n_symbols=50, seed=42)
    ev_chunked = FactorEvaluator(factor, returns, chunk_size=30)
    ev_chunked.run_neutralize()
    ok("chunked neutralized_curve is pd.Series", isinstance(ev_chunked.neutralized_curve, pd.Series))
    ok("neutralized_curve has 120 rows", len(ev_chunked.neutralized_curve) == 120)
    ok("neutralized_curve starts at 1.0", ev_chunked.neutralized_curve.iloc[0] == 1.0)

    # -------------------------------------------------------
    # 2. 数值一致性：分块 vs 全量（demeaned=True）/ consistency: demeaned=True
    # -------------------------------------------------------
    print("\n2. Numerical consistency: chunked vs full (demeaned=True)")
    ev_full = FactorEvaluator(factor, returns, chunk_size=None)
    ev_full.run_neutralize()
    max_diff = np.max(np.abs(ev_full.neutralized_curve.values - ev_chunked.neutralized_curve.values))
    ok(f"chunked vs full match (max diff={max_diff:.2e})", max_diff < 1e-10)

    # -------------------------------------------------------
    # 3. 数值一致性：分块 vs 全量（demeaned=False）/ consistency: demeaned=False
    # -------------------------------------------------------
    print("\n3. Numerical consistency: chunked vs full (demeaned=False)")
    ev_f2 = FactorEvaluator(factor, returns, chunk_size=None)
    ev_f2.run_neutralize(demeaned=False)
    ev_c2 = FactorEvaluator(factor, returns, chunk_size=30)
    ev_c2.run_neutralize(demeaned=False)
    max_diff2 = np.max(np.abs(ev_f2.neutralized_curve.values - ev_c2.neutralized_curve.values))
    ok(f"demeaned=False: match (max diff={max_diff2:.2e})", max_diff2 < 1e-10)

    # -------------------------------------------------------
    # 4. 数值一致性：分块 vs 全量（group_adjust=True）/ consistency: group_adjust=True
    # -------------------------------------------------------
    print("\n4. Numerical consistency: chunked vs full (group_adjust=True)")
    ev_f3 = FactorEvaluator(factor, returns, chunk_size=None)
    ev_f3.run_neutralize(group_adjust=True)
    ev_c3 = FactorEvaluator(factor, returns, chunk_size=30)
    ev_c3.run_neutralize(group_adjust=True)
    max_diff3 = np.max(np.abs(ev_f3.neutralized_curve.values - ev_c3.neutralized_curve.values))
    ok(f"group_adjust=True: match (max diff={max_diff3:.2e})", max_diff3 < 1e-10)

    # -------------------------------------------------------
    # 5. 数值一致性：分块 vs 全量（demeaned=True, group_adjust=True）/ both flags
    # -------------------------------------------------------
    print("\n5. Numerical consistency: chunked vs full (demeaned + group_adjust)")
    ev_f4 = FactorEvaluator(factor, returns, chunk_size=None)
    ev_f4.run_neutralize(demeaned=True, group_adjust=True)
    ev_c4 = FactorEvaluator(factor, returns, chunk_size=30)
    ev_c4.run_neutralize(demeaned=True, group_adjust=True)
    max_diff4 = np.max(np.abs(ev_f4.neutralized_curve.values - ev_c4.neutralized_curve.values))
    ok(f"both flags: match (max diff={max_diff4:.2e})", max_diff4 < 1e-10)

    # -------------------------------------------------------
    # 6. 不同 chunk_size 一致性 / consistency across chunk sizes
    # -------------------------------------------------------
    print("\n6. Consistency across different chunk_size values")
    for cs in [10, 20, 50, 60, 119]:
        ev_cs = FactorEvaluator(factor, returns, chunk_size=cs)
        ev_cs.run_neutralize()
        diff = np.max(np.abs(ev_full.neutralized_curve.values - ev_cs.neutralized_curve.values))
        ok(f"chunk_size={cs}: match (max diff={diff:.2e})", diff < 1e-10)

    # -------------------------------------------------------
    # 7. chunk_size > 数据量时退化为全量 / chunk_size >= n_dates falls back
    # -------------------------------------------------------
    print("\n7. chunk_size >= n_dates: single chunk fallback")
    ev_large = FactorEvaluator(factor, returns, chunk_size=200)
    ev_large.run_neutralize()
    diff_large = np.max(np.abs(ev_full.neutralized_curve.values - ev_large.neutralized_curve.values))
    ok(f"large chunk_size: match (diff={diff_large:.2e})", diff_large < 1e-10)

    # -------------------------------------------------------
    # 8. 多种子稳定性 / multi-seed stability
    # -------------------------------------------------------
    print("\n8. Multi-seed stability")
    for seed in [0, 7, 99, 12345]:
        f, r = make_synthetic(n_dates=100, n_symbols=40, seed=seed)
        ev_f = FactorEvaluator(f, r, chunk_size=None)
        ev_c = FactorEvaluator(f, r, chunk_size=25)
        ev_f.run_neutralize()
        ev_c.run_neutralize()
        diff = np.max(np.abs(ev_f.neutralized_curve.values - ev_c.neutralized_curve.values))
        ok(f"seed={seed}: match (max diff={diff:.2e})", diff < 1e-10)

    # -------------------------------------------------------
    # 9. 边界：含 NaN 因子值 / edge: factor with NaN values
    # -------------------------------------------------------
    print("\n9. Edge: factor with NaN values")
    rng = np.random.default_rng(42)
    f_nan = factor.copy()
    nan_mask = rng.random(len(f_nan)) < 0.1
    f_nan[nan_mask] = np.nan

    ev_f_nan = FactorEvaluator(f_nan, returns, chunk_size=None)
    ev_c_nan = FactorEvaluator(f_nan, returns, chunk_size=30)
    ev_f_nan.run_neutralize()
    ev_c_nan.run_neutralize()
    diff_nan = np.max(np.abs(ev_f_nan.neutralized_curve.values - ev_c_nan.neutralized_curve.values))
    ok(f"NaN factor: match (max diff={diff_nan:.2e})", diff_nan < 1e-10)
    ok("NaN factor: same length", len(ev_c_nan.neutralized_curve) == len(ev_f_nan.neutralized_curve))

    # -------------------------------------------------------
    # 10. 边界：少量数据 / edge: small dataset
    # -------------------------------------------------------
    print("\n10. Edge: small dataset (10 dates, 20 symbols)")
    f_small, r_small = make_synthetic(n_dates=10, n_symbols=20, seed=42)
    ev_fs = FactorEvaluator(f_small, r_small, chunk_size=None)
    ev_cs = FactorEvaluator(f_small, r_small, chunk_size=5)
    ev_fs.run_neutralize()
    ev_cs.run_neutralize()
    ok("small: neutralized_curve computed", ev_cs.neutralized_curve is not None)
    ok("small: same length", len(ev_cs.neutralized_curve) == len(ev_fs.neutralized_curve))
    diff_small = np.max(np.abs(ev_fs.neutralized_curve.values - ev_cs.neutralized_curve.values))
    ok(f"small: match (max diff={diff_small:.2e})", diff_small < 1e-10)

    # -------------------------------------------------------
    # 11. 边界：chunk_size=1 / edge: chunk_size=1
    # -------------------------------------------------------
    print("\n11. Edge: chunk_size=1 (one timestamp per chunk)")
    ev_c1 = FactorEvaluator(factor, returns, chunk_size=1)
    ev_c1.run_neutralize()
    ok("chunk_size=1: neutralized_curve computed", ev_c1.neutralized_curve is not None)
    ok("chunk_size=1: same length", len(ev_c1.neutralized_curve) == len(ev_full.neutralized_curve))
    ok("chunk_size=1: starts at 1.0", ev_c1.neutralized_curve.iloc[0] == 1.0)

    # -------------------------------------------------------
    # 12. 向后兼容：chunk_size=None 行为不变 / backward compat
    # -------------------------------------------------------
    print("\n12. Backward compatibility: chunk_size=None unchanged")
    ev_compat = FactorEvaluator(factor, returns)
    ev_compat.run_neutralize()
    ok("compat: identical", ev_compat.neutralized_curve.equals(ev_full.neutralized_curve))

    # -------------------------------------------------------
    # 13. 不同 n_groups / different n_groups
    # -------------------------------------------------------
    print("\n13. Different n_groups in chunked mode")
    for ng in [2, 3, 4, 10]:
        ev_ng_f = FactorEvaluator(factor, returns, n_groups=ng, chunk_size=None)
        ev_ng_c = FactorEvaluator(factor, returns, n_groups=ng, chunk_size=30)
        ev_ng_f.run_neutralize(n_groups=ng)
        ev_ng_c.run_neutralize(n_groups=ng)
        diff = np.max(np.abs(ev_ng_f.neutralized_curve.values - ev_ng_c.neutralized_curve.values))
        ok(f"n_groups={ng}: match (max diff={diff:.2e})", diff < 1e-10)

    # -------------------------------------------------------
    # 14. 自定义 groups 参数（pd.Series）/ custom groups Series
    # -------------------------------------------------------
    print("\n14. Custom groups (pd.Series)")
    from FactorAnalysis.grouping import quantile_group
    custom_groups = quantile_group(factor, n_groups=3)
    ev_cg_f = FactorEvaluator(factor, returns, chunk_size=None)
    ev_cg_c = FactorEvaluator(factor, returns, chunk_size=30)
    ev_cg_f.run_neutralize(groups=custom_groups)
    ev_cg_c.run_neutralize(groups=custom_groups)
    diff_cg = np.max(np.abs(ev_cg_f.neutralized_curve.values - ev_cg_c.neutralized_curve.values))
    ok(f"custom groups: match (max diff={diff_cg:.2e})", diff_cg < 1e-10)

    # -------------------------------------------------------
    # 15. 与其他子方法联合运行 / combined with other sub-methods
    # -------------------------------------------------------
    print("\n15. Combined: run_metrics + run_neutralize")
    ev_combo = FactorEvaluator(factor, returns, chunk_size=30)
    ev_combo.run_metrics().run_neutralize()
    ok("combo: ic computed", ev_combo.ic is not None)
    ok("combo: neutralized_curve computed", ev_combo.neutralized_curve is not None)

    # -------------------------------------------------------
    # 16. generate_report 中 neutralize 板块 / report neutralize section
    # -------------------------------------------------------
    print("\n16. generate_report: neutralize section")
    ev_report = FactorEvaluator(factor, returns, chunk_size=30)
    ev_report.run_neutralize()
    report = ev_report.generate_report(select=["neutralize"])
    ok("report contains neutralized_return", "neutralized_return" in report.columns)

    # -------------------------------------------------------
    # 17. run_all() 包含 neutralize / run_all includes neutralize
    # -------------------------------------------------------
    print("\n17. run_all() with chunked mode")
    ev_all = FactorEvaluator(factor, returns, chunk_size=30)
    ev_all.run_all()
    ok("run_all: neutralized_curve computed", ev_all.neutralized_curve is not None)
    ok("run_all: starts at 1.0", ev_all.neutralized_curve.iloc[0] == 1.0)
    diff_all = np.max(np.abs(ev_full.neutralized_curve.values - ev_all.neutralized_curve.values))
    ok(f"run_all: match full mode (max diff={diff_all:.2e})", diff_all < 1e-10)

    # -------------------------------------------------------
    # 18. _raw 参数向后兼容 / _raw backward compat
    # -------------------------------------------------------
    print("\n18. _raw parameter backward compat")
    from FactorAnalysis.neutralize import calc_neutralized_curve
    curve_raw = calc_neutralized_curve(factor, returns, groups=3, _raw=True)
    curve_normal = calc_neutralized_curve(factor, returns, groups=3, _raw=False)
    ok("_raw=True: first value NOT forced to 1.0", curve_raw.iloc[0] != 1.0)
    ok("_raw=False: first value IS 1.0", curve_normal.iloc[0] == 1.0)

    print(f"\n{'='*40}")
    print(f"Total: {checks} checks")

except Exception as e:
    print(f"\nFATAL: {e}")
    traceback.print_exc()
    sys.exit(1)
