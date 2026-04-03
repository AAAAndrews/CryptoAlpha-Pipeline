"""
验证测试 — Task 4: run_grouping() 分块计算
run_grouping() chunked computation — numerical consistency verification.
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
    print("=== Task 4: run_grouping() 分块计算 ===\n")

    from FactorAnalysis import FactorEvaluator

    # -------------------------------------------------------
    # 1. 基本功能：分块分组计算不报错 / basic: chunked grouping works
    # -------------------------------------------------------
    print("1. Basic chunked grouping computation")
    factor, returns = make_synthetic(n_dates=120, n_symbols=50, seed=42)
    ev_chunked = FactorEvaluator(factor, returns, chunk_size=30)
    ev_chunked.run_grouping()
    ok("chunked group_labels is pd.Series", isinstance(ev_chunked.group_labels, pd.Series))
    ok("chunked group_labels same index as factor",
       ev_chunked.group_labels.index.equals(factor.index))
    ok("chunked group_labels has 120*50 entries", len(ev_chunked.group_labels) == 120 * 50)
    ok("chunked group_labels values in range",
       set(ev_chunked.group_labels.dropna().unique()).issubset(set(range(5))))

    # -------------------------------------------------------
    # 2. 数值一致性：分块 vs 全量分组标签 / label consistency
    # -------------------------------------------------------
    print("\n2. Numerical consistency: chunked vs full group labels")
    ev_full = FactorEvaluator(factor, returns, chunk_size=None)
    ev_full.run_grouping()
    # 分组标签应完全一致（同一时间戳、同一截面数据）
    # group labels should be identical (same timestamp, same cross-section)
    ok("group_labels values match (max diff < 1e-15)",
       np.max(np.abs(ev_chunked.group_labels.values - ev_full.group_labels.values)) < 1e-15)
    ok("group_labels index matches", ev_chunked.group_labels.index.equals(ev_full.group_labels.index))

    # -------------------------------------------------------
    # 3. 截面完整性：每个时间戳的分组数一致 / cross-sectional completeness
    # -------------------------------------------------------
    print("\n3. Cross-sectional completeness")
    labels_per_ts_chunked = ev_chunked.group_labels.groupby(level=0).count()
    labels_per_ts_full = ev_full.group_labels.groupby(level=0).count()
    ok("symbols per timestamp match",
       (labels_per_ts_chunked == labels_per_ts_full).all())
    ok("all timestamps have all 50 symbols",
       (labels_per_ts_chunked == 50).all())

    # -------------------------------------------------------
    # 4. 不同 chunk_size 一致性 / consistency across chunk sizes
    # -------------------------------------------------------
    print("\n4. Consistency across different chunk_size values")
    for cs in [10, 20, 50, 60, 119]:
        ev_cs = FactorEvaluator(factor, returns, chunk_size=cs)
        ev_cs.run_grouping()
        label_diff = np.max(np.abs(ev_cs.group_labels.values - ev_full.group_labels.values))
        ok(f"chunk_size={cs}: labels match (diff={label_diff:.2e})", label_diff < 1e-15)

    # -------------------------------------------------------
    # 5. chunk_size > 数据量时退化为全量 / chunk_size >= n_dates falls back to full
    # -------------------------------------------------------
    print("\n5. chunk_size >= n_dates: single chunk fallback")
    ev_large = FactorEvaluator(factor, returns, chunk_size=200)
    ev_large.run_grouping()
    ok("large chunk_size: labels match",
       np.max(np.abs(ev_large.group_labels.values - ev_full.group_labels.values)) < 1e-15)

    # -------------------------------------------------------
    # 6. 多种子稳定性 / multi-seed stability
    # -------------------------------------------------------
    print("\n6. Multi-seed stability")
    for seed in [0, 7, 99, 12345]:
        f, r = make_synthetic(n_dates=100, n_symbols=40, seed=seed)
        ev_f = FactorEvaluator(f, r, chunk_size=None)
        ev_c = FactorEvaluator(f, r, chunk_size=25)
        ev_f.run_grouping()
        ev_c.run_grouping()
        diff = np.max(np.abs(ev_f.group_labels.values - ev_c.group_labels.values))
        ok(f"seed={seed}: labels match (diff={diff:.2e})", diff < 1e-15)

    # -------------------------------------------------------
    # 7. 边界：含 NaN 因子值 / edge: factor with NaN values
    # -------------------------------------------------------
    print("\n7. Edge: factor with NaN values")
    rng = np.random.default_rng(42)
    f_nan = factor.copy()
    nan_mask = rng.random(len(f_nan)) < 0.1
    f_nan[nan_mask] = np.nan

    ev_f_nan = FactorEvaluator(f_nan, returns, chunk_size=None)
    ev_c_nan = FactorEvaluator(f_nan, returns, chunk_size=30)
    ev_f_nan.run_grouping()
    ev_c_nan.run_grouping()
    # NaN 因子产生 NaN 分组标签，NaN-NaN 差值为 NaN，需跳过 / NaN labels → NaN diff, skip those
    both_valid = ev_f_nan.group_labels.notna() & ev_c_nan.group_labels.notna()
    if both_valid.sum() > 0:
        nan_diff = np.max(np.abs(ev_f_nan.group_labels[both_valid].values - ev_c_nan.group_labels[both_valid].values))
        ok(f"NaN factor: valid labels match (diff={nan_diff:.2e})", nan_diff < 1e-15)
    else:
        ok("NaN factor: no valid labels (skipped)", True)
    # NaN 位置一致 / NaN positions consistent
    ok("NaN factor: NaN positions match",
       (ev_f_nan.group_labels.isna() == ev_c_nan.group_labels.isna()).all())

    # -------------------------------------------------------
    # 8. 边界：少量数据 / edge: small dataset
    # -------------------------------------------------------
    print("\n8. Edge: small dataset (10 dates, 20 symbols)")
    f_small, r_small = make_synthetic(n_dates=10, n_symbols=20, seed=42)
    ev_fs = FactorEvaluator(f_small, r_small, chunk_size=None)
    ev_cs = FactorEvaluator(f_small, r_small, chunk_size=5)
    ev_fs.run_grouping()
    ev_cs.run_grouping()
    ok("small: group_labels computed", ev_cs.group_labels is not None)
    ok("small: same length", len(ev_cs.group_labels) == len(ev_fs.group_labels))
    ok("small: labels match",
       np.max(np.abs(ev_cs.group_labels.values - ev_fs.group_labels.values)) < 1e-15)

    # -------------------------------------------------------
    # 9. 边界：chunk_size=1 / edge: chunk_size=1
    # -------------------------------------------------------
    print("\n9. Edge: chunk_size=1 (one timestamp per chunk)")
    ev_c1 = FactorEvaluator(factor, returns, chunk_size=1)
    ev_c1.run_grouping()
    ok("chunk_size=1: labels computed", ev_c1.group_labels is not None)
    ok("chunk_size=1: labels match full",
       np.max(np.abs(ev_c1.group_labels.values - ev_full.group_labels.values)) < 1e-15)

    # -------------------------------------------------------
    # 10. 向后兼容：chunk_size=None 行为不变 / backward compat
    # -------------------------------------------------------
    print("\n10. Backward compatibility: chunk_size=None unchanged")
    ev_compat = FactorEvaluator(factor, returns)
    ev_compat.run_grouping()
    ok("compat: group_labels identical", ev_compat.group_labels.equals(ev_full.group_labels))

    # -------------------------------------------------------
    # 11. 与 run_metrics 联合运行 / combined with run_metrics
    # -------------------------------------------------------
    print("\n11. Combined: run_metrics + run_grouping in chunked mode")
    ev_combo = FactorEvaluator(factor, returns, chunk_size=30)
    ev_combo.run_metrics().run_grouping()
    ok("combo: ic computed", ev_combo.ic is not None)
    ok("combo: group_labels computed", ev_combo.group_labels is not None)
    ok("combo: group_labels match full",
       np.max(np.abs(ev_combo.group_labels.values - ev_full.group_labels.values)) < 1e-15)

    # -------------------------------------------------------
    # 12. 不同 n_groups / different n_groups
    # -------------------------------------------------------
    print("\n12. Different n_groups in chunked mode")
    for ng in [2, 3, 4, 10]:
        ev_ng_f = FactorEvaluator(factor, returns, n_groups=ng, chunk_size=None)
        ev_ng_c = FactorEvaluator(factor, returns, n_groups=ng, chunk_size=30)
        ev_ng_f.run_grouping()
        ev_ng_c.run_grouping()
        diff = np.max(np.abs(ev_ng_f.group_labels.values - ev_ng_c.group_labels.values))
        ok(f"n_groups={ng}: labels match (diff={diff:.2e})", diff < 1e-15)
        ok(f"n_groups={ng}: values in range",
           set(ev_ng_c.group_labels.dropna().unique()).issubset(set(range(ng))))

    print(f"\n{'='*40}")
    print(f"Total: {checks} checks")

except Exception as e:
    print(f"\nFATAL: {e}")
    traceback.print_exc()
    sys.exit(1)
