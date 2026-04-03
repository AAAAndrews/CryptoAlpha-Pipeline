"""
验证测试 — Task 6: run_turnover() 分块计算
run_turnover() chunked computation — numerical consistency verification.

覆盖范围:
- 基本功能：分块换手率/排名自相关计算 / Basic chunked turnover/rank_autocorr
- 数值一致性：分块 vs 全量（跨块边界除外）/ Numerical consistency (except boundaries)
- 跨块边界 NaN 验证 / Cross-chunk boundary NaN verification
- 不同 chunk_size / n_groups / 多种子 / NaN / 小数据集 / chunk_size=1
- 向后兼容 / Backward compatibility
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


def _get_boundary_indices(n_dates: int, chunk_size: int) -> set:
    """获取跨块边界的全局索引（后续块的首个时间截面位置）/ Get chunk boundary global indices."""
    boundaries = set()
    ts = pd.date_range("2025-01-01", periods=n_dates, freq="B")
    # 模拟 split_into_chunks 的分块逻辑 / simulate split_into_chunks logic
    starts = list(range(0, n_dates, chunk_size))
    for i in range(1, len(starts)):
        boundaries.add(starts[i])
    return boundaries


try:
    print("=== Task 6: run_turnover() 分块计算 ===\n")

    from FactorAnalysis import FactorEvaluator

    # -------------------------------------------------------
    # 1. 基本功能：分块换手率计算不报错 / basic: chunked turnover works
    # -------------------------------------------------------
    print("1. Basic chunked turnover computation")
    factor, returns = make_synthetic(n_dates=120, n_symbols=50, seed=42)
    ev_chunked = FactorEvaluator(factor, returns, chunk_size=30)
    ev_chunked.run_turnover()
    ok("chunked turnover is pd.DataFrame", isinstance(ev_chunked.turnover, pd.DataFrame))
    ok("chunked rank_autocorr is pd.Series", isinstance(ev_chunked.rank_autocorr, pd.Series))
    ok("turnover has 120 rows", len(ev_chunked.turnover) == 120)
    ok("rank_autocorr has 120 rows", len(ev_chunked.rank_autocorr) == 120)
    ok("turnover columns match n_groups", list(ev_chunked.turnover.columns) == [0, 1, 2, 3, 4])

    # -------------------------------------------------------
    # 2. 数值一致性：分块 vs 全量换手率（跨块边界除外）/ turnover consistency
    # -------------------------------------------------------
    print("\n2. Numerical consistency: chunked vs full turnover (excluding boundaries)")
    ev_full = FactorEvaluator(factor, returns, chunk_size=None)
    ev_full.run_turnover()
    boundaries = _get_boundary_indices(120, 30)
    # 非边界行的换手率应完全一致 / non-boundary turnover should match exactly
    for idx in range(120):
        if idx in boundaries or idx == 0:
            continue
        full_row = ev_full.turnover.iloc[idx]
        chunk_row = ev_chunked.turnover.iloc[idx]
        ok(f"turnover row {idx} match", np.allclose(full_row.values, chunk_row.values, equal_nan=True))

    # -------------------------------------------------------
    # 3. 跨块边界 NaN 验证：换手率 / boundary NaN: turnover
    # -------------------------------------------------------
    print("\n3. Cross-chunk boundary NaN: turnover")
    for b_idx in boundaries:
        ok(f"turnover boundary row {b_idx} is NaN",
           ev_chunked.turnover.iloc[b_idx].isna().all())

    # -------------------------------------------------------
    # 4. 数值一致性：分块 vs 全量排名自相关（跨块边界除外）/ autocorr consistency
    # -------------------------------------------------------
    print("\n4. Numerical consistency: chunked vs full rank_autocorr (excluding boundaries)")
    for idx in range(120):
        if idx in boundaries or idx == 0:
            continue
        full_val = ev_full.rank_autocorr.iloc[idx]
        chunk_val = ev_chunked.rank_autocorr.iloc[idx]
        if np.isnan(full_val) and np.isnan(chunk_val):
            ok(f"rank_autocorr row {idx} both NaN", True)
        elif np.isnan(full_val) or np.isnan(chunk_val):
            ok(f"rank_autocorr row {idx} NaN mismatch", False)
        else:
            ok(f"rank_autocorr row {idx} match (diff={abs(full_val - chunk_val):.2e})",
               abs(full_val - chunk_val) < 1e-12)

    # -------------------------------------------------------
    # 5. 跨块边界 NaN 验证：排名自相关 / boundary NaN: rank_autocorr
    # -------------------------------------------------------
    print("\n5. Cross-chunk boundary NaN: rank_autocorr")
    for b_idx in boundaries:
        ok(f"rank_autocorr boundary {b_idx} is NaN", np.isnan(ev_chunked.rank_autocorr.iloc[b_idx]))

    # -------------------------------------------------------
    # 6. 首期始终为 NaN / first period always NaN
    # -------------------------------------------------------
    print("\n6. First period is NaN")
    ok("turnover first row NaN", ev_chunked.turnover.iloc[0].isna().all())
    ok("rank_autocorr first value NaN", np.isnan(ev_chunked.rank_autocorr.iloc[0]))

    # -------------------------------------------------------
    # 7. 不同 chunk_size 一致性 / consistency across chunk sizes
    # -------------------------------------------------------
    print("\n7. Consistency across different chunk_size values")
    for cs in [10, 20, 50, 60, 119]:
        ev_cs = FactorEvaluator(factor, returns, chunk_size=cs)
        ev_cs.run_turnover()
        b_set = _get_boundary_indices(120, cs)
        # 非边界行应一致 / non-boundary rows should match
        max_diff = 0.0
        for idx in range(120):
            if idx in b_set or idx == 0:
                continue
            diff = np.max(np.abs(
                ev_full.turnover.iloc[idx].values - ev_cs.turnover.iloc[idx].values
            ))
            max_diff = max(max_diff, diff)
        ok(f"chunk_size={cs}: turnover non-boundary match (max diff={max_diff:.2e})", max_diff < 1e-12)

    # -------------------------------------------------------
    # 8. chunk_size > 数据量时退化为全量 / chunk_size >= n_dates falls back
    # -------------------------------------------------------
    print("\n8. chunk_size >= n_dates: single chunk fallback")
    ev_large = FactorEvaluator(factor, returns, chunk_size=200)
    ev_large.run_turnover()
    t_diff = np.nanmax(np.abs(
        ev_large.turnover.values - ev_full.turnover.values
    ))
    a_diff = np.nanmax(np.abs(
        ev_large.rank_autocorr.values - ev_full.rank_autocorr.values
    ))
    ok(f"large chunk_size: turnover match (diff={t_diff:.2e})", t_diff < 1e-12)
    ok(f"large chunk_size: rank_autocorr match (diff={a_diff:.2e})", a_diff < 1e-12)

    # -------------------------------------------------------
    # 9. 多种子稳定性 / multi-seed stability
    # -------------------------------------------------------
    print("\n9. Multi-seed stability")
    for seed in [0, 7, 99, 12345]:
        f, r = make_synthetic(n_dates=100, n_symbols=40, seed=seed)
        ev_f = FactorEvaluator(f, r, chunk_size=None)
        ev_c = FactorEvaluator(f, r, chunk_size=25)
        ev_f.run_turnover()
        ev_c.run_turnover()
        b_set = _get_boundary_indices(100, 25)
        max_diff = 0.0
        for idx in range(100):
            if idx in b_set or idx == 0:
                continue
            diff = np.max(np.abs(
                ev_f.turnover.iloc[idx].values - ev_c.turnover.iloc[idx].values
            ))
            max_diff = max(max_diff, diff)
        ok(f"seed={seed}: turnover non-boundary match (diff={max_diff:.2e})", max_diff < 1e-12)

    # -------------------------------------------------------
    # 10. 边界：含 NaN 因子值 / edge: factor with NaN values
    # -------------------------------------------------------
    print("\n10. Edge: factor with NaN values")
    rng = np.random.default_rng(42)
    f_nan = factor.copy()
    nan_mask = rng.random(len(f_nan)) < 0.1
    f_nan[nan_mask] = np.nan

    ev_f_nan = FactorEvaluator(f_nan, returns, chunk_size=None)
    ev_c_nan = FactorEvaluator(f_nan, returns, chunk_size=30)
    ev_f_nan.run_turnover()
    ev_c_nan.run_turnover()
    b_set = _get_boundary_indices(120, 30)
    max_diff = 0.0
    for idx in range(120):
        if idx in b_set or idx == 0:
            continue
        diff = np.nanmax(np.abs(
            ev_f_nan.turnover.iloc[idx].values - ev_c_nan.turnover.iloc[idx].values
        ))
        max_diff = max(max_diff, diff)
    ok(f"NaN factor: turnover non-boundary match (diff={max_diff:.2e})", max_diff < 1e-12)
    ok("NaN factor: same length", len(ev_c_nan.turnover) == len(ev_f_nan.turnover))

    # -------------------------------------------------------
    # 11. 边界：少量数据 / edge: small dataset
    # -------------------------------------------------------
    print("\n11. Edge: small dataset (10 dates, 20 symbols)")
    f_small, r_small = make_synthetic(n_dates=10, n_symbols=20, seed=42)
    ev_fs = FactorEvaluator(f_small, r_small, chunk_size=None)
    ev_cs = FactorEvaluator(f_small, r_small, chunk_size=5)
    ev_fs.run_turnover()
    ev_cs.run_turnover()
    ok("small: turnover computed", ev_cs.turnover is not None)
    ok("small: same length", len(ev_cs.turnover) == len(ev_fs.turnover))
    b_set = _get_boundary_indices(10, 5)
    max_diff = 0.0
    for idx in range(10):
        if idx in b_set or idx == 0:
            continue
        diff = np.max(np.abs(
            ev_fs.turnover.iloc[idx].values - ev_cs.turnover.iloc[idx].values
        ))
        max_diff = max(max_diff, diff)
    ok(f"small: turnover non-boundary match (diff={max_diff:.2e})", max_diff < 1e-12)

    # -------------------------------------------------------
    # 12. 边界：chunk_size=1 / edge: chunk_size=1
    # -------------------------------------------------------
    print("\n12. Edge: chunk_size=1 (one timestamp per chunk)")
    ev_c1 = FactorEvaluator(factor, returns, chunk_size=1)
    ev_c1.run_turnover()
    ok("chunk_size=1: turnover computed", ev_c1.turnover is not None)
    ok("chunk_size=1: same length", len(ev_c1.turnover) == len(ev_full.turnover))
    # chunk_size=1 时每块只有 1 个时间截面，所有行都是边界 → 全部 NaN
    # chunk_size=1: every row is a boundary → all NaN
    ok("chunk_size=1: all turnover NaN", ev_c1.turnover.isna().all().all())
    ok("chunk_size=1: all rank_autocorr NaN", ev_c1.rank_autocorr.isna().all())

    # -------------------------------------------------------
    # 13. 向后兼容：chunk_size=None 行为不变 / backward compat
    # -------------------------------------------------------
    print("\n13. Backward compatibility: chunk_size=None unchanged")
    ev_compat = FactorEvaluator(factor, returns)
    ev_compat.run_turnover()
    ok("compat: turnover identical", ev_compat.turnover.equals(ev_full.turnover))
    ok("compat: rank_autocorr identical", ev_compat.rank_autocorr.equals(ev_full.rank_autocorr))

    # -------------------------------------------------------
    # 14. 不同 n_groups / different n_groups
    # -------------------------------------------------------
    print("\n14. Different n_groups in chunked mode")
    for ng in [2, 3, 4, 10]:
        ev_ng_f = FactorEvaluator(factor, returns, n_groups=ng, chunk_size=None)
        ev_ng_c = FactorEvaluator(factor, returns, n_groups=ng, chunk_size=30)
        ev_ng_f.run_turnover()
        ev_ng_c.run_turnover()
        b_set = _get_boundary_indices(120, 30)
        max_diff = 0.0
        for idx in range(120):
            if idx in b_set or idx == 0:
                continue
            diff = np.max(np.abs(
                ev_ng_f.turnover.iloc[idx].values - ev_ng_c.turnover.iloc[idx].values
            ))
            max_diff = max(max_diff, diff)
        ok(f"n_groups={ng}: turnover non-boundary match (diff={max_diff:.2e})", max_diff < 1e-12)
        ok(f"n_groups={ng}: columns count", ev_ng_c.turnover.shape[1] == ng)

    # -------------------------------------------------------
    # 15. 与 run_metrics + run_grouping 联合运行 / combined operations
    # -------------------------------------------------------
    print("\n15. Combined: run_metrics + run_grouping + run_turnover")
    ev_combo = FactorEvaluator(factor, returns, chunk_size=30)
    ev_combo.run_metrics().run_grouping().run_turnover()
    ok("combo: ic computed", ev_combo.ic is not None)
    ok("combo: group_labels computed", ev_combo.group_labels is not None)
    ok("combo: turnover computed", ev_combo.turnover is not None)
    ok("combo: rank_autocorr computed", ev_combo.rank_autocorr is not None)

    # -------------------------------------------------------
    # 16. 换手率值域验证 / turnover value range
    # -------------------------------------------------------
    print("\n16. Turnover value range [0, 1]")
    ev_range = FactorEvaluator(factor, returns, chunk_size=30)
    ev_range.run_turnover()
    valid = ev_range.turnover.iloc[1:].dropna(how="all").values.flatten()
    ok("turnover values in [0, 1]", (valid >= 0).all() and (valid <= 1).all())

    # -------------------------------------------------------
    # 17. 排名自相关值域验证 / rank_autocorr value range
    # -------------------------------------------------------
    print("\n17. Rank autocorrelation value range [-1, 1]")
    valid_ac = ev_range.rank_autocorr.dropna().values
    ok("rank_autocorr values in [-1, 1]", (valid_ac >= -1.0).all() and (valid_ac <= 1.0).all())

    # -------------------------------------------------------
    # 18. generate_report 中 turnover 板块 / report turnover section
    # -------------------------------------------------------
    print("\n18. generate_report: turnover section")
    ev_report = FactorEvaluator(factor, returns, chunk_size=30)
    ev_report.run_turnover()
    report = ev_report.generate_report(select=["turnover"])
    ok("report contains avg_turnover", "avg_turnover" in report.columns)
    ok("report contains avg_rank_autocorr", "avg_rank_autocorr" in report.columns)

    print(f"\n{'='*40}")
    print(f"Total: {checks} checks")

except Exception as e:
    print(f"\nFATAL: {e}")
    traceback.print_exc()
    sys.exit(1)
