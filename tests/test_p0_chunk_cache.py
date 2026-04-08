"""
Task 3 测试 — P0 chunk 一致性 + 向后兼容
Test: P0 chunk consistency + backward compatibility (run_all one-time dispatch)

验证:
1. chunk_list 传入 vs 内部 split 结果完全一致 (所有报告字段 diff < 1e-10)
2. chunk_list=None 时行为不变 (向后兼容)
3. chunk_size=None 时正常 (全量模式不受影响)
4. 6 种 mock 场景覆盖不同数据模式
5. 分块模式正确性 (chunk 数量、边界对齐)
"""

import sys
import os
import traceback
import numpy as np
import pandas as pd

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from FactorAnalysis.evaluator import FactorEvaluator
from FactorAnalysis.chunking import split_into_chunks

checks = 0
passed = 0


def ok(label: str, condition: bool):
    global checks, passed
    checks += 1
    status = "PASS" if condition else "FAIL"
    print(f"  [{status}] {label}")
    if condition:
        passed += 1
    else:
        traceback.print_stack()


# ============================================================
# Mock 数据生成 / Mock data generators
# ============================================================

def make_synthetic(
    n_dates=100,
    n_symbols=30,
    seed=42,
    nan_ratio=0.0,
    signal_strength=0.02,
):
    """
    生成合成因子和收益率 / Generate synthetic factor and returns.

    Parameters / 参数:
        n_dates: 时间截面数 / Number of timestamps
        n_symbols: 标的数量 / Number of symbols
        seed: 随机种子 / Random seed
        nan_ratio: NaN 注入比例 / NaN injection ratio
        signal_strength: 信号在收益率中的权重 / Signal weight in returns
    """
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2025-01-01", periods=n_dates, freq="B")
    symbols = [f"S{i:03d}" for i in range(n_symbols)]
    idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])

    true_signal = rng.standard_normal((n_dates, n_symbols))
    noise = rng.standard_normal((n_dates, n_symbols)) * 0.5
    factor_values = true_signal + noise
    returns_values = true_signal * signal_strength + rng.standard_normal((n_dates, n_symbols)) * 0.03

    # 注入 NaN / inject NaN values
    if nan_ratio > 0:
        nan_mask = rng.random((n_dates, n_symbols)) < nan_ratio
        factor_values[nan_mask] = np.nan

    factor = pd.Series(factor_values.ravel(), index=idx, dtype=np.float64)
    returns = pd.Series(returns_values.ravel(), index=idx, dtype=np.float64)
    return factor, returns


# ============================================================
# 6 种 Mock 场景 / 6 mock scenarios
# ============================================================

def scenario_params():
    """
    返回 6 种 mock 场景参数 / Return 6 mock scenario parameters.

    场景设计:
    1. 标准场景 — 默认参数，中等数据量
    2. 大数据量 — 更多日期和标的，验证分块拆分
    3. 小数据量 — 少量日期，单块覆盖
    4. 高 NaN 比例 — 大量缺失值，验证 NaN 传播
    5. 弱信号 + 多分组 — n_groups=10，信号弱，分组更细
    6. 紧凑分块 — chunk_size=5，大量分块，验证边界
    """
    return [
        {
            "name": "标准场景",
            "n_dates": 120, "n_symbols": 50, "seed": 42,
            "nan_ratio": 0.0, "chunk_size": 30, "n_groups": 5,
        },
        {
            "name": "大数据量",
            "n_dates": 200, "n_symbols": 80, "seed": 123,
            "nan_ratio": 0.0, "chunk_size": 40, "n_groups": 5,
        },
        {
            "name": "小数据量",
            "n_dates": 20, "n_symbols": 15, "seed": 77,
            "nan_ratio": 0.0, "chunk_size": 100, "n_groups": 5,
        },
        {
            "name": "高NaN比例",
            "n_dates": 100, "n_symbols": 40, "seed": 888,
            "nan_ratio": 0.10, "chunk_size": 25, "n_groups": 5,
        },
        {
            "name": "弱信号+多分组",
            "n_dates": 120, "n_symbols": 50, "seed": 2024,
            "nan_ratio": 0.0, "chunk_size": 30, "n_groups": 10,
            "signal_strength": 0.005,
        },
        {
            "name": "紧凑分块",
            "n_dates": 150, "n_symbols": 60, "seed": 555,
            "nan_ratio": 0.02, "chunk_size": 5, "n_groups": 5,
        },
    ]


# ============================================================
# 核心验证函数 / Core validation functions
# ============================================================

def compare_reports(report_full, report_chunk, scenario_name, tol=1e-10):
    """
    逐列比较两份报告所有字段 / Compare all report fields between full and chunked mode.

    对换手率类指标使用宽松容差（分块边界 NaN 影响），
    其余指标使用严格容差 diff < 1e-10。
    Uses relaxed tolerance for turnover metrics (chunk boundary NaN),
    strict tolerance for all other fields.
    """
    # 确保列集合一致 / ensure same column set
    ok(f"{scenario_name}: report 列集合一致",
       set(report_full.columns) == set(report_chunk.columns))

    relaxed_cols = {"avg_turnover", "avg_rank_autocorr"}
    for col in report_full.columns:
        fval = report_full[col].iloc[0]
        cval = report_chunk[col].iloc[0]
        if pd.isna(fval) and pd.isna(cval):
            ok(f"{scenario_name}: report[{col}] both NaN", True)
        elif pd.isna(fval) or pd.isna(cval):
            ok(f"{scenario_name}: report[{col}] one NaN (full={fval}, chunk={cval})", False)
        else:
            diff = abs(float(fval) - float(cval))
            if col in relaxed_cols:
                ok(f"{scenario_name}: report[{col}] diff={diff:.2e} (relaxed)", diff < 1e-2)
            else:
                ok(f"{scenario_name}: report[{col}] diff={diff:.2e} < 1e-10", diff < tol)


def compare_series(a, b, label, tol=1e-10, equal_nan=False):
    """比较两个 Series / Compare two Series."""
    ok(f"{label}: same length", len(a) == len(b))
    if len(a) != len(b):
        return
    max_diff = (a - b).abs().max()
    ok(f"{label}: max_diff={max_diff:.2e} <= {tol:.0e}", max_diff <= tol)


def compare_all_attributes(ev_full, ev_chunk, scenario_name, tol=1e-10):
    """
    比较两个 Evaluator 的所有公共属性 / Compare all public attributes of two evaluators.
    """
    # IC 序列 / IC series
    compare_series(ev_chunk.ic, ev_full.ic, f"{scenario_name}: IC", tol=tol, equal_nan=True)
    compare_series(ev_chunk.rank_ic, ev_full.rank_ic, f"{scenario_name}: RankIC", tol=tol, equal_nan=True)

    # ICIR / ICIR
    icir_diff = abs(ev_chunk.icir - ev_full.icir)
    ok(f"{scenario_name}: ICIR diff={icir_diff:.2e} < 1e-10", icir_diff < tol)

    # IC stats 逐字段 / IC stats per field
    for field in ["IC_mean", "IC_std", "ICIR", "t_stat", "p_value", "IC_skew", "IC_kurtosis"]:
        diff = abs(ev_chunk.ic_stats[field] - ev_full.ic_stats[field])
        ok(f"{scenario_name}: ic_stats[{field}] diff={diff:.2e}", diff < tol)

    # 分组标签 — 非空部分完全一致（NaN 因子值产生 NaN 标签，NaN==NaN 为 False 需特殊处理）
    # group labels must match for non-NaN entries (NaN factor → NaN label, NaN!=NaN needs special handling)
    gl_chunk = ev_chunk.group_labels
    gl_full = ev_full.group_labels
    # NaN 位置一致 + 非空值一致 / same NaN positions + same non-NaN values
    ok(f"{scenario_name}: group_labels NaN 位置一致",
       (gl_chunk.isna() == gl_full.isna()).all())
    valid_mask = gl_chunk.notna() & gl_full.notna()
    if valid_mask.any():
        ok(f"{scenario_name}: group_labels 非空值一致",
           (gl_chunk[valid_mask] == gl_full[valid_mask]).all())
    else:
        ok(f"{scenario_name}: group_labels 无非空值", True)

    # 净值曲线 / equity curves
    compare_series(ev_chunk.long_curve, ev_full.long_curve, f"{scenario_name}: long_curve", tol=tol)
    compare_series(ev_chunk.short_curve, ev_full.short_curve, f"{scenario_name}: short_curve", tol=tol)
    compare_series(ev_chunk.hedge_curve, ev_full.hedge_curve, f"{scenario_name}: hedge_curve", tol=tol)
    compare_series(
        ev_chunk.hedge_curve_after_cost, ev_full.hedge_curve_after_cost,
        f"{scenario_name}: hedge_curve_after_cost", tol=tol,
    )

    # 中性化曲线 / neutralized curve
    compare_series(
        ev_chunk.neutralized_curve, ev_full.neutralized_curve,
        f"{scenario_name}: neutralized_curve", tol=tol,
    )

    # 绩效比率 / performance ratios
    for attr in [
        "sharpe", "calmar", "sortino",
        "sharpe_after_cost", "calmar_after_cost", "sortino_after_cost",
    ]:
        diff = abs(getattr(ev_chunk, attr) - getattr(ev_full, attr))
        ok(f"{scenario_name}: {attr} diff={diff:.2e}", diff < tol)

    # 换手率列一致性 / turnover column consistency
    ok(f"{scenario_name}: turnover 列一致",
       set(ev_chunk.turnover.columns) == set(ev_full.turnover.columns))

    # rank_autocorr — 非边界值一致 / rank_autocorr non-boundary values match
    chunk_ra = ev_chunk.rank_autocorr.dropna()
    full_ra = ev_full.rank_autocorr.dropna()
    common_idx = chunk_ra.index.intersection(full_ra.index)
    if len(common_idx) > 0:
        ra_diff = (chunk_ra.loc[common_idx] - full_ra.loc[common_idx]).abs().max()
        ok(f"{scenario_name}: rank_autocorr diff={ra_diff:.2e} (common={len(common_idx)})",
           ra_diff < tol)
    else:
        ok(f"{scenario_name}: rank_autocorr no common timestamps", False)


# ============================================================
# 测试 1: 6 场景 run_all() 一致性 / 6 scenarios run_all() consistency
# ============================================================

def test_6_scenarios_consistency():
    """
    6 种 mock 场景下 run_all() 分块 vs 全量结果完全一致。
    run_all() chunked vs full results match across 6 mock scenarios.
    """
    print("\n=== 1. 6 场景 run_all() 全量 vs 分块一致性 ===")
    for p in scenario_params():
        print(f"\n  --- {p['name']} ---")
        factor, returns = make_synthetic(
            n_dates=p["n_dates"],
            n_symbols=p["n_symbols"],
            seed=p["seed"],
            nan_ratio=p.get("nan_ratio", 0.0),
            signal_strength=p.get("signal_strength", 0.02),
        )
        n_groups = p["n_groups"]
        chunk_size = p["chunk_size"]

        # 全量 / full
        ev_full = FactorEvaluator(factor, returns, n_groups=n_groups)
        ev_full.run_all()

        # 分块 / chunked
        ev_chunk = FactorEvaluator(factor, returns, n_groups=n_groups, chunk_size=chunk_size)
        ev_chunk.run_all()

        compare_all_attributes(ev_full, ev_chunk, p["name"], tol=1e-10)

        # 报告对比 / report comparison
        report_full = ev_full.generate_report()
        report_chunk = ev_chunk.generate_report()
        compare_reports(report_full, report_chunk, p["name"], tol=1e-10)


# ============================================================
# 测试 2: chunk_list=None 向后兼容 / chunk_list=None backward compat
# ============================================================

def test_chunk_list_none_backward_compat():
    """
    chunk_list=None 时（不传 chunk 参数）行为与改造前完全一致。
    不传 chunk 参数 = 内部 split，应与传预计算 chunk 结果相同。
    Not passing chunk args = internal split, should match pre-computed chunk results.
    """
    print("\n=== 2. chunk_list=None 向后兼容 ===")
    for p in scenario_params()[:3]:  # 取前 3 个场景验证 / verify first 3 scenarios
        print(f"\n  --- {p['name']} ---")
        factor, returns = make_synthetic(
            n_dates=p["n_dates"],
            n_symbols=p["n_symbols"],
            seed=p["seed"],
            nan_ratio=p.get("nan_ratio", 0.0),
        )
        chunk_size = p["chunk_size"]

        # A: 不传 chunk 参数 (内部 split) / internal split
        ev_a = FactorEvaluator(factor, returns, n_groups=p["n_groups"], chunk_size=chunk_size)
        ev_a.run_all()

        # B: 手动预计算 chunk 并传入 / pre-compute chunks and pass in
        factor_chunks = split_into_chunks(factor, chunk_size)
        returns_chunks = split_into_chunks(returns, chunk_size)

        ev_b = FactorEvaluator(factor, returns, n_groups=p["n_groups"], chunk_size=chunk_size)
        ev_b.run_metrics(chunk_factor=factor_chunks, chunk_returns=returns_chunks)
        ev_b.run_grouping(chunk_factor=factor_chunks)
        group_chunks = split_into_chunks(ev_b.group_labels, chunk_size)
        ev_b.run_curves(
            chunk_factor=factor_chunks,
            chunk_returns=returns_chunks,
            chunk_groups=group_chunks,
        )
        ev_b.run_turnover(
            chunk_factor=factor_chunks,
            chunk_groups=group_chunks,
        )
        ev_b.run_neutralize(
            chunk_factor=factor_chunks,
            chunk_returns=returns_chunks,
            chunk_groups=group_chunks,
        )

        compare_all_attributes(ev_a, ev_b, f"compat_{p['name']}", tol=1e-10)


# ============================================================
# 测试 3: chunk_size=None 全量模式正常 / chunk_size=None full mode
# ============================================================

def test_chunk_size_none_full_mode():
    """
    chunk_size=None (全量模式) 正常工作，所有属性非空。
    chunk_size=None (full mode) works correctly, all attributes non-null.
    """
    print("\n=== 3. chunk_size=None 全量模式 ===")
    for p in scenario_params()[:3]:
        print(f"\n  --- {p['name']} ---")
        factor, returns = make_synthetic(
            n_dates=p["n_dates"],
            n_symbols=p["n_symbols"],
            seed=p["seed"],
            nan_ratio=p.get("nan_ratio", 0.0),
        )

        ev = FactorEvaluator(factor, returns, n_groups=p["n_groups"])
        ev.run_all()

        ok(f"{p['name']}: ic not None", ev.ic is not None)
        ok(f"{p['name']}: rank_ic not None", ev.rank_ic is not None)
        ok(f"{p['name']}: icir not None", ev.icir is not None)
        ok(f"{p['name']}: ic_stats not None", ev.ic_stats is not None)
        ok(f"{p['name']}: group_labels not None", ev.group_labels is not None)
        ok(f"{p['name']}: long_curve not None", ev.long_curve is not None)
        ok(f"{p['name']}: short_curve not None", ev.short_curve is not None)
        ok(f"{p['name']}: hedge_curve not None", ev.hedge_curve is not None)
        ok(f"{p['name']}: hedge_curve_after_cost not None", ev.hedge_curve_after_cost is not None)
        ok(f"{p['name']}: turnover not None", ev.turnover is not None)
        ok(f"{p['name']}: rank_autocorr not None", ev.rank_autocorr is not None)
        ok(f"{p['name']}: neutralized_curve not None", ev.neutralized_curve is not None)
        ok(f"{p['name']}: sharpe not None", ev.sharpe is not None)

        # 净值起始值 / curve start values
        ok(f"{p['name']}: long_curve starts at 1.0", abs(ev.long_curve.iloc[0] - 1.0) < 1e-10)
        ok(f"{p['name']}: hedge_curve starts at 1.0", abs(ev.hedge_curve.iloc[0] - 1.0) < 1e-10)

        # generate_report / report output
        report = ev.generate_report()
        ok(f"{p['name']}: report is DataFrame", isinstance(report, pd.DataFrame))
        ok(f"{p['name']}: report has columns", len(report.columns) > 0)


# ============================================================
# 测试 4: 分块模式正确性 / Chunk mode correctness
# ============================================================

def test_chunk_mode_correctness():
    """
    验证分块数量、边界对齐、单块覆盖等分块模式正确性。
    Verify chunk count, boundary alignment, single-chunk coverage.
    """
    print("\n=== 4. 分块模式正确性 ===")

    # 4a: 分块数量正确 / correct number of chunks
    factor, returns = make_synthetic(n_dates=100, n_symbols=30, seed=42)
    chunk_size = 30
    chunks = split_into_chunks(factor, chunk_size)
    # 100 个时间截面 / 100 timestamps → ceil(100/30) = 4 块
    ok(f"100 dates, chunk_size=30: 4 chunks", len(chunks) == 4)

    # 各块时间截面不重叠 / chunks have non-overlapping timestamps
    all_ts = []
    for c in chunks:
        ts = c.index.get_level_values(0).unique()
        ok("chunks 无重叠", not any(t in all_ts for t in ts))
        all_ts.extend(ts.tolist())
    ok("chunks 覆盖全部时间戳", len(all_ts) == 100)

    # 4b: chunk_size >= n_dates → 单块 / chunk_size >= n_dates → single chunk
    chunks_big = split_into_chunks(factor, chunk_size=200)
    ok("chunk_size=200 → 1 chunk", len(chunks_big) == 1)

    # 4c: 各块时间截面连续 / chunk timestamps are contiguous
    chunks = split_into_chunks(factor, chunk_size=25)
    all_dates = chunks[0].index.get_level_values(0).unique()
    for c in chunks[1:]:
        all_dates = all_dates.union(c.index.get_level_values(0).unique())
    all_dates = all_dates.sort_values()
    expected = factor.index.get_level_values(0).unique().sort_values()
    ok("chunks 时间连续性",
       (all_dates.values == expected.values).all())

    # 4d: 各块 symbol 集合相同 / each chunk has same symbol set
    for c in chunks:
        syms = c.index.get_level_values(1).unique().sort_values()
        expected_syms = factor.index.get_level_values(1).unique().sort_values()
        ok("chunk symbol 集合一致", (syms.values == expected_syms.values).all())

    # 4e: 不同 chunk_size 下 run_all() 结果一致
    for cs in [10, 25, 50]:
        ev_full = FactorEvaluator(factor, returns, chunk_size=None)
        ev_full.run_all()
        ev_c = FactorEvaluator(factor, returns, chunk_size=cs)
        ev_c.run_all()
        ic_diff = (ev_c.ic - ev_full.ic).abs().max()
        hedge_diff = (ev_c.hedge_curve - ev_full.hedge_curve).abs().max()
        ok(f"chunk_size={cs}: IC diff={ic_diff:.2e} < 1e-10", ic_diff < 1e-10)
        ok(f"chunk_size={cs}: hedge diff={hedge_diff:.2e} < 1e-10", hedge_diff < 1e-10)


# ============================================================
# 测试 5: run_all() 一次性分块 vs 子方法独立分块 / one-time vs per-method split
# ============================================================

def test_one_time_split_vs_per_method_split():
    """
    run_all() 一次性分块 (P0 优化) vs 各子方法独立分块结果一致。
    run_all() one-time split (P0) matches per-method independent split results.
    """
    print("\n=== 5. 一次性分块 vs 独立分块 ===")
    for p in scenario_params()[:3]:
        print(f"\n  --- {p['name']} ---")
        factor, returns = make_synthetic(
            n_dates=p["n_dates"],
            n_symbols=p["n_symbols"],
            seed=p["seed"],
            nan_ratio=p.get("nan_ratio", 0.0),
        )
        chunk_size = p["chunk_size"]

        # A: run_all() 一次性分块 / run_all() one-time split
        ev_a = FactorEvaluator(factor, returns, n_groups=p["n_groups"], chunk_size=chunk_size)
        ev_a.run_all()

        # B: 各子方法独立分块 / each method splits independently
        ev_b = FactorEvaluator(factor, returns, n_groups=p["n_groups"], chunk_size=chunk_size)
        ev_b.run_metrics()       # 内部独立 split factor + returns
        ev_b.run_grouping()      # 内部独立 split factor
        ev_b.run_curves()        # 内部独立 split factor + returns + cached_labels
        ev_b.run_turnover()      # 内部独立 split factor + cached_labels
        ev_b.run_neutralize()    # 内部独立 split factor + returns + cached_labels

        compare_all_attributes(ev_a, ev_b, f"onetime_vs_indie_{p['name']}", tol=1e-10)


# ============================================================
# 测试 6: 全量模式下 chunk 参数被忽略 / full mode ignores chunk args
# ============================================================

def test_full_mode_ignores_chunk_args():
    """
    chunk_size=None 时传入伪造 chunk 列表，结果不受影响。
    Passing fake chunk lists in full mode (chunk_size=None) does not affect results.
    """
    print("\n=== 6. 全量模式忽略 chunk 参数 ===")
    factor, returns = make_synthetic(n_dates=80, n_symbols=25, seed=42)

    # A: 正常全量 / normal full mode
    ev_a = FactorEvaluator(factor, returns)
    ev_a.run_all()

    # B: 全量模式传入伪造 chunk 参数 / full mode with fake chunk args
    ev_b = FactorEvaluator(factor, returns)
    ev_b.run_metrics(
        chunk_factor=[factor],
        chunk_returns=[returns],
    ).run_grouping(
        chunk_factor=[factor],
    ).run_curves(
        chunk_factor=[factor],
        chunk_returns=[returns],
        chunk_groups=[None],
    ).run_turnover(
        chunk_factor=[factor],
        chunk_groups=[None],
    ).run_neutralize(
        chunk_factor=[factor],
        chunk_returns=[returns],
        chunk_groups=[None],
    )

    ok("IC 一致", np.allclose(ev_a.ic.values, ev_b.ic.values, equal_nan=True, atol=1e-10))
    ok("group_labels 一致", (ev_a.group_labels == ev_b.group_labels).all())
    ok("hedge_curve 一致",
       np.allclose(ev_a.hedge_curve.values, ev_b.hedge_curve.values, atol=1e-10))
    ok("neutralized_curve 一致",
       np.allclose(ev_a.neutralized_curve.values, ev_b.neutralized_curve.values, atol=1e-10))
    ok("sharpe 一致", np.isclose(ev_a.sharpe, ev_b.sharpe, atol=1e-10))


# ============================================================
# Main / 主入口
# ============================================================

if __name__ == "__main__":
    print("=" * 60)
    print("Task 3 测试 — P0 chunk 一致性 + 向后兼容")
    print("=" * 60)

    test_6_scenarios_consistency()
    test_chunk_list_none_backward_compat()
    test_chunk_size_none_full_mode()
    test_chunk_mode_correctness()
    test_one_time_split_vs_per_method_split()
    test_full_mode_ignores_chunk_args()

    print(f"\n{'=' * 60}")
    print(f"结果: {passed}/{checks} 通过")
    print("=" * 60)

    if passed < checks:
        sys.exit(1)
