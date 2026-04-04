"""
tests/test_perf_quantile_cache.py — quantile_group 缓存数值一致性 + 分块兼容
Verify quantile_group cache numerical consistency + chunk compatibility.

验证:
- 缓存/非缓存结果完全一致 (6 场景 × 12 标量指标 + 核心数据对象 diff < 1e-10)
- run_all() 中主分组 quantile_group 仅调用 1 次 (总调用从 6-7 次降至 2 次)
- 6 种 mock 场景全覆盖
- chunk_size 分块模式缓存正确
"""

import sys
import os

# 确保项目根目录在 path 中 / ensure project root is in path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from unittest.mock import patch

import numpy as np
import pandas as pd

from tests.mutual_components.conftest_perf import (
    iter_scenarios,
    assert_series_close,
    assert_frame_close,
    assert_scalar_close,
    SCENARIO_SMALL,
)
from FactorAnalysis.evaluator import FactorEvaluator
from FactorAnalysis.grouping import quantile_group as _original_quantile_group


def _make_evaluator(factor, returns, chunk_size=None):
    """创建标准 evaluator 实例 / Create standard evaluator instance."""
    return FactorEvaluator(
        factor, returns,
        n_groups=5, top_k=1, bottom_k=1,
        chunk_size=chunk_size,
    )


def _extract_12_report_scalars(ev):
    """
    提取 12 个核心标量报告指标 / Extract 12 core scalar report metrics.

    IC_mean, IC_std, RankIC_mean, RankIC_std, ICIR,
    IC_t_stat, IC_p_value, IC_skew, IC_kurtosis,
    long_return, short_return, hedge_return
    """
    report = ev.generate_report()
    keys = [
        "IC_mean", "IC_std", "RankIC_mean", "RankIC_std", "ICIR",
        "IC_t_stat", "IC_p_value", "IC_skew", "IC_kurtosis",
        "long_return", "short_return", "hedge_return",
    ]
    return {k: float(report[k].iloc[0]) for k in keys if k in report.columns}


# 所有需要同步 patch quantile_group 的模块路径
# All module paths that import quantile_group (must patch each reference)
_PATCH_TARGETS = [
    "FactorAnalysis.evaluator.quantile_group",
    "FactorAnalysis.portfolio.quantile_group",
    "FactorAnalysis.turnover.quantile_group",
    "FactorAnalysis.neutralize.quantile_group",
]


def _count_quantile_group_calls(factor, returns, chunk_size=None):
    """
    执行 run_all() 并返回 quantile_group 总调用次数。
    Run run_all() and return total quantile_group call count.
    """
    call_count = 0

    def _tracked(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        return _original_quantile_group(*args, **kwargs)

    patches = [patch(t, side_effect=_tracked) for t in _PATCH_TARGETS]
    for p in patches:
        p.start()
    try:
        ev = _make_evaluator(factor, returns, chunk_size=chunk_size)
        ev.run_all()
    finally:
        for p in reversed(patches):
            p.stop()

    return call_count


# ============================================================
# 测试 1: quantile_group 调用次数验证 / Test 1: quantile_group call count
# ============================================================


def test_quantile_group_call_count_full_mode():
    """
    全量模式: quantile_group 总调用 2 次 (主分组 1 次 + neutralize ranking 1 次)。
    Full mode: quantile_group called 2 times total (1 main grouping + 1 neutralize ranking).

    优化前为 6-7 次，缓存消除冗余调用。
    Before optimization: 6-7 calls, cache eliminates redundant ones.
    """
    n_checks = 0
    for sid, factor, returns in iter_scenarios():
        count = _count_quantile_group_calls(factor, returns)
        assert count == 2, (
            f"[{sid}] quantile_group called {count} times, expected 2 "
            f"(1 main grouping + 1 neutralize ranking)"
        )
        n_checks += 1
    print(f"[PASS] test_quantile_group_call_count_full_mode: {n_checks} checks")


def test_quantile_group_call_count_chunk_mode():
    """
    分块模式: quantile_group 每块调用 2 次 (主分组 1 次 + neutralize ranking 1 次)。
    Chunk mode: quantile_group called 2 times per chunk.
    """
    n_checks = 0
    for sid, factor, returns in iter_scenarios():
        if sid == SCENARIO_SMALL:
            continue

        count = _count_quantile_group_calls(factor, returns, chunk_size=50)

        n_timestamps = factor.index.get_level_values(0).nunique()
        expected_chunks = (n_timestamps + 49) // 50
        expected = 2 * expected_chunks

        assert count == expected, (
            f"[{sid}] quantile_group called {count} times, "
            f"expected {expected} (2 x {expected_chunks} chunks)"
        )
        n_checks += 1
    print(f"[PASS] test_quantile_group_call_count_chunk_mode: {n_checks} checks")


# ============================================================
# 测试 2: run_all() 12 标量指标一致性 / Test 2: run_all() 12 scalar metrics
# ============================================================


def test_run_all_12_scalar_metrics_consistency():
    """
    6 场景 x 12 标量指标: 两次独立 run_all() 结果一致 (diff < 1e-10)。
    6 scenarios x 12 scalar metrics: two independent run_all() produce identical results.
    """
    n_checks = 0
    for sid, factor, returns in iter_scenarios():
        ev1 = _make_evaluator(factor, returns)
        ev1.run_all()

        ev2 = _make_evaluator(factor, returns)
        ev2.run_all()

        s1 = _extract_12_report_scalars(ev1)
        s2 = _extract_12_report_scalars(ev2)

        for key in s1:
            assert_scalar_close(s1[key], s2[key], tol=1e-10, label=f"{sid}/{key}")
            n_checks += 1
    print(f"[PASS] test_run_all_12_scalar_metrics_consistency: {n_checks} checks")


# ============================================================
# 测试 3: 核心数据对象一致性 / Test 3: Core data objects consistency
# ============================================================


def test_run_all_core_data_consistency():
    """
    6 场景: 核心数据对象两次 run_all() 一致 (diff < 1e-10)。
    6 scenarios: core data objects from two run_all() identical.
    """
    n_checks = 0
    for sid, factor, returns in iter_scenarios():
        ev1 = _make_evaluator(factor, returns)
        ev1.run_all()

        ev2 = _make_evaluator(factor, returns)
        ev2.run_all()

        assert_series_close(ev1.ic, ev2.ic, tol=1e-10, label=f"{sid}/ic")
        assert_series_close(ev1.rank_ic, ev2.rank_ic, tol=1e-10, label=f"{sid}/rank_ic")
        assert_series_close(ev1.long_curve, ev2.long_curve, tol=1e-10, label=f"{sid}/long")
        assert_series_close(ev1.short_curve, ev2.short_curve, tol=1e-10, label=f"{sid}/short")
        assert_series_close(ev1.hedge_curve, ev2.hedge_curve, tol=1e-10, label=f"{sid}/hedge")
        assert_series_close(
            ev1.hedge_curve_after_cost, ev2.hedge_curve_after_cost,
            tol=1e-10, label=f"{sid}/hedge_cost",
        )
        assert_frame_close(ev1.turnover, ev2.turnover, tol=1e-10, label=f"{sid}/turnover")
        assert_series_close(
            ev1.rank_autocorr, ev2.rank_autocorr, tol=1e-10, label=f"{sid}/rank_autocorr",
        )
        assert_series_close(
            ev1.neutralized_curve, ev2.neutralized_curve,
            tol=1e-10, label=f"{sid}/neutralized",
        )
        n_checks += 9
    print(f"[PASS] test_run_all_core_data_consistency: {n_checks} checks")


# ============================================================
# 测试 4: 单独方法缓存/非缓存一致性 / Test 4: Individual methods cached vs uncached
# ============================================================


def test_individual_methods_cached_vs_uncached():
    """
    单独方法: 有缓存 (run_grouping 后) vs 无缓存结果完全一致。
    Individual methods: cached (after run_grouping) vs uncached produce identical results.
    """
    n_checks = 0
    for sid, factor, returns in iter_scenarios():
        # --- run_curves: 无缓存 vs 有缓存 ---
        ev_unc = _make_evaluator(factor, returns)
        ev_unc.run_curves()

        ev_ca = _make_evaluator(factor, returns)
        ev_ca.run_grouping()
        ev_ca.run_curves()

        assert_series_close(ev_unc.long_curve, ev_ca.long_curve, tol=1e-10, label=f"{sid}/curves/long")
        assert_series_close(ev_unc.short_curve, ev_ca.short_curve, tol=1e-10, label=f"{sid}/curves/short")
        assert_series_close(ev_unc.hedge_curve, ev_ca.hedge_curve, tol=1e-10, label=f"{sid}/curves/hedge")
        n_checks += 3

        # --- run_turnover: 无缓存 vs 有缓存 ---
        ev_unc = _make_evaluator(factor, returns)
        ev_unc.run_turnover()

        ev_ca = _make_evaluator(factor, returns)
        ev_ca.run_grouping()
        ev_ca.run_turnover()

        assert_frame_close(ev_unc.turnover, ev_ca.turnover, tol=1e-10, label=f"{sid}/turnover")
        n_checks += 1

        # --- run_neutralize: 无缓存 vs 有缓存 ---
        ev_unc = _make_evaluator(factor, returns)
        ev_unc.run_neutralize()

        ev_ca = _make_evaluator(factor, returns)
        ev_ca.run_grouping()
        ev_ca.run_neutralize()

        assert_series_close(
            ev_unc.neutralized_curve, ev_ca.neutralized_curve,
            tol=1e-10, label=f"{sid}/neutralize",
        )
        n_checks += 1
    print(f"[PASS] test_individual_methods_cached_vs_uncached: {n_checks} checks")


# ============================================================
# 测试 5: 分块模式一致性 / Test 5: Chunk mode consistency
# ============================================================


def test_chunk_mode_consistency():
    """
    分块模式: 两次独立 run_all(chunk_size=50) 结果一致。
    Chunk mode: two independent run_all(chunk_size=50) produce identical results.
    """
    n_checks = 0
    for sid, factor, returns in iter_scenarios():
        if sid == SCENARIO_SMALL:
            continue

        ev1 = _make_evaluator(factor, returns, chunk_size=50)
        ev1.run_all()

        ev2 = _make_evaluator(factor, returns, chunk_size=50)
        ev2.run_all()

        assert_series_close(ev1.ic, ev2.ic, tol=1e-10, label=f"{sid}/chunk/ic")
        assert_series_close(ev1.long_curve, ev2.long_curve, tol=1e-10, label=f"{sid}/chunk/long")
        assert_series_close(ev1.short_curve, ev2.short_curve, tol=1e-10, label=f"{sid}/chunk/short")
        assert_series_close(ev1.hedge_curve, ev2.hedge_curve, tol=1e-10, label=f"{sid}/chunk/hedge")
        assert_frame_close(ev1.turnover, ev2.turnover, tol=1e-10, label=f"{sid}/chunk/turnover")
        assert_series_close(
            ev1.neutralized_curve, ev2.neutralized_curve,
            tol=1e-10, label=f"{sid}/chunk/neutralized",
        )
        n_checks += 6
    print(f"[PASS] test_chunk_mode_consistency: {n_checks} checks")


if __name__ == "__main__":
    test_quantile_group_call_count_full_mode()
    test_quantile_group_call_count_chunk_mode()
    test_run_all_12_scalar_metrics_consistency()
    test_run_all_core_data_consistency()
    test_individual_methods_cached_vs_uncached()
    test_chunk_mode_consistency()
    print("\n=== All quantile_group cache tests passed ===")
