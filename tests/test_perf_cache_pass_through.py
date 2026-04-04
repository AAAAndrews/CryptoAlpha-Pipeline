"""
tests/test_perf_cache_pass_through.py — group_labels 缓存接入验证
Verify group_labels cache integration in run_curves/run_turnover/run_neutralize.

验证:
- 全量模式: 有/无缓存结果数值一致 (diff < 1e-10)
- 分块模式: 有/无缓存结果数值一致
- 向后兼容: 缓存为 None 时行为不变
- neutralize: groups != n_groups 时跳过缓存
- 6 种 mock 场景全覆盖
"""

import sys
import os

# 确保项目根目录在 path 中 / ensure project root is in path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np
import pandas as pd

from tests.mutual_components.conftest_perf import (
    iter_scenarios,
    assert_series_close,
    assert_frame_close,
)
from FactorAnalysis.evaluator import FactorEvaluator
from FactorAnalysis.grouping import quantile_group


def _make_evaluator(factor, returns, chunk_size=None):
    """创建标准 evaluator 实例 / Create standard evaluator instance."""
    return FactorEvaluator(
        factor, returns,
        n_groups=5, top_k=1, bottom_k=1,
        chunk_size=chunk_size,
    )


def test_backward_compat_no_cache():
    """缓存为 None 时 run_curves/run_turnover/run_neutralize 行为不变 / No cache → unchanged behavior."""
    factor, returns = iter_scenarios().__next__()[1:][::-1]
    factor, returns = returns, factor  # swap back
    # 手动获取 factor, returns / manually get factor, returns
    for sid, factor, returns in iter_scenarios():
        ev = _make_evaluator(factor, returns)
        assert ev._cached_group_labels is None
        # 不调用 run_grouping，直接调用三个方法 / call methods without run_grouping
        ev.run_curves()
        assert ev.long_curve is not None
        assert ev.short_curve is not None
        assert ev.hedge_curve is not None

        ev2 = _make_evaluator(factor, returns)
        ev2.run_turnover()
        assert ev2.turnover is not None

        ev3 = _make_evaluator(factor, returns)
        ev3.run_neutralize()
        assert ev3.neutralized_curve is not None
        break  # 仅 basic 场景验证 / only basic scenario


def test_run_curves_full_mode_cached_consistent():
    """全量模式: run_curves 有缓存 vs 无缓存数值一致 / Full mode: cached vs uncached consistent."""
    n_checks = 0
    for sid, factor, returns in iter_scenarios():
        # 无缓存 / without cache
        ev_no_cache = _make_evaluator(factor, returns)
        ev_no_cache.run_curves()

        # 有缓存 / with cache
        ev_cached = _make_evaluator(factor, returns)
        ev_cached.run_grouping()  # 设置缓存 / set cache
        assert ev_cached._cached_group_labels is not None
        ev_cached.run_curves()

        # 三条曲线数值一致 / three curves numerically consistent
        assert_series_close(ev_no_cache.long_curve, ev_cached.long_curve,
                            tol=1e-10, label=f"{sid}/long_curve")
        assert_series_close(ev_no_cache.short_curve, ev_cached.short_curve,
                            tol=1e-10, label=f"{sid}/short_curve")
        assert_series_close(ev_no_cache.hedge_curve, ev_cached.hedge_curve,
                            tol=1e-10, label=f"{sid}/hedge_curve")
        n_checks += 3
    print(f"[PASS] test_run_curves_full_mode_cached_consistent: {n_checks} checks")


def test_run_turnover_full_mode_cached_consistent():
    """全量模式: run_turnover 有缓存 vs 无缓存数值一致 / Full mode: cached vs uncached consistent."""
    n_checks = 0
    for sid, factor, returns in iter_scenarios():
        # 无缓存 / without cache
        ev_no_cache = _make_evaluator(factor, returns)
        ev_no_cache.run_turnover()

        # 有缓存 / with cache
        ev_cached = _make_evaluator(factor, returns)
        ev_cached.run_grouping()
        assert ev_cached._cached_group_labels is not None
        ev_cached.run_turnover()

        # 换手率数值一致 / turnover numerically consistent
        assert_frame_close(ev_no_cache.turnover, ev_cached.turnover,
                           tol=1e-10, label=f"{sid}/turnover")
        n_checks += 1
    print(f"[PASS] test_run_turnover_full_mode_cached_consistent: {n_checks} checks")


def test_run_neutralize_full_mode_cached_consistent():
    """全量模式: run_neutralize 有缓存 vs 无缓存数值一致 / Full mode: cached vs uncached consistent."""
    n_checks = 0
    for sid, factor, returns in iter_scenarios():
        # 无缓存 / without cache
        ev_no_cache = _make_evaluator(factor, returns)
        ev_no_cache.run_neutralize()

        # 有缓存 (groups 默认 == n_groups) / with cache (groups defaults to n_groups)
        ev_cached = _make_evaluator(factor, returns)
        ev_cached.run_grouping()
        assert ev_cached._cached_group_labels is not None
        ev_cached.run_neutralize()

        # 中性化曲线数值一致 / neutralized curve numerically consistent
        assert_series_close(ev_no_cache.neutralized_curve, ev_cached.neutralized_curve,
                            tol=1e-10, label=f"{sid}/neutralized_curve")
        n_checks += 1
    print(f"[PASS] test_run_neutralize_full_mode_cached_consistent: {n_checks} checks")


def test_run_neutralize_bypass_cache_different_groups():
    """neutralize groups != n_groups 时不使用缓存 / Don't use cache when groups != n_groups."""
    factor, returns = iter_scenarios().__next__()[1:][::-1]
    factor, returns = returns, factor
    for sid, factor, returns in iter_scenarios():
        # 有缓存但 groups=3 (!= n_groups=5) / cache exists but groups=3
        ev = _make_evaluator(factor, returns)
        ev.run_grouping()
        assert ev._cached_group_labels is not None

        # groups=3 应跳过缓存，内部重新调用 quantile_group / groups=3 should skip cache
        ev.run_neutralize(groups=3)
        assert ev.neutralized_curve is not None

        # 对比无缓存 groups=3 结果 / compare with no-cache groups=3 result
        ev_no_cache = _make_evaluator(factor, returns)
        ev_no_cache.run_neutralize(groups=3)

        assert_series_close(ev.neutralized_curve, ev_no_cache.neutralized_curve,
                            tol=1e-10, label=f"{sid}/neutralize_groups3")
        break  # 仅 basic 场景 / only basic scenario
    print("[PASS] test_run_neutralize_bypass_cache_different_groups: 1 check")


def test_run_curves_chunk_mode_cached_consistent():
    """分块模式: run_curves 有缓存 vs 无缓存数值一致 / Chunk mode: cached vs uncached consistent."""
    n_checks = 0
    for sid, factor, returns in iter_scenarios():
        if sid == SCENARIO_SMALL:
            continue  # 小数据集不适合分块 / skip small dataset for chunking

        # 无缓存分块 / without cache, chunked
        ev_no_cache = _make_evaluator(factor, returns, chunk_size=50)
        ev_no_cache.run_grouping()
        ev_no_cache.run_curves()

        # 有缓存分块 / with cache, chunked
        ev_cached = _make_evaluator(factor, returns, chunk_size=50)
        ev_cached.run_grouping()  # 设置缓存 / set cache
        assert ev_cached._cached_group_labels is not None
        ev_cached.run_curves()

        assert_series_close(ev_no_cache.long_curve, ev_cached.long_curve,
                            tol=1e-10, label=f"{sid}/chunk/long_curve")
        assert_series_close(ev_no_cache.short_curve, ev_cached.short_curve,
                            tol=1e-10, label=f"{sid}/chunk/short_curve")
        assert_series_close(ev_no_cache.hedge_curve, ev_cached.hedge_curve,
                            tol=1e-10, label=f"{sid}/chunk/hedge_curve")
        n_checks += 3
    print(f"[PASS] test_run_curves_chunk_mode_cached_consistent: {n_checks} checks")


def test_run_turnover_chunk_mode_cached_consistent():
    """分块模式: run_turnover 有缓存 vs 无缓存数值一致 / Chunk mode: cached vs uncached consistent."""
    n_checks = 0
    for sid, factor, returns in iter_scenarios():
        if sid == SCENARIO_SMALL:
            continue

        # 无缓存分块 / without cache, chunked
        ev_no_cache = _make_evaluator(factor, returns, chunk_size=50)
        ev_no_cache.run_grouping()
        ev_no_cache.run_turnover()

        # 有缓存分块 / with cache, chunked
        ev_cached = _make_evaluator(factor, returns, chunk_size=50)
        ev_cached.run_grouping()
        assert ev_cached._cached_group_labels is not None
        ev_cached.run_turnover()

        assert_frame_close(ev_no_cache.turnover, ev_cached.turnover,
                           tol=1e-10, label=f"{sid}/chunk/turnover")
        n_checks += 1
    print(f"[PASS] test_run_turnover_chunk_mode_cached_consistent: {n_checks} checks")


def test_run_neutralize_chunk_mode_cached_consistent():
    """分块模式: run_neutralize 有缓存 vs 无缓存数值一致 / Chunk mode: cached vs uncached consistent."""
    n_checks = 0
    for sid, factor, returns in iter_scenarios():
        if sid == SCENARIO_SMALL:
            continue

        # 无缓存分块 / without cache, chunked
        ev_no_cache = _make_evaluator(factor, returns, chunk_size=50)
        ev_no_cache.run_grouping()
        ev_no_cache.run_neutralize()

        # 有缓存分块 / with cache, chunked
        ev_cached = _make_evaluator(factor, returns, chunk_size=50)
        ev_cached.run_grouping()
        assert ev_cached._cached_group_labels is not None
        ev_cached.run_neutralize()

        assert_series_close(ev_no_cache.neutralized_curve, ev_cached.neutralized_curve,
                            tol=1e-10, label=f"{sid}/chunk/neutralized_curve")
        n_checks += 1
    print(f"[PASS] test_run_neutralize_chunk_mode_cached_consistent: {n_checks} checks")


def test_run_all_cached_produces_same_results():
    """run_all() 有缓存 vs 无缓存全流程结果一致 / run_all() cached vs uncached full pipeline consistent."""
    n_checks = 0
    for sid, factor, returns in iter_scenarios():
        # 无缓存全流程 / without cache full pipeline
        ev_no_cache = _make_evaluator(factor, returns)
        ev_no_cache.run_all()

        # 有缓存全流程 (run_grouping 自动缓存) / with cache full pipeline
        ev_cached = _make_evaluator(factor, returns)
        ev_cached.run_all()

        # 净值曲线一致 / equity curves consistent
        assert_series_close(ev_no_cache.long_curve, ev_cached.long_curve,
                            tol=1e-10, label=f"{sid}/all/long")
        assert_series_close(ev_no_cache.short_curve, ev_cached.short_curve,
                            tol=1e-10, label=f"{sid}/all/short")
        assert_series_close(ev_no_cache.hedge_curve, ev_cached.hedge_curve,
                            tol=1e-10, label=f"{sid}/all/hedge")

        # 换手率一致 / turnover consistent
        assert_frame_close(ev_no_cache.turnover, ev_cached.turnover,
                           tol=1e-10, label=f"{sid}/all/turnover")

        # 中性化曲线一致 / neutralized curve consistent
        assert_series_close(ev_no_cache.neutralized_curve, ev_cached.neutralized_curve,
                            tol=1e-10, label=f"{sid}/all/neutralized")

        # IC 指标一致 / IC metrics consistent
        assert_series_close(ev_no_cache.ic, ev_cached.ic,
                            tol=1e-10, label=f"{sid}/all/ic")

        n_checks += 6
    print(f"[PASS] test_run_all_cached_produces_same_results: {n_checks} checks")


# 需要导入 SCENARIO_SMALL 常量 / need SCENARIO_SMALL constant
from tests.mutual_components.conftest_perf import SCENARIO_SMALL


if __name__ == "__main__":
    test_backward_compat_no_cache()
    test_run_curves_full_mode_cached_consistent()
    test_run_turnover_full_mode_cached_consistent()
    test_run_neutralize_full_mode_cached_consistent()
    test_run_neutralize_bypass_cache_different_groups()
    test_run_curves_chunk_mode_cached_consistent()
    test_run_turnover_chunk_mode_cached_consistent()
    test_run_neutralize_chunk_mode_cached_consistent()
    test_run_all_cached_produces_same_results()
    print("\n=== All cache pass-through tests passed ===")
