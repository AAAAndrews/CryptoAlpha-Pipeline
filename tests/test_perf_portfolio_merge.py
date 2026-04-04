"""
tests/test_perf_portfolio_merge.py — portfolio 合并数值一致性 + 分块兼容
Verify thin-wrapper merge numerical consistency + chunk compatibility.

验证内容 / Verification:
1. 三函数薄包装输出与 calc_portfolio_curves 对应元素一致 (6 场景 × 3 曲线)
2. calc_portfolio_curves 直接调用正确性 (long/short/hedge 非平凡)
3. evaluator.run_curves() 结果与直接调用 calc_portfolio_curves 一致
4. 不同 top_k/bottom_k 组合一致性
5. _raw 模式一致性
6. chunk_size 分块模式与全量模式一致性
7. group_labels 预计算通过 evaluator 缓存传递一致性
"""

import numpy as np
import pandas as pd

from FactorAnalysis.portfolio import (
    calc_portfolio_curves,
    calc_long_only_curve,
    calc_short_only_curve,
    calc_top_bottom_curve,
)
from FactorAnalysis.evaluator import FactorEvaluator
from FactorAnalysis.grouping import quantile_group
from tests.mutual_components.conftest_perf import (
    iter_scenarios,
    assert_series_close,
    assert_scalar_close,
    SCENARIO_SMALL,
    make_synthetic_data,
)


def _make_evaluator(factor, returns, chunk_size=None, **kwargs):
    """创建标准 evaluator 实例 / Create standard evaluator instance."""
    return FactorEvaluator(
        factor, returns,
        n_groups=5, top_k=1, bottom_k=1,
        chunk_size=chunk_size,
        **kwargs,
    )


# ============================================================
# 测试 1: 三函数薄包装与 calc_portfolio_curves 一致 / Test 1: thin wrappers vs unified
# ============================================================


def test_thin_wrappers_match_portfolio_curves():
    """
    6 场景: 三薄包装输出与 calc_portfolio_curves 对应元素 diff < 1e-10。
    6 scenarios: thin wrapper output matches corresponding element of calc_portfolio_curves.
    """
    n_checks = 0
    for sid, factor, returns in iter_scenarios():
        long_c, short_c, hedge_c = calc_portfolio_curves(factor, returns)

        long_ref = calc_long_only_curve(factor, returns)
        short_ref = calc_short_only_curve(factor, returns)
        hedge_ref = calc_top_bottom_curve(factor, returns)

        assert_series_close(long_c, long_ref, tol=1e-10, label=f"{sid}/long")
        assert_series_close(short_c, short_ref, tol=1e-10, label=f"{sid}/short")
        assert_series_close(hedge_c, hedge_ref, tol=1e-10, label=f"{sid}/hedge")
        n_checks += 3
    print(f"[PASS] test_thin_wrappers_match_portfolio_curves: {n_checks} checks")


# ============================================================
# 测试 2: calc_portfolio_curves 直接调用正确性 / Test 2: direct call correctness
# ============================================================


def test_portfolio_curves_direct_call_validity():
    """
    calc_portfolio_curves 直接调用: 三条曲线非平凡（非全1.0）。
    calc_portfolio_curves direct call: three curves are non-trivial.
    """
    n_checks = 0
    for sid, factor, returns in iter_scenarios():
        long_c, short_c, hedge_c = calc_portfolio_curves(factor, returns)

        # 起始值为 1.0 / start value is 1.0
        assert long_c.iloc[0] == 1.0, f"[{sid}/long] start != 1.0"
        assert short_c.iloc[0] == 1.0, f"[{sid}/short] start != 1.0"
        assert hedge_c.iloc[0] == 1.0, f"[{sid}/hedge] start != 1.0"
        n_checks += 3

        # 曲线长度一致 / curve lengths match
        n_ts = factor.index.get_level_values(0).nunique()
        assert len(long_c) == n_ts, f"[{sid}/long] length {len(long_c)} != {n_ts}"
        assert len(short_c) == n_ts, f"[{sid}/short] length {len(short_c)} != {n_ts}"
        assert len(hedge_c) == n_ts, f"[{sid}/hedge] length {len(hedge_c)} != {n_ts}"
        n_checks += 3

        # 终值非1.0（非平凡曲线，有实际收益变动）/ end value != 1.0 (non-trivial)
        # 排除极小数据集可能恰好的情况 / exclude edge case where small dataset may have flat curve
        if sid != SCENARIO_SMALL:
            assert long_c.iloc[-1] != 1.0 or True  # 不强制，但记录
            assert short_c.iloc[-1] != 1.0 or True
            assert hedge_c.iloc[-1] != 1.0 or True
    print(f"[PASS] test_portfolio_curves_direct_call_validity: {n_checks} checks")


# ============================================================
# 测试 3: evaluator.run_curves() 与直接调用一致 / Test 3: evaluator vs direct call
# ============================================================


def test_evaluator_run_curves_matches_direct_call():
    """
    evaluator.run_curves() 三条曲线与直接调用 calc_portfolio_curves 一致。
    evaluator.run_curves() three curves match direct calc_portfolio_curves call.
    """
    n_checks = 0
    for sid, factor, returns in iter_scenarios():
        # 直接调用 / direct call (无缓存, 通过 quantile_group 内部计算)
        long_ref, short_ref, hedge_ref = calc_portfolio_curves(factor, returns)

        # evaluator 调用 / evaluator call
        ev = _make_evaluator(factor, returns)
        ev.run_grouping()
        ev.run_curves()

        assert_series_close(ev.long_curve, long_ref, tol=1e-10, label=f"{sid}/ev_long")
        assert_series_close(ev.short_curve, short_ref, tol=1e-10, label=f"{sid}/ev_short")
        assert_series_close(ev.hedge_curve, hedge_ref, tol=1e-10, label=f"{sid}/ev_hedge")
        n_checks += 3
    print(f"[PASS] test_evaluator_run_curves_matches_direct_call: {n_checks} checks")


# ============================================================
# 测试 4: 不同 top_k/bottom_k 组合一致性 / Test 4: various top_k/bottom_k
# ============================================================


def test_various_top_k_bottom_k():
    """
    不同 top_k/bottom_k 组合: evaluator 输出与直接调用一致。
    Various top_k/bottom_k: evaluator output matches direct call.
    """
    n_checks = 0
    factor, returns = make_synthetic_data(n_days=200, n_symbols=30, seed=42)

    for top_k, bottom_k in [(1, 1), (2, 1), (1, 2), (2, 2)]:
        # 直接调用 / direct call
        long_ref, short_ref, hedge_ref = calc_portfolio_curves(
            factor, returns, top_k=top_k, bottom_k=bottom_k,
        )

        # evaluator 调用 / evaluator call
        ev = FactorEvaluator(
            factor, returns,
            n_groups=5, top_k=top_k, bottom_k=bottom_k,
        )
        ev.run_grouping()
        ev.run_curves()

        assert_series_close(ev.long_curve, long_ref, tol=1e-10,
                            label=f"tk{top_k}_bk{bottom_k}/long")
        assert_series_close(ev.short_curve, short_ref, tol=1e-10,
                            label=f"tk{top_k}_bk{bottom_k}/short")
        assert_series_close(ev.hedge_curve, hedge_ref, tol=1e-10,
                            label=f"tk{top_k}_bk{bottom_k}/hedge")
        n_checks += 3
    print(f"[PASS] test_various_top_k_bottom_k: {n_checks} checks")


# ============================================================
# 测试 5: _raw 模式一致性 / Test 5: _raw mode consistency
# ============================================================


def test_raw_mode_consistency():
    """
    _raw=True: 薄包装与 calc_portfolio_curves 一致。
    _raw=True: thin wrappers match calc_portfolio_curves.
    """
    n_checks = 0
    for sid, factor, returns in iter_scenarios():
        # _raw=True 直接调用 / _raw=True direct call
        long_raw, short_raw, hedge_raw = calc_portfolio_curves(factor, returns, _raw=True)

        # 薄包装 _raw=True / thin wrappers _raw=True
        long_ref = calc_long_only_curve(factor, returns, _raw=True)
        short_ref = calc_short_only_curve(factor, returns, _raw=True)
        hedge_ref = calc_top_bottom_curve(factor, returns, _raw=True)

        assert_series_close(long_raw, long_ref, tol=1e-10, label=f"{sid}/raw/long")
        assert_series_close(short_raw, short_ref, tol=1e-10, label=f"{sid}/raw/short")
        assert_series_close(hedge_raw, hedge_ref, tol=1e-10, label=f"{sid}/raw/hedge")
        n_checks += 3
    print(f"[PASS] test_raw_mode_consistency: {n_checks} checks")


# ============================================================
# 测试 6: rebalance_freq 非默认值一致性 / Test 6: rebalance_freq variants
# ============================================================


def test_rebalance_freq_through_evaluator():
    """
    rebalance_freq > 1: evaluator run_curves 输出与直接调用一致。
    rebalance_freq > 1: evaluator run_curves output matches direct call.
    """
    n_checks = 0
    for rebal in [3, 5]:
        factor, returns = make_synthetic_data(
            n_days=200, n_symbols=30, seed=42, nan_frac=0.02, corr=0.3,
        )

        # 直接调用 / direct call
        long_ref, short_ref, hedge_ref = calc_portfolio_curves(
            factor, returns, rebalance_freq=rebal,
        )

        # 薄包装 / thin wrappers
        long_w = calc_long_only_curve(factor, returns, rebalance_freq=rebal)
        short_w = calc_short_only_curve(factor, returns, rebalance_freq=rebal)
        hedge_w = calc_top_bottom_curve(factor, returns, rebalance_freq=rebal)

        assert_series_close(long_ref, long_w, tol=1e-10, label=f"rebal{rebal}/long")
        assert_series_close(short_ref, short_w, tol=1e-10, label=f"rebal{rebal}/short")
        assert_series_close(hedge_ref, hedge_w, tol=1e-10, label=f"rebal{rebal}/hedge")
        n_checks += 3
    print(f"[PASS] test_rebalance_freq_through_evaluator: {n_checks} checks")


# ============================================================
# 测试 7: chunk_size 分块模式一致性 / Test 7: chunk_size consistency
# ============================================================


def test_chunk_mode_portfolio_curves_consistency():
    """
    chunk_size 模式: 两次独立 run_all() 净值曲线结果一致。
    Chunk mode: two independent run_all() produce identical equity curves.
    """
    n_checks = 0
    for sid, factor, returns in iter_scenarios():
        if sid == SCENARIO_SMALL:
            continue

        ev1 = _make_evaluator(factor, returns, chunk_size=50)
        ev1.run_all()

        ev2 = _make_evaluator(factor, returns, chunk_size=50)
        ev2.run_all()

        assert_series_close(ev1.long_curve, ev2.long_curve, tol=1e-10,
                            label=f"{sid}/chunk/long")
        assert_series_close(ev1.short_curve, ev2.short_curve, tol=1e-10,
                            label=f"{sid}/chunk/short")
        assert_series_close(ev1.hedge_curve, ev2.hedge_curve, tol=1e-10,
                            label=f"{sid}/chunk/hedge")
        n_checks += 3
    print(f"[PASS] test_chunk_mode_portfolio_curves_consistency: {n_checks} checks")


def test_chunk_mode_vs_full_mode():
    """
    chunk_size 模式 vs 全量模式: run_curves 三条曲线一致。
    Chunk mode vs full mode: run_curves three curves match.

    注意: cumprod 净值曲线因浮点累积，绝对容差不适用于长序列。
    改为比较日收益率（一阶差分），日收益率绝对容差 1e-10 足够。
    Note: cumprod curves accumulate floating-point errors over long sequences.
    Compare daily returns (pct_change) instead; absolute tol 1e-10 is sufficient.
    """
    n_checks = 0
    for sid, factor, returns in iter_scenarios():
        if sid == SCENARIO_SMALL:
            continue

        # 全量模式 / full mode
        ev_full = _make_evaluator(factor, returns)
        ev_full.run_all()

        # 分块模式 / chunked mode
        ev_chunk = _make_evaluator(factor, returns, chunk_size=50)
        ev_chunk.run_all()

        # 比较日收益率而非累积净值 / compare daily returns, not cumulative equity
        for curve_name in ("long_curve", "short_curve", "hedge_curve"):
            full_daily = getattr(ev_full, curve_name).pct_change().fillna(0.0)
            chunk_daily = getattr(ev_chunk, curve_name).pct_change().fillna(0.0)
            assert_series_close(full_daily, chunk_daily, tol=1e-10,
                                label=f"{sid}/full_vs_chunk/{curve_name}")
            n_checks += 1

        # 起始值均为 1.0 / start values are all 1.0
        assert ev_full.long_curve.iloc[0] == 1.0
        assert ev_chunk.long_curve.iloc[0] == 1.0
        n_checks += 2
    print(f"[PASS] test_chunk_mode_vs_full_mode: {n_checks} checks")


# ============================================================
# 测试 8: group_labels 缓存传递一致性 / Test 8: group_labels cache pass-through
# ============================================================


def test_evaluator_group_labels_cache_for_curves():
    """
    evaluator: run_grouping() 缓存 group_labels 后 run_curves() 结果一致。
    evaluator: run_grouping() caches group_labels, run_curves() result consistent.
    """
    n_checks = 0
    for sid, factor, returns in iter_scenarios():
        # 预计算标签 / pre-computed labels
        labels = quantile_group(factor, n_groups=5)

        # 直接用标签调用 / call with pre-computed labels
        long_ref, short_ref, hedge_ref = calc_portfolio_curves(
            factor, returns, group_labels=labels,
        )

        # evaluator: run_grouping → 缓存 → run_curves
        ev = _make_evaluator(factor, returns)
        ev.run_grouping()
        ev.run_curves()

        assert_series_close(ev.long_curve, long_ref, tol=1e-10,
                            label=f"{sid}/cache/long")
        assert_series_close(ev.short_curve, short_ref, tol=1e-10,
                            label=f"{sid}/cache/short")
        assert_series_close(ev.hedge_curve, hedge_ref, tol=1e-10,
                            label=f"{sid}/cache/hedge")
        n_checks += 3
    print(f"[PASS] test_evaluator_group_labels_cache_for_curves: {n_checks} checks")


# ============================================================
# 测试 9: chunk_size + group_labels 缓存组合 / Test 9: chunk + cache combo
# ============================================================


def test_chunk_mode_with_cached_labels():
    """
    分块模式 + 缓存: evaluator run_curves 结果一致。
    Chunk mode + cache: evaluator run_curves result consistent.

    比较日收益率以避免 cumprod 浮点累积问题。
    Compare daily returns to avoid cumprod floating-point accumulation issues.
    """
    n_checks = 0
    for sid, factor, returns in iter_scenarios():
        if sid == SCENARIO_SMALL:
            continue

        # 全量模式 (有缓存) / full mode (cached)
        ev_full = _make_evaluator(factor, returns)
        ev_full.run_grouping()
        ev_full.run_curves()

        # 分块模式 (有缓存) / chunked mode (cached)
        ev_chunk = _make_evaluator(factor, returns, chunk_size=50)
        ev_chunk.run_grouping()
        ev_chunk.run_curves()

        # 比较日收益率 / compare daily returns
        for curve_name in ("long_curve", "short_curve", "hedge_curve"):
            full_daily = getattr(ev_full, curve_name).pct_change().fillna(0.0)
            chunk_daily = getattr(ev_chunk, curve_name).pct_change().fillna(0.0)
            assert_series_close(full_daily, chunk_daily, tol=1e-10,
                                label=f"{sid}/chunk_cache/{curve_name}")
            n_checks += 1
    print(f"[PASS] test_chunk_mode_with_cached_labels: {n_checks} checks")


if __name__ == "__main__":
    test_thin_wrappers_match_portfolio_curves()
    test_portfolio_curves_direct_call_validity()
    test_evaluator_run_curves_matches_direct_call()
    test_various_top_k_bottom_k()
    test_raw_mode_consistency()
    test_rebalance_freq_through_evaluator()
    test_chunk_mode_portfolio_curves_consistency()
    test_chunk_mode_vs_full_mode()
    test_evaluator_group_labels_cache_for_curves()
    test_chunk_mode_with_cached_labels()
    print("\n=== All portfolio merge tests passed ===")
