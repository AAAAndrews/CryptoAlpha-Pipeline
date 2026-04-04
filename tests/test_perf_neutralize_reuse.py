"""
tests/test_perf_neutralize_reuse.py — neutralize 复用数值一致性测试
Verify neutralize reuse of calc_portfolio_curves numerical consistency.

验证内容 / Verification:
1. 6 种 mock 场景 × neutralized_curve 与参考实现 diff < 1e-10
2. demeaned / group_adjust 四种组合一致性
3. 不同 n_groups 参数一致性
4. _raw 模式一致性
5. chunk_size 分块模式 vs 全量模式一致性
6. evaluator.run_neutralize() 缓存复用一致性
7. chunk_size 两次独立运行一致性
"""

import numpy as np
import pandas as pd

from FactorAnalysis.neutralize import calc_neutralized_curve
from FactorAnalysis.grouping import quantile_group
from FactorAnalysis.evaluator import FactorEvaluator
from tests.mutual_components.conftest_perf import (
    iter_scenarios,
    assert_series_close,
    assert_scalar_close,
    SCENARIO_SMALL,
    make_synthetic_data,
)


# ============================================================
# 参考实现：模拟旧版 _hedge_return (独立 groupby.apply)
# Reference: simulate old _hedge_return (standalone groupby.apply)
# ============================================================


def _neutralized_curve_reference(
    factor: pd.Series,
    returns: pd.Series,
    groups: "pd.Series | int",
    demeaned: bool = True,
    group_adjust: bool = False,
    n_groups: int = 5,
    _raw: bool = False,
) -> pd.Series:
    """
    参考实现：模拟 Task 14 重构前的 _hedge_return 逻辑。
    Reference: simulate pre-Task 14 _hedge_return logic.

    使用独立的 groupby.apply 构建对冲收益，不依赖 calc_portfolio_curves。
    Uses standalone groupby.apply to build hedge returns, no calc_portfolio_curves dependency.
    """
    # 解析分组标签 / resolve group labels
    if isinstance(groups, int):
        group_labels = quantile_group(factor, n_groups=groups)
    else:
        group_labels = groups

    # 构建工作 DataFrame / build working DataFrame
    df = pd.DataFrame({
        "factor": factor,
        "returns": returns,
        "group": group_labels,
    })

    # 排除无效数据 / exclude invalid data
    valid_group = df["group"].notna() & np.isfinite(df["group"])
    valid_factor = df["factor"].notna() & np.isfinite(df["factor"])
    valid_returns = df["returns"].notna() & np.isfinite(df["returns"])

    # 组内因子去均值 / demean factor within groups
    if demeaned:
        mask_for_demean = valid_group & valid_factor
        if mask_for_demean.sum() > 0:
            group_mean = (
                df.loc[mask_for_demean]
                .groupby([pd.Grouper(level=0), "group"])["factor"]
                .transform("mean")
            )
            df.loc[mask_for_demean, "factor"] = (
                df.loc[mask_for_demean, "factor"] - group_mean
            )

    # 组内收益去均值 / adjust returns within groups
    if group_adjust:
        mask_for_adjust = valid_group & valid_returns
        if mask_for_adjust.sum() > 0:
            group_mean_ret = (
                df.loc[mask_for_adjust]
                .groupby([pd.Grouper(level=0), "group"])["returns"]
                .transform("mean")
            )
            df.loc[mask_for_adjust, "returns"] = (
                df.loc[mask_for_adjust, "returns"] - group_mean_ret
            )

    # 按中性化因子排名分组 / rank neutralized factor into groups
    neutralized_factor = df["factor"]
    labels = quantile_group(neutralized_factor, n_groups=n_groups)
    df["label"] = labels

    # 独立 groupby.apply 计算对冲收益 (旧版逻辑)
    # standalone groupby.apply for hedge returns (old logic)
    top_labels = set(range(n_groups - 1, n_groups))  # top_k=1
    bottom_labels = set(range(1))  # bottom_k=1

    def _hedge_returns(g: pd.DataFrame) -> float:
        valid = g["returns"].notna() & np.isfinite(g["returns"])
        long_mask = valid & g["label"].isin(top_labels)
        short_mask = valid & g["label"].isin(bottom_labels)
        long_ret = g.loc[long_mask, "returns"].mean() if long_mask.sum() > 0 else 0.0
        short_ret = (
            -g.loc[short_mask, "returns"].mean() if short_mask.sum() > 0 else 0.0
        )
        return long_ret + short_ret

    daily_hedge = df.groupby(level=0).apply(_hedge_returns)
    curve = (1.0 + daily_hedge).cumprod()
    if not _raw:
        curve.iloc[0] = 1.0
    return curve


# ============================================================
# 辅助函数 / Helper functions
# ============================================================


def _make_evaluator(factor, returns, chunk_size=None, **kwargs):
    """创建标准 evaluator 实例 / Create standard evaluator instance."""
    return FactorEvaluator(
        factor, returns,
        n_groups=5, top_k=1, bottom_k=1,
        chunk_size=chunk_size,
        **kwargs,
    )


# ============================================================
# 测试 1: 6 种 mock 场景 × neutralized_curve diff < 1e-10
# Test 1: 6 scenarios × neutralized_curve numerical consistency
# ============================================================


def test_neutralize_6scenarios_vs_reference():
    """
    6 种 mock 场景: calc_neutralized_curve 与参考实现 diff < 1e-10。
    6 scenarios: calc_neutralized_curve matches reference implementation.
    """
    n_checks = 0
    for sid, factor, returns in iter_scenarios():
        curve_actual = calc_neutralized_curve(factor, returns, groups=5)
        curve_ref = _neutralized_curve_reference(factor, returns, groups=5)

        assert_series_close(curve_actual, curve_ref, tol=1e-10,
                            label=f"{sid}/neutralized_curve")
        n_checks += 1
    print(f"[PASS] test_neutralize_6scenarios_vs_reference: {n_checks} checks")


# ============================================================
# 测试 2: demeaned / group_adjust 四种组合一致性
# Test 2: demeaned / group_adjust four combinations
# ============================================================


def test_demeaned_group_adjust_combinations():
    """
    4 种参数组合: demeaned × group_adjust × calc_neutralized_curve 与参考一致。
    4 parameter combos: demeaned × group_adjust matches reference.
    """
    n_checks = 0
    factor, returns = make_synthetic_data(n_days=200, n_symbols=50, seed=42)

    for demeaned in [True, False]:
        for group_adjust in [True, False]:
            curve_actual = calc_neutralized_curve(
                factor, returns, groups=5,
                demeaned=demeaned, group_adjust=group_adjust,
            )
            curve_ref = _neutralized_curve_reference(
                factor, returns, groups=5,
                demeaned=demeaned, group_adjust=group_adjust,
            )
            label = f"demeaned={demeaned}/gadj={group_adjust}"
            assert_series_close(curve_actual, curve_ref, tol=1e-10, label=label)
            n_checks += 1
    print(f"[PASS] test_demeaned_group_adjust_combinations: {n_checks} checks")


# ============================================================
# 测试 3: 不同 n_groups 参数一致性
# Test 3: various n_groups parameter consistency
# ============================================================


def test_various_n_groups():
    """
    不同 n_groups (3/4/5/10): calc_neutralized_curve 与参考一致。
    Various n_groups: calc_neutralized_curve matches reference.
    """
    n_checks = 0
    factor, returns = make_synthetic_data(n_days=200, n_symbols=50, seed=42)

    for n_groups in [3, 4, 5, 10]:
        curve_actual = calc_neutralized_curve(
            factor, returns, groups=5, n_groups=n_groups,
        )
        curve_ref = _neutralized_curve_reference(
            factor, returns, groups=5, n_groups=n_groups,
        )
        assert_series_close(curve_actual, curve_ref, tol=1e-10,
                            label=f"n_groups={n_groups}")
        n_checks += 1
    print(f"[PASS] test_various_n_groups: {n_checks} checks")


# ============================================================
# 测试 4: _raw 模式一致性
# Test 4: _raw mode consistency
# ============================================================


def test_raw_mode_consistency():
    """
    _raw=True: 6 场景 × neutralized_curve 与参考直接比较 (均不覆写起始值)。
    _raw=True: 6 scenarios × compare raw curves directly (both without start overwrite).
    """
    n_checks = 0
    for sid, factor, returns in iter_scenarios():
        curve_actual = calc_neutralized_curve(
            factor, returns, groups=5, _raw=True,
        )
        curve_ref = _neutralized_curve_reference(
            factor, returns, groups=5, _raw=True,
        )
        # 两条 raw 曲线的 cumprod 起始值相同，直接比较
        # both raw curves share same cumprod start, compare directly
        assert_series_close(curve_actual, curve_ref, tol=1e-10,
                            label=f"{sid}/raw_curve")
        n_checks += 1
    print(f"[PASS] test_raw_mode_consistency: {n_checks} checks")


# ============================================================
# 测试 5: chunk_size 分块模式 vs 全量模式一致性
# Test 5: chunk_size vs full mode consistency
# ============================================================


def test_chunk_mode_vs_full_mode():
    """
    chunk_size 模式 vs 全量模式: evaluator run_neutralize 曲线一致。
    Chunk mode vs full mode: evaluator run_neutralize curve matches.

    比较日收益率以避免 cumprod 浮点累积问题。
    Compare daily returns to avoid cumprod floating-point accumulation.
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

        # 比较日收益率 / compare daily returns
        full_daily = ev_full.neutralized_curve.pct_change().fillna(0.0)
        chunk_daily = ev_chunk.neutralized_curve.pct_change().fillna(0.0)
        assert_series_close(full_daily, chunk_daily, tol=1e-10,
                            label=f"{sid}/chunk_vs_full")
        n_checks += 1

        # 起始值均为 1.0 / start values are both 1.0
        assert ev_full.neutralized_curve.iloc[0] == 1.0, f"[{sid}] full start != 1.0"
        assert ev_chunk.neutralized_curve.iloc[0] == 1.0, f"[{sid}] chunk start != 1.0"
        n_checks += 2
    print(f"[PASS] test_chunk_mode_vs_full_mode: {n_checks} checks")


# ============================================================
# 测试 6: evaluator.run_neutralize() 与直接调用一致
# Test 6: evaluator.run_neutralize() vs direct call
# ============================================================


def test_evaluator_run_neutralize_matches_direct():
    """
    evaluator.run_neutralize() 与直接调用 calc_neutralized_curve 一致。
    evaluator.run_neutralize() matches direct calc_neutralized_curve call.
    """
    n_checks = 0
    for sid, factor, returns in iter_scenarios():
        # 直接调用 / direct call
        curve_direct = calc_neutralized_curve(factor, returns, groups=5)

        # evaluator 调用 / evaluator call
        ev = _make_evaluator(factor, returns)
        ev.run_grouping()
        ev.run_neutralize()

        assert_series_close(ev.neutralized_curve, curve_direct, tol=1e-10,
                            label=f"{sid}/evaluator_vs_direct")
        n_checks += 1
    print(f"[PASS] test_evaluator_run_neutralize_matches_direct: {n_checks} checks")


# ============================================================
# 测试 7: evaluator 缓存复用一致性
# Test 7: evaluator cache reuse consistency
# ============================================================


def test_evaluator_neutralize_cache_reuse():
    """
    evaluator: run_grouping() 缓存后 run_neutralize() 结果与无缓存一致。
    evaluator: cached run_neutralize() result matches uncached.
    """
    n_checks = 0
    for sid, factor, returns in iter_scenarios():
        # 有缓存 / with cache
        ev_cached = _make_evaluator(factor, returns)
        ev_cached.run_grouping()
        ev_cached.run_neutralize()

        # 无缓存 (清除后重新 neutralize) / without cache
        ev_uncached = _make_evaluator(factor, returns)
        ev_uncached._clear_group_cache()
        ev_uncached.run_neutralize()

        assert_series_close(
            ev_cached.neutralized_curve, ev_uncached.neutralized_curve,
            tol=1e-10, label=f"{sid}/cache_reuse",
        )
        n_checks += 1
    print(f"[PASS] test_evaluator_neutralize_cache_reuse: {n_checks} checks")


# ============================================================
# 测试 8: chunk_size 两次独立运行一致性
# Test 8: chunk_size two independent runs consistency
# ============================================================


def test_chunk_mode_neutralize_repeatability():
    """
    chunk_size 模式: 两次独立 run_all() neutralized_curve 结果一致。
    Chunk mode: two independent run_all() produce identical neutralized_curve.
    """
    n_checks = 0
    for sid, factor, returns in iter_scenarios():
        if sid == SCENARIO_SMALL:
            continue

        ev1 = _make_evaluator(factor, returns, chunk_size=50)
        ev1.run_all()

        ev2 = _make_evaluator(factor, returns, chunk_size=50)
        ev2.run_all()

        assert_series_close(
            ev1.neutralized_curve, ev2.neutralized_curve,
            tol=1e-10, label=f"{sid}/chunk_repeat",
        )
        n_checks += 1
    print(f"[PASS] test_chunk_mode_neutralize_repeatability: {n_checks} checks")


# ============================================================
# 测试 9: groups 参数为 Series 时一致性
# Test 9: groups as pd.Series consistency
# ============================================================


def test_groups_as_series():
    """
    groups 参数传入 pd.Series: calc_neutralized_curve 与参考一致。
    groups as pd.Series: calc_neutralized_curve matches reference.
    """
    n_checks = 0
    for sid, factor, returns in iter_scenarios():
        groups_series = quantile_group(factor, n_groups=5)

        curve_actual = calc_neutralized_curve(
            factor, returns, groups=groups_series,
        )
        curve_ref = _neutralized_curve_reference(
            factor, returns, groups=groups_series,
        )
        assert_series_close(curve_actual, curve_ref, tol=1e-10,
                            label=f"{sid}/groups_series")
        n_checks += 1
    print(f"[PASS] test_groups_as_series: {n_checks} checks")


# ============================================================
# 测试 10: 返回类型和形状验证
# Test 10: return type and shape verification
# ============================================================


def test_return_type_and_shape():
    """
    6 场景 × 返回类型 (pd.Series)、索引 (timestamp)、起始值 (1.0)、长度。
    6 scenarios × return type, index, start value, length.
    """
    n_checks = 0
    for sid, factor, returns in iter_scenarios():
        curve = calc_neutralized_curve(factor, returns, groups=5)

        # 类型 / type
        assert isinstance(curve, pd.Series), f"[{sid}] not pd.Series"
        n_checks += 1

        # 索引名称为 timestamp / index name is timestamp
        assert curve.index.name == "timestamp", f"[{sid}] index name != timestamp"
        n_checks += 1

        # 起始值 / start value
        assert curve.iloc[0] == 1.0, f"[{sid}] start != 1.0"
        n_checks += 1

        # 长度 / length
        n_ts = factor.index.get_level_values(0).nunique()
        assert len(curve) == n_ts, f"[{sid}] length {len(curve)} != {n_ts}"
        n_checks += 1
    print(f"[PASS] test_return_type_and_shape: {n_checks} checks")


if __name__ == "__main__":
    test_neutralize_6scenarios_vs_reference()
    test_demeaned_group_adjust_combinations()
    test_various_n_groups()
    test_raw_mode_consistency()
    test_chunk_mode_vs_full_mode()
    test_evaluator_run_neutralize_matches_direct()
    test_evaluator_neutralize_cache_reuse()
    test_chunk_mode_neutralize_repeatability()
    test_groups_as_series()
    test_return_type_and_shape()
    print("\n=== All neutralize reuse tests passed ===")
