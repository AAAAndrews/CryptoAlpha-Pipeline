"""
Task 17 测试 — turnover unstack 去重数值一致性
Verify calc_turnover unstack dedup numerical consistency.

对比优化后 (1 次 unstack) 与参考实现 (每分组独立 unstack) 的换手率 DataFrame，
验证 6 种 mock 场景 + 不同 n_groups + NaN 数据 + chunk_size 模式的数值一致性。
Compare optimized (single unstack) vs reference (per-group unstack) turnover DataFrame,
verifying 6 mock scenarios + various n_groups + NaN data + chunk_size consistency.
"""

from __future__ import annotations

import sys
import numpy as np
import pandas as pd

sys.path.insert(0, ".")

from FactorAnalysis.turnover import calc_turnover
from FactorAnalysis.grouping import quantile_group
from FactorAnalysis.evaluator import FactorEvaluator
from tests.mutual_components.conftest_perf import (
    SCENARIOS,
    make_synthetic_data,
    iter_scenarios,
    assert_frame_close,
    assert_series_close,
    assert_scalar_close,
    SCENARIO_SMALL,
)

PASS = 0
FAIL = 0


def check(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  [PASS] {name}")
    else:
        FAIL += 1
        print(f"  [FAIL] {name} — {detail}")


def check_frame_close(name, a, b, tol=1e-10):
    """包装 assert_frame_close 为 check 模式 / Wrap assert_frame_close as check."""
    try:
        assert_frame_close(a, b, tol=tol, label=name)
        check(name, True)
    except AssertionError as e:
        check(name, False, str(e))


def calc_turnover_reference(
    factor: pd.Series,
    n_groups: int = 5,
    group_labels: pd.Series | None = None,
) -> pd.DataFrame:
    """
    参考实现：模拟 Task 16 之前的逻辑（每分组独立 unstack）。
    Reference: simulate pre-Task 16 logic (per-group unstack inside loop).

    每次循环内对 labels==g 做布尔筛选后 unstack，而非一次 unstack 后向量化。
    Per-group: filter labels==g then unstack, instead of single unstack + vectorized.
    """
    # 参数校验 / parameter validation (与实际函数一致)
    if not isinstance(factor, pd.Series):
        raise TypeError(
            f"factor must be pd.Series, got {type(factor).__name__}"
        )

    labels = group_labels if group_labels is not None else quantile_group(factor, n_groups=n_groups)

    results = {}
    for g in range(n_groups):
        # 每分组独立 unstack（旧版行为）/ per-group unstack (old behavior)
        in_group = (labels == g)
        in_group_mat = in_group.unstack(level=1)
        shifted = in_group_mat.shift(1)

        current_count = in_group_mat.sum(axis=1)
        overlap = (in_group_mat & shifted).sum(axis=1)

        turnover = 1.0 - overlap / current_count
        turnover[current_count == 0] = np.nan
        turnover.iloc[0] = np.nan

        results[g] = turnover

    return pd.DataFrame(results)


# ── 1. 6 种 mock 场景一致性 / 6 mock scenarios consistency ─────────────

def test_6_scenarios_consistency():
    """6 种 mock 场景 × turnover DataFrame diff < 1e-10."""
    print("\n=== 6 scenarios consistency ===")
    for sid, factor, _ in iter_scenarios():
        actual = calc_turnover(factor)
        ref = calc_turnover_reference(factor)
        check_frame_close(f"{sid}: optimized vs reference", actual, ref)


# ── 2. 不同 n_groups / Various n_groups ───────────────────────────────

def test_various_n_groups():
    """不同 n_groups (3/4/5/10): 优化后与参考实现一致."""
    print("\n=== various n_groups ===")
    factor, _ = make_synthetic_data(n_days=200, n_symbols=50, seed=42)

    for n_groups in [3, 4, 5, 10]:
        actual = calc_turnover(factor, n_groups=n_groups)
        ref = calc_turnover_reference(factor, n_groups=n_groups)
        check_frame_close(f"n_groups={n_groups}: optimized vs reference", actual, ref)


# ── 3. 预计算 group_labels / Pre-computed group_labels ────────────────

def test_precomputed_group_labels():
    """传入预计算 group_labels: 优化后与参考实现一致."""
    print("\n=== precomputed group_labels ===")
    for sid, factor, _ in iter_scenarios():
        labels = quantile_group(factor, n_groups=5)

        actual = calc_turnover(factor, n_groups=5, group_labels=labels)
        ref = calc_turnover_reference(factor, n_groups=5, group_labels=labels)
        check_frame_close(f"{sid}: precomputed labels", actual, ref)


# ── 4. NaN 数据处理 / NaN handling ────────────────────────────────────

def test_high_nan_consistency():
    """高比例 NaN (10%) × turnover DataFrame 一致性."""
    print("\n=== high NaN consistency ===")
    factor, _ = make_synthetic_data(**SCENARIOS["with_nan"])

    actual = calc_turnover(factor)
    ref = calc_turnover_reference(factor)
    check_frame_close("with_nan: optimized vs reference", actual, ref)


def test_all_nan_input():
    """全 NaN 输入 → 全 NaN 输出."""
    print("\n=== all NaN input ===")
    timestamps = pd.date_range("2025-01-01", periods=10, freq="D")
    symbols = [f"S{i:03d}" for i in range(5)]
    idx = pd.MultiIndex.from_product([timestamps, symbols], names=["timestamp", "symbol"])
    factor = pd.Series(np.nan, index=idx)

    actual = calc_turnover(factor)
    ref = calc_turnover_reference(factor)
    check_frame_close("all_nan: optimized vs reference", actual, ref)
    check("all_nan: all NaN output", actual.isna().all().all())


def test_single_period():
    """单期数据 → 全 NaN."""
    print("\n=== single period ===")
    timestamps = pd.date_range("2025-01-01", periods=1, freq="D")
    symbols = ["A", "B", "C", "D", "E"]
    idx = pd.MultiIndex.from_product([timestamps, symbols], names=["timestamp", "symbol"])
    factor = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0], index=idx)

    actual = calc_turnover(factor, n_groups=3)
    check("single period: all NaN", actual.isna().all().all())


def test_minimal_data():
    """最小数据 (2 期 × 3 资产)."""
    print("\n=== minimal data ===")
    timestamps = pd.date_range("2025-01-01", periods=2, freq="D")
    symbols = ["A", "B", "C"]
    idx = pd.MultiIndex.from_product([timestamps, symbols], names=["timestamp", "symbol"])
    factor = pd.Series([1.0, 2.0, 3.0, 3.0, 1.0, 2.0], index=idx)

    actual = calc_turnover(factor, n_groups=2)
    ref = calc_turnover_reference(factor, n_groups=2)
    check_frame_close("minimal: optimized vs reference", actual, ref)


# ── 5. 稳定因子换手率为 0 / Stable factor turnover = 0 ────────────────

def test_stable_factor_zero_turnover():
    """因子值不变 → 分组不变 → 换手率 = 0."""
    print("\n=== stable factor zero turnover ===")
    n_days, n_syms = 5, 5
    timestamps = pd.date_range("2025-01-01", periods=n_days, freq="D")
    symbols = [f"S{i:03d}" for i in range(n_syms)]
    vals = np.tile(np.arange(n_syms, dtype=float), (n_days, 1))
    idx = pd.MultiIndex.from_product([timestamps, symbols], names=["timestamp", "symbol"])
    factor = pd.Series(vals.ravel(), index=idx)

    actual = calc_turnover(factor, n_groups=3)
    ref = calc_turnover_reference(factor, n_groups=3)
    check_frame_close("stable: optimized vs reference", actual, ref)

    valid = actual.iloc[1:].dropna().values.flatten()
    check("stable: turnover ≈ 0.0", np.allclose(valid, 0.0))


# ── 6. 值域 [0, 1] / Value range [0, 1] ──────────────────────────────

def test_value_range():
    """6 种场景 × 换手率值域 [0, 1]."""
    print("\n=== value range [0, 1] ===")
    for sid, factor, _ in iter_scenarios():
        result = calc_turnover(factor)
        valid = result.iloc[1:].dropna().values.flatten()
        in_range = (valid >= 0.0).all() and (valid <= 1.0).all()
        check(f"{sid}: turnover in [0, 1]", in_range)


# ── 7. 返回类型和形状 / Return type and shape ────────────────────────

def test_return_type_and_shape():
    """6 种场景 × 返回类型和形状."""
    print("\n=== return type and shape ===")
    for sid, factor, _ in iter_scenarios():
        result = calc_turnover(factor)
        check(f"{sid}: isinstance DataFrame", isinstance(result, pd.DataFrame))
        check(f"{sid}: columns 0..4", list(result.columns) == [0, 1, 2, 3, 4])
        check(f"{sid}: dtype float", result.dtypes.apply(lambda x: np.issubdtype(x, np.floating)).all())
        check(f"{sid}: first row all NaN", result.iloc[0].isna().all())


# ── 8. chunk_size 分块模式 vs 全量模式 / chunk_size vs full mode ─────

def test_chunk_mode_vs_full_mode():
    """
    chunk_size 模式 vs 全量模式: evaluator.run_turnover() turnover DataFrame 一致。
    Chunk mode vs full mode: evaluator.run_turnover() turnover DataFrame matches.

    跨块边界首行在分块模式下被设为 NaN（缺少前序截面），因此比较时排除边界行。
    Cross-chunk boundary first rows are NaN in chunked mode (missing predecessor),
    so boundary rows are excluded from comparison.
    """
    print("\n=== chunk_size vs full mode ===")
    for sid, factor, returns in iter_scenarios():
        if sid == SCENARIO_SMALL:
            continue

        # 全量模式 / full mode
        ev_full = FactorEvaluator(
            factor, returns,
            n_groups=5, top_k=1, bottom_k=1,
        )
        ev_full.run_all()

        # 分块模式 / chunked mode
        ev_chunk = FactorEvaluator(
            factor, returns,
            n_groups=5, top_k=1, bottom_k=1,
            chunk_size=50,
        )
        ev_chunk.run_all()

        # 确定分块边界行索引（后续每块的首行）/ identify boundary row indices
        n_ts = len(ev_full.turnover)
        chunk_size = 50
        boundary_indices = []
        start = chunk_size
        while start < n_ts:
            boundary_indices.append(ev_full.turnover.index[start])
            start += chunk_size

        # 排除边界行和首行后比较 / exclude boundary rows and first row, then compare
        full_trimmed = ev_full.turnover.drop(
            [ev_full.turnover.index[0]] + boundary_indices, errors="ignore"
        )
        chunk_trimmed = ev_chunk.turnover.drop(
            [ev_chunk.turnover.index[0]] + boundary_indices, errors="ignore"
        )
        check_frame_close(f"{sid}: chunk vs full turnover", chunk_trimmed, full_trimmed)


# ── 9. chunk_size 两次独立运行一致性 / Chunk mode repeatability ──────

def test_chunk_mode_repeatability():
    """
    chunk_size 模式: 两次独立 run_all() turnover DataFrame 一致。
    Chunk mode: two independent run_all() produce identical turnover DataFrame.
    """
    print("\n=== chunk_mode repeatability ===")
    for sid, factor, returns in iter_scenarios():
        if sid == SCENARIO_SMALL:
            continue

        ev1 = FactorEvaluator(
            factor, returns,
            n_groups=5, top_k=1, bottom_k=1,
            chunk_size=50,
        )
        ev1.run_all()

        ev2 = FactorEvaluator(
            factor, returns,
            n_groups=5, top_k=1, bottom_k=1,
            chunk_size=50,
        )
        ev2.run_all()

        check_frame_close(f"{sid}: chunk repeat", ev1.turnover, ev2.turnover)


# ── 10. evaluator.run_turnover() 与直接调用一致 ──────────────────────

def test_evaluator_vs_direct_call():
    """
    evaluator.run_turnover() 与直接调用 calc_turnover 一致。
    evaluator.run_turnover() matches direct calc_turnover call.
    """
    print("\n=== evaluator vs direct call ===")
    for sid, factor, returns in iter_scenarios():
        # 直接调用 / direct call
        direct = calc_turnover(factor, n_groups=5)

        # evaluator 调用 / evaluator call
        ev = FactorEvaluator(
            factor, returns,
            n_groups=5, top_k=1, bottom_k=1,
        )
        ev.run_grouping()
        ev.run_turnover()

        check_frame_close(f"{sid}: evaluator vs direct", ev.turnover, direct)


# ── 11. evaluator 缓存复用一致性 / Evaluator cache reuse ─────────────

def test_evaluator_turnover_cache_reuse():
    """
    evaluator: run_grouping() 缓存后 run_turnover() 结果与无缓存一致。
    evaluator: cached run_turnover() result matches uncached.
    """
    print("\n=== evaluator turnover cache reuse ===")
    for sid, factor, returns in iter_scenarios():
        # 有缓存 / with cache
        ev_cached = FactorEvaluator(
            factor, returns,
            n_groups=5, top_k=1, bottom_k=1,
        )
        ev_cached.run_grouping()
        ev_cached.run_turnover()

        # 无缓存 (清除后重新计算) / without cache
        ev_uncached = FactorEvaluator(
            factor, returns,
            n_groups=5, top_k=1, bottom_k=1,
        )
        ev_uncached._clear_group_cache()
        ev_uncached.run_turnover()

        check_frame_close(f"{sid}: cache reuse", ev_cached.turnover, ev_uncached.turnover)


# ── 12. 既有回归测试 / Existing regression tests ──────────────────────

def test_existing_regression():
    """既有 test_task12_turnover 的关键断言回归."""
    print("\n=== existing regression ===")

    # 稳定因子换手率 = 0
    ts = pd.date_range("2025-01-01", periods=5, freq="D")
    syms = ["A", "B", "C", "D", "E"]
    vals = np.tile([1.0, 2.0, 3.0, 4.0, 5.0], (5, 1))
    idx = pd.MultiIndex.from_product([ts, syms], names=["timestamp", "symbol"])
    factor = pd.Series(vals.ravel(), index=idx)
    result = calc_turnover(factor, n_groups=3)
    valid = result.iloc[1:].dropna().values.flatten()
    check("stable factor turnover=0", np.allclose(valid, 0.0))

    # 值域 [0, 1]
    rng = np.random.default_rng(42)
    ts2 = pd.date_range("2025-01-01", periods=20, freq="D")
    syms2 = [f"S{i}" for i in range(30)]
    vals2 = rng.standard_normal((20, 30))
    idx2 = pd.MultiIndex.from_product([ts2, syms2], names=["timestamp", "symbol"])
    factor2 = pd.Series(vals2.ravel(), index=idx2)
    result2 = calc_turnover(factor2, n_groups=5)
    valid2 = result2.iloc[1:].dropna().values.flatten()
    check("value range [0, 1]", (valid2 >= 0.0).all() and (valid2 <= 1.0).all())

    # 参数校验
    empty = pd.Series([], dtype=np.float64)
    try:
        calc_turnover(empty, n_groups=3)
        check("empty raises ValueError", False)
    except ValueError:
        check("empty raises ValueError", True)

    try:
        calc_turnover(factor2, n_groups=1)
        check("n_groups=1 raises ValueError", False)
    except ValueError:
        check("n_groups=1 raises ValueError", True)

    try:
        calc_turnover([1, 2, 3], n_groups=3)
        check("non-Series raises TypeError", False)
    except TypeError:
        check("non-Series raises TypeError", True)


# ── Main ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    test_6_scenarios_consistency()
    test_various_n_groups()
    test_precomputed_group_labels()
    test_high_nan_consistency()
    test_all_nan_input()
    test_single_period()
    test_minimal_data()
    test_stable_factor_zero_turnover()
    test_value_range()
    test_return_type_and_shape()
    test_chunk_mode_vs_full_mode()
    test_chunk_mode_repeatability()
    test_evaluator_vs_direct_call()
    test_evaluator_turnover_cache_reuse()
    test_existing_regression()

    print(f"\n{'='*50}")
    print(f"Total: {PASS + FAIL} checks, {PASS} passed, {FAIL} failed")
    if FAIL > 0:
        print("RESULT: FAIL")
        sys.exit(1)
    else:
        print("RESULT: ALL PASSED")
