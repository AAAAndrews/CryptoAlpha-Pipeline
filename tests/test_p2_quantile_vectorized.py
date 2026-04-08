"""
Task 10 测试 — P2 quantile_group numpy 向量化数值一致性
Test: P2 quantile_group numpy vectorized numerical consistency

验证:
1. 6 种 mock 场景 × group labels Series diff < 1e-10
2. zero_aware=True/False 一致性
3. 不同 n_groups (3/5/10)
4. 含 NaN 数据
5. 含大量重复值 (ties)
6. chunk_size 分块模式通过 evaluator 正确工作
"""

import sys
import os
import bisect
import traceback
import numpy as np
import pandas as pd

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from FactorAnalysis.grouping import quantile_group
from FactorAnalysis.evaluator import FactorEvaluator

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

    与 test_p0_chunk_cache.py / test_p1_portfolio_vectorized.py 保持一致。
    Consistent with test_p0_chunk_cache.py / test_p1_portfolio_vectorized.py.
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
# Reference 实现: bisect_right 逐值分组 (与 searchsorted 等价)
# Reference: bisect_right per-value grouping (equivalent to searchsorted)
#
# 使用 Python bisect 模块独立实现与 np.searchsorted(side='right') 等价的逻辑，
# 避免依赖 pd.qcut (其 boundary 行为与 searchsorted 不一致)。
# Uses Python bisect module to independently implement the same logic as
# np.searchsorted(side='right'), avoiding pd.qcut whose boundary behavior
# differs from searchsorted.
# ============================================================

def _ref_assign_group_py(vals: np.ndarray, n_groups: int) -> np.ndarray:
    """
    参考: 纯 Python bisect_right 实现 (单截面) / Reference: pure Python bisect_right (single section).

    与 _assign_group_vec 算法等价: percentile → dedup edges → bisect_right 赋组。
    Equivalent to _assign_group_vec: percentile → dedup edges → bisect_right assignment.
    """
    n = len(vals)
    if n == 0:
        return np.array([], dtype=np.float64)

    percentiles = np.linspace(0, 1, n_groups + 1)
    edges = np.quantile(vals, percentiles)

    # 去除重复边界 (等价于 duplicates='drop') / remove duplicate edges
    unique_edges = [edges[0]]
    for e in edges[1:]:
        if e != unique_edges[-1]:
            unique_edges.append(e)
    n_bins = len(unique_edges) - 1

    if n_bins < 1:
        return np.full(n, n_groups // 2, dtype=np.float64)

    # bisect_right 与 np.searchsorted(side='right') 等价
    # bisect_right is equivalent to np.searchsorted(side='right')
    labels = np.empty(n, dtype=np.float64)
    for i in range(n):
        pos = bisect.bisect_right(unique_edges, vals[i]) - 1
        labels[i] = max(0, min(pos, n_bins - 1))

    return labels


def _ref_zero_aware_py(vals: np.ndarray, n_groups: int) -> np.ndarray:
    """
    参考: 零值感知分组 (纯 Python) / Reference: zero-aware grouping (pure Python).

    按正负拆分后各自 bisect_right 分组，负值标签较低。
    Split by sign, bisect_right separately, negative gets lower labels.
    """
    neg_mask = vals <= 0
    pos_mask = vals > 0
    n_neg = neg_mask.sum()
    n_pos = pos_mask.sum()

    if n_neg == 0 or n_pos == 0:
        return _ref_assign_group_py(vals, n_groups)

    total = n_neg + n_pos

    # 按样本量比例分配分组数 / allocate groups proportionally
    n_neg_groups = max(1, round(n_groups * n_neg / total))
    n_pos_groups = n_groups - n_neg_groups
    if n_pos_groups < 1:
        n_pos_groups = 1
        n_neg_groups = n_groups - n_pos_groups

    result = np.full(len(vals), np.nan, dtype=np.float64)

    if n_neg > 0:
        result[neg_mask] = _ref_assign_group_py(vals[neg_mask], n_neg_groups)

    if n_pos > 0:
        result[pos_mask] = _ref_assign_group_py(vals[pos_mask], n_pos_groups) + n_neg_groups

    return result


def ref_quantile_group(
    factor: pd.Series,
    n_groups: int = 5,
    zero_aware: bool = False,
) -> pd.Series:
    """
    参考: 逐截面 bisect_right 分组 / Reference: per-section bisect_right grouping.

    使用 groupby(level=0) 逐截面调用纯 Python bisect_right 实现，
    与向量化的 np.searchsorted(side='right') 逻辑完全等价。
    Uses groupby(level=0) per-section pure Python bisect_right,
    fully equivalent to vectorized np.searchsorted(side='right') logic.
    """
    labels = pd.Series(np.nan, index=factor.index, dtype=np.float64)

    for ts, group in factor.groupby(level=0):
        valid = group.notna() & np.isfinite(group)
        if valid.sum() == 0:
            continue
        valid_vals = group[valid].values.astype(np.float64)
        if zero_aware:
            ref_labels = _ref_zero_aware_py(valid_vals, n_groups)
        else:
            ref_labels = _ref_assign_group_py(valid_vals, n_groups)
        # 用索引直接赋值 / assign by index directly
        labels.loc[group[valid].index] = ref_labels

    return labels


def series_max_diff(a: pd.Series, b: pd.Series) -> float:
    """两个 Series 的最大绝对差异 / Max absolute difference between two Series."""
    a, b = a.align(b, join="outer")
    mask = a.notna() & b.notna()
    if not mask.any():
        return 0.0
    return float(np.max(np.abs(a[mask] - b[mask])))


# ============================================================
# 6 种 mock 场景 / 6 mock scenarios
# ============================================================

SCENARIOS = [
    {"name": "标准场景", "n_dates": 120, "n_symbols": 50, "nan_ratio": 0.0,
     "signal_strength": 0.02, "seed": 42},
    {"name": "大数据量", "n_dates": 200, "n_symbols": 80, "nan_ratio": 0.0,
     "signal_strength": 0.02, "seed": 123},
    {"name": "小数据量", "n_dates": 20, "n_symbols": 15, "nan_ratio": 0.0,
     "signal_strength": 0.02, "seed": 7},
    {"name": "高NaN比例", "n_dates": 100, "n_symbols": 40, "nan_ratio": 0.10,
     "signal_strength": 0.02, "seed": 99},
    {"name": "弱信号+多分组", "n_dates": 120, "n_symbols": 50, "nan_ratio": 0.0,
     "signal_strength": 0.005, "seed": 55},
    {"name": "紧凑分块", "n_dates": 150, "n_symbols": 60, "nan_ratio": 0.02,
     "signal_strength": 0.02, "seed": 77},
]


# ============================================================
# 测试 1: 6 种 mock 场景 × group labels diff < 1e-10
# Test 1: 6 mock scenarios × group labels diff < 1e-10
# ============================================================

def test_6scenarios_basic():
    """6 种 mock 场景下 vectorized vs reference 分组标签一致性"""
    print("\n=== Test 1: 6 scenarios × group labels diff < 1e-10 ===")
    tol = 1e-10

    for sc in SCENARIOS:
        factor, _ = make_synthetic(**{k: v for k, v in sc.items() if k != "name"})

        # vectorized / 向量化
        v_labels = quantile_group(factor, n_groups=5)
        # reference / 参考
        r_labels = ref_quantile_group(factor, n_groups=5)

        d = series_max_diff(v_labels, r_labels)

        # NaN 位置一致性 / NaN position consistency
        v_nan = v_labels.isna()
        r_nan = r_labels.isna()
        nan_match = (v_nan == r_nan).all()

        ok(f"{sc['name']} diff={d:.2e} < {tol:.0e}", d < tol)
        ok(f"{sc['name']} NaN positions match", nan_match)


# ============================================================
# 测试 2: zero_aware=True/False 一致性
# Test 2: zero_aware=True/False consistency
# ============================================================

def test_zero_aware():
    """zero_aware 模式下 vectorized vs reference 一致性"""
    print("\n=== Test 2: zero_aware=True/False consistency ===")
    tol = 1e-10

    for sc in SCENARIOS:
        factor, _ = make_synthetic(**{k: v for k, v in sc.items() if k != "name"})

        for za in [False, True]:
            v_labels = quantile_group(factor, n_groups=5, zero_aware=za)
            r_labels = ref_quantile_group(factor, n_groups=5, zero_aware=za)

            d = series_max_diff(v_labels, r_labels)
            ok(f"{sc['name']} zero_aware={za} diff={d:.2e} < {tol:.0e}", d < tol)


# ============================================================
# 测试 3: 不同 n_groups (3/5/10)
# Test 3: different n_groups (3/5/10)
# ============================================================

def test_n_groups():
    """不同 n_groups 下 vectorized vs reference 一致性"""
    print("\n=== Test 3: different n_groups (3/5/10) ===")
    tol = 1e-10
    factor, _ = make_synthetic(n_dates=100, n_symbols=40, seed=42)

    for n_groups in [2, 3, 5, 10, 20]:
        v_labels = quantile_group(factor, n_groups=n_groups)
        r_labels = ref_quantile_group(factor, n_groups=n_groups)

        d = series_max_diff(v_labels, r_labels)

        # 标签范围检查 / label range check
        valid = v_labels.dropna()
        label_min = valid.min()
        label_max = valid.max()

        ok(f"n_groups={n_groups} diff={d:.2e} < {tol:.0e}", d < tol)
        ok(f"n_groups={n_groups} labels in [{label_min:.0f}, {label_max:.0f}]",
           label_min >= 0 and label_max <= n_groups - 1)


# ============================================================
# 测试 4: 含 NaN 数据
# Test 4: NaN data handling
# ============================================================

def test_nan_data():
    """含 NaN 因子值时分组标签一致"""
    print("\n=== Test 4: NaN data handling ===")
    tol = 1e-10

    for nan_ratio in [0.05, 0.10, 0.20, 0.30]:
        factor, _ = make_synthetic(n_dates=100, n_symbols=40, seed=42, nan_ratio=nan_ratio)

        for za in [False, True]:
            v_labels = quantile_group(factor, n_groups=5, zero_aware=za)
            r_labels = ref_quantile_group(factor, n_groups=5, zero_aware=za)

            d = series_max_diff(v_labels, r_labels)

            # NaN 位置一致 / NaN positions consistent
            v_nan = v_labels.isna()
            r_nan = r_labels.isna()
            nan_match = (v_nan == r_nan).all()

            ok(f"nan={nan_ratio:.0%} zero_aware={za} diff={d:.2e} < {tol:.0e}", d < tol)
            ok(f"nan={nan_ratio:.0%} zero_aware={za} NaN positions match", nan_match)


# ============================================================
# 测试 5: 含大量重复值 (ties)
# Test 5: large duplicate values (ties)
# ============================================================

def test_duplicate_values():
    """含大量重复值时分组标签一致"""
    print("\n=== Test 5: duplicate values (ties) ===")
    tol = 1e-10

    # 场景 A: 将因子值离散化为有限个整数 (大量 ties)
    # Scenario A: discretize factor values to finite integers (many ties)
    rng = np.random.default_rng(42)
    dates = pd.date_range("2025-01-01", periods=80, freq="B")
    symbols = [f"S{i:03d}" for i in range(50)]
    idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])

    # 仅取 10 个不同值 / only 10 distinct values
    raw = rng.integers(0, 10, size=(80, 50))
    factor = pd.Series(raw.ravel(), index=idx, dtype=np.float64)

    v_labels = quantile_group(factor, n_groups=5)
    r_labels = ref_quantile_group(factor, n_groups=5)
    d = series_max_diff(v_labels, r_labels)
    ok(f"discrete 10 values diff={d:.2e} < {tol:.0e}", d < tol)

    # 场景 B: 所有时点所有 symbol 取相同值 (极端 ties)
    # Scenario B: all symbols same value at every timestamp (extreme ties)
    factor_uniform = pd.Series(3.14, index=idx, dtype=np.float64)
    v_labels_u = quantile_group(factor_uniform, n_groups=5)
    r_labels_u = ref_quantile_group(factor_uniform, n_groups=5)
    d = series_max_diff(v_labels_u, r_labels_u)
    ok(f"all-same value diff={d:.2e} < {tol:.0e}", d < tol)

    # 场景 C: 大量 ties + NaN 混合
    # Scenario C: many ties + NaN mixed
    factor_mixed = factor.copy()
    nan_mask = rng.random(len(factor_mixed)) < 0.15
    factor_mixed[nan_mask] = np.nan
    v_labels_m = quantile_group(factor_mixed, n_groups=5, zero_aware=False)
    r_labels_m = ref_quantile_group(factor_mixed, n_groups=5, zero_aware=False)
    d = series_max_diff(v_labels_m, r_labels_m)
    ok(f"discrete + 15% NaN diff={d:.2e} < {tol:.0e}", d < tol)

    # 场景 D: 3 个不同值, 5 组 → duplicates='drop' 触发
    # Scenario D: 3 distinct values, 5 groups → duplicates='drop' triggered
    factor_3vals = pd.Series(rng.choice([1.0, 2.0, 3.0], size=(80, 50)).ravel(), index=idx, dtype=np.float64)
    v_labels_3 = quantile_group(factor_3vals, n_groups=5)
    r_labels_3 = ref_quantile_group(factor_3vals, n_groups=5)
    d = series_max_diff(v_labels_3, r_labels_3)
    ok(f"3 values / 5 groups diff={d:.2e} < {tol:.0e}", d < tol)

    # zero_aware + ties
    v_labels_za = quantile_group(factor, n_groups=5, zero_aware=True)
    r_labels_za = ref_quantile_group(factor, n_groups=5, zero_aware=True)
    d = series_max_diff(v_labels_za, r_labels_za)
    ok(f"discrete 10 values zero_aware diff={d:.2e} < {tol:.0e}", d < tol)


# ============================================================
# 测试 6: chunk_size 分块模式通过 evaluator
# Test 6: chunk_size mode via evaluator
# ============================================================

def test_evaluator_chunk_mode():
    """evaluator chunk_size 分块模式分组标签正确"""
    print("\n=== Test 6: evaluator chunk_size mode ===")
    tol = 1e-8  # 分块边界可能有微小浮点差异 / chunk boundary may have minor float diff

    factor, returns = make_synthetic(n_dates=120, n_symbols=50, seed=42)

    # 全量模式 / full mode
    ev_full = FactorEvaluator(factor, returns, n_groups=5, chunk_size=None)
    ev_full.run_grouping()

    for chunk_size in [30, 50, 60]:
        ev_chunk = FactorEvaluator(factor, returns, n_groups=5, chunk_size=chunk_size)
        ev_chunk.run_grouping()

        d = series_max_diff(ev_full.group_labels, ev_chunk.group_labels)
        ok(f"chunk_size={chunk_size} diff={d:.2e} < {tol:.0e}", d < tol)

        # NaN 位置一致 / NaN positions consistent
        full_nan = ev_full.group_labels.isna()
        chunk_nan = ev_chunk.group_labels.isna()
        ok(f"chunk_size={chunk_size} NaN positions match", (full_nan == chunk_nan).all())


# ============================================================
# 测试 7: 输出结构一致性 (索引/长度/类型)
# Test 7: output structure consistency (index/length/dtype)
# ============================================================

def test_structure_consistency():
    """输出标签的索引、长度、类型一致性"""
    print("\n=== Test 7: structure consistency ===")
    factor, _ = make_synthetic(n_dates=100, n_symbols=30, seed=42)

    v_labels = quantile_group(factor, n_groups=5)

    # 索引与输入一致 / index matches input
    ok("index matches factor", v_labels.index.equals(factor.index))

    # 长度一致 / length matches
    ok("length matches factor", len(v_labels) == len(factor))

    # dtype 为 float64 / dtype is float64
    ok("dtype is float64", v_labels.dtype == np.float64)

    # 有效标签为整数 (虽然 dtype 是 float64) / valid labels are integer-valued
    valid = v_labels.dropna()
    ok("valid labels are integer-valued", np.all(valid == valid.astype(int)))

    # NaN 因子值对应 NaN 标签 / NaN factor → NaN label
    factor_nan = factor.copy()
    factor_nan.iloc[:10] = np.nan
    v_nan = quantile_group(factor_nan, n_groups=5)
    ok("NaN factor → NaN labels (first 10)", v_nan.iloc[:10].isna().all())


# ============================================================
# 测试 8: n_groups < 2 边界情况
# Test 8: n_groups < 2 edge case
# ============================================================

def test_invalid_n_groups():
    """n_groups < 2 时应抛出 ValueError"""
    print("\n=== Test 8: n_groups < 2 edge case ===")
    factor, _ = make_synthetic(n_dates=20, n_symbols=10, seed=42)

    for bad_n in [0, 1, -1]:
        try:
            quantile_group(factor, n_groups=bad_n)
            ok(f"n_groups={bad_n} raises ValueError", False)
        except ValueError:
            ok(f"n_groups={bad_n} raises ValueError", True)


# ============================================================
# 测试 9: 空 factor / 全 NaN
# Test 9: empty factor / all NaN
# ============================================================

def test_empty_and_all_nan():
    """空 factor 和全 NaN 时正确返回全 NaN 标签"""
    print("\n=== Test 9: empty and all NaN ===")
    dates = pd.date_range("2025-01-01", periods=10, freq="B")
    symbols = [f"S{i:03d}" for i in range(5)]
    idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])

    # 全 NaN / all NaN
    factor_nan = pd.Series(np.nan, index=idx, dtype=np.float64)
    labels_nan = quantile_group(factor_nan, n_groups=5)
    ok("all NaN → all NaN labels", labels_nan.isna().all())
    ok("all NaN → correct length", len(labels_nan) == len(factor_nan))

    # 空 Series / empty Series
    factor_empty = pd.Series([], dtype=np.float64)
    labels_empty = quantile_group(factor_empty, n_groups=5)
    ok("empty → correct length 0", len(labels_empty) == 0)


# ============================================================
# 测试 10: zero_aware 边界情况 (全正/全负/含零)
# Test 10: zero_aware edge cases (all positive / all negative / with zero)
# ============================================================

def test_zero_aware_edge_cases():
    """zero_aware 边界: 全正/全负/含零/混合"""
    print("\n=== Test 10: zero_aware edge cases ===")
    tol = 1e-10

    rng = np.random.default_rng(42)
    dates = pd.date_range("2025-01-01", periods=30, freq="B")
    symbols = [f"S{i:03d}" for i in range(20)]
    idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])

    # 全正 / all positive
    factor_pos = pd.Series(np.abs(rng.standard_normal(600)), index=idx, dtype=np.float64)
    v_pos = quantile_group(factor_pos, n_groups=5, zero_aware=True)
    r_pos = ref_quantile_group(factor_pos, n_groups=5, zero_aware=True)
    d = series_max_diff(v_pos, r_pos)
    ok(f"all positive diff={d:.2e} < {tol:.0e}", d < tol)

    # 全负 / all negative
    factor_neg = pd.Series(-np.abs(rng.standard_normal(600)), index=idx, dtype=np.float64)
    v_neg = quantile_group(factor_neg, n_groups=5, zero_aware=True)
    r_neg = ref_quantile_group(factor_neg, n_groups=5, zero_aware=True)
    d = series_max_diff(v_neg, r_neg)
    ok(f"all negative diff={d:.2e} < {tol:.0e}", d < tol)

    # 含零 / with zero values
    factor_zero = pd.Series(rng.standard_normal(600), index=idx, dtype=np.float64)
    zero_positions = rng.choice(600, size=50, replace=False)
    factor_zero.iloc[zero_positions] = 0.0
    v_zero = quantile_group(factor_zero, n_groups=5, zero_aware=True)
    r_zero = ref_quantile_group(factor_zero, n_groups=5, zero_aware=True)
    d = series_max_diff(v_zero, r_zero)
    ok(f"with zeros diff={d:.2e} < {tol:.0e}", d < tol)


# ============================================================
# 主函数 / Main
# ============================================================

if __name__ == "__main__":
    print("=" * 60)
    print("Task 10: P2 quantile_group numpy 向量化数值一致性")
    print("P2 quantile_group numpy vectorized numerical consistency")
    print("=" * 60)

    test_6scenarios_basic()
    test_zero_aware()
    test_n_groups()
    test_nan_data()
    test_duplicate_values()
    test_evaluator_chunk_mode()
    test_structure_consistency()
    test_invalid_n_groups()
    test_empty_and_all_nan()
    test_zero_aware_edge_cases()

    print("\n" + "=" * 60)
    print(f"Result: {passed}/{checks} checks passed")
    print("=" * 60)

    if passed < checks:
        sys.exit(1)
