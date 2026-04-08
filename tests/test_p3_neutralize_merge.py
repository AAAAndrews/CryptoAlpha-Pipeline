"""
Task 12 测试 — P3 neutralize 合并数值一致性
Test: P3 neutralize merge numerical consistency

验证:
1. 6 种 mock 场景 × neutralized_curve diff < 1e-10 (vs reference groupby)
2. demeaned/group_adjust 四种组合一致性
3. _raw 模式一致性
4. chunk_size 分块模式通过 evaluator 正确工作
5. groups=int vs groups=Series 一致性
6. 边界情况: n_groups < 2, 空 factor, 全 NaN
"""

import sys
import os
import traceback
import numpy as np
import pandas as pd

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from FactorAnalysis.neutralize import calc_neutralized_curve
from FactorAnalysis.grouping import quantile_group
from FactorAnalysis.portfolio import calc_portfolio_curves
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
    Consistent with test_p0_chunk_cache.py and test_p1_portfolio_vectorized.py.
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
# Reference 实现: 旧 groupby.transform("mean") 逻辑
# Reference: old groupby.transform("mean") logic
# ============================================================

def ref_calc_neutralized_curve(
    factor: pd.Series,
    returns: pd.Series,
    groups: "pd.Series | int",
    demeaned: bool = True,
    group_adjust: bool = False,
    n_groups: int = 5,
    _raw: bool = False,
) -> pd.Series:
    """
    参考: 旧 groupby.transform("mean") 实现中性化曲线。

    使用两次独立 groupby.transform 计算 demean 和 group_adjust，
    再通过 quantile_group + calc_portfolio_curves 构建对冲曲线。
    Uses two separate groupby.transform calls for demean and group_adjust,
    then quantile_group + calc_portfolio_curves to build hedge curve.
    """
    # 解析分组标签 / resolve group labels
    if isinstance(groups, int):
        group_labels = quantile_group(factor, n_groups=groups)
    else:
        group_labels = groups

    df = pd.DataFrame({"factor": factor, "returns": returns, "group": group_labels})
    df = df.sort_index()

    # 组内因子去均值 (旧 groupby.transform 逻辑)
    # Demean factor within groups (old groupby.transform logic)
    if demeaned:
        group_means = df.groupby([pd.Grouper(level=0), "group"])["factor"].transform("mean")
        df["factor"] = df["factor"] - group_means

    # 组内收益去均值 (旧 groupby.transform 逻辑)
    # Adjust returns within groups (old groupby.transform logic)
    if group_adjust:
        ret_means = df.groupby([pd.Grouper(level=0), "group"])["returns"].transform("mean")
        df["returns"] = df["returns"] - ret_means

    # 按中性化因子排名分组 / rank neutralized factor into groups
    labels = quantile_group(df["factor"], n_groups=n_groups)

    # 构建对冲净值曲线 / build hedge equity curve
    _, _, hedge_curve = calc_portfolio_curves(
        df["factor"], df["returns"],
        n_groups=n_groups, top_k=1, bottom_k=1,
        rebalance_freq=1, _raw=_raw, group_labels=labels,
    )
    return hedge_curve


def series_max_diff(a: pd.Series, b: pd.Series) -> float:
    """两个 Series 的最大绝对差异 / Max absolute difference between two Series."""
    a, b = a.align(b, join="outer")
    mask = a.notna() & b.notna()
    if not mask.any():
        return 0.0
    return float(np.max(np.abs(a[mask] - b[mask])))


# ============================================================
# 6 种 Mock 场景 / 6 mock scenarios
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
# Test 1: 6 种 mock 场景 × neutralized_curve diff < 1e-10
# Test 1: 6 mock scenarios × neutralized_curve diff < 1e-10
# ============================================================

def test_6scenarios_basic():
    """6 种 mock 场景下 vectorized vs reference neutralized_curve 一致性"""
    print("\n=== Test 1: 6 scenarios × neutralized_curve diff < 1e-10 ===")
    tol = 1e-10

    for sc in SCENARIOS:
        factor, returns = make_synthetic(**{k: v for k, v in sc.items() if k != "name"})

        # vectorized (P3 实现) / vectorized (P3 implementation)
        curve_v = calc_neutralized_curve(factor, returns, groups=5, demeaned=True, group_adjust=False, n_groups=5)

        # reference (旧 groupby.transform 逻辑) / reference (old groupby.transform logic)
        curve_r = ref_calc_neutralized_curve(factor, returns, groups=5, demeaned=True, group_adjust=False, n_groups=5)

        diff = series_max_diff(curve_v, curve_r)
        ok(f"{sc['name']}: demeaned=True, group_adjust=False, diff={diff:.2e}", diff < tol)


# ============================================================
# Test 2: demeaned/group_adjust 四种组合一致性
# Test 2: demeaned/group_adjust 4-combo consistency
# ============================================================

def test_4combos():
    """demeaned/group_adjust 四种组合 × 6 场景一致性"""
    print("\n=== Test 2: demeaned/group_adjust 4 combos × 6 scenarios ===")
    tol = 1e-10
    combos = [
        {"demeaned": True, "group_adjust": False},
        {"demeaned": False, "group_adjust": True},
        {"demeaned": True, "group_adjust": True},
        {"demeaned": False, "group_adjust": False},
    ]

    for combo in combos:
        for sc in SCENARIOS:
            factor, returns = make_synthetic(**{k: v for k, v in sc.items() if k != "name"})

            curve_v = calc_neutralized_curve(
                factor, returns, groups=5,
                demeaned=combo["demeaned"], group_adjust=combo["group_adjust"],
                n_groups=5,
            )
            curve_r = ref_calc_neutralized_curve(
                factor, returns, groups=5,
                demeaned=combo["demeaned"], group_adjust=combo["group_adjust"],
                n_groups=5,
            )

            diff = series_max_diff(curve_v, curve_r)
            ok(
                f"{sc['name']}: demeaned={combo['demeaned']}, group_adjust={combo['group_adjust']}, diff={diff:.2e}",
                diff < tol,
            )


# ============================================================
# Test 3: _raw 模式一致性
# Test 3: _raw mode consistency
# ============================================================

def test_raw_mode():
    """_raw 模式下 vectorized vs reference 一致性"""
    print("\n=== Test 3: _raw mode consistency ===")
    tol = 1e-10

    for sc in SCENARIOS:
        factor, returns = make_synthetic(**{k: v for k, v in sc.items() if k != "name"})

        # _raw=True: 不覆写起始值为 1.0 / _raw=True: don't overwrite start to 1.0
        curve_v = calc_neutralized_curve(
            factor, returns, groups=5, demeaned=True, group_adjust=False,
            n_groups=5, _raw=True,
        )
        curve_r = ref_calc_neutralized_curve(
            factor, returns, groups=5, demeaned=True, group_adjust=False,
            n_groups=5, _raw=True,
        )

        diff = series_max_diff(curve_v, curve_r)
        ok(f"{sc['name']}: _raw=True diff={diff:.2e}", diff < tol)

        # _raw=False: 起始值覆写为 1.0 / _raw=False: start overwritten to 1.0
        curve_v2 = calc_neutralized_curve(
            factor, returns, groups=5, demeaned=True, group_adjust=False,
            n_groups=5, _raw=False,
        )
        curve_r2 = ref_calc_neutralized_curve(
            factor, returns, groups=5, demeaned=True, group_adjust=False,
            n_groups=5, _raw=False,
        )

        diff2 = series_max_diff(curve_v2, curve_r2)
        ok(f"{sc['name']}: _raw=False diff={diff2:.2e}", diff2 < tol)

        # _raw=False 时起始值 = 1.0 / _raw=False start value == 1.0
        ok(f"{sc['name']}: _raw=False start=1.0", abs(curve_v2.iloc[0] - 1.0) < 1e-12)


# ============================================================
# Test 4: groups=int vs groups=Series 一致性
# Test 4: groups=int vs groups=Series consistency
# ============================================================

def test_groups_int_vs_series():
    """groups=int (内部 quantile_group) vs groups=Series (预计算) 一致性"""
    print("\n=== Test 4: groups=int vs groups=Series consistency ===")
    tol = 1e-10

    for sc in SCENARIOS:
        factor, returns = make_synthetic(**{k: v for k, v in sc.items() if k != "name"})

        # groups=int: 内部调用 quantile_group / groups=int: internal quantile_group
        curve_int = calc_neutralized_curve(
            factor, returns, groups=5, demeaned=True, group_adjust=False, n_groups=5,
        )

        # groups=Series: 预计算标签传入 / groups=Series: pre-computed labels
        pre_labels = quantile_group(factor, n_groups=5)
        curve_ser = calc_neutralized_curve(
            factor, returns, groups=pre_labels, demeaned=True, group_adjust=False, n_groups=5,
        )

        diff = series_max_diff(curve_int, curve_ser)
        ok(f"{sc['name']}: groups=int vs Series diff={diff:.2e}", diff < tol)


# ============================================================
# Test 5: chunk_size 分块模式通过 evaluator
# Test 5: chunk_size chunked mode via evaluator
# ============================================================

def test_chunked_evaluator():
    """evaluator chunk_size 分块 vs 全量模式 neutralized_curve 一致性"""
    print("\n=== Test 5: evaluator chunked vs full mode consistency ===")
    tol = 1e-8

    chunk_sizes = [30, 50, 60]

    for sc in SCENARIOS:
        factor, returns = make_synthetic(**{k: v for k, v in sc.items() if k != "name"})

        # 全量模式 / full mode
        ev_full = FactorEvaluator(factor, returns, n_groups=5)
        ev_full.run_neutralize(groups=5, demeaned=True, group_adjust=False)
        full_curve = ev_full.neutralized_curve

        for cs in chunk_sizes:
            # 分块模式 / chunked mode
            ev_chunk = FactorEvaluator(factor, returns, n_groups=5, chunk_size=cs)
            ev_chunk.run_neutralize(groups=5, demeaned=True, group_adjust=False)
            chunk_curve = ev_chunk.neutralized_curve

            diff = series_max_diff(full_curve, chunk_curve)
            ok(f"{sc['name']}: chunk_size={cs} diff={diff:.2e}", diff < tol)


# ============================================================
# Test 6: 边界情况
# Test 6: edge cases
# ============================================================

def test_edge_cases():
    """边界情况: n_groups < 2 ValueError, 空 factor, 全 NaN"""
    print("\n=== Test 6: edge cases ===")

    factor, returns = make_synthetic(n_dates=50, n_symbols=20, seed=42)

    # n_groups < 2 → ValueError
    try:
        calc_neutralized_curve(factor, returns, groups=5, n_groups=1)
        ok("n_groups=1 raises ValueError", False)
    except ValueError:
        ok("n_groups=1 raises ValueError", True)

    # groups < 2 → ValueError
    try:
        calc_neutralized_curve(factor, returns, groups=1)
        ok("groups=1 raises ValueError", False)
    except ValueError:
        ok("groups=1 raises ValueError", True)

    # factor 非 pd.Series → ValueError
    try:
        calc_neutralized_curve(factor.values, returns, groups=5)
        ok("non-Series factor raises ValueError", False)
    except ValueError:
        ok("non-Series factor raises ValueError", True)

    # returns 非 pd.Series → ValueError
    try:
        calc_neutralized_curve(factor, returns.values, groups=5)
        ok("non-Series returns raises ValueError", False)
    except ValueError:
        ok("non-Series returns raises ValueError", True)

    # groups 非法类型 → ValueError
    try:
        calc_neutralized_curve(factor, returns, groups="invalid")
        ok("invalid groups type raises ValueError", False)
    except ValueError:
        ok("invalid groups type raises ValueError", True)

    # 全 NaN factor → 结果长度正确
    factor_nan = pd.Series(np.nan, index=factor.index, dtype=np.float64)
    curve_nan = calc_neutralized_curve(factor_nan, returns, groups=5, n_groups=5)
    ok("all-NaN factor returns curve with correct length", len(curve_nan) == 50)


# ============================================================
# Test 7: 不同 n_groups 一致性
# Test 7: different n_groups consistency
# ============================================================

def test_different_n_groups():
    """不同 n_groups 下 vectorized vs reference 一致性"""
    print("\n=== Test 7: different n_groups consistency ===")
    tol = 1e-10
    n_groups_list = [2, 3, 5, 10]

    for ng in n_groups_list:
        factor, returns = make_synthetic(n_dates=100, n_symbols=40, seed=42 + ng)

        curve_v = calc_neutralized_curve(
            factor, returns, groups=5, demeaned=True, group_adjust=True, n_groups=ng,
        )
        curve_r = ref_calc_neutralized_curve(
            factor, returns, groups=5, demeaned=True, group_adjust=True, n_groups=ng,
        )

        diff = series_max_diff(curve_v, curve_r)
        ok(f"n_groups={ng}: diff={diff:.2e}", diff < tol)


# ============================================================
# Test 8: 结构验证 — 曲线长度、索引、起始值
# Test 8: structure validation — curve length, index, start value
# ============================================================

def test_structure():
    """neutralized_curve 结构验证"""
    print("\n=== Test 8: structure validation ===")

    for sc in SCENARIOS:
        factor, returns = make_synthetic(**{k: v for k, v in sc.items() if k != "name"})
        curve = calc_neutralized_curve(factor, returns, groups=5, n_groups=5)

        n_dates = sc["n_dates"]
        ok(f"{sc['name']}: curve length == {n_dates}", len(curve) == n_dates)
        ok(f"{sc['name']}: start value == 1.0", abs(curve.iloc[0] - 1.0) < 1e-12)
        ok(f"{sc['name']}: index is DatetimeIndex", isinstance(curve.index, pd.DatetimeIndex))
        ok(f"{sc['name']}: dtype is float64", curve.dtype == np.float64)


# ============================================================
# 主函数 / Main
# ============================================================

if __name__ == "__main__":
    test_6scenarios_basic()
    test_4combos()
    test_raw_mode()
    test_groups_int_vs_series()
    test_chunked_evaluator()
    test_edge_cases()
    test_different_n_groups()
    test_structure()

    print(f"\n{'='*60}")
    print(f"Total: {passed}/{checks} checks passed")
    if passed == checks:
        print("ALL PASSED")
    else:
        print(f"FAILED: {checks - passed} checks")
    print(f"{'='*60}")
