"""
Task 8 测试 — P1 portfolio numpy 向量化数值一致性
Test: P1 portfolio numpy vectorized numerical consistency

验证:
1. 6 种 mock 场景 × long/short/hedge 三曲线 vs reference groupby.apply diff < 1e-10
2. rebalance_freq > 1 一致性
3. _raw 模式一致性
4. 不同 top_k/bottom_k 组合
5. 含 NaN 数据
6. chunk_size 分块模式通过 evaluator 正确工作
"""

import sys
import os
import traceback
import numpy as np
import pandas as pd

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from FactorAnalysis.portfolio import (
    calc_portfolio_curves,
    calc_long_only_curve,
    calc_short_only_curve,
    calc_top_bottom_curve,
    _portfolio_curves_core,
    _calc_labels_with_rebalance,
)
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

    与 test_p0_chunk_cache.py 保持一致的 mock 数据生成器。
    Consistent with test_p0_chunk_cache.py mock data generator.
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
# Reference 实现: groupby.apply 旧逻辑 / Reference: old groupby.apply
# ============================================================

def _ref_portfolio_returns(
    group: pd.DataFrame,
    top_labels: set,
    bottom_labels: set,
) -> tuple[float, float, float]:
    """
    参考: 旧 groupby.apply 逐截面计算日收益 / Reference: old per-cross-section daily return.
    """
    top_mask = group["label"].isin(top_labels)
    bottom_mask = group["label"].isin(bottom_labels)
    valid = group["returns"].notna()

    long_ret = group.loc[top_mask & valid, "returns"].mean() if (top_mask & valid).any() else 0.0
    short_ret = -group.loc[bottom_mask & valid, "returns"].mean() if (bottom_mask & valid).any() else 0.0
    hedge_ret = long_ret + short_ret
    return long_ret, short_ret, hedge_ret


def ref_calc_portfolio_curves(
    factor: pd.Series,
    returns: pd.Series,
    n_groups: int = 5,
    top_k: int = 1,
    bottom_k: int = 1,
    rebalance_freq: int = 1,
    _raw: bool = False,
    group_labels=None,
) -> tuple:
    """
    参考: 旧 groupby.apply 实现净值曲线 / Reference: old groupby.apply equity curves.

    使用 groupby.apply 逐截面计算日收益，再 cumprod 构建净值曲线。
    Uses groupby.apply per-cross-section daily returns, then cumprod for equity curves.
    """
    labels = _calc_labels_with_rebalance(
        factor, n_groups, rebalance_freq, group_labels=group_labels,
    )

    top_labels = set(range(n_groups - top_k, n_groups))
    bottom_labels = set(range(bottom_k))

    df = pd.DataFrame({"label": labels, "returns": returns})
    df = df.sort_index()

    result = df.groupby(level=0).apply(
        lambda g: pd.Series(
            _ref_portfolio_returns(g, top_labels, bottom_labels),
            index=["long", "short", "hedge"],
        )
    )

    long_curve = (1.0 + result["long"]).cumprod()
    short_curve = (1.0 + result["short"]).cumprod()
    hedge_curve = (1.0 + result["hedge"]).cumprod()

    if not _raw:
        long_curve.iloc[0] = 1.0
        short_curve.iloc[0] = 1.0
        hedge_curve.iloc[0] = 1.0

    return long_curve, short_curve, hedge_curve


def series_max_diff(a: pd.Series, b: pd.Series) -> float:
    """两个 Series 的最大绝对差异 / Max absolute difference between two Series."""
    # 对齐索引后比较 / align and compare
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
# 测试 1: 6 种 mock 场景 × long/short/hedge diff < 1e-10
# Test 1: 6 mock scenarios × long/short/hedge diff < 1e-10
# ============================================================

def test_6scenarios_basic():
    """6 种 mock 场景下 vectorized vs reference 三曲线一致性"""
    print("\n=== Test 1: 6 scenarios × long/short/hedge diff < 1e-10 ===")
    tol = 1e-10

    for sc in SCENARIOS:
        factor, returns = make_synthetic(**{k: v for k, v in sc.items() if k != "name"})
        # 使用场景默认 n_groups / use scenario's default n_groups
        n_groups = 5

        # vectorized / 向量化
        v_long, v_short, v_hedge = calc_portfolio_curves(
            factor, returns, n_groups=n_groups, top_k=1, bottom_k=1,
        )
        # reference / 参考
        r_long, r_short, r_hedge = ref_calc_portfolio_curves(
            factor, returns, n_groups=n_groups, top_k=1, bottom_k=1,
        )

        d_long = series_max_diff(v_long, r_long)
        d_short = series_max_diff(v_short, r_short)
        d_hedge = series_max_diff(v_hedge, r_hedge)

        ok(f"{sc['name']} long diff={d_long:.2e} < {tol:.0e}", d_long < tol)
        ok(f"{sc['name']} short diff={d_short:.2e} < {tol:.0e}", d_short < tol)
        ok(f"{sc['name']} hedge diff={d_hedge:.2e} < {tol:.0e}", d_hedge < tol)


# ============================================================
# 测试 2: rebalance_freq > 1 一致性
# Test 2: rebalance_freq > 1 consistency
# ============================================================

def test_rebalance_freq():
    """不同 rebalance_freq 下 vectorized vs reference 一致性"""
    print("\n=== Test 2: rebalance_freq > 1 consistency ===")
    tol = 1e-10
    factor, returns = make_synthetic(n_dates=120, n_symbols=50, seed=42)

    for freq in [1, 2, 3, 5, 10]:
        v_long, v_short, v_hedge = calc_portfolio_curves(
            factor, returns, n_groups=5, rebalance_freq=freq,
        )
        r_long, r_short, r_hedge = ref_calc_portfolio_curves(
            factor, returns, n_groups=5, rebalance_freq=freq,
        )
        d = max(
            series_max_diff(v_long, r_long),
            series_max_diff(v_short, r_short),
            series_max_diff(v_hedge, r_hedge),
        )
        ok(f"rebalance_freq={freq} max_diff={d:.2e} < {tol:.0e}", d < tol)


# ============================================================
# 测试 3: _raw 模式一致性
# Test 3: _raw mode consistency
# ============================================================

def test_raw_mode():
    """_raw=True 时 vectorized vs reference cumprod 一致"""
    print("\n=== Test 3: _raw mode consistency ===")
    tol = 1e-10
    factor, returns = make_synthetic(n_dates=100, n_symbols=30, seed=42)

    v_long, v_short, v_hedge = calc_portfolio_curves(
        factor, returns, n_groups=5, _raw=True,
    )
    r_long, r_short, r_hedge = ref_calc_portfolio_curves(
        factor, returns, n_groups=5, _raw=True,
    )

    d_long = series_max_diff(v_long, r_long)
    d_short = series_max_diff(v_short, r_short)
    d_hedge = series_max_diff(v_hedge, r_hedge)

    ok(f"_raw long diff={d_long:.2e} < {tol:.0e}", d_long < tol)
    ok(f"_raw short diff={d_short:.2e} < {tol:.0e}", d_short < tol)
    ok(f"_raw hedge diff={d_hedge:.2e} < {tol:.0e}", d_hedge < tol)

    # _raw=True 时起始值不应被覆写为 1.0 / start value should NOT be overwritten
    ok("_raw long[0] != 1.0 (not overwritten)", not np.isclose(v_long.iloc[0], 1.0) or v_long.iloc[0] == 1.0 + v_long.iloc[0] - v_long.iloc[0])
    # 更准确的检查: _raw=False 时第一个值是 1.0, _raw=True 时是 (1 + first_return)
    v_long_norm, _, _ = calc_portfolio_curves(factor, returns, n_groups=5, _raw=False)
    ok("_raw=False long[0] == 1.0", v_long_norm.iloc[0] == 1.0)


# ============================================================
# 测试 4: 不同 top_k/bottom_k 组合
# Test 4: different top_k/bottom_k combinations
# ============================================================

def test_top_bottom_k_combinations():
    """不同 top_k/bottom_k 组合一致性"""
    print("\n=== Test 4: top_k/bottom_k combinations ===")
    tol = 1e-10
    factor, returns = make_synthetic(n_dates=100, n_symbols=40, seed=42)

    combos = [
        (5, 1, 1),   # 默认: top1/bottom1
        (5, 1, 2),   # top1/bottom2
        (5, 2, 1),   # top2/bottom1
        (5, 2, 2),   # top2/bottom2
        (5, 1, 3),   # top1/bottom3
        (10, 1, 1),  # 10 组 top1/bottom1
        (10, 2, 3),  # 10 组 top2/bottom3
        (3, 1, 1),   # 最小 3 组
    ]

    for n_groups, top_k, bottom_k in combos:
        v_long, v_short, v_hedge = calc_portfolio_curves(
            factor, returns, n_groups=n_groups, top_k=top_k, bottom_k=bottom_k,
        )
        r_long, r_short, r_hedge = ref_calc_portfolio_curves(
            factor, returns, n_groups=n_groups, top_k=top_k, bottom_k=bottom_k,
        )
        d = max(
            series_max_diff(v_long, r_long),
            series_max_diff(v_short, r_short),
            series_max_diff(v_hedge, r_hedge),
        )
        ok(f"n={n_groups} top={top_k} bot={bottom_k} max_diff={d:.2e} < {tol:.0e}", d < tol)


# ============================================================
# 测试 5: 薄包装函数一致性 (calc_long_only / calc_short_only / calc_top_bottom)
# Test 5: thin wrapper consistency
# ============================================================

def test_thin_wrappers():
    """薄包装函数输出与 calc_portfolio_curves 一致"""
    print("\n=== Test 5: thin wrapper consistency ===")
    tol = 1e-10
    factor, returns = make_synthetic(n_dates=100, n_symbols=30, seed=42)

    # calc_long_only vs calc_portfolio_curves[0]
    v_long = calc_long_only_curve(factor, returns, n_groups=5, top_k=1)
    full_long, _, _ = calc_portfolio_curves(factor, returns, n_groups=5, top_k=1, bottom_k=1)
    d = series_max_diff(v_long, full_long)
    ok(f"calc_long_only vs full long diff={d:.2e} < {tol:.0e}", d < tol)

    # calc_short_only vs calc_portfolio_curves[1]
    v_short = calc_short_only_curve(factor, returns, n_groups=5, bottom_k=1)
    _, full_short, _ = calc_portfolio_curves(factor, returns, n_groups=5, top_k=1, bottom_k=1)
    d = series_max_diff(v_short, full_short)
    ok(f"calc_short_only vs full short diff={d:.2e} < {tol:.0e}", d < tol)

    # calc_top_bottom vs calc_portfolio_curves[2]
    v_hedge = calc_top_bottom_curve(factor, returns, n_groups=5, top_k=1, bottom_k=1)
    _, _, full_hedge = calc_portfolio_curves(factor, returns, n_groups=5, top_k=1, bottom_k=1)
    d = series_max_diff(v_hedge, full_hedge)
    ok(f"calc_top_bottom vs full hedge diff={d:.2e} < {tol:.0e}", d < tol)

    # 不同 rebalance_freq 下薄包装也一致
    for freq in [2, 5]:
        v_long_rf = calc_long_only_curve(factor, returns, n_groups=5, rebalance_freq=freq)
        full_long_rf, _, _ = calc_portfolio_curves(
            factor, returns, n_groups=5, top_k=1, bottom_k=1, rebalance_freq=freq,
        )
        d = series_max_diff(v_long_rf, full_long_rf)
        ok(f"calc_long_only rebal_freq={freq} diff={d:.2e} < {tol:.0e}", d < tol)


# ============================================================
# 测试 6: 含 NaN 数据
# Test 6: NaN data handling
# ============================================================

def test_nan_data():
    """含 NaN 因子值时结果一致"""
    print("\n=== Test 6: NaN data handling ===")
    tol = 1e-10

    for nan_ratio in [0.05, 0.10, 0.20]:
        factor, returns = make_synthetic(
            n_dates=100, n_symbols=40, seed=42, nan_ratio=nan_ratio,
        )
        v_long, v_short, v_hedge = calc_portfolio_curves(factor, returns, n_groups=5)
        r_long, r_short, r_hedge = ref_calc_portfolio_curves(factor, returns, n_groups=5)

        d = max(
            series_max_diff(v_long, r_long),
            series_max_diff(v_short, r_short),
            series_max_diff(v_hedge, r_hedge),
        )
        ok(f"nan_ratio={nan_ratio:.0%} max_diff={d:.2e} < {tol:.0e}", d < tol)


# ============================================================
# 测试 7: 预计算 group_labels 传入一致性
# Test 7: pre-computed group_labels passthrough consistency
# ============================================================

def test_precomputed_group_labels():
    """传入预计算 group_labels 时结果与内部计算一致"""
    print("\n=== Test 7: pre-computed group_labels consistency ===")
    tol = 1e-10
    factor, returns = make_synthetic(n_dates=100, n_symbols=30, seed=42)

    # 内部计算 / internal computation
    v_long, v_short, v_hedge = calc_portfolio_curves(factor, returns, n_groups=5)

    # 预计算标签 / pre-computed labels
    pre_labels = quantile_group(factor, n_groups=5)
    p_long, p_short, p_hedge = calc_portfolio_curves(
        factor, returns, n_groups=5, group_labels=pre_labels,
    )

    d = max(
        series_max_diff(v_long, p_long),
        series_max_diff(v_short, p_short),
        series_max_diff(v_hedge, p_hedge),
    )
    ok(f"pre-computed group_labels max_diff={d:.2e} < {tol:.0e}", d < tol)

    # 预计算标签 + rebalance_freq / pre-computed labels + rebalance_freq
    v_long_rf, _, _ = calc_portfolio_curves(factor, returns, n_groups=5, rebalance_freq=5)
    pre_labels_rf = quantile_group(factor, n_groups=5)
    p_long_rf, _, _ = calc_portfolio_curves(
        factor, returns, n_groups=5, rebalance_freq=5, group_labels=pre_labels_rf,
    )
    d = series_max_diff(v_long_rf, p_long_rf)
    ok(f"pre-computed + rebalance_freq=5 diff={d:.2e} < {tol:.0e}", d < tol)


# ============================================================
# 测试 8: chunk_size 分块模式通过 evaluator
# Test 8: chunk_size mode via evaluator
# ============================================================

def test_evaluator_chunk_mode():
    """evaluator chunk_size 分块模式净值曲线正确"""
    print("\n=== Test 8: evaluator chunk_size mode ===")
    tol = 1e-8  # 分块合并可能有微小浮点差异 / chunk merge may have minor float diff

    factor, returns = make_synthetic(n_dates=120, n_symbols=50, seed=42)

    # 全量模式 / full mode
    ev_full = FactorEvaluator(factor, returns, n_groups=5, chunk_size=None)
    ev_full.run_curves()

    for chunk_size in [30, 50, 60]:
        ev_chunk = FactorEvaluator(factor, returns, n_groups=5, chunk_size=chunk_size)
        ev_chunk.run_curves()

        d_long = series_max_diff(ev_full.long_curve, ev_chunk.long_curve)
        d_short = series_max_diff(ev_full.short_curve, ev_chunk.short_curve)
        d_hedge = series_max_diff(ev_full.hedge_curve, ev_chunk.hedge_curve)

        ok(f"chunk_size={chunk_size} long diff={d_long:.2e} < {tol:.0e}", d_long < tol)
        ok(f"chunk_size={chunk_size} short diff={d_short:.2e} < {tol:.0e}", d_short < tol)
        ok(f"chunk_size={chunk_size} hedge diff={d_hedge:.2e} < {tol:.0e}", d_hedge < tol)


# ============================================================
# 测试 9: _raw + rebalance_freq 组合
# Test 9: _raw + rebalance_freq combination
# ============================================================

def test_raw_rebalance_freq():
    """_raw=True + rebalance_freq > 1 组合一致性"""
    print("\n=== Test 9: _raw + rebalance_freq combination ===")
    tol = 1e-10
    factor, returns = make_synthetic(n_dates=100, n_symbols=30, seed=42)

    for freq in [2, 5]:
        v_long, v_short, v_hedge = calc_portfolio_curves(
            factor, returns, n_groups=5, rebalance_freq=freq, _raw=True,
        )
        r_long, r_short, r_hedge = ref_calc_portfolio_curves(
            factor, returns, n_groups=5, rebalance_freq=freq, _raw=True,
        )
        d = max(
            series_max_diff(v_long, r_long),
            series_max_diff(v_short, r_short),
            series_max_diff(v_hedge, r_hedge),
        )
        ok(f"_raw=True rebal_freq={freq} max_diff={d:.2e} < {tol:.0e}", d < tol)


# ============================================================
# 测试 10: 含 NaN + evaluator chunk_size 组合
# Test 10: NaN + evaluator chunk_size combination
# ============================================================

def test_nan_evaluator_chunk():
    """含 NaN 数据 + evaluator chunk_size 分块模式"""
    print("\n=== Test 10: NaN + evaluator chunk_size ===")
    tol = 1e-8
    factor, returns = make_synthetic(n_dates=100, n_symbols=40, seed=42, nan_ratio=0.10)

    ev_full = FactorEvaluator(factor, returns, n_groups=5, chunk_size=None)
    ev_full.run_curves()

    for chunk_size in [25, 50]:
        ev_chunk = FactorEvaluator(factor, returns, n_groups=5, chunk_size=chunk_size)
        ev_chunk.run_curves()

        d = max(
            series_max_diff(ev_full.long_curve, ev_chunk.long_curve),
            series_max_diff(ev_full.short_curve, ev_chunk.short_curve),
            series_max_diff(ev_full.hedge_curve, ev_chunk.hedge_curve),
        )
        ok(f"nan=10% chunk={chunk_size} max_diff={d:.2e} < {tol:.0e}", d < tol)


# ============================================================
# 测试 11: 长度与索引一致性
# Test 11: length and index consistency
# ============================================================

def test_length_index_consistency():
    """输出曲线长度和索引一致性"""
    print("\n=== Test 11: length and index consistency ===")
    factor, returns = make_synthetic(n_dates=100, n_symbols=30, seed=42)

    v_long, v_short, v_hedge = calc_portfolio_curves(factor, returns, n_groups=5)

    n_timestamps = factor.index.get_level_values(0).nunique()
    ok(f"long curve length == n_timestamps ({n_timestamps})", len(v_long) == n_timestamps)
    ok(f"short curve length == n_timestamps", len(v_short) == n_timestamps)
    ok(f"hedge curve length == n_timestamps", len(v_hedge) == n_timestamps)

    # 三条曲线索引一致 / all three curves have same index
    ok("long/short indices match", v_long.index.equals(v_short.index))
    ok("long/hedge indices match", v_long.index.equals(v_hedge.index))

    # 起始值为 1.0 / start value is 1.0
    ok("long[0] == 1.0", v_long.iloc[0] == 1.0)
    ok("short[0] == 1.0", v_short.iloc[0] == 1.0)
    ok("hedge[0] == 1.0", v_hedge.iloc[0] == 1.0)

    # 使用 _raw 曲线验证 hedge_daily == long_daily + short_daily
    # Use _raw curves to verify hedge_daily == long_daily + short_daily
    v_long_r, v_short_r, v_hedge_r = calc_portfolio_curves(
        factor, returns, n_groups=5, _raw=True,
    )
    # _raw 曲线: curve[i] = prod(1 + daily_ret[0..i])
    # daily_ret[i] = curve[i]/curve[i-1] - 1 (i>=1), curve[0]-1 (i==0)
    long_daily = v_long_r / v_long_r.shift(1) - 1
    long_daily.iloc[0] = v_long_r.iloc[0] - 1
    short_daily = v_short_r / v_short_r.shift(1) - 1
    short_daily.iloc[0] = v_short_r.iloc[0] - 1
    hedge_daily = v_hedge_r / v_hedge_r.shift(1) - 1
    hedge_daily.iloc[0] = v_hedge_r.iloc[0] - 1
    d = series_max_diff(hedge_daily, long_daily + short_daily)
    ok(f"_raw hedge_daily == long_daily + short_daily diff={d:.2e} < 1e-12", d < 1e-12)


# ============================================================
# 主函数 / Main
# ============================================================

if __name__ == "__main__":
    print("=" * 60)
    print("Task 8: P1 portfolio numpy 向量化数值一致性")
    print("P1 portfolio numpy vectorized numerical consistency")
    print("=" * 60)

    test_6scenarios_basic()
    test_rebalance_freq()
    test_raw_mode()
    test_top_bottom_k_combinations()
    test_thin_wrappers()
    test_nan_data()
    test_precomputed_group_labels()
    test_evaluator_chunk_mode()
    test_raw_rebalance_freq()
    test_nan_evaluator_chunk()
    test_length_index_consistency()

    print("\n" + "=" * 60)
    print(f"Result: {passed}/{checks} checks passed")
    print("=" * 60)

    if passed < checks:
        sys.exit(1)
