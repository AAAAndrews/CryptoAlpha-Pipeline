"""
验证测试 — Task 24: FactorEvaluator
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


def make_synthetic(n_dates=60, n_symbols=50, seed=42):
    """生成合成因子和收益率 / Generate synthetic factor and returns."""
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2025-01-01", periods=n_dates, freq="B")
    symbols = [f"S{i:03d}" for i in range(n_symbols)]
    idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])

    # 因子值含一定预测能力 / factor with some predictive power
    true_signal = rng.standard_normal((n_dates, n_symbols))
    noise = rng.standard_normal((n_dates, n_symbols)) * 0.5
    factor_values = true_signal + noise
    # 收益率与因子值弱正相关 / returns weakly correlated with factor
    returns_values = true_signal * 0.02 + rng.standard_normal((n_dates, n_symbols)) * 0.03

    factor = pd.Series(factor_values.ravel(), index=idx, dtype=np.float64)
    returns = pd.Series(returns_values.ravel(), index=idx, dtype=np.float64)
    return factor, returns


try:
    print("=== Task 24: FactorEvaluator 验证 ===\n")

    # 1. 导入 / Import
    print("1. Import")
    from FactorAnalysis import FactorEvaluator
    ok("FactorEvaluator importable from FactorAnalysis", True)
    ok("FactorEvaluator is a class", isinstance(FactorEvaluator, type))

    # 2. 实例化 / Instantiation
    print("\n2. Instantiation")
    factor, returns = make_synthetic()
    ev = FactorEvaluator(factor, returns)
    ok("instance created", ev is not None)
    ok("factor stored", ev.factor is factor)
    ok("returns stored", ev.returns is returns)
    ok("n_groups default 5", ev.n_groups == 5)
    ok("top_k default 1", ev.top_k == 1)
    ok("bottom_k default 1", ev.bottom_k == 1)
    ok("cost_rate default 0.001", ev.cost_rate == 0.001)
    ok("risk_free_rate default 0.0", ev.risk_free_rate == 0.0)
    ok("periods_per_year default 252", ev.periods_per_year == 252)

    # 自定义参数 / custom params
    ev2 = FactorEvaluator(factor, returns, n_groups=3, top_k=1, bottom_k=1,
                           cost_rate=0.002, risk_free_rate=0.03, periods_per_year=365)
    ok("custom n_groups", ev2.n_groups == 3)
    ok("custom cost_rate", ev2.cost_rate == 0.002)
    ok("custom risk_free_rate", ev2.risk_free_rate == 0.03)
    ok("custom periods_per_year", ev2.periods_per_year == 365)

    # 3. run() 返回 self / run() returns self
    print("\n3. run() execution")
    result = ev.run()
    ok("run() returns self", result is ev)

    # 4. IC 指标 / IC metrics
    print("\n4. IC metrics")
    ok("ic is pd.Series", isinstance(ev.ic, pd.Series))
    ok("ic indexed by timestamp", isinstance(ev.ic.index, pd.DatetimeIndex))
    ok("ic has valid length", len(ev.ic) == 60)
    ok("ic has finite values", np.all(np.isfinite(ev.ic.dropna().values)))
    ok("rank_ic is pd.Series", isinstance(ev.rank_ic, pd.Series))
    ok("rank_ic indexed by timestamp", isinstance(ev.rank_ic.index, pd.DatetimeIndex))
    ok("rank_ic has valid length", len(ev.rank_ic) == 60)
    ok("icir is float", isinstance(ev.icir, float))
    ok("icir is finite", np.isfinite(ev.icir))

    # 5. 分组标签 / Group labels
    print("\n5. Group labels")
    ok("group_labels is pd.Series", isinstance(ev.group_labels, pd.Series))
    ok("group_labels same index as factor", ev.group_labels.index.equals(factor.index))
    ok("group_labels values in range", set(ev.group_labels.dropna().unique()).issubset(set(range(5))))

    # 6. 净值曲线 / Equity curves
    print("\n6. Equity curves")
    for name, curve in [("long_curve", ev.long_curve),
                        ("short_curve", ev.short_curve),
                        ("hedge_curve", ev.hedge_curve)]:
        ok(f"{name} is pd.Series", isinstance(curve, pd.Series))
        ok(f"{name} indexed by timestamp", isinstance(curve.index, pd.DatetimeIndex))
        ok(f"{name} length matches dates", len(curve) == 60)
        ok(f"{name} starts at 1.0", abs(curve.iloc[0] - 1.0) < 1e-10)
        ok(f"{name} no NaN", curve.notna().all())
        ok(f"{name} all finite", np.all(np.isfinite(curve.values)))

    # 7. 成本扣除 / Cost deduction
    print("\n7. Cost-adjusted curve")
    ok("hedge_curve_after_cost is pd.Series", isinstance(ev.hedge_curve_after_cost, pd.Series))
    ok("after_cost starts at 1.0", abs(ev.hedge_curve_after_cost.iloc[0] - 1.0) < 1e-10)
    ok("after_cost no NaN", ev.hedge_curve_after_cost.notna().all())
    # 成本后净值应 <= 成本前 / after-cost equity <= before-cost
    ok("after_cost <= hedge_curve (final)", ev.hedge_curve_after_cost.iloc[-1] <= ev.hedge_curve.iloc[-1] + 1e-10)

    # 8. 绩效比率 / Performance ratios
    print("\n8. Performance ratios")
    for name, val in [("sharpe", ev.sharpe), ("calmar", ev.calmar), ("sortino", ev.sortino)]:
        ok(f"{name} is float", isinstance(val, float))
        ok(f"{name} is finite", np.isfinite(val))
    for name, val in [("sharpe_after_cost", ev.sharpe_after_cost),
                      ("calmar_after_cost", ev.calmar_after_cost),
                      ("sortino_after_cost", ev.sortino_after_cost)]:
        ok(f"{name} is float", isinstance(val, float))
        ok(f"{name} is finite", np.isfinite(val))

    # 成本后 sharpe <= 成本前 / after-cost sharpe <= before-cost sharpe
    ok("sharpe_after_cost <= sharpe", ev.sharpe_after_cost <= ev.sharpe + 1e-10)

    # 9. 自定义参数运行 / Custom params run
    print("\n9. Custom params run")
    ev2.run()
    ok("ev2.ic computed", ev2.ic is not None)
    ok("ev2.hedge_curve computed", ev2.hedge_curve is not None)
    ok("ev2 group_labels with 3 groups", set(ev2.group_labels.dropna().unique()).issubset(set(range(3))))

    # 10. __all__ 导出 / __all__ export
    print("\n10. Public exports")
    import FactorAnalysis
    ok("FactorEvaluator in __all__", "FactorEvaluator" in FactorAnalysis.__all__)

    # 11. Edge case: all-NaN factor
    print("\n11. Edge case: all-NaN factor")
    nan_factor = pd.Series(np.nan, index=factor.index, dtype=np.float64)
    ev_nan = FactorEvaluator(nan_factor, returns)
    ev_nan.run()
    ok("all-NaN: ic computed (all NaN)", ev_nan.ic.notna().sum() == 0)
    ok("all-NaN: icir == 0.0", ev_nan.icir == 0.0)
    ok("all-NaN: hedge_curve is flat 1.0", (ev_nan.hedge_curve == 1.0).all())

    # 12. Edge case: custom risk_free_rate
    print("\n12. Edge case: non-zero risk_free_rate")
    ev_rf = FactorEvaluator(factor, returns, risk_free_rate=0.03)
    ev_rf.run()
    ok("non-zero rf: sharpe computed", np.isfinite(ev_rf.sharpe))
    # 有 rf 时 sharpe 应低于无 rf / sharpe with rf should be <= sharpe without rf
    ok("sharpe with rf <= sharpe without rf", ev_rf.sharpe <= ev.sharpe + 1e-10)

    print(f"\n{'='*40}")
    print(f"Total: {checks} checks")

except Exception as e:
    print(f"\nFATAL: {e}")
    traceback.print_exc()
    sys.exit(1)
