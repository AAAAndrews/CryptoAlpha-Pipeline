"""
FactorAnalysis 端到端验证测试 / FactorAnalysis E2E Validation Tests

使用合成数据验证 FactorEvaluator 全流程：
IC/RankIC/ICIR 数值合理性、分组收益单调性、净值曲线无 NaN、
交易成本扣除后收益下降、report 输出 DataFrame 包含所有关键列。

Uses synthetic data to validate the full FactorEvaluator pipeline:
IC/RankIC/ICIR reasonableness, group return monotonicity, no NaN in equity curves,
cost deduction reduces returns, report DataFrame contains all key columns.
"""

import sys
import warnings

import numpy as np
import pandas as pd

# 项目根目录加入 sys.path / add project root to sys.path
PROJECT_ROOT = r"F:\MyCryptoTrading\CryptoAlpha\CryptoAlpha-Pipeline"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from FactorAnalysis import FactorEvaluator, generate_report, calc_ic, calc_rank_ic, calc_icir

warnings.filterwarnings("ignore")

# ---------------------------------------------------------------------------
# 合成数据生成 / Synthetic data generation
# ---------------------------------------------------------------------------

def make_synthetic_data(
    n_days: int = 120,
    n_symbols: int = 50,
    ic_strength: float = 0.15,
    seed: int = 42,
) -> tuple[pd.Series, pd.Series]:
    """
    生成因子值与前向收益率，使其具有可控的 IC 强度。
    Generate factor values and forward returns with controllable IC strength.

    因子值 ~ N(0,1)，收益率 = ic_strength * factor + noise。
    Factor ~ N(0,1), returns = ic_strength * factor + noise.
    """
    rng = np.random.default_rng(seed)

    dates = pd.bdate_range("2025-01-01", periods=n_days)
    symbols = [f"S{i:03d}" for i in range(n_symbols)]

    idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])
    n = len(idx)

    factor_values = rng.standard_normal(n)
    noise = rng.standard_normal(n) * (1.0 - abs(ic_strength))
    return_values = ic_strength * factor_values + noise

    factor = pd.Series(factor_values, index=idx, dtype=np.float64)
    returns = pd.Series(return_values, index=idx, dtype=np.float64)

    return factor, returns


# ---------------------------------------------------------------------------
# 测试执行 / Test execution
# ---------------------------------------------------------------------------

passed = 0
failed = 0
check_log = []


def check(name: str, condition: bool) -> None:
    """记录检查结果 / Log check result."""
    global passed, failed
    if condition:
        passed += 1
        check_log.append(f"  [PASS] {name}")
    else:
        failed += 1
        check_log.append(f"  [FAIL] {name}")


def main() -> None:
    global passed, failed

    print("=" * 60)
    print("FactorAnalysis E2E Validation Tests")
    print("=" * 60)

    # --- 1. 合成数据生成 / Synthetic data generation ---
    print("\n--- 1. Synthetic Data ---")
    factor, returns = make_synthetic_data(n_days=120, n_symbols=50, ic_strength=0.15)
    check("factor type is pd.Series", isinstance(factor, pd.Series))
    check("returns type is pd.Series", isinstance(returns, pd.Series))
    check("factor has MultiIndex", isinstance(factor.index, pd.MultiIndex))
    check("returns has MultiIndex", isinstance(returns.index, pd.MultiIndex))
    check("factor index names", factor.index.names == ["timestamp", "symbol"])
    check("factor shape", factor.shape == (120 * 50,))
    check("factor finite", np.all(np.isfinite(factor)))
    check("returns finite", np.all(np.isfinite(returns)))

    # --- 2. FactorEvaluator 默认参数运行 / FactorEvaluator default run ---
    print("\n--- 2. FactorEvaluator Default Run ---")
    ev = FactorEvaluator(factor, returns, cost_rate=0.001)
    result = ev.run()
    check("run() returns self", result is ev)
    check("ic is pd.Series", isinstance(ev.ic, pd.Series))
    check("rank_ic is pd.Series", isinstance(ev.rank_ic, pd.Series))
    check("icir is float", isinstance(ev.icir, float))

    # --- 3. IC / RankIC / ICIR 数值合理性 / IC metrics reasonableness ---
    print("\n--- 3. IC / RankIC / ICIR Reasonableness ---")
    ic_valid = ev.ic.dropna()
    rank_ic_valid = ev.rank_ic.dropna()
    check("IC has valid values", len(ic_valid) > 0)
    check("RankIC has valid values", len(rank_ic_valid) > 0)
    check("IC mean > 0 (positive IC strength)", ic_valid.mean() > 0)
    check("IC mean < 0.5 (not perfect)", ic_valid.mean() < 0.5)
    check("RankIC mean > 0 (positive IC strength)", rank_ic_valid.mean() > 0)
    check("ICIR > 0 (positive predictive power)", ev.icir > 0)
    check("IC values in [-1, 1]", ((ic_valid >= -1) & (ic_valid <= 1)).all())
    check("RankIC values in [-1, 1]", ((rank_ic_valid >= -1) & (rank_ic_valid <= 1)).all())

    # --- 4. 分组标签 / Group labels ---
    print("\n--- 4. Group Labels ---")
    check("group_labels is pd.Series", isinstance(ev.group_labels, pd.Series))
    check("group_labels has MultiIndex", isinstance(ev.group_labels.index, pd.MultiIndex))
    check("group_labels unique values", set(ev.group_labels.dropna().unique()) <= set(range(5)))
    check("group_labels no NaN", ev.group_labels.isna().sum() == 0)

    # --- 5. 分组收益单调性 / Group return monotonicity ---
    print("\n--- 5. Group Return Monotonicity ---")
    # 按分组计算平均收益，验证单调递增 / compute avg return per group, verify monotonic
    labels = ev.group_labels
    rets = ev.returns
    df = pd.DataFrame({"label": labels, "returns": rets})
    group_mean_ret = df.groupby("label")["returns"].mean()
    # 组号越大（因子值越高），平均收益应越大 / higher group → higher return
    group_values = sorted(group_mean_ret.index.dropna().tolist())
    monotonic_count = 0
    for i in range(len(group_values) - 1):
        if group_mean_ret[group_values[i]] <= group_mean_ret[group_values[i + 1]]:
            monotonic_count += 1
    monotonic_ratio = monotonic_count / max(len(group_values) - 1, 1)
    check(
        f"group return monotonic ratio={monotonic_ratio:.2f} >= 0.6",
        monotonic_ratio >= 0.6,
    )

    # --- 6. 净值曲线 / Equity curves ---
    print("\n--- 6. Equity Curves ---")
    for curve_name, curve in [
        ("long_curve", ev.long_curve),
        ("short_curve", ev.short_curve),
        ("hedge_curve", ev.hedge_curve),
    ]:
        check(f"{curve_name} is pd.Series", isinstance(curve, pd.Series))
        check(f"{curve_name} has DatetimeIndex", isinstance(curve.index, pd.DatetimeTZDtype) or isinstance(curve.index, pd.DatetimeIndex))
        check(f"{curve_name} start=1.0", abs(curve.iloc[0] - 1.0) < 1e-10)
        check(f"{curve_name} no NaN", curve.isna().sum() == 0)
        check(f"{curve_name} all finite", np.all(np.isfinite(curve)))
        check(f"{curve_name} length > 1", len(curve) > 1)

    # 对冲曲线应优于单一方向（因为因子有正向 IC）
    # hedge curve should outperform single direction (factor has positive IC)
    check(
        "hedge_return > long_return (positive IC)",
        (ev.hedge_curve.iloc[-1] - 1.0) >= (ev.long_curve.iloc[-1] - 1.0),
    )
    check(
        "hedge_return > short_return (positive IC)",
        (ev.hedge_curve.iloc[-1] - 1.0) >= (ev.short_curve.iloc[-1] - 1.0),
    )

    # --- 7. 交易成本扣除 / Cost deduction ---
    print("\n--- 7. Cost Deduction ---")
    check(
        "hedge_curve_after_cost is pd.Series",
        isinstance(ev.hedge_curve_after_cost, pd.Series),
    )
    check(
        "hedge_curve_after_cost start=1.0",
        abs(ev.hedge_curve_after_cost.iloc[0] - 1.0) < 1e-10,
    )
    check("hedge_curve_after_cost no NaN", ev.hedge_curve_after_cost.isna().sum() == 0)
    check("hedge_curve_after_cost all finite", np.all(np.isfinite(ev.hedge_curve_after_cost)))

    # 成本扣除后收益应低于扣除前 / after-cost return should be lower
    hedge_ret = ev.hedge_curve.iloc[-1] - 1.0
    hedge_after_ret = ev.hedge_curve_after_cost.iloc[-1] - 1.0
    check(
        f"hedge_after_cost return ({hedge_after_ret:.4f}) <= hedge return ({hedge_ret:.4f})",
        hedge_after_ret <= hedge_ret,
    )

    # --- 8. 绩效比率 / Performance ratios ---
    print("\n--- 8. Performance Ratios ---")
    for ratio_name in ["sharpe", "calmar", "sortino", "sharpe_after_cost", "calmar_after_cost", "sortino_after_cost"]:
        val = getattr(ev, ratio_name)
        check(f"{ratio_name} is float", isinstance(val, (int, float)))
        check(f"{ratio_name} is finite", np.isfinite(val))

    # 成本后比率应 <= 成本前比率 / after-cost ratios <= before-cost ratios
    check("sharpe_after_cost <= sharpe", ev.sharpe_after_cost <= ev.sharpe + 1e-10)
    check("calmar_after_cost <= calmar", ev.calmar_after_cost <= ev.calmar + 1e-10)
    check("sortino_after_cost <= sortino", ev.sortino_after_cost <= ev.sortino + 1e-10)

    # --- 9. generate_report / Report output ---
    print("\n--- 9. Report Output ---")
    report = generate_report(ev)
    expected_columns = [
        "IC_mean", "IC_std", "RankIC_mean", "RankIC_std", "ICIR",
        "long_return", "short_return", "hedge_return", "hedge_return_after_cost",
        "sharpe", "calmar", "sortino",
        "sharpe_after_cost", "calmar_after_cost", "sortino_after_cost",
        "n_days",
    ]
    check("report is pd.DataFrame", isinstance(report, pd.DataFrame))
    check("report has 1 row", len(report) == 1)
    check(f"report has {len(expected_columns)} columns", len(report.columns) == len(expected_columns))
    for col in expected_columns:
        check(f"report has column '{col}'", col in report.columns)
    check("report all values finite", report.apply(lambda c: np.all(np.isfinite(c))).all())

    # --- 10. 自定义参数运行 / Custom parameters run ---
    print("\n--- 10. Custom Parameters Run ---")
    ev2 = FactorEvaluator(
        factor, returns,
        n_groups=3, top_k=1, bottom_k=1,
        cost_rate=0.005, risk_free_rate=0.03, periods_per_year=252,
    )
    ev2.run()
    report2 = generate_report(ev2)
    check("custom params report is DataFrame", isinstance(report2, pd.DataFrame))
    check("custom params report has 1 row", len(report2) == 1)
    # 更高成本应导致更低收益 / higher cost → lower return
    check(
        "higher cost → lower hedge_after_cost return",
        report2["hedge_return_after_cost"].iloc[0] <= report["hedge_return_after_cost"].iloc[0],
    )

    # --- 11. 未调用 run() 时 generate_report 报错 / ValueError on unrun evaluator ---
    print("\n--- 11. Error Handling ---")
    ev3 = FactorEvaluator(factor, returns)
    try:
        generate_report(ev3)
        check("ValueError on unrun evaluator", False)
    except ValueError:
        check("ValueError on unrun evaluator", True)

    # --- 12. 弱因子（零 IC）场景 / Weak factor (zero IC) scenario ---
    print("\n--- 12. Weak Factor Scenario ---")
    rng = np.random.default_rng(99)
    dates = pd.bdate_range("2025-01-01", periods=60)
    symbols = [f"W{i:03d}" for i in range(30)]
    idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])
    weak_factor = pd.Series(rng.standard_normal(len(idx)), index=idx, dtype=np.float64)
    weak_returns = pd.Series(rng.standard_normal(len(idx)) * 0.01, index=idx, dtype=np.float64)
    # 因子和收益独立 → IC ≈ 0 / factor and returns independent → IC ≈ 0
    ev_weak = FactorEvaluator(weak_factor, weak_returns)
    ev_weak.run()
    check("weak factor ICIR near zero", abs(ev_weak.icir) < 1.0)
    check("weak factor report OK", isinstance(generate_report(ev_weak), pd.DataFrame))

    # --- 13. 公共导出 / Public exports ---
    print("\n--- 13. Public Exports ---")
    import FactorAnalysis
    check("__all__ has 13 exports", len(FactorAnalysis.__all__) == 13)
    check("FactorEvaluator in __all__", "FactorEvaluator" in FactorAnalysis.__all__)
    check("generate_report in __all__", "generate_report" in FactorAnalysis.__all__)

    # --- 汇总 / Summary ---
    print("\n" + "=" * 60)
    total = passed + failed
    for line in check_log:
        print(line)
    print("=" * 60)
    print(f"TOTAL: {total} | PASSED: {passed} | FAILED: {failed}")
    if failed == 0:
        print("ALL CHECKS PASSED")
    else:
        print(f"WARNING: {failed} check(s) failed")
    print("=" * 60)

    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
