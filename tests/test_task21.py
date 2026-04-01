"""
Task 21 验证: FactorEvaluator Tear Sheet 分层编排模式
Validate: independent sub-method calls, generate_report(select),
run_all() full pipeline, new attributes, backward compatibility.
"""

import sys
import traceback
import numpy as np
import pandas as pd

sys.path.insert(0, "F:/MyCryptoTrading/CryptoAlpha/CryptoAlpha-Pipeline")

passed = 0
failed = 0


def check(name: str, condition: bool):
    global passed, failed
    if condition:
        passed += 1
        print(f"  [PASS] {name}")
    else:
        failed += 1
        print(f"  [FAIL] {name}")


def make_factor_returns(n_dates=80, n_symbols=30, seed=42):
    """生成合成因子和收益率 / Generate synthetic factor and returns."""
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2024-01-01", periods=n_dates, freq="B")
    symbols = [f"S{i:03d}" for i in range(n_symbols)]
    idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])

    true_signal = rng.standard_normal((n_dates, n_symbols))
    noise = rng.standard_normal((n_dates, n_symbols)) * 0.3
    factor_values = true_signal + noise
    returns_values = true_signal * 0.02 + rng.standard_normal((n_dates, n_symbols)) * 0.03

    factor = pd.Series(factor_values.ravel(), index=idx, dtype=np.float64)
    returns = pd.Series(returns_values.ravel(), index=idx, dtype=np.float64)
    return factor, returns


print("=" * 60)
print("Task 21 验证: FactorEvaluator Tear Sheet 分层编排")
print("=" * 60)

# --- 1. Import ---
print("\n[1] Import 测试")
try:
    from FactorAnalysis import FactorEvaluator
    check("FactorEvaluator importable", True)
    check("FactorEvaluator is a class", isinstance(FactorEvaluator, type))
except Exception as e:
    check(f"import failed: {e}", False)
    traceback.print_exc()

# --- 2. Independent sub-method: run_metrics() ---
print("\n[2] run_metrics() 独立调用测试")
factor, returns = make_factor_returns()
try:
    ev = FactorEvaluator(factor, returns)
    result = ev.run_metrics()
    check("run_metrics() returns self", result is ev)
    check("ic is pd.Series", isinstance(ev.ic, pd.Series))
    check("ic indexed by timestamp", isinstance(ev.ic.index, pd.DatetimeIndex))
    check("ic has finite values", np.all(np.isfinite(ev.ic.dropna().values)))
    check("rank_ic is pd.Series", isinstance(ev.rank_ic, pd.Series))
    check("rank_ic indexed by timestamp", isinstance(ev.rank_ic.index, pd.DatetimeIndex))
    check("icir is float", isinstance(ev.icir, float))
    check("icir is finite", np.isfinite(ev.icir))
    # 新增属性 / new attribute
    check("ic_stats is pd.Series", isinstance(ev.ic_stats, pd.Series))
    check("ic_stats has t_stat", "t_stat" in ev.ic_stats.index)
    check("ic_stats has p_value", "p_value" in ev.ic_stats.index)
    check("ic_stats has IC_skew", "IC_skew" in ev.ic_stats.index)
    check("ic_stats has IC_kurtosis", "IC_kurtosis" in ev.ic_stats.index)
    # 其他属性仍为 None / other attributes remain None
    check("group_labels is None", ev.group_labels is None)
    check("long_curve is None", ev.long_curve is None)
    check("turnover is None", ev.turnover is None)
    check("neutralized_curve is None", ev.neutralized_curve is None)
except Exception as e:
    check(f"run_metrics failed: {e}", False)
    traceback.print_exc()

# --- 3. Independent sub-method: run_grouping() ---
print("\n[3] run_grouping() 独立调用测试")
try:
    ev = FactorEvaluator(factor, returns)
    result = ev.run_grouping()
    check("run_grouping() returns self", result is ev)
    check("group_labels is pd.Series", isinstance(ev.group_labels, pd.Series))
    check("group_labels same index as factor", ev.group_labels.index.equals(factor.index))
    check("group_labels in range [0,4]", set(ev.group_labels.dropna().unique()).issubset(set(range(5))))
    check("ic is None (not run)", ev.ic is None)
except Exception as e:
    check(f"run_grouping failed: {e}", False)
    traceback.print_exc()

# --- 4. Independent sub-method: run_curves() ---
print("\n[4] run_curves() 独立调用测试")
try:
    ev = FactorEvaluator(factor, returns)
    result = ev.run_curves()
    check("run_curves() returns self", result is ev)
    for name in ["long_curve", "short_curve", "hedge_curve", "hedge_curve_after_cost"]:
        curve = getattr(ev, name)
        check(f"{name} is pd.Series", isinstance(curve, pd.Series))
        check(f"{name} starts at 1.0", abs(curve.iloc[0] - 1.0) < 1e-10)
        check(f"{name} no NaN", curve.notna().all())
    check("sharpe is float", isinstance(ev.sharpe, float))
    check("calmar is float", isinstance(ev.calmar, float))
    check("sortino is float", isinstance(ev.sortino, float))
    check("sharpe_after_cost is float", isinstance(ev.sharpe_after_cost, float))
    check("sharpe_after_cost <= sharpe", ev.sharpe_after_cost <= ev.sharpe + 1e-10)
    check("ic is None (not run)", ev.ic is None)
except Exception as e:
    check(f"run_curves failed: {e}", False)
    traceback.print_exc()

# --- 5. Independent sub-method: run_turnover() ---
print("\n[5] run_turnover() 独立调用测试")
try:
    ev = FactorEvaluator(factor, returns)
    result = ev.run_turnover()
    check("run_turnover() returns self", result is ev)
    # 新增属性 / new attributes
    check("turnover is pd.DataFrame", isinstance(ev.turnover, pd.DataFrame))
    check("turnover not empty", len(ev.turnover) > 0)
    check("rank_autocorr is pd.Series", isinstance(ev.rank_autocorr, pd.Series))
    check("rank_autocorr indexed by timestamp", isinstance(ev.rank_autocorr.index, pd.DatetimeIndex))
    check("ic is None (not run)", ev.ic is None)
except Exception as e:
    check(f"run_turnover failed: {e}", False)
    traceback.print_exc()

# --- 6. Independent sub-method: run_neutralize() ---
print("\n[6] run_neutralize() 独立调用测试")
try:
    ev = FactorEvaluator(factor, returns)
    result = ev.run_neutralize()
    check("run_neutralize() returns self", result is ev)
    check("neutralized_curve is pd.Series", isinstance(ev.neutralized_curve, pd.Series))
    check("neutralized_curve starts at 1.0", abs(ev.neutralized_curve.iloc[0] - 1.0) < 1e-10)
    check("neutralized_curve no NaN", ev.neutralized_curve.notna().all())
    check("ic is None (not run)", ev.ic is None)
except Exception as e:
    check(f"run_neutralize failed: {e}", False)
    traceback.print_exc()

# --- 7. run_neutralize() with custom params ---
print("\n[7] run_neutralize() 自定义参数测试")
try:
    ev = FactorEvaluator(factor, returns)
    ev.run_neutralize(demeaned=True, group_adjust=True, n_groups=3)
    check("neutralize with group_adjust OK", ev.neutralized_curve is not None)
    check("neutralize curve length correct", len(ev.neutralized_curve) == 80)
except Exception as e:
    check(f"run_neutralize params failed: {e}", False)
    traceback.print_exc()

# --- 8. Method chaining ---
print("\n[8] 链式调用测试")
try:
    ev = FactorEvaluator(factor, returns)
    result = (ev.run_metrics()
                 .run_grouping()
                 .run_turnover())
    check("chaining returns self", result is ev)
    check("chaining: ic computed", ev.ic is not None)
    check("chaining: group_labels computed", ev.group_labels is not None)
    check("chaining: turnover computed", ev.turnover is not None)
    check("chaining: curves NOT computed", ev.long_curve is None)
    check("chaining: neutralized NOT computed", ev.neutralized_curve is None)
except Exception as e:
    check(f"chaining failed: {e}", False)
    traceback.print_exc()

# --- 9. run_all() full pipeline ---
print("\n[9] run_all() 完整流程测试")
try:
    ev = FactorEvaluator(factor, returns)
    result = ev.run_all()
    check("run_all() returns self", result is ev)
    # 所有属性都应被计算 / all attributes should be computed
    check("all: ic computed", ev.ic is not None)
    check("all: rank_ic computed", ev.rank_ic is not None)
    check("all: icir computed", ev.icir is not None)
    check("all: ic_stats computed", ev.ic_stats is not None)
    check("all: group_labels computed", ev.group_labels is not None)
    check("all: long_curve computed", ev.long_curve is not None)
    check("all: short_curve computed", ev.short_curve is not None)
    check("all: hedge_curve computed", ev.hedge_curve is not None)
    check("all: hedge_curve_after_cost computed", ev.hedge_curve_after_cost is not None)
    check("all: sharpe computed", ev.sharpe is not None)
    check("all: calmar computed", ev.calmar is not None)
    check("all: sortino computed", ev.sortino is not None)
    check("all: sharpe_after_cost computed", ev.sharpe_after_cost is not None)
    check("all: turnover computed", ev.turnover is not None)
    check("all: rank_autocorr computed", ev.rank_autocorr is not None)
    check("all: neutralized_curve computed", ev.neutralized_curve is not None)
except Exception as e:
    check(f"run_all failed: {e}", False)
    traceback.print_exc()

# --- 10. Backward compatibility: run() == run_all() ---
print("\n[10] 向后兼容测试: run() == run_all()")
try:
    ev1 = FactorEvaluator(factor, returns, seed_compat := 123)
    ev2 = FactorEvaluator(factor, returns, seed_compat := 123)
    ev1.run()
    ev2.run_all()
    # 所有属性应完全一致 / all attributes should match
    check("run==run_all: ic equal", ev1.ic.equals(ev2.ic))
    check("run==run_all: rank_ic equal", ev1.rank_ic.equals(ev2.rank_ic))
    check("run==run_all: icir equal", ev1.icir == ev2.icir)
    check("run==run_all: ic_stats equal", ev1.ic_stats.equals(ev2.ic_stats))
    check("run==run_all: group_labels equal", ev1.group_labels.equals(ev2.group_labels))
    check("run==run_all: long_curve equal", ev1.long_curve.equals(ev2.long_curve))
    check("run==run_all: short_curve equal", ev1.short_curve.equals(ev2.short_curve))
    check("run==run_all: hedge_curve equal", ev1.hedge_curve.equals(ev2.hedge_curve))
    check("run==run_all: hedge_curve_after_cost equal", ev1.hedge_curve_after_cost.equals(ev2.hedge_curve_after_cost))
    check("run==run_all: sharpe equal", ev1.sharpe == ev2.sharpe)
    check("run==run_all: calmar equal", ev1.calmar == ev2.calmar)
    check("run==run_all: sortino equal", ev1.sortino == ev2.sortino)
    check("run==run_all: turnover equal", ev1.turnover.equals(ev2.turnover))
    check("run==run_all: rank_autocorr equal", ev1.rank_autocorr.equals(ev2.rank_autocorr))
    check("run==run_all: neutralized_curve equal", ev1.neutralized_curve.equals(ev2.neutralized_curve))
except Exception as e:
    check(f"backward compat failed: {e}", False)
    traceback.print_exc()

# --- 11. generate_report(select=None) — full report ---
print("\n[11] generate_report(select=None) 全量报告测试")
try:
    ev = FactorEvaluator(factor, returns)
    ev.run_all()
    report = ev.generate_report()
    check("full report is pd.DataFrame", isinstance(report, pd.DataFrame))
    check("full report has 1 row", len(report) == 1)
    # 检查所有预期字段 / check all expected fields
    expected_fields = [
        "IC_mean", "IC_std", "RankIC_mean", "RankIC_std", "ICIR",
        "IC_t_stat", "IC_p_value", "IC_skew", "IC_kurtosis",
        "n_groups_used", "long_return", "short_return", "hedge_return",
        "hedge_return_after_cost", "sharpe", "calmar", "sortino",
        "sharpe_after_cost", "calmar_after_cost", "sortino_after_cost",
        "n_days", "avg_turnover", "avg_rank_autocorr", "neutralized_return",
    ]
    for field in expected_fields:
        check(f"report has '{field}'", field in report.columns)
    check("report values are finite", report.notna().all().all())
except Exception as e:
    check(f"full report failed: {e}", False)
    traceback.print_exc()

# --- 12. generate_report(select=["metrics"]) — selective ---
print("\n[12] generate_report(select) 选择性报告测试")
try:
    ev = FactorEvaluator(factor, returns)
    ev.run_all()

    # 仅 metrics / metrics only
    r1 = ev.generate_report(select=["metrics"])
    check("select=metrics: is DataFrame", isinstance(r1, pd.DataFrame))
    check("select=metrics: has IC_mean", "IC_mean" in r1.columns)
    check("select=metrics: has IC_t_stat", "IC_t_stat" in r1.columns)
    check("select=metrics: no long_return", "long_return" not in r1.columns)
    check("select=metrics: no avg_turnover", "avg_turnover" not in r1.columns)

    # 仅 curves / curves only
    r2 = ev.generate_report(select=["curves"])
    check("select=curves: has sharpe", "sharpe" in r2.columns)
    check("select=curves: has n_days", "n_days" in r2.columns)
    check("select=curves: no IC_mean", "IC_mean" not in r2.columns)

    # 仅 turnover / turnover only
    r3 = ev.generate_report(select=["turnover"])
    check("select=turnover: has avg_turnover", "avg_turnover" in r3.columns)
    check("select=turnover: has avg_rank_autocorr", "avg_rank_autocorr" in r3.columns)
    check("select=turnover: no sharpe", "sharpe" not in r3.columns)

    # 仅 neutralize / neutralize only
    r4 = ev.generate_report(select=["neutralize"])
    check("select=neutralize: has neutralized_return", "neutralized_return" in r4.columns)
    check("select=neutralize: no IC_mean", "IC_mean" not in r4.columns)

    # 仅 grouping / grouping only
    r5 = ev.generate_report(select=["grouping"])
    check("select=grouping: has n_groups_used", "n_groups_used" in r5.columns)
    check("select=grouping: no sharpe", "sharpe" not in r5.columns)

    # 多选 / multi-select
    r6 = ev.generate_report(select=["metrics", "curves"])
    check("select=[metrics,curves]: has IC_mean", "IC_mean" in r6.columns)
    check("select=[metrics,curves]: has sharpe", "sharpe" in r6.columns)
    check("select=[metrics,curves]: no avg_turnover", "avg_turnover" not in r6.columns)
except Exception as e:
    check(f"selective report failed: {e}", False)
    traceback.print_exc()

# --- 13. generate_report(select) with uncomputed sections → NaN ---
print("\n[13] generate_report 未计算板块填 NaN 测试")
try:
    ev = FactorEvaluator(factor, returns)
    ev.run_metrics()  # 只运行 metrics / only run metrics
    report = ev.generate_report()
    check("uncomputed: is DataFrame", isinstance(report, pd.DataFrame))
    check("uncomputed: has IC_mean (computed)", "IC_mean" in report.columns)
    check("uncomputed: has n_groups_used but NaN", "n_groups_used" not in report.columns)
except Exception as e:
    check(f"uncomputed report failed: {e}", False)
    traceback.print_exc()

# --- 14. generate_report(select) with invalid section → ValueError ---
print("\n[14] generate_report 无效板块 ValueError 测试")
try:
    ev = FactorEvaluator(factor, returns)
    ev.run_all()
    ev.generate_report(select=["metrics", "invalid_section"])
    check("invalid section raises ValueError", False)
except ValueError:
    check("invalid section raises ValueError", True)
except Exception as e:
    check(f"invalid section wrong exception: {type(e).__name__}", False)

# --- 15. New attributes correctness: ic_stats ---
print("\n[15] ic_stats 属性正确性测试")
try:
    ev = FactorEvaluator(factor, returns)
    ev.run_metrics()
    stats = ev.ic_stats
    check("ic_stats has 7 fields", len(stats) == 7)
    check("ic_stats has IC_mean", "IC_mean" in stats.index)
    check("ic_stats has IC_std", "IC_std" in stats.index)
    check("ic_stats has ICIR", "ICIR" in stats.index)
    check("ic_stats values finite", stats.notna().all())
except Exception as e:
    check(f"ic_stats correctness failed: {e}", False)
    traceback.print_exc()

# --- 16. New attributes correctness: turnover ---
print("\n[16] turnover 属性正确性测试")
try:
    ev = FactorEvaluator(factor, returns)
    ev.run_turnover()
    check("turnover columns are group labels", all(isinstance(c, (int, np.integer)) for c in ev.turnover.columns))
    to_valid = ev.turnover.dropna()
    check("turnover values in [0,1]", (to_valid >= 0).all().all() and (to_valid <= 1).all().all())
    check("rank_autocorr values in [-1,1]", ev.rank_autocorr.dropna().between(-1, 1).all())
except Exception as e:
    check(f"turnover correctness failed: {e}", False)
    traceback.print_exc()

# --- 17. Custom n_groups run_all ---
print("\n[17] 自定义 n_groups run_all 测试")
try:
    ev = FactorEvaluator(factor, returns, n_groups=3, top_k=1, bottom_k=1)
    ev.run_all()
    report = ev.generate_report()
    check("n_groups=3: n_groups_used == 3", report["n_groups_used"].iloc[0] == 3)
    check("n_groups=3: turnover has 3 columns", len(ev.turnover.columns) == 3)
except Exception as e:
    check(f"custom n_groups failed: {e}", False)
    traceback.print_exc()

# --- 18. Edge case: all-NaN factor ---
print("\n[18] 全 NaN 因子边界测试")
try:
    nan_factor = pd.Series(np.nan, index=factor.index, dtype=np.float64)
    ev = FactorEvaluator(nan_factor, returns)
    ev.run_all()
    check("all-NaN: ic all NaN", ev.ic.notna().sum() == 0)
    check("all-NaN: hedge_curve flat 1.0", (ev.hedge_curve == 1.0).all())
    check("all-NaN: report generated", isinstance(ev.generate_report(), pd.DataFrame))
except Exception as e:
    check(f"all-NaN failed: {e}", False)
    traceback.print_exc()

# --- 19. Multi-seed stability ---
print("\n[19] 多种子稳定性测试")
try:
    all_ok = True
    for seed in [7, 42, 99, 200]:
        f, r = make_factor_returns(seed=seed)
        ev = FactorEvaluator(f, r)
        ev.run_all()
        report = ev.generate_report()
        if not isinstance(report, pd.DataFrame) or len(report) != 1:
            all_ok = False
            break
        if not report.notna().all().all():
            all_ok = False
            break
    check("4 seeds all produce valid reports", all_ok)
except Exception as e:
    check(f"multi-seed failed: {e}", False)
    traceback.print_exc()

# --- 20. Public export ---
print("\n[20] 公共导出测试")
try:
    import FactorAnalysis
    check("FactorEvaluator in __all__", "FactorEvaluator" in FactorAnalysis.__all__)
except Exception as e:
    check(f"export failed: {e}", False)

print("\n" + "=" * 60)
print(f"结果: {passed} passed, {failed} failed (total {passed + failed})")
print("=" * 60)
sys.exit(0 if failed == 0 else 1)
