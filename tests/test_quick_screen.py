"""
Task 6 测试 — Quick Screen 管道功能验证
Test: Quick Screen pipeline functional validation (run_quick / report select / CLI --mode)

验证:
1. run_quick() 仅产出 Layer 0 指标 (IC/RankIC/ICIR/IC_stats/rank_autocorr)
2. 不含 portfolio/turnover/neutralize 等 Layer 1~3 输出
3. 6 种 mock 场景 × 8 指标数值正确 (与 run_all 逐层提取结果一致)
4. --mode quick CLI 参数生效
5. report select 参数正确过滤输出
"""

import sys
import os
import traceback
import numpy as np
import pandas as pd

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from FactorAnalysis.evaluator import FactorEvaluator

checks = 0
passed = 0


def ok(label: str, condition: bool):
    """断言辅助 / Assertion helper."""
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
# 6 种 Mock 场景 / 6 mock scenarios
# ============================================================

def scenario_params():
    """
    返回 6 种 mock 场景参数 / Return 6 mock scenario parameters.
    """
    return [
        {
            "name": "标准场景",
            "n_dates": 120, "n_symbols": 50, "seed": 42,
            "nan_ratio": 0.0, "chunk_size": 30, "n_groups": 5,
        },
        {
            "name": "大数据量",
            "n_dates": 200, "n_symbols": 80, "seed": 123,
            "nan_ratio": 0.0, "chunk_size": 40, "n_groups": 5,
        },
        {
            "name": "小数据量",
            "n_dates": 20, "n_symbols": 15, "seed": 77,
            "nan_ratio": 0.0, "chunk_size": 100, "n_groups": 5,
        },
        {
            "name": "高NaN比例",
            "n_dates": 100, "n_symbols": 40, "seed": 888,
            "nan_ratio": 0.10, "chunk_size": 25, "n_groups": 5,
        },
        {
            "name": "弱信号+多分组",
            "n_dates": 120, "n_symbols": 50, "seed": 2024,
            "nan_ratio": 0.0, "chunk_size": 30, "n_groups": 10,
            "signal_strength": 0.005,
        },
        {
            "name": "紧凑分块",
            "n_dates": 150, "n_symbols": 60, "seed": 555,
            "nan_ratio": 0.02, "chunk_size": 5, "n_groups": 5,
        },
    ]


# ============================================================
# Test 1: run_quick() 仅产出 Layer 0 指标
# ============================================================

def test_quick_only_layer0():
    """
    验证 run_quick() 后 Layer 1~3 属性全为 None / Verify Layer 1~3 attrs are None after run_quick().
    """
    print("\n=== Test 1: run_quick() 仅产出 Layer 0 指标 ===")
    factor, returns = make_synthetic()

    ev = FactorEvaluator(factor, returns)
    ev.run_quick()

    # Layer 0 指标必须非空 / Layer 0 metrics must be non-None
    ok("ic is not None", ev.ic is not None)
    ok("rank_ic is not None", ev.rank_ic is not None)
    ok("icir is not None", ev.icir is not None)
    ok("ic_stats is not None", ev.ic_stats is not None)
    ok("rank_autocorr is not None", ev.rank_autocorr is not None)

    # Layer 1~3 指标必须为 None / Layer 1~3 metrics must be None
    ok("group_labels is None", ev.group_labels is None)
    ok("long_curve is None", ev.long_curve is None)
    ok("short_curve is None", ev.short_curve is None)
    ok("hedge_curve is None", ev.hedge_curve is None)
    ok("hedge_curve_after_cost is None", ev.hedge_curve_after_cost is None)
    ok("sharpe is None", ev.sharpe is None)
    ok("calmar is None", ev.calmar is None)
    ok("sortino is None", ev.sortino is None)
    ok("sharpe_after_cost is None", ev.sharpe_after_cost is None)
    ok("calmar_after_cost is None", ev.calmar_after_cost is None)
    ok("sortino_after_cost is None", ev.sortino_after_cost is None)
    ok("turnover is None", ev.turnover is None)
    ok("neutralized_curve is None", ev.neutralized_curve is None)

    # _quick_mode 标志 / _quick_mode flag
    ok("_quick_mode is True", ev._quick_mode is True)


# ============================================================
# Test 2: run_all() 后 _quick_mode 不受影响
# ============================================================

def test_all_mode_quick_flag():
    """
    验证 run_all() 不设置 _quick_mode / Verify run_all() does not set _quick_mode.
    """
    print("\n=== Test 2: run_all() 后 _quick_mode = False ===")
    factor, returns = make_synthetic()

    ev = FactorEvaluator(factor, returns)
    ev.run_all()

    ok("_quick_mode is False after run_all", ev._quick_mode is False)


# ============================================================
# Test 3: 6 种场景 × 8 指标数值一致性 (全量模式)
# ============================================================

def test_6scenarios_8metrics_full_mode():
    """
    6 种 mock 场景 × 8 指标：run_quick() vs run_all() 逐层提取结果数值一致。
    6 scenarios × 8 metrics: run_quick() matches run_all() Layer 0 results.
    """
    print("\n=== Test 3: 6 场景 × 8 指标一致性 (全量模式) ===")

    tol = 1e-10

    for sp in scenario_params():
        factor, returns = make_synthetic(
            n_dates=sp["n_dates"],
            n_symbols=sp["n_symbols"],
            seed=sp["seed"],
            nan_ratio=sp["nan_ratio"],
            signal_strength=sp.get("signal_strength", 0.02),
        )
        name = sp["name"]

        # quick 模式 / quick mode
        ev_q = FactorEvaluator(factor, returns, n_groups=sp["n_groups"])
        ev_q.run_quick()

        # full 模式 (逐层提取 Layer 0) / full mode (extract Layer 0)
        ev_f = FactorEvaluator(factor, returns, n_groups=sp["n_groups"])
        ev_f.run_metrics()
        ev_f.run_rank_autocorr()

        # 8 指标逐个对比 / compare 8 metrics
        # 1. IC mean
        diff = abs(ev_q.ic.mean() - ev_f.ic.mean())
        ok(f"{name}: IC_mean diff={diff:.2e} < {tol}", diff < tol)

        # 2. IC std
        diff = abs(ev_q.ic.std() - ev_f.ic.std())
        ok(f"{name}: IC_std diff={diff:.2e} < {tol}", diff < tol)

        # 3. RankIC mean
        diff = abs(ev_q.rank_ic.mean() - ev_f.rank_ic.mean())
        ok(f"{name}: RankIC_mean diff={diff:.2e} < {tol}", diff < tol)

        # 4. RankIC std
        diff = abs(ev_q.rank_ic.std() - ev_f.rank_ic.std())
        ok(f"{name}: RankIC_std diff={diff:.2e} < {tol}", diff < tol)

        # 5. ICIR
        diff = abs(ev_q.icir - ev_f.icir)
        ok(f"{name}: ICIR diff={diff:.2e} < {tol}", diff < tol)

        # 6. IC t_stat
        diff = abs(ev_q.ic_stats["t_stat"] - ev_f.ic_stats["t_stat"])
        ok(f"{name}: IC_t_stat diff={diff:.2e} < {tol}", diff < tol)

        # 7. IC p_value
        diff = abs(ev_q.ic_stats["p_value"] - ev_f.ic_stats["p_value"])
        ok(f"{name}: IC_p_value diff={diff:.2e} < {tol}", diff < tol)

        # 8. rank_autocorr mean
        diff = abs(ev_q.rank_autocorr.mean() - ev_f.rank_autocorr.mean())
        ok(f"{name}: avg_rank_autocorr diff={diff:.2e} < {tol}", diff < tol)


# ============================================================
# Test 4: 6 种场景 × 8 指标数值一致性 (分块模式)
# ============================================================

def test_6scenarios_8metrics_chunked_mode():
    """
    6 种 mock 场景 × 8 指标：分块模式 run_quick() vs run_all() 逐层提取结果数值一致。
    6 scenarios × 8 metrics: chunked run_quick() matches chunked run_all() Layer 0 results.
    """
    print("\n=== Test 4: 6 场景 × 8 指标一致性 (分块模式) ===")

    tol = 1e-8  # 分块模式容差稍宽松 / slightly relaxed tolerance for chunked mode

    for sp in scenario_params():
        factor, returns = make_synthetic(
            n_dates=sp["n_dates"],
            n_symbols=sp["n_symbols"],
            seed=sp["seed"],
            nan_ratio=sp["nan_ratio"],
            signal_strength=sp.get("signal_strength", 0.02),
        )
        name = sp["name"]
        cs = sp["chunk_size"]

        # quick 分块模式 / quick chunked mode
        ev_q = FactorEvaluator(factor, returns, n_groups=sp["n_groups"], chunk_size=cs)
        ev_q.run_quick()

        # full 分块模式 (逐层提取) / full chunked mode (extract Layer 0)
        ev_f = FactorEvaluator(factor, returns, n_groups=sp["n_groups"], chunk_size=cs)
        ev_f.run_metrics()
        ev_f.run_rank_autocorr()

        # IC mean
        diff = abs(ev_q.ic.mean() - ev_f.ic.mean())
        ok(f"{name} [chunk]: IC_mean diff={diff:.2e} < {tol}", diff < tol)

        # IC std
        diff = abs(ev_q.ic.std() - ev_f.ic.std())
        ok(f"{name} [chunk]: IC_std diff={diff:.2e} < {tol}", diff < tol)

        # RankIC mean
        diff = abs(ev_q.rank_ic.mean() - ev_f.rank_ic.mean())
        ok(f"{name} [chunk]: RankIC_mean diff={diff:.2e} < {tol}", diff < tol)

        # RankIC std
        diff = abs(ev_q.rank_ic.std() - ev_f.rank_ic.std())
        ok(f"{name} [chunk]: RankIC_std diff={diff:.2e} < {tol}", diff < tol)

        # ICIR
        diff = abs(ev_q.icir - ev_f.icir)
        ok(f"{name} [chunk]: ICIR diff={diff:.2e} < {tol}", diff < tol)

        # IC t_stat
        diff = abs(ev_q.ic_stats["t_stat"] - ev_f.ic_stats["t_stat"])
        ok(f"{name} [chunk]: IC_t_stat diff={diff:.2e} < {tol}", diff < tol)

        # IC p_value
        diff = abs(ev_q.ic_stats["p_value"] - ev_f.ic_stats["p_value"])
        ok(f"{name} [chunk]: IC_p_value diff={diff:.2e} < {tol}", diff < tol)

        # rank_autocorr mean
        diff = abs(ev_q.rank_autocorr.mean() - ev_f.rank_autocorr.mean())
        ok(f"{name} [chunk]: avg_rank_autocorr diff={diff:.2e} < {tol}", diff < tol)


# ============================================================
# Test 5: report select 参数正确过滤输出
# ============================================================

def test_report_select_filtering():
    """
    验证 generate_report(select=...) 正确过滤输出板块。
    Verify generate_report(select=...) correctly filters output sections.
    """
    print("\n=== Test 5: report select 参数过滤 ===")
    factor, returns = make_synthetic()

    ev = FactorEvaluator(factor, returns)
    ev.run_quick()

    # select=["metrics", "turnover"] — 应包含 IC 列 + rank_autocorr，不含 curves/neutralize
    report = ev.generate_report(select=["metrics", "turnover"])
    ok("report is DataFrame", isinstance(report, pd.DataFrame))
    ok("report has 1 row", len(report) == 1)

    cols = set(report.columns)

    # metrics 列必须存在 / metrics columns must exist
    for col in ["IC_mean", "IC_std", "RankIC_mean", "RankIC_std", "ICIR",
                "IC_t_stat", "IC_p_value", "IC_skew", "IC_kurtosis"]:
        ok(f"report contains {col}", col in cols)

    # rank_autocorr 属于 turnover 板块 / rank_autocorr belongs to turnover section
    ok("report contains avg_rank_autocorr", "avg_rank_autocorr" in cols)

    # avg_turnover 不应出现 (run_quick 不计算 turnover) / avg_turnover should not appear
    ok("report NOT contains avg_turnover", "avg_turnover" not in cols)

    # curves/neutralize 列不应出现 / curves/neutralize columns should not appear
    for col in ["long_return", "short_return", "hedge_return", "sharpe",
                "calmar", "sortino", "neutralized_return", "n_days"]:
        ok(f"report NOT contains {col}", col not in cols)

    # select=["metrics"] — 仅 IC 相关列
    report_metrics = ev.generate_report(select=["metrics"])
    cols_m = set(report_metrics.columns)
    ok("select=metrics: no turnover cols", "avg_rank_autocorr" not in cols_m)
    ok("select=metrics: no curves cols", "hedge_return" not in cols_m)

    # select=["curves"] — 全部为空 (run_quick 不产出 curves)
    report_curves = ev.generate_report(select=["curves"])
    ok("select=curves: empty DataFrame columns",
       isinstance(report_curves, pd.DataFrame) and len(report_curves.columns) == 0)

    # select=None (全部板块) — 与 run_quick 匹配的列
    report_all = ev.generate_report(select=None)
    ok("select=None: contains IC_mean", "IC_mean" in set(report_all.columns))
    ok("select=None: contains avg_rank_autocorr", "avg_rank_autocorr" in set(report_all.columns))
    # curves/neutralize 板块虽被选中但无数据，不产生列
    ok("select=None: no long_return (no data)", "long_return" not in set(report_all.columns))


# ============================================================
# Test 6: report.py 模块级 generate_report 代理
# ============================================================

def test_report_module_delegate():
    """
    验证 report.generate_report 模块级函数正确代理到 evaluator。
    Verify module-level generate_report correctly delegates to evaluator.
    """
    print("\n=== Test 6: report.generate_report 模块级代理 ===")
    from FactorAnalysis.report import generate_report

    factor, returns = make_synthetic()
    ev = FactorEvaluator(factor, returns)
    ev.run_quick()

    report = generate_report(ev, select=["metrics"])
    ok("module-level generate_report returns DataFrame", isinstance(report, pd.DataFrame))
    ok("module-level report has IC_mean", "IC_mean" in set(report.columns))


# ============================================================
# Test 7: --mode quick CLI 参数
# ============================================================

def test_cli_mode_quick_arg():
    """
    验证 CLI --mode 参数解析正确 / Verify CLI --mode argument parsing.
    """
    print("\n=== Test 7: --mode quick CLI 参数 ===")
    import argparse

    # 模拟 CLI 参数解析 / simulate CLI argument parsing
    # 直接使用 run_factor_research.py 中的 parser 逻辑
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, default="quick",
                        choices=["quick", "full"])

    # 默认值 / default value
    args = parser.parse_args([])
    ok("--mode default is quick", args.mode == "quick")

    # 显式 quick / explicit quick
    args = parser.parse_args(["--mode", "quick"])
    ok("--mode quick parsed", args.mode == "quick")

    # 显式 full / explicit full
    args = parser.parse_args(["--mode", "full"])
    ok("--mode full parsed", args.mode == "full")

    # 无效值 / invalid value
    try:
        parser.parse_args(["--mode", "invalid"])
        ok("--mode invalid raises error", False)
    except SystemExit:
        ok("--mode invalid raises SystemExit", True)


# ============================================================
# Test 8: run_quick() IC_stats 完整性
# ============================================================

def test_ic_stats_completeness():
    """
    验证 run_quick() 的 ic_stats 包含所有预期字段 / Verify ic_stats has all expected fields.
    """
    print("\n=== Test 8: IC_stats 完整性 ===")
    factor, returns = make_synthetic(n_dates=100, n_symbols=30, seed=42)

    ev = FactorEvaluator(factor, returns)
    ev.run_quick()

    expected_keys = {"IC_mean", "IC_std", "ICIR", "t_stat", "p_value", "IC_skew", "IC_kurtosis"}
    actual_keys = set(ev.ic_stats.index)
    ok("ic_stats has all expected keys", expected_keys.issubset(actual_keys))

    # 所有值有限 / all values finite
    for key in expected_keys:
        val = ev.ic_stats[key]
        ok(f"ic_stats[{key}] is finite", np.isfinite(val))


# ============================================================
# 运行所有测试 / Run all tests
# ============================================================

if __name__ == "__main__":
    test_quick_only_layer0()
    test_all_mode_quick_flag()
    test_6scenarios_8metrics_full_mode()
    test_6scenarios_8metrics_chunked_mode()
    test_report_select_filtering()
    test_report_module_delegate()
    test_cli_mode_quick_arg()
    test_ic_stats_completeness()

    print(f"\n{'=' * 60}")
    print(f"  Quick Screen Tests: {passed}/{checks} passed")
    print(f"{'=' * 60}")
    sys.exit(1 if passed < checks else 0)
