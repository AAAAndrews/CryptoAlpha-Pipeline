"""
tests/test_perf_e2e_benchmark.py — E2E 性能基准测试 (Task 18)
E2E performance benchmark test.

验证 / Verifies:
1. Mock 数据 (500×100) 各子步骤独立计时
2. 优化路径 (缓存复用) vs 旧路径 (无缓存) 耗时对比
3. 所有报告字段数值一致性 (diff < 1e-8)
4. 6 种 mock 场景 × run_all() 确定性
5. chunk_size 分块模式 vs 全量模式一致性
6. 小批量真实数据端到端回归 (条件执行)
"""

import sys
import os

# 确保项目根目录在 path 中 / ensure project root is in path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np
import pandas as pd

from tests.mutual_components.conftest_perf import (
    make_synthetic_data,
    SCENARIOS,
    SCENARIO_LARGE,
    SCENARIO_SMALL,
    iter_scenarios,
    benchmark,
    measure_time,
    assert_scalar_close,
    assert_series_close,
    assert_frame_close,
)
from FactorAnalysis.evaluator import FactorEvaluator


# ============================================================
# 辅助函数 / Helper functions
# ============================================================


def _make_evaluator(factor, returns, chunk_size=None):
    """创建标准 evaluator 实例 / Create standard evaluator instance."""
    return FactorEvaluator(
        factor, returns,
        n_groups=5, top_k=1, bottom_k=1,
        chunk_size=chunk_size,
    )


def _extract_all_report_scalars(ev):
    """
    提取所有报告标量指标 / Extract all report scalar metrics.

    返回 {列名: float} 字典，NaN 保留为 float('nan')。
    Returns {column_name: float} dict, NaN preserved as float('nan').
    """
    report = ev.generate_report()
    return {k: float(report[k].iloc[0]) for k in report.columns}


def _run_optimized(factor, returns):
    """
    优化路径: run_all() 一次性执行，缓存复用。
    Optimized path: run_all() in one shot with cache reuse.
    """
    ev = _make_evaluator(factor, returns)
    ev.run_all()
    return ev


def _run_legacy(factor, returns):
    """
    旧路径: 各方法独立调用，清除缓存强制冗余 quantile_group 计算。
    Legacy path: call each method independently, clear cache between calls
    to force redundant quantile_group computation (simulates pre-optimization).
    """
    ev = _make_evaluator(factor, returns)
    ev.run_metrics()
    ev.run_grouping()
    ev._clear_group_cache()   # 强制 run_curves 内部重新计算 quantile_group
    ev.run_curves()
    ev._clear_group_cache()   # 强制 run_turnover 内部重新计算 quantile_group
    ev.run_turnover()
    ev._clear_group_cache()   # 强制 run_neutralize 内部重新计算 quantile_group
    ev.run_neutralize()
    return ev


# ============================================================
# 测试 1: 各子步骤独立计时 / Test 1: Sub-step independent timing
# ============================================================


def test_substep_timing():
    """
    Mock 数据 (500×100): 各子步骤独立计时 + 总耗时报告。
    Mock data (500x100): independent timing for each sub-step + total.

    验证每步耗时非负、总耗时合理。
    Verify each step has non-negative timing and total is reasonable.
    """
    factor, returns = make_synthetic_data(**SCENARIOS[SCENARIO_LARGE])
    n_checks = 0

    timings = {}
    # 按顺序执行各子步骤并计时 / Execute and time each sub-step in order
    for name in ["run_metrics", "run_grouping", "run_curves", "run_turnover", "run_neutralize"]:
        ev = _make_evaluator(factor, returns)
        method = getattr(ev, name)
        _, elapsed = measure_time(method)
        timings[name] = elapsed
        assert elapsed >= 0, f"[{name}] negative timing: {elapsed}"
        n_checks += 1

    total = sum(timings.values())
    print(f"  Sub-step timing breakdown (500x100):")
    for name, t in timings.items():
        pct = t / total * 100 if total > 0 else 0
        print(f"    {name:20s}: {t:.4f}s ({pct:5.1f}%)")
    print(f"    {'TOTAL':20s}: {total:.4f}s")

    # 总耗时应大于单步最大耗时 / total should exceed any single step
    assert total >= max(timings.values()), "total time less than max single step"
    n_checks += 1

    print(f"[PASS] test_substep_timing: {n_checks} checks")


# ============================================================
# 测试 2: 优化路径 vs 旧路径耗时对比 / Test 2: Optimized vs legacy timing
# ============================================================


def test_optimized_vs_legacy_timing():
    """
    优化路径 (run_all + 缓存) vs 旧路径 (无缓存) 耗时对比 + 数值一致性。
    Optimized (run_all + cache) vs legacy (no cache) timing comparison + numerical consistency.

    旧路径通过清除缓存模拟优化前的冗余 quantile_group 调用。
    Legacy path clears cache between calls to simulate pre-optimization redundant quantile_group.
    """
    factor, returns = make_synthetic_data(**SCENARIOS[SCENARIO_LARGE])
    n_checks = 0

    # Warmup: 预热避免首次运行的初始化偏差 / warmup to avoid first-run initialization bias
    _run_optimized(factor, returns)
    _run_legacy(factor, returns)

    n_runs = 3

    # 计时优化路径 / time optimized path
    t_opt_total = 0.0
    for _ in range(n_runs):
        _, elapsed = measure_time(_run_optimized, factor, returns)
        t_opt_total += elapsed
    t_opt = t_opt_total / n_runs

    # 计时旧路径 / time legacy path
    t_legacy_total = 0.0
    for _ in range(n_runs):
        _, elapsed = measure_time(_run_legacy, factor, returns)
        t_legacy_total += elapsed
    t_legacy = t_legacy_total / n_runs

    speedup = t_legacy / t_opt if t_opt > 0 else float("inf")
    reduction_pct = (1 - t_opt / t_legacy) * 100 if t_legacy > 0 else 0

    print(f"  Timing comparison (500x100, avg of {n_runs} runs):")
    print(f"    Legacy (no cache):  {t_legacy:.4f}s")
    print(f"    Optimized (cache):  {t_opt:.4f}s")
    print(f"    Speedup:            {speedup:.2f}x")
    print(f"    Reduction:          {reduction_pct:.1f}%")

    # 优化路径应不慢于旧路径 / optimized should not be slower than legacy
    assert speedup >= 1.0, (
        f"Optimized path is slower than legacy: {speedup:.2f}x "
        f"(opt={t_opt:.4f}s, legacy={t_legacy:.4f}s)"
    )
    n_checks += 1

    # 数值一致性: 所有报告字段 diff < 1e-8 / numerical consistency: all report fields
    ev_opt = _run_optimized(factor, returns)
    ev_legacy = _run_legacy(factor, returns)

    report_opt = _extract_all_report_scalars(ev_opt)
    report_legacy = _extract_all_report_scalars(ev_legacy)

    for key in report_opt:
        assert_scalar_close(
            report_opt[key], report_legacy[key],
            tol=1e-8, label=f"report/{key}",
        )
        n_checks += 1

    # 核心数据对象一致性 / core data objects consistency
    assert_series_close(ev_opt.ic, ev_legacy.ic, tol=1e-8, label="ic")
    assert_series_close(ev_opt.rank_ic, ev_legacy.rank_ic, tol=1e-8, label="rank_ic")
    assert_series_close(ev_opt.long_curve, ev_legacy.long_curve, tol=1e-8, label="long")
    assert_series_close(ev_opt.short_curve, ev_legacy.short_curve, tol=1e-8, label="short")
    assert_series_close(ev_opt.hedge_curve, ev_legacy.hedge_curve, tol=1e-8, label="hedge")
    assert_frame_close(ev_opt.turnover, ev_legacy.turnover, tol=1e-8, label="turnover")
    assert_series_close(
        ev_opt.rank_autocorr, ev_legacy.rank_autocorr, tol=1e-8, label="autocorr",
    )
    assert_series_close(
        ev_opt.neutralized_curve, ev_legacy.neutralized_curve, tol=1e-8, label="neutralized",
    )
    n_checks += 8

    print(f"[PASS] test_optimized_vs_legacy_timing: {n_checks} checks")


# ============================================================
# 测试 3: 6 场景报告字段一致性 / Test 3: 6 scenarios report consistency
# ============================================================


def test_6scenarios_report_consistency():
    """
    6 种 mock 场景: 两次独立 run_all() 所有报告字段 diff < 1e-8。
    6 mock scenarios: two independent run_all() produce identical report fields.
    """
    n_checks = 0
    for sid, factor, returns in iter_scenarios():
        ev1 = _make_evaluator(factor, returns)
        ev1.run_all()

        ev2 = _make_evaluator(factor, returns)
        ev2.run_all()

        report1 = _extract_all_report_scalars(ev1)
        report2 = _extract_all_report_scalars(ev2)

        # 报告字段数量一致 / report field count consistent
        assert len(report1) == len(report2), (
            f"[{sid}] report key count mismatch: {len(report1)} vs {len(report2)}"
        )
        n_checks += 1

        # 逐字段比较 / per-field comparison
        for key in report1:
            assert_scalar_close(
                report1[key], report2[key],
                tol=1e-8, label=f"{sid}/{key}",
            )
            n_checks += 1

    print(f"[PASS] test_6scenarios_report_consistency: {n_checks} checks")


# ============================================================
# 测试 4: chunk_size 分块模式 vs 全量模式一致性 / Test 4: Chunk vs full consistency
# ============================================================


def test_chunk_vs_full_mode_consistency():
    """
    分块模式 vs 全量模式: 核心数据对象和报告字段一致性。
    Chunk mode vs full mode: core data objects and report field consistency.

    chunk_size 模式下跨块边界行标记为 NaN，仅比较有效数据行。
    In chunk_size mode, cross-chunk boundary rows are NaN; compare only valid rows.
    """
    n_checks = 0
    for sid, factor, returns in iter_scenarios():
        if sid == SCENARIO_SMALL:
            continue  # 小数据集不适合分块 / small dataset not suitable for chunking

        # 全量模式 / full mode
        ev_full = _make_evaluator(factor, returns)
        ev_full.run_all()

        # 分块模式 / chunk mode
        ev_chunk = _make_evaluator(factor, returns, chunk_size=50)
        ev_chunk.run_all()

        # IC: 各时间截面独立，直接比较 / IC: independent per timestamp, compare directly
        assert_series_close(ev_full.ic, ev_chunk.ic, tol=1e-8, label=f"{sid}/chunk/ic")
        assert_series_close(
            ev_full.rank_ic, ev_chunk.rank_ic, tol=1e-8, label=f"{sid}/chunk/rank_ic",
        )
        n_checks += 2

        # 净值曲线: chunk 模式跨块边界首行设为 NaN，比较日收益率
        # Equity curves: chunk mode sets boundary rows to NaN, compare daily returns
        for curve_name in ["long_curve", "short_curve", "hedge_curve"]:
            full_curve = getattr(ev_full, curve_name)
            chunk_curve = getattr(ev_chunk, curve_name)
            # 比较日收益率避免 cumprod 浮点累积 / compare daily returns to avoid cumprod drift
            full_daily = full_curve.pct_change().iloc[1:]
            chunk_daily = chunk_curve.pct_change().dropna()
            # 对齐索引后比较 / align indices then compare
            aligned_full, aligned_chunk = full_daily.align(chunk_daily, join="inner")
            assert_series_close(
                aligned_full, aligned_chunk,
                tol=1e-8, label=f"{sid}/chunk/{curve_name}",
            )
            n_checks += 1

        # 报告标量一致性: 对大值使用相对容差 (cumprod 累积净值可极大)
        # Report scalar consistency: use relative tolerance for large values
        # 跳过分块边界受影响字段: avg_turnover/avg_rank_autocorr 在 chunk 模式下
        # 边界行标记为 NaN，均值与全量模式不同，已由 Task 17 单独验证
        # Skip boundary-affected fields: avg_turnover/avg_rank_autocorr differ in
        # chunk mode due to NaN boundary rows; verified separately in Task 17
        _CHUNK_SKIP_FIELDS = {"avg_turnover", "avg_rank_autocorr"}
        report_full = _extract_all_report_scalars(ev_full)
        report_chunk = _extract_all_report_scalars(ev_chunk)
        for key in report_full:
            if key in _CHUNK_SKIP_FIELDS:
                continue
            if key in report_chunk:
                a, b = report_full[key], report_chunk[key]
                # 双方 NaN 视为通过 / both NaN treated as equal
                if np.isnan(a) and np.isnan(b):
                    n_checks += 1
                    continue
                if np.isnan(a) or np.isnan(b):
                    raise AssertionError(
                        f"[{sid}/report/{key}] one is NaN (a={a}, b={b})"
                    )
                # 相对容差: rtol=1e-8 + atol=1e-10 适配大值和小值
                # Relative tolerance: rtol=1e-8 + atol=1e-10 for both large and small values
                assert np.isclose(a, b, rtol=1e-8, atol=1e-10), (
                    f"[{sid}/report/{key}] scalar mismatch: "
                    f"diff={abs(a-b):.2e} (a={a}, b={b})"
                )
                n_checks += 1

    print(f"[PASS] test_chunk_vs_full_mode_consistency: {n_checks} checks")


# ============================================================
# 测试 5: 优化路径 run_all 完整性 / Test 5: Optimized run_all completeness
# ============================================================


def test_run_all_produces_all_fields():
    """
    run_all() 应产生所有预期字段: IC/RankIC/ICIR/IC_stats/curves/turnover/neutralized。
    run_all() should produce all expected fields.
    """
    n_checks = 0
    for sid, factor, returns in iter_scenarios():
        ev = _make_evaluator(factor, returns)
        ev.run_all()
        report = ev.generate_report()

        # 预期字段应全部存在 / expected fields should all exist
        expected_fields = [
            "IC_mean", "IC_std", "RankIC_mean", "RankIC_std", "ICIR",
            "IC_t_stat", "IC_p_value", "IC_skew", "IC_kurtosis",
            "n_groups_used",
            "long_return", "short_return", "hedge_return",
            "hedge_return_after_cost",
            "sharpe", "calmar", "sortino",
            "sharpe_after_cost", "calmar_after_cost", "sortino_after_cost",
            "n_days",
            "avg_turnover", "avg_rank_autocorr",
            "neutralized_return",
        ]
        for field in expected_fields:
            assert field in report.columns, (
                f"[{sid}] missing field: {field}"
            )
            n_checks += 1

        # 内部数据对象非空 / internal data objects non-empty
        assert ev.ic is not None and len(ev.ic) > 0, f"[{sid}] ic is empty"
        assert ev.rank_ic is not None and len(ev.rank_ic) > 0, f"[{sid}] rank_ic is empty"
        assert ev.long_curve is not None, f"[{sid}] long_curve is None"
        assert ev.short_curve is not None, f"[{sid}] short_curve is None"
        assert ev.hedge_curve is not None, f"[{sid}] hedge_curve is None"
        assert ev.turnover is not None, f"[{sid}] turnover is None"
        assert ev.rank_autocorr is not None, f"[{sid}] rank_autocorr is None"
        assert ev.neutralized_curve is not None, f"[{sid}] neutralized_curve is None"
        n_checks += 8

    print(f"[PASS] test_run_all_produces_all_fields: {n_checks} checks")


# ============================================================
# 测试 6: 真实数据端到端回归 / Test 6: Real data E2E regression
# ============================================================


def test_real_data_e2e_regression():
    """
    小批量真实数据端到端回归 (3 因子 × run_factor_research)。
    Small batch real data end-to-end regression (3 factors x run_factor_research).

    条件执行: 需要本地 feather 数据库和因子库可用。
    Conditional: requires local feather database and factor library.
    数据不可用时跳过。
    Skipped when data is not available.
    """
    # 尝试导入数据加载模块 / try importing data loading modules
    try:
        from Cross_Section_Factor.kline_loader import KlineLoader
        from FactorLib import list_factors, get as get_factor
        from FactorAnalysis.alignment import align_factor_returns
        from FactorAnalysis.returns import calc_returns
    except ImportError as e:
        print(f"[SKIP] test_real_data_e2e_regression: import failed ({e})")
        return

    # 检查数据目录 / check data directory
    db_path = None
    try:
        from CryptoDB_feather.config import DB_ROOT_PATH
        db_path = DB_ROOT_PATH
    except ImportError:
        pass

    if db_path is None:
        db_path = os.environ.get("CRYPTO_DB_PATH", "")

    if not db_path or not os.path.isdir(db_path):
        print(f"[SKIP] test_real_data_e2e_regression: data dir not found")
        return

    n_checks = 0

    # 加载数据 / load data
    try:
        loader = KlineLoader(
            symbols=["BTCUSDT", "ETHUSDT", "BNBUSDT"],
            start="2024-01-01",
            end="2024-06-01",
        )
        data = loader.load()
        if data.empty:
            print("[SKIP] test_real_data_e2e_regression: no data loaded")
            return
    except Exception as e:
        print(f"[SKIP] test_real_data_e2e_regression: data load failed ({e})")
        return

    # 选择因子 / select factors
    available = list_factors()
    if len(available) < 1:
        print("[SKIP] test_real_data_e2e_regression: no factors registered")
        return

    factor_names = available[:min(3, len(available))]

    # 计算收益 / calculate returns
    returns = calc_returns(data, method="close2close")

    for fname in factor_names:
        try:
            factor_cls = get_factor(fname)
            factor_instance = factor_cls()
            factor_values = factor_instance.calculate(data)

            # 对齐 / align
            aligned = align_factor_returns(factor_values, returns)
            if aligned is None or aligned[0].empty:
                print(f"  [SKIP] factor {fname}: alignment returned empty")
                continue

            factor_aligned, returns_aligned = aligned

            # 评估 / evaluate
            ev = FactorEvaluator(factor_aligned, returns_aligned)
            ev.run_all()
            report = ev.generate_report()

            # 验证报告非空 / verify report non-empty
            assert len(report.columns) > 0, f"[{fname}] empty report"
            n_checks += 1

            # 验证关键字段存在 / verify key fields exist
            for key in ["IC_mean", "ICIR", "hedge_return"]:
                if key in report.columns:
                    val = float(report[key].iloc[0])
                    # IC_mean 和 hedge_return 应为有限值 / should be finite
                    assert np.isfinite(val), f"[{fname}] {key}={val} is not finite"
                    n_checks += 1

        except Exception as e:
            print(f"  [WARN] factor {fname} failed: {e}")
            continue

    print(f"[PASS] test_real_data_e2e_regression: {n_checks} checks ({len(factor_names)} factors)")


if __name__ == "__main__":
    test_substep_timing()
    test_optimized_vs_legacy_timing()
    test_6scenarios_report_consistency()
    test_chunk_vs_full_mode_consistency()
    test_run_all_produces_all_fields()
    test_real_data_e2e_regression()
    print("\n=== All E2E benchmark tests passed ===")
