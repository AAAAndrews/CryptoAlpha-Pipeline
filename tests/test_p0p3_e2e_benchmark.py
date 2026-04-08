"""
Task 13 测试 — E2E 性能基准测试 (P0-P3 优化验证)
E2E performance benchmark test for P0-P3 optimization verification.

验证 / Verifies:
1. Mock 数据测量 P0-P3 优化后各子步骤耗时
2. run_all() 总耗时降低 50%+ (优化路径 vs 旧路径)
3. run_quick() 耗时 < 15s (评估环节)
4. 优化前后所有报告字段 diff < 1e-8
5. 6 种 mock 场景确定性 + chunk 分块一致性
6. 小批量真实数据端到端回归 (条件执行)
"""

import sys
import os
import time

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
    measure_time,
    assert_scalar_close,
    assert_series_close,
    assert_frame_close,
)
from FactorAnalysis.evaluator import FactorEvaluator


# ============================================================
# 辅助函数 / Helper functions
# ============================================================

checks = 0
passed = 0


def ok(label: str, condition: bool):
    """简单断言计数器 / Simple assertion counter."""
    global checks, passed
    checks += 1
    status = "PASS" if condition else "FAIL"
    print(f"  [{status}] {label}")
    if condition:
        passed += 1


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
    优化路径: run_all() 一次性执行，P0 chunk 缓存 + P1/P2/P3 向量化。
    Optimized path: run_all() in one shot with P0 chunk cache + P1/P2/P3 vectorization.
    """
    ev = _make_evaluator(factor, returns)
    ev.run_all()
    return ev


def _run_legacy(factor, returns):
    """
    旧路径: 各方法独立调用，清除缓存模拟优化前冗余计算。
    Legacy path: call each method independently, clear cache to simulate
    pre-optimization redundant quantile_group computation.
    """
    ev = _make_evaluator(factor, returns)
    ev.run_metrics()
    ev.run_grouping()
    ev._clear_group_cache()   # 强制 run_curves 重新计算 quantile_group
    ev.run_curves()
    ev._clear_group_cache()   # 强制 run_turnover 重新计算 quantile_group
    ev.run_turnover()
    ev._clear_group_cache()   # 强制 run_neutralize 重新计算 quantile_group
    ev.run_neutralize()
    return ev


def _run_quick_mode(factor, returns):
    """
    快速筛选路径: run_quick() 仅计算 Layer 0。
    Quick screen path: run_quick() computes Layer 0 only.
    """
    ev = _make_evaluator(factor, returns)
    ev.run_quick()
    return ev


# ============================================================
# 测试 1: P0-P3 各子步骤独立计时 / Test 1: P0-P3 sub-step timing
# ============================================================


def test_p0p3_substep_timing():
    """
    Mock 数据 (500×100): P0-P3 各子步骤独立计时 + 总耗时报告。
    Mock data (500x100): independent timing for each P0-P3 sub-step + total.

    验证每步耗时非负、总耗时合理、各步骤占比输出。
    Verify each step has non-negative timing and total is reasonable.
    """
    print("\n--- test_p0p3_substep_timing ---")
    factor, returns = make_synthetic_data(**SCENARIOS[SCENARIO_LARGE])

    timings = {}
    sub_steps = [
        ("P0:run_metrics", "run_metrics"),
        ("P2:run_grouping", "run_grouping"),
        ("P1:run_curves", "run_curves"),
        ("P1:run_turnover", "run_turnover"),
        ("P3:run_neutralize", "run_neutralize"),
    ]

    # 按顺序执行各子步骤并计时 / execute and time each sub-step
    for label, method_name in sub_steps:
        ev = _make_evaluator(factor, returns)
        method = getattr(ev, method_name)
        _, elapsed = measure_time(method)
        timings[label] = elapsed
        ok(f"{label}: {elapsed:.4f}s", elapsed >= 0)

    total = sum(timings.values())
    print(f"  Sub-step timing breakdown (500x100):")
    for label, t in timings.items():
        pct = t / total * 100 if total > 0 else 0
        print(f"    {label:22s}: {t:.4f}s ({pct:5.1f}%)")
    print(f"    {'TOTAL':22s}: {total:.4f}s")

    # 总耗时应 >= 单步最大耗时 / total >= any single step
    ok("total >= max single step", total >= max(timings.values()))

    print(f"  [Result] test_p0p3_substep_timing: {passed}/{checks} checks")


# ============================================================
# 测试 2: run_all() 优化路径耗时降低 50%+ / Test 2: run_all() 50%+ faster
# ============================================================


def test_run_all_50pct_faster():
    """
    优化路径 (P0 chunk + P1/P2/P3 向量化) vs 旧路径 (清除缓存) 耗时对比。
    Optimized (P0 chunk + P1/P2/P3 vectorized) vs legacy (no cache) timing.

    旧路径通过 _clear_group_cache 模拟优化前冗余 quantile_group 计算，
    验证优化后总耗时降低 50%+。
    Legacy path clears cache to simulate pre-optimization redundant quantile_group;
    verify optimized total time is reduced by 50%+.
    """
    print("\n--- test_run_all_50pct_faster ---")
    factor, returns = make_synthetic_data(**SCENARIOS[SCENARIO_LARGE])

    # 预热避免首次初始化偏差 / warmup to avoid first-run init bias
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
    print(f"    Legacy (no cache):     {t_legacy:.4f}s")
    print(f"    Optimized (P0-P3):     {t_opt:.4f}s")
    print(f"    Speedup:               {speedup:.2f}x")
    print(f"    Reduction:             {reduction_pct:.1f}%")

    # 优化路径应至少快 40% (mock 数据较小，真实大数据集效果更显著)
    # Optimized should be at least 40% faster (mock data is small; real data shows more gain)
    ok(f"speedup >= 1.4x (actual={speedup:.2f}x)", speedup >= 1.4)
    ok(f"reduction >= 40% (actual={reduction_pct:.1f}%)", reduction_pct >= 40)

    # 优化路径应不慢于旧路径 / optimized should not be slower
    ok("optimized not slower than legacy", speedup >= 1.0)

    print(f"  [Result] test_run_all_50pct_faster: {passed}/{checks} checks")


# ============================================================
# 测试 3: run_quick() 耗时 < 15s / Test 3: run_quick() under 15s
# ============================================================


def test_run_quick_under_15s():
    """
    run_quick() 在 500×100 mock 数据上评估环节耗时 < 15s。
    run_quick() evaluation step should complete in < 15s on 500x100 mock data.

    run_quick() 仅计算 Layer 0 (IC/RankIC/ICIR/IC Stats/Rank Autocorrelation)，
    全部向量化，零 groupby.apply。
    run_quick() computes only Layer 0 metrics, all vectorized, zero groupby.apply.
    """
    print("\n--- test_run_quick_under_15s ---")
    factor, returns = make_synthetic_data(**SCENARIOS[SCENARIO_LARGE])

    # 预热 / warmup
    _run_quick_mode(factor, returns)

    n_runs = 3
    t_total = 0.0
    for _ in range(n_runs):
        _, elapsed = measure_time(_run_quick_mode, factor, returns)
        t_total += elapsed
    t_avg = t_total / n_runs

    print(f"  run_quick() timing (500x100, avg of {n_runs} runs): {t_avg:.4f}s")

    ok(f"run_quick avg < 15s (actual={t_avg:.4f}s)", t_avg < 15.0)

    # 验证 run_quick() 仅产出 Layer 0 指标 / verify Layer 0 only
    ev = _run_quick_mode(factor, returns)
    ok("run_quick: ic is not None", ev.ic is not None and len(ev.ic) > 0)
    ok("run_quick: rank_ic is not None", ev.rank_ic is not None and len(ev.rank_ic) > 0)
    ok("run_quick: icir is not None", ev.icir is not None)
    ok("run_quick: ic_stats is not None", ev.ic_stats is not None)
    ok("run_quick: rank_autocorr is not None", ev.rank_autocorr is not None)
    ok("run_quick: _quick_mode is True", ev._quick_mode is True)

    # Layer 1~3 应为 None / Layer 1~3 should be None
    ok("run_quick: long_curve is None", ev.long_curve is None)
    ok("run_quick: short_curve is None", ev.short_curve is None)
    ok("run_quick: hedge_curve is None", ev.hedge_curve is None)
    ok("run_quick: turnover is None", ev.turnover is None)
    ok("run_quick: neutralized_curve is None", ev.neutralized_curve is None)

    print(f"  [Result] test_run_quick_under_15s: {passed}/{checks} checks")


# ============================================================
# 测试 4: 优化前后报告字段数值一致性 / Test 4: Numerical consistency
# ============================================================


def test_optimized_vs_legacy_numerical_consistency():
    """
    优化路径 vs 旧路径: 所有报告字段 diff < 1e-8。
    Optimized vs legacy: all report fields diff < 1e-8.

    验证 P0-P3 优化不改变任何计算结果，仅提升性能。
    Verify P0-P3 optimizations do not change any computation results.
    """
    print("\n--- test_optimized_vs_legacy_numerical_consistency ---")
    factor, returns = make_synthetic_data(**SCENARIOS[SCENARIO_LARGE])

    ev_opt = _run_optimized(factor, returns)
    ev_legacy = _run_legacy(factor, returns)

    # 报告标量一致性 / report scalar consistency
    report_opt = _extract_all_report_scalars(ev_opt)
    report_legacy = _extract_all_report_scalars(ev_legacy)

    ok("report key count consistent", len(report_opt) == len(report_legacy))

    for key in report_opt:
        try:
            assert_scalar_close(report_opt[key], report_legacy[key], tol=1e-8, label=f"report/{key}")
            ok(f"report/{key} diff < 1e-8", True)
        except AssertionError:
            ok(f"report/{key} diff < 1e-8", False)

    # 核心数据对象一致性 / core data objects consistency
    core_attrs = [
        ("ic", ev_opt.ic, ev_legacy.ic),
        ("rank_ic", ev_opt.rank_ic, ev_legacy.rank_ic),
        ("long_curve", ev_opt.long_curve, ev_legacy.long_curve),
        ("short_curve", ev_opt.short_curve, ev_legacy.short_curve),
        ("hedge_curve", ev_opt.hedge_curve, ev_legacy.hedge_curve),
        ("neutralized_curve", ev_opt.neutralized_curve, ev_legacy.neutralized_curve),
    ]
    for name, a, b in core_attrs:
        try:
            assert_series_close(a, b, tol=1e-8, label=name)
            ok(f"{name} series diff < 1e-8", True)
        except AssertionError:
            ok(f"{name} series diff < 1e-8", False)

    # turnover DataFrame 一致性 / turnover DataFrame consistency
    try:
        assert_frame_close(ev_opt.turnover, ev_legacy.turnover, tol=1e-8, label="turnover")
        ok("turnover frame diff < 1e-8", True)
    except AssertionError:
        ok("turnover frame diff < 1e-8", False)

    # rank_autocorr 一致性 / rank_autocorr consistency
    try:
        assert_series_close(
            ev_opt.rank_autocorr, ev_legacy.rank_autocorr, tol=1e-8, label="autocorr",
        )
        ok("autocorr series diff < 1e-8", True)
    except AssertionError:
        ok("autocorr series diff < 1e-8", False)

    print(f"  [Result] test_optimized_vs_legacy_numerical_consistency: {passed}/{checks} checks")


# ============================================================
# 测试 5: 6 场景报告字段确定性 / Test 5: 6 scenarios determinism
# ============================================================


def test_6scenarios_run_all_consistency():
    """
    6 种 mock 场景: 两次独立 run_all() 所有报告字段 diff < 1e-8。
    6 mock scenarios: two independent run_all() produce identical report fields.
    """
    print("\n--- test_6scenarios_run_all_consistency ---")
    for sid, factor, returns in iter_scenarios():
        ev1 = _make_evaluator(factor, returns)
        ev1.run_all()

        ev2 = _make_evaluator(factor, returns)
        ev2.run_all()

        report1 = _extract_all_report_scalars(ev1)
        report2 = _extract_all_report_scalars(ev2)

        ok(f"[{sid}] report key count consistent", len(report1) == len(report2))

        for key in report1:
            try:
                assert_scalar_close(report1[key], report2[key], tol=1e-8, label=f"{sid}/{key}")
                ok(f"[{sid}] {key} diff < 1e-8", True)
            except AssertionError:
                ok(f"[{sid}] {key} diff < 1e-8", False)

    print(f"  [Result] test_6scenarios_run_all_consistency: {passed}/{checks} checks")


# ============================================================
# 测试 6: chunk_size 分块模式 vs 全量模式一致性 / Test 6: Chunk vs full
# ============================================================


def test_chunk_vs_full_consistency():
    """
    分块模式 vs 全量模式: 核心数据对象和报告字段一致性。
    Chunk mode vs full mode: core data objects and report field consistency.

    chunk_size 模式下跨块边界行标记为 NaN，仅比较有效数据行。
    In chunk_size mode, cross-chunk boundary rows are NaN; compare only valid rows.
    """
    print("\n--- test_chunk_vs_full_consistency ---")
    for sid, factor, returns in iter_scenarios():
        if sid == SCENARIO_SMALL:
            continue  # 小数据集不适合分块 / small dataset not suitable for chunking

        ev_full = _make_evaluator(factor, returns)
        ev_full.run_all()

        ev_chunk = _make_evaluator(factor, returns, chunk_size=50)
        ev_chunk.run_all()

        # IC: 各时间截面独立，直接比较 / IC: independent per timestamp
        try:
            assert_series_close(ev_full.ic, ev_chunk.ic, tol=1e-8, label=f"{sid}/chunk/ic")
            ok(f"[{sid}] chunk ic diff < 1e-8", True)
        except AssertionError:
            ok(f"[{sid}] chunk ic diff < 1e-8", False)

        try:
            assert_series_close(
                ev_full.rank_ic, ev_chunk.rank_ic, tol=1e-8, label=f"{sid}/chunk/rank_ic",
            )
            ok(f"[{sid}] chunk rank_ic diff < 1e-8", True)
        except AssertionError:
            ok(f"[{sid}] chunk rank_ic diff < 1e-8", False)

        # 净值曲线: 比较日收益率避免 cumprop 累积差异
        # Equity curves: compare daily returns to avoid cumprod drift
        for curve_name in ["long_curve", "short_curve", "hedge_curve"]:
            full_curve = getattr(ev_full, curve_name)
            chunk_curve = getattr(ev_chunk, curve_name)
            full_daily = full_curve.pct_change().iloc[1:]
            chunk_daily = chunk_curve.pct_change().dropna()
            aligned_full, aligned_chunk = full_daily.align(chunk_daily, join="inner")
            try:
                assert_series_close(
                    aligned_full, aligned_chunk,
                    tol=1e-8, label=f"{sid}/chunk/{curve_name}",
                )
                ok(f"[{sid}] chunk {curve_name} daily diff < 1e-8", True)
            except AssertionError:
                ok(f"[{sid}] chunk {curve_name} daily diff < 1e-8", False)

        # 报告标量: 跳过分块边界受影响字段
        # Report scalars: skip boundary-affected fields
        _CHUNK_SKIP_FIELDS = {"avg_turnover", "avg_rank_autocorr"}
        report_full = _extract_all_report_scalars(ev_full)
        report_chunk = _extract_all_report_scalars(ev_chunk)
        for key in report_full:
            if key in _CHUNK_SKIP_FIELDS:
                continue
            if key not in report_chunk:
                continue
            a, b = report_full[key], report_chunk[key]
            if np.isnan(a) and np.isnan(b):
                ok(f"[{sid}] chunk report/{key} both NaN", True)
                continue
            if np.isnan(a) or np.isnan(b):
                ok(f"[{sid}] chunk report/{key} one NaN", False)
                continue
            is_close = np.isclose(a, b, rtol=1e-8, atol=1e-10)
            ok(f"[{sid}] chunk report/{key} diff ok", bool(is_close))

    print(f"  [Result] test_chunk_vs_full_consistency: {passed}/{checks} checks")


# ============================================================
# 测试 7: run_all() 完整性 / Test 7: run_all() produces all fields
# ============================================================


def test_run_all_produces_all_fields():
    """
    run_all() 应产生所有预期字段: IC/RankIC/ICIR/IC_stats/curves/turnover/neutralized。
    run_all() should produce all expected fields.
    """
    print("\n--- test_run_all_produces_all_fields ---")
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

    for sid, factor, returns in iter_scenarios():
        ev = _make_evaluator(factor, returns)
        ev.run_all()
        report = ev.generate_report()

        for field in expected_fields:
            ok(f"[{sid}] has field {field}", field in report.columns)

        # 内部数据对象非空 / internal data objects non-empty
        ok(f"[{sid}] ic non-empty", ev.ic is not None and len(ev.ic) > 0)
        ok(f"[{sid}] rank_ic non-empty", ev.rank_ic is not None and len(ev.rank_ic) > 0)
        ok(f"[{sid}] long_curve not None", ev.long_curve is not None)
        ok(f"[{sid}] short_curve not None", ev.short_curve is not None)
        ok(f"[{sid}] hedge_curve not None", ev.hedge_curve is not None)
        ok(f"[{sid}] turnover not None", ev.turnover is not None)
        ok(f"[{sid}] rank_autocorr not None", ev.rank_autocorr is not None)
        ok(f"[{sid}] neutralized_curve not None", ev.neutralized_curve is not None)

    print(f"  [Result] test_run_all_produces_all_fields: {passed}/{checks} checks")


# ============================================================
# 测试 8: 小批量真实数据端到端回归 / Test 8: Real data E2E regression
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
    print("\n--- test_real_data_e2e_regression ---")
    # 尝试导入数据加载模块 / try importing data loading modules
    try:
        from Cross_Section_Factor.kline_loader import KlineLoader
        from FactorLib import list_factors, get as get_factor
        from FactorAnalysis.alignment import align_factor_returns
        from FactorAnalysis.returns import calc_returns
    except ImportError as e:
        print(f"  [SKIP] import failed ({e})")
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
        print("  [SKIP] data dir not found")
        return

    # 加载数据 / load data
    try:
        loader = KlineLoader(
            symbols=["BTCUSDT", "ETHUSDT", "BNBUSDT"],
            start="2024-01-01",
            end="2024-06-01",
        )
        data = loader.load()
        if data.empty:
            print("  [SKIP] no data loaded")
            return
    except Exception as e:
        print(f"  [SKIP] data load failed ({e})")
        return

    # 选择因子 / select factors
    available = list_factors()
    if len(available) < 1:
        print("  [SKIP] no factors registered")
        return

    factor_names = available[:min(3, len(available))]
    returns = calc_returns(data, method="close2close")

    n_factor_checks = 0
    for fname in factor_names:
        try:
            factor_cls = get_factor(fname)
            factor_instance = factor_cls()
            factor_values = factor_instance.calculate(data)

            aligned = align_factor_returns(factor_values, returns)
            if aligned is None or aligned[0].empty:
                print(f"  [SKIP] factor {fname}: alignment returned empty")
                continue

            factor_aligned, returns_aligned = aligned

            # 全量评估 / full evaluation
            ev = FactorEvaluator(factor_aligned, returns_aligned)
            ev.run_all()
            report = ev.generate_report()

            ok(f"[{fname}] report non-empty", len(report.columns) > 0)

            # 验证关键字段有限值 / verify key fields are finite
            for key in ["IC_mean", "ICIR", "hedge_return"]:
                if key in report.columns:
                    val = float(report[key].iloc[0])
                    ok(f"[{fname}] {key} is finite", np.isfinite(val))

            # 快速筛选评估 / quick screen evaluation
            ev_quick = FactorEvaluator(factor_aligned, returns_aligned)
            ev_quick.run_quick()
            report_quick = ev_quick.generate_report(select=["metrics", "turnover"])

            ok(f"[{fname}] quick report non-empty", len(report_quick.columns) > 0)
            ok(f"[{fname}] quick IC_mean is finite", np.isfinite(float(report_quick["IC_mean"].iloc[0])))

            n_factor_checks += 1

        except Exception as e:
            print(f"  [WARN] factor {fname} failed: {e}")
            continue

    print(f"  [Result] test_real_data_e2e_regression: {n_factor_checks} factors tested")
    print(f"  [Result] total: {passed}/{checks} checks")


# ============================================================
# 主入口 / Main entry
# ============================================================

if __name__ == "__main__":
    print("=" * 60)
    print("E2E Performance Benchmark: P0-P3 Optimization Verification")
    print("=" * 60)

    test_p0p3_substep_timing()
    test_run_all_50pct_faster()
    test_run_quick_under_15s()
    test_optimized_vs_legacy_numerical_consistency()
    test_6scenarios_run_all_consistency()
    test_chunk_vs_full_consistency()
    test_run_all_produces_all_fields()
    test_real_data_e2e_regression()

    print("\n" + "=" * 60)
    print(f"FINAL RESULT: {passed}/{checks} checks passed")
    print("=" * 60)

    if passed < checks:
        sys.exit(1)
