"""
Script: 因子投研全量基准测试 — 内存 + 耗时逐步骤监控
Benchmark script with per-step memory tracking and timing.

用法 / Usage:
    python scripts/bench_factor_research.py --factor AlphaPriceRange
    python scripts/bench_factor_research.py --factor AlphaVolatility --chunk-size 500
"""

from __future__ import annotations

import sys
import os
import argparse
import time
import gc
import tracemalloc
from datetime import datetime

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

for submodule in ["CryptoDataProviders", "CryptoDB_feather"]:
    submodule_path = os.path.join(project_root, submodule)
    if submodule_path not in sys.path:
        sys.path.insert(0, submodule_path)


def get_memory_mb() -> float:
    """获取当前进程 RSS 内存 (MB) / Get current process RSS memory in MB."""
    import psutil
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024


def benchmark_step(name: str, fn, *args, **kwargs):
    """
    执行一个步骤，记录耗时和内存变化 / Run a step, record timing and memory delta.
    """
    gc.collect()
    mem_before = get_memory_mb()

    tracemalloc.start()
    t0 = time.perf_counter()
    result = fn(*args, **kwargs)
    elapsed = time.perf_counter() - t0
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    gc.collect()
    mem_after = get_memory_mb()

    print(f"    {name:<30s}  {elapsed:>8.2f}s  |  "
          f"RSS: {mem_before:>8.1f} → {mem_after:>8.1f} MB  "
          f"(+{mem_after - mem_before:>+7.1f})  |  "
          f"tracemalloc peak: {peak / 1024 / 1024:>7.1f} MB")

    return result, elapsed, mem_before, mem_after, peak / 1024 / 1024


def main():
    parser = argparse.ArgumentParser(description="因子投研基准测试 / Factor research benchmark")
    parser.add_argument("--factor", type=str, required=True, help="因子名称")
    parser.add_argument("--chunk-size", type=int, default=1000, help="分块大小 (default: 1000)")
    parser.add_argument("--interval", type=str, default="1h", help="K线周期 (default: 1h)")
    args = parser.parse_args()

    mem_start = get_memory_mb()

    print("=" * 80)
    print(f"  CryptoAlpha Factor Research BENCHMARK")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  factor={args.factor}  chunk_size={args.chunk_size}  interval={args.interval}")
    print(f"  Initial RSS: {mem_start:.1f} MB")
    print("=" * 80)

    timings = {}

    # ── Step 1: 数据加载 / Data Loading ──
    print(f"\n{'─' * 80}")
    print(f"  [Step 1/5] Data Loading")
    print(f"{'─' * 80}")

    from Cross_Section_Factor.kline_loader import KlineLoader
    loader = KlineLoader(interval=args.interval)
    data, t1, _, _, _ = benchmark_step("loader.compile()", loader.compile)
    n_symbols = data["symbol"].nunique()
    n_timestamps = data["timestamp"].nunique()
    print(f"    → {len(data)} rows, {n_symbols} symbols, {n_timestamps} timestamps")
    timings["1_load"] = t1
    del loader
    gc.collect()

    # ── Step 2: 因子计算 + 提取 slim data / Factor + slim extract ──
    print(f"\n{'─' * 80}")
    print(f"  [Step 2/5] Factor Calculation + Slim Data Extract")
    print(f"{'─' * 80}")

    from FactorLib import list_factors, get
    available = list_factors()
    print(f"    Available factors: {available}")
    if args.factor not in available:
        print(f"    ERROR: Factor '{args.factor}' not found. Aborting.")
        return

    factor_cls = get(args.factor)
    factor_inst = factor_cls()

    from scripts.run_factor_research import _build_factor_multiindex

    def calc_factor():
        raw = factor_inst.calculate(data)
        return _build_factor_multiindex(raw, data)

    factor_values, t2a, _, _, _ = benchmark_step(f"calculate({args.factor})", calc_factor)
    print(f"    → {factor_values.notna().sum()} valid factor values")
    timings["2a_factor"] = t2a

    # 提取收益计算所需的最小列 / Extract minimal columns for returns
    gc.collect()
    mem_before_slim = get_memory_mb()
    t0 = time.perf_counter()
    data_slim = data[["timestamp", "symbol", "close"]].copy()
    del data
    gc.collect()
    t2b = time.perf_counter() - t0
    mem_after_slim = get_memory_mb()
    print(f"    {'slim extract + del data':<30s}  {t2b:>8.2f}s  |  "
          f"RSS: {mem_before_slim:>8.1f} → {mem_after_slim:>8.1f} MB  "
          f"(+{mem_after_slim - mem_before_slim:>+7.1f})")
    timings["2b_slim"] = t2b

    # ── Step 3: 收益率计算 / Returns Calculation ──
    print(f"\n{'─' * 80}")
    print(f"  [Step 3/5] Returns Calculation")
    print(f"{'─' * 80}")

    from FactorAnalysis.returns import calc_returns
    returns, t3, _, _, _ = benchmark_step("calc_returns()", calc_returns, data_slim)
    print(f"    → {returns.notna().sum()} valid values")
    timings["3_returns"] = t3

    del data_slim
    gc.collect()

    # ── Step 4: 对齐 + 质检 / Alignment + Quality ──
    print(f"\n{'─' * 80}")
    print(f"  [Step 4/5] Alignment + Quality")
    print(f"{'─' * 80}")

    from FactorAnalysis.alignment import align_factor_returns
    from FactorAnalysis.data_quality import check_data_quality

    def align_and_check():
        clean = align_factor_returns(factor_values, returns)
        coverage = check_data_quality(clean["factor"], clean["returns"])
        return clean, coverage

    (clean, coverage), t4, _, _, _ = benchmark_step("align + quality", align_and_check)
    print(f"    → {len(clean)} aligned pairs, coverage={coverage:.1%}")
    timings["4_align"] = t4

    del factor_values, returns
    gc.collect()

    # ── Step 5: 评估环节逐步骤 / Evaluation per-step ──
    print(f"\n{'─' * 80}")
    print(f"  [Step 5/5] Factor Evaluation (mode=full, chunk_size={args.chunk_size})")
    print(f"{'─' * 80}")

    from FactorAnalysis.evaluator import FactorEvaluator
    from FactorAnalysis.chunking import split_into_chunks

    ev = FactorEvaluator(
        clean["factor"], clean["returns"],
        n_groups=5, chunk_size=args.chunk_size,
    )

    # 5a. split_into_chunks
    def do_split():
        cf = split_into_chunks(clean["factor"], args.chunk_size)
        cr = split_into_chunks(clean["returns"], args.chunk_size)
        return cf, cr

    (chunk_factor, chunk_returns), t5a, _, _, _ = benchmark_step(
        "split_into_chunks (×2)", do_split)
    print(f"    → {len(chunk_factor)} chunks")
    timings["5a_split"] = t5a

    # 5b. run_metrics
    _, t5b, _, _, _ = benchmark_step(
        "run_metrics", ev.run_metrics,
        chunk_factor=chunk_factor, chunk_returns=chunk_returns)
    timings["5b_metrics"] = t5b

    # 5c. run_grouping
    _, t5c, _, _, _ = benchmark_step(
        "run_grouping", ev.run_grouping,
        chunk_factor=chunk_factor)
    timings["5c_grouping"] = t5c

    # 5d. split group_labels
    chunk_groups, t5d, _, _, _ = benchmark_step(
        "split group_labels",
        split_into_chunks, ev.group_labels, args.chunk_size)
    timings["5d_split_groups"] = t5d

    # 5e. run_curves
    _, t5e, _, _, _ = benchmark_step(
        "run_curves", ev.run_curves,
        chunk_factor=chunk_factor, chunk_returns=chunk_returns,
        chunk_groups=chunk_groups)
    timings["5e_curves"] = t5e

    # 5f. run_turnover
    _, t5f, _, _, _ = benchmark_step(
        "run_turnover", ev.run_turnover,
        chunk_factor=chunk_factor, chunk_groups=chunk_groups)
    timings["5f_turnover"] = t5f

    # 5g. run_neutralize
    _, t5g, _, _, _ = benchmark_step(
        "run_neutralize", ev.run_neutralize,
        chunk_factor=chunk_factor, chunk_returns=chunk_returns,
        chunk_groups=chunk_groups)
    timings["5g_neutralize"] = t5g

    eval_total = sum(timings[k] for k in timings if k.startswith("5"))

    # 报告 / Report
    print(f"\n{'─' * 80}")
    print(f"  Report")
    print(f"{'─' * 80}")
    report = ev.generate_report()
    print(report.to_string(index=False))

    # ── 总结 / Summary ──
    mem_final = get_memory_mb()
    total = sum(timings.values())

    print(f"\n{'=' * 80}")
    print(f"  BENCHMARK SUMMARY")
    print(f"{'=' * 80}")
    print(f"  {'Step':<35s} {'Time':>8s} {'% Total':>8s}")
    print(f"  {'─' * 35} {'─' * 8} {'─' * 8}")
    for k, v in timings.items():
        pct = v / total * 100 if total > 0 else 0
        print(f"  {k:<35s} {v:>7.2f}s {pct:>7.1f}%")
    print(f"  {'─' * 35} {'─' * 8} {'─' * 8}")
    print(f"  {'TOTAL':<35s} {total:>7.2f}s {100.0:>7.1f}%")
    print(f"  {'Eval total (5a-5g)':<35s} {eval_total:>7.2f}s {eval_total / total * 100:>7.1f}%")
    print(f"\n  Memory:  RSS {mem_start:.1f} → {mem_final:.1f} MB  "
          f"(+{mem_final - mem_start:+.1f} MB)")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    main()
