"""
验证测试 — Task 11: 分块内存占用对比验证
Chunked memory usage comparison verification.

验证 chunk_size=100 时峰值内存 < 全量模式 40%，
以及内存监控日志输出格式正确。
Verify that peak memory with chunk_size=100 is < 40% of full mode,
and memory monitoring log output format is correct.
"""

import logging
import sys
import traceback
import tracemalloc

import numpy as np
import pandas as pd

sys.path.insert(0, ".")

from FactorAnalysis.evaluator import FactorEvaluator

checks = 0


def ok(label: str, condition: bool):
    global checks
    checks += 1
    status = "PASS" if condition else "FAIL"
    print(f"  [{status}] {label}")
    if not condition:
        traceback.print_stack()


def make_large_synthetic(n_dates=500, n_symbols=200, seed=42):
    """
    生成大规模合成数据用于内存对比 / Generate large synthetic data for memory comparison.

    使用较大数据量确保分块与全量模式的内存差异可测量。
    Use sufficiently large data so memory difference is measurable.
    """
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2024-01-01", periods=n_dates, freq="B")
    symbols = [f"S{i:03d}" for i in range(n_symbols)]
    idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])

    true_signal = rng.standard_normal((n_dates, n_symbols))
    noise = rng.standard_normal((n_dates, n_symbols)) * 0.5
    factor_values = true_signal + noise
    returns_values = true_signal * 0.02 + rng.standard_normal((n_dates, n_symbols)) * 0.03

    factor = pd.Series(factor_values.ravel(), index=idx, dtype=np.float64)
    returns = pd.Series(returns_values.ravel(), index=idx, dtype=np.float64)
    return factor, returns


def measure_peak_memory(fn):
    """
    测量函数执行期间的 tracemalloc 峰值内存 (MB) / Measure tracemalloc peak memory during fn execution (MB).

    启动 tracemalloc，执行函数，返回峰值分配内存（MB）。
    Starts tracemalloc, runs fn, returns peak allocated memory in MB.
    """
    tracemalloc.stop()
    tracemalloc.start()
    fn()
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return peak / 1024 / 1024


def test_chunked_memory_lower_than_full():
    """
    场景 1: 分块模式峰值内存 < 全量模式 40%
    Scenario 1: Chunked mode peak memory < 40% of full mode.

    使用 500 日期 × 200 交易对的大规模数据，对比全量与分块 (chunk_size=100) 的峰值内存。
    Use 500 dates × 200 symbols large data, compare full vs chunked (chunk_size=100) peak memory.
    """
    print("\n=== Scenario 1: chunked peak memory < 40% of full mode ===")
    factor, returns = make_large_synthetic(n_dates=500, n_symbols=200, seed=42)

    def run_full():
        ev = FactorEvaluator(factor, returns, chunk_size=None)
        ev.run_all()

    def run_chunked():
        ev = FactorEvaluator(factor, returns, chunk_size=100)
        ev.run_all()

    full_peak = measure_peak_memory(run_full)
    chunked_peak = measure_peak_memory(run_chunked)

    print(f"  Full mode peak: {full_peak:.2f} MB")
    print(f"  Chunked mode peak: {chunked_peak:.2f} MB")
    print(f"  Ratio: {chunked_peak / full_peak:.2%}")

    ok("full mode peak > 0", full_peak > 0)
    ok("chunked mode peak > 0", chunked_peak > 0)
    ok(
        f"chunked peak ({chunked_peak:.1f} MB) < 40% of full ({full_peak:.1f} MB)",
        chunked_peak < full_peak * 0.4,
    )


def test_chunked_memory_various_chunk_sizes():
    """
    场景 2: 多种 chunk_size 的内存占用递减趋势
    Scenario 2: Memory usage decreasing trend across various chunk sizes.

    验证 chunk_size 越小，峰值内存越低（或至少不高于全量模式）。
    Verify that smaller chunk_size leads to lower (or at most equal) peak memory.
    """
    print("\n=== Scenario 2: memory across various chunk sizes ===")
    factor, returns = make_large_synthetic(n_dates=500, n_symbols=200, seed=42)

    chunk_sizes = [None, 250, 100, 50]
    peaks = {}

    for cs in chunk_sizes:
        label = f"chunk_size={cs}" if cs else "full"
        peak = measure_peak_memory(
            lambda cs=cs: FactorEvaluator(factor, returns, chunk_size=cs).run_all()
        )
        peaks[cs] = peak
        print(f"  {label}: {peak:.2f} MB")

    full_peak = peaks[None]
    ok("full mode peak > 0", full_peak > 0)

    for cs in [250, 100, 50]:
        ok(f"chunk_size={cs} peak <= full mode peak", peaks[cs] <= full_peak * 1.05)


def test_memory_tracker_attributes():
    """
    场景 3: ChunkMemoryTracker 属性记录正确
    Scenario 3: ChunkMemoryTracker attributes recorded correctly.

    验证 ChunkMemoryTracker 上下文管理器正确记录 peak_mb 和 rss_mb。
    Verify ChunkMemoryTracker context manager correctly records peak_mb and rss_mb.
    """
    print("\n=== Scenario 3: ChunkMemoryTracker attributes ===")
    from FactorAnalysis.chunking import ChunkMemoryTracker

    tracker = ChunkMemoryTracker(0, 3, description="test")
    ok("peak_mb is None before enter", tracker.peak_mb is None)
    ok("rss_mb is None before enter", tracker.rss_mb is None)

    with tracker:
        # 分配一些内存以产生可追踪的峰值 / allocate some memory for measurable peak
        _ = [bytearray(1024 * 1024) for _ in range(5)]

    ok("peak_mb is not None after exit", tracker.peak_mb is not None)
    ok("peak_mb > 0", tracker.peak_mb > 0)

    # chunk_idx 和 total_chunks 保持不变 / chunk_idx and total_chunks unchanged
    ok("chunk_idx preserved", tracker.chunk_idx == 0)
    ok("total_chunks preserved", tracker.total_chunks == 3)


def test_memory_tracker_log_output():
    """
    场景 4: 内存监控日志输出格式正确
    Scenario 4: Memory monitoring log output format correct.

    捕获 logger 输出，验证日志包含预期格式字段。
    Capture logger output, verify log contains expected format fields.
    """
    print("\n=== Scenario 4: memory log output format ===")
    import io
    from FactorAnalysis.chunking import ChunkMemoryTracker
    from FactorAnalysis import chunking

    logger = chunking.logger
    logger.setLevel(logging.INFO)

    # 捕获日志输出 / capture log output
    log_buffer = io.StringIO()
    handler = logging.StreamHandler(log_buffer)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    try:
        with ChunkMemoryTracker(2, 5, description="run_metrics"):
            _ = [bytearray(1024 * 128) for _ in range(3)]

        log_text = log_buffer.getvalue()
        print(f"  Captured log: {log_text.strip()}")

        ok("log contains chunk index", "[chunk 3/5]" in log_text)
        ok("log contains description", "run_metrics" in log_text)
        ok("log contains peak_alloc", "peak_alloc=" in log_text)
        ok("log contains MB unit", "MB" in log_text)
    finally:
        logger.removeHandler(handler)


def test_memory_tracker_multiple_chunks():
    """
    场景 5: 多块内存追踪一致性
    Scenario 5: Multi-chunk memory tracking consistency.

    连续使用多个 ChunkMemoryTracker，验证每次独立追踪。
    Use multiple ChunkMemoryTracker instances consecutively, verify independent tracking.
    """
    print("\n=== Scenario 5: multi-chunk tracking consistency ===")
    from FactorAnalysis.chunking import ChunkMemoryTracker

    peaks = []
    for i in range(4):
        tracker = ChunkMemoryTracker(i, 4, description=f"chunk_{i}")
        with tracker:
            # 每块分配不同大小的内存 / allocate different sizes per chunk
            _ = [bytearray(1024 * 1024 * (i + 1)) for _ in range(2)]
        peaks.append(tracker.peak_mb)
        ok(f"chunk {i} peak_mb > 0", tracker.peak_mb > 0)

    # 每个块应独立追踪，不一定单调递增（tracemalloc 是全局的）
    # each chunk tracks independently; may not be monotonically increasing
    ok("all 4 chunks recorded peaks", all(p > 0 for p in peaks))
    ok("at least one peak recorded for each chunk", len(peaks) == 4)


def test_chunked_memory_log_during_evaluation():
    """
    场景 6: 分块评估过程中内存日志输出
    Scenario 6: Memory log output during chunked evaluation.

    验证 run_all() 分块模式下每个子方法都产生内存日志。
    Verify that each sub-method produces memory log in chunked run_all().
    """
    print("\n=== Scenario 6: memory log during chunked evaluation ===")
    import io
    from FactorAnalysis import chunking

    logger = chunking.logger
    logger.setLevel(logging.INFO)

    log_buffer = io.StringIO()
    handler = logging.StreamHandler(log_buffer)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    try:
        factor, returns = make_large_synthetic(n_dates=120, n_symbols=50, seed=42)
        ev = FactorEvaluator(factor, returns, chunk_size=30)
        ev.run_all()

        log_text = log_buffer.getvalue()
        print(f"  Total log lines: {len(log_text.strip().splitlines())}")
        # 5 个子方法，每个分 4 块 = 20 条日志 / 5 sub-methods × 4 chunks = 20 log lines
        log_lines = [l for l in log_text.strip().splitlines() if l.strip()]
        print(f"  Non-empty log lines: {len(log_lines)}")

        # 每个子方法至少有 1 条日志 / each sub-method has at least 1 log line
        ok("log contains run_metrics", any("run_metrics" in l for l in log_lines))
        ok("log contains run_grouping", any("run_grouping" in l for l in log_lines))
        ok("log contains run_curves", any("run_curves" in l for l in log_lines))
        ok("log contains run_turnover", any("run_turnover" in l for l in log_lines))
        ok("log contains run_neutralize", any("run_neutralize" in l for l in log_lines))
    finally:
        logger.removeHandler(handler)


def test_chunk_size_100_specific_memory_ratio():
    """
    场景 7: chunk_size=100 在不同数据规模下的内存比
    Scenario 7: Memory ratio with chunk_size=100 across different data scales.

    在 500/800 日期规模下验证 chunk_size=100 的内存比均 < 40%。
    Verify memory ratio < 40% with chunk_size=100 at 500/800 date scales.
    """
    print("\n=== Scenario 7: chunk_size=100 across data scales ===")

    for n_dates in [500, 800]:
        factor, returns = make_large_synthetic(n_dates=n_dates, n_symbols=200, seed=123)

        def run_full():
            FactorEvaluator(factor, returns, chunk_size=None).run_all()

        def run_chunked():
            FactorEvaluator(factor, returns, chunk_size=100).run_all()

        full_peak = measure_peak_memory(run_full)
        chunked_peak = measure_peak_memory(run_chunked)
        ratio = chunked_peak / full_peak

        print(f"  n_dates={n_dates}: full={full_peak:.1f} MB, chunked={chunked_peak:.1f} MB, ratio={ratio:.2%}")
        ok(
            f"n_dates={n_dates}: chunked ratio ({ratio:.2%}) < 40%",
            ratio < 0.4,
        )


# ============================================================
# 主入口 / Main entry
# ============================================================

if __name__ == "__main__":
    print("=" * 60)
    print("Task 11: 分块内存占用对比验证 / Chunked Memory Comparison")
    print("=" * 60)

    test_chunked_memory_lower_than_full()
    test_chunked_memory_various_chunk_sizes()
    test_memory_tracker_attributes()
    test_memory_tracker_log_output()
    test_memory_tracker_multiple_chunks()
    test_chunked_memory_log_during_evaluation()
    test_chunk_size_100_specific_memory_ratio()

    print(f"\n{'=' * 60}")
    print(f"Total: {checks} checks")
    print(f"{'=' * 60}")
