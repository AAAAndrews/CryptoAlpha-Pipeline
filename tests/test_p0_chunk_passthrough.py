"""
Task 1 测试 — P0 chunk 列表参数向后兼容 + 传入一致性
Test: P0 chunk list parameter backward compatibility + passthrough consistency

验证:
1. chunk_list=None (默认) 时行为与改造前完全一致 (向后兼容)
2. chunk_list 传入预计算分块 vs 内部 split 结果完全一致
3. chunk_size=None (全量模式) 下 chunk 参数被忽略
4. 各 run_* 方法签名变更不影响既有调用方式
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
from FactorAnalysis.chunking import split_into_chunks

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


def make_synthetic(n_dates=100, n_symbols=30, seed=42):
    """生成合成因子和收益率 / Generate synthetic factor and returns."""
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2025-01-01", periods=n_dates, freq="B")
    symbols = [f"S{i:03d}" for i in range(n_symbols)]
    idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])
    true_signal = rng.standard_normal((n_dates, n_symbols))
    noise = rng.standard_normal((n_dates, n_symbols)) * 0.5
    factor_values = true_signal + noise
    returns_values = true_signal * 0.02 + rng.standard_normal((n_dates, n_symbols)) * 0.03
    factor = pd.Series(factor_values.ravel(), index=idx, dtype=np.float64)
    returns = pd.Series(returns_values.ravel(), index=idx, dtype=np.float64)
    return factor, returns


def test_backward_compatible_no_chunk_args():
    """不传 chunk 参数时行为完全不变 / No chunk args → behavior unchanged."""
    print("\n=== 1. Backward Compatibility (no chunk args) ===")
    factor, returns = make_synthetic()

    # 全量模式 (chunk_size=None) — 不传任何 chunk 参数
    ev = FactorEvaluator(factor, returns)
    ev.run_all()
    ok("全量 run_all() 正常完成", ev.ic is not None)
    ok("全量 group_labels 已计算", ev.group_labels is not None)
    ok("全量 long_curve 已计算", ev.long_curve is not None)
    ok("全量 turnover 已计算", ev.turnover is not None)
    ok("全量 neutralized_curve 已计算", ev.neutralized_curve is not None)

    # 分块模式 — 不传任何 chunk 参数（使用内部 split）
    ev_chunk = FactorEvaluator(factor, returns, chunk_size=30)
    ev_chunk.run_all()
    ok("分块 run_all() 正常完成", ev_chunk.ic is not None)
    ok("分块 group_labels 已计算", ev_chunk.group_labels is not None)
    ok("分块 long_curve 已计算", ev_chunk.long_curve is not None)
    ok("分块 turnover 已计算", ev_chunk.turnover is not None)
    ok("分块 neutralized_curve 已计算", ev_chunk.neutralized_curve is not None)


def test_chunk_passthrough_metrics():
    """run_metrics 传入预计算 chunk 与内部 split 结果一致."""
    print("\n=== 2. Chunk Passthrough: run_metrics ===")
    factor, returns = make_synthetic()
    chunk_size = 30

    # 预计算分块 / pre-compute chunks
    factor_chunks = split_into_chunks(factor, chunk_size)
    returns_chunks = split_into_chunks(returns, chunk_size)

    # A: 不传 chunk 参数（内部 split）/ internal split
    ev_a = FactorEvaluator(factor, returns, chunk_size=chunk_size)
    ev_a.run_metrics()

    # B: 传入预计算 chunk / pass pre-computed chunks
    ev_b = FactorEvaluator(factor, returns, chunk_size=chunk_size)
    ev_b.run_metrics(chunk_factor=factor_chunks, chunk_returns=returns_chunks)

    ok("IC 一致", np.allclose(ev_a.ic.values, ev_b.ic.values, equal_nan=True, atol=1e-10))
    ok("RankIC 一致", np.allclose(ev_a.rank_ic.values, ev_b.rank_ic.values, equal_nan=True, atol=1e-10))
    ok("ICIR 一致", np.isclose(ev_a.icir, ev_b.icir, atol=1e-10))
    ok("IC_stats IC_mean 一致", np.isclose(ev_a.ic_stats["IC_mean"], ev_b.ic_stats["IC_mean"], atol=1e-10))


def test_chunk_passthrough_grouping():
    """run_grouping 传入预计算 chunk 与内部 split 结果一致."""
    print("\n=== 3. Chunk Passthrough: run_grouping ===")
    factor, returns = make_synthetic()
    chunk_size = 30

    factor_chunks = split_into_chunks(factor, chunk_size)

    ev_a = FactorEvaluator(factor, returns, chunk_size=chunk_size)
    ev_a.run_grouping()

    ev_b = FactorEvaluator(factor, returns, chunk_size=chunk_size)
    ev_b.run_grouping(chunk_factor=factor_chunks)

    ok("group_labels 一致", (ev_a.group_labels == ev_b.group_labels).all())


def test_chunk_passthrough_curves():
    """run_curves 传入预计算 chunk 与内部 split 结果一致."""
    print("\n=== 4. Chunk Passthrough: run_curves ===")
    factor, returns = make_synthetic()
    chunk_size = 30

    factor_chunks = split_into_chunks(factor, chunk_size)
    returns_chunks = split_into_chunks(returns, chunk_size)

    # 先计算分组标签以提供 group chunks
    ev_ref = FactorEvaluator(factor, returns, chunk_size=chunk_size)
    ev_ref.run_grouping()
    group_chunks = split_into_chunks(ev_ref.group_labels, chunk_size)

    # A: 不传 chunk 参数
    ev_a = FactorEvaluator(factor, returns, chunk_size=chunk_size)
    ev_a.run_grouping().run_curves()

    # B: 传入预计算 chunk
    ev_b = FactorEvaluator(factor, returns, chunk_size=chunk_size)
    ev_b.run_grouping().run_curves(
        chunk_factor=factor_chunks,
        chunk_returns=returns_chunks,
        chunk_groups=group_chunks,
    )

    ok("long_curve 一致", np.allclose(ev_a.long_curve.values, ev_b.long_curve.values, atol=1e-10))
    ok("short_curve 一致", np.allclose(ev_a.short_curve.values, ev_b.short_curve.values, atol=1e-10))
    ok("hedge_curve 一致", np.allclose(ev_a.hedge_curve.values, ev_b.hedge_curve.values, atol=1e-10))
    ok("sharpe 一致", np.isclose(ev_a.sharpe, ev_b.sharpe, atol=1e-10))


def test_chunk_passthrough_turnover():
    """run_turnover 传入预计算 chunk 与内部 split 结果一致."""
    print("\n=== 5. Chunk Passthrough: run_turnover ===")
    factor, returns = make_synthetic()
    chunk_size = 30

    factor_chunks = split_into_chunks(factor, chunk_size)

    ev_ref = FactorEvaluator(factor, returns, chunk_size=chunk_size)
    ev_ref.run_grouping()
    group_chunks = split_into_chunks(ev_ref.group_labels, chunk_size)

    ev_a = FactorEvaluator(factor, returns, chunk_size=chunk_size)
    ev_a.run_grouping().run_turnover()

    ev_b = FactorEvaluator(factor, returns, chunk_size=chunk_size)
    ev_b.run_grouping().run_turnover(
        chunk_factor=factor_chunks,
        chunk_groups=group_chunks,
    )

    ok("turnover 一致", np.allclose(
        ev_a.turnover.values, ev_b.turnover.values, atol=1e-10, equal_nan=True,
    ))
    ok("rank_autocorr 一致", np.allclose(
        ev_a.rank_autocorr.values, ev_b.rank_autocorr.values, atol=1e-10, equal_nan=True,
    ))


def test_chunk_passthrough_neutralize():
    """run_neutralize 传入预计算 chunk 与内部 split 结果一致."""
    print("\n=== 6. Chunk Passthrough: run_neutralize ===")
    factor, returns = make_synthetic()
    chunk_size = 30

    factor_chunks = split_into_chunks(factor, chunk_size)
    returns_chunks = split_into_chunks(returns, chunk_size)

    ev_a = FactorEvaluator(factor, returns, chunk_size=chunk_size)
    ev_a.run_grouping().run_neutralize()

    # 预计算 group chunks (从 cached_group_labels)
    group_chunks = split_into_chunks(ev_a._cached_group_labels, chunk_size)

    ev_b = FactorEvaluator(factor, returns, chunk_size=chunk_size)
    ev_b.run_grouping().run_neutralize(
        chunk_factor=factor_chunks,
        chunk_returns=returns_chunks,
        chunk_groups=group_chunks,
    )

    ok("neutralized_curve 一致", np.allclose(
        ev_a.neutralized_curve.values, ev_b.neutralized_curve.values, atol=1e-10,
    ))


def test_full_mode_chunk_args_ignored():
    """全量模式 (chunk_size=None) 下 chunk 参数不影响结果."""
    print("\n=== 7. Full Mode: chunk args ignored ===")
    factor, returns = make_synthetic()

    ev_a = FactorEvaluator(factor, returns)
    ev_a.run_all()

    # 传入伪造 chunk 列表，全量模式下应被忽略
    ev_b = FactorEvaluator(factor, returns)
    ev_b.run_metrics(
        chunk_factor=[factor],  # 会被忽略因为 chunk_size=None
        chunk_returns=[returns],
    ).run_grouping(
        chunk_factor=[factor],
    ).run_curves(
        chunk_factor=[factor],
        chunk_returns=[returns],
        chunk_groups=[None],
    ).run_turnover(
        chunk_factor=[factor],
        chunk_groups=[None],
    ).run_neutralize(
        chunk_factor=[factor],
        chunk_returns=[returns],
        chunk_groups=[None],
    )

    ok("IC 一致", np.allclose(ev_a.ic.values, ev_b.ic.values, equal_nan=True, atol=1e-10))
    ok("group_labels 一致", (ev_a.group_labels == ev_b.group_labels).all())
    ok("hedge_curve 一致", np.allclose(ev_a.hedge_curve.values, ev_b.hedge_curve.values, atol=1e-10))
    ok("neutralized_curve 一致", np.allclose(
        ev_a.neutralized_curve.values, ev_b.neutralized_curve.values, atol=1e-10,
    ))


if __name__ == "__main__":
    print("=" * 60)
    print("Task 1 测试 — P0 chunk 列表参数")
    print("=" * 60)

    test_backward_compatible_no_chunk_args()
    test_chunk_passthrough_metrics()
    test_chunk_passthrough_grouping()
    test_chunk_passthrough_curves()
    test_chunk_passthrough_turnover()
    test_chunk_passthrough_neutralize()
    test_full_mode_chunk_args_ignored()

    print(f"\n{'=' * 60}")
    print(f"结果: {passed}/{checks} 通过")
    print("=" * 60)

    if passed < checks:
        sys.exit(1)
