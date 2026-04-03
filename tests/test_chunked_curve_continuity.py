"""
验证测试 — Task 10: 分块净值曲线持仓连续性验证
Chunked equity curve continuity verification.

验证多块拼接后曲线连续、cumprod 衔接点无跳变、rebalance_freq 与分块交互正常。
Verifies multi-chunk merged curves are continuous, cumprod junction points have no jumps,
and rebalance_freq interacts correctly with chunking.
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


def make_synthetic(n_dates=120, n_symbols=50, seed=42):
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


try:
    print("=== Task 10: 分块净值曲线持仓连续性验证 ===\n")

    from FactorAnalysis import FactorEvaluator
    from FactorAnalysis.evaluator import _merge_raw_curves
    from FactorAnalysis.chunking import split_into_chunks
    from FactorAnalysis.portfolio import (
        calc_long_only_curve,
        calc_short_only_curve,
        calc_top_bottom_curve,
    )

    # -------------------------------------------------------
    # 1. 曲线连续性：分块曲线隐含的日收益率与全量曲线一致
    # Curve continuity: implied daily returns from chunked curve match full curve
    # -------------------------------------------------------
    print("1. Curve continuity: implied daily returns match at boundaries")
    factor, returns = make_synthetic(n_dates=120, n_symbols=50, seed=42)

    ev_full = FactorEvaluator(factor, returns, chunk_size=None)
    ev_full.run_curves()
    ev_chunked = FactorEvaluator(factor, returns, chunk_size=30)
    ev_chunked.run_curves()

    # 从曲线反推日收益率 / derive daily returns from curve
    full_daily = ev_full.hedge_curve.pct_change().fillna(0.0)
    chunked_daily = ev_chunked.hedge_curve.pct_change().fillna(0.0)
    daily_diff = np.max(np.abs(chunked_daily.values - full_daily.values))
    ok(f"hedge implied daily returns match (diff={daily_diff:.2e})", daily_diff < 1e-10)

    full_long_daily = ev_full.long_curve.pct_change().fillna(0.0)
    chunked_long_daily = ev_chunked.long_curve.pct_change().fillna(0.0)
    long_daily_diff = np.max(np.abs(chunked_long_daily.values - full_long_daily.values))
    ok(f"long implied daily returns match (diff={long_daily_diff:.2e})", long_daily_diff < 1e-10)

    full_short_daily = ev_full.short_curve.pct_change().fillna(0.0)
    chunked_short_daily = ev_chunked.short_curve.pct_change().fillna(0.0)
    short_daily_diff = np.max(np.abs(chunked_short_daily.values - full_short_daily.values))
    ok(f"short implied daily returns match (diff={short_daily_diff:.2e})", short_daily_diff < 1e-10)

    # -------------------------------------------------------
    # 2. cumprod 衔接点无跳变：在每个块边界处验证比值一致性
    # cumprod junction: verify ratio consistency at each chunk boundary
    # -------------------------------------------------------
    print("\n2. cumprod junction points: no jumps at boundaries")
    timestamps = factor.index.get_level_values(0).unique().sort_values()
    n_ts = len(timestamps)

    for cs in [10, 20, 30, 50]:
        ev_c = FactorEvaluator(factor, returns, chunk_size=cs)
        ev_c.run_curves()

        chunk_starts = list(range(0, n_ts, cs))
        for i, start in enumerate(chunk_starts):
            if i + 1 < len(chunk_starts):
                end = chunk_starts[i + 1]
                prev_ts = timestamps[end - 1]
                next_ts = timestamps[end]
                prev_val = ev_c.hedge_curve.loc[prev_ts]
                next_val = ev_c.hedge_curve.loc[next_ts]
                full_prev = ev_full.hedge_curve.loc[prev_ts]
                full_next = ev_full.hedge_curve.loc[next_ts]
                # 边界比值应一致 / boundary ratio should match
                if full_prev != 0 and prev_val != 0:
                    cr = next_val / prev_val
                    fr = full_next / full_prev
                    ok(f"cs={cs}: boundary {prev_ts.date()}->{next_ts.date()} "
                       f"ratio diff={abs(cr - fr):.2e}", abs(cr - fr) < 1e-10)

    # -------------------------------------------------------
    # 3. 不同 chunk_size 下的曲线连续性
    # Continuity across various chunk sizes
    # -------------------------------------------------------
    print("\n3. Continuity across various chunk sizes")
    for cs in [5, 7, 13, 17, 23, 37, 59]:
        ev_cs = FactorEvaluator(factor, returns, chunk_size=cs)
        ev_cs.run_curves()
        cs_daily = ev_cs.hedge_curve.pct_change().fillna(0.0)
        cs_diff = np.max(np.abs(cs_daily.values - full_daily.values))
        ok(f"chunk_size={cs}: daily returns match (diff={cs_diff:.2e})", cs_diff < 1e-10)

    # -------------------------------------------------------
    # 4. rebalance_freq > 1 与分块交互：直接调用 portfolio 函数
    # rebalance_freq > 1 with chunking: call portfolio functions directly
    # -------------------------------------------------------
    print("\n4. rebalance_freq > 1 interaction with chunking")

    for reb_freq in [2, 3, 5, 7, 10]:
        full_long = calc_long_only_curve(
            factor, returns, n_groups=5, top_k=1, rebalance_freq=reb_freq,
        )
        full_short = calc_short_only_curve(
            factor, returns, n_groups=5, bottom_k=1, rebalance_freq=reb_freq,
        )
        full_hedge = calc_top_bottom_curve(
            factor, returns, n_groups=5, top_k=1, bottom_k=1, rebalance_freq=reb_freq,
        )

        chunk_size = 30
        factor_chunks = split_into_chunks(factor, chunk_size, rebalance_freq=reb_freq)
        returns_chunks = split_into_chunks(returns, chunk_size, rebalance_freq=reb_freq)

        long_chunks_raw = [
            calc_long_only_curve(fc, rc, n_groups=5, top_k=1,
                                 rebalance_freq=reb_freq, _raw=True)
            for fc, rc in zip(factor_chunks, returns_chunks)
        ]
        short_chunks_raw = [
            calc_short_only_curve(fc, rc, n_groups=5, bottom_k=1,
                                  rebalance_freq=reb_freq, _raw=True)
            for fc, rc in zip(factor_chunks, returns_chunks)
        ]
        hedge_chunks_raw = [
            calc_top_bottom_curve(fc, rc, n_groups=5, top_k=1, bottom_k=1,
                                  rebalance_freq=reb_freq, _raw=True)
            for fc, rc in zip(factor_chunks, returns_chunks)
        ]

        merged_long = _merge_raw_curves(long_chunks_raw)
        merged_short = _merge_raw_curves(short_chunks_raw)
        merged_hedge = _merge_raw_curves(hedge_chunks_raw)

        if len(merged_long) > 0:
            merged_long.iloc[0] = 1.0
        if len(merged_short) > 0:
            merged_short.iloc[0] = 1.0
        if len(merged_hedge) > 0:
            merged_hedge.iloc[0] = 1.0

        ok(f"reb_freq={reb_freq}: long length match", len(merged_long) == len(full_long))
        ok(f"reb_freq={reb_freq}: short length match", len(merged_short) == len(full_short))
        ok(f"reb_freq={reb_freq}: hedge length match", len(merged_hedge) == len(full_hedge))

        long_diff = np.max(np.abs(merged_long.values - full_long.values))
        short_diff = np.max(np.abs(merged_short.values - full_short.values))
        hedge_diff = np.max(np.abs(merged_hedge.values - full_hedge.values))
        ok(f"reb_freq={reb_freq}: long match (diff={long_diff:.2e})", long_diff < 1e-10)
        ok(f"reb_freq={reb_freq}: short match (diff={short_diff:.2e})", short_diff < 1e-10)
        ok(f"reb_freq={reb_freq}: hedge match (diff={hedge_diff:.2e})", hedge_diff < 1e-10)

        # 边界连续性 / boundary continuity
        merged_hd = merged_hedge.pct_change().fillna(0.0)
        full_hd = full_hedge.pct_change().fillna(0.0)
        hd_diff = np.max(np.abs(merged_hd.values - full_hd.values))
        ok(f"reb_freq={reb_freq}: hedge daily returns match (diff={hd_diff:.2e})",
           hd_diff < 1e-10)

    # -------------------------------------------------------
    # 5. chunk_size 不对齐 rebalance_freq 时的自动对齐
    # Auto alignment when chunk_size not aligned with rebalance_freq
    # -------------------------------------------------------
    print("\n5. Auto alignment of chunk_size to rebalance_freq")
    for reb_freq, raw_cs in [(5, 12), (5, 17), (3, 10), (7, 25)]:
        effective_size = raw_cs
        if raw_cs % reb_freq != 0:
            effective_size = raw_cs + (reb_freq - raw_cs % reb_freq)

        chunks = split_into_chunks(factor, raw_cs, rebalance_freq=reb_freq)
        returns_chunks = split_into_chunks(returns, raw_cs, rebalance_freq=reb_freq)

        # 非末块应包含 effective_size 个时间截面 / non-last chunks have effective_size ts
        for i, chunk in enumerate(chunks):
            chunk_ts = chunk.index.get_level_values(0).unique()
            if i < len(chunks) - 1:
                ok(f"reb={reb_freq}, cs={raw_cs}: chunk {i} has {effective_size} ts",
                   len(chunk_ts) == effective_size)

        full_hedge = calc_top_bottom_curve(
            factor, returns, n_groups=5, top_k=1, bottom_k=1, rebalance_freq=reb_freq,
        )
        hedge_chunks_raw = [
            calc_top_bottom_curve(fc, rc, n_groups=5, top_k=1, bottom_k=1,
                                  rebalance_freq=reb_freq, _raw=True)
            for fc, rc in zip(chunks, returns_chunks)
        ]
        merged_hedge = _merge_raw_curves(hedge_chunks_raw)
        if len(merged_hedge) > 0:
            merged_hedge.iloc[0] = 1.0

        ok(f"reb={reb_freq}, cs={raw_cs}: hedge length match",
           len(merged_hedge) == len(full_hedge))
        h_diff = np.max(np.abs(merged_hedge.values - full_hedge.values))
        ok(f"reb={reb_freq}, cs={raw_cs}: hedge numerical match (diff={h_diff:.2e})",
           h_diff < 1e-10)

    # -------------------------------------------------------
    # 6. chunk_size=1 极端情况的曲线连续性
    # chunk_size=1 extreme case continuity
    # -------------------------------------------------------
    print("\n6. Extreme: chunk_size=1 continuity")
    ev_c1 = FactorEvaluator(factor, returns, chunk_size=1)
    ev_c1.run_curves()

    c1_daily = ev_c1.hedge_curve.pct_change().fillna(0.0)
    c1_diff = np.max(np.abs(c1_daily.values - full_daily.values))
    ok(f"chunk_size=1: hedge daily returns match (diff={c1_diff:.2e})", c1_diff < 1e-10)

    c1_long_daily = ev_c1.long_curve.pct_change().fillna(0.0)
    c1_long_diff = np.max(np.abs(c1_long_daily.values - full_long_daily.values))
    ok(f"chunk_size=1: long daily returns match (diff={c1_long_diff:.2e})", c1_long_diff < 1e-10)

    c1_short_daily = ev_c1.short_curve.pct_change().fillna(0.0)
    c1_short_diff = np.max(np.abs(c1_short_daily.values - full_short_daily.values))
    ok(f"chunk_size=1: short daily returns match (diff={c1_short_diff:.2e})", c1_short_diff < 1e-10)

    # -------------------------------------------------------
    # 7. 多种子 + 多 chunk_size 的连续性稳定性
    # Multi-seed stability for curve continuity
    # -------------------------------------------------------
    print("\n7. Multi-seed stability for curve continuity")
    for seed in [0, 7, 99, 12345, 999]:
        f, r = make_synthetic(n_dates=100, n_symbols=40, seed=seed)
        ev_f = FactorEvaluator(f, r, chunk_size=None)
        ev_c = FactorEvaluator(f, r, chunk_size=25)
        ev_f.run_curves()
        ev_c.run_curves()
        f_daily = ev_f.hedge_curve.pct_change().fillna(0.0)
        c_daily = ev_c.hedge_curve.pct_change().fillna(0.0)
        d_diff = np.max(np.abs(c_daily.values - f_daily.values))
        ok(f"seed={seed}: hedge daily returns match (diff={d_diff:.2e})", d_diff < 1e-10)

    # -------------------------------------------------------
    # 8. 含 NaN 数据的曲线连续性
    # Curve continuity with NaN factor values
    # -------------------------------------------------------
    print("\n8. Curve continuity with NaN factor values")
    rng = np.random.default_rng(42)
    f_nan = factor.copy()
    nan_mask = rng.random(len(f_nan)) < 0.15
    f_nan[nan_mask] = np.nan

    ev_f_nan = FactorEvaluator(f_nan, returns, chunk_size=None)
    ev_c_nan = FactorEvaluator(f_nan, returns, chunk_size=30)
    ev_f_nan.run_curves()
    ev_c_nan.run_curves()

    fn_daily = ev_f_nan.hedge_curve.pct_change().fillna(0.0)
    cn_daily = ev_c_nan.hedge_curve.pct_change().fillna(0.0)
    nan_daily_diff = np.max(np.abs(cn_daily.values - fn_daily.values))
    ok(f"NaN factor: hedge daily returns match (diff={nan_daily_diff:.2e})",
       nan_daily_diff < 1e-10)

    # -------------------------------------------------------
    # 9. hedge_curve_after_cost 连续性
    # Cost-adjusted curve continuity
    # -------------------------------------------------------
    print("\n9. Cost-adjusted curve continuity")
    for cr in [0.0, 0.001, 0.002, 0.005]:
        ev_cost_full = FactorEvaluator(factor, returns, chunk_size=None, cost_rate=cr)
        ev_cost_full.run_curves()
        for cs in [15, 30, 50]:
            ev_cost_c = FactorEvaluator(factor, returns, chunk_size=cs, cost_rate=cr)
            ev_cost_c.run_curves()
            cost_daily = ev_cost_c.hedge_curve_after_cost.pct_change().fillna(0.0)
            full_cost_daily = ev_cost_full.hedge_curve_after_cost.pct_change().fillna(0.0)
            common_idx = cost_daily.index.intersection(full_cost_daily.index)
            if len(common_idx) > 0:
                cd_diff = np.max(np.abs(
                    cost_daily.loc[common_idx].values
                    - full_cost_daily.loc[common_idx].values
                ))
                ok(f"cr={cr}, cs={cs}: after_cost daily match (diff={cd_diff:.2e})",
                   cd_diff < 1e-10)

    # -------------------------------------------------------
    # 10. rebalance_freq + 不同 n_groups / top_k / bottom_k 组合
    # rebalance_freq with various group parameters
    # -------------------------------------------------------
    print("\n10. rebalance_freq with various group parameters")
    for reb_freq in [3, 5]:
        for n_groups, top_k, bottom_k in [(5, 1, 1), (5, 2, 1), (10, 2, 2), (3, 1, 1)]:
            full_h = calc_top_bottom_curve(
                factor, returns, n_groups=n_groups, top_k=top_k,
                bottom_k=bottom_k, rebalance_freq=reb_freq,
            )
            chunks_f = split_into_chunks(factor, 25, rebalance_freq=reb_freq)
            chunks_r = split_into_chunks(returns, 25, rebalance_freq=reb_freq)
            h_chunks = [
                calc_top_bottom_curve(
                    fc, rc, n_groups=n_groups, top_k=top_k,
                    bottom_k=bottom_k, rebalance_freq=reb_freq, _raw=True,
                )
                for fc, rc in zip(chunks_f, chunks_r)
            ]
            merged_h = _merge_raw_curves(h_chunks)
            if len(merged_h) > 0:
                merged_h.iloc[0] = 1.0
            h_diff = np.max(np.abs(merged_h.values - full_h.values))
            ok(f"reb={reb_freq}, ng={n_groups}, t={top_k}, b={bottom_k}: "
               f"match (diff={h_diff:.2e})", h_diff < 1e-10)

    print(f"\n{'='*40}")
    print(f"Total: {checks} checks")

except Exception as e:
    print(f"\nFATAL: {e}")
    traceback.print_exc()
    sys.exit(1)
