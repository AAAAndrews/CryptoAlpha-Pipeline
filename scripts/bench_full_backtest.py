"""
全量回测基准测试脚本 / Full backtest benchmark script
测试全历史、全币对、1h K线的因子回测耗时。
Tests full-history, all-symbols, 1h kline factor backtest timing.
"""

import os
import sys
import re
import time
import gc
from datetime import datetime

# 项目根目录 / Project root
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
for submodule in ["CryptoDataProviders", "CryptoDB_feather"]:
    sp = os.path.join(project_root, submodule)
    if sp not in sys.path:
        sys.path.insert(0, sp)

import pandas as pd
import numpy as np
from CryptoDB_feather.config import DB_ROOT_PATH


def get_perpetual_symbols():
    """获取所有永续合约列表 / Get all perpetual swap symbols."""
    exchange_path = os.path.join(DB_ROOT_PATH, "binance")
    if not os.path.exists(exchange_path):
        return []
    dirs = [d for d in os.listdir(exchange_path)
            if os.path.isdir(os.path.join(exchange_path, d))]
    # 过滤掉季度合约(_6位数字)、BUSD、SETTLED / Filter out quarterly, BUSD, SETTLED
    perpetual = [d for d in dirs
                 if not re.search(r'_\d{6}$', d)
                 and not d.endswith('BUSD')
                 and not d.endswith('SETTLED')]
    perpetual.sort()
    return perpetual


def estimate_data_size(symbols):
    """估算数据行数 / Estimate total data rows."""
    from CryptoDB_feather.core.reader import read_symbol_klines
    total = 0
    # 抽样 5 个估算 / Sample 5 to estimate
    sample = symbols[:5] if len(symbols) >= 5 else symbols
    sample_total = 0
    for s in sample:
        df = read_symbol_klines(DB_ROOT_PATH, "binance", s, "swap", "1h")
        sample_total += len(df)
    avg_per_symbol = sample_total / len(sample)
    return avg_per_symbol * len(symbols), avg_per_symbol


def main():
    print("=" * 70)
    print(f"  Full Backtest Benchmark")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    symbols = get_perpetual_symbols()
    print(f"\n  Perpetual symbols: {len(symbols)}")

    # 估算数据量 / Estimate data size
    est_total, avg_per = estimate_data_size(symbols)
    print(f"  Avg rows/symbol: {avg_per:.0f}")
    print(f"  Estimated total rows: {est_total:,.0f}")
    est_gb = est_total * 12 * 8 / 1024**3  # 12 cols, float64
    print(f"  Estimated raw data size: {est_gb:.2f} GB")

    # 检查可用内存 / Check available memory
    import psutil
    avail_gb = psutil.virtual_memory().available / 1024**3
    print(f"  Available RAM: {avail_gb:.1f} GB")

    if est_gb > avail_gb * 0.6:
        print(f"\n  WARNING: Estimated data ({est_gb:.1f} GB) exceeds 60% of available RAM ({avail_gb:.1f} GB)")
        print(f"  Falling back to USDC perpetuals only.")
        symbols = [s for s in symbols if s.endswith('USDC')]
        print(f"  Using {len(symbols)} USDC perpetuals.")
        est_total, avg_per = estimate_data_size(symbols)
        est_gb = est_total * 12 * 8 / 1024**3
        print(f"  Estimated total rows: {est_total:,.0f} ({est_gb:.2f} GB)")

    # ── Step 1: Data Loading ──────────────────────────
    print(f"\n{'─' * 70}")
    print(f"  [Step 1] Data Loading — {len(symbols)} symbols")
    print(f"{'─' * 70}")

    from CryptoDB_feather.core.reader import load_multi_klines
    from Cross_Section_Factor.kline_loader import KlineLoader

    t0 = time.time()
    loader = KlineLoader(symbols=symbols, interval="1h", kline_type="swap")
    data = loader.compile()
    t_load = time.time() - t0
    n_symbols = data["symbol"].nunique()
    print(f"  Loaded {len(data):,} rows, {n_symbols} symbols in {t_load:.1f}s")

    # ── Step 2: Factor Calculation ────────────────────
    print(f"\n{'─' * 70}")
    print(f"  [Step 2] Factor Calculation — AlphaMomentum")
    print(f"{'─' * 70}")

    from FactorLib import get
    factor_cls = get("AlphaMomentum")
    factor_inst = factor_cls()

    t0 = time.time()
    factor_raw = factor_inst.calculate(data)

    from scripts.run_factor_research import _build_factor_multiindex
    factor_values = _build_factor_multiindex(factor_raw, data)
    t_factor = time.time() - t0
    print(f"  Factor computed, {factor_values.notna().sum():,} valid values in {t_factor:.1f}s")

    # ── Step 3: Returns Calculation ───────────────────
    print(f"\n{'─' * 70}")
    print(f"  [Step 3] Returns Calculation — close2close")
    print(f"{'─' * 70}")

    from FactorAnalysis.returns import calc_returns

    t0 = time.time()
    returns = calc_returns(data, label="close2close")
    t_returns = time.time() - t0
    print(f"  Returns computed, {returns.notna().sum():,} valid values in {t_returns:.1f}s")

    # ── Step 4: Alignment + Quality ──────────────────
    print(f"\n{'─' * 70}")
    print(f"  [Step 4] Factor-Returns Alignment + Data Quality")
    print(f"{'─' * 70}")

    from FactorAnalysis.alignment import align_factor_returns
    from FactorAnalysis.data_quality import check_data_quality

    t0 = time.time()
    clean = align_factor_returns(factor_values, returns)
    coverage = check_data_quality(clean["factor"], clean["returns"], max_loss=0.35)
    t_align = time.time() - t0
    print(f"  Aligned: {len(clean):,} clean pairs, coverage {coverage:.1%} in {t_align:.1f}s")

    # ── Step 5: Evaluation ───────────────────────────
    print(f"\n{'─' * 70}")
    print(f"  [Step 5] Factor Evaluation (Tear Sheet)")
    print(f"{'─' * 70}")

    from FactorAnalysis.evaluator import FactorEvaluator

    # 尝试全量模式，OOM 时自动降级为分块模式
    # Try full mode, fallback to chunked on OOM
    chunk_size = None
    eval_succeeded = False

    for try_chunk in [None, 200, 100, 50]:
        try:
            ev = FactorEvaluator(
                clean["factor"], clean["returns"],
                n_groups=5, top_k=1, bottom_k=1,
                cost_rate=0.001, risk_free_rate=0.0,
                periods_per_year=252,
                chunk_size=try_chunk,
            )
            t0 = time.time()
            ev.run_all()
            t_eval = time.time() - t0
            chunk_size = try_chunk
            eval_succeeded = True
            if try_chunk is not None:
                print(f"  (used chunk_size={try_chunk} due to memory constraints)")
            break
        except MemoryError as e:
            print(f"  OOM with chunk_size={try_chunk}, trying smaller...")
            del ev
            gc.collect()
            continue

    if not eval_succeeded:
        print("  ERROR: Evaluation failed even with smallest chunk_size")
        return
    print(f"  Evaluation completed in {t_eval:.1f}s")

    # ── Report ────────────────────────────────────────
    t_total = t_load + t_factor + t_returns + t_align + t_eval

    print(f"\n{'=' * 70}")
    print(f"  Benchmark Results")
    print(f"{'=' * 70}")
    print(f"  Symbols:        {n_symbols}")
    print(f"  Data Rows:      {len(data):,}")
    print(f"  Time Range:     {pd.to_datetime(data['timestamp'].min(), unit='ms')} ~ {pd.to_datetime(data['timestamp'].max(), unit='ms')}")
    print(f"  {'─' * 40}")
    print(f"  Data Loading:   {t_load:>8.1f}s  ({t_load/t_total*100:>5.1f}%)")
    print(f"  Factor Calc:    {t_factor:>8.1f}s  ({t_factor/t_total*100:>5.1f}%)")
    print(f"  Returns Calc:   {t_returns:>8.1f}s  ({t_returns/t_total*100:>5.1f}%)")
    print(f"  Alignment:      {t_align:>8.1f}s  ({t_align/t_total*100:>5.1f}%)")
    print(f"  Evaluation:     {t_eval:>8.1f}s  ({t_eval/t_total*100:>5.1f}%)")
    print(f"  {'─' * 40}")
    print(f"  TOTAL:          {t_total:>8.1f}s")
    print(f"  TOTAL:          {t_total/60:>8.1f}min")
    print(f"{'=' * 70}")

    # 关键指标 / Key metrics
    if ev.icir is not None:
        print(f"  ICIR:           {ev.icir:.4f}")
    if ev.sharpe is not None:
        print(f"  Sharpe:         {ev.sharpe:.4f}")
    if ev.hedge_curve is not None:
        print(f"  Hedge Return:   {ev.hedge_curve.iloc[-1] - 1.0:.4f}")
    if ev.hedge_curve_after_cost is not None:
        print(f"  Hedge (cost):   {ev.hedge_curve_after_cost.iloc[-1] - 1.0:.4f}")
    print(f"{'=' * 70}")

    # 释放内存 / Free memory
    del data, factor_raw, factor_values, returns, clean, ev
    gc.collect()


if __name__ == "__main__":
    main()
