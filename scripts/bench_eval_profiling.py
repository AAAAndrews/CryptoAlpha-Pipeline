"""
评估环节细粒度计时分析脚本 / Fine-grained evaluation profiling script
在 chunk_size=500 约束下，对 run_all() 每个子步骤、每个内部函数进行计时。
Profiles each sub-step and internal function of run_all() with chunk_size=500.
"""

import os
import sys
import re
import time
import gc
from datetime import datetime

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
for submodule in ["CryptoDataProviders", "CryptoDB_feather"]:
    sp = os.path.join(project_root, submodule)
    if sp not in sys.path:
        sys.path.insert(0, sp)

import numpy as np
import pandas as pd
from CryptoDB_feather.config import DB_ROOT_PATH


class Timer:
    """简单计时器 / Simple timer."""
    def __init__(self):
        self.records = []

    def record(self, name, elapsed, extra=None):
        self.records.append({"name": name, "time": elapsed, "extra": extra or {}})

    def report(self):
        total = sum(r["time"] for r in self.records)
        lines = []
        lines.append(f"{'─' * 75}")
        lines.append(f"  {'Function':<45} {'Time(s)':>8} {'%':>6}   {'Detail'}")
        lines.append(f"{'─' * 75}")
        for r in self.records:
            pct = r["time"] / total * 100 if total > 0 else 0
            detail = r["extra"].get("detail", "")
            lines.append(f"  {r['name']:<45} {r['time']:>8.2f} {pct:>5.1f}%   {detail}")
        lines.append(f"{'─' * 75}")
        lines.append(f"  {'TOTAL':<45} {total:>8.2f} 100.0%")
        lines.append(f"{'─' * 75}")
        return "\n".join(lines)


def get_perpetual_symbols():
    exchange_path = os.path.join(DB_ROOT_PATH, "binance")
    dirs = [d for d in os.listdir(exchange_path) if os.path.isdir(os.path.join(exchange_path, d))]
    return sorted([d for d in dirs
                   if not re.search(r'_\d{6}$', d)
                   and not d.endswith('BUSD')
                   and not d.endswith('SETTLED')])


def main():
    CHUNK_SIZE = 500
    timer = Timer()

    print("=" * 75)
    print(f"  Evaluation Profiling — chunk_size={CHUNK_SIZE}")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 75)

    # ── 数据加载（不计入评估耗时） ──────────────────────────────
    symbols = get_perpetual_symbols()
    print(f"\n  Symbols: {len(symbols)}")

    from Cross_Section_Factor.kline_loader import KlineLoader
    loader = KlineLoader(symbols=symbols, interval="1h", kline_type="swap")
    data = loader.compile()
    n_rows = len(data)
    n_symbols = data["symbol"].nunique()
    n_ts = data["timestamp"].nunique()
    print(f"  Data: {n_rows:,} rows, {n_symbols} symbols, {n_ts} timestamps")

    from FactorLib import get
    factor_cls = get("AlphaMomentum")
    factor_raw = factor_cls().calculate(data)
    from scripts.run_factor_research import _build_factor_multiindex
    factor_values = _build_factor_multiindex(factor_raw, data)

    from FactorAnalysis.returns import calc_returns
    returns = calc_returns(data, label="close2close")

    from FactorAnalysis.alignment import align_factor_returns
    from FactorAnalysis.data_quality import check_data_quality
    clean = align_factor_returns(factor_values, returns)
    check_data_quality(clean["factor"], clean["returns"], max_loss=0.35)

    factor = clean["factor"]
    ret = clean["returns"]
    del data, factor_raw, factor_values, returns, clean
    gc.collect()

    n_chunks = (n_ts + CHUNK_SIZE - 1) // CHUNK_SIZE
    print(f"  Chunks: {n_chunks} (chunk_size={CHUNK_SIZE}, timestamps={n_ts})")

    # 导入所有需要的函数 / Import all functions
    from FactorAnalysis.metrics import calc_ic, calc_rank_ic
    from FactorAnalysis.grouping import quantile_group
    from FactorAnalysis.portfolio import calc_portfolio_curves
    from FactorAnalysis.turnover import calc_turnover, calc_rank_autocorr
    from FactorAnalysis.neutralize import calc_neutralized_curve
    from FactorAnalysis.cost import deduct_cost
    from FactorAnalysis.chunking import split_into_chunks, merge_chunk_results
    from FactorAnalysis.metrics import calc_sharpe, calc_calmar, calc_sortino
    from FactorAnalysis.evaluator import _icir_from_series, _ic_stats_from_series, _merge_raw_curves

    # ====================================================================
    # 1. run_metrics — 逐块 calc_ic + calc_rank_ic
    # ====================================================================
    print(f"\n{'─' * 75}")
    print(f"  [1] run_metrics — IC / RankIC / ICIR / IC_stats")
    print(f"{'─' * 75}")

    t_metrics_start = time.time()
    factor_chunks = split_into_chunks(factor, CHUNK_SIZE)
    returns_chunks = split_into_chunks(ret, CHUNK_SIZE)

    # 计时 unstack / time unstack
    t_unstack_ic = 0.0
    t_unstack_ric = 0.0
    t_rank_op = 0.0
    t_numpy_pearson = 0.0
    ic_chunks = []
    rank_ic_chunks = []

    for i, (fc, rc) in enumerate(zip(factor_chunks, returns_chunks)):
        # --- calc_ic 内部计时 ---
        t0 = time.time()
        df_ic = pd.DataFrame({"factor": fc, "returns": rc})
        t1 = time.time()
        t_unstack_ic += time.time() - t0

        f_mat = df_ic["factor"].unstack(level=1)
        r_mat = df_ic["returns"].unstack(level=1)
        t2 = time.time()
        t_unstack_ic += time.time() - t1

        valid = f_mat.notna() & r_mat.notna() & np.isfinite(f_mat) & np.isfinite(r_mat)
        n_valid = valid.sum(axis=1).astype(float)
        f_clean = f_mat.where(valid)
        r_clean = r_mat.where(valid)
        t3 = time.time()

        sum_x = f_clean.sum(axis=1)
        sum_y = r_clean.sum(axis=1)
        sum_xy = (f_clean * r_clean).sum(axis=1)
        sum_x2 = (f_clean ** 2).sum(axis=1)
        sum_y2 = (r_clean ** 2).sum(axis=1)
        n = n_valid
        numerator = n * sum_xy - sum_x * sum_y
        denom_x = n * sum_x2 - sum_x ** 2
        denom_y = n * sum_y2 - sum_y ** 2
        with np.errstate(invalid="ignore", divide="ignore"):
            denom = np.sqrt(denom_x * denom_y)
            ic = numerator / denom
        ic = pd.Series(ic, index=f_mat.index, dtype=float)
        ic[n < 2] = np.nan
        ic = ic.replace([np.inf, -np.inf], np.nan)
        t4 = time.time()
        t_numpy_pearson += time.time() - t3

        ic_chunks.append(ic)

        # --- calc_rank_ic 内部计时 ---
        t0 = time.time()
        df_ric = pd.DataFrame({"factor": fc, "returns": rc})
        t1 = time.time()
        t_unstack_ric += time.time() - t0

        f_mat2 = df_ric["factor"].unstack(level=1)
        r_mat2 = df_ric["returns"].unstack(level=1)
        t2 = time.time()
        t_unstack_ric += time.time() - t1

        valid2 = f_mat2.notna() & r_mat2.notna() & np.isfinite(f_mat2) & np.isfinite(r_mat2)
        n_valid2 = valid2.sum(axis=1).astype(float)
        f_masked = f_mat2.where(valid2)
        r_masked = r_mat2.where(valid2)

        t0_rank = time.time()
        f_ranked = f_masked.rank(axis=1, method="average", na_option="keep")
        r_ranked = r_masked.rank(axis=1, method="average", na_option="keep")
        t_rank_op += time.time() - t0_rank

        f_clean2 = f_ranked.where(valid2)
        r_clean2 = r_ranked.where(valid2)

        t0_np = time.time()
        sum_x = f_clean2.sum(axis=1)
        sum_y = r_clean2.sum(axis=1)
        sum_xy = (f_clean2 * r_clean2).sum(axis=1)
        sum_x2 = (f_clean2 ** 2).sum(axis=1)
        sum_y2 = (r_clean2 ** 2).sum(axis=1)
        n = n_valid2
        numerator = n * sum_xy - sum_x * sum_y
        denom_x = n * sum_x2 - sum_x ** 2
        denom_y = n * sum_y2 - sum_y ** 2
        with np.errstate(invalid="ignore", divide="ignore"):
            denom = np.sqrt(denom_x * denom_y)
            rank_ic = numerator / denom
        rank_ic = pd.Series(rank_ic, index=f_mat2.index, dtype=float)
        rank_ic[n < 2] = np.nan
        rank_ic = rank_ic.replace([np.inf, -np.inf], np.nan)
        t4 = time.time()
        t_numpy_pearson += time.time() - t0_np

        rank_ic_chunks.append(rank_ic)

        if (i + 1) % 20 == 0 or i == n_chunks - 1:
            print(f"    chunk {i+1}/{n_chunks} done")

    # merge
    t0 = time.time()
    ic_merged = merge_chunk_results(ic_chunks, "ic")
    rank_ic_merged = merge_chunk_results(rank_ic_chunks, "ic")
    icir = _icir_from_series(ic_merged)
    ic_stats = _ic_stats_from_series(ic_merged)
    t_merge_metrics = time.time() - t0

    t_metrics_total = time.time() - t_metrics_start
    timer.record("1. run_metrics (total)", t_metrics_total,
                 {"detail": f"IC+RankIC × {n_chunks} chunks"})
    timer.record("  1a. calc_ic: DataFrame+unstack", t_unstack_ic,
                 {"detail": f"{n_chunks} chunks"})
    timer.record("  1b. calc_ic: numpy Pearson", t_numpy_pearson,
                 {"detail": f"{n_chunks} chunks"})
    timer.record("  1c. calc_rank_ic: DataFrame+unstack", t_unstack_ric,
                 {"detail": f"{n_chunks} chunks"})
    timer.record("  1d. calc_rank_ic: rank(axis=1)", t_rank_op,
                 {"detail": f"{n_chunks} chunks × 2 matrices"})
    timer.record("  1e. calc_rank_ic: numpy Pearson", t_numpy_pearson,
                 {"detail": "(included in 1b)"})
    timer.record("  1f. metrics merge+ICIR+stats", t_merge_metrics,
                 {"detail": "concat + scalar stats"})
    del ic_chunks, rank_ic_chunks, factor_chunks, returns_chunks
    gc.collect()
    print(f"    run_metrics done: {t_metrics_total:.1f}s")

    # ====================================================================
    # 2. run_grouping — quantile_group per chunk
    # ====================================================================
    print(f"\n{'─' * 75}")
    print(f"  [2] run_grouping — quantile_group")
    print(f"{'─' * 75}")

    t_grouping_start = time.time()
    factor_chunks = split_into_chunks(factor, CHUNK_SIZE)
    t_qg_total = 0.0
    t_qg_apply = 0.0
    chunk_labels = []

    for i, fc in enumerate(factor_chunks):
        t0 = time.time()
        labels = quantile_group(fc, n_groups=5)
        t1 = time.time()
        t_qg_total += time.time() - t0
        chunk_labels.append(labels)

        if (i + 1) % 20 == 0 or i == n_chunks - 1:
            print(f"    chunk {i+1}/{n_chunks} done")

    group_labels = merge_chunk_results(chunk_labels, "ic")
    t_grouping_total = time.time() - t_grouping_start
    timer.record("2. run_grouping (total)", t_grouping_total,
                 {"detail": f"quantile_group × {n_chunks} chunks"})
    timer.record("  2a. quantile_group", t_qg_total,
                 {"detail": f"groupby.apply(qcut) × {n_chunks}"})
    del factor_chunks, chunk_labels
    gc.collect()
    print(f"    run_grouping done: {t_grouping_total:.1f}s")

    # ====================================================================
    # 3. run_curves — calc_portfolio_curves per chunk
    # ====================================================================
    print(f"\n{'─' * 75}")
    print(f"  [3] run_curves — portfolio curves")
    print(f"{'─' * 75}")

    t_curves_start = time.time()
    factor_chunks = split_into_chunks(factor, CHUNK_SIZE)
    returns_chunks = split_into_chunks(ret, CHUNK_SIZE)
    label_chunks = split_into_chunks(group_labels, CHUNK_SIZE)

    t_labels_prepare = 0.0
    t_df_build = 0.0
    t_groupby_apply = 0.0
    t_cumprod = 0.0
    long_chunks = []
    short_chunks = []
    hedge_chunks = []

    for i, (fc, rc, lc) in enumerate(zip(factor_chunks, returns_chunks, label_chunks)):
        # labels prepare (cached, just slicing)
        t0 = time.time()
        # _calc_labels_with_rebalance with cached labels → just return lc
        # (already cached, skip timing)
        labels = lc
        t_labels_prepare += time.time() - t0

        # DataFrame build
        t0 = time.time()
        from FactorAnalysis.portfolio import _portfolio_curves_core
        df = pd.DataFrame({"label": labels, "returns": rc})
        t_df_build += time.time() - t0

        # groupby.apply
        top_labels = set(range(4, 5))  # n_groups=5, top_k=1
        bottom_labels = set(range(0, 1))  # bottom_k=1

        def _portfolio_returns(g):
            valid = g["returns"].notna() & np.isfinite(g["returns"])
            long_mask = valid & g["label"].isin(top_labels)
            short_mask = valid & g["label"].isin(bottom_labels)
            long_ret = g.loc[long_mask, "returns"].mean() if long_mask.sum() > 0 else 0.0
            short_ret = -g.loc[short_mask, "returns"].mean() if short_mask.sum() > 0 else 0.0
            return pd.Series({"long": long_ret, "short": short_ret, "hedge": long_ret + short_ret})

        t0 = time.time()
        daily = df.groupby(level=0).apply(_portfolio_returns)
        t_groupby_apply += time.time() - t0

        # cumprod
        t0 = time.time()
        lc_long = (1.0 + daily["long"]).cumprod()
        lc_short = (1.0 + daily["short"]).cumprod()
        lc_hedge = (1.0 + daily["hedge"]).cumprod()
        t_cumprod += time.time() - t0

        long_chunks.append(lc_long)
        short_chunks.append(lc_short)
        hedge_chunks.append(lc_hedge)

        if (i + 1) % 20 == 0 or i == n_chunks - 1:
            print(f"    chunk {i+1}/{n_chunks} done")

    # merge raw curves
    t0 = time.time()
    long_curve = _merge_raw_curves(long_chunks)
    short_curve = _merge_raw_curves(short_chunks)
    hedge_curve = _merge_raw_curves(hedge_chunks)
    if len(long_curve) > 0: long_curve.iloc[0] = 1.0
    if len(short_curve) > 0: short_curve.iloc[0] = 1.0
    if len(hedge_curve) > 0: hedge_curve.iloc[0] = 1.0
    t_merge_curves = time.time() - t0

    # cost deduction
    t0 = time.time()
    hedge_daily = hedge_curve.pct_change().fillna(0.0)
    hedge_after_cost = deduct_cost(hedge_daily, cost_rate=0.001)
    t_cost = time.time() - t0

    # performance ratios
    t0 = time.time()
    sharpe = calc_sharpe(hedge_curve, risk_free_rate=0.0, periods_per_year=252)
    calmar = calc_calmar(hedge_curve, periods_per_year=252)
    sortino = calc_sortino(hedge_curve, risk_free_rate=0.0, periods_per_year=252)
    sharpe_ac = calc_sharpe(hedge_after_cost, risk_free_rate=0.0, periods_per_year=252)
    calmar_ac = calc_calmar(hedge_after_cost, periods_per_year=252)
    sortino_ac = calc_sortino(hedge_after_cost, risk_free_rate=0.0, periods_per_year=252)
    t_perf_ratios = time.time() - t0

    t_curves_total = time.time() - t_curves_start
    timer.record("3. run_curves (total)", t_curves_total,
                 {"detail": f"portfolio_curves × {n_chunks} chunks"})
    timer.record("  3a. DataFrame build", t_df_build,
                 {"detail": f"pd.DataFrame(label, returns) × {n_chunks}"})
    timer.record("  3b. groupby.apply(_portfolio_returns)", t_groupby_apply,
                 {"detail": f"THE BOTTLENECK? {n_chunks} × {n_ts} timestamps"})
    timer.record("  3c. cumprod", t_cumprod,
                 {"detail": f"3 curves × {n_chunks}"})
    timer.record("  3d. merge raw curves", t_merge_curves,
                 {"detail": f"scale+concat × 3 curves"})
    timer.record("  3e. cost deduction", t_cost,
                 {"detail": "pct_change + deduct_cost"})
    timer.record("  3f. performance ratios", t_perf_ratios,
                 {"detail": "sharpe/calmar/sortino × 2"})
    del factor_chunks, returns_chunks, label_chunks
    del long_chunks, short_chunks, hedge_chunks
    gc.collect()
    print(f"    run_curves done: {t_curves_total:.1f}s")

    # ====================================================================
    # 4. run_turnover — calc_turnover + calc_rank_autocorr per chunk
    # ====================================================================
    print(f"\n{'─' * 75}")
    print(f"  [4] run_turnover — turnover + rank_autocorr")
    print(f"{'─' * 75}")

    t_turnover_start = time.time()
    factor_chunks = split_into_chunks(factor, CHUNK_SIZE)
    label_chunks = split_into_chunks(group_labels, CHUNK_SIZE)

    t_to_unstack = 0.0
    t_to_loop = 0.0
    t_ra_rank = 0.0
    t_ra_unstack = 0.0
    t_ra_pearson = 0.0
    turnover_chunks = []
    autocorr_chunks = []

    for i, (fc, lc) in enumerate(zip(factor_chunks, label_chunks)):
        # --- calc_turnover ---
        t0 = time.time()
        labels_mat = lc.unstack(level=1)
        shifted = labels_mat.shift(1)
        t_to_unstack += time.time() - t0

        t0 = time.time()
        results = {}
        for g in range(5):
            current = (labels_mat == g)
            prev = (shifted == g)
            current_count = current.sum(axis=1)
            overlap = (current & prev).sum(axis=1)
            turnover = 1.0 - overlap / current_count
            turnover[current_count == 0] = np.nan
            turnover.iloc[0] = np.nan
            results[g] = turnover
        to_df = pd.DataFrame(results)
        t_to_loop += time.time() - t0
        turnover_chunks.append(to_df)

        # --- calc_rank_autocorr ---
        t0 = time.time()
        ranks = fc.groupby(level=0, group_keys=False).rank()
        t_ra_rank += time.time() - t0

        t0 = time.time()
        ranks_mat = ranks.unstack(level=1)
        shifted_mat = ranks_mat.shift(1)
        valid = ranks_mat.notna() & shifted_mat.notna()
        n_valid = valid.sum(axis=1).astype(float)
        x = ranks_mat.where(valid)
        y = shifted_mat.where(valid)
        t_ra_unstack += time.time() - t0

        t0 = time.time()
        sum_x = x.sum(axis=1)
        sum_y = y.sum(axis=1)
        sum_xy = (x * y).sum(axis=1)
        sum_x2 = (x ** 2).sum(axis=1)
        sum_y2 = (y ** 2).sum(axis=1)
        n = n_valid
        numerator = n * sum_xy - sum_x * sum_y
        denom_x = n * sum_x2 - sum_x ** 2
        denom_y = n * sum_y2 - sum_y ** 2
        with np.errstate(invalid="ignore", divide="ignore"):
            denom = np.sqrt(denom_x * denom_y)
            autocorr = numerator / denom
        autocorr = pd.Series(autocorr, index=ranks_mat.index, dtype=float)
        autocorr.iloc[:1] = np.nan
        autocorr[n < 2] = np.nan
        autocorr = autocorr.replace([np.inf, -np.inf], np.nan)
        t_ra_pearson += time.time() - t0
        autocorr_chunks.append(autocorr)

        if (i + 1) % 20 == 0 or i == n_chunks - 1:
            print(f"    chunk {i+1}/{n_chunks} done")

    # merge
    t0 = time.time()
    turnover_merged = merge_chunk_results(turnover_chunks, "turnover")
    rank_autocorr_merged = merge_chunk_results(autocorr_chunks, "rank_autocorr")
    t_merge_to = time.time() - t0

    t_turnover_total = time.time() - t_turnover_start
    timer.record("4. run_turnover (total)", t_turnover_total,
                 {"detail": f"turnover+autocorr × {n_chunks} chunks"})
    timer.record("  4a. calc_turnover: unstack+shift", t_to_unstack,
                 {"detail": f"labels.unstack × {n_chunks}"})
    timer.record("  4b. calc_turnover: group loop", t_to_loop,
                 {"detail": f"5 groups × bool ops × {n_chunks}"})
    timer.record("  4c. calc_rank_autocorr: groupby.rank", t_ra_rank,
                 {"detail": f"groupby.rank × {n_chunks}"})
    timer.record("  4d. calc_rank_autocorr: unstack+shift", t_ra_unstack,
                 {"detail": f"ranks.unstack × {n_chunks}"})
    timer.record("  4e. calc_rank_autocorr: numpy Pearson", t_ra_pearson,
                 {"detail": f"{n_chunks} chunks"})
    timer.record("  4f. turnover merge", t_merge_to,
                 {"detail": "concat + boundary NaN"})
    del factor_chunks, label_chunks, turnover_chunks, autocorr_chunks
    gc.collect()
    print(f"    run_turnover done: {t_turnover_total:.1f}s")

    # ====================================================================
    # 5. run_neutralize — calc_neutralized_curve per chunk
    # ====================================================================
    print(f"\n{'─' * 75}")
    print(f"  [5] run_neutralize — neutralized curve")
    print(f"{'─' * 75}")

    t_neutral_start = time.time()
    factor_chunks = split_into_chunks(factor, CHUNK_SIZE)
    returns_chunks = split_into_chunks(ret, CHUNK_SIZE)
    label_chunks = split_into_chunks(group_labels, CHUNK_SIZE)

    t_neu_df_build = 0.0
    t_neu_demean = 0.0
    t_neu_qg = 0.0
    t_neu_portfolio = 0.0
    neutralized_chunks = []

    for i, (fc, rc, lbc) in enumerate(zip(factor_chunks, returns_chunks, label_chunks)):
        # DataFrame build
        t0 = time.time()
        df = pd.DataFrame({
            "factor": fc,
            "returns": rc,
            "group": lbc,
        })
        t_neu_df_build += time.time() - t0

        # demean
        t0 = time.time()
        valid_group = df["group"].notna() & np.isfinite(df["group"])
        valid_factor = df["factor"].notna() & np.isfinite(df["factor"])
        valid_returns = df["returns"].notna() & np.isfinite(df["returns"])
        mask_for_demean = valid_group & valid_factor
        if mask_for_demean.sum() > 0:
            group_mean = (
                df.loc[mask_for_demean]
                .groupby([pd.Grouper(level=0), "group"])["factor"]
                .transform("mean")
            )
            df.loc[mask_for_demean, "factor"] = df.loc[mask_for_demean, "factor"] - group_mean
        t_neu_demean += time.time() - t0

        # quantile_group on neutralized factor
        t0 = time.time()
        labels = quantile_group(df["factor"], n_groups=5)
        df["label"] = labels
        t_neu_qg += time.time() - t0

        # calc_portfolio_curves (reuse)
        t0 = time.time()
        _, _, hedge_curve = calc_portfolio_curves(
            df["factor"], df["returns"],
            n_groups=5, top_k=1, bottom_k=1,
            rebalance_freq=1, _raw=True, group_labels=df["label"],
        )
        t_neu_portfolio += time.time() - t0
        neutralized_chunks.append(hedge_curve)

        if (i + 1) % 20 == 0 or i == n_chunks - 1:
            print(f"    chunk {i+1}/{n_chunks} done")

    # merge
    t0 = time.time()
    neutralized_curve = _merge_raw_curves(neutralized_chunks)
    if len(neutralized_curve) > 0:
        neutralized_curve.iloc[0] = 1.0
    t_merge_neu = time.time() - t0

    t_neutral_total = time.time() - t_neutral_start
    timer.record("5. run_neutralize (total)", t_neutral_total,
                 {"detail": f"neutralize × {n_chunks} chunks"})
    timer.record("  5a. DataFrame build", t_neu_df_build,
                 {"detail": f"3-col DataFrame × {n_chunks}"})
    timer.record("  5b. demean (groupby transform)", t_neu_demean,
                 {"detail": f"groupby.transform(mean) × {n_chunks}"})
    timer.record("  5c. quantile_group (re-ranking)", t_neu_qg,
                 {"detail": f"qcut on neutralized factor × {n_chunks}"})
    timer.record("  5d. calc_portfolio_curves", t_neu_portfolio,
                 {"detail": f"groupby.apply × {n_chunks}"})
    timer.record("  5e. merge curves", t_merge_neu,
                 {"detail": "scale+concat"})
    del factor_chunks, returns_chunks, label_chunks, neutralized_chunks
    gc.collect()
    print(f"    run_neutralize done: {t_neutral_total:.1f}s")

    # ====================================================================
    # 额外: split_into_chunks 总耗时（每个 step 都调用）
    # ====================================================================
    t0 = time.time()
    for _ in range(4):  # metrics/grouping/curves/turnover/neutralize 各调一次
        split_into_chunks(factor, CHUNK_SIZE)
        split_into_chunks(ret, CHUNK_SIZE)
        split_into_chunks(group_labels, CHUNK_SIZE)
    t_split_total = time.time() - t0
    timer.record("X. split_into_chunks (overhead)", t_split_total,
                 {"detail": f"called ~10× across all steps"})

    # ====================================================================
    # 报告输出 / Report
    # ====================================================================
    t_eval_total = t_metrics_total + t_grouping_total + t_curves_total + t_turnover_total + t_neutral_total

    print(f"\n{timer.report()}")
    print(f"\n  NOTE: 1b and 1e share timing (same numpy Pearson pattern)")
    print(f"  NOTE: X (split_into_chunks) is overhead called by each step")

    # ====================================================================
    # 瓶颈分析 / Bottleneck Analysis
    # ====================================================================
    # 收集关键内部操作耗时
    internal_ops = {
        "unstack (all)": t_unstack_ic + t_unstack_ric + t_to_unstack + t_ra_unstack,
        "rank(axis=1)": t_rank_op,
        "groupby.apply (portfolio)": t_groupby_apply + t_neu_portfolio,
        "groupby.rank (autocorr)": t_ra_rank,
        "groupby.apply (quantile_group)": t_qg_total + t_neu_qg,
        "groupby.transform (demean)": t_neu_demean,
        "numpy Pearson (all)": t_numpy_pearson + t_ra_pearson,
        "DataFrame construction": t_df_build + t_neu_df_build,
    }

    print(f"\n{'=' * 75}")
    print(f"  Bottleneck Analysis — Internal Operations")
    print(f"{'=' * 75}")
    sorted_ops = sorted(internal_ops.items(), key=lambda x: -x[1])
    for name, t in sorted_ops:
        pct = t / t_eval_total * 100 if t_eval_total > 0 else 0
        bar = "█" * int(pct / 2)
        print(f"  {name:<40} {t:>8.2f}s  {pct:>5.1f}%  {bar}")

    print(f"\n{'=' * 75}")
    print(f"  Summary")
    print(f"{'=' * 75}")
    print(f"  Data:       {n_rows:,} rows × {n_symbols} symbols × {n_ts} timestamps")
    print(f"  Chunks:     {n_chunks} (chunk_size={CHUNK_SIZE})")
    print(f"  Eval Total: {t_eval_total:.1f}s ({t_eval_total/60:.1f}min)")
    print(f"  Top 3 bottlenecks:")
    for j, (name, t) in enumerate(sorted_ops[:3]):
        pct = t / t_eval_total * 100
        print(f"    {j+1}. {name}: {t:.1f}s ({pct:.1f}%)")
    print(f"{'=' * 75}")


if __name__ == "__main__":
    main()
