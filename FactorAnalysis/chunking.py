"""
FactorAnalysis/chunking.py — 分块处理核心逻辑 / Chunked Processing Core Logic

时间分块与结果汇总，用于大样本内存优化。
Time-based chunking and result aggregation for memory optimization on large samples.
"""

import logging
import tracemalloc

import numpy as np
import pandas as pd

try:
    import psutil
    _HAS_PSUTIL = True
except ImportError:
    _HAS_PSUTIL = False

logger = logging.getLogger(__name__)


def split_into_chunks(
    data: "pd.Series | pd.DataFrame",
    chunk_size: int,
    rebalance_freq: int = 1,
) -> list:
    """
    按时间戳分块，边界对齐 rebalance 频率 / Split data into time-based chunks with rebalance alignment.

    将 MultiIndex (timestamp, symbol) 的数据按时间维度切分为多个子集。
    当 rebalance_freq > 1 时，自动将 chunk_size 向上取整至 rebalance_freq 的整数倍，
    确保分块边界不会切断调仓周期，避免跨块信号丢失。

    Split MultiIndex (timestamp, symbol) data along the time dimension.
    When rebalance_freq > 1, chunk_size is rounded up to the nearest multiple
    of rebalance_freq so that chunk boundaries never split a rebalance period.

    Parameters / 参数:
        data: 输入数据，pd.Series 或 pd.DataFrame，MultiIndex (timestamp, symbol)
              Input data with MultiIndex (timestamp, symbol)
        chunk_size: 每块的时间截面数量 / Number of timestamps per chunk
        rebalance_freq: 调仓频率（每 N 个截面调仓一次），默认 1
                        Rebalance frequency (rebalance every N cross-sections), default 1

    Returns / 返回:
        list[pd.Series | pd.DataFrame]: 分块数据列表 / List of data chunks

    Raises / 异常:
        ValueError: chunk_size < 1 或 rebalance_freq < 1
    """
    if chunk_size < 1:
        raise ValueError(f"chunk_size 必须 >= 1，当前值: {chunk_size}")
    if rebalance_freq < 1:
        raise ValueError(f"rebalance_freq 必须 >= 1，当前值: {rebalance_freq}")

    # 获取所有唯一时间戳并排序 / get unique timestamps sorted
    timestamps = data.index.get_level_values(0).unique().sort_values()
    n_ts = len(timestamps)

    if n_ts == 0:
        return []

    # 单块即可覆盖全部数据 / single chunk covers all data
    if chunk_size >= n_ts:
        return [data]

    # 当 rebalance_freq > 1 时，向上取整 chunk_size 以对齐调仓周期
    # round up chunk_size to align with rebalance periods when rebalance_freq > 1
    effective_size = chunk_size
    if rebalance_freq > 1:
        remainder = effective_size % rebalance_freq
        if remainder != 0:
            effective_size = effective_size + (rebalance_freq - remainder)

    # 生成分块起始索引 / generate chunk start indices
    starts = list(range(0, n_ts, effective_size))

    chunks = []
    for i, start_idx in enumerate(starts):
        end_idx = starts[i + 1] if i + 1 < len(starts) else n_ts
        chunk_ts = timestamps[start_idx:end_idx]
        mask = data.index.get_level_values(0).isin(chunk_ts)
        chunks.append(data.loc[mask])

    return chunks


def merge_chunk_results(
    chunk_results: list,
    metric_type: str,
) -> "pd.Series | pd.DataFrame":
    """
    汇总聚合分块计算结果 / Aggregate chunked computation results.

    根据 metric_type 选择不同的汇总策略：
    - ic: IC/RankIC 序列直接拼接（各时间截面 IC 值相互独立）
    - curve: 净值曲线缩放拼接，保持 cumprod 衔接连续性
    - turnover: 换手率 DataFrame 拼接，跨块边界首行设为 NaN
    - rank_autocorr: 排名自相关拼接，跨块边界首值设为 NaN
    - ic_stats: IC 统计量按样本量加权平均（需包含 count 字段）

    Select aggregation strategy based on metric_type:
    - ic: concatenate IC/RankIC series (IC values are independent per timestamp)
    - curve: scale + concatenate equity curves for cumprod continuity
    - turnover: concat turnover DataFrames, first row of subsequent chunks set to NaN
    - rank_autocorr: concat rank autocorrelation, first value of subsequent chunks set to NaN
    - ic_stats: weighted average of IC statistics by sample count (requires 'count' field)

    Parameters / 参数:
        chunk_results: 分块计算结果列表 / List of per-chunk computation results
        metric_type: 指标类型 / Metric type.
            可选值 / Valid values: "ic", "ic_stats", "curve", "turnover", "rank_autocorr"

    Returns / 返回:
        pd.Series | pd.DataFrame: 汇总后的结果 / Aggregated result

    Raises / 异常:
        ValueError: metric_type 不是支持的类型 / Unsupported metric_type
    """
    if not chunk_results:
        if metric_type == "turnover":
            return pd.DataFrame()
        return pd.Series(dtype=float)

    if metric_type == "ic":
        return pd.concat(chunk_results)

    elif metric_type == "ic_stats":
        return _merge_ic_stats(chunk_results)

    elif metric_type == "curve":
        return _merge_curves(chunk_results)

    elif metric_type == "turnover":
        return _merge_turnover(chunk_results)

    elif metric_type == "rank_autocorr":
        return _merge_rank_autocorr(chunk_results)

    else:
        raise ValueError(
            f"Unknown metric_type: {metric_type}. "
            f"Expected one of: ic, ic_stats, curve, turnover, rank_autocorr"
        )


# ============================================================
# 内部汇总函数 / Internal merge helpers
# ============================================================


def _merge_curves(chunk_results: list[pd.Series]) -> pd.Series:
    """
    缩放拼接净值曲线，保持 cumprod 衔接连续性 / Scale and concat equity curves for continuity.

    每块净值曲线起始值为 1.0，后续块的曲线需乘以前一块末尾值以保持连续。
    Each chunk's curve starts at 1.0; subsequent chunks are scaled by
    the previous chunk's last value to maintain continuity.
    """
    merged = chunk_results[0].copy()
    for i in range(1, len(chunk_results)):
        # 前一块末尾的净值 / last equity value of previous chunk
        scale = merged.iloc[-1]
        chunk = chunk_results[i]
        if len(chunk) > 1:
            # 跳过首元素（起始值 1.0），缩放后续值 / skip first element (1.0), scale rest
            scaled = chunk.iloc[1:] * scale
            merged = pd.concat([merged, scaled])
    return merged


def _merge_turnover(chunk_results: list[pd.DataFrame]) -> pd.DataFrame:
    """
    拼接换手率，跨块边界首行设为 NaN / Concat turnover, boundary first row set to NaN.

    跨块边界的换手率无法直接计算（缺少前一块末尾的截面数据），
    因此将后续每块的首行标记为 NaN。
    Cross-chunk boundary turnover cannot be computed (missing predecessor
    cross-section), so the first row of each subsequent chunk is set to NaN.
    """
    chunks = []
    for i, chunk in enumerate(chunk_results):
        c = chunk.copy()
        if i > 0 and len(c) > 0:
            # 跨块边界：无前序截面数据 / cross-chunk boundary: no predecessor
            c.iloc[0] = np.nan
        chunks.append(c)
    return pd.concat(chunks)


def _merge_rank_autocorr(chunk_results: list[pd.Series]) -> pd.Series:
    """
    拼接排名自相关，跨块边界首值设为 NaN / Concat rank autocorr, boundary first value set to NaN.

    跨块边界的排名自相关需要跨块比较排名，无法在分块内计算，标记为 NaN。
    Cross-chunk boundary rank autocorrelation requires cross-chunk comparison,
    which cannot be computed within a chunk, so it's marked as NaN.
    """
    chunks = []
    for i, chunk in enumerate(chunk_results):
        c = chunk.copy()
        if i > 0 and len(c) > 0:
            c.iloc[0] = np.nan
        chunks.append(c)
    return pd.concat(chunks)


def _merge_ic_stats(chunk_results: list) -> pd.Series:
    """
    按样本量加权平均合并 IC 统计量 / Weighted average merge of IC statistics by sample count.

    使用合并方差公式 (pooled variance) 计算整体 IC 标准差：
    Var_pooled = E[X^2] - E[X]^2，其中 E[X] 和 E[X^2] 均按样本量加权。

    Uses pooled variance formula to compute overall IC standard deviation:
    Var_pooled = E[X^2] - E[X]^2, where E[X] and E[X^2] are weighted by sample count.

    Each chunk_result must be a dict or pd.Series with keys:
        IC_mean, IC_std, count (number of valid IC observations in the chunk)

    Returns / 返回:
        pd.Series: 包含 IC_mean, IC_std, ICIR / Contains IC_mean, IC_std, ICIR
    """
    total_count = 0
    weighted_mean_sum = 0.0
    weighted_var_sum = 0.0

    for chunk in chunk_results:
        if isinstance(chunk, pd.Series):
            ic_mean = float(chunk.get("IC_mean", np.nan))
            ic_std = float(chunk.get("IC_std", np.nan))
            count = float(chunk.get("count", np.nan))
        elif isinstance(chunk, dict):
            ic_mean = chunk.get("IC_mean", np.nan)
            ic_std = chunk.get("IC_std", np.nan)
            count = chunk.get("count", np.nan)
        else:
            raise TypeError(
                f"ic_stats chunk must be dict or pd.Series, got {type(chunk).__name__}"
            )

        # 跳过无效块 / skip invalid chunks
        if (
            np.isnan(ic_mean) if isinstance(ic_mean, float) else False
        ) or (
            np.isnan(ic_std) if isinstance(ic_std, float) else False
        ) or (
            np.isnan(count) if isinstance(count, float) else False
        ) or count <= 0:
            continue

        total_count += count
        weighted_mean_sum += ic_mean * count
        weighted_var_sum += count * (ic_std ** 2 + ic_mean ** 2)

    if total_count == 0:
        return pd.Series({"IC_mean": np.nan, "IC_std": np.nan, "ICIR": np.nan})

    # 加权均值 / weighted mean
    overall_mean = weighted_mean_sum / total_count
    # 合并方差 / pooled variance: E[X^2] - E[X]^2
    overall_var = weighted_var_sum / total_count - overall_mean ** 2
    overall_std = np.sqrt(max(overall_var, 0.0))
    overall_icir = overall_mean / overall_std if overall_std > 0 else np.nan

    return pd.Series({
        "IC_mean": overall_mean,
        "IC_std": overall_std,
        "ICIR": overall_icir,
    })


# ============================================================
# 内存监控 / Memory monitoring
# ============================================================


class ChunkMemoryTracker:
    """
    分块内存监控上下文管理器 / Chunk memory tracking context manager.

    使用 tracemalloc 追踪每块的 Python 堆内存峰值，
    可选使用 psutil 获取进程 RSS 内存，结果写入 logger。
    Tracks peak Python heap memory per chunk via tracemalloc,
    optionally gets process RSS via psutil, logs results.

    Parameters / 参数:
        chunk_idx: 当前块索引（从 0 开始）/ Current chunk index (0-based)
        total_chunks: 总块数 / Total number of chunks
        description: 操作描述，用于日志 / Operation description for logging

    Attributes / 属性:
        peak_mb: 本块 tracemalloc 峰值内存 (MB) / tracemalloc peak memory (MB)
        rss_mb: 本块结束时进程 RSS 内存 (MB)，需 psutil / Process RSS at chunk end (MB), requires psutil

    Example / 示例:
        with ChunkMemoryTracker(0, 5, description="run_metrics"):
            result = process_chunk(data)
        # logger.info: [chunk 1/5] run_metrics | peak_alloc=12.34 MB, RSS=45.67 MB
    """

    def __init__(self, chunk_idx: int, total_chunks: int, description: str = "chunk"):
        self.chunk_idx = chunk_idx
        self.total_chunks = total_chunks
        self.description = description
        self.peak_mb: float | None = None
        self.rss_mb: float | None = None

    def __enter__(self) -> "ChunkMemoryTracker":
        # 确保 tracemalloc 已启动 / ensure tracemalloc is running
        if not tracemalloc.is_tracing():
            tracemalloc.start()
        tracemalloc.reset_peak()
        return self

    def __exit__(self, *exc) -> None:
        _, peak = tracemalloc.get_traced_memory()
        self.peak_mb = peak / 1024 / 1024

        msg = (
            f"[chunk {self.chunk_idx + 1}/{self.total_chunks}] "
            f"{self.description} | "
            f"peak_alloc={self.peak_mb:.2f} MB"
        )

        if _HAS_PSUTIL:
            self.rss_mb = psutil.Process().memory_info().rss / 1024 / 1024
            msg += f", RSS={self.rss_mb:.2f} MB"

        logger.info(msg)
