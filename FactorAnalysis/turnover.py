"""
FactorAnalysis/turnover.py — 分组换手率与因子排名自相关 / Turnover & Rank Autocorrelation

分组换手率衡量相邻时间截面上各分组内成员的变动比例，评估信号稳定性。
因子排名自相关衡量横截面排名在时间维度上的持续性，评估因子衰减速度。
Quantile turnover measures member changes in groups between consecutive periods.
Rank autocorrelation measures persistence of cross-sectional ranks over time.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from .grouping import quantile_group


def calc_turnover(
    factor: pd.Series,
    n_groups: int = 5,
    group_labels: pd.Series | None = None,
) -> pd.DataFrame:
    """
    计算分组换手率 / Calculate quantile group turnover.

    衡量相邻时间截面上，各分组内成员的变动比例。
    turnover_g(t) = 1 - |group_g(t) ∩ group_g(t-1)| / |group_g(t)|
    值域 [0, 1]，值越大说明分组越不稳定。
    Measure the fraction of members that changed in each group between consecutive periods.
    Value range [0, 1], higher values indicate less stable grouping.

    Parameters / 参数:
        factor: 因子值，MultiIndex (timestamp, symbol)
                Factor values, MultiIndex (timestamp, symbol)
        n_groups: 分组数量，默认 5
                  Number of groups, default 5
        group_labels: 预计算分组标签，传入时跳过内部 quantile_group
                      Pre-computed group labels; skip quantile_group when provided

    Returns / 返回:
        pd.DataFrame: columns 为分组标签 (0 ~ n_groups-1)，index 为 timestamp。
                      首个时间截面无前序，值为 NaN。
                      DataFrame with group labels as columns, timestamp as index.
                      First period has no predecessor, value is NaN.

    Raises / 异常:
        TypeError: factor 不是 pd.Series
        ValueError: n_groups < 2 或数据不足
    """
    # 参数校验 / parameter validation
    if not isinstance(factor, pd.Series):
        raise TypeError(
            f"factor must be pd.Series, got {type(factor).__name__}"
            f"\nfactor 必须是 pd.Series，收到 {type(factor).__name__}。"
        )
    if n_groups < 2:
        raise ValueError(f"n_groups 必须 >= 2，当前值: {n_groups}")

    if len(factor) == 0:
        raise ValueError("factor 不能为空 / factor must not be empty")

    # 获取分组标签：优先使用预计算，否则内部计算 / get labels: prefer pre-computed, else compute
    labels = group_labels if group_labels is not None else quantile_group(factor, n_groups=n_groups)

    # unstack 一次：行=timestamp, 列=symbol / unstack once: rows=timestamp, cols=symbol
    labels_mat = labels.unstack(level=1)
    # 上一期的标签矩阵（行方向移位）/ previous period's label matrix (row shift)
    shifted = labels_mat.shift(1)

    # 对每个分组计算换手率 / compute turnover for each group
    results = {}
    for g in range(n_groups):
        # 当前期是否在该分组 / is in group at current period
        current = (labels_mat == g)
        # 上一期是否在该分组 / was in group at previous period
        prev = (shifted == g)

        # 当前分组内的资产数 / number of assets in group at current period
        current_count = current.sum(axis=1)

        # 两期都在分组内的资产数 / assets in group at both periods
        overlap = (current & prev).sum(axis=1)

        # 换手率 = 1 - 重叠比例 / turnover = 1 - overlap ratio
        turnover = 1.0 - overlap / current_count
        # 当前分组无资产时设为 NaN / set NaN when no assets in group
        turnover[current_count == 0] = np.nan
        # 首期无前序，强制为 NaN / first period has no predecessor, force NaN
        turnover.iloc[0] = np.nan

        results[g] = turnover

    return pd.DataFrame(results)


def calc_rank_autocorr(factor: pd.Series, lag: int = 1) -> pd.Series:
    """
    计算因子排名自相关系数 / Calculate factor rank autocorrelation.

    对每个时间截面内的因子值做横截面排名（rank），
    然后计算相邻时间截面排名向量的 Pearson 相关系数。
    向量化实现：unstack 为 2D 矩阵后 numpy 批量行级 Pearson 相关，替代逐截面 xs+corr 循环。
    值域 [-1, 1]，越接近 1 说明因子排名越稳定（衰减越慢）。
    Rank factor values within each cross-section, then compute Pearson
    correlation between consecutive periods' rank vectors.
    Vectorized: unstack to 2D matrix, numpy batch row-level Pearson, replacing xs+corr loop.
    Value range [-1, 1], closer to 1 means more persistent ranks.

    Parameters / 参数:
        factor: 因子值，MultiIndex (timestamp, symbol)
                Factor values, MultiIndex (timestamp, symbol)
        lag: 滞后期数，默认 1 / Lag periods, default 1

    Returns / 返回:
        pd.Series: 每个时间截面（除前 lag 期）的自相关系数，index 为 timestamp。
                   Series of autocorrelation per timestamp (first lag periods are NaN).
    """
    # 参数校验 / parameter validation
    if not isinstance(factor, pd.Series):
        raise TypeError(
            f"factor must be pd.Series, got {type(factor).__name__}"
            f"\nfactor 必须是 pd.Series，收到 {type(factor).__name__}。"
        )
    if lag < 1:
        raise ValueError(f"lag 必须 >= 1，当前值: {lag}")

    if len(factor) == 0:
        raise ValueError("factor 不能为空 / factor must not be empty")

    # 横截面排名 / cross-sectional ranking
    ranks = factor.groupby(level=0, group_keys=False).rank()

    # unstack 为 2D 矩阵 (timestamp × symbol) / unstack to 2D matrix
    ranks_mat = ranks.unstack(level=1)
    timestamps = ranks_mat.index

    if len(timestamps) <= lag:
        # 时间截面不足以计算自相关 / not enough periods for autocorrelation
        return pd.Series(np.nan, index=timestamps, dtype=np.float64)

    # 滞后矩阵：将排名矩阵按行下移 lag 行 / shifted matrix: shift rows down by lag
    shifted_mat = ranks_mat.shift(lag)

    # 有效值掩码：当前行和滞后行均非 NaN / valid mask: both current and lagged are non-NaN
    valid = ranks_mat.notna() & shifted_mat.notna()
    n_valid = valid.sum(axis=1).astype(float)

    # 将无效值置 NaN，sum(skipna=True) 自动跳过 / set invalid to NaN, sum skips them
    x = ranks_mat.where(valid)
    y = shifted_mat.where(valid)

    # 向量化 Pearson 公式 / vectorized Pearson formula
    # r = (n*Σxy - Σx*Σy) / sqrt((n*Σx² - (Σx)²)(n*Σy² - (Σy)²))
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

    autocorr = pd.Series(autocorr, index=timestamps, dtype=float)

    # 前 lag 期无前序，强制 NaN / first lag periods have no predecessor, force NaN
    autocorr.iloc[:lag] = np.nan

    # 有效数不足或分母为零返回 NaN / insufficient data or zero denom → NaN
    autocorr[n < 2] = np.nan
    autocorr = autocorr.replace([np.inf, -np.inf], np.nan)

    return autocorr
