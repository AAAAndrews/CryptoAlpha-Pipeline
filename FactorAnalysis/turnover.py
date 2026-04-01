"""
FactorAnalysis/turnover.py — 分组换手率与因子排名自相关 / Turnover & Rank Autocorrelation

分组换手率衡量相邻时间截面上各分组内成员的变动比例，评估信号稳定性。
因子排名自相关衡量横截面排名在时间维度上的持续性，评估因子衰减速度。
Quantile turnover measures member changes in groups between consecutive periods.
Rank autocorrelation measures persistence of cross-sectional ranks over time.
"""

import numpy as np
import pandas as pd

from .grouping import quantile_group


def calc_turnover(factor: pd.Series, n_groups: int = 5) -> pd.DataFrame:
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

    # 获取分组标签 / get group labels
    labels = quantile_group(factor, n_groups=n_groups)

    # 对每个分组计算换手率 / compute turnover for each group
    results = {}
    for g in range(n_groups):
        # 构建二元矩阵：该资产是否在该分组 / binary matrix: is asset in this group
        in_group = (labels == g).astype(np.float64)
        # unstack: 行=timestamp, 列=symbol / rows=timestamp, cols=symbol
        matrix = in_group.unstack(level=1)

        # 当前分组内的资产数 / number of assets in group at current period
        current_count = matrix.sum(axis=1)

        # 上一期的分组矩阵（行方向移位）/ previous period's matrix (row shift)
        shifted = matrix.shift(1)

        # 两期都在分组内的资产数 / assets in group at both periods
        overlap = (matrix * shifted).sum(axis=1)

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
    值域 [-1, 1]，越接近 1 说明因子排名越稳定（衰减越慢）。
    Rank factor values within each cross-section, then compute Pearson
    correlation between consecutive periods' rank vectors.
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

    # 按时间排序的截面列表 / sorted time cross-sections
    timestamps = sorted(ranks.index.get_level_values(0).unique())

    if len(timestamps) <= lag:
        # 时间截面不足以计算自相关 / not enough periods for autocorrelation
        return pd.Series(np.nan, index=timestamps, dtype=np.float64)

    autocorr = pd.Series(np.nan, index=timestamps, dtype=np.float64)

    for i in range(lag, len(timestamps)):
        t_curr = timestamps[i]
        t_prev = timestamps[i - lag]

        # 取两个截面的排名值 / get rank values for both periods
        curr = ranks.xs(t_curr, level=0).dropna()
        prev = ranks.xs(t_prev, level=0).dropna()

        # 仅计算共有的资产 / only compute for common assets
        common = curr.index.intersection(prev.index)

        if len(common) < 2:
            autocorr[t_curr] = np.nan
        else:
            autocorr[t_curr] = curr[common].corr(prev[common])

    return autocorr
