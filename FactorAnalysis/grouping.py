"""
FactorAnalysis/grouping.py — 分位数分组分割 / Quantile grouping

可调节分组数量，将因子值按横截面分位切分。
Adjustable number of groups, splits factor values by cross-sectional quantiles.
"""

import numpy as np
import pandas as pd


def quantile_group(factor: pd.Series, n_groups: int = 5) -> pd.Series:
    """
    按横截面分位数将因子值分组 / Group factor values by cross-sectional quantiles.

    每个时间截面上，根据因子值从小到大分成 n_groups 组，
    组标签为 0（最低）到 n_groups-1（最高）。
    At each time cross-section, split assets into n_groups by factor value,
    group labels range from 0 (lowest) to n_groups-1 (highest).

    Parameters / 参数:
        factor: 因子值，MultiIndex (timestamp, symbol)
                Factor values, MultiIndex (timestamp, symbol)
        n_groups: 分组数量，默认 5
                  Number of groups, default 5

    Returns / 返回:
        pd.Series: 分组标签，与 factor 同索引（NaN 因子值保留 NaN 标签）
                   Group labels, same index as factor (NaN factor values keep NaN label)
    """
    if n_groups < 2:
        raise ValueError(f"n_groups 必须 >= 2，当前值: {n_groups}")

    # 过滤无效值用于分位数计算 / filter invalid values for quantile computation
    valid_mask = factor.notna() & np.isfinite(factor)
    labels = pd.Series(np.nan, index=factor.index, dtype=np.float64)

    if valid_mask.sum() == 0:
        return labels

    # 对每个时间截面做分位数分组 / quantile grouping per time cross-section
    factor_valid = factor[valid_mask]

    def _assign_group(g: pd.Series) -> pd.Series:
        """在单个截面内按分位数赋组 / Assign quantile groups within one cross-section."""
        try:
            # qcut 按值均匀分箱，duplicates='drop' 处理重复值
            # qcut splits by equal frequency, duplicates='drop' handles tied values
            bins = pd.qcut(g, q=n_groups, labels=False, duplicates="drop")
            return bins
        except ValueError:
            # 样本过少无法分箱时全部归为中间组 / all to middle group when too few samples
            return pd.Series(np.full(len(g), n_groups // 2), index=g.index)

    group_result = factor_valid.groupby(level=0, group_keys=False).apply(_assign_group)

    # 写回标签 / write back labels
    labels.loc[group_result.index] = group_result

    return labels
