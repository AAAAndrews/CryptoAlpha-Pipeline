"""
FactorAnalysis/grouping.py — 分位数分组分割 / Quantile grouping

可调节分组数量，将因子值按横截面分位切分。
Adjustable number of groups, splits factor values by cross-sectional quantiles.
"""

import numpy as np
import pandas as pd


def quantile_group(factor: pd.Series, n_groups: int = 5, zero_aware: bool = False) -> pd.Series:
    """
    按横截面分位数将因子值分组 / Group factor values by cross-sectional quantiles.

    每个时间截面上，根据因子值从小到大分成 n_groups 组，
    组标签为 0（最低）到 n_groups-1（最高）。
    At each time cross-section, split assets into n_groups by factor value,
    group labels range from 0 (lowest) to n_groups-1 (highest).

    当 zero_aware=True 时，按正负拆分后各自做分位数分组：
    负值（含零）获得较低标签，正值获得较高标签，
    按两侧样本量比例分配分组数。
    When zero_aware=True, split by sign and group separately:
    negative values (including zero) get lower labels, positives get higher labels,
    groups allocated proportionally by sample count on each side.

    Parameters / 参数:
        factor: 因子值，MultiIndex (timestamp, symbol)
                Factor values, MultiIndex (timestamp, symbol)
        n_groups: 分组数量，默认 5
                  Number of groups, default 5
        zero_aware: 是否按正负拆分分组，默认 False
                    Whether to split by sign before grouping, default False

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

    def _assign_zero_aware(g: pd.Series) -> pd.Series:
        """零值感知分组：正负拆分后各自分位数分组 / Zero-aware: split by sign then quantile group."""
        neg_mask = g <= 0
        pos_mask = g > 0
        n_neg = neg_mask.sum()
        n_pos = pos_mask.sum()

        # 某一侧为空时退化为普通分组 / fall back to standard if one side is empty
        if n_neg == 0 or n_pos == 0:
            return _assign_group(g)

        total = n_neg + n_pos

        # 按样本量比例分配分组数，每侧至少 1 组
        # allocate groups proportionally, at least 1 per side
        n_neg_groups = max(1, round(n_groups * n_neg / total))
        n_pos_groups = n_groups - n_neg_groups
        if n_pos_groups < 1:
            n_pos_groups = 1
            n_neg_groups = n_groups - n_pos_groups

        result = pd.Series(np.nan, index=g.index, dtype=np.float64)

        # 负值（含零）分组 / negative (including zero) grouping
        neg_vals = g[neg_mask]
        try:
            neg_bins = pd.qcut(neg_vals, q=n_neg_groups, labels=False, duplicates="drop")
            result.loc[neg_vals.index] = neg_bins
        except ValueError:
            result.loc[neg_vals.index] = np.full(n_neg, n_neg_groups // 2)

        # 正值分组，标签偏移 n_neg_groups / positive grouping, offset labels
        pos_vals = g[pos_mask]
        try:
            pos_bins = pd.qcut(pos_vals, q=n_pos_groups, labels=False, duplicates="drop")
            result.loc[pos_vals.index] = pos_bins + n_neg_groups
        except ValueError:
            result.loc[pos_vals.index] = np.full(n_pos, n_neg_groups + n_pos_groups // 2)

        return result

    _fn = _assign_zero_aware if zero_aware else _assign_group
    group_result = factor_valid.groupby(level=0, group_keys=False).apply(_fn)

    # 写回标签 / write back labels
    labels.loc[group_result.index] = group_result

    return labels
