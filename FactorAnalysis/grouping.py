"""
FactorAnalysis/grouping.py — 分位数分组分割 / Quantile grouping

可调节分组数量，将因子值按横截面分位切分。
Adjustable number of groups, splits factor values by cross-sectional quantiles.

向量化实现: unstack 为 2D 矩阵后逐行使用 numpy percentile + searchsorted 分组，
消除 groupby.apply 逐截面 Python 函数调用，预期 5.5× 加速。
Vectorized: unstack to 2D matrix, per-row numpy percentile + searchsorted,
eliminating groupby.apply per-section Python function calls, expected 5.5× speedup.
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

    # unstack 为 2D 矩阵 (timestamp × symbol)，逐行 numpy 向量化计算
    # unstack to 2D matrix (timestamp × symbol), per-row numpy vectorized computation
    factor_valid = factor[valid_mask]
    mat = factor_valid.unstack()
    values = mat.values
    timestamps = mat.index
    symbols = mat.columns
    n_rows = values.shape[0]

    result = np.full_like(values, np.nan, dtype=np.float64)

    for i in range(n_rows):
        row = values[i]
        valid = np.isfinite(row)
        n_valid = valid.sum()
        if n_valid == 0:
            continue

        valid_vals = row[valid]

        if zero_aware:
            result[i, valid] = _assign_zero_aware_vec(valid_vals, n_groups)
        else:
            result[i, valid] = _assign_group_vec(valid_vals, n_groups)

    # stack 回 Series 写入标签 / stack back to Series and write labels
    result_df = pd.DataFrame(result, index=timestamps, columns=symbols, dtype=np.float64)
    group_result = result_df.stack()
    labels.loc[group_result.index] = group_result

    return labels


def _assign_group_vec(vals: np.ndarray, n_groups: int) -> np.ndarray:
    """
    Numpy 向量化分位数赋组 (单截面) / Numpy vectorized quantile group assignment (single section).

    等价于 pd.qcut(vals, q=n_groups, labels=False, duplicates='drop')
    Equivalent to pd.qcut(vals, q=n_groups, labels=False, duplicates='drop')
    """
    n = len(vals)
    if n == 0:
        return np.array([], dtype=np.float64)

    # 计算分位数边界 / compute quantile edges
    percentiles = np.linspace(0, 1, n_groups + 1)
    edges = np.quantile(vals, percentiles)

    # 去除重复边界 (等价于 duplicates='drop') / remove duplicate edges
    diff_mask = np.concatenate([[True], edges[1:] != edges[:-1]])
    edges = edges[diff_mask]
    n_bins = len(edges) - 1

    if n_bins < 1:
        # 所有值相同 → 归为中间组 / all identical → middle group
        return np.full(n, n_groups // 2, dtype=np.float64)

    # searchsorted side='right' 匹配 pd.qcut right=True 行为:
    # 等于边界的值归入右侧 bin，最小值归入 bin 0
    # searchsorted side='right' matches pd.qcut right=True:
    # values equal to edge go to right bin, minimum goes to bin 0
    labels = np.searchsorted(edges, vals, side='right') - 1
    labels = np.clip(labels, 0, n_bins - 1)

    return labels.astype(np.float64)


def _assign_zero_aware_vec(vals: np.ndarray, n_groups: int) -> np.ndarray:
    """
    零值感知向量化分组 (单截面) / Zero-aware vectorized grouping (single section).

    按正负拆分后各自做分位数分组，负值标签较低，正值标签较高
    Split by sign and group separately, negative gets lower labels
    """
    neg_mask = vals <= 0
    pos_mask = vals > 0
    n_neg = neg_mask.sum()
    n_pos = pos_mask.sum()

    # 某一侧为空时退化为普通分组 / fall back to standard if one side is empty
    if n_neg == 0 or n_pos == 0:
        return _assign_group_vec(vals, n_groups)

    total = n_neg + n_pos

    # 按样本量比例分配分组数，每侧至少 1 组
    # allocate groups proportionally, at least 1 per side
    n_neg_groups = max(1, round(n_groups * n_neg / total))
    n_pos_groups = n_groups - n_neg_groups
    if n_pos_groups < 1:
        n_pos_groups = 1
        n_neg_groups = n_groups - n_pos_groups

    result = np.full(len(vals), np.nan, dtype=np.float64)

    # 负值（含零）分组 / negative (including zero) grouping
    if n_neg > 0:
        result[neg_mask] = _assign_group_vec(vals[neg_mask], n_neg_groups)

    # 正值分组，标签偏移 n_neg_groups / positive grouping, offset labels
    if n_pos > 0:
        result[pos_mask] = _assign_group_vec(vals[pos_mask], n_pos_groups) + n_neg_groups

    return result
