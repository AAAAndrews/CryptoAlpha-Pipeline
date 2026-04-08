"""
FactorAnalysis/neutralize.py — 分组中性化净值曲线 / Group-Neutralized Equity Curve

支持组内因子去均值（demeaned）和组内收益调整（group_adjust）后构建多空组合净值曲线。
Supports within-group factor demeaning and return adjustment before building
long-short hedged equity curve.
"""

import numpy as np
import pandas as pd

from .grouping import quantile_group
from .portfolio import calc_portfolio_curves


def calc_neutralized_curve(
    factor: pd.Series,
    returns: pd.Series,
    groups: "pd.Series | int",
    demeaned: bool = True,
    group_adjust: bool = False,
    n_groups: int = 5,
    _raw: bool = False,
) -> pd.Series:
    """
    分组中性化后构建多空组合净值曲线 / Build group-neutralized long-short equity curve.

    先在指定组内对因子值或收益率做去均值处理（消除组间差异），
    再按中性化后的因子值排名构建多空对冲净值曲线。
    First remove group effects from factor values or returns within specified groups,
    then rank by the neutralized factor and build a long-short hedged equity curve.

    Parameters / 参数:
        factor: 因子值，MultiIndex (timestamp, symbol)
                Factor values, MultiIndex (timestamp, symbol)
        returns: 前向收益率，MultiIndex (timestamp, symbol)
                 Forward returns, MultiIndex (timestamp, symbol)
        groups: 分组标签 / Group labels.
            - int: 使用 quantile_group(factor, n_groups=groups) 生成中性化分组
              Use quantile_group to generate neutralization groups
            - pd.Series: 直接使用提供的分组标签（需与 factor 同索引）
              Use provided group labels directly (must share index with factor)
        demeaned: 是否在组内对因子值去均值，默认 True
                  Whether to demean factor within groups, default True
        group_adjust: 是否在组内对收益率去均值，默认 False
                      Whether to adjust returns within groups, default False
        n_groups: 中性化后排名分组的数量，默认 5
                  Number of ranking groups after neutralization, default 5
        _raw: 内部参数，为 True 时不覆写起始值为 1.0（用于分块合并场景）
              Internal param; when True, skip overwriting start to 1.0 (for chunked merging)
        pd.Series: 多空对冲净值曲线，index=timestamp，起始值 1.0
                   Long-short hedged equity curve, index=timestamp, starting at 1.0

    Raises / 异常:
        ValueError: 参数类型不合法或 n_groups < 2
    """
    # 参数类型校验 / Validate input types
    if not isinstance(factor, pd.Series):
        raise ValueError(
            f"'factor' must be pd.Series, got {type(factor).__name__}."
            f"\n'factor' 必须是 pd.Series，收到 {type(factor).__name__}。"
        )
    if not isinstance(returns, pd.Series):
        raise ValueError(
            f"'returns' must be pd.Series, got {type(returns).__name__}."
            f"\n'returns' 必须是 pd.Series，收到 {type(returns).__name__}。"
        )
    if not isinstance(groups, (pd.Series, int)):
        raise ValueError(
            f"'groups' must be pd.Series or int, got {type(groups).__name__}."
            f"\n'groups' 必须是 pd.Series 或 int，收到 {type(groups).__name__}。"
        )
    if n_groups < 2:
        raise ValueError(f"n_groups 必须 >= 2，当前值: {n_groups}")
    if isinstance(groups, int) and groups < 2:
        raise ValueError(f"groups (int) 必须 >= 2，当前值: {groups}")

    # 解析分组标签 / Resolve group labels
    if isinstance(groups, int):
        group_labels = quantile_group(factor, n_groups=groups)
    else:
        group_labels = groups

    # P3: unstack 为 2D 矩阵，向量化 demean + group_adjust，避免两次独立 groupby
    # P3: unstack to 2D matrices, vectorized demean + group_adjust, avoiding two separate groupby
    factor_mat = factor.unstack()
    returns_mat = returns.unstack()
    groups_mat = group_labels.unstack()

    # 对齐行列索引（与 pd.DataFrame({"factor":..., "returns":..., "group":...}) 行为一致）
    # Align row/column indices (same behavior as pd.DataFrame alignment)
    all_idx = factor_mat.index.union(returns_mat.index).union(groups_mat.index)
    all_cols = factor_mat.columns.union(returns_mat.columns).union(groups_mat.columns)
    factor_mat = factor_mat.reindex(index=all_idx, columns=all_cols)
    returns_mat = returns_mat.reindex(index=all_idx, columns=all_cols)
    groups_mat = groups_mat.reindex(index=all_idx, columns=all_cols)

    factor_np = factor_mat.values.astype(np.float64)
    returns_np = returns_mat.values.astype(np.float64)
    groups_np = groups_mat.values

    # 有效值掩码 / valid value masks
    valid_factor = np.isfinite(factor_np)
    valid_returns = np.isfinite(returns_np)
    valid_groups = np.isfinite(groups_np)

    # 获取唯一组标签 / get unique group labels
    unique_groups = np.unique(groups_np[valid_groups])

    # 组内因子去均值 (numpy 向量化)
    # Demean factor within groups (numpy vectorized)
    # 等价于 groupby([timestamp, group]).transform("mean") 但无 groupby 开销
    # Equivalent to groupby([timestamp, group]).transform("mean") but without groupby overhead
    if demeaned and len(unique_groups) > 0:
        mask_for_demean = valid_groups & valid_factor
        if mask_for_demean.sum() > 0:
            for g in unique_groups:
                g_mask = mask_for_demean & (groups_np == g)
                count = g_mask.sum(axis=1, keepdims=True)
                safe_count = np.where(count > 0, count, 1).astype(np.float64)
                mean = np.where(g_mask, factor_np, 0.0).sum(axis=1, keepdims=True) / safe_count
                factor_np = np.where(g_mask, factor_np - mean, factor_np)

    # 组内收益去均值 (numpy 向量化)
    # Adjust returns within groups (numpy vectorized)
    if group_adjust and len(unique_groups) > 0:
        mask_for_adjust = valid_groups & valid_returns
        if mask_for_adjust.sum() > 0:
            for g in unique_groups:
                g_mask = mask_for_adjust & (groups_np == g)
                count = g_mask.sum(axis=1, keepdims=True)
                safe_count = np.where(count > 0, count, 1).astype(np.float64)
                mean = np.where(g_mask, returns_np, 0.0).sum(axis=1, keepdims=True) / safe_count
                returns_np = np.where(g_mask, returns_np - mean, returns_np)

    # stack 回 Series（保持与原始 factor/returns 相同的 MultiIndex 结构）
    # stack back to Series (maintaining same MultiIndex structure as original factor/returns)
    neutralized_factor = pd.DataFrame(factor_np, index=all_idx, columns=all_cols).stack()
    neutralized_returns = pd.DataFrame(returns_np, index=all_idx, columns=all_cols).stack()

    # 按中性化因子排名分组 / rank neutralized factor into groups
    labels = quantile_group(neutralized_factor, n_groups=n_groups)

    # 复用 P1 向量化 calc_portfolio_curves 获取对冲净值曲线
    # Reuse P1 vectorized calc_portfolio_curves for hedge equity curve
    _, _, hedge_curve = calc_portfolio_curves(
        neutralized_factor, neutralized_returns,
        n_groups=n_groups, top_k=1, bottom_k=1,
        rebalance_freq=1, _raw=_raw, group_labels=labels,
    )
    return hedge_curve
