"""
FactorAnalysis/neutralize.py — 分组中性化净值曲线 / Group-Neutralized Equity Curve

支持组内因子去均值（demeaned）和组内收益调整（group_adjust）后构建多空组合净值曲线。
Supports within-group factor demeaning and return adjustment before building
long-short hedged equity curve.
"""

import numpy as np
import pandas as pd

from .grouping import quantile_group


def calc_neutralized_curve(
    factor: pd.Series,
    returns: pd.Series,
    groups: "pd.Series | int",
    demeaned: bool = True,
    group_adjust: bool = False,
    n_groups: int = 5,
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

    Returns / 返回:
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

    # 构建工作 DataFrame / Build working DataFrame
    df = pd.DataFrame({
        "factor": factor,
        "returns": returns,
        "group": group_labels,
    })

    # 排除无效组标签的行 / Exclude rows with invalid group labels
    valid_group = df["group"].notna() & np.isfinite(df["group"])
    valid_factor = df["factor"].notna() & np.isfinite(df["factor"])
    valid_returns = df["returns"].notna() & np.isfinite(df["returns"])

    # 组内因子去均值 / Demean factor within groups
    # 对有效因子值和有效组标签的行做去均值
    # Demean only on rows with valid factor and group
    if demeaned:
        mask_for_demean = valid_group & valid_factor
        # 需要有足够数据才能做 transform / Need enough data for transform
        if mask_for_demean.sum() > 0:
            group_mean = (
                df.loc[mask_for_demean]
                .groupby([pd.Grouper(level=0), "group"])["factor"]
                .transform("mean")
            )
            # 只对有效行更新因子值 / Only update factor for valid rows
            df.loc[mask_for_demean, "factor"] = (
                df.loc[mask_for_demean, "factor"] - group_mean
            )

    # 组内收益去均值 / Adjust returns within groups
    if group_adjust:
        mask_for_adjust = valid_group & valid_returns
        if mask_for_adjust.sum() > 0:
            group_mean_ret = (
                df.loc[mask_for_adjust]
                .groupby([pd.Grouper(level=0), "group"])["returns"]
                .transform("mean")
            )
            df.loc[mask_for_adjust, "returns"] = (
                df.loc[mask_for_adjust, "returns"] - group_mean_ret
            )

    # 按中性化因子排名分组 / Rank neutralized factor into groups
    neutralized_factor = df["factor"]
    labels = quantile_group(neutralized_factor, n_groups=n_groups)
    df["label"] = labels

    # 多空对冲：最高组做多、最低组做空 / Long-short: long top group, short bottom group
    top_labels = set(range(n_groups - 1, n_groups))
    bottom_labels = set(range(0, 1))

    def _hedge_return(g: pd.DataFrame) -> float:
        """截面内多空对冲收益 / Long-short hedged return in one cross-section."""
        valid = g["returns"].notna() & np.isfinite(g["returns"])
        long_mask = valid & g["label"].isin(top_labels)
        short_mask = valid & g["label"].isin(bottom_labels)
        long_ret = g.loc[long_mask, "returns"].mean() if long_mask.sum() > 0 else 0.0
        short_ret = -g.loc[short_mask, "returns"].mean() if short_mask.sum() > 0 else 0.0
        return long_ret + short_ret

    daily_returns = df.groupby(level=0).apply(_hedge_return)

    # 累积净值 / Cumulative equity
    equity = (1.0 + daily_returns).cumprod()
    equity.iloc[0] = 1.0  # 确保起始值为 1.0 / Ensure start value is 1.0
    return equity
