"""
FactorAnalysis/portfolio.py — 净值曲线计算

支持纯多、纯空、top多-bottom空组合净值。
Supports long-only, short-only, and top-bottom hedged equity curves.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from .grouping import quantile_group


def _calc_labels_with_rebalance(
    factor: pd.Series,
    n_groups: int,
    rebalance_freq: int,
    group_labels: pd.Series | None = None,
) -> pd.Series:
    """
    计算带调仓频率的分组标签 / Calculate group labels with rebalance frequency.

    rebalance_freq=1 时每日调仓，rebalance_freq=N 时每 N 日调仓一次，
    非调仓日沿用上一个调仓日的分组结果。
    When rebalance_freq=1, rebalance daily. When rebalance_freq=N,
    rebalance every N days, carrying forward labels from the last rebalance date.

    当传入 group_labels 时跳过内部 quantile_group 调用，直接使用预计算结果。
    When group_labels is provided, skip internal quantile_group and use pre-computed result.

    Parameters / 参数:
        factor: 因子值，MultiIndex (timestamp, symbol) / Factor values
        n_groups: 分组数量 / Number of groups
        rebalance_freq: 调仓频率 / Rebalance frequency (every N cross-sections)
        group_labels: 预计算的分组标签，传入时跳过 quantile_group
                      Pre-computed group labels; skip quantile_group when provided
    """
    if rebalance_freq <= 1:
        # 每日调仓：直接使用预计算或内部计算 / daily rebalance: use pre-computed or compute
        if group_labels is not None:
            return group_labels
        return quantile_group(factor, n_groups=n_groups)

    # 按时间排序的唯一截面日期 / Unique timestamps sorted
    timestamps = factor.index.get_level_values(0).unique().sort_values()
    # 调仓日：每隔 rebalance_freq 个截面取一个 / Rebalance dates: every N-th timestamp
    rebalance_dates = timestamps[::rebalance_freq]

    # 仅在调仓日取分组标签 / Get labels only at rebalance dates
    reb_mask = factor.index.get_level_values(0).isin(rebalance_dates)
    if group_labels is not None:
        # 使用预计算标签，仅在调仓日取值 / use pre-computed labels at rebalance dates
        reb_labels = group_labels[reb_mask]
    else:
        reb_labels = quantile_group(factor[reb_mask], n_groups=n_groups)

    # 构建完整标签序列，初始全 NaN / Build full label series, initially all NaN
    all_labels = pd.Series(np.nan, index=factor.index, dtype=float)

    # 填入调仓日标签 / Fill in labels at rebalance dates
    all_labels.loc[reb_labels.index] = reb_labels

    # 按资产维度前向填充：非调仓日沿用上一个调仓日的标签
    # Forward-fill per symbol: non-rebalance days carry forward last rebalance labels
    labels_df = all_labels.unstack(level=1)
    labels_df = labels_df.ffill()
    return labels_df.stack()


def calc_long_only_curve(
    factor: pd.Series,
    returns: pd.Series,
    n_groups: int = 5,
    top_k: int = 1,
    rebalance_freq: int = 1,
    _raw: bool = False,
    group_labels: pd.Series | None = None,
) -> pd.Series:
    """
    计算仅多组（按因子值最高的 top_k 组）等权净值曲线 / Calculate long-only equity curve.

    每个截面选取因子值最高的 top_k 组，等权持有，计算累积净值。
    At each cross-section, hold the top_k groups with highest factor values equally,
    compute cumulative equity curve.

    Parameters / 参数:
        factor: 因子值，MultiIndex (timestamp, symbol) / Factor values, MultiIndex (timestamp, symbol)
        returns: 前向收益率，MultiIndex (timestamp, symbol) / Forward returns, MultiIndex (timestamp, symbol)
        n_groups: 分组数量，默认 5 / Number of groups, default 5
        top_k: 持有最高的几组，默认 1 / Number of top groups to hold, default 1
        rebalance_freq: 调仓频率（每 N 个截面调仓一次），默认 1 / Rebalance every N cross-sections, default 1
        group_labels: 预计算分组标签，传入时跳过内部 quantile_group / Pre-computed group labels

    Returns / 返回:
        pd.Series: 净值曲线，index 为 timestamp，起始值为 1.0
                   Equity curve indexed by timestamp, starting value 1.0
    """
    if not isinstance(rebalance_freq, int):
        raise TypeError(f"rebalance_freq 必须为整数，当前类型: {type(rebalance_freq).__name__}")
    if rebalance_freq < 1:
        raise ValueError(f"rebalance_freq 必须 >= 1，当前值: {rebalance_freq}")
    if top_k < 1:
        raise ValueError(f"top_k 必须 >= 1，当前值: {top_k}")
    if top_k > n_groups:
        raise ValueError(f"top_k ({top_k}) 不能超过 n_groups ({n_groups})")

    labels = _calc_labels_with_rebalance(factor, n_groups, rebalance_freq, group_labels=group_labels)
    df = pd.DataFrame({"label": labels, "returns": returns})

    # 最高组标签 / highest group label
    top_labels = set(range(n_groups - top_k, n_groups))

    def _long_return(g: pd.DataFrame) -> float:
        """截面内 top_k 组等权平均收益 / Equal-weighted avg return of top_k groups in one cross-section."""
        mask = g["label"].isin(top_labels) & g["returns"].notna() & np.isfinite(g["returns"])
        if mask.sum() == 0:
            return 0.0
        return g.loc[mask, "returns"].mean()

    daily_returns = df.groupby(level=0).apply(_long_return)
    # 累积净值 / cumulative equity
    equity = (1.0 + daily_returns).cumprod()
    if not _raw:
        equity.iloc[0] = 1.0  # 确保起始值为 1.0 / ensure start value is 1.0
    return equity


def calc_short_only_curve(
    factor: pd.Series,
    returns: pd.Series,
    n_groups: int = 5,
    bottom_k: int = 1,
    rebalance_freq: int = 1,
    _raw: bool = False,
    group_labels: pd.Series | None = None,
) -> pd.Series:
    """
    计算仅空组（按因子值最低的 bottom_k 组）等权做空净值曲线 / Calculate short-only equity curve.

    每个截面选取因子值最低的 bottom_k 组，等权做空（收益取反），计算累积净值。
    At each cross-section, short the bottom_k groups with lowest factor values equally
    (negate returns), compute cumulative equity curve.

    Parameters / 参数:
        factor: 因子值，MultiIndex (timestamp, symbol) / Factor values, MultiIndex (timestamp, symbol)
        returns: 前向收益率，MultiIndex (timestamp, symbol) / Forward returns, MultiIndex (timestamp, symbol)
        n_groups: 分组数量，默认 5 / Number of groups, default 5
        bottom_k: 做空最低的几组，默认 1 / Number of bottom groups to short, default 1
        rebalance_freq: 调仓频率（每 N 个截面调仓一次），默认 1 / Rebalance every N cross-sections, default 1
        group_labels: 预计算分组标签，传入时跳过内部 quantile_group / Pre-computed group labels

    Returns / 返回:
        pd.Series: 净值曲线，index 为 timestamp，起始值为 1.0
                   Equity curve indexed by timestamp, starting value 1.0
    """
    if not isinstance(rebalance_freq, int):
        raise TypeError(f"rebalance_freq 必须为整数，当前类型: {type(rebalance_freq).__name__}")
    if rebalance_freq < 1:
        raise ValueError(f"rebalance_freq 必须 >= 1，当前值: {rebalance_freq}")
    if bottom_k < 1:
        raise ValueError(f"bottom_k 必须 >= 1，当前值: {bottom_k}")
    if bottom_k > n_groups:
        raise ValueError(f"bottom_k ({bottom_k}) 不能超过 n_groups ({n_groups})")

    labels = _calc_labels_with_rebalance(factor, n_groups, rebalance_freq, group_labels=group_labels)
    df = pd.DataFrame({"label": labels, "returns": returns})

    # 最低组标签 / lowest group labels
    bottom_labels = set(range(bottom_k))

    def _short_return(g: pd.DataFrame) -> float:
        """截面内 bottom_k 组等权做空收益 / Equal-weighted short return of bottom_k groups in one cross-section."""
        mask = g["label"].isin(bottom_labels) & g["returns"].notna() & np.isfinite(g["returns"])
        if mask.sum() == 0:
            return 0.0
        # 做空 = 收益取反 / short = negate returns
        return -g.loc[mask, "returns"].mean()

    daily_returns = df.groupby(level=0).apply(_short_return)
    # 累积净值 / cumulative equity
    equity = (1.0 + daily_returns).cumprod()
    if not _raw:
        equity.iloc[0] = 1.0  # 确保起始值为 1.0 / ensure start value is 1.0
    return equity


def calc_portfolio_curves(
    factor: pd.Series,
    returns: pd.Series,
    n_groups: int = 5,
    top_k: int = 1,
    bottom_k: int = 1,
    rebalance_freq: int = 1,
    _raw: bool = False,
    group_labels: pd.Series | None = None,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    """
    统一计算多/空/对冲三条净值曲线 / Unified long/short/hedge equity curve calculation.

    单次 groupby.apply 同时输出三条日收益序列，将三次独立调用合并为一次，
    消除重复的标签计算和 DataFrame 构建。
    Single groupby.apply outputs three daily return series simultaneously,
    merging three independent calls into one, eliminating redundant label
    computation and DataFrame construction.

    Parameters / 参数:
        factor: 因子值，MultiIndex (timestamp, symbol) / Factor values
        returns: 前向收益率，MultiIndex (timestamp, symbol) / Forward returns
        n_groups: 分组数量，默认 5 / Number of groups, default 5
        top_k: 做多最高的几组，默认 1 / Number of top groups to long, default 1
        bottom_k: 做空最低的几组，默认 1 / Number of bottom groups to short, default 1
        rebalance_freq: 调仓频率（每 N 个截面调仓一次），默认 1
                      Rebalance every N cross-sections, default 1
        _raw: 是否返回原始 cumprod（不覆写起始值为 1.0），默认 False
              Return raw cumprod without overwriting start to 1.0, default False
        group_labels: 预计算分组标签，传入时跳过内部 quantile_group
                      Pre-computed group labels; skip quantile_group when provided

    Returns / 返回:
        tuple[pd.Series, pd.Series, pd.Series]:
            (long_curve, short_curve, hedge_curve) 三条净值曲线
            Three equity curves indexed by timestamp, starting value 1.0
    """
    # 参数校验 / parameter validation
    if not isinstance(rebalance_freq, int):
        raise TypeError(
            f"rebalance_freq 必须为整数，当前类型: {type(rebalance_freq).__name__}"
        )
    if rebalance_freq < 1:
        raise ValueError(f"rebalance_freq 必须 >= 1，当前值: {rebalance_freq}")
    if top_k < 1:
        raise ValueError(f"top_k 必须 >= 1，当前值: {top_k}")
    if bottom_k < 1:
        raise ValueError(f"bottom_k 必须 >= 1，当前值: {bottom_k}")
    if top_k + bottom_k > n_groups:
        raise ValueError(
            f"top_k ({top_k}) + bottom_k ({bottom_k}) 不能超过 n_groups ({n_groups})"
        )

    # 一次性计算分组标签 / compute group labels once
    labels = _calc_labels_with_rebalance(
        factor, n_groups, rebalance_freq, group_labels=group_labels,
    )
    df = pd.DataFrame({"label": labels, "returns": returns})

    top_labels = set(range(n_groups - top_k, n_groups))
    bottom_labels = set(range(bottom_k))

    def _portfolio_returns(g: pd.DataFrame) -> pd.Series:
        """
        截面内同时计算多/空/对冲收益 / Compute long/short/hedge returns in one cross-section.
        """
        valid = g["returns"].notna() & np.isfinite(g["returns"])
        long_mask = valid & g["label"].isin(top_labels)
        short_mask = valid & g["label"].isin(bottom_labels)
        long_ret = g.loc[long_mask, "returns"].mean() if long_mask.sum() > 0 else 0.0
        # 做空 = 收益取反 / short = negate returns
        short_ret = (
            -g.loc[short_mask, "returns"].mean() if short_mask.sum() > 0 else 0.0
        )
        return pd.Series({
            "long": long_ret,
            "short": short_ret,
            "hedge": long_ret + short_ret,
        })

    daily = df.groupby(level=0).apply(_portfolio_returns)

    # 累积净值 / cumulative equity
    long_curve = (1.0 + daily["long"]).cumprod()
    short_curve = (1.0 + daily["short"]).cumprod()
    hedge_curve = (1.0 + daily["hedge"]).cumprod()

    if not _raw:
        long_curve.iloc[0] = 1.0
        short_curve.iloc[0] = 1.0
        hedge_curve.iloc[0] = 1.0

    return long_curve, short_curve, hedge_curve


def calc_top_bottom_curve(
    factor: pd.Series,
    returns: pd.Series,
    n_groups: int = 5,
    top_k: int = 1,
    bottom_k: int = 1,
    rebalance_freq: int = 1,
    _raw: bool = False,
    group_labels: pd.Series | None = None,
) -> pd.Series:
    """
    计算多空对冲组合净值曲线：做多 top_k 组 - 做空 bottom_k 组 / Calculate long-short hedged equity curve.

    每个截面选取因子值最高的 top_k 组做多、最低的 bottom_k 组做空，
    日收益 = 多头收益 - 空头收益（空头收益取反后相加）。
    At each cross-section, go long on top_k highest groups and short on bottom_k lowest groups.
    Daily return = long return - short return (short returns are negated before adding).

    Parameters / 参数:
        factor: 因子值，MultiIndex (timestamp, symbol) / Factor values, MultiIndex (timestamp, symbol)
        returns: 前向收益率，MultiIndex (timestamp, symbol) / Forward returns, MultiIndex (timestamp, symbol)
        n_groups: 分组数量，默认 5 / Number of groups, default 5
        top_k: 做多最高的几组，默认 1 / Number of top groups to long, default 1
        bottom_k: 做空最低的几组，默认 1 / Number of bottom groups to short, default 1
        rebalance_freq: 调仓频率（每 N 个截面调仓一次），默认 1 / Rebalance every N cross-sections, default 1
        group_labels: 预计算分组标签，传入时跳过内部 quantile_group / Pre-computed group labels

    Returns / 返回:
        pd.Series: 净值曲线，index 为 timestamp，起始值为 1.0
                   Equity curve indexed by timestamp, starting value 1.0
    """
    if not isinstance(rebalance_freq, int):
        raise TypeError(f"rebalance_freq 必须为整数，当前类型: {type(rebalance_freq).__name__}")
    if rebalance_freq < 1:
        raise ValueError(f"rebalance_freq 必须 >= 1，当前值: {rebalance_freq}")
    if top_k < 1:
        raise ValueError(f"top_k 必须 >= 1，当前值: {top_k}")
    if bottom_k < 1:
        raise ValueError(f"bottom_k 必须 >= 1，当前值: {bottom_k}")
    if top_k + bottom_k > n_groups:
        raise ValueError(
            f"top_k ({top_k}) + bottom_k ({bottom_k}) 不能超过 n_groups ({n_groups})"
        )

    labels = _calc_labels_with_rebalance(factor, n_groups, rebalance_freq, group_labels=group_labels)
    df = pd.DataFrame({"label": labels, "returns": returns})

    top_labels = set(range(n_groups - top_k, n_groups))
    bottom_labels = set(range(bottom_k))

    def _hedge_return(g: pd.DataFrame) -> float:
        """截面内多空对冲收益 / Long-short hedged return in one cross-section."""
        valid = g["returns"].notna() & np.isfinite(g["returns"])
        long_mask = valid & g["label"].isin(top_labels)
        short_mask = valid & g["label"].isin(bottom_labels)
        long_ret = g.loc[long_mask, "returns"].mean() if long_mask.sum() > 0 else 0.0
        short_ret = -g.loc[short_mask, "returns"].mean() if short_mask.sum() > 0 else 0.0
        return long_ret + short_ret

    daily_returns = df.groupby(level=0).apply(_hedge_return)
    # 累积净值 / cumulative equity
    equity = (1.0 + daily_returns).cumprod()
    if not _raw:
        equity.iloc[0] = 1.0  # 确保起始值为 1.0 / ensure start value is 1.0
    return equity
