"""
FactorAnalysis/portfolio.py — 净值曲线计算

支持纯多、纯空、top多-bottom空组合净值。
Supports long-only, short-only, and top-bottom hedged equity curves.
"""

import numpy as np
import pandas as pd

from .grouping import quantile_group


def calc_long_only_curve(
    factor: pd.Series,
    returns: pd.Series,
    n_groups: int = 5,
    top_k: int = 1,
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

    Returns / 返回:
        pd.Series: 净值曲线，index 为 timestamp，起始值为 1.0
                   Equity curve indexed by timestamp, starting value 1.0
    """
    if top_k < 1:
        raise ValueError(f"top_k 必须 >= 1，当前值: {top_k}")
    if top_k > n_groups:
        raise ValueError(f"top_k ({top_k}) 不能超过 n_groups ({n_groups})")

    labels = quantile_group(factor, n_groups=n_groups)
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
    equity.iloc[0] = 1.0  # 确保起始值为 1.0 / ensure start value is 1.0
    return equity
