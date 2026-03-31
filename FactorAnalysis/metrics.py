"""
FactorAnalysis/metrics.py — IC/RankIC/ICIR 与 Sharpe/Calmar/Sortino 指标计算

使用 pandas 实现，兼容 FactorLib 的 Series 输出格式（MultiIndex: timestamp, symbol）。
重构自 deap_alpha 的 numpy 版本，改用 DataFrame groupby + 向量化操作。
Refactored from deap_alpha numpy versions, using DataFrame groupby + vectorized ops.
"""

import numpy as np
import pandas as pd


def calc_ic(factor: pd.Series, returns: pd.Series) -> pd.Series:
    """
    计算日频 Pearson IC（信息系数）/ Calculate daily Pearson IC (Information Coefficient).

    每个时间截面上，计算因子值与前向收益率的 Pearson 相关系数。
    At each time cross-section, compute Pearson correlation between factor and forward returns.

    Parameters / 参数:
        factor: 因子值，MultiIndex (timestamp, symbol) / Factor values, MultiIndex (timestamp, symbol)
        returns: 前向收益率，MultiIndex (timestamp, symbol) / Forward returns, MultiIndex (timestamp, symbol)

    Returns / 返回:
        pd.Series: 日频 IC 值，index 为 timestamp / Daily IC values indexed by timestamp
    """
    df = pd.DataFrame({"factor": factor, "returns": returns})

    def _pearson(g: pd.DataFrame) -> float:
        if len(g) < 2:
            return np.nan
        f = g["factor"]
        r = g["returns"]
        # 过滤无效值 / filter invalid values
        mask = f.notna() & r.notna() & np.isfinite(f) & np.isfinite(r)
        if mask.sum() < 2:
            return np.nan
        return f[mask].corr(r[mask])

    return df.groupby(level=0).apply(_pearson)


def calc_rank_ic(factor: pd.Series, returns: pd.Series) -> pd.Series:
    """
    计算日频 Spearman Rank IC（秩信息系数）/ Calculate daily Spearman Rank IC.

    每个时间截面上，计算因子值与前向收益率的 Spearman 秩相关系数。
    At each time cross-section, compute Spearman rank correlation between factor and forward returns.

    Parameters / 参数:
        factor: 因子值，MultiIndex (timestamp, symbol) / Factor values, MultiIndex (timestamp, symbol)
        returns: 前向收益率，MultiIndex (timestamp, symbol) / Forward returns, MultiIndex (timestamp, symbol)

    Returns / 返回:
        pd.Series: 日频 Rank IC 值，index 为 timestamp / Daily Rank IC values indexed by timestamp
    """
    df = pd.DataFrame({"factor": factor, "returns": returns})

    def _spearman(g: pd.DataFrame) -> float:
        if len(g) < 2:
            return np.nan
        f = g["factor"]
        r = g["returns"]
        # 过滤无效值 / filter invalid values
        mask = f.notna() & r.notna() & np.isfinite(f) & np.isfinite(r)
        if mask.sum() < 2:
            return np.nan
        return f[mask].corr(r[mask], method="spearman")

    return df.groupby(level=0).apply(_spearman)


def calc_icir(factor: pd.Series, returns: pd.Series) -> float:
    """
    计算 ICIR（信息系数信息比率）/ Calculate ICIR (Information Coefficient Information Ratio).

    ICIR = mean(IC) / std(IC)，衡量因子预测能力的稳定性。
    ICIR = mean(IC) / std(IC), measuring stability of factor predictive power.

    Parameters / 参数:
        factor: 因子值，MultiIndex (timestamp, symbol) / Factor values, MultiIndex (timestamp, symbol)
        returns: 前向收益率，MultiIndex (timestamp, symbol) / Forward returns, MultiIndex (timestamp, symbol)

    Returns / 返回:
        float: ICIR 值 / ICIR value
    """
    ic_series = calc_ic(factor, returns)
    ic_valid = ic_series.dropna()
    if len(ic_valid) == 0:
        return 0.0
    mean_ic = ic_valid.mean()
    std_ic = ic_valid.std()
    if std_ic == 0:
        return 0.0
    return mean_ic / std_ic
