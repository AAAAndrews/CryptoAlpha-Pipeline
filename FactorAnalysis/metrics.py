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


def calc_sharpe(equity: pd.Series, risk_free_rate: float = 0.0, periods_per_year: int = 252) -> float:
    """
    计算年化 Sharpe 比率 / Calculate annualized Sharpe ratio.

    从净值曲线计算日收益率，再年化处理。
    Compute daily returns from equity curve, then annualize.

    Sharpe = (mean(daily_ret) - rf_daily) / std(daily_ret) * sqrt(periods_per_year)

    Parameters / 参数:
        equity: 净值曲线，pd.Series index 为 timestamp，起始值 1.0
                Equity curve, pd.Series indexed by timestamp, starting at 1.0
        risk_free_rate: 年化无风险利率，默认 0.0 / Annualized risk-free rate, default 0.0
        periods_per_year: 年化交易日数，默认 252 / Trading periods per year, default 252

    Returns / 返回:
        float: 年化 Sharpe 比率，数据不足时返回 0.0 / Annualized Sharpe, 0.0 if insufficient data
    """
    daily_ret = equity.pct_change().dropna()
    daily_ret = daily_ret.replace([np.inf, -np.inf], np.nan).dropna()
    if len(daily_ret) < 2:
        return 0.0
    rf_daily = risk_free_rate / periods_per_year
    excess = daily_ret - rf_daily
    std = excess.std(ddof=1)
    if std == 0:
        return 0.0
    return (excess.mean() / std) * np.sqrt(periods_per_year)


def calc_calmar(equity: pd.Series, periods_per_year: int = 252) -> float:
    """
    计算年化 Calmar 比率 / Calculate annualized Calmar ratio.

    Calmar = 年化收益率 / 最大回撤绝对值
    Calmar = annualized return / absolute max drawdown

    Parameters / 参数:
        equity: 净值曲线，pd.Series index 为 timestamp，起始值 1.0
                Equity curve, pd.Series indexed by timestamp, starting at 1.0
        periods_per_year: 年化交易日数，默认 252 / Trading periods per year, default 252

    Returns / 返回:
        float: Calmar 比率，最大回撤为 0 或数据不足时返回 0.0
               Calmar ratio, 0.0 if max drawdown is 0 or insufficient data
    """
    daily_ret = equity.pct_change().dropna()
    daily_ret = daily_ret.replace([np.inf, -np.inf], np.nan).dropna()
    n = len(daily_ret)
    if n < 2:
        return 0.0
    # 年化收益率 / annualized return
    total_return = equity.iloc[-1] / equity.iloc[0] - 1
    years = n / periods_per_year
    if years <= 0:
        return 0.0
    annualized_return = (1 + total_return) ** (1 / years) - 1
    # 最大回撤 / max drawdown
    cummax = equity.cummax()
    drawdown = (equity - cummax) / cummax
    max_dd = drawdown.min()
    if max_dd == 0:
        return 0.0
    return annualized_return / abs(max_dd)


def calc_sortino(equity: pd.Series, risk_free_rate: float = 0.0, periods_per_year: int = 252) -> float:
    """
    计算年化 Sortino 比率 / Calculate annualized Sortino ratio.

    下行偏差仅考虑低于无风险利率的收益率。
    Downside deviation only considers returns below the risk-free rate.

    Sortino = (mean(daily_ret) - rf_daily) / downside_dev * sqrt(periods_per_year)

    Parameters / 参数:
        equity: 净值曲线，pd.Series index 为 timestamp，起始值 1.0
                Equity curve, pd.Series indexed by timestamp, starting at 1.0
        risk_free_rate: 年化无风险利率，默认 0.0 / Annualized risk-free rate, default 0.0
        periods_per_year: 年化交易日数，默认 252 / Trading periods per year, default 252

    Returns / 返回:
        float: 年化 Sortino 比率，下行偏差为 0 或数据不足时返回 0.0
               Annualized Sortino, 0.0 if downside dev is 0 or insufficient data
    """
    daily_ret = equity.pct_change().dropna()
    daily_ret = daily_ret.replace([np.inf, -np.inf], np.nan).dropna()
    if len(daily_ret) < 2:
        return 0.0
    rf_daily = risk_free_rate / periods_per_year
    excess = daily_ret - rf_daily
    # 下行偏差 / downside deviation: only negative excess returns
    downside = excess[excess < 0]
    if len(downside) == 0:
        return 0.0
    downside_dev = np.sqrt((downside ** 2).mean())
    if downside_dev == 0:
        return 0.0
    return (excess.mean() / downside_dev) * np.sqrt(periods_per_year)
