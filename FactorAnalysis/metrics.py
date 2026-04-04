"""
FactorAnalysis/metrics.py — IC/RankIC/ICIR 与 Sharpe/Calmar/Sortino 指标计算

使用 pandas 实现，兼容 FactorLib 的 Series 输出格式（MultiIndex: timestamp, symbol）。
重构自 deap_alpha 的 numpy 版本，改用 DataFrame groupby + 向量化操作。
Refactored from deap_alpha numpy versions, using DataFrame groupby + vectorized ops.
"""

import warnings

import numpy as np
import pandas as pd
from scipy import stats


def calc_ic(factor: pd.Series, returns: pd.Series) -> pd.Series:
    """
    计算日频 Pearson IC（信息系数）/ Calculate daily Pearson IC (Information Coefficient).

    每个时间截面上，计算因子值与前向收益率的 Pearson 相关系数。
    向量化实现：unstack 为 2D 矩阵后 numpy 批量行级 Pearson 相关，替代 groupby.apply。
    At each time cross-section, compute Pearson correlation between factor and forward returns.
    Vectorized: unstack to 2D matrix, numpy batch row-level Pearson, replacing groupby.apply.

    Parameters / 参数:
        factor: 因子值，MultiIndex (timestamp, symbol) / Factor values, MultiIndex (timestamp, symbol)
        returns: 前向收益率，MultiIndex (timestamp, symbol) / Forward returns, MultiIndex (timestamp, symbol)

    Returns / 返回:
        pd.Series: 日频 IC 值，index 为 timestamp / Daily IC values indexed by timestamp
    """
    df = pd.DataFrame({"factor": factor, "returns": returns})

    # unstack 为 2D 矩阵 (timestamp × symbol) / unstack to 2D matrix
    f_mat = df["factor"].unstack(level=1)
    r_mat = df["returns"].unstack(level=1)

    # 有效值掩码：非 NaN 且有限 / valid mask: non-NaN and finite
    valid = f_mat.notna() & r_mat.notna() & np.isfinite(f_mat) & np.isfinite(r_mat)
    n_valid = valid.sum(axis=1).astype(float)

    # 将无效值置 NaN，sum(skipna=True) 自动跳过 / set invalid to NaN, sum skips them
    f_clean = f_mat.where(valid)
    r_clean = r_mat.where(valid)

    # 向量化 Pearson 公式 / vectorized Pearson formula
    # r = (n*Σxy - Σx*Σy) / sqrt((n*Σx² - (Σx)²)(n*Σy² - (Σy)²))
    sum_x = f_clean.sum(axis=1)
    sum_y = r_clean.sum(axis=1)
    sum_xy = (f_clean * r_clean).sum(axis=1)
    sum_x2 = (f_clean ** 2).sum(axis=1)
    sum_y2 = (r_clean ** 2).sum(axis=1)

    n = n_valid
    numerator = n * sum_xy - sum_x * sum_y
    denom_x = n * sum_x2 - sum_x ** 2
    denom_y = n * sum_y2 - sum_y ** 2

    with np.errstate(invalid="ignore", divide="ignore"):
        denom = np.sqrt(denom_x * denom_y)
        ic = numerator / denom

    # 边界处理：有效数不足或分母为零返回 NaN / edge cases: insufficient data or zero denom → NaN
    ic = pd.Series(ic, index=f_mat.index, dtype=float)
    ic[n < 2] = np.nan
    ic = ic.replace([np.inf, -np.inf], np.nan)

    return ic


def calc_rank_ic(factor: pd.Series, returns: pd.Series) -> pd.Series:
    """
    计算日频 Spearman Rank IC（秩信息系数）/ Calculate daily Spearman Rank IC.

    每个时间截面上，计算因子值与前向收益率的 Spearman 秩相关系数。
    向量化实现：unstack 为 2D 矩阵，按行排名后 numpy 批量 Pearson 相关，替代 groupby.apply。
    At each time cross-section, compute Spearman rank correlation between factor and forward returns.
    Vectorized: unstack to 2D matrix, rank per-row, numpy batch Pearson, replacing groupby.apply.

    Parameters / 参数:
        factor: 因子值，MultiIndex (timestamp, symbol) / Factor values, MultiIndex (timestamp, symbol)
        returns: 前向收益率，MultiIndex (timestamp, symbol) / Forward returns, MultiIndex (timestamp, symbol)

    Returns / 返回:
        pd.Series: 日频 Rank IC 值，index 为 timestamp / Daily Rank IC values indexed by timestamp
    """
    df = pd.DataFrame({"factor": factor, "returns": returns})

    # unstack 为 2D 矩阵 (timestamp × symbol) / unstack to 2D matrix
    f_mat = df["factor"].unstack(level=1)
    r_mat = df["returns"].unstack(level=1)

    # 有效值掩码：非 NaN 且有限 / valid mask: non-NaN and finite
    valid = f_mat.notna() & r_mat.notna() & np.isfinite(f_mat) & np.isfinite(r_mat)
    n_valid = valid.sum(axis=1).astype(float)

    # 先用 valid 掩码屏蔽无效值，再按行排名（确保排名范围与参考实现一致）/ mask invalid first, then rank
    # 参考实现仅对 factor 和 returns 同时有效的样本排名 / reference ranks only jointly-valid samples
    f_masked = f_mat.where(valid)
    r_masked = r_mat.where(valid)
    f_ranked = f_masked.rank(axis=1, method="average", na_option="keep")
    r_ranked = r_masked.rank(axis=1, method="average", na_option="keep")

    # 仅保留有效值 / keep only valid entries
    f_clean = f_ranked.where(valid)
    r_clean = r_ranked.where(valid)

    # 向量化 Pearson 公式（作用于排名后的数据）/ vectorized Pearson on ranked data
    sum_x = f_clean.sum(axis=1)
    sum_y = r_clean.sum(axis=1)
    sum_xy = (f_clean * r_clean).sum(axis=1)
    sum_x2 = (f_clean ** 2).sum(axis=1)
    sum_y2 = (r_clean ** 2).sum(axis=1)

    n = n_valid
    numerator = n * sum_xy - sum_x * sum_y
    denom_x = n * sum_x2 - sum_x ** 2
    denom_y = n * sum_y2 - sum_y ** 2

    with np.errstate(invalid="ignore", divide="ignore"):
        denom = np.sqrt(denom_x * denom_y)
        rank_ic = numerator / denom

    # 边界处理：有效数不足或分母为零返回 NaN / edge cases: insufficient data or zero denom → NaN
    rank_ic = pd.Series(rank_ic, index=f_mat.index, dtype=float)
    rank_ic[n < 2] = np.nan
    rank_ic = rank_ic.replace([np.inf, -np.inf], np.nan)

    return rank_ic


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


def calc_ic_stats(factor: pd.Series, returns: pd.Series) -> pd.Series:
    """
    计算 IC 统计显著性指标 / Calculate IC statistical significance metrics.

    基于日频 Pearson IC 序列，计算均值、标准差、ICIR、t 检验、偏度、峰度等统计量，
    衡量因子预测能力的显著性和分布特征。
    Compute mean, std, ICIR, t-test, skewness, kurtosis from daily Pearson IC series,
    measuring significance and distribution of factor predictive power.

    Parameters / 参数:
        factor: 因子值，MultiIndex (timestamp, symbol) / Factor values, MultiIndex (timestamp, symbol)
        returns: 前向收益率，MultiIndex (timestamp, symbol) / Forward returns, MultiIndex (timestamp, symbol)

    Returns / 返回:
        pd.Series: 包含以下字段的统计指标 / Statistics with the following fields:
            - IC_mean: IC 均值 / IC mean
            - IC_std: IC 标准差 / IC standard deviation
            - ICIR: IC 信息比率 (IC_mean / IC_std) / IC information ratio
            - t_stat: t 统计量，检验 IC 均值是否显著不为零 / t-statistic for IC mean != 0
            - p_value: t 检验 p 值 / t-test p-value
            - IC_skew: IC 序列偏度 / IC series skewness
            - IC_kurtosis: IC 序列峰度 / IC series kurtosis
    """
    # 参数校验 / parameter validation
    if not isinstance(factor, pd.Series):
        raise TypeError(f"factor must be pd.Series, got {type(factor).__name__}")
    if not isinstance(returns, pd.Series):
        raise TypeError(f"returns must be pd.Series, got {type(returns).__name__}")
    if len(factor) == 0 or len(returns) == 0:
        raise ValueError("factor and returns must not be empty")

    # 计算日频 IC / compute daily IC
    ic_series = calc_ic(factor, returns)
    ic_valid = ic_series.dropna()

    # 数据不足时返回全 NaN 并发出警告 / return all-NaN with warning if insufficient data
    if len(ic_valid) < 3:
        warnings.warn(
            f"IC series has only {len(ic_valid)} valid observations (need >= 3), "
            "returning NaN stats",
            UserWarning,
        )
        return pd.Series({
            "IC_mean": np.nan,
            "IC_std": np.nan,
            "ICIR": np.nan,
            "t_stat": np.nan,
            "p_value": np.nan,
            "IC_skew": np.nan,
            "IC_kurtosis": np.nan,
        })

    # 核心统计量 / core statistics
    ic_mean = float(ic_valid.mean())
    ic_std = float(ic_valid.std(ddof=1))

    # ICIR: 均值 / 标准差 / ICIR: mean / std
    icir = ic_mean / ic_std if ic_std != 0 else np.nan

    # t 检验：IC 均值是否显著不为零 / t-test: is IC mean significantly non-zero
    t_stat, p_value = stats.ttest_1samp(ic_valid.values, 0.0)

    # 分布统计 / distribution statistics
    ic_skew = float(stats.skew(ic_valid.values, bias=False))
    ic_kurtosis = float(stats.kurtosis(ic_valid.values, bias=False))

    return pd.Series({
        "IC_mean": ic_mean,
        "IC_std": ic_std,
        "ICIR": icir,
        "t_stat": float(t_stat),
        "p_value": float(p_value),
        "IC_skew": ic_skew,
        "IC_kurtosis": ic_kurtosis,
    })
