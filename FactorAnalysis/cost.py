"""
FactorAnalysis/cost.py — 交易成本向量化扣除

按比例滑差对换仓收益进行向量化扣除。
Vectorized deduction of proportional slippage from rebalancing returns.
"""

import pandas as pd


def deduct_cost(
    daily_returns: pd.Series,
    cost_rate: float = 0.001,
) -> pd.Series:
    """
    向量化扣除按比例滑差的交易成本 / Vectorized deduction of proportional slippage cost.

    假设每个截面均进行换仓，对日收益率扣除固定比例的交易成本（滑差），
    然后重新计算累积净值曲线。
    Assumes rebalancing at every cross-section; deducts a fixed proportional
    cost (slippage) from daily returns, then recomputes cumulative equity curve.

    Parameters / 参数:
        daily_returns: 日收益率序列，index 为 timestamp
                       Daily returns series, indexed by timestamp
        cost_rate: 每次换仓的交易成本比例，如 0.001 表示 0.1%，默认 0.001
                   Transaction cost rate per rebalance, e.g. 0.001 for 0.1%, default 0.001

    Returns / 返回:
        pd.Series: 扣除成本后的累积净值曲线，起始值为 1.0
                   Cumulative equity curve after cost deduction, starting value 1.0

    Raises / 异常:
        ValueError: cost_rate < 0 或 cost_rate >= 1
    """
    if cost_rate < 0:
        raise ValueError(f"cost_rate 必须 >= 0，当前值: {cost_rate}")
    if cost_rate >= 1:
        raise ValueError(f"cost_rate 必须 < 1，当前值: {cost_rate}")

    # 向量化扣除成本 / vectorized cost deduction
    adjusted_returns = daily_returns - cost_rate
    # 累积净值 / cumulative equity
    equity = (1.0 + adjusted_returns).cumprod()
    equity.iloc[0] = 1.0
    return equity
