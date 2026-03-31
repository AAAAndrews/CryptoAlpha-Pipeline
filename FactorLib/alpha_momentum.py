"""
动量因子模块 / Momentum factor module

Alpha1: 基于收盘价过去 N 个周期收益率计算的动量因子。
Alpha1: Momentum factor based on close-price return over the past N periods.
"""

import pandas as pd

from FactorLib.base import BaseFactor


class AlphaMomentum(BaseFactor):
    """
    Alpha1 动量因子 / Alpha1 momentum factor.

    计算每个交易对在回看窗口内的累计收益率，作为动量信号。
    正值表示该资产在过去窗口内上涨（多头信号），负值表示下跌（空头信号）。

    Calculates the cumulative return of each trading pair over a lookback window,
    serving as a momentum signal.
    Positive values indicate the asset rose over the window (long signal),
    negative values indicate decline (short signal).

    Parameters / 参数:
        lookback (int): 回看周期数 / Number of lookback periods. Default 10.
    """

    def __init__(self, lookback: int = 10):
        super().__init__()
        self.lookback = lookback
        # 更新名称以反映参数 / update name to reflect the parameter
        self.name = f"{self.__class__.__name__}(lookback={self.lookback})"

    def calculate(self, data: pd.DataFrame) -> pd.Series:
        """
        计算动量因子值 / Calculate momentum factor values.

        对每个交易对独立计算：
        momentum = close / close.shift(lookback) - 1

        For each trading pair independently:
        momentum = close / close.shift(lookback) - 1

        Parameters / 参数:
            data (pd.DataFrame): 标准化行情数据，包含 timestamp, symbol, close 列。
                Standardized market data with timestamp, symbol, close columns.

        Returns / 返回:
            pd.Series: 动量因子值，索引为 timestamp。
                Momentum factor values, indexed by timestamp.
        """
        # 按交易对分组，计算过去 N 个周期的累计收益率
        # Group by symbol, compute cumulative return over past N periods
        momentum = (
            data.groupby("symbol")["close"]
            .pct_change(self.lookback)
            .reset_index(level=0, drop=True)
        )
        # 确保 Series 名称为因子名 / ensure Series name matches factor name
        momentum.name = self.name
        return momentum
