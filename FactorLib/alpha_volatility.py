"""
波动率因子模块 / Volatility factor module

Alpha2: 基于收盘价过去 N 个周期收益率的标准差计算的波动率因子。
Alpha2: Volatility factor based on the standard deviation of close-price
        returns over the past N periods.
"""

import pandas as pd

from FactorLib.base import BaseFactor


class AlphaVolatility(BaseFactor):
    """
    Alpha2 波动率因子 / Alpha2 volatility factor.

    计算每个交易对在回看窗口内的收益率标准差，作为波动率信号。
    高值表示该资产价格波动剧烈（高风险），低值表示价格相对稳定。

    Calculates the standard deviation of returns for each trading pair
    over a lookback window, serving as a volatility signal.
    High values indicate large price swings (high risk), low values
    indicate relative price stability.

    Parameters / 参数:
        lookback (int): 回看周期数 / Number of lookback periods. Default 20.
    """

    def __init__(self, lookback: int = 20):
        super().__init__()
        self.lookback = lookback
        # 更新名称以反映参数 / update name to reflect the parameter
        self.name = f"{self.__class__.__name__}(lookback={self.lookback})"

    def calculate(self, data: pd.DataFrame) -> pd.Series:
        """
        计算波动率因子值 / Calculate volatility factor values.

        对每个交易对独立计算：
        volatility = close.pct_change().rolling(lookback).std()

        For each trading pair independently:
        volatility = close.pct_change().rolling(lookback).std()

        Parameters / 参数:
            data (pd.DataFrame): 标准化行情数据，包含 timestamp, symbol, close 列。
                Standardized market data with timestamp, symbol, close columns.

        Returns / 返回:
            pd.Series: 波动率因子值，索引为 timestamp。
                Volatility factor values, indexed by timestamp.
        """
        # 按交易对分组，计算过去 N 个周期的收益率标准差
        # Group by symbol, compute return std over past N periods
        volatility = (
            data.groupby("symbol")["close"]
            .transform(lambda s: s.pct_change().rolling(window=self.lookback, min_periods=self.lookback).std())
        )
        # 确保 Series 名称为因子名 / ensure Series name matches factor name
        volatility.name = self.name
        return volatility
