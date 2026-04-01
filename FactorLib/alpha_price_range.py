"""
价格振幅因子模块 / Price range factor module

Alpha3: 基于开盘价与收盘价之差除以最高价与最低价之差的价格振幅因子。
Alpha3: Price range factor based on (open - close) / (high - low).
"""

import numpy as np
import pandas as pd

from FactorLib.base import BaseFactor


class AlphaPriceRange(BaseFactor):
    """
    Alpha3 价格振幅因子 / Alpha3 price range factor.

    衡量收盘价相对于当日价格区间的位置，反映日内价格走势方向和力度。
    值为正表示收盘高于开盘（看涨），值为负表示收盘低于开盘（看跌）。
    绝对值越接近 1，说明收盘越接近极值端。

    Measures where the close sits relative to the day's price range,
    reflecting intraday direction and magnitude.
    Positive = close above open (bullish), negative = close below open (bearish).
    Absolute value closer to 1 means close is near the extreme.

    Formula / 公式:
        price_range = (open - close) / (high - low)

    当 high == low 时（无波动），结果为 NaN。
    When high == low (no volatility), the result is NaN.
    """

    def calculate(self, data: pd.DataFrame) -> pd.Series:
        """
        计算价格振幅因子值 / Calculate price range factor values.

        对每行独立计算：
        price_range = (open - close) / (high - low)

        For each row independently:
        price_range = (open - close) / (high - low)

        Parameters / 参数:
            data (pd.DataFrame): 标准化行情数据，包含 open, high, low, close 列。
                Standardized market data with open, high, low, close columns.

        Returns / 返回:
            pd.Series: 价格振幅因子值，索引与输入 DataFrame 一致。
                Price range factor values, index aligned with input DataFrame.
        """
        # 价格振幅：(open - close) / (high - low)
        # Price range: (open - close) / (high - low)
        denominator = data["high"] - data["low"]
        # high == low 时分母为零，结果为 NaN / division by zero when high == low → NaN
        price_range = np.where(
            denominator == 0,
            np.nan,
            (data["open"] - data["close"]) / denominator,
        )
        result = pd.Series(price_range, index=data.index, name=self.name)
        return result
