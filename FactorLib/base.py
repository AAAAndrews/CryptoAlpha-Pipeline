"""
因子基类模块 / Factor base class module

提供所有手动定义因子的抽象基类，确保统一的接口约定。
Provides the abstract base class for all manually defined factors,
ensuring a uniform interface convention.
"""

from abc import ABC, abstractmethod

import pandas as pd


class BaseFactor(ABC):
    """
    因子抽象基类 / Abstract base class for factors.

    每个因子子类必须实现 ``calculate(data)`` 方法，
    接收标准化 DataFrame，返回 pd.Series（索引为 timestamp）。

    Every factor subclass must implement the ``calculate(data)`` method,
    accepting a standardized DataFrame and returning a pd.Series indexed by timestamp.

    Attributes / 属性:
        name (str): 因子名称，默认为类名 / Factor name, defaults to class name.
    """

    def __init__(self):
        # 因子名称默认取类名 / factor name defaults to class name
        self.name = self.__class__.__name__

    @abstractmethod
    def calculate(self, data: pd.DataFrame) -> pd.Series:
        """
        计算因子值 / Calculate factor values.

        Parameters / 参数:
            data (pd.DataFrame): 标准化行情数据，至少包含以下列:
                - ``timestamp`` (datetime): 时间戳
                - ``symbol`` (str): 交易对标识
                - ``open``, ``high``, ``low``, ``close`` (float): OHLC 价格
                Standardized market data with at least the following columns:
                - ``timestamp`` (datetime): time index
                - ``symbol`` (str): trading pair identifier
                - ``open``, ``high``, ``low``, ``close`` (float): OHLC prices

        Returns / 返回:
            pd.Series: 因子值，索引为 timestamp。
                Series indexed by timestamp, containing factor values.
        """
        ...

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r})"
