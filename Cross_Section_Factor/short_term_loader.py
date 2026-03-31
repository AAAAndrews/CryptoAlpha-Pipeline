"""
短期数据加载器 / Short-term data loader

从 CryptoDB_feather 加载所有交易对在短期时间窗口内的 K 线数据，
返回长格式 DataFrame，供因子分析和 MultiAssetDataHandler 使用。

Load short-term kline data for all trading pairs from CryptoDB_feather.
Returns a long-format DataFrame for factor analysis and MultiAssetDataHandler.
"""

import os
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Union

import pandas as pd

from Cross_Section_Factor.datapreprocess import BaseDataLoader
from CryptoDB_feather.core.reader import load_multi_klines
from CryptoDB_feather.config import DB_ROOT_PATH


class ShortTermDataLoader(BaseDataLoader):
    """
    短期数据加载器，基于 CryptoDB_feather 读取器。

    Short-term data loader backed by CryptoDB_feather reader.
    Loads klines for all (or specified) trading pairs within a configurable
    recent time window, returning a long-format DataFrame.

    Parameters
    ----------
    db_root_path : str
        Feather 数据库根路径。默认使用 CryptoDB_feather.config.DB_ROOT_PATH。
        Root path of the feather database.
    exchange : str
        交易所名称，默认 'binance'。
        Exchange name, default 'binance'.
    kline_type : str
        K 线类型，默认 'swap'。
        Kline type, default 'swap'.
    interval : str
        K 线周期，默认 '1h'。
        Kline interval, default '1h'.
    lookback_days : int
        回溯天数，默认 30。从当前时刻向前推算时间窗口。
        Number of days to look back from now. Default 30.
    symbols : Optional[List[str]]
        指定交易对列表。为 None 时自动扫描数据库中所有交易对。
        List of trading pairs. Scans all pairs in DB if None.
    num_workers : int
        并发读取线程数，默认 8。
        Number of concurrent reader threads. Default 8.
    """

    def __init__(
        self,
        db_root_path: Optional[str] = None,
        exchange: str = "binance",
        kline_type: str = "swap",
        interval: str = "1h",
        lookback_days: int = 30,
        symbols: Optional[List[str]] = None,
        num_workers: int = 8,
    ):
        super().__init__()
        self.db_root_path = db_root_path or DB_ROOT_PATH
        self.exchange = exchange
        self.kline_type = kline_type
        self.interval = interval
        self.lookback_days = lookback_days
        self.symbols = symbols
        self.num_workers = num_workers

    def receive(self, **kwargs) -> pd.DataFrame:
        """
        从 feather 数据库加载短期 K 线数据。

        Load short-term kline data from the feather database.
        """
        # 计算时间窗口 / Calculate time window
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(days=self.lookback_days)

        df = load_multi_klines(
            db_root_path=self.db_root_path,
            exchange=self.exchange,
            symbols=self.symbols,
            kline_type=self.kline_type,
            interval=self.interval,
            start_time=start_time,
            num_workers=self.num_workers,
        )

        if df.empty:
            print(f"[ShortTermDataLoader] No data loaded for "
                  f"exchange={self.exchange}, kline_type={self.kline_type}, "
                  f"interval={self.interval}, lookback={self.lookback_days}d")

        self.content = df
        return df

    def compile(self, **kwargs) -> pd.DataFrame:
        """
        校验并返回加载的数据。

        Validate and return the loaded data.
        Ensures required columns (timestamp, symbol, open, high, low, close) exist.
        """
        if self.content is None:
            self.receive()

        df = self.content

        if df.empty:
            return df

        # 校验必要列 / Validate required columns
        required = {"timestamp", "symbol", "open", "high", "low", "close"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(
                f"[ShortTermDataLoader] Missing required columns: {missing}. "
                f"Available columns: {list(df.columns)}"
            )

        # 确保按时间+交易对排序 / Ensure sorted by time + symbol
        df = df.sort_values(["timestamp", "symbol"]).reset_index(drop=True)

        return df
