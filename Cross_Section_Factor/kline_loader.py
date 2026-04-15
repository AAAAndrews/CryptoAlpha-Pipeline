"""
通用 K 线数据加载器 / General-purpose kline data loader

基于 CryptoDB_feather 读取器，支持参数化时间/交易对过滤、
数据格式校验和全样本加载。

General-purpose kline data loader backed by CryptoDB_feather reader.
Supports parametric time/symbol filtering, data validation,
and full-sample loading.
"""

from datetime import datetime
from typing import List, Optional, Union

import pandas as pd

from Cross_Section_Factor.datapreprocess import BaseDataLoader
from CryptoDB_feather.core.reader import load_multi_klines
from CryptoDB_feather.config import DB_ROOT_PATH


# 必需列 / Required columns for OHLC kline data
_REQUIRED_COLUMNS = {"timestamp", "symbol", "open", "high", "low", "close"}


class KlineLoader(BaseDataLoader):
    """
    通用 K 线数据加载器，支持参数化过滤与数据校验。

    General-purpose kline data loader with parametric filtering and validation.
    Loads klines from CryptoDB_feather, validates data integrity,
    and returns a long-format DataFrame.

    Parameters
    ----------
    db_root_path : Optional[str]
        Feather 数据库根路径，默认使用 CryptoDB_feather.config.DB_ROOT_PATH。
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
    start_time : Optional[Union[int, datetime]]
        起始时间，支持毫秒时间戳或 datetime 对象。为 None 时不做下限过滤。
        Start time filter (ms timestamp or datetime). No lower bound if None.
    end_time : Optional[Union[int, datetime]]
        结束时间，支持毫秒时间戳或 datetime 对象。为 None 时不做上限过滤。
        End time filter (ms timestamp or datetime). No upper bound if None.
    symbols : Optional[List[str]]
        指定交易对列表。为 None 时自动扫描数据库中所有交易对。
        List of trading pairs. Scans all pairs in DB if None.
    num_workers : int
        并发读取线程数，默认 8。
        Number of concurrent reader threads. Default 8.
    validate : bool
        是否在加载后执行数据校验，默认 True。
        Whether to validate data after loading. Default True.
    """

    def __init__(
        self,
        db_root_path: Optional[str] = None,
        exchange: str = "binance",
        kline_type: str = "swap",
        interval: str = "1h",
        start_time: Optional[Union[int, datetime]] = None,
        end_time: Optional[Union[int, datetime]] = None,
        symbols: Optional[List[str]] = None,
        num_workers: int = 8,
        validate: bool = True,
    ):
        super().__init__()
        self.db_root_path = db_root_path or DB_ROOT_PATH
        self.exchange = exchange
        self.kline_type = kline_type
        self.interval = interval
        self.start_time = start_time
        self.end_time = end_time
        self.symbols = symbols
        self.num_workers = num_workers
        self.validate = validate

    def receive(self, **kwargs) -> pd.DataFrame:
        """
        从 feather 数据库加载 K 线数据。

        Load kline data from the feather database.
        Passes start_time / end_time / symbols directly to load_multi_klines
        for server-side filtering. When both time filters are None and symbols
        is None, a full-sample load is performed.
        """
        # symbol 列由 read_symbol_klines 代码添加，不在 feather 文件中
        # symbol column is added programmatically, not stored in feather files
        read_cols = list(_REQUIRED_COLUMNS - {"symbol"})
        df = load_multi_klines(
            db_root_path=self.db_root_path,
            exchange=self.exchange,
            symbols=self.symbols,
            kline_type=self.kline_type,
            interval=self.interval,
            start_time=self.start_time,
            end_time=self.end_time,
            num_workers=self.num_workers,
            columns=read_cols,
        )

        if df.empty:
            print(
                f"[KlineLoader] No data loaded for "
                f"exchange={self.exchange}, kline_type={self.kline_type}, "
                f"interval={self.interval}"
            )

        self.content = df
        return df

    def compile(self, **kwargs) -> pd.DataFrame:
        """
        校验并返回加载的数据。

        Validate and return the loaded data.
        Runs column, NaN, duplicate, and OHLC logic checks.
        """
        if self.content is None:
            df = self.receive()
        else:
            df = self.content

        if df.empty:
            return df

        # 校验必要列 / Validate required columns
        missing = _REQUIRED_COLUMNS - set(df.columns)
        if missing:
            raise ValueError(
                f"[KlineLoader] Missing required columns: {missing}. "
                f"Available columns: {list(df.columns)}"
            )

        # 排序由 load_multi_klines 保证，此处无需重复
        # Sorting is guaranteed by load_multi_klines, skip redundant sort

        # 跳过校验时直接返回 / Skip validation and return early
        if not self.validate:
            return df

        self._validate_data(df)
        return df

    # ---- 数据校验 / Data validation ----

    def _validate_data(self, df: pd.DataFrame) -> None:
        """
        执行完整数据校验，失败时抛出 ValueError。

        Run full data validation; raises ValueError on failure.
        Checks: NaN in OHLC columns, duplicate rows, high >= low.
        """
        ohlc = ["open", "high", "low", "close"]

        # 无缺失值检查 / Check for NaN in OHLC columns
        nan_counts = df[ohlc].isna().sum()
        if nan_counts.any():
            raise ValueError(
                f"[KlineLoader] NaN values found in OHLC columns:\n{nan_counts[nan_counts > 0]}"
            )

        # 无重复行检查 / Check for duplicate rows
        dup_count = df.duplicated(subset=["timestamp", "symbol"]).sum()
        if dup_count > 0:
            raise ValueError(
                f"[KlineLoader] Found {dup_count} duplicate (timestamp, symbol) rows"
            )

        # high >= low 检查 / Ensure high >= low
        invalid_hl = df[df["high"] < df["low"]]
        if not invalid_hl.empty:
            raise ValueError(
                f"[KlineLoader] Found {len(invalid_hl)} rows where high < low"
            )
