"""
Task 1 验证测试：通用 K 线数据加载器 KlineLoader

Task 1 verification test: KlineLoader parametric filtering,
data validation, full-sample loading.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock
import pytest

from Cross_Section_Factor.kline_loader import KlineLoader, _REQUIRED_COLUMNS


# ---- 构造测试数据 / Build test data ----

def _make_df(n=100, start_dt=None):
    """生成一个模拟的长格式 K 线 DataFrame。"""
    if start_dt is None:
        start_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    timestamps = pd.date_range(start_dt, periods=n, freq="1h")
    symbols = ["BTCUSDT", "ETHUSDT"]
    rows = []
    for ts in timestamps:
        for sym in symbols:
            base = 100.0 if sym == "BTCUSDT" else 10.0
            rows.append({
                "timestamp": ts,
                "symbol": sym,
                "open": base,
                "high": base + 2.0,
                "low": base - 1.0,
                "close": base + 0.5,
            })
    return pd.DataFrame(rows)


class TestKlineLoaderImport:
    """模块导入与基本属性验证 / Import and basic attribute checks."""

    def test_import(self):
        """KlineLoader 可正常导入"""
        assert callable(KlineLoader)

    def test_required_columns(self):
        """必需列集合包含 OHLCV 核心字段"""
        assert _REQUIRED_COLUMNS == {"timestamp", "symbol", "open", "high", "low", "close"}


class TestKlineLoaderInit:
    """构造函数参数验证 / Constructor parameter checks."""

    def test_default_params(self):
        """默认参数正确设置"""
        loader = KlineLoader()
        assert loader.exchange == "binance"
        assert loader.kline_type == "swap"
        assert loader.interval == "1h"
        assert loader.start_time is None
        assert loader.end_time is None
        assert loader.symbols is None
        assert loader.validate is True
        assert loader.num_workers == 8

    def test_custom_params(self):
        """自定义参数正确传递"""
        st = datetime(2024, 1, 1)
        et = datetime(2024, 6, 1)
        loader = KlineLoader(
            exchange="okx",
            kline_type="spot",
            interval="1d",
            start_time=st,
            end_time=et,
            symbols=["BTCUSDT"],
            num_workers=4,
            validate=False,
        )
        assert loader.exchange == "okx"
        assert loader.kline_type == "spot"
        assert loader.interval == "1d"
        assert loader.start_time is st
        assert loader.end_time is et
        assert loader.symbols == ["BTCUSDT"]
        assert loader.num_workers == 4
        assert loader.validate is False


class TestKlineLoaderReceive:
    """receive() 方法：数据加载与参数传递 / receive() method checks."""

    @patch("Cross_Section_Factor.kline_loader.load_multi_klines")
    def test_receive_passes_params(self, mock_load):
        """receive() 正确传递所有过滤参数给 load_multi_klines"""
        test_df = _make_df(50)
        mock_load.return_value = test_df

        st = datetime(2024, 1, 1)
        et = datetime(2024, 6, 1)
        loader = KlineLoader(
            exchange="binance",
            kline_type="swap",
            interval="1h",
            start_time=st,
            end_time=et,
            symbols=["BTCUSDT"],
        )
        result = loader.receive()

        mock_load.assert_called_once_with(
            db_root_path=loader.db_root_path,
            exchange="binance",
            symbols=["BTCUSDT"],
            kline_type="swap",
            interval="1h",
            start_time=st,
            end_time=et,
            num_workers=8,
        )
        assert result is test_df
        assert loader.content is test_df

    @patch("Cross_Section_Factor.kline_loader.load_multi_klines")
    def test_receive_full_sample(self, mock_load):
        """不传时间范围和 symbols 时执行全样本加载"""
        test_df = _make_df(50)
        mock_load.return_value = test_df

        loader = KlineLoader()
        loader.receive()

        # start_time 和 end_time 均为 None，symbols 也为 None
        call_kwargs = mock_load.call_args[1]
        assert call_kwargs["start_time"] is None
        assert call_kwargs["end_time"] is None
        assert call_kwargs["symbols"] is None

    @patch("Cross_Section_Factor.kline_loader.load_multi_klines")
    def test_receive_empty_result(self, mock_load):
        """数据库无数据时返回空 DataFrame 并打印提示"""
        mock_load.return_value = pd.DataFrame()

        loader = KlineLoader()
        result = loader.receive()

        assert result.empty
        assert loader.content.empty


class TestKlineLoaderCompile:
    """compile() 方法：数据校验与返回 / compile() method checks."""

    def _get_loader(self, df, validate=True):
        """辅助：创建一个 content 已设置的 loader"""
        loader = KlineLoader(validate=validate)
        loader.content = df
        return loader

    def test_compile_empty_df(self):
        """空 DataFrame 直接返回"""
        loader = self._get_loader(pd.DataFrame())
        result = loader.compile()
        assert result.empty

    def test_compile_missing_columns(self):
        """缺少必需列时抛出 ValueError"""
        bad_df = pd.DataFrame({"timestamp": [1], "symbol": ["A"]})
        loader = self._get_loader(bad_df)
        with pytest.raises(ValueError, match="Missing required columns"):
            loader.compile()

    def test_compile_nan_in_ohlc(self):
        """OHLC 含 NaN 时抛出 ValueError"""
        df = _make_df(10)
        df.loc[0, "open"] = np.nan
        loader = self._get_loader(df)
        with pytest.raises(ValueError, match="NaN values found"):
            loader.compile()

    def test_compile_duplicate_rows(self):
        """重复 (timestamp, symbol) 行时抛出 ValueError"""
        df = _make_df(10)
        dup = df.iloc[[0]].copy()
        df = pd.concat([df, dup], ignore_index=True)
        loader = self._get_loader(df)
        with pytest.raises(ValueError, match="duplicate"):
            loader.compile()

    def test_compile_high_less_than_low(self):
        """high < low 时抛出 ValueError"""
        df = _make_df(10)
        df.loc[0, "high"] = 50.0
        df.loc[0, "low"] = 60.0
        loader = self._get_loader(df)
        with pytest.raises(ValueError, match="high < low"):
            loader.compile()

    def test_compile_valid_data(self):
        """合法数据正常返回并按 (timestamp, symbol) 排序"""
        df = _make_df(50)
        loader = self._get_loader(df)
        result = loader.compile()

        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        # 验证排序 / Verify sorted
        assert result["timestamp"].is_monotonic_increasing

    def test_compile_skip_validation(self):
        """validate=False 时跳过校验，含 NaN 数据也能通过"""
        df = _make_df(10)
        df.loc[0, "open"] = np.nan
        loader = self._get_loader(df, validate=False)
        result = loader.compile()
        # 不应抛异常 / Should not raise
        assert len(result) == len(df)

    def test_compile_auto_receive(self):
        """content 为 None 时 compile() 自动调用 receive()"""
        loader = KlineLoader(validate=False)
        loader.receive = MagicMock(return_value=_make_df(10))
        result = loader.compile()
        loader.receive.assert_called_once()
        assert not result.empty


class TestKlineLoaderInheritance:
    """继承与接口一致性 / Inheritance and interface checks."""

    def test_inherits_base_data_loader(self):
        """KlineLoader 继承自 BaseDataLoader"""
        from Cross_Section_Factor.datapreprocess import BaseDataLoader
        assert issubclass(KlineLoader, BaseDataLoader)

    def test_has_receive_compile(self):
        """实现 receive 和 compile 方法"""
        loader = KlineLoader()
        assert hasattr(loader, "receive")
        assert hasattr(loader, "compile")
        assert callable(loader.receive)
        assert callable(loader.compile)

    def test_dataset_property(self):
        """dataset 属性可正常调用（BaseDataLoader 提供）"""
        loader = KlineLoader(validate=False)
        loader.receive = MagicMock(return_value=_make_df(10))
        result = loader.dataset
        assert not result.empty
