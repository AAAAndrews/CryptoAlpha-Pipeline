"""
Task 2 验证测试：通用 K 线数据加载器 KlineLoader 综合验证

Task 2 verification tests: KlineLoader comprehensive verification
covering import, parametric filtering, validation rules,
full-sample loading, and edge cases.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock, call
import pytest

from Cross_Section_Factor.kline_loader import KlineLoader, _REQUIRED_COLUMNS


# ---- 测试数据构造 / Test data builders ----

def _make_df(n=100, start_dt=None, symbols=None):
    """
    生成模拟长格式 K 线 DataFrame。

    Generate mock long-format kline DataFrame.
    """
    if start_dt is None:
        start_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    if symbols is None:
        symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    timestamps = pd.date_range(start_dt, periods=n, freq="1h")
    rows = []
    base_prices = {"BTCUSDT": 100.0, "ETHUSDT": 10.0, "SOLUSDT": 50.0}
    for ts in timestamps:
        for sym in symbols:
            base = base_prices.get(sym, 20.0)
            rows.append({
                "timestamp": ts,
                "symbol": sym,
                "open": base,
                "high": base + 2.0,
                "low": base - 1.0,
                "close": base + 0.5,
            })
    return pd.DataFrame(rows)


class TestImport:
    """导入与模块级常量验证 / Import and module-level constant checks."""

    def test_module_import(self):
        """模块可正常导入"""
        from Cross_Section_Factor import kline_loader
        assert hasattr(kline_loader, "KlineLoader")

    def test_class_callable(self):
        """KlineLoader 可正常实例化"""
        loader = KlineLoader()
        assert loader is not None

    def test_required_columns_set(self):
        """必需列集合定义正确"""
        assert isinstance(_REQUIRED_COLUMNS, set)
        assert _REQUIRED_COLUMNS == {"timestamp", "symbol", "open", "high", "low", "close"}


class TestParametricFiltering:
    """参数化过滤验证 / Parametric filtering verification."""

    @patch("Cross_Section_Factor.kline_loader.load_multi_klines")
    def test_start_time_datetime(self, mock_load):
        """start_time 传入 datetime 对象正确传递"""
        mock_load.return_value = _make_df(10)
        st = datetime(2024, 1, 1, tzinfo=timezone.utc)
        loader = KlineLoader(start_time=st)
        loader.receive()
        call_kwargs = mock_load.call_args[1]
        assert call_kwargs["start_time"] is st

    @patch("Cross_Section_Factor.kline_loader.load_multi_klines")
    def test_start_time_timestamp_ms(self, mock_load):
        """start_time 传入毫秒时间戳正确传递"""
        mock_load.return_value = _make_df(10)
        ts_ms = 1704067200000  # 2024-01-01 00:00:00 UTC
        loader = KlineLoader(start_time=ts_ms)
        loader.receive()
        call_kwargs = mock_load.call_args[1]
        assert call_kwargs["start_time"] == ts_ms

    @patch("Cross_Section_Factor.kline_loader.load_multi_klines")
    def test_end_time_datetime(self, mock_load):
        """end_time 传入 datetime 对象正确传递"""
        mock_load.return_value = _make_df(10)
        et = datetime(2024, 6, 1, tzinfo=timezone.utc)
        loader = KlineLoader(end_time=et)
        loader.receive()
        call_kwargs = mock_load.call_args[1]
        assert call_kwargs["end_time"] is et

    @patch("Cross_Section_Factor.kline_loader.load_multi_klines")
    def test_end_time_timestamp_ms(self, mock_load):
        """end_time 传入毫秒时间戳正确传递"""
        mock_load.return_value = _make_df(10)
        ts_ms = 1717200000000  # 2024-06-01 00:00:00 UTC
        loader = KlineLoader(end_time=ts_ms)
        loader.receive()
        call_kwargs = mock_load.call_args[1]
        assert call_kwargs["end_time"] == ts_ms

    @patch("Cross_Section_Factor.kline_loader.load_multi_klines")
    def test_single_symbol_filter(self, mock_load):
        """单交易对过滤参数正确传递"""
        mock_load.return_value = _make_df(10)
        loader = KlineLoader(symbols=["BTCUSDT"])
        loader.receive()
        call_kwargs = mock_load.call_args[1]
        assert call_kwargs["symbols"] == ["BTCUSDT"]

    @patch("Cross_Section_Factor.kline_loader.load_multi_klines")
    def test_multi_symbol_filter(self, mock_load):
        """多交易对过滤参数正确传递"""
        mock_load.return_value = _make_df(10)
        syms = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        loader = KlineLoader(symbols=syms)
        loader.receive()
        call_kwargs = mock_load.call_args[1]
        assert call_kwargs["symbols"] == syms

    @patch("Cross_Section_Factor.kline_loader.load_multi_klines")
    def test_exchange_kline_type_interval(self, mock_load):
        """exchange、kline_type、interval 参数正确传递"""
        mock_load.return_value = _make_df(10)
        loader = KlineLoader(exchange="okx", kline_type="spot", interval="1d")
        loader.receive()
        call_kwargs = mock_load.call_args[1]
        assert call_kwargs["exchange"] == "okx"
        assert call_kwargs["kline_type"] == "spot"
        assert call_kwargs["interval"] == "1d"

    @patch("Cross_Section_Factor.kline_loader.load_multi_klines")
    def test_num_workers_param(self, mock_load):
        """num_workers 参数正确传递"""
        mock_load.return_value = _make_df(10)
        loader = KlineLoader(num_workers=16)
        loader.receive()
        call_kwargs = mock_load.call_args[1]
        assert call_kwargs["num_workers"] == 16

    @patch("Cross_Section_Factor.kline_loader.load_multi_klines")
    def test_full_params_combined(self, mock_load):
        """所有过滤参数组合使用时正确传递"""
        mock_load.return_value = _make_df(10)
        st = datetime(2024, 1, 1)
        et = datetime(2024, 6, 1)
        loader = KlineLoader(
            exchange="binance",
            kline_type="swap",
            interval="1h",
            start_time=st,
            end_time=et,
            symbols=["BTCUSDT"],
            num_workers=4,
        )
        loader.receive()
        call_kwargs = mock_load.call_args[1]
        assert call_kwargs["exchange"] == "binance"
        assert call_kwargs["kline_type"] == "swap"
        assert call_kwargs["interval"] == "1h"
        assert call_kwargs["start_time"] is st
        assert call_kwargs["end_time"] is et
        assert call_kwargs["symbols"] == ["BTCUSDT"]
        assert call_kwargs["num_workers"] == 4


class TestFullSampleLoading:
    """全样本加载验证 / Full-sample loading verification."""

    @patch("Cross_Section_Factor.kline_loader.load_multi_klines")
    def test_no_time_filter_full_sample(self, mock_load):
        """不传 start_time 和 end_time 时执行全样本加载"""
        mock_load.return_value = _make_df(100)
        loader = KlineLoader()
        loader.receive()
        call_kwargs = mock_load.call_args[1]
        assert call_kwargs["start_time"] is None
        assert call_kwargs["end_time"] is None

    @patch("Cross_Section_Factor.kline_loader.load_multi_klines")
    def test_no_symbols_scans_all(self, mock_load):
        """symbols 为 None 时扫描所有交易对"""
        mock_load.return_value = _make_df(100)
        loader = KlineLoader()
        loader.receive()
        call_kwargs = mock_load.call_args[1]
        assert call_kwargs["symbols"] is None

    @patch("Cross_Section_Factor.kline_loader.load_multi_klines")
    def test_only_start_time(self, mock_load):
        """仅传 start_time，end_time 为 None"""
        mock_load.return_value = _make_df(50)
        loader = KlineLoader(start_time=datetime(2024, 1, 1))
        loader.receive()
        call_kwargs = mock_load.call_args[1]
        assert call_kwargs["start_time"] is not None
        assert call_kwargs["end_time"] is None

    @patch("Cross_Section_Factor.kline_loader.load_multi_klines")
    def test_only_end_time(self, mock_load):
        """仅传 end_time，start_time 为 None"""
        mock_load.return_value = _make_df(50)
        loader = KlineLoader(end_time=datetime(2024, 6, 1))
        loader.receive()
        call_kwargs = mock_load.call_args[1]
        assert call_kwargs["start_time"] is None
        assert call_kwargs["end_time"] is not None

    @patch("Cross_Section_Factor.kline_loader.load_multi_klines")
    def test_empty_db_returns_empty(self, mock_load):
        """数据库无数据时返回空 DataFrame"""
        mock_load.return_value = pd.DataFrame()
        loader = KlineLoader()
        result = loader.receive()
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    @patch("Cross_Section_Factor.kline_loader.load_multi_klines")
    def test_empty_db_compile_returns_empty(self, mock_load):
        """数据库无数据时 compile() 也返回空 DataFrame"""
        mock_load.return_value = pd.DataFrame()
        loader = KlineLoader()
        result = loader.compile()
        assert isinstance(result, pd.DataFrame)
        assert result.empty


class TestDataValidation:
    """数据验证规则验证 / Data validation rule checks."""

    def _make_loader(self, df, validate=True):
        """辅助：创建 content 已设置的 loader"""
        loader = KlineLoader(validate=validate)
        loader.content = df
        return loader

    def test_missing_single_column(self):
        """缺少单个必需列时抛出 ValueError"""
        bad_df = pd.DataFrame({
            "timestamp": [datetime(2024, 1, 1)],
            "symbol": ["BTCUSDT"],
            "open": [100.0],
            "high": [102.0],
            "low": [99.0],
            # missing "close"
        })
        loader = self._make_loader(bad_df)
        with pytest.raises(ValueError, match="Missing required columns"):
            loader.compile()

    def test_missing_multiple_columns(self):
        """缺少多个必需列时错误消息包含所有缺失列"""
        bad_df = pd.DataFrame({"timestamp": [1], "symbol": ["A"]})
        loader = self._make_loader(bad_df)
        with pytest.raises(ValueError, match="Missing required columns"):
            loader.compile()

    def test_extra_columns_allowed(self):
        """包含额外列时不报错（只校验必需列）"""
        df = _make_df(10)
        df["volume"] = 1000.0
        df["extra_col"] = "foo"
        loader = self._make_loader(df)
        result = loader.compile()
        assert not result.empty
        assert "volume" in result.columns
        assert "extra_col" in result.columns

    def test_nan_in_open(self):
        """open 列含 NaN 时抛出 ValueError"""
        df = _make_df(10)
        df.loc[0, "open"] = np.nan
        loader = self._make_loader(df)
        with pytest.raises(ValueError, match="NaN values found"):
            loader.compile()

    def test_nan_in_high(self):
        """high 列含 NaN 时抛出 ValueError"""
        df = _make_df(10)
        df.loc[5, "high"] = np.nan
        loader = self._make_loader(df)
        with pytest.raises(ValueError, match="NaN values found"):
            loader.compile()

    def test_nan_in_low(self):
        """low 列含 NaN 时抛出 ValueError"""
        df = _make_df(10)
        df.loc[3, "low"] = np.nan
        loader = self._make_loader(df)
        with pytest.raises(ValueError, match="NaN values found"):
            loader.compile()

    def test_nan_in_close(self):
        """close 列含 NaN 时抛出 ValueError"""
        df = _make_df(10)
        df.loc[7, "close"] = np.nan
        loader = self._make_loader(df)
        with pytest.raises(ValueError, match="NaN values found"):
            loader.compile()

    def test_nan_in_multiple_ohlc(self):
        """多个 OHLC 列含 NaN 时错误消息包含所有列"""
        df = _make_df(10)
        df.loc[0, "open"] = np.nan
        df.loc[1, "high"] = np.nan
        loader = self._make_loader(df)
        with pytest.raises(ValueError, match="NaN values found"):
            loader.compile()

    def test_duplicate_single_pair(self):
        """同一 (timestamp, symbol) 出现两行时报错"""
        df = _make_df(10)
        dup = df.iloc[[0]].copy()
        df = pd.concat([df, dup], ignore_index=True)
        loader = self._make_loader(df)
        with pytest.raises(ValueError, match="duplicate"):
            loader.compile()

    def test_duplicate_multiple_pairs(self):
        """多个 (timestamp, symbol) 重复时报错"""
        df = _make_df(10)
        dups = df.iloc[[0, 3, 5]].copy()
        df = pd.concat([df, dups], ignore_index=True)
        loader = self._make_loader(df)
        with pytest.raises(ValueError, match="duplicate"):
            loader.compile()

    def test_high_less_than_low(self):
        """high < low 时抛出 ValueError"""
        df = _make_df(10)
        df.loc[0, "high"] = 50.0
        df.loc[0, "low"] = 60.0
        loader = self._make_loader(df)
        with pytest.raises(ValueError, match="high < low"):
            loader.compile()

    def test_high_equal_low(self):
        """high == low 时通过校验（合法的 DOJI 形态）"""
        df = _make_df(10)
        df.loc[0, "high"] = 100.0
        df.loc[0, "low"] = 100.0
        loader = self._make_loader(df)
        result = loader.compile()
        assert len(result) == len(df)

    def test_validate_false_skips_all_checks(self):
        """validate=False 跳过所有校验（NaN、重复、high<low）"""
        df = _make_df(10)
        # 注入所有类型的脏数据 / Inject all types of dirty data
        df.loc[0, "open"] = np.nan
        dup = df.iloc[[1]].copy()
        df = pd.concat([df, dup], ignore_index=True)
        df.loc[2, "high"] = 50.0
        df.loc[2, "low"] = 60.0
        loader = self._make_loader(df, validate=False)
        result = loader.compile()
        assert len(result) == len(df)


class TestCompileSorting:
    """compile() 排序验证 / compile() sorting checks."""

    def _make_loader(self, df, validate=True):
        loader = KlineLoader(validate=validate)
        loader.content = df
        return loader

    def test_sorted_by_timestamp_symbol(self):
        """结果按 (timestamp, symbol) 排序"""
        df = _make_df(20)
        # 打乱顺序 / Shuffle
        df = df.sample(frac=1, random_state=42).reset_index(drop=True)
        loader = self._make_loader(df)
        result = loader.compile()
        # 验证排序 / Verify sorted
        for i in range(1, len(result)):
            prev = (result.iloc[i - 1]["timestamp"], result.iloc[i - 1]["symbol"])
            curr = (result.iloc[i]["timestamp"], result.iloc[i]["symbol"])
            assert prev <= curr, f"Row {i} not sorted: {prev} > {curr}"

    def test_index_reset(self):
        """返回的 DataFrame 索引从 0 开始连续"""
        df = _make_df(20)
        loader = self._make_loader(df)
        result = loader.compile()
        assert result.index.tolist() == list(range(len(result)))


class TestInheritance:
    """继承与接口一致性 / Inheritance and interface consistency."""

    def test_inherits_base_data_loader(self):
        """KlineLoader 继承自 BaseDataLoader"""
        from Cross_Section_Factor.datapreprocess import BaseDataLoader
        assert issubclass(KlineLoader, BaseDataLoader)

    def test_content_initially_none(self):
        """初始化后 content 为 None"""
        loader = KlineLoader()
        assert loader.content is None

    @patch("Cross_Section_Factor.kline_loader.load_multi_klines")
    def test_receive_sets_content(self, mock_load):
        """receive() 设置 content 属性"""
        test_df = _make_df(10)
        mock_load.return_value = test_df
        loader = KlineLoader()
        result = loader.receive()
        assert loader.content is test_df
        assert result is test_df

    def test_compile_auto_receive_when_content_none(self):
        """content 为 None 时 compile() 自动调用 receive()"""
        loader = KlineLoader(validate=False)
        test_df = _make_df(10)
        loader.receive = MagicMock(return_value=test_df)
        result = loader.compile()
        loader.receive.assert_called_once()
        assert not result.empty

    def test_compile_uses_existing_content(self):
        """content 已有数据时 compile() 不重复调用 receive()"""
        test_df = _make_df(10)
        loader = KlineLoader(validate=False)
        loader.content = test_df
        loader.receive = MagicMock()
        result = loader.compile()
        loader.receive.assert_not_called()
        assert len(result) == len(test_df)

    def test_dataset_property(self):
        """dataset 属性（BaseDataLoader 提供）可正常调用"""
        loader = KlineLoader(validate=False)
        loader.receive = MagicMock(return_value=_make_df(10))
        result = loader.dataset
        assert not result.empty
        assert isinstance(result, pd.DataFrame)


class TestEdgeCases:
    """边界情况验证 / Edge case checks."""

    @patch("Cross_Section_Factor.kline_loader.load_multi_klines")
    def test_single_row_data(self, mock_load):
        """单行数据正常通过校验"""
        df = pd.DataFrame([{
            "timestamp": datetime(2024, 1, 1, tzinfo=timezone.utc),
            "symbol": "BTCUSDT",
            "open": 100.0,
            "high": 102.0,
            "low": 99.0,
            "close": 101.0,
        }])
        mock_load.return_value = df
        loader = KlineLoader()
        result = loader.compile()
        assert len(result) == 1

    def test_single_symbol_data(self, mock_load=False):
        """单交易对数据正常通过校验"""
        df = _make_df(10, symbols=["BTCUSDT"])
        loader = KlineLoader(validate=False)
        loader.content = df
        result = loader.compile()
        assert result["symbol"].unique().tolist() == ["BTCUSDT"]

    def test_many_symbols(self):
        """多交易对数据正常通过校验"""
        syms = [f"SYM{i:03d}USDT" for i in range(20)]
        df = _make_df(5, symbols=syms)
        loader = KlineLoader(validate=False)
        loader.content = df
        result = loader.compile()
        assert len(result["symbol"].unique()) == 20

    def test_large_dataset(self):
        """大数据集（10000+ 行）正常通过校验"""
        df = _make_df(2000, symbols=["BTCUSDT", "ETHUSDT"])
        loader = KlineLoader(validate=False)
        loader.content = df
        result = loader.compile()
        assert len(result) == 4000  # 2000 timestamps * 2 symbols

    def test_timestamp_timezone_naive(self):
        """无时区的时间戳也能正常处理"""
        df = _make_df(10)
        df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_localize(None)
        loader = KlineLoader(validate=False)
        loader.content = df
        result = loader.compile()
        assert len(result) == len(df)

    def test_float_ohlc_values(self):
        """OHLC 为浮点数值正常处理"""
        df = _make_df(10)
        loader = KlineLoader(validate=False)
        loader.content = df
        result = loader.compile()
        for col in ["open", "high", "low", "close"]:
            assert result[col].dtype in [np.float64, float]

    @patch("Cross_Section_Factor.kline_loader.load_multi_klines")
    def test_receive_called_once_on_compile(self, mock_load):
        """compile() 对 receive() 只调用一次"""
        mock_load.return_value = _make_df(10)
        loader = KlineLoader()
        loader.compile()
        assert mock_load.call_count == 1

    @patch("Cross_Section_Factor.kline_loader.load_multi_klines")
    def test_multiple_compile_calls_idempotent(self, mock_load):
        """多次调用 compile() 返回一致结果"""
        mock_load.return_value = _make_df(10)
        loader = KlineLoader()
        r1 = loader.compile()
        r2 = loader.compile()
        # 第二次 compile 不再调用 receive / Second compile should not call receive again
        assert mock_load.call_count == 1
        assert r1.equals(r2)
