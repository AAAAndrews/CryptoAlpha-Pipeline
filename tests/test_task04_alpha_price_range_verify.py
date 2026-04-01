"""
Task 4 验证测试：Alpha3 价格振幅因子 AlphaPriceRange 综合验证

Task 4 verification tests: AlphaPriceRange comprehensive verification
covering inheritance, factor value shape, boundary cases (high==low → NaN),
registry integration, and public exports.
"""

import numpy as np
import pandas as pd
import pytest

from FactorLib.alpha_price_range import AlphaPriceRange
from FactorLib.base import BaseFactor


# ---- 测试数据构造 / Test data builders ----

def _make_df(n=50):
    """
    生成模拟行情 DataFrame，包含 OHLC 列。

    Generate mock market DataFrame with OHLC columns.
    """
    timestamps = pd.date_range("2024-01-01", periods=n, freq="1h")
    np.random.seed(42)
    base = 100.0
    close = base + np.random.randn(n) * 5
    open_ = base + np.random.randn(n) * 5
    high = np.maximum(open_, close) + np.abs(np.random.randn(n)) * 2
    low = np.minimum(open_, close) - np.abs(np.random.randn(n)) * 2
    return pd.DataFrame({
        "timestamp": timestamps,
        "symbol": "BTCUSDT",
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
    })


def _make_doji_df(n=5):
    """
    生成 high == low 的十字星数据（分母为零边界）。

    Generate doji data where high == low (division-by-zero boundary).
    """
    timestamps = pd.date_range("2024-01-01", periods=n, freq="1h")
    return pd.DataFrame({
        "timestamp": timestamps,
        "symbol": "BTCUSDT",
        "open": 100.0,
        "high": 100.0,
        "low": 100.0,
        "close": 100.0,
    })


class TestInheritance:
    """继承关系验证 / Inheritance checks."""

    def test_is_instance_of_base_factor(self):
        """AlphaPriceRange 是 BaseFactor 的实例"""
        factor = AlphaPriceRange()
        assert isinstance(factor, BaseFactor)

    def test_cannot_instantiate_base_factor(self):
        """BaseFactor 是抽象类，不能直接实例化"""
        with pytest.raises(TypeError):
            BaseFactor()

    def test_has_calculate_method(self):
        """必须实现 calculate 方法"""
        factor = AlphaPriceRange()
        assert callable(factor.calculate)

    def test_name_attribute(self):
        """name 属性默认为类名"""
        factor = AlphaPriceRange()
        assert factor.name == "AlphaPriceRange"

    def test_repr(self):
        """__repr__ 输出格式正确"""
        factor = AlphaPriceRange()
        r = repr(factor)
        assert "AlphaPriceRange" in r
        assert "AlphaPriceRange" in r


class TestFactorValueShape:
    """因子值形状与类型验证 / Factor value shape and type checks."""

    def test_returns_series(self):
        """calculate 返回 pd.Series"""
        factor = AlphaPriceRange()
        data = _make_df()
        result = factor.calculate(data)
        assert isinstance(result, pd.Series)

    def test_length_matches_input(self):
        """输出长度与输入一致"""
        factor = AlphaPriceRange()
        data = _make_df(n=30)
        result = factor.calculate(data)
        assert len(result) == len(data)

    def test_index_aligned(self):
        """输出索引与输入一致"""
        factor = AlphaPriceRange()
        data = _make_df(n=20)
        result = factor.calculate(data)
        assert (result.index == data.index).all()

    def test_series_name(self):
        """Series name 等于因子名称"""
        factor = AlphaPriceRange()
        data = _make_df()
        result = factor.calculate(data)
        assert result.name == "AlphaPriceRange"


class TestCalculation:
    """因子计算正确性验证 / Factor calculation correctness checks."""

    def test_basic_formula(self):
        """基本公式：(open - close) / (high - low)"""
        factor = AlphaPriceRange()
        # 构造精确数据：open=110, close=100, high=120, low=90
        # expected = (110 - 100) / (120 - 90) = 10 / 30 = 1/3
        data = pd.DataFrame({
            "open": [110.0], "high": [120.0], "low": [90.0], "close": [100.0],
        })
        result = factor.calculate(data)
        np.testing.assert_allclose(result.values[0], 10.0 / 30.0)

    def test_bullish_signal(self):
        """看涨信号：close > open 时因子值为负"""
        factor = AlphaPriceRange()
        # open=100, close=110 → open-close = -10 → 负值
        data = pd.DataFrame({
            "open": [100.0], "high": [120.0], "low": [90.0], "close": [110.0],
        })
        result = factor.calculate(data)
        assert result.values[0] < 0

    def test_bearish_signal(self):
        """看跌信号：close < open 时因子值为正"""
        factor = AlphaPriceRange()
        # open=110, close=100 → open-close = 10 → 正值
        data = pd.DataFrame({
            "open": [110.0], "high": [120.0], "low": [90.0], "close": [100.0],
        })
        result = factor.calculate(data)
        assert result.values[0] > 0

    def test_value_range(self):
        """因子值在 [-1, 1] 范围内"""
        factor = AlphaPriceRange()
        data = _make_df(n=200)
        result = factor.calculate(data)
        valid = result.dropna()
        assert (valid >= -1).all() and (valid <= 1).all()

    def test_close_at_high(self):
        """close == high 时因子值 = (open - high) / (high - low)"""
        factor = AlphaPriceRange()
        # open=100, high=120, low=90, close=120
        # expected = (100 - 120) / (120 - 90) = -20/30 = -2/3
        data = pd.DataFrame({
            "open": [100.0], "high": [120.0], "low": [90.0], "close": [120.0],
        })
        result = factor.calculate(data)
        np.testing.assert_allclose(result.values[0], -20.0 / 30.0)


class TestBoundaryCases:
    """边界情况验证 / Boundary case checks."""

    def test_high_equals_low_returns_nan(self):
        """high == low 时返回 NaN"""
        factor = AlphaPriceRange()
        data = _make_doji_df(n=5)
        result = factor.calculate(data)
        assert result.isna().all()

    def test_mixed_doji_normal(self):
        """混合数据中，doji 行返回 NaN，正常行返回数值"""
        factor = AlphaPriceRange()
        data = pd.DataFrame({
            "open": [100.0, 100.0],
            "high": [100.0, 120.0],
            "low": [100.0, 90.0],
            "close": [100.0, 110.0],
        })
        result = factor.calculate(data)
        assert pd.isna(result.iloc[0])
        assert not pd.isna(result.iloc[1])

    def test_single_row(self):
        """单行数据正常计算"""
        factor = AlphaPriceRange()
        data = pd.DataFrame({
            "open": [105.0], "high": [110.0], "low": [100.0], "close": [102.0],
        })
        result = factor.calculate(data)
        assert len(result) == 1
        np.testing.assert_allclose(result.values[0], 3.0 / 10.0)

    def test_multiple_symbols(self):
        """多交易对数据正常计算"""
        factor = AlphaPriceRange()
        data = pd.DataFrame({
            "open": [100.0, 10.0, 50.0],
            "high": [120.0, 12.0, 60.0],
            "low": [90.0, 8.0, 40.0],
            "close": [110.0, 11.0, 55.0],
        })
        result = factor.calculate(data)
        assert len(result) == 3
        assert result.isna().sum() == 0


class TestRegistryIntegration:
    """注册表集成验证 / Registry integration checks."""

    def test_registered_in_global_registry(self):
        """AlphaPriceRange 已注册到全局注册表"""
        from FactorLib.registry import get
        cls = get("AlphaPriceRange")
        assert cls is not None
        assert cls is AlphaPriceRange

    def test_list_factors_contains(self):
        """list_factors 包含 AlphaPriceRange"""
        from FactorLib.registry import list_factors
        names = list_factors()
        assert "AlphaPriceRange" in names

    def test_registry_instance_works(self):
        """通过注册表获取的类可以正常实例化并计算"""
        from FactorLib.registry import get
        cls = get("AlphaPriceRange")
        factor = cls()
        data = _make_df(n=10)
        result = factor.calculate(data)
        assert isinstance(result, pd.Series)
        assert len(result) == 10


class TestPublicExports:
    """公共导出验证 / Public export checks."""

    def test_export_from_module(self):
        """可从 FactorLib.alpha_price_range 导入"""
        from FactorLib.alpha_price_range import AlphaPriceRange
        assert AlphaPriceRange is not None

    def test_export_from_package(self):
        """可从 FactorLib 包直接导入"""
        from FactorLib import AlphaPriceRange
        assert AlphaPriceRange is not None

    def test_package_all_contains(self):
        """FactorLib.__all__ 包含 AlphaPriceRange"""
        from FactorLib import __all__
        assert "AlphaPriceRange" in __all__
