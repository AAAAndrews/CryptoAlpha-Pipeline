"""
Task 8 验证测试 — calc_ic_stats IC 统计显著性指标
Verification tests for calc_ic_stats IC statistical significance metrics.
"""

import warnings

import numpy as np
import pandas as pd
import pytest

from FactorAnalysis.metrics import calc_ic_stats


# ---------------------------------------------------------------------------
# Helpers / 辅助函数
# ---------------------------------------------------------------------------

def _make_factor_returns(n_days=100, n_symbols=50, seed=42, ic_strength=0.3):
    """
    生成具有可控相关性的因子值和收益率数据。
    Generate factor and returns data with controllable correlation strength.
    """
    rng = np.random.RandomState(seed)
    timestamps = pd.date_range("2024-01-01", periods=n_days, freq="D")
    symbols = [f"S{i:03d}" for i in range(n_symbols)]

    idx = pd.MultiIndex.from_product([timestamps, symbols], names=["timestamp", "symbol"])
    # 因子值 / factor values
    factor = pd.Series(rng.randn(n_days * n_symbols), index=idx, name="factor")
    # 收益率 = ic_strength * factor + noise / returns = ic_strength * factor + noise
    noise = pd.Series(rng.randn(n_days * n_symbols) * np.sqrt(1 - ic_strength**2), index=idx)
    returns = ic_strength * factor + noise
    returns.name = "returns"

    return factor, returns


def _make_weak_factor_returns(n_days=100, n_symbols=50, seed=99, ic_strength=0.02):
    """生成弱因子数据 / Generate weak factor data."""
    return _make_factor_returns(n_days, n_symbols, seed, ic_strength)


# ---------------------------------------------------------------------------
# 1. 导入与基础检查 / Import and basic checks
# ---------------------------------------------------------------------------

class TestImport:
    """导入和类型检查 / Import and type checks."""

    def test_importable(self):
        """calc_ic_stats 可从 metrics 模块导入 / calc_ic_stats is importable."""
        assert callable(calc_ic_stats)

    def test_returns_series(self):
        """返回类型为 pd.Series / Return type is pd.Series."""
        f, r = _make_factor_returns()
        result = calc_ic_stats(f, r)
        assert isinstance(result, pd.Series)

    def test_required_fields(self):
        """返回包含所有 7 个必需字段 / Returns all 7 required fields."""
        f, r = _make_factor_returns()
        result = calc_ic_stats(f, r)
        expected_fields = {"IC_mean", "IC_std", "ICIR", "t_stat", "p_value", "IC_skew", "IC_kurtosis"}
        assert set(result.index) == expected_fields


# ---------------------------------------------------------------------------
# 2. 强因子统计合理性 / Strong factor statistics reasonableness
# ---------------------------------------------------------------------------

class TestStrongFactor:
    """强因子（IC≈0.3）下的统计行为 / Statistics behavior under strong factor (IC≈0.3)."""

    @pytest.fixture
    def strong_data(self):
        return _make_factor_returns(n_days=200, n_symbols=80, ic_strength=0.4)

    def test_ic_mean_positive(self, strong_data):
        """强因子 IC 均值应显著为正 / Strong factor IC mean should be significantly positive."""
        f, r = strong_data
        result = calc_ic_stats(f, r)
        assert result["IC_mean"] > 0.1

    def test_icir_reasonable(self, strong_data):
        """强因子 ICIR 应大于 1.0 / Strong factor ICIR should be > 1.0."""
        f, r = strong_data
        result = calc_ic_stats(f, r)
        assert result["ICIR"] > 1.0

    def test_p_value_small(self, strong_data):
        """强因子 p_value 应小于 0.05 / Strong factor p_value should be < 0.05."""
        f, r = strong_data
        result = calc_ic_stats(f, r)
        assert result["p_value"] < 0.05

    def test_t_stat_positive(self, strong_data):
        """强因子 t_stat 应为正值 / Strong factor t_stat should be positive."""
        f, r = strong_data
        result = calc_ic_stats(f, r)
        assert result["t_stat"] > 0

    def test_ic_std_positive(self, strong_data):
        """IC 标准差应为正数 / IC std should be positive."""
        f, r = strong_data
        result = calc_ic_stats(f, r)
        assert result["IC_std"] > 0


# ---------------------------------------------------------------------------
# 3. 弱因子统计合理性 / Weak factor statistics reasonableness
# ---------------------------------------------------------------------------

class TestWeakFactor:
    """弱因子（IC≈0.02）下的统计行为 / Statistics behavior under weak factor (IC≈0.02)."""

    @pytest.fixture
    def weak_data(self):
        return _make_weak_factor_returns(n_days=200, n_symbols=80, ic_strength=0.02)

    def test_ic_mean_near_zero(self, weak_data):
        """弱因子 IC 均值应接近零 / Weak factor IC mean should be near zero."""
        f, r = weak_data
        result = calc_ic_stats(f, r)
        assert abs(result["IC_mean"]) < 0.15

    def test_p_value_not_significant(self, weak_data):
        """弱因子 p_value 通常不显著（> 0.05）/ Weak factor p_value usually not significant."""
        f, r = weak_data
        result = calc_ic_stats(f, r)
        # 注意：随机数据偶有显著情况，此测试允许一定概率失败
        # note: random data may occasionally be significant
        assert result["p_value"] > 0.01


# ---------------------------------------------------------------------------
# 4. ICIR 计算 / ICIR calculation
# ---------------------------------------------------------------------------

class TestICIR:
    """ICIR 计算逻辑验证 / ICIR calculation logic verification."""

    def test_icir_equals_mean_over_std(self):
        """ICIR 应等于 IC_mean / IC_std / ICIR should equal IC_mean / IC_std."""
        f, r = _make_factor_returns(n_days=200, n_symbols=50, ic_strength=0.3)
        result = calc_ic_stats(f, r)
        if result["IC_std"] != 0:
            expected_icir = result["IC_mean"] / result["IC_std"]
            assert abs(result["ICIR"] - expected_icir) < 1e-10

    def test_icir_nan_when_std_zero(self):
        """当 IC 标准差为零时 ICIR 应为 NaN / ICIR should be NaN when IC std is zero."""
        # 所有因子值相同时 IC 不变 / constant factor values give constant IC
        n_days, n_symbols = 10, 5
        timestamps = pd.date_range("2024-01-01", periods=n_days, freq="D")
        symbols = [f"S{i}" for i in range(n_symbols)]
        idx = pd.MultiIndex.from_product([timestamps, symbols], names=["timestamp", "symbol"])
        # 每天因子值全相同 → IC 为 NaN（无法计算相关系数）
        # same factor value per day → IC is NaN
        factor = pd.Series(1.0, index=idx)
        returns = pd.Series(np.random.randn(n_days * n_symbols), index=idx)
        # 这种情况下 IC 全部为 NaN，会触发 insufficient data 警告
        # this case gives all-NaN IC, triggers insufficient data warning
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = calc_ic_stats(factor, returns)
        assert np.isnan(result["ICIR"])


# ---------------------------------------------------------------------------
# 5. 分布统计 / Distribution statistics
# ---------------------------------------------------------------------------

class TestDistribution:
    """IC 偏度和峰度验证 / IC skewness and kurtosis verification."""

    def test_skew_range(self):
        """偏度值应在合理范围内 / Skewness should be in reasonable range."""
        f, r = _make_factor_returns(n_days=200, n_symbols=50)
        result = calc_ic_stats(f, r)
        # 偏度通常在 [-3, 3] 范围内 / skewness typically in [-3, 3]
        assert -3 < result["IC_skew"] < 3

    def test_kurtosis_range(self):
        """峰度值应在合理范围内 / Kurtosis should be in reasonable range."""
        f, r = _make_factor_returns(n_days=200, n_symbols=50)
        result = calc_ic_stats(f, r)
        # 峰度通常在 [-10, 10] 范围内 / kurtosis typically in [-10, 10]
        assert -10 < result["IC_kurtosis"] < 10

    def test_all_values_are_float(self):
        """所有返回值应为 float 类型 / All return values should be float."""
        f, r = _make_factor_returns()
        result = calc_ic_stats(f, r)
        for val in result.values:
            assert isinstance(val, (float, np.floating)), f"Expected float, got {type(val)}"


# ---------------------------------------------------------------------------
# 6. 边界情况 / Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    """边界情况处理 / Edge case handling."""

    def test_empty_factor_raises(self):
        """空因子应抛出 ValueError / Empty factor should raise ValueError."""
        with pytest.raises(ValueError, match="must not be empty"):
            calc_ic_stats(pd.Series([], dtype=float), pd.Series([1.0, 2.0]))

    def test_empty_returns_raises(self):
        """空收益率应抛出 ValueError / Empty returns should raise ValueError."""
        with pytest.raises(ValueError, match="must not be empty"):
            calc_ic_stats(pd.Series([1.0, 2.0]), pd.Series([], dtype=float))

    def test_non_series_factor_raises(self):
        """非 pd.Series 类型的 factor 应抛出 TypeError / Non-Series factor should raise TypeError."""
        with pytest.raises(TypeError, match="factor must be pd.Series"):
            calc_ic_stats([1, 2, 3], pd.Series([1.0, 2.0, 3.0]))

    def test_non_series_returns_raises(self):
        """非 pd.Series 类型的 returns 应抛出 TypeError / Non-Series returns should raise TypeError."""
        with pytest.raises(TypeError, match="returns must be pd.Series"):
            calc_ic_stats(pd.Series([1.0, 2.0, 3.0]), np.array([1, 2, 3]))

    def test_insufficient_data_warns(self):
        """数据不足（<3 个有效 IC）时应发出 UserWarning / Should warn when <3 valid ICs."""
        # 仅 2 天数据 → 最多 2 个有效 IC
        # only 2 days → at most 2 valid ICs
        f, r = _make_factor_returns(n_days=2, n_symbols=3)
        with pytest.warns(UserWarning, match="valid observations"):
            result = calc_ic_stats(f, r)
        # 所有值应为 NaN / all values should be NaN
        assert result.isna().all()

    def test_single_symbol_warns(self):
        """单交易对（每截面仅 1 个点无法计算相关系数）应返回全 NaN / Single symbol should return all NaN."""
        f, r = _make_factor_returns(n_days=10, n_symbols=1)
        # 每个时间截面只有 1 个点，corr 返回 NaN
        # only 1 point per cross-section, corr returns NaN
        with pytest.warns(UserWarning, match="valid observations"):
            result = calc_ic_stats(f, r)
        assert result.isna().all()

    def test_two_symbols_warns(self):
        """2 个交易对时每个截面仅能计算 1 个相关系数，但数据量不足 / 2 symbols insufficient."""
        f, r = _make_factor_returns(n_days=2, n_symbols=2)
        with pytest.warns(UserWarning, match="valid observations"):
            result = calc_ic_stats(f, r)
        assert result.isna().all()
