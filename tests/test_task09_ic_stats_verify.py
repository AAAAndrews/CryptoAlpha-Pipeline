"""
Task 9 验证测试 — calc_ic_stats IC 统计显著性深度验证
Deep verification tests for calc_ic_stats IC statistical significance metrics.

与 task 8 单元测试互补，聚焦于：
- 返回类型与字段完整性的一致性
- t_stat / p_value 在多种子、强弱因子、负相关因子下的合理性
- skew / kurtosis 数值范围与统计一致性
- 边界情况：NaN 数据、单日数据、全零因子、极值因子
"""

import warnings

import numpy as np
import pandas as pd
import pytest

from FactorAnalysis.metrics import calc_ic_stats, calc_ic


# ---------------------------------------------------------------------------
# Helpers / 辅助函数
# ---------------------------------------------------------------------------

def _make_data(n_days=100, n_symbols=50, seed=42, ic_strength=0.3):
    """
    生成可控相关性的因子值和收益率。
    Generate factor and returns with controllable correlation strength.
    """
    rng = np.random.RandomState(seed)
    timestamps = pd.date_range("2024-01-01", periods=n_days, freq="D")
    symbols = [f"S{i:03d}" for i in range(n_symbols)]
    idx = pd.MultiIndex.from_product([timestamps, symbols], names=["timestamp", "symbol"])
    factor = pd.Series(rng.randn(n_days * n_symbols), index=idx, name="factor")
    noise = pd.Series(rng.randn(n_days * n_symbols) * np.sqrt(1 - ic_strength**2), index=idx)
    returns = ic_strength * factor + noise
    returns.name = "returns"
    return factor, returns


# ---------------------------------------------------------------------------
# 1. 返回类型与字段完整性 / Return type and field completeness
# ---------------------------------------------------------------------------

class TestReturnTypeAndFields:
    """返回类型和字段完整性验证 / Verify return type and field completeness."""

    def test_return_type_is_series(self):
        """返回类型必须是 pd.Series / Must return pd.Series."""
        f, r = _make_data()
        result = calc_ic_stats(f, r)
        assert isinstance(result, pd.Series)

    def test_all_seven_fields_present(self):
        """必须包含全部 7 个字段 / All 7 fields must be present."""
        f, r = _make_data()
        result = calc_ic_stats(f, r)
        expected = {"IC_mean", "IC_std", "ICIR", "t_stat", "p_value", "IC_skew", "IC_kurtosis"}
        assert set(result.index) == expected

    def test_field_order_consistent(self):
        """字段顺序应与实现一致 / Field order should be consistent."""
        f, r = _make_data()
        result = calc_ic_stats(f, r)
        expected_order = ["IC_mean", "IC_std", "ICIR", "t_stat", "p_value", "IC_skew", "IC_kurtosis"]
        assert list(result.index) == expected_order

    def test_values_are_float_or_nan(self):
        """所有值应为 float 或 np.nan / All values should be float or NaN."""
        f, r = _make_data()
        result = calc_ic_stats(f, r)
        for val in result.values:
            assert isinstance(val, (float, np.floating)) or np.isnan(val)

    def test_result_deterministic_same_seed(self):
        """相同种子应产生相同结果 / Same seed should produce same result."""
        f1, r1 = _make_data(seed=123)
        f2, r2 = _make_data(seed=123)
        res1 = calc_ic_stats(f1, r1)
        res2 = calc_ic_stats(f2, r2)
        pd.testing.assert_series_equal(res1, res2)


# ---------------------------------------------------------------------------
# 2. t_stat / p_value 强因子合理性 / t_stat/p_value strong factor reasonableness
# ---------------------------------------------------------------------------

class TestStrongFactorStats:
    """强因子（高 IC）下 t_stat 和 p_value 的合理性 / t_stat/p_value under strong factor."""

    @pytest.fixture(params=[0.3, 0.5, 0.7])
    def strong_data(self, request):
        """多种强因子强度 / Multiple strong factor strengths."""
        return _make_data(n_days=200, n_symbols=80, seed=42, ic_strength=request.param)

    def test_ic_mean_significantly_positive(self, strong_data):
        """强因子 IC_mean 应显著为正 / Strong factor IC_mean should be significantly positive."""
        f, r = strong_data
        result = calc_ic_stats(f, r)
        assert result["IC_mean"] > 0.05

    def test_t_stat_positive(self, strong_data):
        """强因子 t_stat 应为正值 / Strong factor t_stat should be positive."""
        f, r = strong_data
        result = calc_ic_stats(f, r)
        assert result["t_stat"] > 0

    def test_p_value_below_threshold(self, strong_data):
        """强因子 p_value 应 < 0.05 / Strong factor p_value should be < 0.05."""
        f, r = strong_data
        result = calc_ic_stats(f, r)
        assert result["p_value"] < 0.05

    def test_t_stat_equals_manual_calc(self, strong_data):
        """t_stat 应等于 IC_mean / (IC_std / sqrt(n)) / t_stat = IC_mean / (IC_std / sqrt(n))."""
        f, r = strong_data
        result = calc_ic_stats(f, r)
        # 手动计算 t 统计量 / manually compute t-stat
        ic_series = calc_ic(f, r).dropna()
        n = len(ic_series)
        if result["IC_std"] > 0:
            manual_t = result["IC_mean"] / (result["IC_std"] / np.sqrt(n))
            assert abs(result["t_stat"] - manual_t) < 1e-8

    def test_ic_std_positive(self, strong_data):
        """IC_std 应严格大于零 / IC_std should be strictly positive."""
        f, r = strong_data
        result = calc_ic_stats(f, r)
        assert result["IC_std"] > 0

    def test_icir_positive(self, strong_data):
        """强因子 ICIR 应为正 / Strong factor ICIR should be positive."""
        f, r = strong_data
        result = calc_ic_stats(f, r)
        assert result["ICIR"] > 0


# ---------------------------------------------------------------------------
# 3. t_stat / p_value 弱因子合理性 / t_stat/p_value weak factor reasonableness
# ---------------------------------------------------------------------------

class TestWeakFactorStats:
    """弱因子（低 IC）下 t_stat 和 p_value 的合理性 / t_stat/p_value under weak factor."""

    @pytest.fixture(params=[0.01, 0.02, 0.03])
    def weak_data(self, request):
        """多种弱因子强度 / Multiple weak factor strengths."""
        return _make_data(n_days=200, n_symbols=80, seed=99, ic_strength=request.param)

    def test_ic_mean_near_zero(self, weak_data):
        """弱因子 IC_mean 应接近零 / Weak factor IC_mean should be near zero."""
        f, r = weak_data
        result = calc_ic_stats(f, r)
        assert abs(result["IC_mean"]) < 0.1

    def test_p_value_not_highly_significant(self, weak_data):
        """弱因子 p_value 通常 > 0.001 / Weak factor p_value usually > 0.001."""
        f, r = weak_data
        result = calc_ic_stats(f, r)
        assert result["p_value"] > 0.001

    def test_t_stat_near_zero(self, weak_data):
        """弱因子 t_stat 应接近零 / Weak factor t_stat should be near zero."""
        f, r = weak_data
        result = calc_ic_stats(f, r)
        assert abs(result["t_stat"]) < 3.0


# ---------------------------------------------------------------------------
# 4. 负相关因子 / Negative correlation factor
# ---------------------------------------------------------------------------

class TestNegativeCorrelation:
    """负相关因子下的统计行为 / Statistics under negatively correlated factor."""

    def test_negative_factor_ic_mean_negative(self):
        """负相关因子 IC_mean 应为负 / Negative factor IC_mean should be negative."""
        f, r = _make_data(n_days=200, n_symbols=80, seed=42, ic_strength=0.4)
        # 翻转因子符号 / flip factor sign
        result = calc_ic_stats(-f, r)
        assert result["IC_mean"] < -0.05

    def test_negative_factor_t_stat_negative(self):
        """负相关因子 t_stat 应为负 / Negative factor t_stat should be negative."""
        f, r = _make_data(n_days=200, n_symbols=80, seed=42, ic_strength=0.4)
        result = calc_ic_stats(-f, r)
        assert result["t_stat"] < 0

    def test_negative_factor_p_value_small(self):
        """负相关因子 p_value 应 < 0.05 / Negative factor p_value should be < 0.05."""
        f, r = _make_data(n_days=200, n_symbols=80, seed=42, ic_strength=0.4)
        result = calc_ic_stats(-f, r)
        assert result["p_value"] < 0.05

    def test_negative_factor_icir_negative(self):
        """负相关因子 ICIR 应为负 / Negative factor ICIR should be negative."""
        f, r = _make_data(n_days=200, n_symbols=80, seed=42, ic_strength=0.4)
        result = calc_ic_stats(-f, r)
        assert result["ICIR"] < 0


# ---------------------------------------------------------------------------
# 5. skew / kurtosis 数值范围 / skew / kurtosis value ranges
# ---------------------------------------------------------------------------

class TestDistributionStats:
    """偏度和峰度的数值范围验证 / Skewness and kurtosis range verification."""

    def test_skew_in_reasonable_range(self):
        """偏度应在 [-3, 3] 范围内 / Skewness should be in [-3, 3]."""
        f, r = _make_data(n_days=200, n_symbols=50)
        result = calc_ic_stats(f, r)
        assert -3 <= result["IC_skew"] <= 3

    def test_kurtosis_in_reasonable_range(self):
        """峰度应在 [-10, 10] 范围内 / Kurtosis should be in [-10, 10]."""
        f, r = _make_data(n_days=200, n_symbols=50)
        result = calc_ic_stats(f, r)
        assert -10 <= result["IC_kurtosis"] <= 10

    def test_skew_multi_seed_stability(self):
        """多种子下偏度不应系统性偏离 / Skewness should not systematically deviate across seeds."""
        skews = []
        for seed in range(10):
            f, r = _make_data(n_days=150, n_symbols=50, seed=seed)
            result = calc_ic_stats(f, r)
            skews.append(result["IC_skew"])
        # 偏度均值应接近零 / mean skewness should be near zero
        assert abs(np.mean(skews)) < 1.5

    def test_kurtosis_multi_seed_stability(self):
        """多种子下峰度均值应在合理范围内 / Mean kurtosis across seeds should be reasonable."""
        kurtosis_vals = []
        for seed in range(10):
            f, r = _make_data(n_days=150, n_symbols=50, seed=seed)
            result = calc_ic_stats(f, r)
            kurtosis_vals.append(result["IC_kurtosis"])
        # 峰度均值应在 [-5, 5] 范围内 / mean kurtosis in [-5, 5]
        assert -5 <= np.mean(kurtosis_vals) <= 5

    def test_skew_kurtosis_finite(self):
        """偏度和峰度必须为有限值 / Skewness and kurtosis must be finite."""
        f, r = _make_data(n_days=200, n_symbols=50)
        result = calc_ic_stats(f, r)
        assert np.isfinite(result["IC_skew"])
        assert np.isfinite(result["IC_kurtosis"])


# ---------------------------------------------------------------------------
# 6. 边界情况 / Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    """边界情况处理验证 / Edge case handling verification."""

    def test_empty_factor_raises_value_error(self):
        """空因子应抛出 ValueError / Empty factor raises ValueError."""
        with pytest.raises(ValueError, match="must not be empty"):
            calc_ic_stats(pd.Series([], dtype=float), pd.Series([1.0]))

    def test_empty_returns_raises_value_error(self):
        """空收益率应抛出 ValueError / Empty returns raises ValueError."""
        with pytest.raises(ValueError, match="must not be empty"):
            calc_ic_stats(pd.Series([1.0]), pd.Series([], dtype=float))

    def test_non_series_factor_raises_type_error(self):
        """非 Series 因子应抛出 TypeError / Non-Series factor raises TypeError."""
        with pytest.raises(TypeError, match="factor must be pd.Series"):
            calc_ic_stats(np.array([1, 2, 3]), pd.Series([1.0, 2.0, 3.0]))

    def test_non_series_returns_raises_type_error(self):
        """非 Series 收益率应抛出 TypeError / Non-Series returns raises TypeError."""
        with pytest.raises(TypeError, match="returns must be pd.Series"):
            calc_ic_stats(pd.Series([1.0, 2.0, 3.0]), [1, 2, 3])

    def test_single_day_warns_and_returns_nan(self):
        """单日数据应发出警告并返回全 NaN / Single day warns and returns all NaN."""
        f, r = _make_data(n_days=1, n_symbols=10)
        with pytest.warns(UserWarning, match="valid observations"):
            result = calc_ic_stats(f, r)
        assert result.isna().all()

    def test_two_days_warns_and_returns_nan(self):
        """两天数据应发出警告并返回全 NaN / Two days warns and returns all NaN."""
        f, r = _make_data(n_days=2, n_symbols=10)
        with pytest.warns(UserWarning, match="valid observations"):
            result = calc_ic_stats(f, r)
        assert result.isna().all()

    def test_single_symbol_warns_and_returns_nan(self):
        """单交易对应发出警告并返回全 NaN / Single symbol warns and returns all NaN."""
        f, r = _make_data(n_days=20, n_symbols=1)
        with pytest.warns(UserWarning, match="valid observations"):
            result = calc_ic_stats(f, r)
        assert result.isna().all()

    def test_constant_factor_warns_and_returns_nan(self):
        """全零因子应发出警告并返回全 NaN / Constant factor warns and returns all NaN."""
        f, r = _make_data(n_days=20, n_symbols=10)
        constant_factor = pd.Series(0.0, index=f.index)
        with pytest.warns(UserWarning, match="valid observations"):
            result = calc_ic_stats(constant_factor, r)
        assert result.isna().all()

    def test_factor_with_nan_rows_still_works(self):
        """因子含部分 NaN 时仍能正常计算 / Factor with partial NaN still works."""
        f, r = _make_data(n_days=100, n_symbols=50)
        # 随机将 10% 的因子值设为 NaN / randomly set 10% factor values to NaN
        rng = np.random.RandomState(42)
        mask = rng.random(len(f)) < 0.1
        f_nan = f.copy()
        f_nan.iloc[mask] = np.nan
        result = calc_ic_stats(f_nan, r)
        assert not result.isna().all()
        assert isinstance(result, pd.Series)

    def test_returns_with_nan_rows_still_works(self):
        """收益率含部分 NaN 时仍能正常计算 / Returns with partial NaN still works."""
        f, r = _make_data(n_days=100, n_symbols=50)
        rng = np.random.RandomState(42)
        mask = rng.random(len(r)) < 0.1
        r_nan = r.copy()
        r_nan.iloc[mask] = np.nan
        result = calc_ic_stats(f, r_nan)
        assert not result.isna().all()
        assert isinstance(result, pd.Series)

    def test_insufficient_ic_returns_all_nan_fields(self):
        """不足 3 个有效 IC 时所有字段均为 NaN / All NaN when <3 valid ICs."""
        f, r = _make_data(n_days=2, n_symbols=5)
        with pytest.warns(UserWarning, match="valid observations"):
            result = calc_ic_stats(f, r)
        assert len(result) == 7
        assert result.isna().all()
