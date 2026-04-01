"""
tests/test_task18_rebalance.py — rebalance_freq 多调仓频率衰减验证测试

Task 18: 在 portfolio.py 的三个净值曲线函数中新增 rebalance_freq 参数
"""

import numpy as np
import pandas as pd
import pytest

from FactorAnalysis.portfolio import (
    calc_long_only_curve,
    calc_short_only_curve,
    calc_top_bottom_curve,
    _calc_labels_with_rebalance,
)


# ============================================================
# 测试数据构建 / Test data fixtures
# ============================================================

def _make_factor_returns(
    n_days: int = 100,
    n_assets: int = 20,
    seed: int = 42,
    signal_strength: float = 0.05,
) -> tuple[pd.Series, pd.Series]:
    """构建带微弱信号的因子和收益数据 / Build factor & returns with weak signal."""
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2024-01-01", periods=n_days, freq="B")
    symbols = [f"SYM{i:03d}" for i in range(n_assets)]
    idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])

    # 因子值含信号 + 噪声 / factor with signal + noise
    true_signal = rng.standard_normal((n_days, n_assets)).flatten()
    noise = rng.standard_normal((n_days, n_assets)) * 0.5
    factor_values = true_signal * signal_strength + noise.flatten()

    # 收益 = 因子信号 + 噪声 / returns = factor signal + noise
    ret_noise = rng.standard_normal((n_days, n_assets)).flatten() * 0.01
    returns_values = true_signal * signal_strength * 0.3 + ret_noise

    factor = pd.Series(factor_values, index=idx, name="factor")
    returns = pd.Series(returns_values, index=idx, name="returns")
    return factor, returns


# ============================================================
# 1. 导入与基础检查 / Import & basic checks
# ============================================================

class TestImport:
    """导入与基础校验 / Import and basic validation."""

    def test_import_long_only(self):
        """calc_long_only_curve 可正常导入."""
        from FactorAnalysis.portfolio import calc_long_only_curve
        assert callable(calc_long_only_curve)

    def test_import_short_only(self):
        """calc_short_only_curve 可正常导入."""
        from FactorAnalysis.portfolio import calc_short_only_curve
        assert callable(calc_short_only_curve)

    def test_import_top_bottom(self):
        """calc_top_bottom_curve 可正常导入."""
        from FactorAnalysis.portfolio import calc_top_bottom_curve
        assert callable(calc_top_bottom_curve)

    def test_helper_import(self):
        """_calc_labels_with_rebalance 内部辅助函数可导入."""
        from FactorAnalysis.portfolio import _calc_labels_with_rebalance
        assert callable(_calc_labels_with_rebalance)


# ============================================================
# 2. 返回结构与类型 / Return structure & type
# ============================================================

class TestReturnType:
    """返回类型与结构验证 / Return type and structure validation."""

    @pytest.fixture
    def data(self):
        return _make_factor_returns()

    def test_long_only_returns_series(self, data):
        """calc_long_only_curve 返回 pd.Series."""
        factor, returns = data
        result = calc_long_only_curve(factor, returns, rebalance_freq=5)
        assert isinstance(result, pd.Series)

    def test_short_only_returns_series(self, data):
        """calc_short_only_curve 返回 pd.Series."""
        factor, returns = data
        result = calc_short_only_curve(factor, returns, rebalance_freq=5)
        assert isinstance(result, pd.Series)

    def test_top_bottom_returns_series(self, data):
        """calc_top_bottom_curve 返回 pd.Series."""
        factor, returns = data
        result = calc_top_bottom_curve(factor, returns, rebalance_freq=5)
        assert isinstance(result, pd.Series)

    def test_index_is_timestamp_not_multiindex(self, data):
        """返回值索引为单层 timestamp，非 MultiIndex."""
        factor, returns = data
        for func in [calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve]:
            result = func(factor, returns, rebalance_freq=3)
            assert not isinstance(result.index, pd.MultiIndex)

    def test_start_value_is_one(self, data):
        """净值曲线起始值为 1.0."""
        factor, returns = data
        for func in [calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve]:
            result = func(factor, returns, rebalance_freq=5)
            assert abs(result.iloc[0] - 1.0) < 1e-10

    def test_curve_length_matches_timestamps(self, data):
        """曲线长度等于唯一时间戳数."""
        factor, returns = data
        n_ts = factor.index.get_level_values(0).nunique()
        for func in [calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve]:
            result = func(factor, returns, rebalance_freq=5)
            assert len(result) == n_ts


# ============================================================
# 3. rebalance_freq=1 向后兼容 / Backward compatibility
# ============================================================

class TestBackwardCompatibility:
    """rebalance_freq=1（默认值）与原始行为一致 / rebalance_freq=1 matches original behavior."""

    @pytest.fixture
    def data(self):
        return _make_factor_returns(n_days=50, n_assets=15, seed=42)

    def test_long_only_freq1_equals_default(self, data):
        """calc_long_only_curve: rebalance_freq=1 与不传参数结果一致."""
        factor, returns = data
        curve_default = calc_long_only_curve(factor, returns, n_groups=5, top_k=1)
        curve_freq1 = calc_long_only_curve(factor, returns, n_groups=5, top_k=1, rebalance_freq=1)
        np.testing.assert_allclose(curve_default.values, curve_freq1.values, atol=1e-12)

    def test_short_only_freq1_equals_default(self, data):
        """calc_short_only_curve: rebalance_freq=1 与不传参数结果一致."""
        factor, returns = data
        curve_default = calc_short_only_curve(factor, returns, n_groups=5, bottom_k=1)
        curve_freq1 = calc_short_only_curve(factor, returns, n_groups=5, bottom_k=1, rebalance_freq=1)
        np.testing.assert_allclose(curve_default.values, curve_freq1.values, atol=1e-12)

    def test_top_bottom_freq1_equals_default(self, data):
        """calc_top_bottom_curve: rebalance_freq=1 与不传参数结果一致."""
        factor, returns = data
        curve_default = calc_top_bottom_curve(factor, returns, n_groups=5)
        curve_freq1 = calc_top_bottom_curve(factor, returns, n_groups=5, rebalance_freq=1)
        np.testing.assert_allclose(curve_default.values, curve_freq1.values, atol=1e-12)


# ============================================================
# 4. rebalance_freq 参数校验 / Parameter validation
# ============================================================

class TestParamValidation:
    """rebalance_freq 参数校验 / rebalance_freq parameter validation."""

    @pytest.fixture
    def data(self):
        return _make_factor_returns()

    @pytest.mark.parametrize("func", [calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve])
    def test_rebalance_freq_zero_raises(self, func, data):
        """rebalance_freq=0 抛出 ValueError."""
        factor, returns = data
        with pytest.raises(ValueError, match="rebalance_freq"):
            func(factor, returns, rebalance_freq=0)

    @pytest.mark.parametrize("func", [calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve])
    def test_rebalance_freq_negative_raises(self, func, data):
        """rebalance_freq=-1 抛出 ValueError."""
        factor, returns = data
        with pytest.raises(ValueError, match="rebalance_freq"):
            func(factor, returns, rebalance_freq=-1)

    @pytest.mark.parametrize("func", [calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve])
    def test_rebalance_freq_float_raises(self, func, data):
        """rebalance_freq=2.5（非整数）抛出 TypeError."""
        factor, returns = data
        with pytest.raises(TypeError, match="rebalance_freq"):
            func(factor, returns, rebalance_freq=2.5)

    @pytest.mark.parametrize("func", [calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve])
    def test_rebalance_freq_string_raises(self, func, data):
        """rebalance_freq='5'（字符串）抛出 TypeError."""
        factor, returns = data
        with pytest.raises(TypeError, match="rebalance_freq"):
            func(factor, returns, rebalance_freq="5")


# ============================================================
# 5. 不同调仓频率产出不同曲线 / Different freq → different curves
# ============================================================

class TestDifferentFreqs:
    """rebalance_freq=1/5/10 产出不同净值曲线 / Different rebalance_freq produce different curves."""

    @pytest.fixture
    def data(self):
        return _make_factor_returns(n_days=100, n_assets=20, signal_strength=0.1)

    @pytest.mark.parametrize("func", [calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve])
    def test_freq5_differs_from_freq1(self, func, data):
        """rebalance_freq=5 的曲线与 rebalance_freq=1 不同."""
        factor, returns = data
        curve_1 = func(factor, returns, rebalance_freq=1)
        curve_5 = func(factor, returns, rebalance_freq=5)
        # 不应完全相同
        assert not np.allclose(curve_1.values, curve_5.values, atol=1e-12)

    @pytest.mark.parametrize("func", [calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve])
    def test_freq10_differs_from_freq1(self, func, data):
        """rebalance_freq=10 的曲线与 rebalance_freq=1 不同."""
        factor, returns = data
        curve_1 = func(factor, returns, rebalance_freq=1)
        curve_10 = func(factor, returns, rebalance_freq=10)
        assert not np.allclose(curve_1.values, curve_10.values, atol=1e-12)

    @pytest.mark.parametrize("func", [calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve])
    def test_freq5_differs_from_freq10(self, func, data):
        """rebalance_freq=5 和 rebalance_freq=10 产出不同曲线."""
        factor, returns = data
        curve_5 = func(factor, returns, rebalance_freq=5)
        curve_10 = func(factor, returns, rebalance_freq=10)
        assert not np.allclose(curve_5.values, curve_10.values, atol=1e-12)

    @pytest.mark.parametrize("func", [calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve])
    def test_all_positive_values(self, func, data):
        """不同调仓频率下净值曲线值 > 0."""
        factor, returns = data
        for freq in [1, 5, 10]:
            curve = func(factor, returns, rebalance_freq=freq)
            assert (curve > 0).all(), f"freq={freq} has non-positive values"


# ============================================================
# 6. 标签沿用逻辑验证 / Label carry-forward logic
# ============================================================

class TestLabelCarryForward:
    """验证非调仓日沿用上一个调仓日的分组标签 / Verify non-rebalance days carry forward labels."""

    def test_helper_freq1_same_as_quantile_group(self):
        """_calc_labels_with_rebalance(freq=1) 与 quantile_group 结果一致."""
        from FactorAnalysis.grouping import quantile_group
        factor, _ = _make_factor_returns(n_days=30, n_assets=10, seed=42)
        labels_orig = quantile_group(factor, n_groups=5)
        labels_helper = _calc_labels_with_rebalance(factor, n_groups=5, rebalance_freq=1)
        np.testing.assert_array_equal(labels_orig.values, labels_helper.values)

    def test_labels_forward_fill_within_symbol(self):
        """非调仓日的标签等于上一个调仓日的标签."""
        factor, _ = _make_factor_returns(n_days=20, n_assets=5, seed=42)
        n_groups = 3
        rebalance_freq = 5

        labels = _calc_labels_with_rebalance(factor, n_groups, rebalance_freq)

        timestamps = factor.index.get_level_values(0).unique().sort_values()
        rebalance_dates = timestamps[::rebalance_freq]

        # 抽查每个资产的标签沿用
        symbols = factor.index.get_level_values(1).unique()
        for sym in symbols[:3]:  # 抽查前 3 个资产
            for ts in timestamps:
                if ts in rebalance_dates:
                    # 调仓日本身应有标签
                    lbl = labels.loc[(ts, sym)]
                    assert not np.isnan(lbl), f"Rebalance date {ts} sym {sym} has NaN label"
                else:
                    # 非调仓日：找到上一个调仓日
                    reb_before = rebalance_dates[rebalance_dates <= ts]
                    if len(reb_before) > 0:
                        last_reb = reb_before[-1]
                        lbl_current = labels.loc[(ts, sym)]
                        lbl_reb = labels.loc[(last_reb, sym)]
                        assert lbl_current == lbl_reb, (
                            f"Label carry-forward failed: sym={sym}, "
                            f"ts={ts}, last_reb={last_reb}"
                        )

    def test_labels_change_at_rebalance_dates(self):
        """调仓日之间的标签应该一致，不同调仓周期标签应可能不同."""
        factor, _ = _make_factor_returns(n_days=30, n_assets=10, seed=42)
        n_groups = 3
        rebalance_freq = 10

        labels = _calc_labels_with_rebalance(factor, n_groups, rebalance_freq)

        timestamps = factor.index.get_level_values(0).unique().sort_values()
        rebalance_dates = timestamps[::rebalance_freq]

        # 第一个调仓周期（day 0-9）内标签不变
        period1_labels = labels.loc[
            labels.index.get_level_values(0).isin(timestamps[0:10])
        ]
        unique_labels_p1 = period1_labels.dropna().unique()
        # 每个资产在周期内标签应一致
        for sym in factor.index.get_level_values(1).unique()[:5]:
            sym_labels = period1_labels.loc[
                period1_labels.index.get_level_values(1) == sym
            ]
            assert sym_labels.nunique() <= 1, (
                f"Symbol {sym} has changing labels within rebalance period"
            )


# ============================================================
# 7. 边界情况 / Edge cases
# ============================================================

class TestEdgeCases:
    """边界情况验证 / Edge case validation."""

    def test_rebalance_freq_larger_than_timestamps(self):
        """rebalance_freq 大于时间戳数时仍正常工作（仅调仓一次）."""
        factor, returns = _make_factor_returns(n_days=10, n_assets=5)
        for func in [calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve]:
            result = func(factor, returns, rebalance_freq=100)
            assert isinstance(result, pd.Series)
            assert abs(result.iloc[0] - 1.0) < 1e-10

    def test_rebalance_freq_equals_timestamps(self):
        """rebalance_freq 等于时间戳数时仅调仓一次."""
        factor, returns = _make_factor_returns(n_days=10, n_assets=5)
        for func in [calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve]:
            result = func(factor, returns, rebalance_freq=10)
            assert isinstance(result, pd.Series)

    def test_small_data(self):
        """小数据量正常工作."""
        factor, returns = _make_factor_returns(n_days=5, n_assets=3)
        for func in [calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve]:
            result = func(factor, returns, rebalance_freq=2)
            assert isinstance(result, pd.Series)

    def test_two_groups(self):
        """n_groups=2 最小分组正常工作."""
        factor, returns = _make_factor_returns(n_days=30, n_assets=10)
        for func in [calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve]:
            result = func(factor, returns, n_groups=2, rebalance_freq=5)
            assert isinstance(result, pd.Series)

    def test_top_k_greater_than_one(self):
        """top_k > 1 与 rebalance_freq 组合正常."""
        factor, returns = _make_factor_returns(n_days=50, n_assets=20)
        result = calc_long_only_curve(factor, returns, n_groups=5, top_k=2, rebalance_freq=5)
        assert isinstance(result, pd.Series)
        assert abs(result.iloc[0] - 1.0) < 1e-10

    def test_bottom_k_greater_than_one(self):
        """bottom_k > 1 与 rebalance_freq 组合正常."""
        factor, returns = _make_factor_returns(n_days=50, n_assets=20)
        result = calc_short_only_curve(factor, returns, n_groups=5, bottom_k=2, rebalance_freq=5)
        assert isinstance(result, pd.Series)
        assert abs(result.iloc[0] - 1.0) < 1e-10

    def test_constant_factor(self):
        """常量因子不崩溃."""
        factor, returns = _make_factor_returns(n_days=20, n_assets=10)
        constant_factor = pd.Series(1.0, index=factor.index)
        for func in [calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve]:
            result = func(constant_factor, returns, rebalance_freq=5)
            assert isinstance(result, pd.Series)

    def test_no_signal_data(self):
        """纯噪声数据不崩溃."""
        rng = np.random.default_rng(123)
        dates = pd.date_range("2024-01-01", periods=30, freq="B")
        symbols = [f"A{i}" for i in range(10)]
        idx = pd.MultiIndex.from_product([dates, symbols])
        factor = pd.Series(rng.standard_normal(len(idx)), index=idx)
        returns = pd.Series(rng.standard_normal(len(idx)) * 0.01, index=idx)
        for func in [calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve]:
            result = func(factor, returns, rebalance_freq=7)
            assert isinstance(result, pd.Series)


# ============================================================
# 8. 多种子稳定性 / Multi-seed stability
# ============================================================

class TestStability:
    """多种子稳定性验证 / Multi-seed stability tests."""

    @pytest.mark.parametrize("seed", [10, 20, 30, 42, 99])
    def test_long_only_all_seeds(self, seed):
        """不同种子下 calc_long_only_curve(rebalance_freq=5) 正常."""
        factor, returns = _make_factor_returns(n_days=50, n_assets=15, seed=seed)
        result = calc_long_only_curve(factor, returns, rebalance_freq=5)
        assert isinstance(result, pd.Series)
        assert abs(result.iloc[0] - 1.0) < 1e-10
        assert (result > 0).all()

    @pytest.mark.parametrize("seed", [10, 20, 30, 42, 99])
    def test_short_only_all_seeds(self, seed):
        """不同种子下 calc_short_only_curve(rebalance_freq=5) 正常."""
        factor, returns = _make_factor_returns(n_days=50, n_assets=15, seed=seed)
        result = calc_short_only_curve(factor, returns, rebalance_freq=5)
        assert isinstance(result, pd.Series)
        assert abs(result.iloc[0] - 1.0) < 1e-10

    @pytest.mark.parametrize("seed", [10, 20, 30, 42, 99])
    def test_top_bottom_all_seeds(self, seed):
        """不同种子下 calc_top_bottom_curve(rebalance_freq=5) 正常."""
        factor, returns = _make_factor_returns(n_days=50, n_assets=15, seed=seed)
        result = calc_top_bottom_curve(factor, returns, rebalance_freq=5)
        assert isinstance(result, pd.Series)
        assert abs(result.iloc[0] - 1.0) < 1e-10

    @pytest.mark.parametrize("freq", [1, 2, 5, 10, 20])
    def test_long_only_all_freqs(self, freq):
        """不同调仓频率均正常."""
        factor, returns = _make_factor_returns(n_days=60, n_assets=15)
        result = calc_long_only_curve(factor, returns, rebalance_freq=freq)
        assert isinstance(result, pd.Series)
        assert abs(result.iloc[0] - 1.0) < 1e-10
