"""
tests/test_task19_rebalance_verify.py — rebalance_freq 多调仓频率衰减深度验证测试

Task 19: 多调仓频率衰减验证测试
验证 rebalance_freq=1 与原始行为一致、rebalance_freq=5/10 产出不同曲线、
非调仓日收益沿用逻辑、rebalance_freq 参数校验、衰减效应、确定性等。
"""

import numpy as np
import pandas as pd
import pytest

from FactorAnalysis.grouping import quantile_group
from FactorAnalysis.portfolio import (
    _calc_labels_with_rebalance,
    calc_long_only_curve,
    calc_short_only_curve,
    calc_top_bottom_curve,
)


# ============================================================
# 测试数据构建 / Test data fixtures
# ============================================================

def _make_factor_returns(
    n_days: int = 100,
    n_assets: int = 20,
    seed: int = 42,
    signal_strength: float = 0.1,
) -> tuple[pd.Series, pd.Series]:
    """构建带信号的因子和收益数据 / Build factor & returns with signal."""
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2024-01-01", periods=n_days, freq="B")
    symbols = [f"SYM{i:03d}" for i in range(n_assets)]
    idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])

    true_signal = rng.standard_normal((n_days, n_assets)).flatten()
    noise = rng.standard_normal((n_days, n_assets)) * 0.5
    factor_values = true_signal * signal_strength + noise.flatten()

    ret_noise = rng.standard_normal((n_days, n_assets)).flatten() * 0.01
    returns_values = true_signal * signal_strength * 0.3 + ret_noise

    factor = pd.Series(factor_values, index=idx, name="factor")
    returns = pd.Series(returns_values, index=idx, name="returns")
    return factor, returns


# ============================================================
# 1. 返回结构与类型深度验证 / Return structure deep validation
# ============================================================

class TestReturnTypeDeep:
    """返回类型与结构深度验证 / Deep return type and structure validation."""

    @pytest.fixture
    def data(self):
        return _make_factor_returns()

    def test_index_is_datetime(self, data):
        """返回值索引为 datetime 类型."""
        factor, returns = data
        result = calc_long_only_curve(factor, returns, rebalance_freq=5)
        assert pd.api.types.is_datetime64_any_dtype(result.index)

    @pytest.mark.parametrize("func", [calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve])
    def test_index_is_datetime_all_funcs(self, func, data):
        """三个函数返回值索引均为 datetime."""
        factor, returns = data
        result = func(factor, returns, rebalance_freq=5)
        assert pd.api.types.is_datetime64_any_dtype(result.index)

    @pytest.mark.parametrize("func", [calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve])
    def test_curve_length_matches_unique_timestamps(self, func, data):
        """曲线长度等于唯一时间戳数."""
        factor, returns = data
        n_ts = factor.index.get_level_values(0).nunique()
        result = func(factor, returns, rebalance_freq=7)
        assert len(result) == n_ts

    def test_cumprod_consistency_long_only(self, data):
        """long_only 净值曲线等于 (1 + daily_return) 累积乘积."""
        factor, returns = data
        result = calc_long_only_curve(factor, returns, rebalance_freq=5)
        daily_ret = result.pct_change().fillna(0)
        manual_curve = (1 + daily_ret).cumprod()
        manual_curve.iloc[0] = 1.0
        np.testing.assert_allclose(result.values, manual_curve.values, atol=1e-10)

    def test_cumprod_consistency_short_only(self, data):
        """short_only 净值曲线等于 (1 + daily_return) 累积乘积."""
        factor, returns = data
        result = calc_short_only_curve(factor, returns, rebalance_freq=5)
        daily_ret = result.pct_change().fillna(0)
        manual_curve = (1 + daily_ret).cumprod()
        manual_curve.iloc[0] = 1.0
        np.testing.assert_allclose(result.values, manual_curve.values, atol=1e-10)

    def test_cumprod_consistency_top_bottom(self, data):
        """top_bottom 净值曲线等于 (1 + daily_return) 累积乘积."""
        factor, returns = data
        result = calc_top_bottom_curve(factor, returns, rebalance_freq=5)
        daily_ret = result.pct_change().fillna(0)
        manual_curve = (1 + daily_ret).cumprod()
        manual_curve.iloc[0] = 1.0
        np.testing.assert_allclose(result.values, manual_curve.values, atol=1e-10)


# ============================================================
# 2. rebalance_freq=1 向后兼容深度验证 / Backward compatibility deep
# ============================================================

class TestBackwardCompatibilityDeep:
    """rebalance_freq=1 与原始行为深度一致验证 / Deep backward compatibility."""

    @pytest.mark.parametrize("seed", [10, 42, 77, 99])
    def test_long_only_freq1_exact_match(self, seed):
        """不同种子下 rebalance_freq=1 与默认值精确一致."""
        factor, returns = _make_factor_returns(n_days=60, n_assets=15, seed=seed)
        curve_default = calc_long_only_curve(factor, returns, n_groups=5, top_k=1)
        curve_freq1 = calc_long_only_curve(factor, returns, n_groups=5, top_k=1, rebalance_freq=1)
        np.testing.assert_allclose(curve_default.values, curve_freq1.values, atol=1e-12)

    @pytest.mark.parametrize("seed", [10, 42, 77, 99])
    def test_short_only_freq1_exact_match(self, seed):
        """不同种子下 short_only rebalance_freq=1 精确一致."""
        factor, returns = _make_factor_returns(n_days=60, n_assets=15, seed=seed)
        curve_default = calc_short_only_curve(factor, returns, n_groups=5, bottom_k=1)
        curve_freq1 = calc_short_only_curve(factor, returns, n_groups=5, bottom_k=1, rebalance_freq=1)
        np.testing.assert_allclose(curve_default.values, curve_freq1.values, atol=1e-12)

    @pytest.mark.parametrize("seed", [10, 42, 77, 99])
    def test_top_bottom_freq1_exact_match(self, seed):
        """不同种子下 top_bottom rebalance_freq=1 精确一致."""
        factor, returns = _make_factor_returns(n_days=60, n_assets=15, seed=seed)
        curve_default = calc_top_bottom_curve(factor, returns, n_groups=5)
        curve_freq1 = calc_top_bottom_curve(factor, returns, n_groups=5, rebalance_freq=1)
        np.testing.assert_allclose(curve_default.values, curve_freq1.values, atol=1e-12)

    @pytest.mark.parametrize("n_groups", [2, 3, 5, 10])
    def test_long_only_different_n_groups(self, n_groups):
        """不同分组数下 rebalance_freq=1 与默认值一致."""
        factor, returns = _make_factor_returns(n_days=50, n_assets=20)
        curve_default = calc_long_only_curve(factor, returns, n_groups=n_groups, top_k=1)
        curve_freq1 = calc_long_only_curve(factor, returns, n_groups=n_groups, top_k=1, rebalance_freq=1)
        np.testing.assert_allclose(curve_default.values, curve_freq1.values, atol=1e-12)


# ============================================================
# 3. 不同调仓频率产出不同曲线深度验证 / Different freqs deep
# ============================================================

class TestDifferentFreqsDeep:
    """不同 rebalance_freq 产出不同曲线的深度验证 / Deep different freqs validation."""

    @pytest.mark.parametrize("func", [calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve])
    def test_freq_pairwise_different(self, func):
        """freq=1/3/5/10/20 两两不同."""
        factor, returns = _make_factor_returns(n_days=100, n_assets=20, signal_strength=0.15)
        freqs = [1, 3, 5, 10, 20]
        curves = {f: func(factor, returns, rebalance_freq=f) for f in freqs}
        for i in range(len(freqs)):
            for j in range(i + 1, len(freqs)):
                assert not np.allclose(
                    curves[freqs[i]].values, curves[freqs[j]].values, atol=1e-12,
                ), f"freq={freqs[i]} and freq={freqs[j]} produce identical curves"

    @pytest.mark.parametrize("func", [calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve])
    def test_all_freqs_positive(self, func):
        """所有频率下净值曲线值 > 0."""
        factor, returns = _make_factor_returns(n_days=100, n_assets=20)
        for freq in [1, 2, 3, 5, 7, 10, 15, 20, 50]:
            curve = func(factor, returns, rebalance_freq=freq)
            assert (curve > 0).all(), f"freq={freq} has non-positive values"

    @pytest.mark.parametrize("func", [calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve])
    def test_all_freqs_start_at_one(self, func):
        """所有频率下起始值为 1.0."""
        factor, returns = _make_factor_returns(n_days=100, n_assets=20)
        for freq in [1, 5, 10, 20]:
            curve = func(factor, returns, rebalance_freq=freq)
            assert abs(curve.iloc[0] - 1.0) < 1e-10


# ============================================================
# 4. 收益率衰减特征验证 / Return decay characteristics
# ============================================================

class TestDecayCharacteristics:
    """验证不同调仓频率下收益率的变化特征 / Verify return characteristics across freqs."""

    def test_large_freq_converges_to_hold(self):
        """非常大的 rebalance_freq 曲线接近一次性调仓."""
        factor, returns = _make_factor_returns(n_days=50, n_assets=10, signal_strength=0.2)
        curve_50 = calc_long_only_curve(factor, returns, rebalance_freq=50)
        curve_inf = calc_long_only_curve(factor, returns, rebalance_freq=100)
        # freq=50 和 freq=100 应该非常接近（几乎都是只调仓一次）
        np.testing.assert_allclose(curve_50.values, curve_inf.values, atol=1e-8)

    def test_all_three_funcs_converge_at_large_freq(self):
        """三个函数在极大 rebalance_freq 下曲线与 freq=总天数 一致."""
        factor, returns = _make_factor_returns(n_days=30, n_assets=10)
        n_days = factor.index.get_level_values(0).nunique()
        for func in [calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve]:
            curve_n = func(factor, returns, rebalance_freq=n_days)
            curve_2n = func(factor, returns, rebalance_freq=n_days * 2)
            np.testing.assert_allclose(curve_n.values, curve_2n.values, atol=1e-8)

    def test_daily_returns_bounded(self):
        """不同频率下日收益率在合理范围内（无极端值）."""
        factor, returns = _make_factor_returns(n_days=100, n_assets=20)
        for func in [calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve]:
            for freq in [1, 5, 10, 20]:
                curve = func(factor, returns, rebalance_freq=freq)
                daily_ret = curve.pct_change().dropna()
                # 等权组合日收益率通常在 [-50%, +50%] 范围内
                assert (daily_ret.abs() < 0.5).all(), (
                    f"{func.__name__} freq={freq} has extreme daily return"
                )

    def test_curve_end_value_different_across_freqs(self):
        """不同频率下曲线终值不同."""
        factor, returns = _make_factor_returns(n_days=100, n_assets=20, signal_strength=0.15)
        end_values = {}
        for freq in [1, 5, 10, 20]:
            curve = calc_long_only_curve(factor, returns, rebalance_freq=freq)
            end_values[freq] = curve.iloc[-1]
        # 至少有两个频率的终值不同
        unique_ends = set(f"{v:.6f}" for v in end_values.values())
        assert len(unique_ends) > 1, "All frequencies produce identical end values"


# ============================================================
# 5. 标签沿用逻辑深度验证 / Label carry-forward deep verification
# ============================================================

class TestLabelCarryForwardDeep:
    """非调仓日沿用标签的深度验证 / Deep label carry-forward verification."""

    def test_rebalance_date_labels_match_quantile_group(self):
        """调仓日的标签与直接调用 quantile_group 完全一致."""
        factor, _ = _make_factor_returns(n_days=30, n_assets=10, seed=42)
        n_groups = 5
        rebalance_freq = 7

        labels = _calc_labels_with_rebalance(factor, n_groups, rebalance_freq)
        timestamps = factor.index.get_level_values(0).unique().sort_values()
        rebalance_dates = timestamps[::rebalance_freq]

        # 调仓日直接计算 quantile_group
        reb_mask = factor.index.get_level_values(0).isin(rebalance_dates)
        expected_labels = quantile_group(factor[reb_mask], n_groups=n_groups)

        # 沿用标签在调仓日应完全匹配
        np.testing.assert_array_equal(
            labels.loc[expected_labels.index].values,
            expected_labels.values,
        )

    def test_no_label_change_within_rebalance_period(self):
        """调仓周期内每个资产的标签不变."""
        factor, _ = _make_factor_returns(n_days=50, n_assets=15, seed=42)
        n_groups = 5
        rebalance_freq = 10

        labels = _calc_labels_with_rebalance(factor, n_groups, rebalance_freq)
        timestamps = factor.index.get_level_values(0).unique().sort_values()
        symbols = factor.index.get_level_values(1).unique()

        for sym in symbols:
            sym_labels = labels.xs(sym, level=1)
            for reb_start_idx in range(0, len(timestamps), rebalance_freq):
                reb_end_idx = min(reb_start_idx + rebalance_freq, len(timestamps))
                period_labels = sym_labels.iloc[reb_start_idx:reb_end_idx]
                # 周期内非 NaN 标签应全部相同
                valid_labels = period_labels.dropna()
                if len(valid_labels) > 0:
                    assert valid_labels.nunique() == 1, (
                        f"Symbol {sym} period [{reb_start_idx}, {reb_end_idx}) "
                        f"has {valid_labels.nunique()} distinct labels"
                    )

    def test_labels_change_between_rebalance_periods(self):
        """不同调仓周期之间标签可能不同."""
        factor, _ = _make_factor_returns(n_days=100, n_assets=20, seed=42)
        n_groups = 5
        rebalance_freq = 20

        labels = _calc_labels_with_rebalance(factor, n_groups, rebalance_freq)

        # 统计每个资产在相邻调仓日之间标签是否发生变化
        timestamps = factor.index.get_level_values(0).unique().sort_values()
        rebalance_dates = timestamps[::rebalance_freq]
        symbols = factor.index.get_level_values(1).unique()

        # 至少有一个资产在某个调仓日发生标签变化
        any_changed = False
        for sym in symbols[:10]:
            sym_labels = labels.xs(sym, level=1)
            prev_label = None
            for rd in rebalance_dates:
                lbl = sym_labels.loc[rd]
                if prev_label is not None and not np.isnan(lbl) and not np.isnan(prev_label):
                    if lbl != prev_label:
                        any_changed = True
                        break
                prev_label = lbl
            if any_changed:
                break

        assert any_changed, "Expected at least one symbol to change labels between rebalance periods"

    def test_first_period_all_labeled(self):
        """第一个调仓周期内所有资产都应有标签（无 NaN）."""
        factor, _ = _make_factor_returns(n_days=50, n_assets=15, seed=42)
        n_groups = 5
        rebalance_freq = 10

        labels = _calc_labels_with_rebalance(factor, n_groups, rebalance_freq)
        timestamps = factor.index.get_level_values(0).unique().sort_values()
        first_period = timestamps[:rebalance_freq]

        period_labels = labels[labels.index.get_level_values(0).isin(first_period)]
        assert period_labels.notna().all(), "First rebalance period should have no NaN labels"


# ============================================================
# 6. rebalance_freq 参数校验深度验证 / Parameter validation deep
# ============================================================

class TestParamValidationDeep:
    """rebalance_freq 参数校验深度验证 / Deep parameter validation."""

    @pytest.fixture
    def data(self):
        return _make_factor_returns()

    @pytest.mark.parametrize("func", [calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve])
    def test_rebalance_freq_none_raises(self, func, data):
        """rebalance_freq=None 抛出 TypeError."""
        factor, returns = data
        with pytest.raises(TypeError, match="rebalance_freq"):
            func(factor, returns, rebalance_freq=None)

    @pytest.mark.parametrize("func", [calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve])
    def test_rebalance_freq_bool_accepted_as_int(self, func, data):
        """rebalance_freq=True（bool 是 int 子类）被接受，行为同 freq=1."""
        factor, returns = data
        result = func(factor, returns, rebalance_freq=True)
        assert isinstance(result, pd.Series)

    @pytest.mark.parametrize("func", [calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve])
    def test_rebalance_freq_list_raises(self, func, data):
        """rebalance_freq=[5]（list）抛出 TypeError."""
        factor, returns = data
        with pytest.raises(TypeError, match="rebalance_freq"):
            func(factor, returns, rebalance_freq=[5])

    @pytest.mark.parametrize("func", [calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve])
    def test_rebalance_freq_very_large_valid(self, func, data):
        """rebalance_freq=10000 不抛异常."""
        factor, returns = data
        result = func(factor, returns, rebalance_freq=10000)
        assert isinstance(result, pd.Series)


# ============================================================
# 7. 边界情况深度验证 / Edge cases deep
# ============================================================

class TestEdgeCasesDeep:
    """边界情况深度验证 / Deep edge case validation."""

    def test_nan_in_factor(self):
        """因子含 NaN 时所有频率正常."""
        factor, returns = _make_factor_returns(n_days=30, n_assets=10)
        factor.iloc[:15] = np.nan
        for freq in [1, 5, 10]:
            result = calc_long_only_curve(factor, returns, rebalance_freq=freq)
            assert isinstance(result, pd.Series)
            assert abs(result.iloc[0] - 1.0) < 1e-10

    def test_nan_in_returns(self):
        """收益含 NaN 时所有频率正常."""
        factor, returns = _make_factor_returns(n_days=30, n_assets=10)
        returns.iloc[:15] = np.nan
        for freq in [1, 5, 10]:
            result = calc_long_only_curve(factor, returns, rebalance_freq=freq)
            assert isinstance(result, pd.Series)

    def test_inf_in_factor(self):
        """因子含 inf 时所有频率正常."""
        factor, returns = _make_factor_returns(n_days=30, n_assets=10)
        factor.iloc[0] = np.inf
        factor.iloc[-1] = -np.inf
        for freq in [1, 5, 10]:
            result = calc_long_only_curve(factor, returns, rebalance_freq=freq)
            assert isinstance(result, pd.Series)

    def test_inf_in_returns(self):
        """收益含 inf 时所有频率正常."""
        factor, returns = _make_factor_returns(n_days=30, n_assets=10)
        returns.iloc[0] = np.inf
        for freq in [1, 5, 10]:
            result = calc_long_only_curve(factor, returns, rebalance_freq=freq)
            assert isinstance(result, pd.Series)

    def test_very_few_days(self):
        """极少交易日（3 天）下正常工作."""
        factor, returns = _make_factor_returns(n_days=3, n_assets=5)
        for func in [calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve]:
            for freq in [1, 2, 3]:
                result = func(factor, returns, rebalance_freq=freq)
                assert isinstance(result, pd.Series)

    def test_rebalance_freq_equals_two(self):
        """rebalance_freq=2（最小有效多日频率）正常工作."""
        factor, returns = _make_factor_returns(n_days=20, n_assets=10)
        for func in [calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve]:
            result = func(factor, returns, rebalance_freq=2)
            assert isinstance(result, pd.Series)
            assert abs(result.iloc[0] - 1.0) < 1e-10

    def test_large_top_k_with_rebalance(self):
        """top_k=4, n_groups=5 与 rebalance_freq 组合正常."""
        factor, returns = _make_factor_returns(n_days=50, n_assets=20)
        result = calc_long_only_curve(
            factor, returns, n_groups=5, top_k=4, rebalance_freq=5,
        )
        assert isinstance(result, pd.Series)

    def test_large_bottom_k_with_rebalance(self):
        """bottom_k=4, n_groups=5 与 rebalance_freq 组合正常."""
        factor, returns = _make_factor_returns(n_days=50, n_assets=20)
        result = calc_short_only_curve(
            factor, returns, n_groups=5, bottom_k=4, rebalance_freq=5,
        )
        assert isinstance(result, pd.Series)

    def test_top_bottom_both_k_with_rebalance(self):
        """top_k=2 + bottom_k=2 与 rebalance_freq 组合正常."""
        factor, returns = _make_factor_returns(n_days=50, n_assets=20)
        result = calc_top_bottom_curve(
            factor, returns, n_groups=5, top_k=2, bottom_k=2, rebalance_freq=7,
        )
        assert isinstance(result, pd.Series)

    def test_constant_factor_all_freqs(self):
        """常量因子在不同频率下不崩溃."""
        factor, returns = _make_factor_returns(n_days=50, n_assets=10)
        constant_factor = pd.Series(1.0, index=factor.index)
        for func in [calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve]:
            for freq in [1, 5, 10, 25]:
                result = func(constant_factor, returns, rebalance_freq=freq)
                assert isinstance(result, pd.Series)

    def test_pure_noise_all_freqs(self):
        """纯噪声数据在不同频率下不崩溃."""
        rng = np.random.default_rng(123)
        dates = pd.date_range("2024-01-01", periods=50, freq="B")
        symbols = [f"A{i}" for i in range(10)]
        idx = pd.MultiIndex.from_product([dates, symbols])
        factor = pd.Series(rng.standard_normal(len(idx)), index=idx)
        returns = pd.Series(rng.standard_normal(len(idx)) * 0.01, index=idx)
        for func in [calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve]:
            for freq in [1, 5, 10]:
                result = func(factor, returns, rebalance_freq=freq)
                assert isinstance(result, pd.Series)


# ============================================================
# 8. 多种子稳定性 / Multi-seed stability
# ============================================================

class TestStability:
    """多种子稳定性验证 / Multi-seed stability tests."""

    @pytest.mark.parametrize("seed", [10, 20, 42, 77, 99, 123, 200, 314])
    def test_long_only_all_seeds(self, seed):
        """不同种子下 calc_long_only_curve(rebalance_freq=5) 正常."""
        factor, returns = _make_factor_returns(n_days=60, n_assets=15, seed=seed)
        result = calc_long_only_curve(factor, returns, rebalance_freq=5)
        assert isinstance(result, pd.Series)
        assert abs(result.iloc[0] - 1.0) < 1e-10
        assert (result > 0).all()

    @pytest.mark.parametrize("seed", [10, 20, 42, 77, 99, 123, 200, 314])
    def test_short_only_all_seeds(self, seed):
        """不同种子下 calc_short_only_curve(rebalance_freq=5) 正常."""
        factor, returns = _make_factor_returns(n_days=60, n_assets=15, seed=seed)
        result = calc_short_only_curve(factor, returns, rebalance_freq=5)
        assert isinstance(result, pd.Series)
        assert abs(result.iloc[0] - 1e-10) < 1.0  # start near 1.0

    @pytest.mark.parametrize("seed", [10, 20, 42, 77, 99, 123, 200, 314])
    def test_top_bottom_all_seeds(self, seed):
        """不同种子下 calc_top_bottom_curve(rebalance_freq=5) 正常."""
        factor, returns = _make_factor_returns(n_days=60, n_assets=15, seed=seed)
        result = calc_top_bottom_curve(factor, returns, rebalance_freq=5)
        assert isinstance(result, pd.Series)
        assert abs(result.iloc[0] - 1.0) < 1e-10

    @pytest.mark.parametrize("seed", [10, 42, 77])
    def test_deterministic_same_seed(self, seed):
        """相同种子产出完全一致的结果（确定性）."""
        factor1, returns1 = _make_factor_returns(n_days=50, n_assets=15, seed=seed)
        factor2, returns2 = _make_factor_returns(n_days=50, n_assets=15, seed=seed)
        result1 = calc_long_only_curve(factor1, returns1, rebalance_freq=7)
        result2 = calc_long_only_curve(factor2, returns2, rebalance_freq=7)
        np.testing.assert_array_equal(result1.values, result2.values)

    @pytest.mark.parametrize("seed", [10, 42, 77, 99])
    def test_curves_differ_across_seeds(self, seed):
        """不同种子下 freq=1 和 freq=10 产出不同曲线."""
        factor, returns = _make_factor_returns(n_days=100, n_assets=20, seed=seed, signal_strength=0.15)
        curve_1 = calc_long_only_curve(factor, returns, rebalance_freq=1)
        curve_10 = calc_long_only_curve(factor, returns, rebalance_freq=10)
        assert not np.allclose(curve_1.values, curve_10.values, atol=1e-12)

    @pytest.mark.parametrize("seed", [10, 42, 77])
    def test_all_seeds_all_funcs_all_freqs(self, seed):
        """不同种子下三个函数 × 多频率全部正常."""
        factor, returns = _make_factor_returns(n_days=60, n_assets=15, seed=seed)
        for func in [calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve]:
            for freq in [1, 3, 5, 10]:
                result = func(factor, returns, rebalance_freq=freq)
                assert isinstance(result, pd.Series)
                assert (result > 0).all()
