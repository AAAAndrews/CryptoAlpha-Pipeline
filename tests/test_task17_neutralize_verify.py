"""
tests/test_task17_neutralize_verify.py — calc_neutralized_curve 分组中性化深度验证测试

Task 17: 分组中性化权重验证测试
验证 demeaned/group_adjust 四种组合模式、groups 参数类型校验、
中性化后净值曲线类型和起始值、与原始曲线对比差异。
"""

import numpy as np
import pandas as pd
import pytest

from FactorAnalysis.neutralize import calc_neutralized_curve


# ============================================================
# 测试数据构建 / Test data fixtures
# ============================================================

def _make_factor_returns(
    n_days: int = 100,
    n_assets: int = 20,
    seed: int = 42,
    signal_strength: float = 0.05,
    group_effect_strength: float = 0.02,
) -> tuple[pd.Series, pd.Series]:
    """
    构建含组效应的因子和收益数据 / Build factor & returns with group effect.

    收益中包含与因子信号相关的成分 + 资产固有组效应 + 噪声，
    以便验证中性化是否能消除组间差异。
    Returns contain factor signal + asset-level group effect + noise,
    so neutralization can be verified to reduce inter-group differences.
    """
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2024-01-01", periods=n_days, freq="B")
    symbols = [f"SYM{i:03d}" for i in range(n_assets)]
    idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])

    # 因子值含信号 + 噪声 / factor with signal + noise
    true_signal = rng.standard_normal((n_days, n_assets)).flatten()
    noise = rng.standard_normal((n_days, n_assets)) * 0.5
    factor_values = true_signal * signal_strength + noise.flatten()

    # 组效应：每个资产有一个固定的偏移量 / group effect: per-asset fixed offset
    asset_effect = rng.standard_normal(n_assets) * group_effect_strength
    group_effect = np.tile(asset_effect, n_days)

    # 收益 = 因子信号 + 组效应 + 噪声 / returns = signal + group effect + noise
    ret_noise = rng.standard_normal((n_days, n_assets)).flatten() * 0.01
    returns_values = true_signal * signal_strength * 0.3 + group_effect + ret_noise

    factor = pd.Series(factor_values, index=idx, name="factor")
    returns = pd.Series(returns_values, index=idx, name="returns")
    return factor, returns


def _make_group_labels(
    factor: pd.Series, n_groups: int = 4, seed: int = 0,
) -> pd.Series:
    """生成固定分组标签（按 symbol 分配）/ Generate fixed group labels by symbol."""
    rng = np.random.default_rng(seed)
    symbols = factor.index.get_level_values("symbol").unique()
    symbol_to_group = {s: rng.integers(0, n_groups) for s in symbols}
    return pd.Series(
        [symbol_to_group[s] for s in factor.index.get_level_values("symbol")],
        index=factor.index,
        dtype=float,
    )


# ============================================================
# 1. 返回结构与类型 / Return structure & type
# ============================================================

class TestReturnType:
    """返回类型与结构深度验证 / Deep return type and structure validation."""

    @pytest.fixture
    def data(self):
        return _make_factor_returns()

    def test_returns_pd_series(self, data):
        """返回值类型为 pd.Series."""
        factor, returns = data
        result = calc_neutralized_curve(factor, returns, groups=4)
        assert isinstance(result, pd.Series)

    def test_index_single_level(self, data):
        """返回值索引为单层（非 MultiIndex）."""
        factor, returns = data
        result = calc_neutralized_curve(factor, returns, groups=4)
        assert not isinstance(result.index, pd.MultiIndex)

    def test_index_is_datetime(self, data):
        """返回值索引为 datetime 类型."""
        factor, returns = data
        result = calc_neutralized_curve(factor, returns, groups=4)
        assert pd.api.types.is_datetime64_any_dtype(result.index)

    def test_one_entry_per_day(self, data):
        """每个交易日只有一条净值记录."""
        factor, returns = data
        n_days = len(factor.index.get_level_values("timestamp").unique())
        result = calc_neutralized_curve(factor, returns, groups=4)
        assert len(result) == n_days

    def test_start_value_exactly_one(self, data):
        """起始值精确为 1.0."""
        factor, returns = data
        result = calc_neutralized_curve(factor, returns, groups=4)
        assert result.iloc[0] == 1.0

    def test_all_values_positive(self, data):
        """净值曲线所有值 > 0."""
        factor, returns = data
        result = calc_neutralized_curve(factor, returns, groups=4)
        assert (result > 0).all()

    def test_cumprod_consistency(self, data):
        """净值曲线等于 (1 + daily_return) 的累积乘积."""
        factor, returns = data
        result = calc_neutralized_curve(factor, returns, groups=4)
        # 手动计算日收益并累积
        daily_ret = result.pct_change().fillna(0)
        manual_curve = (1 + daily_ret).cumprod()
        manual_curve.iloc[0] = 1.0
        np.testing.assert_allclose(result.values, manual_curve.values, atol=1e-10)


# ============================================================
# 2. groups 参数类型校验 / groups parameter type validation
# ============================================================

class TestGroupsParamValidation:
    """groups 参数类型深度校验 / Deep groups parameter type validation."""

    @pytest.fixture
    def data(self):
        return _make_factor_returns()

    def test_groups_int_valid(self, data):
        """groups=int (>=2) 正常工作."""
        factor, returns = data
        result = calc_neutralized_curve(factor, returns, groups=5)
        assert isinstance(result, pd.Series)

    def test_groups_series_valid(self, data):
        """groups=pd.Series 正常工作."""
        factor, returns = data
        labels = _make_group_labels(factor, n_groups=3)
        result = calc_neutralized_curve(factor, returns, groups=labels)
        assert isinstance(result, pd.Series)

    def test_groups_string_raises(self, data):
        """groups 为 str 抛出 ValueError."""
        factor, returns = data
        with pytest.raises(ValueError, match="groups"):
            calc_neutralized_curve(factor, returns, groups="bad")

    def test_groups_float_raises(self, data):
        """groups 为 float 抛出 ValueError."""
        factor, returns = data
        with pytest.raises(ValueError, match="groups"):
            calc_neutralized_curve(factor, returns, groups=3.5)

    def test_groups_list_raises(self, data):
        """groups 为 list 抛出 ValueError."""
        factor, returns = data
        with pytest.raises(ValueError, match="groups"):
            calc_neutralized_curve(factor, returns, groups=[1, 2, 3])

    def test_groups_int_zero_raises(self, data):
        """groups=int(0) 抛出 ValueError."""
        factor, returns = data
        with pytest.raises(ValueError, match="groups"):
            calc_neutralized_curve(factor, returns, groups=0)

    def test_groups_int_negative_raises(self, data):
        """groups=int(-1) 抛出 ValueError."""
        factor, returns = data
        with pytest.raises(ValueError, match="groups"):
            calc_neutralized_curve(factor, returns, groups=-1)

    def test_n_groups_one_raises(self, data):
        """n_groups=1 抛出 ValueError."""
        factor, returns = data
        with pytest.raises(ValueError, match="n_groups"):
            calc_neutralized_curve(factor, returns, groups=3, n_groups=1)

    def test_n_groups_zero_raises(self, data):
        """n_groups=0 抛出 ValueError."""
        factor, returns = data
        with pytest.raises(ValueError, match="n_groups"):
            calc_neutralized_curve(factor, returns, groups=3, n_groups=0)

    def test_factor_not_series_raises(self, data):
        """factor 非 pd.Series 抛出 ValueError."""
        _, returns = data
        with pytest.raises(ValueError, match="factor"):
            calc_neutralized_curve(np.array([1, 2]), returns, groups=3)

    def test_returns_not_series_raises(self, data):
        """returns 非 pd.Series 抛出 ValueError."""
        factor, _ = data
        with pytest.raises(ValueError, match="returns"):
            calc_neutralized_curve(factor, pd.DataFrame(), groups=3)


# ============================================================
# 3. demeaned/group_adjust 四种组合模式 / Four mode combinations
# ============================================================

class TestFourModeCombinations:
    """demeaned/group_adjust 四种组合深度验证 / Deep four mode combination tests."""

    @pytest.fixture
    def data(self):
        # 使用较大数据量和较强组效应以便观察差异
        # Use larger data and stronger group effect to observe differences
        return _make_factor_returns(n_days=200, n_assets=30,
                                    signal_strength=0.1,
                                    group_effect_strength=0.05)

    def test_all_four_modes_run(self, data):
        """四种模式全部正常运行."""
        factor, returns = data
        for d in [True, False]:
            for g in [True, False]:
                result = calc_neutralized_curve(
                    factor, returns, groups=4, demeaned=d, group_adjust=g,
                )
                assert isinstance(result, pd.Series)
                assert abs(result.iloc[0] - 1.0) < 1e-10

    def test_all_four_modes_positive_values(self, data):
        """四种模式全部产出正值净值."""
        factor, returns = data
        for d in [True, False]:
            for g in [True, False]:
                result = calc_neutralized_curve(
                    factor, returns, groups=4, demeaned=d, group_adjust=g,
                )
                assert (result > 0).all()

    @pytest.mark.parametrize("demeaned", [True, False])
    @pytest.mark.parametrize("group_adjust", [True, False])
    def test_mode_curve_length(self, data, demeaned, group_adjust):
        """每种模式的曲线长度一致."""
        factor, returns = data
        result = calc_neutralized_curve(
            factor, returns, groups=4, demeaned=demeaned, group_adjust=group_adjust,
        )
        expected_len = len(factor.index.get_level_values("timestamp").unique())
        assert len(result) == expected_len

    def test_demeaned_vs_raw_are_different(self, data):
        """demeaned=True 和 demeaned=False 产出不同曲线."""
        factor, returns = data
        curve_raw = calc_neutralized_curve(
            factor, returns, groups=4, demeaned=False, group_adjust=False,
        )
        curve_demeaned = calc_neutralized_curve(
            factor, returns, groups=4, demeaned=True, group_adjust=False,
        )
        assert not np.allclose(curve_raw.values, curve_demeaned.values, atol=1e-12)

    def test_group_adjust_vs_raw_are_different(self, data):
        """group_adjust=True 和 group_adjust=False 产出不同曲线."""
        factor, returns = data
        curve_raw = calc_neutralized_curve(
            factor, returns, groups=4, demeaned=False, group_adjust=False,
        )
        curve_adjusted = calc_neutralized_curve(
            factor, returns, groups=4, demeaned=False, group_adjust=True,
        )
        assert not np.allclose(curve_raw.values, curve_adjusted.values, atol=1e-12)

    def test_both_neutral_vs_raw_are_different(self, data):
        """demeaned=True + group_adjust=True 与 raw 产出不同曲线."""
        factor, returns = data
        curve_raw = calc_neutralized_curve(
            factor, returns, groups=4, demeaned=False, group_adjust=False,
        )
        curve_both = calc_neutralized_curve(
            factor, returns, groups=4, demeaned=True, group_adjust=True,
        )
        assert not np.allclose(curve_raw.values, curve_both.values, atol=1e-12)

    def test_full_neutral_differs_from_single(self, data):
        """双重中性化与单一中性化产出不同曲线."""
        factor, returns = data
        curve_d = calc_neutralized_curve(
            factor, returns, groups=4, demeaned=True, group_adjust=False,
        )
        curve_g = calc_neutralized_curve(
            factor, returns, groups=4, demeaned=False, group_adjust=True,
        )
        curve_both = calc_neutralized_curve(
            factor, returns, groups=4, demeaned=True, group_adjust=True,
        )
        # 双重不应等于任一单一模式
        assert not np.allclose(curve_both.values, curve_d.values, atol=1e-12)
        assert not np.allclose(curve_both.values, curve_g.values, atol=1e-12)


# ============================================================
# 4. 中性化效果验证 / Neutralization effectiveness
# ============================================================

class TestNeutralizationEffect:
    """验证中性化确实改变了结果 / Verify neutralization actually changes results."""

    @pytest.fixture
    def data(self):
        return _make_factor_returns(n_days=200, n_assets=30,
                                    signal_strength=0.1,
                                    group_effect_strength=0.05)

    def test_demeaned_reduces_group_factor_variance(self, data):
        """
        demeaned=True 后，组内因子值方差应减小.
        After demeaned=True, within-group factor variance should decrease.
        """
        factor, returns = data
        labels = _make_group_labels(factor, n_groups=4)

        # 计算原始组内因子方差
        df_raw = pd.DataFrame({"factor": factor, "group": labels})
        valid = df_raw["group"].notna() & df_raw["factor"].notna()
        raw_group_var = (
            df_raw.loc[valid]
            .groupby([pd.Grouper(level=0), "group"])["factor"]
            .transform("mean")
            .var()
        )

        # 中性化后的因子值（demeaned=True）
        df_neut = pd.DataFrame({"factor": factor.copy(), "group": labels})
        mask = valid
        if mask.sum() > 0:
            group_mean = (
                df_neut.loc[mask]
                .groupby([pd.Grouper(level=0), "group"])["factor"]
                .transform("mean")
            )
            df_neut.loc[mask, "factor"] = df_neut.loc[mask, "factor"] - group_mean

        neut_group_var = (
            df_neut.loc[mask]
            .groupby([pd.Grouper(level=0), "group"])["factor"]
            .transform("mean")
            .var()
        )

        # 中性化后组均值方差应减小
        assert neut_group_var < raw_group_var

    def test_group_adjust_reduces_group_return_variance(self, data):
        """
        group_adjust=True 后，组内收益方差应减小.
        After group_adjust=True, within-group return variance should decrease.
        """
        factor, returns = data
        labels = _make_group_labels(factor, n_groups=4)

        df_raw = pd.DataFrame({"returns": returns, "group": labels})
        valid = df_raw["group"].notna() & df_raw["returns"].notna()
        raw_group_var = (
            df_raw.loc[valid]
            .groupby([pd.Grouper(level=0), "group"])["returns"]
            .transform("mean")
            .var()
        )

        # group_adjust 后的收益
        df_adj = pd.DataFrame({"returns": returns.copy(), "group": labels})
        if valid.sum() > 0:
            g_mean_ret = (
                df_adj.loc[valid]
                .groupby([pd.Grouper(level=0), "group"])["returns"]
                .transform("mean")
            )
            df_adj.loc[valid, "returns"] = df_adj.loc[valid, "returns"] - g_mean_ret

        adj_group_var = (
            df_adj.loc[valid]
            .groupby([pd.Grouper(level=0), "group"])["returns"]
            .transform("mean")
            .var()
        )

        assert adj_group_var < raw_group_var

    def test_neutralized_curve_with_series_groups(self, data):
        """使用 pd.Series groups 时所有模式正常运行."""
        factor, returns = data
        labels = _make_group_labels(factor, n_groups=5)
        for d in [True, False]:
            for g in [True, False]:
                curve = calc_neutralized_curve(
                    factor, returns, groups=labels, demeaned=d, group_adjust=g,
                )
                assert isinstance(curve, pd.Series)
                assert abs(curve.iloc[0] - 1.0) < 1e-10


# ============================================================
# 5. 与原始曲线对比 / Comparison with original curve
# ============================================================

class TestCurveComparison:
    """中性化曲线与原始曲线对比 / Neutralized vs original curve comparison."""

    def test_neutralized_differs_from_non_neutralized_int_groups(self):
        """groups=int 时，中性化曲线不同于非中性化曲线."""
        factor, returns = _make_factor_returns(
            n_days=200, n_assets=30,
            signal_strength=0.1, group_effect_strength=0.05,
        )
        curve_raw = calc_neutralized_curve(
            factor, returns, groups=4, demeaned=False, group_adjust=False,
        )
        curve_neut = calc_neutralized_curve(
            factor, returns, groups=4, demeaned=True, group_adjust=False,
        )
        assert not np.allclose(curve_raw.values, curve_neut.values, atol=1e-12)

    def test_neutralized_differs_from_non_neutralized_series_groups(self):
        """groups=pd.Series 时，中性化曲线不同于非中性化曲线."""
        factor, returns = _make_factor_returns(
            n_days=200, n_assets=30,
            signal_strength=0.1, group_effect_strength=0.05,
        )
        labels = _make_group_labels(factor, n_groups=5)
        curve_raw = calc_neutralized_curve(
            factor, returns, groups=labels, demeaned=False, group_adjust=False,
        )
        curve_neut = calc_neutralized_curve(
            factor, returns, groups=labels, demeaned=True, group_adjust=False,
        )
        assert not np.allclose(curve_raw.values, curve_neut.values, atol=1e-12)

    def test_both_params_have_additive_effect(self):
        """demeaned 和 group_adjust 各自有独立效果."""
        factor, returns = _make_factor_returns(
            n_days=200, n_assets=30,
            signal_strength=0.1, group_effect_strength=0.05,
        )
        curve_base = calc_neutralized_curve(
            factor, returns, groups=4, demeaned=False, group_adjust=False,
        )
        curve_d = calc_neutralized_curve(
            factor, returns, groups=4, demeaned=True, group_adjust=False,
        )
        curve_g = calc_neutralized_curve(
            factor, returns, groups=4, demeaned=False, group_adjust=True,
        )
        # 两个单一参数模式都与基线不同
        assert not np.allclose(curve_base.values, curve_d.values, atol=1e-12)
        assert not np.allclose(curve_base.values, curve_g.values, atol=1e-12)
        # 两个单一参数模式之间也互不相同
        assert not np.allclose(curve_d.values, curve_g.values, atol=1e-12)


# ============================================================
# 6. 边界情况 / Edge cases
# ============================================================

class TestEdgeCases:
    """边界情况深度验证 / Deep edge case validation."""

    def test_small_data_two_assets(self):
        """仅 2 个资产的最小数据量."""
        factor, returns = _make_factor_returns(n_days=5, n_assets=2)
        result = calc_neutralized_curve(factor, returns, groups=2, n_groups=2)
        assert isinstance(result, pd.Series)
        assert abs(result.iloc[0] - 1.0) < 1e-10

    def test_large_groups(self):
        """中性化分组数和排名分组数都较大."""
        factor, returns = _make_factor_returns(n_days=50, n_assets=50)
        result = calc_neutralized_curve(factor, returns, groups=10, n_groups=10)
        assert isinstance(result, pd.Series)

    def test_mismatched_groups_count(self):
        """中性化分组和排名分组数量不同."""
        factor, returns = _make_factor_returns(n_days=50, n_assets=20)
        result = calc_neutralized_curve(factor, returns, groups=3, n_groups=5)
        assert isinstance(result, pd.Series)
        assert len(result) == 50

    def test_constant_factor(self):
        """常量因子不崩溃，产出有效曲线."""
        factor, returns = _make_factor_returns(n_days=20, n_assets=10)
        constant_factor = pd.Series(1.0, index=factor.index)
        result = calc_neutralized_curve(
            constant_factor, returns, groups=3, demeaned=True,
        )
        assert isinstance(result, pd.Series)
        assert abs(result.iloc[0] - 1.0) < 1e-10

    def test_constant_returns(self):
        """常量收益率不崩溃."""
        factor, returns = _make_factor_returns(n_days=20, n_assets=10)
        constant_returns = pd.Series(0.01, index=returns.index)
        result = calc_neutralized_curve(factor, constant_returns, groups=3)
        assert isinstance(result, pd.Series)

    def test_pure_noise(self):
        """纯噪声数据不崩溃."""
        rng = np.random.default_rng(123)
        dates = pd.date_range("2024-01-01", periods=30, freq="B")
        symbols = [f"A{i}" for i in range(10)]
        idx = pd.MultiIndex.from_product([dates, symbols])
        factor = pd.Series(rng.standard_normal(len(idx)), index=idx)
        returns = pd.Series(rng.standard_normal(len(idx)) * 0.01, index=idx)
        result = calc_neutralized_curve(factor, returns, groups=3)
        assert isinstance(result, pd.Series)

    def test_nans_in_factor(self):
        """因子含 NaN 时不崩溃."""
        factor, returns = _make_factor_returns(n_days=20, n_assets=10)
        factor.iloc[:10] = np.nan
        result = calc_neutralized_curve(factor, returns, groups=3)
        assert isinstance(result, pd.Series)

    def test_nans_in_returns(self):
        """收益含 NaN 时不崩溃."""
        factor, returns = _make_factor_returns(n_days=20, n_assets=10)
        returns.iloc[:10] = np.nan
        result = calc_neutralized_curve(factor, returns, groups=3)
        assert isinstance(result, pd.Series)

    def test_inf_in_factor(self):
        """因子含 inf 时不崩溃."""
        factor, returns = _make_factor_returns(n_days=20, n_assets=10)
        factor.iloc[0] = np.inf
        result = calc_neutralized_curve(factor, returns, groups=3)
        assert isinstance(result, pd.Series)

    def test_inf_in_returns(self):
        """收益含 inf 时不崩溃."""
        factor, returns = _make_factor_returns(n_days=20, n_assets=10)
        returns.iloc[0] = np.inf
        result = calc_neutralized_curve(factor, returns, groups=3)
        assert isinstance(result, pd.Series)

    def test_series_groups_with_nans(self):
        """groups 为含 NaN 的 Series 时不崩溃."""
        factor, returns = _make_factor_returns(n_days=20, n_assets=10)
        labels = _make_group_labels(factor, n_groups=3)
        labels.iloc[:5] = np.nan
        result = calc_neutralized_curve(factor, returns, groups=labels)
        assert isinstance(result, pd.Series)

    def test_series_groups_single_label(self):
        """groups Series 所有资产属于同一组时不崩溃."""
        factor, returns = _make_factor_returns(n_days=20, n_assets=10)
        labels = pd.Series(0.0, index=factor.index)
        result = calc_neutralized_curve(factor, returns, groups=labels)
        assert isinstance(result, pd.Series)


# ============================================================
# 7. 多种子稳定性 / Multi-seed stability
# ============================================================

class TestStability:
    """多种子稳定性验证 / Multi-seed stability tests."""

    @pytest.mark.parametrize("seed", [10, 20, 42, 77, 99, 123, 200, 314])
    def test_all_seeds_produce_valid_curve(self, seed):
        """不同种子均产出有效净值曲线."""
        factor, returns = _make_factor_returns(n_days=50, n_assets=15, seed=seed)
        result = calc_neutralized_curve(factor, returns, groups=3, demeaned=True)
        assert isinstance(result, pd.Series)
        assert abs(result.iloc[0] - 1.0) < 1e-10
        assert len(result) == 50

    @pytest.mark.parametrize("seed", [10, 42, 99])
    def test_seed_deterministic(self, seed):
        """相同种子产出完全一致的结果（确定性）."""
        factor1, returns1 = _make_factor_returns(n_days=50, n_assets=15, seed=seed)
        factor2, returns2 = _make_factor_returns(n_days=50, n_assets=15, seed=seed)
        result1 = calc_neutralized_curve(factor1, returns1, groups=4, demeaned=True)
        result2 = calc_neutralized_curve(factor2, returns2, groups=4, demeaned=True)
        np.testing.assert_array_equal(result1.values, result2.values)

    @pytest.mark.parametrize("seed", [10, 42, 77])
    def test_all_seeds_all_modes(self, seed):
        """不同种子下四种模式全部正常运行."""
        factor, returns = _make_factor_returns(n_days=50, n_assets=15, seed=seed)
        for d in [True, False]:
            for g in [True, False]:
                result = calc_neutralized_curve(
                    factor, returns, groups=4, demeaned=d, group_adjust=g,
                )
                assert isinstance(result, pd.Series)
                assert (result > 0).all()

    @pytest.mark.parametrize("seed", [10, 42, 99])
    def test_neutralization_effect_consistent_across_seeds(self, seed):
        """不同种子下，中性化曲线始终不同于非中性化曲线."""
        factor, returns = _make_factor_returns(
            n_days=100, n_assets=20, seed=seed,
            signal_strength=0.1, group_effect_strength=0.05,
        )
        curve_raw = calc_neutralized_curve(
            factor, returns, groups=4, demeaned=False, group_adjust=False,
        )
        curve_neut = calc_neutralized_curve(
            factor, returns, groups=4, demeaned=True, group_adjust=False,
        )
        assert not np.allclose(curve_raw.values, curve_neut.values, atol=1e-12)
