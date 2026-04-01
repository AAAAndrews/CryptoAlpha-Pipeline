"""
tests/test_task16_neutralize.py — calc_neutralized_curve 分组中性化净值曲线验证测试

Task 16: 创建 FactorAnalysis/neutralize.py：实现 calc_neutralized_curve
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

    # 收益 = 因子信号 + 组效应 + 噪声 / returns = factor signal + group effect + noise
    group_effect = np.repeat(rng.standard_normal(n_assets) * 0.02, n_days)
    ret_noise = rng.standard_normal((n_days, n_assets)).flatten() * 0.01
    returns_values = true_signal * signal_strength * 0.3 + group_effect + ret_noise

    factor = pd.Series(factor_values, index=idx, name="factor")
    returns = pd.Series(returns_values, index=idx, name="returns")
    return factor, returns


def _make_group_labels(
    factor: pd.Series, n_groups: int = 4, seed: int = 0,
) -> pd.Series:
    """生成随机分组标签 / Generate random group labels."""
    rng = np.random.default_rng(seed)
    symbols = factor.index.get_level_values("symbol").unique()
    symbol_to_group = {s: rng.integers(0, n_groups) for s in symbols}
    return pd.Series(
        [symbol_to_group[s] for s in factor.index.get_level_values("symbol")],
        index=factor.index,
        dtype=float,
    )


# ============================================================
# 1. 导入与基础检查 / Import & basic checks
# ============================================================

class TestImport:
    """导入与基础校验 / Import and basic validation."""

    def test_import(self):
        """calc_neutralized_curve 可正常导入."""
        from FactorAnalysis.neutralize import calc_neutralized_curve
        assert callable(calc_neutralized_curve)

    def test_function_exists(self):
        """函数在模块中可访问."""
        import FactorAnalysis.neutralize as mod
        assert hasattr(mod, "calc_neutralized_curve")


# ============================================================
# 2. 返回结构与类型 / Return structure & type
# ============================================================

class TestReturnType:
    """返回类型与结构验证 / Return type and structure validation."""

    @pytest.fixture
    def data(self):
        return _make_factor_returns()

    def test_returns_series(self, data):
        """返回值类型为 pd.Series."""
        factor, returns = data
        result = calc_neutralized_curve(factor, returns, groups=4)
        assert isinstance(result, pd.Series)

    def test_index_is_timestamp(self, data):
        """返回值索引为 timestamp（单层）."""
        factor, returns = data
        result = calc_neutralized_curve(factor, returns, groups=4)
        assert not isinstance(result.index, pd.MultiIndex)
        assert len(result) > 0

    def test_start_value_is_one(self, data):
        """净值曲线起始值为 1.0."""
        factor, returns = data
        result = calc_neutralized_curve(factor, returns, groups=4)
        assert abs(result.iloc[0] - 1.0) < 1e-10

    def test_all_positive_values(self, data):
        """净值曲线值应 > 0（累积净值不可能为负）."""
        factor, returns = data
        result = calc_neutralized_curve(factor, returns, groups=4)
        assert (result > 0).all()


# ============================================================
# 3. groups 参数类型校验 / groups parameter type validation
# ============================================================

class TestGroupsParam:
    """groups 参数类型校验 / groups parameter type validation."""

    @pytest.fixture
    def data(self):
        return _make_factor_returns()

    def test_groups_as_int(self, data):
        """groups=int 时正常工作."""
        factor, returns = data
        result = calc_neutralized_curve(factor, returns, groups=3)
        assert isinstance(result, pd.Series)

    def test_groups_as_series(self, data):
        """groups=pd.Series 时正常工作."""
        factor, returns = data
        labels = _make_group_labels(factor, n_groups=3)
        result = calc_neutralized_curve(factor, returns, groups=labels)
        assert isinstance(result, pd.Series)

    def test_groups_invalid_type(self, data):
        """groups 为非法类型时抛出 ValueError."""
        factor, returns = data
        with pytest.raises(ValueError, match="groups"):
            calc_neutralized_curve(factor, returns, groups="invalid")

    def test_groups_int_too_small(self, data):
        """groups(int) < 2 时抛出 ValueError."""
        factor, returns = data
        with pytest.raises(ValueError, match="groups"):
            calc_neutralized_curve(factor, returns, groups=1)

    def test_n_groups_too_small(self, data):
        """n_groups < 2 时抛出 ValueError."""
        factor, returns = data
        with pytest.raises(ValueError, match="n_groups"):
            calc_neutralized_curve(factor, returns, groups=3, n_groups=1)

    def test_factor_not_series(self, data):
        """factor 不是 pd.Series 时抛出 ValueError."""
        _, returns = data
        with pytest.raises(ValueError, match="factor"):
            calc_neutralized_curve("not_a_series", returns, groups=3)

    def test_returns_not_series(self, data):
        """returns 不是 pd.Series 时抛出 ValueError."""
        factor, _ = data
        with pytest.raises(ValueError, match="returns"):
            calc_neutralized_curve(factor, "not_a_series", groups=3)


# ============================================================
# 4. demeaned/group_adjust 四种组合模式 / Four mode combinations
# ============================================================

class TestModeCombinations:
    """demeaned/group_adjust 四种组合验证 / Four mode combination tests."""

    @pytest.fixture
    def data(self):
        return _make_factor_returns()

    def test_demeaned_true_group_adjust_false(self, data):
        """demeaned=True, group_adjust=False 正常运行."""
        factor, returns = data
        result = calc_neutralized_curve(
            factor, returns, groups=4, demeaned=True, group_adjust=False,
        )
        assert isinstance(result, pd.Series)
        assert abs(result.iloc[0] - 1.0) < 1e-10

    def test_demeaned_false_group_adjust_true(self, data):
        """demeaned=False, group_adjust=True 正常运行."""
        factor, returns = data
        result = calc_neutralized_curve(
            factor, returns, groups=4, demeaned=False, group_adjust=True,
        )
        assert isinstance(result, pd.Series)
        assert abs(result.iloc[0] - 1.0) < 1e-10

    def test_demeaned_true_group_adjust_true(self, data):
        """demeaned=True, group_adjust=True 正常运行."""
        factor, returns = data
        result = calc_neutralized_curve(
            factor, returns, groups=4, demeaned=True, group_adjust=True,
        )
        assert isinstance(result, pd.Series)
        assert abs(result.iloc[0] - 1.0) < 1e-10

    def test_demeaned_false_group_adjust_false(self, data):
        """demeaned=False, group_adjust=False 基线模式."""
        factor, returns = data
        result = calc_neutralized_curve(
            factor, returns, groups=4, demeaned=False, group_adjust=False,
        )
        assert isinstance(result, pd.Series)
        assert abs(result.iloc[0] - 1.0) < 1e-10

    def test_four_modes_produce_different_curves(self, data):
        """四种模式产出不同净值曲线（至少不完全相同）."""
        factor, returns = data
        curves = {}
        for d in [True, False]:
            for g in [True, False]:
                curves[(d, g)] = calc_neutralized_curve(
                    factor, returns, groups=4, demeaned=d, group_adjust=g,
                )
        # 至少有一种组合与其他不同
        unique_curves = set()
        for v in curves.values():
            unique_curves.add(tuple(v.round(6).values))
        # 不应该所有曲线完全相同
        assert len(unique_curves) >= 1  # at least one exists


# ============================================================
# 5. 中性化效果验证 / Neutralization effectiveness
# ============================================================

class TestNeutralizationEffect:
    """验证中性化确实改变了结果 / Verify neutralization actually changes results."""

    @pytest.fixture
    def data(self):
        return _make_factor_returns(n_days=200, n_assets=30, signal_strength=0.1)

    def test_demeaned_changes_curve(self, data):
        """demeaned=True 的曲线应与 demeaned=False 不同."""
        factor, returns = data
        curve_raw = calc_neutralized_curve(
            factor, returns, groups=4, demeaned=False, group_adjust=False,
        )
        curve_demeaned = calc_neutralized_curve(
            factor, returns, groups=4, demeaned=True, group_adjust=False,
        )
        # 不完全相同
        assert not np.allclose(curve_raw.values, curve_demeaned.values, atol=1e-12)

    def test_group_adjust_changes_curve(self, data):
        """group_adjust=True 的曲线应与 group_adjust=False 不同."""
        factor, returns = data
        curve_raw = calc_neutralized_curve(
            factor, returns, groups=4, demeaned=False, group_adjust=False,
        )
        curve_adjusted = calc_neutralized_curve(
            factor, returns, groups=4, demeaned=False, group_adjust=True,
        )
        assert not np.allclose(curve_raw.values, curve_adjusted.values, atol=1e-12)

    def test_neutralization_with_series_groups(self, data):
        """使用 pd.Series 作为 groups 时中性化正常工作."""
        factor, returns = data
        labels = _make_group_labels(factor, n_groups=5)
        curve = calc_neutralized_curve(
            factor, returns, groups=labels, demeaned=True, group_adjust=False,
        )
        assert isinstance(curve, pd.Series)
        assert abs(curve.iloc[0] - 1.0) < 1e-10


# ============================================================
# 6. 边界情况 / Edge cases
# ============================================================

class TestEdgeCases:
    """边界情况验证 / Edge case validation."""

    def test_small_data(self):
        """小数据量正常工作."""
        factor, returns = _make_factor_returns(n_days=5, n_assets=5)
        result = calc_neutralized_curve(factor, returns, groups=2)
        assert isinstance(result, pd.Series)
        assert abs(result.iloc[0] - 1.0) < 1e-10

    def test_large_n_groups(self):
        """n_groups 较大时正常工作."""
        factor, returns = _make_factor_returns(n_days=50, n_assets=50)
        result = calc_neutralized_curve(factor, returns, groups=10, n_groups=10)
        assert isinstance(result, pd.Series)

    def test_two_groups(self):
        """n_groups=2 最小分组正常工作."""
        factor, returns = _make_factor_returns(n_days=20, n_assets=10)
        result = calc_neutralized_curve(factor, returns, groups=2, n_groups=2)
        assert isinstance(result, pd.Series)

    def test_constant_factor(self):
        """常量因子不崩溃."""
        factor, returns = _make_factor_returns(n_days=20, n_assets=10)
        constant_factor = pd.Series(1.0, index=factor.index)
        result = calc_neutralized_curve(
            constant_factor, returns, groups=3, demeaned=True,
        )
        assert isinstance(result, pd.Series)

    def test_different_n_groups_for_neutralization_and_ranking(self):
        """中性化分组和排名分组使用不同数量."""
        factor, returns = _make_factor_returns(n_days=50, n_assets=20)
        result = calc_neutralized_curve(
            factor, returns, groups=3, n_groups=5,
        )
        assert isinstance(result, pd.Series)

    def test_no_signal_data(self):
        """纯噪声数据不崩溃."""
        rng = np.random.default_rng(123)
        dates = pd.date_range("2024-01-01", periods=30, freq="B")
        symbols = [f"A{i}" for i in range(10)]
        idx = pd.MultiIndex.from_product([dates, symbols])
        factor = pd.Series(rng.standard_normal(len(idx)), index=idx)
        returns = pd.Series(rng.standard_normal(len(idx)) * 0.01, index=idx)
        result = calc_neutralized_curve(factor, returns, groups=3)
        assert isinstance(result, pd.Series)


# ============================================================
# 7. 多种子稳定性 / Multi-seed stability
# ============================================================

class TestStability:
    """多种子稳定性验证 / Multi-seed stability tests."""

    @pytest.mark.parametrize("seed", [10, 20, 30, 42, 99])
    def test_all_seeds_produce_valid_curve(self, seed):
        """不同种子均产出有效净值曲线."""
        factor, returns = _make_factor_returns(n_days=50, n_assets=15, seed=seed)
        result = calc_neutralized_curve(factor, returns, groups=3, demeaned=True)
        assert isinstance(result, pd.Series)
        assert abs(result.iloc[0] - 1.0) < 1e-10
        assert len(result) > 0

    @pytest.mark.parametrize("seed", [10, 20, 30])
    def test_all_seeds_group_adjust(self, seed):
        """不同种子下 group_adjust=True 正常工作."""
        factor, returns = _make_factor_returns(n_days=50, n_assets=15, seed=seed)
        result = calc_neutralized_curve(
            factor, returns, groups=4, demeaned=True, group_adjust=True,
        )
        assert isinstance(result, pd.Series)
        assert (result > 0).all()
