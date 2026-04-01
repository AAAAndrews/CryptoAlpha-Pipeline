"""
Task 12 验证测试 — 分组换手率与因子排名自相关 / Turnover & Rank Autocorrelation

覆盖范围:
- 导入校验 / Import checks
- calc_turnover 返回类型和值域 / calc_turnover return type and value range
- calc_rank_autocorr 返回类型和值域 / calc_rank_autocorr return type and value range
- 强/弱因子下的预期行为 / Expected behavior under strong/weak factors
- 边界情况 / Edge cases
"""

import numpy as np
import pandas as pd
import pytest

from FactorAnalysis.turnover import calc_turnover, calc_rank_autocorr


# ── 辅助函数 / Helpers ──────────────────────────────────────────────

def _make_factor(values_2d, timestamps, symbols):
    """构建 MultiIndex (timestamp, symbol) 的因子 Series / Build factor Series with MultiIndex."""
    records = []
    for i, t in enumerate(timestamps):
        for j, s in enumerate(symbols):
            records.append((t, s, values_2d[i][j]))
    idx = pd.MultiIndex.from_tuples([(r[0], r[1]) for r in records], names=["timestamp", "symbol"])
    return pd.Series([r[2] for r in records], index=idx, dtype=np.float64, name="factor")


# ── 1. 导入校验 / Import checks ────────────────────────────────────

class TestImport:
    def test_import_calc_turnover(self):
        """calc_turnover 可正常导入 / calc_turnover is importable."""
        assert callable(calc_turnover)

    def test_import_calc_rank_autocorr(self):
        """calc_rank_autocorr 可正常导入 / calc_rank_autocorr is importable."""
        assert callable(calc_rank_autocorr)


# ── 2. calc_turnover 基础功能 / calc_turnover basic functionality ──

class TestCalcTurnoverBasic:
    def test_return_type(self):
        """返回 pd.DataFrame / Returns pd.DataFrame."""
        np.random.seed(42)
        ts = pd.date_range("2024-01-01", periods=5, freq="D")
        syms = ["A", "B", "C", "D", "E"]
        vals = np.random.randn(5, 5)
        factor = _make_factor(vals, ts, syms)
        result = calc_turnover(factor, n_groups=3)
        assert isinstance(result, pd.DataFrame)

    def test_columns_match_groups(self):
        """列名为 0 ~ n_groups-1 / Column names match group labels."""
        np.random.seed(42)
        ts = pd.date_range("2024-01-01", periods=5, freq="D")
        syms = ["A", "B", "C", "D", "E"]
        vals = np.random.randn(5, 5)
        factor = _make_factor(vals, ts, syms)
        result = calc_turnover(factor, n_groups=3)
        assert list(result.columns) == [0, 1, 2]

    def test_first_period_is_nan(self):
        """首期无前序，换手率为 NaN / First period has no predecessor, turnover is NaN."""
        np.random.seed(42)
        ts = pd.date_range("2024-01-01", periods=3, freq="D")
        syms = ["A", "B", "C", "D", "E"]
        vals = np.random.randn(3, 5)
        factor = _make_factor(vals, ts, syms)
        result = calc_turnover(factor, n_groups=3)
        assert result.iloc[0].isna().all()

    def test_value_range_0_to_1(self):
        """换手率值域 [0, 1] / Turnover values in [0, 1]."""
        np.random.seed(42)
        ts = pd.date_range("2024-01-01", periods=10, freq="D")
        syms = [f"S{i}" for i in range(20)]
        vals = np.random.randn(10, 20)
        factor = _make_factor(vals, ts, syms)
        result = calc_turnover(factor, n_groups=5)
        valid = result.iloc[1:].dropna().values.flatten()
        assert (valid >= 0).all() and (valid <= 1).all()

    def test_stable_factor_low_turnover(self):
        """稳定因子换手率低 / Stable factor has low turnover."""
        ts = pd.date_range("2024-01-01", periods=5, freq="D")
        syms = ["A", "B", "C", "D", "E"]
        # 因子值完全不变 → 分组不变 → 换手率为 0
        # constant factor → same groups → zero turnover
        vals = np.tile([1.0, 2.0, 3.0, 4.0, 5.0], (5, 1))
        factor = _make_factor(vals, ts, syms)
        result = calc_turnover(factor, n_groups=3)
        valid = result.iloc[1:].dropna().values.flatten()
        assert np.allclose(valid, 0.0)


# ── 3. calc_rank_autocorr 基础功能 / calc_rank_autocorr basic ──────

class TestRankAutocorrBasic:
    def test_return_type(self):
        """返回 pd.Series / Returns pd.Series."""
        np.random.seed(42)
        ts = pd.date_range("2024-01-01", periods=5, freq="D")
        syms = ["A", "B", "C", "D", "E"]
        vals = np.random.randn(5, 5)
        factor = _make_factor(vals, ts, syms)
        result = calc_rank_autocorr(factor)
        assert isinstance(result, pd.Series)

    def test_value_range_neg1_to_1(self):
        """自相关系数值域 [-1, 1] / Autocorrelation in [-1, 1]."""
        np.random.seed(42)
        ts = pd.date_range("2024-01-01", periods=20, freq="D")
        syms = [f"S{i}" for i in range(30)]
        vals = np.random.randn(20, 30)
        factor = _make_factor(vals, ts, syms)
        result = calc_rank_autocorr(factor)
        valid = result.dropna().values
        assert (valid >= -1.0).all() and (valid <= 1.0).all()

    def test_first_period_is_nan(self):
        """首期无前序，自相关为 NaN / First period has no predecessor."""
        np.random.seed(42)
        ts = pd.date_range("2024-01-01", periods=3, freq="D")
        syms = ["A", "B", "C"]
        vals = np.random.randn(3, 3)
        factor = _make_factor(vals, ts, syms)
        result = calc_rank_autocorr(factor)
        assert np.isnan(result.iloc[0])

    def test_stable_factor_high_autocorr(self):
        """稳定因子排名自相关接近 1 / Stable factor has autocorrelation near 1."""
        ts = pd.date_range("2024-01-01", periods=5, freq="D")
        syms = ["A", "B", "C", "D", "E"]
        # 因子值不变 → 排名不变 → 自相关 = 1.0
        # constant factor → same ranks → autocorrelation = 1.0
        vals = np.tile([1.0, 2.0, 3.0, 4.0, 5.0], (5, 1))
        factor = _make_factor(vals, ts, syms)
        result = calc_rank_autocorr(factor)
        valid = result.dropna().values
        assert np.allclose(valid, 1.0)

    def test_lag_parameter(self):
        """lag=2 时前两期为 NaN / lag=2 makes first two periods NaN."""
        np.random.seed(42)
        ts = pd.date_range("2024-01-01", periods=5, freq="D")
        syms = ["A", "B", "C", "D", "E"]
        vals = np.random.randn(5, 5)
        factor = _make_factor(vals, ts, syms)
        result = calc_rank_autocorr(factor, lag=2)
        assert np.isnan(result.iloc[0])
        assert np.isnan(result.iloc[1])


# ── 4. 强/弱因子行为 / Strong/weak factor behavior ─────────────────

class TestFactorBehavior:
    def test_random_factor_moderate_autocorr(self):
        """随机因子自相关在中等范围 / Random factor has moderate autocorrelation."""
        np.random.seed(42)
        ts = pd.date_range("2024-01-01", periods=50, freq="D")
        syms = [f"S{i}" for i in range(50)]
        vals = np.random.randn(50, 50)
        factor = _make_factor(vals, ts, syms)
        result = calc_rank_autocorr(factor)
        valid = result.dropna().values
        # 随机因子的平均自相关应接近 0 / random factor autocorrelation ~ 0
        assert abs(np.mean(valid)) < 0.15

    def test_oscillating_factor_low_autocorr(self):
        """振荡因子排名自相关低 / Oscillating factor has low autocorrelation."""
        ts = pd.date_range("2024-01-01", periods=6, freq="D")
        syms = ["A", "B", "C", "D", "E"]
        # 奇偶期排名反转 / reverse ranks on odd/even periods
        vals_odd = [1.0, 2.0, 3.0, 4.0, 5.0]
        vals_even = [5.0, 4.0, 3.0, 2.0, 1.0]
        vals = [vals_odd if i % 2 == 0 else vals_even for i in range(6)]
        factor = _make_factor(vals, ts, syms)
        result = calc_rank_autocorr(factor)
        valid = result.dropna().values
        # 排名完全反转 → 自相关 = -1.0
        # completely reversed ranks → autocorrelation = -1.0
        assert np.allclose(valid, -1.0)


# ── 5. 边界情况 / Edge cases ───────────────────────────────────────

class TestEdgeCases:
    def test_empty_factor_raises(self):
        """空因子抛出 ValueError / Empty factor raises ValueError."""
        empty = pd.Series([], dtype=np.float64)
        with pytest.raises(ValueError):
            calc_turnover(empty, n_groups=3)
        with pytest.raises(ValueError):
            calc_rank_autocorr(empty)

    def test_single_period_turnover(self):
        """单期数据换手率全 NaN / Single period turnover is all NaN."""
        np.random.seed(42)
        ts = pd.date_range("2024-01-01", periods=1, freq="D")
        syms = ["A", "B", "C"]
        vals = np.random.randn(1, 3)
        factor = _make_factor(vals, ts, syms)
        result = calc_turnover(factor, n_groups=2)
        assert result.iloc[0].isna().all()

    def test_single_period_autocorr(self):
        """单期数据自相关为 NaN / Single period autocorrelation is NaN."""
        np.random.seed(42)
        ts = pd.date_range("2024-01-01", periods=1, freq="D")
        syms = ["A", "B", "C"]
        vals = np.random.randn(1, 3)
        factor = _make_factor(vals, ts, syms)
        result = calc_rank_autocorr(factor)
        assert np.isnan(result.iloc[0])

    def test_two_periods_autocorr(self):
        """两期数据自相关正常计算 / Two periods autocorrelation computed normally."""
        np.random.seed(42)
        ts = pd.date_range("2024-01-01", periods=2, freq="D")
        syms = ["A", "B", "C", "D", "E"]
        vals = np.random.randn(2, 5)
        factor = _make_factor(vals, ts, syms)
        result = calc_rank_autocorr(factor)
        assert np.isnan(result.iloc[0])
        assert not np.isnan(result.iloc[1])

    def test_n_groups_less_than_2_raises(self):
        """n_groups < 2 抛出 ValueError / n_groups < 2 raises ValueError."""
        np.random.seed(42)
        ts = pd.date_range("2024-01-01", periods=3, freq="D")
        syms = ["A", "B", "C"]
        vals = np.random.randn(3, 3)
        factor = _make_factor(vals, ts, syms)
        with pytest.raises(ValueError):
            calc_turnover(factor, n_groups=1)

    def test_lag_less_than_1_raises(self):
        """lag < 1 抛出 ValueError / lag < 1 raises ValueError."""
        np.random.seed(42)
        ts = pd.date_range("2024-01-01", periods=3, freq="D")
        syms = ["A", "B", "C"]
        vals = np.random.randn(3, 3)
        factor = _make_factor(vals, ts, syms)
        with pytest.raises(ValueError):
            calc_rank_autocorr(factor, lag=0)

    def test_not_series_raises(self):
        """非 pd.Series 输入抛出 TypeError / Non-Series input raises TypeError."""
        with pytest.raises(TypeError):
            calc_turnover([1, 2, 3], n_groups=3)
        with pytest.raises(TypeError):
            calc_rank_autocorr([1, 2, 3])

    def test_with_nan_values(self):
        """含 NaN 的因子值正常处理 / Factor with NaN values handled normally."""
        np.random.seed(42)
        ts = pd.date_range("2024-01-01", periods=5, freq="D")
        syms = ["A", "B", "C", "D", "E"]
        vals = np.random.randn(5, 5)
        # 注入 NaN / inject NaN
        vals[2, 1] = np.nan
        vals[3, 3] = np.nan
        factor = _make_factor(vals, ts, syms)
        # 不应抛出异常 / should not raise
        turnover = calc_turnover(factor, n_groups=3)
        autocorr = calc_rank_autocorr(factor)
        assert isinstance(turnover, pd.DataFrame)
        assert isinstance(autocorr, pd.Series)

    def test_single_asset(self):
        """单资产因子正常处理 / Single asset factor handled normally."""
        ts = pd.date_range("2024-01-01", periods=5, freq="D")
        syms = ["A"]
        vals = np.random.randn(5, 1)
        factor = _make_factor(vals, ts, syms)
        # 单资产只能分到一组 → 换手率应为 0 或 NaN
        # single asset can only be in one group → turnover should be 0 or NaN
        turnover = calc_turnover(factor, n_groups=2)
        # 单资产无法计算排名相关 → 自相关为 NaN
        # single asset cannot compute rank correlation → autocorrelation is NaN
        autocorr = calc_rank_autocorr(factor)
        assert isinstance(turnover, pd.DataFrame)
        assert isinstance(autocorr, pd.Series)
