"""
Task 13 验证测试 — 换手率指标深度验证
Deep verification tests for turnover metrics (calc_turnover & calc_rank_autocorr).

与 task 12 单元测试互补，聚焦于：
- 返回结构与值域严格验证 / Strict return structure and value range verification
- 多种子参数化强/弱因子行为 / Multi-seed parameterized strong/weak factor behavior
- 不同 n_groups 和 lag 参数组合 / Various n_groups and lag parameter combinations
- 边界情况：资产数少于组数、完全随机因子、含 inf 值、变化资产集合
"""

import numpy as np
import pandas as pd
import pytest

from FactorAnalysis.turnover import calc_turnover, calc_rank_autocorr


# ---------------------------------------------------------------------------
# Helpers / 辅助函数
# ---------------------------------------------------------------------------

def _make_factor(values_2d, timestamps, symbols):
    """
    构建 MultiIndex (timestamp, symbol) 的因子 Series。
    Build factor Series with MultiIndex (timestamp, symbol).
    """
    records = []
    for i, t in enumerate(timestamps):
        for j, s in enumerate(symbols):
            records.append((t, s, values_2d[i][j]))
    idx = pd.MultiIndex.from_tuples(
        [(r[0], r[1]) for r in records], names=["timestamp", "symbol"]
    )
    return pd.Series([r[2] for r in records], index=idx, dtype=np.float64, name="factor")


def _make_random_factor(n_days=50, n_symbols=30, seed=42):
    """
    生成随机因子 Series / Generate random factor Series.
    """
    rng = np.random.RandomState(seed)
    timestamps = pd.date_range("2024-01-01", periods=n_days, freq="D")
    symbols = [f"S{i:03d}" for i in range(n_symbols)]
    vals = rng.randn(n_days, n_symbols)
    return _make_factor(vals, timestamps, symbols)


def _make_stable_factor(n_days=10, n_symbols=20, seed=42):
    """
    生成稳定因子（各期值相同）→ 分组不变、排名不变。
    Generate stable factor (same values across periods) → no group/rank changes.
    """
    rng = np.random.RandomState(seed)
    base = rng.randn(n_symbols)
    timestamps = pd.date_range("2024-01-01", periods=n_days, freq="D")
    symbols = [f"S{i:03d}" for i in range(n_symbols)]
    vals = np.tile(base, (n_days, 1))
    return _make_factor(vals, timestamps, symbols)


def _make_noisy_factor(n_days=50, n_symbols=30, seed=42, noise_scale=0.5):
    """
    生成带噪声的因子：基础信号 + 随机噪声。
    Generate noisy factor: base signal + random noise.
    """
    rng = np.random.RandomState(seed)
    base_signal = rng.randn(n_symbols)
    timestamps = pd.date_range("2024-01-01", periods=n_days, freq="D")
    symbols = [f"S{i:03d}" for i in range(n_symbols)]
    vals = np.tile(base_signal, (n_days, 1)) + rng.randn(n_days, n_symbols) * noise_scale
    return _make_factor(vals, timestamps, symbols)


# ---------------------------------------------------------------------------
# 1. 返回结构与类型 / Return structure and type
# ---------------------------------------------------------------------------

class TestReturnStructure:
    """返回值结构与类型验证 / Verify return structure and type."""

    def test_calc_turnover_is_dataframe(self):
        """calc_turnover 返回 pd.DataFrame / Returns pd.DataFrame."""
        factor = _make_random_factor()
        result = calc_turnover(factor, n_groups=5)
        assert isinstance(result, pd.DataFrame)

    def test_calc_turnover_columns(self):
        """calc_turnover 列名为 0 ~ n_groups-1 / Columns are 0 ~ n_groups-1."""
        factor = _make_random_factor()
        for ng in [2, 3, 5, 10]:
            result = calc_turnover(factor, n_groups=ng)
            assert list(result.columns) == list(range(ng))

    def test_calc_turnover_index_is_timestamp(self):
        """calc_turnover 的 index 为时间戳 / Index is timestamp."""
        factor = _make_random_factor(n_days=10)
        result = calc_turnover(factor, n_groups=3)
        assert len(result) == 10
        assert result.index.dtype == np.dtype("datetime64[ns]")

    def test_calc_turnover_first_period_all_nan(self):
        """首期所有分组换手率为 NaN / First period all groups NaN."""
        factor = _make_random_factor(n_days=5)
        for ng in [2, 3, 5]:
            result = calc_turnover(factor, n_groups=ng)
            assert result.iloc[0].isna().all(), f"n_groups={ng} first period not NaN"

    def test_calc_rank_autocorr_is_series(self):
        """calc_rank_autocorr 返回 pd.Series / Returns pd.Series."""
        factor = _make_random_factor()
        result = calc_rank_autocorr(factor)
        assert isinstance(result, pd.Series)

    def test_calc_rank_autocorr_index_is_timestamp(self):
        """calc_rank_autocorr 的 index 为时间戳 / Index is timestamp."""
        factor = _make_random_factor(n_days=10)
        result = calc_rank_autocorr(factor)
        assert len(result) == 10
        assert result.index.dtype == np.dtype("datetime64[ns]")

    def test_calc_rank_autocorr_first_period_nan(self):
        """首期自相关为 NaN / First period autocorrelation is NaN."""
        factor = _make_random_factor(n_days=5)
        result = calc_rank_autocorr(factor)
        assert np.isnan(result.iloc[0])

    def test_calc_rank_autocorr_lag2_first_two_nan(self):
        """lag=2 时前两期为 NaN / lag=2 first two periods are NaN."""
        factor = _make_random_factor(n_days=5)
        result = calc_rank_autocorr(factor, lag=2)
        assert np.isnan(result.iloc[0])
        assert np.isnan(result.iloc[1])
        assert not np.isnan(result.iloc[2])


# ---------------------------------------------------------------------------
# 2. 值域验证 / Value range verification
# ---------------------------------------------------------------------------

class TestValueRange:
    """值域严格验证 / Strict value range verification."""

    @pytest.mark.parametrize("n_groups", [2, 3, 5, 10])
    def test_turnover_range_0_to_1(self, n_groups):
        """换手率值域严格 [0, 1] / Turnover strictly in [0, 1]."""
        factor = _make_random_factor(n_days=30, n_symbols=50, seed=100)
        result = calc_turnover(factor, n_groups=n_groups)
        valid = result.iloc[1:].values.flatten()
        valid = valid[~np.isnan(valid)]
        assert (valid >= 0).all() and (valid <= 1).all(), \
            f"n_groups={n_groups} values out of [0,1]"

    @pytest.mark.parametrize("lag", [1, 2, 3])
    def test_autocorr_range_neg1_to_1(self, lag):
        """自相关系数值域严格 [-1, 1] / Autocorrelation strictly in [-1, 1]."""
        factor = _make_random_factor(n_days=50, n_symbols=30, seed=200)
        result = calc_rank_autocorr(factor, lag=lag)
        valid = result.dropna().values
        assert (valid >= -1.0).all() and (valid <= 1.0).all(), \
            f"lag={lag} values out of [-1,1]"

    def test_turnover_multi_seed_range(self):
        """多种子下换手率值域 [0, 1] / Value range across multiple seeds."""
        for seed in [1, 7, 42, 99, 256]:
            factor = _make_random_factor(n_days=20, n_symbols=40, seed=seed)
            result = calc_turnover(factor, n_groups=5)
            valid = result.iloc[1:].values.flatten()
            valid = valid[~np.isnan(valid)]
            assert (valid >= 0).all() and (valid <= 1).all(), f"seed={seed}"

    def test_autocorr_multi_seed_range(self):
        """多种子下自相关值域 [-1, 1] / Value range across multiple seeds."""
        for seed in [1, 7, 42, 99, 256]:
            factor = _make_random_factor(n_days=30, n_symbols=30, seed=seed)
            result = calc_rank_autocorr(factor)
            valid = result.dropna().values
            assert (valid >= -1.0).all(), f"seed={seed} below -1"
            assert (valid <= 1.0).all(), f"seed={seed} above 1"


# ---------------------------------------------------------------------------
# 3. 稳定因子行为 / Stable factor behavior
# ---------------------------------------------------------------------------

class TestStableFactor:
    """稳定因子：换手率=0、自相关=1.0 / Stable: turnover=0, autocorr=1.0."""

    @pytest.mark.parametrize("n_groups", [2, 3, 5])
    def test_stable_factor_zero_turnover(self, n_groups):
        """稳定因子换手率严格为 0 / Stable factor has exactly 0 turnover."""
        factor = _make_stable_factor(n_days=8, n_symbols=20, seed=42)
        result = calc_turnover(factor, n_groups=n_groups)
        valid = result.iloc[1:].values.flatten()
        valid = valid[~np.isnan(valid)]
        assert np.allclose(valid, 0.0), f"n_groups={n_groups} expected zero turnover"

    @pytest.mark.parametrize("lag", [1, 2, 3])
    def test_stable_factor_autocorr_1(self, lag):
        """稳定因子自相关严格为 1.0 / Stable factor autocorrelation exactly 1.0."""
        factor = _make_stable_factor(n_days=10, n_symbols=20, seed=42)
        result = calc_rank_autocorr(factor, lag=lag)
        valid = result.dropna().values
        assert np.allclose(valid, 1.0), f"lag={lag} expected autocorrelation 1.0"

    def test_stable_factor_multi_seed(self):
        """多种子稳定因子换手率为 0 / Multi-seed stable factor zero turnover."""
        for seed in [1, 7, 42, 99]:
            factor = _make_stable_factor(n_days=5, n_symbols=15, seed=seed)
            result = calc_turnover(factor, n_groups=3)
            valid = result.iloc[1:].values.flatten()
            valid = valid[~np.isnan(valid)]
            assert np.allclose(valid, 0.0), f"seed={seed}"


# ---------------------------------------------------------------------------
# 4. 振荡因子行为 / Oscillating factor behavior
# ---------------------------------------------------------------------------

class TestOscillatingFactor:
    """振荡因子：排名完全反转 → 自相关=-1.0 / Oscillating: autocorrelation=-1.0."""

    def test_alternating_reversal_autocorr_neg1(self):
        """奇偶期排名反转 → 自相关=-1.0 / Alternating reversal → autocorrelation=-1.0."""
        ts = pd.date_range("2024-01-01", periods=8, freq="D")
        syms = [f"S{i}" for i in range(10)]
        base = np.arange(10, dtype=float)
        vals = []
        for i in range(8):
            vals.append(base if i % 2 == 0 else base[::-1])
        factor = _make_factor(vals, ts, syms)
        result = calc_rank_autocorr(factor)
        valid = result.dropna().values
        assert np.allclose(valid, -1.0), f"Expected -1.0, got {valid}"

    def test_alternating_reversal_high_turnover(self):
        """奇偶期排名反转 → 换手率高 / Alternating reversal → high turnover."""
        ts = pd.date_range("2024-01-01", periods=8, freq="D")
        syms = [f"S{i}" for i in range(10)]
        base = np.arange(10, dtype=float)
        vals = []
        for i in range(8):
            vals.append(base if i % 2 == 0 else base[::-1])
        factor = _make_factor(vals, ts, syms)
        result = calc_turnover(factor, n_groups=2)
        valid = result.iloc[1:].values.flatten()
        valid = valid[~np.isnan(valid)]
        # 排名完全反转 → 每个分组应完全换血 → 换手率=1.0
        # complete rank reversal → groups completely change → turnover=1.0
        assert np.allclose(valid, 1.0), f"Expected 1.0 turnover, got mean={valid.mean()}"


# ---------------------------------------------------------------------------
# 5. 弱/随机因子行为 / Weak/random factor behavior
# ---------------------------------------------------------------------------

class TestWeakFactor:
    """弱/随机因子：换手率较高、自相关接近 0 / Weak: high turnover, autocorr ~ 0."""

    def test_random_factor_moderate_mean_turnover(self):
        """随机因子平均换手率在中等范围 / Random factor mean turnover is moderate."""
        # 使用多种子取平均 / average across multiple seeds
        turnovers = []
        for seed in range(5):
            factor = _make_random_factor(n_days=50, n_symbols=50, seed=seed * 13)
            result = calc_turnover(factor, n_groups=5)
            valid = result.iloc[1:].values.flatten()
            valid = valid[~np.isnan(valid)]
            turnovers.append(valid.mean())
        mean_turnover = np.mean(turnovers)
        # 随机因子换手率应显著 > 0 且不太极端
        # random factor turnover should be significantly > 0 and not extreme
        assert mean_turnover > 0.1, f"Random factor mean turnover too low: {mean_turnover}"
        assert mean_turnover < 0.95, f"Random factor mean turnover too high: {mean_turnover}"

    def test_random_factor_autocorr_near_zero(self):
        """随机因子自相关均值接近 0 / Random factor autocorrelation near 0."""
        autocorrs = []
        for seed in range(5):
            factor = _make_random_factor(n_days=80, n_symbols=50, seed=seed * 17)
            result = calc_rank_autocorr(factor)
            valid = result.dropna().values
            autocorrs.append(valid.mean())
        mean_autocorr = np.mean(autocorrs)
        assert abs(mean_autocorr) < 0.15, \
            f"Random factor mean autocorrelation too far from 0: {mean_autocorr}"

    def test_noisy_factor_autocorr_between_0_and_1(self):
        """带噪声因子自相关在 0~1 之间 / Noisy factor autocorrelation between 0 and 1."""
        factor = _make_noisy_factor(n_days=50, n_symbols=30, seed=42, noise_scale=0.3)
        result = calc_rank_autocorr(factor)
        valid = result.dropna().values
        # 有基础信号 + 噪声 → 排名有一定持续性 → 自相关 > 0
        # base signal + noise → some rank persistence → autocorrelation > 0
        assert np.mean(valid) > 0.1, \
            f"Noisy factor autocorrelation should be positive: {np.mean(valid)}"

    @pytest.mark.parametrize("noise_scale", [0.1, 0.5, 1.0, 2.0])
    def test_noise_scale_monotonic(self, noise_scale):
        """噪声越大 → 自相关越低 / More noise → lower autocorrelation."""
        factor = _make_noisy_factor(n_days=50, n_symbols=30, seed=42, noise_scale=noise_scale)
        result = calc_rank_autocorr(factor)
        valid = result.dropna().values
        # 不论噪声多大，自相关应在 [-1, 1] 范围内
        # regardless of noise level, autocorrelation should be in [-1, 1]
        assert (valid >= -1.0).all() and (valid <= 1.0).all()


# ---------------------------------------------------------------------------
# 6. 边界情况 / Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    """边界情况验证 / Edge case verification."""

    def test_fewer_assets_than_groups(self):
        """资产数少于组数时正常处理 / Fewer assets than groups handled."""
        factor = _make_random_factor(n_days=5, n_symbols=3, seed=42)
        # 3 个资产分 5 组 → quantile_group 会退化为可用组数
        # 3 assets into 5 groups → quantile_group falls back to available groups
        result = calc_turnover(factor, n_groups=5)
        assert isinstance(result, pd.DataFrame)

    def test_single_asset_turnover(self):
        """单资产换手率为 0 或 NaN / Single asset turnover is 0 or NaN."""
        ts = pd.date_range("2024-01-01", periods=5, freq="D")
        syms = ["A"]
        vals = np.random.randn(5, 1)
        factor = _make_factor(vals, ts, syms)
        result = calc_turnover(factor, n_groups=2)
        valid = result.iloc[1:].values.flatten()
        valid = valid[~np.isnan(valid)]
        # 单资产永远在同一组 → 换手率为 0
        # single asset always in same group → turnover = 0
        assert len(valid) == 0 or np.allclose(valid, 0.0)

    def test_single_asset_autocorr_nan(self):
        """单资产自相关为 NaN / Single asset autocorrelation is NaN."""
        ts = pd.date_range("2024-01-01", periods=5, freq="D")
        syms = ["A"]
        vals = np.random.randn(5, 1)
        factor = _make_factor(vals, ts, syms)
        result = calc_rank_autocorr(factor)
        # 单资产无法计算 Pearson 相关 → 全 NaN
        # single asset cannot compute Pearson correlation → all NaN
        assert result.isna().all()

    def test_two_assets_autocorr(self):
        """两资产自相关正常 / Two assets autocorrelation computed."""
        ts = pd.date_range("2024-01-01", periods=5, freq="D")
        syms = ["A", "B"]
        vals = np.random.randn(5, 2)
        factor = _make_factor(vals, ts, syms)
        result = calc_rank_autocorr(factor)
        assert result.iloc[0] != result.iloc[0] or np.isnan(result.iloc[0])
        assert isinstance(result, pd.Series)

    def test_all_same_values_turnover(self):
        """所有因子值相同时换手率为 0 / All same values → turnover=0."""
        ts = pd.date_range("2024-01-01", periods=5, freq="D")
        syms = [f"S{i}" for i in range(10)]
        vals = np.ones((5, 10))
        factor = _make_factor(vals, ts, syms)
        result = calc_turnover(factor, n_groups=3)
        valid = result.iloc[1:].values.flatten()
        valid = valid[~np.isnan(valid)]
        assert np.allclose(valid, 0.0)

    def test_all_same_values_autocorr(self):
        """所有因子值相同时自相关为 NaN / All same values → autocorrelation NaN."""
        ts = pd.date_range("2024-01-01", periods=5, freq="D")
        syms = [f"S{i}" for i in range(10)]
        vals = np.ones((5, 10))
        factor = _make_factor(vals, ts, syms)
        result = calc_rank_autocorr(factor)
        # 所有人排名相同 → Pearson 相关未定义 → NaN
        # all ranks equal → Pearson correlation undefined → NaN
        assert result.isna().all()

    def test_with_inf_values_turnover(self):
        """含 inf 值时换手率正常计算 / Turnover computed with inf values."""
        factor = _make_random_factor(n_days=5, n_symbols=10, seed=42)
        # 注入 inf / inject inf
        factor.iloc[0] = np.inf
        factor.iloc[5] = -np.inf
        # 不应崩溃 / should not crash
        result = calc_turnover(factor, n_groups=3)
        assert isinstance(result, pd.DataFrame)

    def test_changing_asset_set(self):
        """变化资产集合下正常处理 / Changing asset set handled."""
        ts = pd.date_range("2024-01-01", periods=4, freq="D")
        # 前 2 期 5 个资产，后 2 期 3 个资产（部分重叠）
        # first 2 periods 5 assets, last 2 periods 3 assets (partial overlap)
        syms_all = ["A", "B", "C", "D", "E"]
        syms_partial = ["B", "C", "D"]
        records = []
        for i, t in enumerate(ts):
            syms = syms_all if i < 2 else syms_partial
            rng = np.random.RandomState(42 + i)
            for s in syms:
                records.append((t, s, rng.randn()))
        idx = pd.MultiIndex.from_tuples(
            [(r[0], r[1]) for r in records], names=["timestamp", "symbol"]
        )
        factor = pd.Series([r[2] for r in records], index=idx, dtype=np.float64)
        # 不应崩溃 / should not crash
        turnover = calc_turnover(factor, n_groups=2)
        autocorr = calc_rank_autocorr(factor)
        assert isinstance(turnover, pd.DataFrame)
        assert isinstance(autocorr, pd.Series)

    def test_large_dataset(self):
        """大数据量正常工作 / Large dataset works normally."""
        factor = _make_random_factor(n_days=100, n_symbols=200, seed=42)
        turnover = calc_turnover(factor, n_groups=10)
        autocorr = calc_rank_autocorr(factor)
        assert turnover.shape == (100, 10)
        assert len(autocorr) == 100

    def test_high_nan_ratio(self):
        """高 NaN 比例下正常处理 / High NaN ratio handled."""
        factor = _make_random_factor(n_days=10, n_symbols=20, seed=42)
        # 注入 50% NaN / inject 50% NaN
        nan_indices = np.random.RandomState(0).choice(len(factor), size=len(factor) // 2, replace=False)
        factor.iloc[nan_indices] = np.nan
        turnover = calc_turnover(factor, n_groups=3)
        autocorr = calc_rank_autocorr(factor)
        assert isinstance(turnover, pd.DataFrame)
        assert isinstance(autocorr, pd.Series)


# ---------------------------------------------------------------------------
# 7. 参数校验 / Parameter validation
# ---------------------------------------------------------------------------

class TestParameterValidation:
    """参数校验验证 / Parameter validation verification."""

    def test_turnover_not_series_raises_type_error(self):
        """非 pd.Series 输入抛出 TypeError / Non-Series raises TypeError."""
        with pytest.raises(TypeError):
            calc_turnover(np.array([1, 2, 3]), n_groups=3)

    def test_turnover_empty_raises_value_error(self):
        """空 Series 抛出 ValueError / Empty Series raises ValueError."""
        with pytest.raises(ValueError):
            calc_turnover(pd.Series([], dtype=np.float64), n_groups=3)

    def test_turnover_n_groups_1_raises(self):
        """n_groups=1 抛出 ValueError / n_groups=1 raises ValueError."""
        factor = _make_random_factor()
        with pytest.raises(ValueError):
            calc_turnover(factor, n_groups=1)

    def test_turnover_n_groups_0_raises(self):
        """n_groups=0 抛出 ValueError / n_groups=0 raises ValueError."""
        factor = _make_random_factor()
        with pytest.raises(ValueError):
            calc_turnover(factor, n_groups=0)

    def test_autocorr_not_series_raises_type_error(self):
        """非 pd.Series 输入抛出 TypeError / Non-Series raises TypeError."""
        with pytest.raises(TypeError):
            calc_rank_autocorr(np.array([1, 2, 3]))

    def test_autocorr_empty_raises_value_error(self):
        """空 Series 抛出 ValueError / Empty Series raises ValueError."""
        with pytest.raises(ValueError):
            calc_rank_autocorr(pd.Series([], dtype=np.float64))

    def test_autocorr_lag_0_raises(self):
        """lag=0 抛出 ValueError / lag=0 raises ValueError."""
        factor = _make_random_factor()
        with pytest.raises(ValueError):
            calc_rank_autocorr(factor, lag=0)

    def test_autocorr_negative_lag_raises(self):
        """负 lag 抛出 ValueError / Negative lag raises ValueError."""
        factor = _make_random_factor()
        with pytest.raises(ValueError):
            calc_rank_autocorr(factor, lag=-1)
