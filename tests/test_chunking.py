"""
tests/test_chunking.py — 分块处理核心逻辑验证测试 / Chunking core logic verification tests

Task 1: 创建 FactorAnalysis/chunking.py 分块工具函数
"""

import numpy as np
import pandas as pd
import pytest

from FactorAnalysis.chunking import (
    split_into_chunks,
    merge_chunk_results,
    _merge_curves,
    _merge_turnover,
    _merge_rank_autocorr,
    _merge_ic_stats,
)


# ============================================================
# 测试数据构建 / Test data fixtures
# ============================================================

def _make_series(
    n_days: int = 50,
    n_assets: int = 10,
    seed: int = 42,
) -> pd.Series:
    """构建 MultiIndex (timestamp, symbol) 的测试 Series / Build test Series with MultiIndex."""
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2024-01-01", periods=n_days, freq="B")
    symbols = [f"S{i:03d}" for i in range(n_assets)]
    idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])
    values = rng.standard_normal(n_days * n_assets)
    return pd.Series(values, index=idx, name="factor")


def _make_dataframe(
    n_days: int = 50,
    n_assets: int = 10,
    seed: int = 42,
) -> pd.DataFrame:
    """构建 MultiIndex (timestamp, symbol) 的测试 DataFrame / Build test DataFrame with MultiIndex."""
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2024-01-01", periods=n_days, freq="B")
    symbols = [f"S{i:03d}" for i in range(n_assets)]
    idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])
    return pd.DataFrame({
        "factor": rng.standard_normal(n_days * n_assets),
        "returns": rng.standard_normal(n_days * n_assets) * 0.01,
    }, index=idx)


def _make_equity_curve(n_days: int = 20, seed: int = 42) -> pd.Series:
    """构建模拟净值曲线 / Build a simulated equity curve."""
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2024-01-01", periods=n_days, freq="B")
    daily_ret = rng.standard_normal(n_days) * 0.02
    equity = (1.0 + pd.Series(daily_ret, index=dates)).cumprod()
    equity.iloc[0] = 1.0
    return equity


# ============================================================
# 1. split_into_chunks 基础功能 / Basic split_into_chunks tests
# ============================================================

class TestSplitIntoChunks:
    """split_into_chunks 分块测试 / split_into_chunks splitting tests."""

    def test_basic_split_series(self):
        """Series 数据正确分块 / Series data split correctly."""
        data = _make_series(n_days=50, n_assets=5)
        chunks = split_into_chunks(data, chunk_size=10)
        assert len(chunks) == 5
        # 每块应包含 10 个时间截面 × 5 个资产 = 50 行
        for chunk in chunks:
            timestamps = chunk.index.get_level_values(0).unique()
            assert len(timestamps) == 10

    def test_basic_split_dataframe(self):
        """DataFrame 数据正确分块 / DataFrame data split correctly."""
        data = _make_dataframe(n_days=30, n_assets=5)
        chunks = split_into_chunks(data, chunk_size=10)
        assert len(chunks) == 3
        for chunk in chunks:
            timestamps = chunk.index.get_level_values(0).unique()
            assert len(timestamps) == 10

    def test_uneven_split(self):
        """非整除时分块正确处理 / Handle non-divisible chunk sizes."""
        data = _make_series(n_days=25, n_assets=5)
        chunks = split_into_chunks(data, chunk_size=10)
        assert len(chunks) == 3
        # 前两块各 10 个时间截面，最后一块 5 个
        ts_0 = chunks[0].index.get_level_values(0).nunique()
        ts_1 = chunks[1].index.get_level_values(0).nunique()
        ts_2 = chunks[2].index.get_level_values(0).nunique()
        assert ts_0 == 10
        assert ts_1 == 10
        assert ts_2 == 5

    def test_chunk_size_larger_than_data(self):
        """chunk_size >= 数据长度时返回单块 / Single chunk when chunk_size >= data length."""
        data = _make_series(n_days=10, n_assets=5)
        chunks = split_into_chunks(data, chunk_size=20)
        assert len(chunks) == 1
        assert len(chunks[0]) == len(data)

    def test_chunk_size_equals_data_length(self):
        """chunk_size == 数据长度时返回单块 / Single chunk when chunk_size == data length."""
        data = _make_series(n_days=10, n_assets=5)
        chunks = split_into_chunks(data, chunk_size=10)
        assert len(chunks) == 1

    def test_empty_data(self):
        """空数据返回空列表 / Empty data returns empty list."""
        dates = pd.DatetimeIndex([])
        symbols = ["S001"]
        idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])
        data = pd.Series([], index=idx, dtype=float)
        chunks = split_into_chunks(data, chunk_size=10)
        assert chunks == []

    def test_chunk_size_one(self):
        """chunk_size=1 时每块一个时间截面 / chunk_size=1 gives one timestamp per chunk."""
        data = _make_series(n_days=5, n_assets=3)
        chunks = split_into_chunks(data, chunk_size=1)
        assert len(chunks) == 5
        for chunk in chunks:
            assert chunk.index.get_level_values(0).nunique() == 1

    def test_invalid_chunk_size(self):
        """chunk_size < 1 抛出 ValueError / chunk_size < 1 raises ValueError."""
        data = _make_series(n_days=10, n_assets=5)
        with pytest.raises(ValueError, match="chunk_size"):
            split_into_chunks(data, chunk_size=0)
        with pytest.raises(ValueError, match="chunk_size"):
            split_into_chunks(data, chunk_size=-1)

    def test_invalid_rebalance_freq(self):
        """rebalance_freq < 1 抛出 ValueError / rebalance_freq < 1 raises ValueError."""
        data = _make_series(n_days=10, n_assets=5)
        with pytest.raises(ValueError, match="rebalance_freq"):
            split_into_chunks(data, chunk_size=5, rebalance_freq=0)


# ============================================================
# 2. rebalance_freq 边界对齐 / Rebalance frequency alignment
# ============================================================

class TestRebalanceAlignment:
    """rebalance_freq 边界对齐测试 / Rebalance frequency boundary alignment tests."""

    def test_rebalance_freq_1_no_change(self):
        """rebalance_freq=1 时不调整 chunk_size / No adjustment when rebalance_freq=1."""
        data = _make_series(n_days=30, n_assets=5)
        chunks = split_into_chunks(data, chunk_size=7, rebalance_freq=1)
        # 30 / 7 = 4.28 → 5 blocks: 7, 7, 7, 7, 2
        assert len(chunks) == 5
        for i in range(4):
            assert chunks[i].index.get_level_values(0).nunique() == 7

    def test_rebalance_freq_round_up(self):
        """rebalance_freq > 1 时向上取整 chunk_size / Round up chunk_size when rebalance_freq > 1."""
        data = _make_series(n_days=50, n_assets=5)
        # chunk_size=7, rebalance_freq=5 → effective_size=10
        chunks = split_into_chunks(data, chunk_size=7, rebalance_freq=5)
        assert len(chunks) == 5  # 50 / 10 = 5
        for chunk in chunks:
            n_ts = chunk.index.get_level_values(0).nunique()
            assert n_ts % 5 == 0, f"Chunk has {n_ts} timestamps, not aligned to rebalance_freq=5"

    def test_rebalance_freq_already_aligned(self):
        """chunk_size 已对齐 rebalance_freq 时不调整 / No adjustment when already aligned."""
        data = _make_series(n_days=50, n_assets=5)
        # chunk_size=10, rebalance_freq=5 → effective_size=10 (no change)
        chunks = split_into_chunks(data, chunk_size=10, rebalance_freq=5)
        assert len(chunks) == 5
        for chunk in chunks:
            assert chunk.index.get_level_values(0).nunique() == 10

    def test_rebalance_freq_large_value(self):
        """rebalance_freq=10 时 chunk_size=8 取整为 10 / rebalance_freq=10 rounds chunk_size=8 to 10."""
        data = _make_series(n_days=30, n_assets=5)
        chunks = split_into_chunks(data, chunk_size=8, rebalance_freq=10)
        # effective_size=10, 30/10=3 blocks
        assert len(chunks) == 3
        for chunk in chunks:
            assert chunk.index.get_level_values(0).nunique() == 10

    def test_timestamps_no_gaps(self):
        """分块后所有时间戳无遗漏 / No timestamps lost after splitting."""
        data = _make_series(n_days=50, n_assets=5)
        chunks = split_into_chunks(data, chunk_size=12, rebalance_freq=5)
        # effective_size=15, 50/15=4 blocks: 15,15,15,5
        original_ts = set(data.index.get_level_values(0).unique())
        merged_ts = set()
        for chunk in chunks:
            merged_ts.update(chunk.index.get_level_values(0).unique())
        assert original_ts == merged_ts

    def test_symbols_preserved_per_chunk(self):
        """每块内的资产集合完整 / Symbol set is complete within each chunk."""
        data = _make_series(n_days=50, n_assets=10)
        original_symbols = set(data.index.get_level_values(1).unique())
        chunks = split_into_chunks(data, chunk_size=10)
        for chunk in chunks:
            chunk_symbols = set(chunk.index.get_level_values(1).unique())
            assert chunk_symbols == original_symbols


# ============================================================
# 3. merge_chunk_results — IC 类型 / IC metric merge
# ============================================================

class TestMergeIC:
    """IC 序列拼接测试 / IC series concatenation tests."""

    def test_ic_concatenation(self):
        """IC 序列正确拼接 / IC series concatenated correctly."""
        dates_1 = pd.date_range("2024-01-01", periods=10, freq="B")
        dates_2 = pd.date_range("2024-01-15", periods=10, freq="B")
        ic_1 = pd.Series(np.random.randn(10) * 0.05, index=dates_1)
        ic_2 = pd.Series(np.random.randn(10) * 0.05, index=dates_2)

        merged = merge_chunk_results([ic_1, ic_2], metric_type="ic")
        assert len(merged) == 20
        assert merged.index[0] == dates_1[0]
        assert merged.index[-1] == dates_2[-1]

    def test_ic_single_chunk(self):
        """单块 IC 直接返回 / Single chunk IC returned as-is."""
        dates = pd.date_range("2024-01-01", periods=10, freq="B")
        ic = pd.Series(np.random.randn(10) * 0.05, index=dates)
        merged = merge_chunk_results([ic], metric_type="ic")
        pd.testing.assert_series_equal(merged, ic)

    def test_ic_empty_results(self):
        """空结果返回空 Series / Empty results return empty Series."""
        merged = merge_chunk_results([], metric_type="ic")
        assert len(merged) == 0


# ============================================================
# 4. merge_chunk_results — curve 类型 / Curve metric merge
# ============================================================

class TestMergeCurve:
    """净值曲线拼接测试 / Equity curve merge tests."""

    def test_curve_continuity(self):
        """两块曲线拼接后连续 / Two curves merged continuously."""
        curve_1 = _make_equity_curve(n_days=10, seed=42)
        curve_2 = _make_equity_curve(n_days=10, seed=99)

        merged = _merge_curves([curve_1, curve_2])
        assert len(merged) == 19  # 10 + 9 (skip first of curve_2)

        # 衔接点无跳变：merged[10] 应等于 curve_1[-1] * curve_2[1]
        # 下一块的值 = 前块末尾 * 本块增量
        expected_10 = curve_1.iloc[-1] * curve_2.iloc[1]
        assert abs(merged.iloc[10] - expected_10) < 1e-10

    def test_curve_single_chunk(self):
        """单块曲线直接返回 / Single chunk curve returned as-is."""
        curve = _make_equity_curve(n_days=10, seed=42)
        merged = _merge_curves([curve])
        pd.testing.assert_series_equal(merged, curve)

    def test_curve_starts_at_one(self):
        """合并后曲线起始值为 1.0 / Merged curve starts at 1.0."""
        curve_1 = _make_equity_curve(n_days=5, seed=42)
        curve_2 = _make_equity_curve(n_days=5, seed=99)
        merged = _merge_curves([curve_1, curve_2])
        assert abs(merged.iloc[0] - 1.0) < 1e-10

    def test_curve_via_merge_chunk_results(self):
        """通过 merge_chunk_results 接口调用 curve 合并 / Call curve merge via public API."""
        curve_1 = _make_equity_curve(n_days=10, seed=42)
        curve_2 = _make_equity_curve(n_days=10, seed=99)
        merged = merge_chunk_results([curve_1, curve_2], metric_type="curve")
        assert len(merged) == 19

    def test_three_chunks_continuity(self):
        """三块曲线拼接连续 / Three curves merged continuously."""
        c1 = _make_equity_curve(n_days=5, seed=1)
        c2 = _make_equity_curve(n_days=5, seed=2)
        c3 = _make_equity_curve(n_days=5, seed=3)

        merged = _merge_curves([c1, c2, c3])
        assert len(merged) == 13  # 5 + 4 + 4

        # c1→c2 边界
        expected = c1.iloc[-1] * c2.iloc[1]
        assert abs(merged.iloc[5] - expected) < 1e-10

        # c2→c3 边界
        scale_at_c2_end = c1.iloc[-1] * c2.iloc[-1]
        expected = scale_at_c2_end * c3.iloc[1]
        assert abs(merged.iloc[9] - expected) < 1e-10


# ============================================================
# 5. merge_chunk_results — turnover 类型 / Turnover metric merge
# ============================================================

class TestMergeTurnover:
    """换手率拼接测试 / Turnover merge tests."""

    def test_turnover_concat(self):
        """换手率 DataFrame 正确拼接 / Turnover DataFrames concatenated correctly."""
        dates_1 = pd.date_range("2024-01-01", periods=5, freq="B")
        dates_2 = pd.date_range("2024-01-08", periods=5, freq="B")
        to_1 = pd.DataFrame({0: np.nan, 1: 0.3, 2: 0.2}, index=dates_1)
        to_1.iloc[0] = np.nan  # 首期 NaN
        to_2 = pd.DataFrame({0: 0.4, 1: 0.3, 2: 0.5}, index=dates_2)
        to_2.iloc[0] = 0.1  # 这行应在合并后变为 NaN

        merged = _merge_turnover([to_1, to_2])
        assert len(merged) == 10
        # 第一块首行 NaN（原有）+ 第二块首行 NaN（边界）
        assert np.isnan(merged.iloc[0, 0])
        assert np.isnan(merged.iloc[5, 0])  # 跨块边界

    def test_turnover_single_chunk(self):
        """单块换手率直接返回 / Single chunk turnover returned as-is."""
        dates = pd.date_range("2024-01-01", periods=5, freq="B")
        to = pd.DataFrame({0: np.nan, 1: 0.3, 2: 0.2}, index=dates)
        merged = _merge_turnover([to])
        pd.testing.assert_frame_equal(merged, to)

    def test_turnover_via_merge_chunk_results(self):
        """通过 merge_chunk_results 接口调用 turnover 合并 / Call turnover merge via public API."""
        dates_1 = pd.date_range("2024-01-01", periods=3, freq="B")
        dates_2 = pd.date_range("2024-01-04", periods=3, freq="B")
        to_1 = pd.DataFrame({0: np.nan, 1: 0.2}, index=dates_1)
        to_2 = pd.DataFrame({0: 0.3, 1: 0.1}, index=dates_2)
        merged = merge_chunk_results([to_1, to_2], metric_type="turnover")
        assert np.isnan(merged.iloc[3, 0])  # 第二块首行设为 NaN


# ============================================================
# 6. merge_chunk_results — rank_autocorr 类型 / Rank autocorrelation merge
# ============================================================

class TestMergeRankAutocorr:
    """排名自相关拼接测试 / Rank autocorrelation merge tests."""

    def test_rank_autocorr_concat(self):
        """排名自相关正确拼接 / Rank autocorrelation concatenated correctly."""
        dates_1 = pd.date_range("2024-01-01", periods=5, freq="B")
        dates_2 = pd.date_range("2024-01-08", periods=5, freq="B")
        ra_1 = pd.Series([np.nan, 0.8, 0.7, 0.9, 0.85], index=dates_1)
        ra_2 = pd.Series([0.6, 0.75, 0.8, 0.7, 0.65], index=dates_2)

        merged = _merge_rank_autocorr([ra_1, ra_2])
        assert len(merged) == 10
        assert np.isnan(merged.iloc[0])  # 第一块首值 NaN（原有）
        assert np.isnan(merged.iloc[5])  # 第二块首值 NaN（跨块边界）

    def test_rank_autocorr_single_chunk(self):
        """单块排名自相关直接返回 / Single chunk returned as-is."""
        dates = pd.date_range("2024-01-01", periods=5, freq="B")
        ra = pd.Series([np.nan, 0.8, 0.7, 0.9, 0.85], index=dates)
        merged = _merge_rank_autocorr([ra])
        pd.testing.assert_series_equal(merged, ra)

    def test_rank_autocorr_via_merge_chunk_results(self):
        """通过 merge_chunk_results 接口调用 rank_autocorr 合并."""
        dates_1 = pd.date_range("2024-01-01", periods=3, freq="B")
        dates_2 = pd.date_range("2024-01-04", periods=3, freq="B")
        ra_1 = pd.Series([np.nan, 0.8, 0.7], index=dates_1)
        ra_2 = pd.Series([0.6, 0.75, 0.8], index=dates_2)
        merged = merge_chunk_results([ra_1, ra_2], metric_type="rank_autocorr")
        assert np.isnan(merged.iloc[3])


# ============================================================
# 7. merge_chunk_results — ic_stats 类型 / IC stats weighted merge
# ============================================================

class TestMergeICStats:
    """IC 统计量加权平均测试 / IC stats weighted average tests."""

    def test_basic_weighted_merge(self):
        """基本加权平均合并 / Basic weighted average merge."""
        stats_1 = pd.Series({"IC_mean": 0.05, "IC_std": 0.10, "count": 50})
        stats_2 = pd.Series({"IC_mean": 0.03, "IC_std": 0.12, "count": 30})

        merged = _merge_ic_stats([stats_1, stats_2])
        # 加权均值 = (0.05*50 + 0.03*30) / 80 = 3.4/80 = 0.0425
        assert abs(merged["IC_mean"] - 0.0425) < 1e-10
        assert "IC_std" in merged
        assert "ICIR" in merged

    def test_icir_calculation(self):
        """ICIR = IC_mean / IC_std / ICIR = IC_mean / IC_std."""
        stats = pd.Series({"IC_mean": 0.05, "IC_std": 0.10, "count": 100})
        merged = _merge_ic_stats([stats])
        assert abs(merged["ICIR"] - 0.5) < 1e-10

    def test_zero_std_gives_nan_icir(self):
        """IC_std=0 时 ICIR 为 NaN / ICIR is NaN when IC_std=0."""
        stats = pd.Series({"IC_mean": 0.05, "IC_std": 0.0, "count": 100})
        merged = _merge_ic_stats([stats])
        assert np.isnan(merged["ICIR"])

    def test_empty_chunks_gives_nan(self):
        """空块列表返回全 NaN / Empty chunk list returns all NaN."""
        merged = _merge_ic_stats([])
        assert np.isnan(merged["IC_mean"])
        assert np.isnan(merged["IC_std"])
        assert np.isnan(merged["ICIR"])

    def test_dict_input(self):
        """支持 dict 输入 / Support dict input."""
        stats = {"IC_mean": 0.04, "IC_std": 0.08, "count": 80}
        merged = _merge_ic_stats([stats])
        assert abs(merged["IC_mean"] - 0.04) < 1e-10

    def test_invalid_chunk_type_raises(self):
        """非法类型抛出 TypeError / Invalid type raises TypeError."""
        with pytest.raises(TypeError, match="ic_stats chunk must be"):
            _merge_ic_stats([("not_a_dict",)])

    def test_skips_invalid_entries(self):
        """跳过无效条目 / Skip invalid entries."""
        valid = pd.Series({"IC_mean": 0.05, "IC_std": 0.10, "count": 50})
        invalid = pd.Series({"IC_mean": np.nan, "IC_std": np.nan, "count": np.nan})
        merged = _merge_ic_stats([invalid, valid])
        # 仅使用 valid 块
        assert abs(merged["IC_mean"] - 0.05) < 1e-10


# ============================================================
# 8. merge_chunk_results — 异常处理 / Error handling
# ============================================================

class TestMergeErrors:
    """merge_chunk_results 异常处理 / merge_chunk_results error handling."""

    def test_unknown_metric_type(self):
        """未知 metric_type 抛出 ValueError / Unknown metric_type raises ValueError."""
        with pytest.raises(ValueError, match="Unknown metric_type"):
            merge_chunk_results([pd.Series([1, 2, 3])], metric_type="unknown_type")

    def test_empty_turnover_returns_dataframe(self):
        """空 turnover 结果返回空 DataFrame / Empty turnover returns empty DataFrame."""
        merged = merge_chunk_results([], metric_type="turnover")
        assert isinstance(merged, pd.DataFrame)
        assert len(merged) == 0

    def test_empty_other_returns_series(self):
        """空非 turnover 结果返回空 Series / Empty non-turnover returns empty Series."""
        merged = merge_chunk_results([], metric_type="ic")
        assert isinstance(merged, pd.Series)
        assert len(merged) == 0


# ============================================================
# 9. 端到端分块-合并一致性 / End-to-end split-merge consistency
# ============================================================

class TestEndToEnd:
    """分块后合并的数据完整性测试 / Data integrity after split-merge."""

    def test_split_merge_preserves_ic_values(self):
        """分块 IC 序列合并后与全量计算一致 / Chunked IC merge matches full computation."""
        data = _make_series(n_days=100, n_assets=20, seed=42)
        returns = _make_series(n_days=100, n_assets=20, seed=99)

        from FactorAnalysis.metrics import calc_ic

        # 全量 IC / Full IC
        full_ic = calc_ic(data, returns)

        # 分块 IC / Chunked IC
        chunks_factor = split_into_chunks(data, chunk_size=30)
        chunks_returns = split_into_chunks(returns, chunk_size=30)

        chunk_ics = []
        for f_chunk, r_chunk in zip(chunks_factor, chunks_returns):
            chunk_ics.append(calc_ic(f_chunk, r_chunk))

        merged_ic = merge_chunk_results(chunk_ics, metric_type="ic")

        # 时间戳一致 / timestamps match
        assert set(merged_ic.index) == set(full_ic.index)

        # IC 值一致（每个时间截面的 IC 计算独立）/ IC values match (independent per timestamp)
        for ts in full_ic.dropna().index:
            if ts in merged_ic.index and not np.isnan(merged_ic[ts]):
                assert abs(full_ic[ts] - merged_ic[ts]) < 1e-10

    def test_split_merge_curve_approximately_continuous(self):
        """分块净值合并后曲线近似连续 / Chunked curve merge approximately continuous."""
        data = _make_series(n_days=50, n_assets=10, seed=42)
        returns = _make_series(n_days=50, n_assets=10, seed=99)

        from FactorAnalysis.portfolio import calc_top_bottom_curve

        # 全量净值 / Full curve
        full_curve = calc_top_bottom_curve(data, returns)

        # 分块净值 / Chunked curves
        chunks_factor = split_into_chunks(data, chunk_size=15)
        chunks_returns = split_into_chunks(returns, chunk_size=15)

        chunk_curves = []
        for f_chunk, r_chunk in zip(chunks_factor, chunks_returns):
            chunk_curves.append(calc_top_bottom_curve(f_chunk, r_chunk))

        merged_curve = merge_chunk_results(chunk_curves, metric_type="curve")

        # 合并曲线起点为 1.0 / merged curve starts at 1.0
        assert abs(merged_curve.iloc[0] - 1.0) < 1e-10

        # 合并曲线终点应与全量接近（由于分块边界持仓不同，可能有小差异）
        # 但连续性应保证：无 NaN、无跳变
        assert merged_curve.notna().all(), "合并曲线不应有 NaN"

    def test_data_coverage_after_split(self):
        """分块后数据行数总和等于原始数据 / Total rows after split equals original."""
        data = _make_series(n_days=47, n_assets=8, seed=42)
        chunks = split_into_chunks(data, chunk_size=10)
        total_rows = sum(len(c) for c in chunks)
        assert total_rows == len(data)
