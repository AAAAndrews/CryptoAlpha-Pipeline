"""
Task 7 验证测试 — 收益矩阵与对齐集成验证
Integration verification for calc_returns + align_factor_returns pipeline.

验证两种收益率标签计算正确性、T+1 前向收益无未来函数、对齐后无 NaN、索引一致性、边界情况。
"""

import warnings
import numpy as np
import pandas as pd
import pytest

from FactorAnalysis.returns import calc_returns
from FactorAnalysis.alignment import align_factor_returns


# ---------------------------------------------------------------------------
# Helpers / 辅助函数
# ---------------------------------------------------------------------------

def _make_ohlcv(prices_by_symbol):
    """从价格字典构建 OHLC DataFrame / Build OHLC DataFrame from price dict.

    prices_by_symbol: {symbol: [p0, p1, p2, ...]} — open=close=给定价格
    """
    records = []
    max_len = max(len(v) for v in prices_by_symbol.values())
    timestamps = pd.date_range("2024-01-01", periods=max_len, freq="D")
    for ts in timestamps:
        for sym, prices in prices_by_symbol.items():
            idx = timestamps.get_loc(ts)
            if idx < len(prices):
                p = prices[idx]
                records.append({
                    "timestamp": ts,
                    "symbol": sym,
                    "open": p,
                    "high": p + 1,
                    "low": p - 1,
                    "close": p,
                })
    return pd.DataFrame(records).sort_values(["timestamp", "symbol"]).reset_index(drop=True)


def _make_factor_series(values, timestamps, symbols):
    """构建因子值 Series with MultiIndex / Build factor Series with MultiIndex."""
    idx = pd.MultiIndex.from_arrays([timestamps, symbols], names=["timestamp", "symbol"])
    return pd.Series(values, index=idx, name="factor")


# ---------------------------------------------------------------------------
# 1. 两种收益率标签端到端计算 / Both Labels End-to-End
# ---------------------------------------------------------------------------

class TestBothLabels:
    """验证 close2close 与 open2open 标签在完整管道中的正确性 / Verify both labels through full pipeline."""

    def test_close2close_pipeline(self):
        """close2close 标签：收益 → 对齐 → 结果正确。"""
        data = _make_ohlcv({"BTCUSDT": [100, 110, 99]})
        returns = calc_returns(data, label="close2close")

        # 构造因子值，与收益共享索引 / Build factor with same index
        factor = pd.Series([0.5, 0.3, np.nan], index=returns.index, name="factor")

        aligned = align_factor_returns(factor, returns)

        # T=2 的收益为 NaN（最后一期），应被剔除 / Last period NaN dropped
        assert len(aligned) == 2
        # 验证收益值 / Verify return values
        btc_returns = aligned["returns"].values
        np.testing.assert_almost_equal(btc_returns[0], 0.10, decimal=4)   # (110/100)-1
        np.testing.assert_almost_equal(btc_returns[1], -0.10, decimal=4)  # (99/110)-1

    def test_open2open_pipeline(self):
        """open2open 标签：收益 → 对齐 → 结果正确。"""
        data = _make_ohlcv({"ETHUSDT": [10, 15, 12]})
        returns = calc_returns(data, label="open2open")

        factor = pd.Series([1.0, 2.0, 3.0], index=returns.index, name="factor")

        aligned = align_factor_returns(factor, returns)

        assert len(aligned) == 2
        eth_returns = aligned["returns"].values
        np.testing.assert_almost_equal(eth_returns[0], 0.5, decimal=4)    # (15/10)-1
        np.testing.assert_almost_equal(eth_returns[1], -0.2, decimal=4)   # (12/15)-1

    def test_different_labels_different_results(self):
        """open ≠ close 时两种标签产出不同收益 / Different labels yield different results when open != close."""
        records = []
        for i, (o, c) in enumerate([(100, 102), (105, 108), (95, 100)]):
            records.append({
                "timestamp": pd.Timestamp("2024-01-01") + pd.Timedelta(days=i),
                "symbol": "BTCUSDT",
                "open": o, "high": max(o, c) + 5, "low": min(o, c) - 5, "close": c,
            })
        data = pd.DataFrame(records)

        ret_c2c = calc_returns(data, label="close2close")
        ret_o2o = calc_returns(data, label="open2open")

        # 同一因子值 / Same factor values
        factor = pd.Series([1.0, 2.0, 3.0], index=ret_c2c.index, name="factor")

        aligned_c = align_factor_returns(factor, ret_c2c)
        aligned_o = align_factor_returns(factor, ret_o2o)

        # 对齐后收益值应不同 / Aligned returns should differ
        assert not np.allclose(aligned_c["returns"].values, aligned_o["returns"].values)


# ---------------------------------------------------------------------------
# 2. T+1 前向收益无未来函数 / No Look-Ahead Bias
# ---------------------------------------------------------------------------

class TestNoFutureFunction:
    """验证 T+1 前向收益不含未来信息 / Verify no look-ahead bias in T+1 forward returns."""

    def test_returns_only_use_next_price(self):
        """收益仅依赖下一期价格，不涉及更远期 / Returns only use next period price."""
        # 构造波动价格：100 → 200 → 50 → 300 / Volatile prices
        data = _make_ohlcv({"BTCUSDT": [100, 200, 50, 300]})
        returns = calc_returns(data, label="close2close")
        btc = returns.xs("BTCUSDT", level=1)

        # T=0: (200/100)-1 = 1.0 — 仅看 T=1，不看 T=2/T=3
        np.testing.assert_almost_equal(btc.iloc[0], 1.0, decimal=4)
        # T=1: (50/200)-1 = -0.75 — 仅看 T=2
        np.testing.assert_almost_equal(btc.iloc[1], -0.75, decimal=4)
        # T=2: (300/50)-1 = 5.0 — 仅看 T=3
        np.testing.assert_almost_equal(btc.iloc[2], 5.0, decimal=4)
        # T=3: NaN — 无下一期
        assert pd.isna(btc.iloc[3])

    def test_last_period_always_nan(self):
        """每个交易对的最后一期收益必为 NaN / Last period of each symbol is always NaN."""
        data = _make_ohlcv({
            "BTCUSDT": [100, 110, 120, 130],
            "ETHUSDT": [10, 20, 30],
        })
        returns = calc_returns(data, label="close2close")

        btc = returns.xs("BTCUSDT", level=1)
        eth = returns.xs("ETHUSDT", level=1)

        assert pd.isna(btc.iloc[-1])
        assert pd.isna(eth.iloc[-1])

    def test_aligned_no_future_leakage(self):
        """对齐后数据不包含任何来自未来的因子值或收益 / Aligned data contains no future leakage."""
        data = _make_ohlcv({"SOLUSDT": [10, 12, 15, 20, 18]})
        returns = calc_returns(data, label="close2close")

        # 因子值 = 价格 / 10（纯示例） / Factor = price / 10 (example only)
        factor = pd.Series(
            data.set_index(["timestamp", "symbol"])["close"].values / 10,
            index=returns.index,
            name="factor",
        )

        aligned = align_factor_returns(factor, returns)

        # 验证：因子值 × 10 应等于当前价格（非下一期价格）
        # Factor * 10 should equal current price, not next price
        for idx, row in aligned.iterrows():
            ts, sym = idx
            current_price = data[
                (data["timestamp"] == ts) & (data["symbol"] == sym)
            ]["close"].values[0]
            np.testing.assert_almost_equal(row["factor"] * 10, current_price, decimal=4)


# ---------------------------------------------------------------------------
# 3. 对齐后无 NaN / Aligned Output Has No NaN
# ---------------------------------------------------------------------------

class TestAlignedNoNaN:
    """验证对齐后输出完全无 NaN / Verify aligned output has zero NaN."""

    def test_no_nan_after_alignment(self):
        """包含 NaN 的收益对齐后完全干净 / Aligned output is clean even with NaN inputs."""
        data = _make_ohlcv({"BTCUSDT": [100, 110, 120, np.nan, 130]})
        # 替换 NaN 价格为极小值以避免构建问题 / Replace NaN to avoid build issues
        data["close"] = data["close"].fillna(1)
        data["open"] = data["open"].fillna(1)

        returns = calc_returns(data, label="close2close")
        factor = pd.Series([0.1, np.nan, 0.3, 0.4, 0.5], index=returns.index, name="factor")

        aligned = align_factor_returns(factor, returns)

        assert not aligned.isna().any().any(), "对齐后输出不应包含任何 NaN"

    def test_no_inf_after_alignment(self):
        """对齐后输出不含 inf / Aligned output has no inf."""
        data = _make_ohlcv({"ETHUSDT": [1, 2, 3, 4, 5]})
        returns = calc_returns(data, label="close2close")

        # 注入 inf 因子值 / Inject inf factor values
        factor_vals = np.array([0.1, 0.2, np.inf, 0.4, 0.5], dtype=float)
        factor = pd.Series(factor_vals, index=returns.index, name="factor")

        aligned = align_factor_returns(factor, returns)

        assert np.isfinite(aligned["factor"]).all(), "因子列不应含 inf"
        assert np.isfinite(aligned["returns"]).all(), "收益列不应含 inf"

    def test_all_nan_input_returns_empty(self):
        """因子值全为 NaN 时对齐结果为空 / All-NaN factor yields empty aligned result."""
        data = _make_ohlcv({"BTCUSDT": [100, 110]})
        returns = calc_returns(data, label="close2close")

        factor = pd.Series([np.nan, np.nan], index=returns.index, name="factor")
        aligned = align_factor_returns(factor, returns)

        assert len(aligned) == 0


# ---------------------------------------------------------------------------
# 4. 索引一致性 / Index Consistency
# ---------------------------------------------------------------------------

class TestIndexConsistency:
    """验证收益与对齐后的索引结构一致 / Verify index structure consistency."""

    def test_returns_multiindex(self):
        """收益 Series 具有 (timestamp, symbol) MultiIndex。"""
        data = _make_ohlcv({"BTCUSDT": [100, 110, 120]})
        returns = calc_returns(data, label="close2close")

        assert isinstance(returns.index, pd.MultiIndex)
        assert returns.index.names == ["timestamp", "symbol"]

    def test_aligned_keeps_multiindex(self):
        """对齐后 DataFrame 保留 (timestamp, symbol) MultiIndex。"""
        data = _make_ohlcv({"BTCUSDT": [100, 110, 120], "ETHUSDT": [10, 15, 20]})
        returns = calc_returns(data, label="close2close")
        factor = pd.Series(np.random.randn(len(returns)), index=returns.index, name="factor")

        aligned = align_factor_returns(factor, returns)

        assert isinstance(aligned.index, pd.MultiIndex)
        assert aligned.index.names == ["timestamp", "symbol"]

    def test_aligned_index_subset_of_returns(self):
        """对齐后索引是收益索引的子集 / Aligned index is a subset of returns index."""
        data = _make_ohlcv({"BTCUSDT": [100, 110, 120, 130]})
        returns = calc_returns(data, label="close2close")
        factor = pd.Series([0.1, 0.2, np.nan, 0.4], index=returns.index, name="factor")

        aligned = align_factor_returns(factor, returns)

        # 对齐后索引应全部存在于原始收益索引中
        for idx in aligned.index:
            assert idx in returns.index

    def test_multi_symbol_aligned_consistent(self):
        """多交易对对齐后索引按 (timestamp, symbol) 正确保留。"""
        data = _make_ohlcv({
            "BTCUSDT": [100, 110, 120],
            "ETHUSDT": [10, 15, 20],
            "SOLUSDT": [1, 1.5, 2],
        })
        returns = calc_returns(data, label="close2close")
        factor = pd.Series(np.arange(len(returns), dtype=float), index=returns.index, name="factor")

        aligned = align_factor_returns(factor, returns)

        # 最后一期每交易对收益为 NaN → 3 行被剔除 / 3 rows dropped (last period NaN per symbol)
        assert len(aligned) == len(returns) - 3
        symbols_in_aligned = set(aligned.index.get_level_values("symbol"))
        assert symbols_in_aligned == {"BTCUSDT", "ETHUSDT", "SOLUSDT"}


# ---------------------------------------------------------------------------
# 5. 边界情况 / Edge Cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    """管道边界情况 / Pipeline edge cases."""

    def test_single_symbol_single_period(self):
        """单交易对单期：收益 NaN → 对齐后为空。"""
        data = _make_ohlcv({"BTCUSDT": [100]})
        returns = calc_returns(data, label="close2close")
        factor = pd.Series([0.5], index=returns.index, name="factor")

        aligned = align_factor_returns(factor, returns)
        assert len(aligned) == 0

    def test_single_symbol_two_periods(self):
        """单交易对两期：一期有效收益。"""
        data = _make_ohlcv({"BTCUSDT": [100, 120]})
        returns = calc_returns(data, label="close2close")
        factor = pd.Series([0.5, 0.8], index=returns.index, name="factor")

        aligned = align_factor_returns(factor, returns)
        assert len(aligned) == 1
        np.testing.assert_almost_equal(aligned.iloc[0]["returns"], 0.2, decimal=4)

    def test_zero_price_in_pipeline(self):
        """零价格通过管道：收益 NaN → 对齐剔除。"""
        records = [
            {"timestamp": pd.Timestamp("2024-01-01"), "symbol": "BTCUSDT",
             "open": 0, "high": 0, "low": 0, "close": 0},
            {"timestamp": pd.Timestamp("2024-01-02"), "symbol": "BTCUSDT",
             "open": 50, "high": 60, "low": 40, "close": 50},
        ]
        data = pd.DataFrame(records)
        returns = calc_returns(data, label="close2close")
        factor = pd.Series([0.1, 0.2], index=returns.index, name="factor")

        aligned = align_factor_returns(factor, returns)

        # T=0 close=0 → 收益 NaN，被剔除；T=1 无下一期 → 收益 NaN，被剔除
        assert len(aligned) == 0

    def test_unchanged_prices_pipeline(self):
        """价格不变时收益为零，对齐后保留。"""
        data = _make_ohlcv({"BTCUSDT": [100, 100, 100, 100]})
        returns = calc_returns(data, label="close2close")
        factor = pd.Series([0.1, 0.2, 0.3, 0.4], index=returns.index, name="factor")

        aligned = align_factor_returns(factor, returns)

        # 最后一个 NaN 被剔除 / Last period NaN dropped
        assert len(aligned) == 3
        assert (aligned["returns"] == 0.0).all()

    def test_mismatched_indexes_dropped(self):
        """因子与收益索引不完全匹配时，仅保留交集。"""
        data = _make_ohlcv({"BTCUSDT": [100, 110, 120]})
        returns = calc_returns(data, label="close2close")

        # 构造部分不匹配的因子索引 / Build partially mismatched factor index
        ts_list = list(returns.index.get_level_values("timestamp"))
        sym_list = list(returns.index.get_level_values("symbol"))
        # 替换最后一个 timestamp 使其不匹配 / Replace last timestamp to mismatch
        ts_list[-1] = pd.Timestamp("2099-12-31")
        factor_idx = pd.MultiIndex.from_arrays([ts_list, sym_list], names=["timestamp", "symbol"])
        factor = pd.Series([0.1, 0.2, 0.3], index=factor_idx, name="factor")

        aligned = align_factor_returns(factor, returns)

        # 仅前两行匹配 / Only first two rows match
        assert len(aligned) == 2

    def test_zero_price_creates_nan_in_returns(self):
        """管道中零价格导致对应期收益为 NaN。"""
        records = [
            {"timestamp": pd.Timestamp("2024-01-01"), "symbol": "BTCUSDT",
             "open": 0, "high": 5, "low": 0, "close": 0},
            {"timestamp": pd.Timestamp("2024-01-02"), "symbol": "BTCUSDT",
             "open": 50, "high": 60, "low": 40, "close": 50},
        ]
        data = pd.DataFrame(records)
        returns = calc_returns(data, label="close2close")

        # T=0 close=0 → 收益 NaN（分母为零保护）
        assert pd.isna(returns.iloc[0])

    def test_large_multi_symbol_pipeline(self):
        """大规模多交易对管道性能正常 / Large multi-symbol pipeline performs well."""
        n_symbols = 50
        n_periods = 100
        symbols = [f"S{i:04d}" for i in range(n_symbols)]
        prices = {}
        np.random.seed(42)
        for sym in symbols:
            prices[sym] = 100 + np.cumsum(np.random.randn(n_periods) * 0.5)

        data = _make_ohlcv(prices)
        returns = calc_returns(data, label="close2close")

        # 因子值 = 随机 / Factor = random
        factor = pd.Series(np.random.randn(len(returns)), index=returns.index, name="factor")

        aligned = align_factor_returns(factor, returns)

        # 每个交易对最后一期 NaN 被剔除 / Last period per symbol dropped
        assert len(aligned) == n_symbols * (n_periods - 1)
        assert not aligned.isna().any().any()
