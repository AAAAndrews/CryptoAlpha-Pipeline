"""
Task 5 验证测试 — FactorAnalysis/returns.py calc_returns 收益矩阵计算
Verification tests for calc_returns: return labels, T+1 forward logic, NaN handling, edge cases.
"""

import warnings
import numpy as np
import pandas as pd
import pytest

from FactorAnalysis.returns import calc_returns, _VALID_LABELS


# ---------------------------------------------------------------------------
# Fixtures / 测试数据
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_data():
    """构建含 3 个交易对 × 5 个时间戳的 OHLC 数据 / 3 symbols × 5 timestamps OHLC data."""
    timestamps = pd.date_range("2024-01-01", periods=5, freq="D")
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    records = []
    for ts in timestamps:
        for sym in symbols:
            base = {"BTCUSDT": 100, "ETHUSDT": 10, "SOLUSDT": 1}[sym]
            records.append({
                "timestamp": ts,
                "symbol": sym,
                "open": base + np.random.RandomState(hash((ts, sym)) % 2**31).randint(-5, 5),
                "high": base + 10,
                "low": base - 10,
                "close": base + np.random.RandomState(hash((ts, sym, "c")) % 2**31).randint(-5, 5),
            })
    df = pd.DataFrame(records).sort_values(["timestamp", "symbol"]).reset_index(drop=True)
    # 确保没有零价格 / Ensure no zero prices
    df["open"] = df["open"].abs().clip(lower=1)
    df["close"] = df["close"].abs().clip(lower=1)
    return df


@pytest.fixture
def sample_data_multiindex(sample_data):
    """返回已设置 MultiIndex 的版本 / Version with MultiIndex already set."""
    df = sample_data.copy()
    return df.set_index(["timestamp", "symbol"])


def _make_simple_data(prices_by_symbol):
    """从价格字典构建 DataFrame / Build DataFrame from price dict.

    prices_by_symbol: {symbol: [p0, p1, p2, ...]} — 单一价格列（open=close 相同）
    """
    n_periods = max(len(v) for v in prices_by_symbol.values())
    timestamps = pd.date_range("2024-01-01", periods=n_periods, freq="D")
    records = []
    for ts in timestamps[:n_periods]:
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


# ---------------------------------------------------------------------------
# 1. 导入校验 / Import Validation
# ---------------------------------------------------------------------------

class TestImport:
    """验证模块可正常导入 / Verify module imports correctly."""

    def test_import_calc_returns(self):
        assert callable(calc_returns)

    def test_valid_labels_defined(self):
        assert "close2close" in _VALID_LABELS
        assert "open2open" in _VALID_LABELS

    def test_valid_labels_count(self):
        assert len(_VALID_LABELS) == 2


# ---------------------------------------------------------------------------
# 2. close2close 收益计算正确性 / close2close Return Correctness
# ---------------------------------------------------------------------------

class TestClose2Close:
    """验证 close2close 标签的 T+1 前向收益 / Verify close2close T+1 forward returns."""

    def test_return_formula(self):
        """(next_close / current_close) - 1 / Verify return formula."""
        data = _make_simple_data({"BTCUSDT": [100, 110, 99]})
        result = calc_returns(data, label="close2close")

        # T=0: (110/100) - 1 = 0.10
        # T=1: (99/110) - 1 = -0.10
        # T=2: NaN（无下一期）
        btc = result.xs("BTCUSDT", level=1)
        assert len(btc) == 3
        np.testing.assert_almost_equal(btc.iloc[0], 0.10, decimal=4)
        np.testing.assert_almost_equal(btc.iloc[1], -0.10, decimal=4)
        assert pd.isna(btc.iloc[2])

    def test_no_future_function(self):
        """T+1 前向收益不含未来信息 / T+1 forward return has no look-ahead bias."""
        # 构造单调递增价格 / Monotonically increasing prices
        data = _make_simple_data({"ETHUSDT": [10, 20, 30, 40]})
        result = calc_returns(data, label="close2close")
        eth = result.xs("ETHUSDT", level=1)

        # 每期收益应为 (next/current) - 1，全部为正
        # Each return = (next/current) - 1, all positive
        np.testing.assert_almost_equal(eth.iloc[0], 1.0, decimal=4)   # (20/10)-1
        np.testing.assert_almost_equal(eth.iloc[1], 0.5, decimal=4)   # (30/20)-1
        np.testing.assert_almost_equal(eth.iloc[2], 1 / 3, decimal=4) # (40/30)-1
        assert pd.isna(eth.iloc[3])  # 最后一期为 NaN

    def test_negative_return(self):
        """验证负收益计算 / Verify negative return calculation."""
        data = _make_simple_data({"SOLUSDT": [50, 40, 30]})
        result = calc_returns(data, label="close2close")
        sol = result.xs("SOLUSDT", level=1)

        np.testing.assert_almost_equal(sol.iloc[0], -0.2, decimal=4)   # (40/50)-1
        np.testing.assert_almost_equal(sol.iloc[1], -0.25, decimal=4)  # (30/40)-1
        assert pd.isna(sol.iloc[2])


# ---------------------------------------------------------------------------
# 3. open2open 收益计算正确性 / open2open Return Correctness
# ---------------------------------------------------------------------------

class TestOpen2Open:
    """验证 open2open 标签的 T+1 前向收益 / Verify open2open T+1 forward returns."""

    def test_return_formula(self):
        """(next_open / current_open) - 1 / Verify open2open formula."""
        data = _make_simple_data({"BTCUSDT": [100, 110, 99]})
        # open 和 close 相同，close2close 和 open2open 结果一致
        result = calc_returns(data, label="open2open")
        btc = result.xs("BTCUSDT", level=1)

        np.testing.assert_almost_equal(btc.iloc[0], 0.10, decimal=4)
        np.testing.assert_almost_equal(btc.iloc[1], -0.10, decimal=4)
        assert pd.isna(btc.iloc[2])

    def test_different_open_close(self):
        """open 与 close 不同时，两种标签结果不同 / Different results when open != close."""
        records = []
        for i, (o, c) in enumerate([(100, 102), (105, 108), (95, 100)]):
            records.append({
                "timestamp": pd.Timestamp("2024-01-01") + pd.Timedelta(days=i),
                "symbol": "BTCUSDT",
                "open": o,
                "high": max(o, c) + 5,
                "low": min(o, c) - 5,
                "close": c,
            })
        data = pd.DataFrame(records)

        result_c2c = calc_returns(data, label="close2close")
        result_o2o = calc_returns(data, label="open2open")

        btc_c = result_c2c.xs("BTCUSDT", level=1)
        btc_o = result_o2o.xs("BTCUSDT", level=1)

        # 结果应不同 / Results should differ
        assert not np.allclose(btc_c.values[:2], btc_o.values[:2])


# ---------------------------------------------------------------------------
# 4. 返回值结构 / Return Structure
# ---------------------------------------------------------------------------

class TestReturnStructure:
    """验证返回值的数据类型和索引结构 / Verify return type and index structure."""

    def test_returns_is_series(self, sample_data):
        result = calc_returns(sample_data)
        assert isinstance(result, pd.Series)

    def test_multiindex_timestamp_symbol(self, sample_data):
        result = calc_returns(sample_data)
        assert isinstance(result.index, pd.MultiIndex)
        assert result.index.names == ["timestamp", "symbol"]

    def test_multiindex_input(self, sample_data_multiindex):
        """传入 MultiIndex DataFrame 也能正常工作 / Works with MultiIndex input."""
        result = calc_returns(sample_data_multiindex)
        assert isinstance(result, pd.Series)
        assert isinstance(result.index, pd.MultiIndex)

    def test_series_name(self, sample_data):
        result_c2c = calc_returns(sample_data, label="close2close")
        result_o2o = calc_returns(sample_data, label="open2open")
        assert result_c2c.name == "close2close_return"
        assert result_o2o.name == "open2open_return"

    def test_output_length_matches_input(self, sample_data):
        result = calc_returns(sample_data)
        assert len(result) == len(sample_data)


# ---------------------------------------------------------------------------
# 5. 多交易对独立性 / Multi-Symbol Independence
# ---------------------------------------------------------------------------

class TestMultiSymbol:
    """验证多交易对之间收益计算独立 / Verify independence across symbols."""

    def test_symbols_independent(self):
        """不同交易对的前向收益互不影响 / Different symbols' returns are independent."""
        data = _make_simple_data({
            "BTCUSDT": [100, 110, 120],
            "ETHUSDT": [10, 5, 3],
        })
        result = calc_returns(data, label="close2close")

        btc = result.xs("BTCUSDT", level=1)
        eth = result.xs("ETHUSDT", level=1)

        # BTC 全部正收益 / BTC all positive
        assert all(btc.dropna() > 0)
        # ETH 全部负收益 / ETH all negative
        assert all(eth.dropna() < 0)

    def test_different_length_symbols(self):
        """不同交易对可以有不同时间长度 / Different symbols can have different lengths."""
        records = []
        # BTC: 4 periods
        for i, p in enumerate([100, 110, 120, 130]):
            records.append({
                "timestamp": pd.Timestamp("2024-01-01") + pd.Timedelta(days=i),
                "symbol": "BTCUSDT",
                "open": p, "high": p + 1, "low": p - 1, "close": p,
            })
        # ETH: 2 periods
        for i, p in enumerate([10, 12]):
            records.append({
                "timestamp": pd.Timestamp("2024-01-01") + pd.Timedelta(days=i),
                "symbol": "ETHUSDT",
                "open": p, "high": p + 1, "low": p - 1, "close": p,
            })
        data = pd.DataFrame(records)
        result = calc_returns(data, label="close2close")

        btc = result.xs("BTCUSDT", level=1)
        eth = result.xs("ETHUSDT", level=1)

        assert len(btc) == 4
        assert len(eth) == 2
        assert pd.isna(btc.iloc[-1])  # BTC 最后一期为 NaN
        assert pd.isna(eth.iloc[-1])  # ETH 最后一期为 NaN


# ---------------------------------------------------------------------------
# 6. 边界情况 / Edge Cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    """边界情况处理 / Edge case handling."""

    def test_invalid_label_raises(self, sample_data):
        with pytest.raises(ValueError, match="Invalid label"):
            calc_returns(sample_data, label="invalid_label")

    def test_missing_price_column_raises(self):
        """缺少价格列时报错 / Error when price column is missing."""
        data = pd.DataFrame({
            "timestamp": [pd.Timestamp("2024-01-01")],
            "symbol": ["BTCUSDT"],
            "high": [110], "low": [90],
        })
        with pytest.raises(ValueError, match="Missing columns"):
            calc_returns(data, label="close2close")

    def test_missing_index_columns_raises(self):
        """缺少 timestamp/symbol 列且无 MultiIndex 时报错 / Error without index columns or MultiIndex."""
        data = pd.DataFrame({"close": [100, 110]})
        with pytest.raises(ValueError, match="timestamp.*symbol"):
            calc_returns(data, label="close2close")

    def test_zero_price_returns_nan(self):
        """当前价格为零时返回 NaN / Zero current price returns NaN."""
        records = [
            {"timestamp": pd.Timestamp("2024-01-01"), "symbol": "BTCUSDT",
             "open": 100, "high": 110, "low": 90, "close": 0},
            {"timestamp": pd.Timestamp("2024-01-02"), "symbol": "BTCUSDT",
             "open": 100, "high": 110, "low": 90, "close": 50},
        ]
        data = pd.DataFrame(records)
        result = calc_returns(data, label="close2close")
        btc = result.xs("BTCUSDT", level=1)

        # close=0 时，收益应为 NaN / Zero price → NaN
        assert pd.isna(btc.iloc[0])
        # 第二期正常 / Second period normal
        assert pd.isna(btc.iloc[1])  # 无下一期

    def test_single_period_returns_nan(self):
        """仅一个时间戳时全部为 NaN / Single timestamp returns all NaN."""
        data = _make_simple_data({"BTCUSDT": [100]})
        result = calc_returns(data, label="close2close")
        assert len(result) == 1
        assert pd.isna(result.iloc[0])

    def test_single_symbol(self):
        """单交易对也能正常计算 / Single symbol works correctly."""
        data = _make_simple_data({"BTCUSDT": [100, 120]})
        result = calc_returns(data, label="close2close")
        btc = result.xs("BTCUSDT", level=1)
        np.testing.assert_almost_equal(btc.iloc[0], 0.2, decimal=4)
        assert pd.isna(btc.iloc[1])

    def test_unchanged_prices(self):
        """价格不变时收益为零 / Unchanged prices yield zero returns."""
        data = _make_simple_data({"BTCUSDT": [100, 100, 100]})
        result = calc_returns(data, label="close2close")
        btc = result.xs("BTCUSDT", level=1)
        np.testing.assert_almost_equal(btc.iloc[0], 0.0, decimal=4)
        np.testing.assert_almost_equal(btc.iloc[1], 0.0, decimal=4)
        assert pd.isna(btc.iloc[2])
