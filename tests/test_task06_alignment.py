"""
Task 6 验证测试 — align_factor_returns 因子值与收益矩阵对齐
Verification tests for align_factor_returns factor-returns alignment.
"""

import warnings
import numpy as np
import pandas as pd
import pytest

from FactorAnalysis.alignment import align_factor_returns


# ── fixtures ──────────────────────────────────────────────────────────────

def _make_series(values, timestamps, symbols, name="factor"):
    """构建 MultiIndex (timestamp, symbol) 的 Series。"""
    idx = pd.MultiIndex.from_arrays([timestamps, symbols], names=["timestamp", "symbol"])
    return pd.Series(values, index=idx, name=name)


class TestImport:
    """导入校验 / Import validation."""

    def test_can_import(self):
        """模块可正常导入。/ Module can be imported."""
        from FactorAnalysis.alignment import align_factor_returns  # noqa: F401
        assert callable(align_factor_returns)

    def test_function_name(self):
        """函数名正确。/ Function name is correct."""
        assert align_factor_returns.__name__ == "align_factor_returns"


class TestAlignment:
    """核心对齐逻辑 / Core alignment logic."""

    def test_basic_inner_join(self):
        """两侧索引交集保留，非交集丢弃。/ Inner join keeps common index only."""
        t = [1, 1, 2, 2]
        s = ["A", "B", "A", "B"]
        factor = _make_series([0.1, 0.2, 0.3, 0.4], t, s)
        returns = _make_series([0.01, 0.02, 0.03, 0.04], t, s, name="ret")

        result = align_factor_returns(factor, returns)
        assert len(result) == 4
        assert list(result.columns) == ["factor", "returns"]
        assert isinstance(result.index, pd.MultiIndex)

    def test_non_overlapping_dropped(self):
        """仅在 factor 中存在的行被丢弃。/ Rows only in factor are dropped."""
        factor = _make_series(
            [0.1, 0.2, 0.3],
            [1, 1, 2],
            ["A", "B", "C"],
        )
        returns = _make_series(
            [0.01, 0.02],
            [1, 1],
            ["A", "B"],
            name="ret",
        )

        result = align_factor_returns(factor, returns)
        assert len(result) == 2
        assert set(result.index.get_level_values("symbol")) == {"A", "B"}

    def test_extra_returns_dropped(self):
        """仅在 returns 中存在的行也被丢弃。/ Rows only in returns are dropped."""
        factor = _make_series([0.1], [1], ["A"])
        returns = _make_series([0.01, 0.02], [1, 1], ["A", "B"], name="ret")

        result = align_factor_returns(factor, returns)
        assert len(result) == 1
        assert result.index.get_level_values("symbol")[0] == "A"


class TestNaNDrop:
    """NaN / inf 剔除 / NaN and inf removal."""

    def test_nan_in_factor_dropped(self):
        """因子值为 NaN 的行被剔除。/ Rows with NaN factor are dropped."""
        t = [1, 1, 2]
        s = ["A", "B", "A"]
        factor = _make_series([0.1, np.nan, 0.3], t, s)
        returns = _make_series([0.01, 0.02, 0.03], t, s, name="ret")

        result = align_factor_returns(factor, returns)
        assert len(result) == 2
        assert not result["factor"].isna().any()

    def test_nan_in_returns_dropped(self):
        """收益率为 NaN 的行被剔除。/ Rows with NaN returns are dropped."""
        t = [1, 1, 2]
        s = ["A", "B", "A"]
        factor = _make_series([0.1, 0.2, 0.3], t, s)
        returns = _make_series([0.01, np.nan, 0.03], t, s, name="ret")

        result = align_factor_returns(factor, returns)
        assert len(result) == 2
        assert not result["returns"].isna().any()

    def test_inf_in_factor_dropped(self):
        """因子值为 inf 的行被剔除。/ Rows with infinite factor are dropped."""
        t = [1, 1, 2]
        s = ["A", "B", "A"]
        factor = _make_series([0.1, np.inf, 0.3], t, s)
        returns = _make_series([0.01, 0.02, 0.03], t, s, name="ret")

        result = align_factor_returns(factor, returns)
        assert len(result) == 2
        assert np.isfinite(result["factor"]).all()

    def test_inf_in_returns_dropped(self):
        """收益率为 inf 的行被剔除。/ Rows with infinite returns are dropped."""
        t = [1, 1, 2]
        s = ["A", "B", "A"]
        factor = _make_series([0.1, 0.2, 0.3], t, s)
        returns = _make_series([0.01, -np.inf, 0.03], t, s, name="ret")

        result = align_factor_returns(factor, returns)
        assert len(result) == 2
        assert np.isfinite(result["returns"]).all()

    def test_both_nan_dropped(self):
        """两侧均为 NaN 的行被剔除。/ Rows with both NaN are dropped."""
        t = [1, 1, 2]
        s = ["A", "B", "A"]
        factor = _make_series([0.1, np.nan, 0.3], t, s)
        returns = _make_series([0.01, np.nan, 0.03], t, s, name="ret")

        result = align_factor_returns(factor, returns)
        assert len(result) == 2


class TestOutputStructure:
    """返回值结构校验 / Return value structure validation."""

    def test_columns(self):
        """返回 DataFrame 列名为 ['factor', 'returns']。/ Columns are ['factor', 'returns']."""
        factor = _make_series([0.1], [1], ["A"])
        returns = _make_series([0.01], [1], ["A"], name="ret")

        result = align_factor_returns(factor, returns)
        assert list(result.columns) == ["factor", "returns"]

    def test_multiindex(self):
        """返回 DataFrame 索引为 MultiIndex (timestamp, symbol)。"""
        factor = _make_series([0.1, 0.2], [1, 1], ["A", "B"])
        returns = _make_series([0.01, 0.02], [1, 1], ["A", "B"], name="ret")

        result = align_factor_returns(factor, returns)
        assert isinstance(result.index, pd.MultiIndex)
        assert result.index.names == ["timestamp", "symbol"]

    def test_no_nan_in_output(self):
        """输出不含任何 NaN。/ Output has no NaN at all."""
        t = list(range(5))
        s = ["A"] * 5
        factor = _make_series([0.1, np.nan, 0.3, 0.4, 0.5], t, s)
        returns = _make_series([0.01, 0.02, 0.03, np.nan, 0.05], t, s, name="ret")

        result = align_factor_returns(factor, returns)
        assert not result.isna().any().any()

    def test_values_preserved(self):
        """有效行的数值精确保留。/ Valid rows have exact values preserved."""
        factor = _make_series([1.5, 2.5], [1, 1], ["A", "B"])
        returns = _make_series([0.05, -0.03], [1, 1], ["A", "B"], name="ret")

        result = align_factor_returns(factor, returns)
        assert result.loc[(1, "A"), "factor"] == 1.5
        assert result.loc[(1, "A"), "returns"] == 0.05
        assert result.loc[(1, "B"), "factor"] == 2.5
        assert result.loc[(1, "B"), "returns"] == -0.03


class TestWarning:
    """数据丢失告警 / Data loss warning."""

    def test_high_loss_warns(self):
        """丢弃超过 50% 时发出 UserWarning。/ Warns when >50% rows are dropped."""
        factor = _make_series([0.1, np.nan, np.nan, np.nan], [1, 1, 1, 1], ["A", "B", "C", "D"])
        returns = _make_series([0.01, 0.02, 0.03, 0.04], [1, 1, 1, 1], ["A", "B", "C", "D"], name="ret")

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            align_factor_returns(factor, returns)
            assert any(issubclass(x.category, UserWarning) for x in w)

    def test_no_warn_low_loss(self):
        """丢弃 ≤50% 时不告警。/ No warning when ≤50% rows are dropped."""
        factor = _make_series([0.1, np.nan], [1, 1], ["A", "B"])
        returns = _make_series([0.01, 0.02], [1, 1], ["A", "B"], name="ret")

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            align_factor_returns(factor, returns)
            assert not any(issubclass(x.category, UserWarning) for x in w)


class TestEdgeCases:
    """边界情况 / Edge cases."""

    def test_empty_intersection(self):
        """两侧索引无交集时返回空 DataFrame。/ Empty result when no overlap."""
        factor = _make_series([0.1], [1], ["A"])
        returns = _make_series([0.01], [2], ["B"], name="ret")

        result = align_factor_returns(factor, returns)
        assert len(result) == 0

    def test_single_row(self):
        """仅一行有效数据时正常工作。/ Works with a single valid row."""
        factor = _make_series([0.42], [100], ["X"])
        returns = _make_series([0.07], [100], ["X"], name="ret")

        result = align_factor_returns(factor, returns)
        assert len(result) == 1
        assert result.iloc[0]["factor"] == 0.42
        assert result.iloc[0]["returns"] == 0.07

    def test_all_nan_returns_empty(self):
        """全部为 NaN 时返回空 DataFrame。/ All NaN returns empty DataFrame."""
        factor = _make_series([np.nan, np.nan], [1, 1], ["A", "B"])
        returns = _make_series([0.01, 0.02], [1, 1], ["A", "B"], name="ret")

        result = align_factor_returns(factor, returns)
        assert len(result) == 0

    def test_large_dataset(self):
        """大数据量下性能正常。/ Performance is fine with larger data."""
        n = 10000
        timestamps = np.repeat(np.arange(100), 100)
        symbols = np.tile([f"S{i:03d}" for i in range(100)], 100)
        np.random.seed(42)
        factor_vals = np.random.randn(n)
        return_vals = np.random.randn(n) * 0.01

        factor = _make_series(factor_vals, timestamps, symbols)
        returns = _make_series(return_vals, timestamps, symbols, name="ret")

        result = align_factor_returns(factor, returns)
        assert len(result) == n


class TestValidation:
    """参数校验 / Input validation."""

    def test_factor_not_series_raises(self):
        """factor 非 Series 时抛 ValueError。/ Raises when factor is not Series."""
        with pytest.raises(ValueError, match="factor.*pd.Series"):
            align_factor_returns(
                pd.DataFrame({"factor": [0.1]}),
                _make_series([0.01], [1], ["A"], name="ret"),
            )

    def test_returns_not_series_raises(self):
        """returns 非 Series 时抛 ValueError。/ Raises when returns is not Series."""
        with pytest.raises(ValueError, match="returns.*pd.Series"):
            align_factor_returns(
                _make_series([0.1], [1], ["A"]),
                pd.DataFrame({"returns": [0.01]}),
            )

    def test_factor_no_multiindex_raises(self):
        """factor 索引非 MultiIndex 时抛 ValueError。"""
        s = pd.Series([0.1], index=[1], name="factor")
        with pytest.raises(ValueError, match="factor.*MultiIndex"):
            align_factor_returns(s, _make_series([0.01], [1], ["A"], name="ret"))

    def test_returns_no_multiindex_raises(self):
        """returns 索引非 MultiIndex 时抛 ValueError。"""
        s = pd.Series([0.01], index=[1], name="ret")
        with pytest.raises(ValueError, match="returns.*MultiIndex"):
            align_factor_returns(_make_series([0.1], [1], ["A"]), s)
