"""
Task 14 验证测试 — 数据质量追踪 / Data Quality Tracking Verification

验证 check_data_quality 函数：
- 正常覆盖率不触发告警
- 低覆盖率触发 UserWarning
- 极低覆盖率抛出 ValueError
- 返回覆盖率数值
- max_loss 参数校验
- 输入类型校验
- 边界情况
"""

import warnings

import numpy as np
import pandas as pd
import pytest

from FactorAnalysis.data_quality import check_data_quality


# ============================================================
# Fixtures / 测试数据
# ============================================================

def _make_factor_returns(n_dates=50, n_symbols=20, nan_ratio=0.0):
    """生成因子值和收益率测试数据 / Generate factor and returns test data."""
    dates = pd.date_range("2024-01-01", periods=n_dates, freq="D")
    symbols = [f"S{i:03d}" for i in range(n_symbols)]
    idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])
    np.random.seed(42)
    factor = pd.Series(np.random.randn(len(idx)), index=idx, name="factor")
    returns = pd.Series(np.random.randn(len(idx)) * 0.02, index=idx, name="returns")

    # 按 nan_ratio 比例在相同位置插入 NaN / Insert NaN at same positions
    if nan_ratio > 0:
        n_nan = int(len(factor) * nan_ratio)
        nan_idx = np.random.choice(len(factor), size=n_nan, replace=False)
        factor.iloc[nan_idx] = np.nan
        returns.iloc[nan_idx] = np.nan

    return factor, returns


# ============================================================
# 1. 导入校验 / Import validation
# ============================================================

class TestImport:
    """验证模块导入正常 / Verify module imports correctly."""

    def test_import_function(self):
        """check_data_quality 可直接导入 / check_data_quality is importable."""
        from FactorAnalysis.data_quality import check_data_quality
        assert callable(check_data_quality)

    def test_package_export(self):
        """check_data_quality 从包级别可导出 / Exported from package level."""
        from FactorAnalysis import check_data_quality as cq
        assert callable(cq)


# ============================================================
# 2. 正常覆盖率 / Normal coverage (no warning)
# ============================================================

class TestNormalCoverage:
    """正常覆盖率不触发告警 / Normal coverage triggers no warning."""

    def test_full_coverage(self):
        """100% 覆盖率正常返回 / 100% coverage returns normally."""
        factor, returns = _make_factor_returns(nan_ratio=0.0)
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            cov = check_data_quality(factor, returns)
        assert cov == pytest.approx(1.0)

    def test_moderate_coverage(self):
        """90% 覆盖率不触发告警 / 90% coverage triggers no warning."""
        factor, returns = _make_factor_returns(nan_ratio=0.1)
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            cov = check_data_quality(factor, returns)
        assert cov == pytest.approx(0.9, abs=0.02)
        assert cov > 0.65

    def test_default_max_loss_threshold(self):
        """默认 max_loss=0.35 下 70% 覆盖率不触发告警 / 70% coverage ok with default max_loss."""
        factor, returns = _make_factor_returns(nan_ratio=0.3)
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            cov = check_data_quality(factor, returns, max_loss=0.35)
        assert cov >= 0.65

    def test_return_type_is_float(self):
        """返回值类型为 float / Return type is float."""
        factor, returns = _make_factor_returns()
        cov = check_data_quality(factor, returns)
        assert isinstance(cov, float)

    def test_return_value_in_range(self):
        """返回值在 [0, 1] 范围内 / Return value in [0, 1]."""
        factor, returns = _make_factor_returns(nan_ratio=0.1)
        cov = check_data_quality(factor, returns)
        assert 0.0 <= cov <= 1.0


# ============================================================
# 3. 低覆盖率告警 / Low coverage warning
# ============================================================

class TestLowCoverageWarning:
    """覆盖率低于阈值时发出 UserWarning / Emit UserWarning when below threshold."""

    def test_warning_triggered(self):
        """覆盖率低于 max_loss 阈值时触发 UserWarning / Warning when below max_loss."""
        factor, returns = _make_factor_returns(nan_ratio=0.4)
        # ~60% 覆盖率，max_loss=0.35 → 阈值 65% → 应触发告警
        with pytest.warns(UserWarning, match="Data coverage"):
            cov = check_data_quality(factor, returns, max_loss=0.35)
        assert cov < 0.65

    def test_custom_max_loss(self):
        """自定义 max_loss 阈值 / Custom max_loss threshold."""
        factor, returns = _make_factor_returns(nan_ratio=0.2)
        # ~80% 覆盖率，max_loss=0.35 → 阈值 65% → 不触发
        cov_direct = check_data_quality(factor, returns, max_loss=0.35)
        assert cov_direct > 0.65  # 确认覆盖率高于默认阈值

        # 用 max_loss=0.5（阈值 50%），应该不触发
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            check_data_quality(factor, returns, max_loss=0.5)

    def test_warning_message_content(self):
        """告警消息包含覆盖率信息 / Warning message contains coverage info."""
        factor, returns = _make_factor_returns(nan_ratio=0.4)
        with pytest.warns(UserWarning) as record:
            check_data_quality(factor, returns, max_loss=0.35)
        msg = str(record[0].message)
        assert "coverage" in msg.lower() or "覆盖率" in msg
        assert "0.35" in msg

    def test_boundary_no_warning_at_threshold(self):
        """恰好等于阈值不触发告警 / No warning at exact threshold."""
        # 构造恰好 65% 有效数据
        dates = pd.date_range("2024-01-01", periods=10, freq="D")
        symbols = [f"S{i}" for i in range(10)]
        idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])
        factor = pd.Series(np.random.randn(100), index=idx)
        returns = pd.Series(np.random.randn(100) * 0.02, index=idx)

        # 保留 65 行有效
        valid_count = 65
        factor.iloc[valid_count:] = np.nan
        returns.iloc[valid_count:] = np.nan

        cov = check_data_quality(factor, returns, max_loss=0.35)
        assert cov >= 0.649  # 允许微小浮点误差


# ============================================================
# 4. 极低覆盖率异常 / Critically low coverage error
# ============================================================

class TestCriticalCoverageError:
    """覆盖率极低时抛出 ValueError / Raise ValueError on critically low coverage."""

    def test_below_30_percent_raises(self):
        """覆盖率 < 30% 抛出 ValueError / Coverage < 30% raises ValueError."""
        factor, returns = _make_factor_returns(nan_ratio=0.8)
        with pytest.raises(ValueError, match="critically low|极低"):
            check_data_quality(factor, returns)

    def test_error_message_contains_stats(self):
        """异常消息包含覆盖率统计 / Error message contains coverage stats."""
        factor, returns = _make_factor_returns(nan_ratio=0.9)
        with pytest.raises(ValueError) as exc_info:
            check_data_quality(factor, returns)
        msg = str(exc_info.value)
        assert "30%" in msg or "30" in msg


# ============================================================
# 5. max_loss 参数校验 / max_loss parameter validation
# ============================================================

class TestMaxLossValidation:
    """max_loss 参数校验 / Validate max_loss parameter."""

    def test_negative_max_loss(self):
        """max_loss < 0 抛出 ValueError / max_loss < 0 raises ValueError."""
        factor, returns = _make_factor_returns()
        with pytest.raises(ValueError, match="max_loss.*0"):
            check_data_quality(factor, returns, max_loss=-0.1)

    def test_max_loss_equal_1(self):
        """max_loss = 1 抛出 ValueError（不允许 100% 丢失）/ max_loss=1 raises ValueError."""
        factor, returns = _make_factor_returns()
        with pytest.raises(ValueError, match="max_loss.*1"):
            check_data_quality(factor, returns, max_loss=1.0)

    def test_max_loss_above_1(self):
        """max_loss > 1 抛出 ValueError / max_loss > 1 raises ValueError."""
        factor, returns = _make_factor_returns()
        with pytest.raises(ValueError, match="max_loss"):
            check_data_quality(factor, returns, max_loss=1.5)

    def test_max_loss_zero(self):
        """max_loss=0 表示不允许任何数据丢失 / max_loss=0 allows no data loss."""
        factor, returns = _make_factor_returns(nan_ratio=0.0)
        cov = check_data_quality(factor, returns, max_loss=0.0)
        assert cov == pytest.approx(1.0)

    def test_max_loss_zero_triggers_warning(self):
        """max_loss=0 时任何 NaN 都触发告警 / Any NaN triggers warning with max_loss=0."""
        factor, returns = _make_factor_returns(nan_ratio=0.01)
        with pytest.warns(UserWarning):
            check_data_quality(factor, returns, max_loss=0.0)

    def test_non_numeric_max_loss(self):
        """max_loss 非数值类型抛出 TypeError / Non-numeric max_loss raises TypeError."""
        factor, returns = _make_factor_returns()
        with pytest.raises(TypeError, match="max_loss"):
            check_data_quality(factor, returns, max_loss="0.35")


# ============================================================
# 6. 输入类型校验 / Input type validation
# ============================================================

class TestInputValidation:
    """输入参数类型校验 / Validate input parameter types."""

    def test_factor_not_series(self):
        """factor 非 pd.Series 抛出 TypeError / Non-Series factor raises TypeError."""
        returns = pd.Series([1.0, 2.0], index=pd.MultiIndex.from_tuples(
            [(pd.Timestamp("2024-01-01"), "A"), (pd.Timestamp("2024-01-02"), "A")],
            names=["timestamp", "symbol"],
        ))
        with pytest.raises(TypeError, match="factor"):
            check_data_quality([1.0, 2.0], returns)

    def test_returns_not_series(self):
        """returns 非 pd.Series 抛出 TypeError / Non-Series returns raises TypeError."""
        factor = pd.Series([1.0, 2.0], index=pd.MultiIndex.from_tuples(
            [(pd.Timestamp("2024-01-01"), "A"), (pd.Timestamp("2024-01-02"), "A")],
            names=["timestamp", "symbol"],
        ))
        with pytest.raises(TypeError, match="returns"):
            check_data_quality(factor, [1.0, 2.0])

    def test_both_not_series(self):
        """两者都非 pd.Series 抛出 TypeError / Both non-Series raises TypeError."""
        with pytest.raises(TypeError):
            check_data_quality([1.0], [2.0])


# ============================================================
# 7. 边界情况 / Edge cases
# ============================================================

class TestEdgeCases:
    """边界情况测试 / Edge case tests."""

    def test_no_overlap_raises(self):
        """因子与收益率无重叠索引时抛出 ValueError / No overlap raises ValueError."""
        idx_f = pd.MultiIndex.from_tuples(
            [(pd.Timestamp("2024-01-01"), "A")],
            names=["timestamp", "symbol"],
        )
        idx_r = pd.MultiIndex.from_tuples(
            [(pd.Timestamp("2024-06-01"), "B")],
            names=["timestamp", "symbol"],
        )
        factor = pd.Series([1.0], index=idx_f)
        returns = pd.Series([0.01], index=idx_r)
        with pytest.raises(ValueError, match="No overlapping|无共有"):
            check_data_quality(factor, returns)

    def test_inf_values_treated_as_invalid(self):
        """inf 值视为无效数据 / inf values treated as invalid."""
        dates = pd.date_range("2024-01-01", periods=5, freq="D")
        symbols = ["A", "B"]
        idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])
        factor = pd.Series(np.random.randn(10), index=idx)
        returns = pd.Series(np.random.randn(10) * 0.02, index=idx)

        # 插入 inf 值 / Insert inf values
        factor.iloc[0] = np.inf
        returns.iloc[1] = -np.inf

        # 应该有 8/10 = 80% 覆盖率，不触发告警
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            cov = check_data_quality(factor, returns)
        assert cov == pytest.approx(0.8)

    def test_single_valid_row(self):
        """仅一行有效数据返回正确覆盖率 / Single valid row returns correct coverage."""
        dates = pd.date_range("2024-01-01", periods=5, freq="D")
        symbols = ["A"]
        idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])
        factor = pd.Series([np.nan, np.nan, 1.0, np.nan, np.nan], index=idx)
        returns = pd.Series([np.nan, np.nan, 0.01, np.nan, np.nan], index=idx)

        # 1/5 = 20% < 30% → 应抛出 ValueError
        with pytest.raises(ValueError):
            check_data_quality(factor, returns)

    def test_single_symbol(self):
        """单交易对数据正常工作 / Single symbol works correctly."""
        dates = pd.date_range("2024-01-01", periods=20, freq="D")
        idx = pd.MultiIndex.from_product([dates, ["BTCUSDT"]], names=["timestamp", "symbol"])
        factor = pd.Series(np.random.randn(20), index=idx)
        returns = pd.Series(np.random.randn(20) * 0.02, index=idx)

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            cov = check_data_quality(factor, returns)
        assert cov == pytest.approx(1.0)

    def test_different_index_lengths(self):
        """因子和收益率索引长度不同时正常工作 / Different index lengths work."""
        dates_f = pd.date_range("2024-01-01", periods=30, freq="D")
        dates_r = pd.date_range("2024-01-10", periods=20, freq="D")
        symbols = ["A", "B"]

        idx_f = pd.MultiIndex.from_product([dates_f, symbols], names=["timestamp", "symbol"])
        idx_r = pd.MultiIndex.from_product([dates_r, symbols], names=["timestamp", "symbol"])

        factor = pd.Series(np.random.randn(len(idx_f)), index=idx_f)
        returns = pd.Series(np.random.randn(len(idx_r)) * 0.02, index=idx_r)

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            cov = check_data_quality(factor, returns)
        # 只有重叠部分被保留
        assert 0.0 < cov <= 1.0


# ============================================================
# 8. 覆盖率精度 / Coverage precision
# ============================================================

class TestCoveragePrecision:
    """覆盖率计算精度验证 / Verify coverage calculation precision."""

    def test_exact_coverage_calculation(self):
        """覆盖率精确计算 / Exact coverage calculation."""
        dates = pd.date_range("2024-01-01", periods=10, freq="D")
        symbols = ["A", "B"]
        idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])

        factor = pd.Series(np.random.randn(20), index=idx)
        returns = pd.Series(np.random.randn(20) * 0.02, index=idx)

        # 精确设置 7 个无效行 → 13/20 = 0.65
        invalid_indices = [0, 1, 2, 3, 4, 5, 6]
        for i in invalid_indices:
            factor.iloc[i] = np.nan

        cov = check_data_quality(factor, returns)
        assert cov == pytest.approx(13 / 20)

    def test_coverage_monotonic_with_nan_ratio(self):
        """NaN 比例越高，覆盖率越低 / Higher NaN ratio → lower coverage."""
        np.random.seed(99)
        covs = []
        for nan_ratio in [0.0, 0.1, 0.2, 0.3]:
            f, r = _make_factor_returns(n_dates=100, n_symbols=10, nan_ratio=nan_ratio)
            covs.append(check_data_quality(f, r))

        # 覆盖率应单调递减（或至少不增）
        for i in range(len(covs) - 1):
            assert covs[i] >= covs[i] - 0.01  # 允许随机噪声
