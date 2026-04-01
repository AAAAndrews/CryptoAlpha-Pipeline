"""
Task 15 验证测试 — 数据质量追踪深度验证
Deep verification tests for check_data_quality.

与 task 14 单元测试互补，聚焦于：
- 返回结构与值域严格验证 / Strict return structure and value range verification
- 多种子参数化不同 NaN 比例下的覆盖率行为 / Multi-seed parameterized NaN ratio behavior
- max_loss 阈值边界精确验证 / Precise max_loss threshold boundary verification
- 对齐后重叠索引的覆盖率计算 / Coverage after index alignment
- inf / -inf / 混合无效值处理 / Mixed invalid value handling
- 不同规模数据的性能与正确性 / Correctness at various data scales
"""

import warnings

import numpy as np
import pandas as pd
import pytest

from FactorAnalysis.data_quality import check_data_quality


# ---------------------------------------------------------------------------
# Helpers / 辅助函数
# ---------------------------------------------------------------------------

def _make_aligned_data(n_dates, n_symbols, nan_factor_ratio=0.0, nan_returns_ratio=0.0, seed=42):
    """
    生成因子值和收益率 Series，支持分别控制 NaN 比例。
    Generate factor and returns Series with independent NaN ratio control.
    """
    rng = np.random.RandomState(seed)
    dates = pd.date_range("2024-01-01", periods=n_dates, freq="D")
    symbols = [f"S{i:03d}" for i in range(n_symbols)]
    idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])
    n = len(idx)

    factor = pd.Series(rng.randn(n), index=idx, name="factor")
    returns = pd.Series(rng.randn(n) * 0.02, index=idx, name="returns")

    # 分别在因子和收益率中插入 NaN / Insert NaN independently
    if nan_factor_ratio > 0:
        k = int(n * nan_factor_ratio)
        pos = rng.choice(n, size=k, replace=False)
        factor.iloc[pos] = np.nan

    if nan_returns_ratio > 0:
        k = int(n * nan_returns_ratio)
        pos = rng.choice(n, size=k, replace=False)
        returns.iloc[pos] = np.nan

    return factor, returns


def _make_partial_overlap(factor_dates, returns_dates, symbols, seed=42):
    """
    生成因子与收益率仅有部分重叠索引的数据。
    Generate data where factor and returns have partial index overlap.
    """
    rng = np.random.RandomState(seed)

    idx_f = pd.MultiIndex.from_product([factor_dates, symbols], names=["timestamp", "symbol"])
    idx_r = pd.MultiIndex.from_product([returns_dates, symbols], names=["timestamp", "symbol"])

    factor = pd.Series(rng.randn(len(idx_f)), index=idx_f, name="factor")
    returns = pd.Series(rng.randn(len(idx_r)) * 0.02, index=idx_r, name="returns")
    return factor, returns


# ============================================================
# 1. 返回结构与类型 / Return structure and type
# ============================================================

class TestReturnStructure:
    """验证返回值结构与类型 / Verify return value structure and type."""

    @pytest.mark.parametrize("n_dates,n_symbols", [(10, 5), (50, 20), (100, 50)])
    def test_return_type_float(self, n_dates, n_symbols):
        """返回值始终为 float / Return is always float."""
        factor, returns = _make_aligned_data(n_dates, n_symbols, seed=7)
        cov = check_data_quality(factor, returns)
        assert isinstance(cov, (float, np.floating))

    @pytest.mark.parametrize("n_dates,n_symbols", [(10, 5), (50, 20)])
    def test_return_in_unit_range(self, n_dates, n_symbols):
        """返回值在 [0.0, 1.0] / Return in [0.0, 1.0]."""
        factor, returns = _make_aligned_data(n_dates, n_symbols, seed=11)
        cov = check_data_quality(factor, returns)
        assert 0.0 <= cov <= 1.0

    def test_full_coverage_is_one(self):
        """无 NaN 时覆盖率精确为 1.0 / Exact 1.0 when no NaN."""
        factor, returns = _make_aligned_data(30, 10, seed=1)
        cov = check_data_quality(factor, returns)
        assert cov == pytest.approx(1.0)


# ============================================================
# 2. 正常覆盖率无告警 / Normal coverage no warning
# ============================================================

class TestNormalCoverageNoWarning:
    """正常覆盖率不触发告警 / Normal coverage triggers no warning."""

    @pytest.mark.parametrize("nan_ratio", [0.0, 0.05, 0.1, 0.2, 0.3])
    def test_no_warning_various_nan_ratios(self, nan_ratio):
        """各种 NaN 比例（≤30%）不触发告警 / Various NaN ratios ≤30% no warning."""
        factor, returns = _make_aligned_data(50, 20, nan_factor_ratio=nan_ratio, seed=42)
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            cov = check_data_quality(factor, returns, max_loss=0.35)
        assert cov >= 0.65

    @pytest.mark.parametrize("seed", [1, 42, 99, 123, 255])
    def test_multi_seed_stability(self, seed):
        """多种子稳定性：相同参数下覆盖率一致 / Multi-seed stability."""
        factor, returns = _make_aligned_data(40, 15, seed=seed)
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            cov = check_data_quality(factor, returns)
        assert cov == pytest.approx(1.0)


# ============================================================
# 3. 低覆盖率告警触发 / Low coverage warning triggered
# ============================================================

class TestLowCoverageWarning:
    """低覆盖率触发 UserWarning / Low coverage triggers UserWarning."""

    @pytest.mark.parametrize("nan_ratio", [0.4, 0.5, 0.6])
    def test_warning_at_various_nan_ratios(self, nan_ratio):
        """不同 NaN 比例触发告警 / Warning at various NaN ratios."""
        factor, returns = _make_aligned_data(50, 20, nan_factor_ratio=nan_ratio, seed=42)
        with pytest.warns(UserWarning, match="coverage|覆盖率"):
            cov = check_data_quality(factor, returns, max_loss=0.35)
        assert cov < 0.65

    def test_warning_message_contains_max_loss(self):
        """告警消息包含 max_loss 值 / Warning message contains max_loss value."""
        factor, returns = _make_aligned_data(50, 20, nan_factor_ratio=0.5, seed=42)
        with pytest.warns(UserWarning) as record:
            check_data_quality(factor, returns, max_loss=0.35)
        msg = str(record[0].message)
        assert "0.35" in msg

    def test_warning_message_contains_valid_rows(self):
        """告警消息包含有效行数信息 / Warning message contains valid row count."""
        factor, returns = _make_aligned_data(50, 20, nan_factor_ratio=0.5, seed=42)
        with pytest.warns(UserWarning) as record:
            check_data_quality(factor, returns, max_loss=0.35)
        msg = str(record[0].message)
        # 消息中应包含有效行/总行数格式信息
        assert "/" in msg or "valid" in msg.lower() or "有效" in msg

    @pytest.mark.parametrize("max_loss,expect_warn", [
        (0.1, True),   # 阈值 90%，~50% 覆盖率 → 告警
        (0.5, False),  # 阈值 50%，~50% 覆盖率 → 不告警
        (0.7, False),  # 阈值 30%，~50% 覆盖率 → 不告警
    ])
    def test_max_loss_controls_threshold(self, max_loss, expect_warn):
        """max_loss 控制告警阈值 / max_loss controls warning threshold."""
        factor, returns = _make_aligned_data(50, 20, nan_factor_ratio=0.5, seed=42)
        if expect_warn:
            with pytest.warns(UserWarning):
                check_data_quality(factor, returns, max_loss=max_loss)
        else:
            with warnings.catch_warnings():
                warnings.simplefilter("error")
                check_data_quality(factor, returns, max_loss=max_loss)


# ============================================================
# 4. 极低覆盖率异常 / Critically low coverage error
# ============================================================

class TestCriticalCoverageError:
    """覆盖率 < 30% 抛出 ValueError / Coverage < 30% raises ValueError."""

    @pytest.mark.parametrize("nan_ratio", [0.71, 0.8, 0.9, 0.99])
    def test_raises_at_various_low_ratios(self, nan_ratio):
        """不同极低 NaN 比例均抛出异常 / Various critically low ratios raise error."""
        factor, returns = _make_aligned_data(50, 20, nan_factor_ratio=nan_ratio, seed=42)
        with pytest.raises(ValueError, match="critically low|极低"):
            check_data_quality(factor, returns)

    def test_error_precedes_warning(self):
        """覆盖率 < 30% 时 ValueError 优先于 UserWarning / ValueError takes precedence."""
        # 覆盖率 < 30% 但也 < (1-max_loss)，应直接抛异常而非先告警
        factor, returns = _make_aligned_data(50, 20, nan_factor_ratio=0.8, seed=42)
        # 不应看到 UserWarning，直接 ValueError
        with warnings.catch_warnings():
            warnings.simplefilter("error")  # 将 warning 也视为错误
            with pytest.raises(ValueError):
                check_data_quality(factor, returns)

    def test_error_message_contains_coverage_pct(self):
        """异常消息包含覆盖率百分比 / Error message contains coverage percentage."""
        factor, returns = _make_aligned_data(50, 20, nan_factor_ratio=0.9, seed=42)
        with pytest.raises(ValueError) as exc_info:
            check_data_quality(factor, returns)
        msg = str(exc_info.value)
        assert "%" in msg or "30" in msg


# ============================================================
# 5. max_loss 参数校验 / max_loss parameter validation
# ============================================================

class TestMaxLossValidation:
    """max_loss 参数校验 / Validate max_loss parameter."""

    @pytest.mark.parametrize("bad_value", [-1.0, -0.5, -0.01])
    def test_negative_max_loss_rejected(self, bad_value):
        """负值 max_loss 被拒绝 / Negative max_loss rejected."""
        factor, returns = _make_aligned_data(10, 5, seed=1)
        with pytest.raises(ValueError, match="max_loss"):
            check_data_quality(factor, returns, max_loss=bad_value)

    @pytest.mark.parametrize("bad_value", [1.0, 1.5, 2.0, 100.0])
    def test_max_loss_ge_one_rejected(self, bad_value):
        """max_loss ≥ 1 被拒绝 / max_loss ≥ 1 rejected."""
        factor, returns = _make_aligned_data(10, 5, seed=1)
        with pytest.raises(ValueError, match="max_loss"):
            check_data_quality(factor, returns, max_loss=bad_value)

    @pytest.mark.parametrize("bad_value", ["0.5", None, [0.5], {"v": 0.5}])
    def test_non_numeric_max_loss_rejected(self, bad_value):
        """非数值类型 max_loss 被拒绝 / Non-numeric max_loss rejected."""
        factor, returns = _make_aligned_data(10, 5, seed=1)
        with pytest.raises(TypeError, match="max_loss"):
            check_data_quality(factor, returns, max_loss=bad_value)

    @pytest.mark.parametrize("good_value", [0.0, 0.01, 0.35, 0.5, 0.99])
    def test_valid_max_loss_accepted(self, good_value):
        """合法 max_loss 值正常工作 / Valid max_loss works correctly."""
        factor, returns = _make_aligned_data(20, 10, seed=1)
        # 只要数据干净就不触发告警
        cov = check_data_quality(factor, returns, max_loss=good_value)
        assert cov == pytest.approx(1.0)


# ============================================================
# 6. 输入类型校验 / Input type validation
# ============================================================

class TestInputTypeValidation:
    """输入参数类型校验 / Validate input parameter types."""

    def test_factor_list_raises_type_error(self):
        """factor 为 list 抛出 TypeError / factor as list raises TypeError."""
        returns = pd.Series([0.01], index=pd.MultiIndex.from_tuples(
            [(pd.Timestamp("2024-01-01"), "A")], names=["timestamp", "symbol"]))
        with pytest.raises(TypeError, match="factor"):
            check_data_quality([1.0], returns)

    def test_returns_dataframe_raises_type_error(self):
        """returns 为 DataFrame 抛出 TypeError / returns as DataFrame raises TypeError."""
        idx = pd.MultiIndex.from_tuples(
            [(pd.Timestamp("2024-01-01"), "A")], names=["timestamp", "symbol"])
        factor = pd.Series([1.0], index=idx)
        returns_df = pd.DataFrame({"ret": [0.01]}, index=idx)
        with pytest.raises(TypeError, match="returns"):
            check_data_quality(factor, returns_df)

    def test_numpy_array_factor_raises_type_error(self):
        """factor 为 numpy 数组抛出 TypeError / factor as ndarray raises TypeError."""
        factor = np.array([1.0, 2.0])
        returns = pd.Series([0.01, 0.02], index=pd.MultiIndex.from_tuples(
            [(pd.Timestamp("2024-01-01"), "A"), (pd.Timestamp("2024-01-02"), "A")],
            names=["timestamp", "symbol"]))
        with pytest.raises(TypeError, match="factor"):
            check_data_quality(factor, returns)


# ============================================================
# 7. 索引对齐 / Index alignment
# ============================================================

class TestIndexAlignment:
    """索引对齐后覆盖率计算 / Coverage after index alignment."""

    def test_partial_overlap_coverage(self):
        """部分重叠索引仅计算交集 / Only intersection used for coverage."""
        factor_dates = pd.date_range("2024-01-01", periods=30, freq="D")
        returns_dates = pd.date_range("2024-01-10", periods=20, freq="D")
        symbols = ["A", "B"]

        factor, returns = _make_partial_overlap(factor_dates, returns_dates, symbols, seed=42)

        # 重叠部分：10 dates × 2 symbols = 20 rows
        # 全部有效 → 覆盖率应为 1.0（基于交集）
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            cov = check_data_quality(factor, returns)
        assert cov == pytest.approx(1.0)

    def test_partial_overlap_with_nan(self):
        """部分重叠且有 NaN 时正确计算 / Partial overlap with NaN computed correctly."""
        factor_dates = pd.date_range("2024-01-01", periods=30, freq="D")
        returns_dates = pd.date_range("2024-01-10", periods=20, freq="D")
        symbols = ["A", "B"]

        factor, returns = _make_partial_overlap(factor_dates, returns_dates, symbols, seed=42)

        # 在重叠区域（索引位置 20~39，即 10 dates × 2 symbols）中插入 NaN
        # 重叠索引为 2024-01-10 到 2024-01-29
        overlap_dates = pd.date_range("2024-01-10", periods=20, freq="D")
        for i, d in enumerate(overlap_dates[:5]):  # 前 5 天设 NaN
            factor.loc[(d, "A")] = np.nan
            factor.loc[(d, "B")] = np.nan

        cov = check_data_quality(factor, returns)
        # 重叠 40 行中 10 行 NaN → 覆盖率 30/40 = 0.75
        assert cov == pytest.approx(0.75)

    def test_no_overlap_raises(self):
        """无重叠索引抛出 ValueError / No overlap raises ValueError."""
        factor_dates = pd.date_range("2024-01-01", periods=10, freq="D")
        returns_dates = pd.date_range("2024-06-01", periods=10, freq="D")
        factor, returns = _make_partial_overlap(factor_dates, returns_dates, ["A"], seed=1)
        with pytest.raises(ValueError, match="No overlapping|无共有"):
            check_data_quality(factor, returns)

    def test_single_symbol_overlap(self):
        """单交易对部分重叠 / Single symbol partial overlap."""
        factor_dates = pd.date_range("2024-01-01", periods=20, freq="D")
        returns_dates = pd.date_range("2024-01-05", periods=15, freq="D")
        factor, returns = _make_partial_overlap(factor_dates, returns_dates, ["BTCUSDT"], seed=42)

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            cov = check_data_quality(factor, returns)
        assert cov == pytest.approx(1.0)


# ============================================================
# 8. inf / -inf / 混合无效值 / inf / -inf / mixed invalid values
# ============================================================

class TestInvalidValues:
    """无效值处理 / Invalid value handling."""

    def test_inf_in_factor(self):
        """因子含 inf 视为无效 / inf in factor treated as invalid."""
        factor, returns = _make_aligned_data(10, 5, seed=1)
        factor.iloc[0] = np.inf
        cov = check_data_quality(factor, returns)
        # 1 row invalid out of 50
        assert cov == pytest.approx(49 / 50)

    def test_neg_inf_in_returns(self):
        """收益率含 -inf 视为无效 / -inf in returns treated as invalid."""
        factor, returns = _make_aligned_data(10, 5, seed=1)
        returns.iloc[1] = -np.inf
        cov = check_data_quality(factor, returns)
        assert cov == pytest.approx(49 / 50)

    def test_inf_both_sides(self):
        """两侧同位置 inf 视为 1 行无效 / inf on both sides = 1 invalid row."""
        factor, returns = _make_aligned_data(10, 5, seed=1)
        factor.iloc[0] = np.inf
        returns.iloc[0] = np.inf
        cov = check_data_quality(factor, returns)
        assert cov == pytest.approx(49 / 50)

    def test_inf_different_positions(self):
        """两侧不同位置 inf 视为 2 行无效 / inf on different positions = 2 invalid rows."""
        factor, returns = _make_aligned_data(10, 5, seed=1)
        factor.iloc[0] = np.inf
        returns.iloc[1] = np.inf
        cov = check_data_quality(factor, returns)
        assert cov == pytest.approx(48 / 50)

    def test_nan_and_inf_mixed(self):
        """NaN 和 inf 混合无效值 / Mixed NaN and inf invalid values."""
        factor, returns = _make_aligned_data(10, 5, seed=1)
        factor.iloc[0] = np.nan
        factor.iloc[1] = np.inf
        returns.iloc[2] = -np.inf
        returns.iloc[3] = np.nan
        # 4 rows invalid (0,1 from factor; 2,3 from returns)
        cov = check_data_quality(factor, returns)
        assert cov == pytest.approx(46 / 50)

    @pytest.mark.parametrize("bad_val", [np.inf, -np.inf, np.nan])
    def test_single_inf_or_nan_in_small_data(self, bad_val):
        """小数据集中单个无效值影响显著 / Single invalid value significant in small data."""
        dates = pd.date_range("2024-01-01", periods=5, freq="D")
        symbols = ["A"]
        idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])
        factor = pd.Series([1.0, 2.0, bad_val, 4.0, 5.0], index=idx)
        returns = pd.Series([0.01] * 5, index=idx)
        cov = check_data_quality(factor, returns)
        assert cov == pytest.approx(4 / 5)


# ============================================================
# 9. 因子与收益率独立 NaN / Independent NaN in factor vs returns
# ============================================================

class TestIndependentNaN:
    """因子与收益率独立 NaN / Independent NaN in factor and returns."""

    def test_nan_in_factor_only(self):
        """仅因子含 NaN 时覆盖率正确 / NaN only in factor → correct coverage."""
        factor, returns = _make_aligned_data(20, 10, nan_factor_ratio=0.2, seed=42)
        cov = check_data_quality(factor, returns)
        assert 0.75 <= cov <= 0.85  # ~80% coverage

    def test_nan_in_returns_only(self):
        """仅收益率含 NaN 时覆盖率正确 / NaN only in returns → correct coverage."""
        factor, returns = _make_aligned_data(20, 10, nan_returns_ratio=0.2, seed=42)
        cov = check_data_quality(factor, returns)
        assert 0.75 <= cov <= 0.85

    def test_nan_in_both_independent(self):
        """两侧独立 NaN 取并集 / Independent NaN on both sides → union."""
        factor, returns = _make_aligned_data(
            20, 10, nan_factor_ratio=0.1, nan_returns_ratio=0.1, seed=42
        )
        cov = check_data_quality(factor, returns)
        # 两侧各 10% NaN，取并集后有效行 < 90%
        assert cov < 0.95
        assert cov > 0.75  # 但不会太低


# ============================================================
# 10. 边界情况 / Edge cases
# ============================================================

class TestEdgeCases:
    """边界情况 / Edge cases."""

    def test_minimal_valid_data_30_percent(self):
        """恰好 30% 覆盖率不抛异常 / Exactly 30% coverage does not raise."""
        dates = pd.date_range("2024-01-01", periods=10, freq="D")
        symbols = ["A"]
        idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])
        factor = pd.Series([np.nan] * 7 + [1.0, 2.0, 3.0], index=idx)
        returns = pd.Series([np.nan] * 7 + [0.01, 0.02, 0.03], index=idx)

        # 3/10 = 30% → 不应抛 ValueError（≥ 0.3），但 max_loss=0.35 阈值 65% → 应触发 warning
        with pytest.warns(UserWarning):
            cov = check_data_quality(factor, returns, max_loss=0.35)
        assert cov == pytest.approx(0.3)

    def test_just_below_30_percent_raises(self):
        """略低于 30% 覆盖率抛异常 / Just below 30% raises."""
        dates = pd.date_range("2024-01-01", periods=10, freq="D")
        symbols = ["A"]
        idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])
        factor = pd.Series([np.nan] * 8 + [1.0, 2.0], index=idx)
        returns = pd.Series([np.nan] * 8 + [0.01, 0.02], index=idx)

        # 2/10 = 20% < 30% → ValueError
        with pytest.raises(ValueError):
            check_data_quality(factor, returns)

    def test_large_data_scale(self):
        """大数据量（1000 dates × 100 symbols）正常工作 / Large data works."""
        factor, returns = _make_aligned_data(100, 100, nan_factor_ratio=0.05, seed=42)
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            cov = check_data_quality(factor, returns)
        assert cov > 0.9

    def test_many_symbols_few_dates(self):
        """多交易对少日期正常工作 / Many symbols few dates works."""
        factor, returns = _make_aligned_data(3, 200, seed=42)
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            cov = check_data_quality(factor, returns)
        assert cov == pytest.approx(1.0)

    def test_few_symbols_many_dates(self):
        """少交易对多日期正常工作 / Few symbols many dates works."""
        factor, returns = _make_aligned_data(200, 3, seed=42)
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            cov = check_data_quality(factor, returns)
        assert cov == pytest.approx(1.0)

    def test_max_loss_zero_strict(self):
        """max_loss=0 且有 NaN 必触发告警 / max_loss=0 with any NaN must warn."""
        factor, returns = _make_aligned_data(50, 20, nan_factor_ratio=0.001, seed=42)
        with pytest.warns(UserWarning):
            check_data_quality(factor, returns, max_loss=0.0)

    def test_max_loss_close_to_one(self):
        """max_loss=0.99 允许几乎所有数据丢失 / max_loss=0.99 allows almost all loss."""
        factor, returns = _make_aligned_data(50, 20, nan_factor_ratio=0.95, seed=42)
        # ~5% 覆盖率，max_loss=0.99 → 阈值 1% → 但 5% < 30% → 仍抛 ValueError
        with pytest.raises(ValueError):
            check_data_quality(factor, returns, max_loss=0.99)


# ============================================================
# 11. 覆盖率单调性 / Coverage monotonicity
# ============================================================

class TestCoverageMonotonicity:
    """覆盖率随 NaN 比例单调递减 / Coverage decreases monotonically with NaN ratio."""

    @pytest.mark.parametrize("seed", [42, 99, 123])
    def test_monotonic_decrease(self, seed):
        """NaN 比例递增 → 覆盖率递减 / Increasing NaN → decreasing coverage."""
        coverages = []
        for nan_ratio in [0.0, 0.05, 0.1, 0.15, 0.2]:
            f, r = _make_aligned_data(100, 20, nan_factor_ratio=nan_ratio, seed=seed)
            coverages.append(check_data_quality(f, r))

        for i in range(len(coverages) - 1):
            assert coverages[i] >= coverages[i + 1] - 1e-10, (
                f"Coverage not monotonic at seed={seed}: "
                f"{coverages[i]:.4f} < {coverages[i+1]:.4f}"
            )
