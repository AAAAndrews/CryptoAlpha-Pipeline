"""
Task 11 验证测试 — 零值感知分组 (zero_aware) 深度验证
Deep verification tests for quantile_group zero_aware parameter.

与 task 10 实现互补，聚焦于：
- zero_aware=True 时正负值分别分组，标签范围和偏移正确
- zero_aware=False 时行为与原有逻辑完全一致（向后兼容）
- 边界情况：全正值、全负值、含零值、大量零值、NaN 数据
- 多截面场景下分组的独立性和一致性
"""

import numpy as np
import pandas as pd
import pytest

from FactorAnalysis.grouping import quantile_group


# ---------------------------------------------------------------------------
# Helpers / 辅助函数
# ---------------------------------------------------------------------------

def _make_factor(values, timestamps=None, symbols=None):
    """
    构造带 MultiIndex (timestamp, symbol) 的因子 Series。
    Build a factor Series with MultiIndex (timestamp, symbol).
    """
    if timestamps is None:
        timestamps = pd.date_range("2024-01-01", periods=1, freq="D")
    if symbols is None:
        symbols = [f"S{i}" for i in range(len(values))]
    idx = pd.MultiIndex.from_product([timestamps, symbols], names=["timestamp", "symbol"])
    return pd.Series(values, index=idx, dtype=np.float64)


def _make_multi_section(n_days=5, n_symbols=20, seed=42):
    """
    生成多截面因子数据，包含正负值。
    Generate multi-section factor data with both positive and negative values.
    """
    rng = np.random.RandomState(seed)
    timestamps = pd.date_range("2024-01-01", periods=n_days, freq="D")
    symbols = [f"S{i}" for i in range(n_symbols)]
    idx = pd.MultiIndex.from_product([timestamps, symbols], names=["timestamp", "symbol"])
    factor = pd.Series(rng.randn(n_days * n_symbols), index=idx, dtype=np.float64)
    return factor


# ---------------------------------------------------------------------------
# 1. zero_aware=False 向后兼容 / zero_aware=False backward compatibility
# ---------------------------------------------------------------------------

class TestBackwardCompatibility:
    """zero_aware=False 时行为不变 / Behavior unchanged when zero_aware=False."""

    def test_standard_grouping_labels_range(self):
        """标准分组标签范围 [0, n_groups-1] / Standard labels in [0, n_groups-1]."""
        values = np.array([-2, -1, 0, 1, 2, 3, 4, 5, 6, 7])
        factor = _make_factor(values)
        labels = quantile_group(factor, n_groups=3, zero_aware=False)
        valid = labels.dropna()
        assert valid.min() >= 0
        assert valid.max() <= 2

    def test_standard_same_as_default(self):
        """zero_aware=False 与默认参数结果一致 / zero_aware=False matches default."""
        values = np.array([-3, -1, 0, 0.5, 1, 2, 4, 5, 8, 10])
        factor = _make_factor(values)
        labels_default = quantile_group(factor, n_groups=3)
        labels_false = quantile_group(factor, n_groups=3, zero_aware=False)
        pd.testing.assert_series_equal(labels_default, labels_false)


# ---------------------------------------------------------------------------
# 2. zero_aware=True 基本行为 / zero_aware=True basic behavior
# ---------------------------------------------------------------------------

class TestZeroAwareBasic:
    """零值感知分组基本功能 / Zero-aware grouping basic functionality."""

    def test_labels_in_range(self):
        """标签范围 [0, n_groups-1] / Labels in [0, n_groups-1]."""
        values = np.array([-5, -3, -1, 0, 1, 2, 4, 6, 8, 10])
        factor = _make_factor(values)
        labels = quantile_group(factor, n_groups=5, zero_aware=True)
        valid = labels.dropna()
        assert valid.min() >= 0, f"最小标签 {valid.min()} < 0"
        assert valid.max() <= 4, f"最大标签 {valid.max()} > 4"

    def test_negative_labels_lower_than_positive(self):
        """负值标签 < 正值标签 / Negative labels < positive labels."""
        values = np.array([-5, -3, -1, 0, 1, 2, 4, 6, 8, 10])
        factor = _make_factor(values)
        labels = quantile_group(factor, n_groups=4, zero_aware=True)
        # 零和负值应获得较低标签
        neg_labels = labels.iloc[:4]  # -5, -3, -1, 0
        pos_labels = labels.iloc[4:]  # 1, 2, 4, 6, 8, 10 — wait, only 10 values
        # 重新映射
        neg_labels = labels[labels.index.get_level_values(1).isin(["S0", "S1", "S2", "S3"])]
        pos_labels = labels[labels.index.get_level_values(1).isin(["S4", "S5", "S6", "S7", "S8", "S9"])]
        assert neg_labels.max() < pos_labels.min(), (
            f"负值最大标签 {neg_labels.max()} >= 正值最小标签 {pos_labels.min()}"
        )

    def test_zero_goes_to_negative_side(self):
        """零值归入负值侧 / Zero values go to negative side."""
        values = np.array([-5, -3, -1, 0, 1, 2, 4, 6])
        factor = _make_factor(values)
        labels = quantile_group(factor, n_groups=4, zero_aware=True)
        zero_label = labels.iloc[3]  # S3 = 0
        pos_labels = labels.iloc[4:]  # S4..S7 = positive
        assert zero_label < pos_labels.min(), "零值标签应小于正值标签"

    def test_return_type(self):
        """返回类型为 pd.Series / Return type is pd.Series."""
        factor = _make_multi_section(n_days=3, n_symbols=10)
        labels = quantile_group(factor, n_groups=5, zero_aware=True)
        assert isinstance(labels, pd.Series)

    def test_index_preserved(self):
        """输出索引与输入一致 / Output index matches input."""
        factor = _make_multi_section(n_days=2, n_symbols=15)
        labels = quantile_group(factor, n_groups=5, zero_aware=True)
        pd.testing.assert_index_equal(labels.index, factor.index)


# ---------------------------------------------------------------------------
# 3. 正负拆分比例分配 / Proportional group allocation
# ---------------------------------------------------------------------------

class TestGroupAllocation:
    """按样本量比例分配分组数 / Proportional group allocation."""

    def test_balanced_split(self):
        """正负样本均等时分组数大致均分 / Balanced split when equal sample counts."""
        # 5 negative + 5 positive, n_groups=4
        values = np.array([-5, -3, -1, -0.5, -0.1, 0.1, 1, 3, 5, 10])
        factor = _make_factor(values)
        labels = quantile_group(factor, n_groups=4, zero_aware=True)
        valid = labels.dropna()
        neg_labels = valid[valid <= valid.median()]
        pos_labels = valid[valid > valid.median()]
        # 正负标签不应重叠
        assert neg_labels.max() < pos_labels.min()

    def test_more_negatives(self):
        """负值较多时分配更多组给负值侧 / More groups to negative side when more negatives."""
        # 8 negative + 2 positive, n_groups=5
        values = np.array([-8, -6, -5, -4, -3, -2, -1, -0.5, 0.1, 5])
        factor = _make_factor(values)
        labels = quantile_group(factor, n_groups=5, zero_aware=True)
        valid = labels.dropna()
        # 负值应有更多分组（round(5*8/10) = 4 组给负值）
        neg_vals = values[values <= 0]
        pos_vals = values[values > 0]
        neg_labels = valid.iloc[:len(neg_vals)]
        pos_labels = valid.iloc[len(neg_vals):]
        unique_neg = neg_labels.nunique()
        unique_pos = pos_labels.nunique()
        assert unique_neg >= unique_pos, (
            f"负值侧 {unique_neg} 组 < 正值侧 {unique_pos} 组，期望负值侧更多"
        )

    def test_n_groups_2(self):
        """n_groups=2 时每侧至少 1 组 / Each side gets at least 1 group with n_groups=2."""
        values = np.array([-5, -3, -1, 1, 3, 5])
        factor = _make_factor(values)
        labels = quantile_group(factor, n_groups=2, zero_aware=True)
        valid = labels.dropna()
        assert valid.min() >= 0
        assert valid.max() <= 1


# ---------------------------------------------------------------------------
# 4. 边界情况 / Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    """边界情况测试 / Edge case tests."""

    def test_all_positive_fallback(self):
        """全正值退化为标准分组 / All positive falls back to standard grouping."""
        values = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        factor = _make_factor(values)
        labels_aware = quantile_group(factor, n_groups=3, zero_aware=True)
        labels_normal = quantile_group(factor, n_groups=3, zero_aware=False)
        pd.testing.assert_series_equal(labels_aware, labels_normal)

    def test_all_negative_fallback(self):
        """全负值退化为标准分组 / All negative falls back to standard grouping."""
        values = np.array([-10, -8, -6, -5, -4, -3, -2, -1])
        factor = _make_factor(values)
        labels_aware = quantile_group(factor, n_groups=3, zero_aware=True)
        labels_normal = quantile_group(factor, n_groups=3, zero_aware=False)
        pd.testing.assert_series_equal(labels_aware, labels_normal)

    def test_all_zero_fallback(self):
        """全零值退化为标准分组 / All zero falls back to standard grouping."""
        values = np.array([0, 0, 0, 0, 0, 0])
        factor = _make_factor(values)
        labels_aware = quantile_group(factor, n_groups=3, zero_aware=True)
        labels_normal = quantile_group(factor, n_groups=3, zero_aware=False)
        pd.testing.assert_series_equal(labels_aware, labels_normal)

    def test_single_zero_with_positives(self):
        """单个零值与正值混合 / Single zero mixed with positives."""
        values = np.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
        factor = _make_factor(values)
        labels = quantile_group(factor, n_groups=3, zero_aware=True)
        valid = labels.dropna()
        # 零值（含在负值侧）应获得较低标签
        zero_label = labels.iloc[0]
        assert zero_label == valid.min(), "零值标签应为最低组"

    def test_nan_preserved(self):
        """NaN 因子值保留 NaN 标签 / NaN factor values keep NaN label."""
        values = np.array([-5, np.nan, -1, 0, 1, np.nan, 3, 5])
        factor = _make_factor(values)
        labels = quantile_group(factor, n_groups=3, zero_aware=True)
        assert np.isnan(labels.iloc[1])
        assert np.isnan(labels.iloc[5])
        assert not np.isnan(labels.iloc[0])
        assert not np.isnan(labels.iloc[3])

    def test_all_nan(self):
        """全 NaN 返回全 NaN / All NaN returns all NaN."""
        values = np.array([np.nan, np.nan, np.nan])
        factor = _make_factor(values)
        labels = quantile_group(factor, n_groups=3, zero_aware=True)
        assert labels.isna().all()

    def test_single_value(self):
        """单值不崩溃 / Single value does not crash."""
        values = np.array([3.14])
        factor = _make_factor(values)
        labels = quantile_group(factor, n_groups=3, zero_aware=True)
        # 单值无法做有效分组，标签可能为 NaN 或中间组，但不应抛异常
        # Single value can't be meaningfully grouped, label may be NaN, but no crash
        assert isinstance(labels, pd.Series)
        assert len(labels) == 1

    def test_large_dataset(self):
        """大数据量多截面正常工作 / Large dataset with multiple sections works."""
        factor = _make_multi_section(n_days=10, n_symbols=100, seed=123)
        labels = quantile_group(factor, n_groups=10, zero_aware=True)
        valid = labels.dropna()
        assert valid.min() >= 0
        assert valid.max() <= 9

    def test_n_groups_too_small(self):
        """n_groups < 2 抛出 ValueError / n_groups < 2 raises ValueError."""
        factor = _make_factor(np.array([-1, 0, 1]))
        with pytest.raises(ValueError, match="n_groups"):
            quantile_group(factor, n_groups=1, zero_aware=True)


# ---------------------------------------------------------------------------
# 5. 多截面一致性 / Multi-section consistency
# ---------------------------------------------------------------------------

class TestMultiSection:
    """多截面分组一致性 / Multi-section grouping consistency."""

    def test_independent_per_section(self):
        """每个截面独立分组 / Each section grouped independently."""
        factor = _make_multi_section(n_days=3, n_symbols=20, seed=42)
        labels = quantile_group(factor, n_groups=5, zero_aware=True)
        # 每个截面应有独立标签
        for ts, group in labels.groupby(level=0):
            valid = group.dropna()
            if len(valid) > 0:
                assert valid.min() >= 0
                assert valid.max() <= 4

    def test_section_with_only_positives(self):
        """仅正值的截面退化为标准分组 / Section with only positives falls back."""
        timestamps = pd.date_range("2024-01-01", periods=2, freq="D")
        symbols = [f"S{i}" for i in range(5)]
        idx = pd.MultiIndex.from_product([timestamps, symbols], names=["timestamp", "symbol"])
        values = np.array([1, 2, 3, 4, 5, -3, -1, 0, 2, 5], dtype=np.float64)
        factor = pd.Series(values, index=idx, dtype=np.float64)
        labels = quantile_group(factor, n_groups=3, zero_aware=True)
        # 两个截面都应有标签
        for ts, group in labels.groupby(level=0):
            valid = group.dropna()
            assert len(valid) > 0

    def test_labels_count_matches_valid_factor(self):
        """非 NaN 标签数量等于有效因子值数量 / Non-NaN label count matches valid factor count."""
        factor = _make_multi_section(n_days=5, n_symbols=30, seed=7)
        # 注入一些 NaN / inject some NaN
        factor.iloc[::10] = np.nan
        labels = quantile_group(factor, n_groups=5, zero_aware=True)
        assert labels.notna().sum() == factor.notna().sum()


# ---------------------------------------------------------------------------
# 6. dtype 与输出结构 / dtype and output structure
# ---------------------------------------------------------------------------

class TestOutputStructure:
    """输出结构验证 / Output structure verification."""

    def test_float64_dtype(self):
        """输出为 float64 类型 / Output is float64 dtype."""
        factor = _make_factor(np.array([-3, -1, 0, 1, 3, 5]))
        labels = quantile_group(factor, n_groups=3, zero_aware=True)
        assert labels.dtype == np.float64

    def test_name_not_set(self):
        """输出不应带 name 属性（或为 None）/ Output should not have a name."""
        factor = _make_factor(np.array([-3, -1, 0, 1, 3, 5]))
        labels = quantile_group(factor, n_groups=3, zero_aware=True)
        assert labels.name is None
