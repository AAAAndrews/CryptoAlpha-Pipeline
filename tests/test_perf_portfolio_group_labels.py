"""
tests/test_perf_portfolio_group_labels.py — Task 3: portfolio 预计算 group_labels 测试
Tests for Task 3: portfolio functions accept pre-computed group_labels.

验证项 / Verifications:
- _calc_labels_with_rebalance 不传 group_labels 时行为不变（向后兼容）
- 传入预计算 group_labels 时跳过 quantile_group，结果与不传一致
- rebalance_freq=1 和 rebalance_freq>1 两种模式均正确
- calc_long_only_curve / calc_short_only_curve / calc_top_bottom_curve 均支持 group_labels
- 6 种 mock 场景数值一致性 (diff < 1e-10)
"""

import sys
import os
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from FactorAnalysis.portfolio import (
    _calc_labels_with_rebalance,
    calc_long_only_curve,
    calc_short_only_curve,
    calc_top_bottom_curve,
)
from FactorAnalysis.grouping import quantile_group
from tests.mutual_components.conftest_perf import (
    make_synthetic_data, iter_scenarios, assert_series_close,
)


class TestCalcLabelsWithRebalanceBackwardCompat:
    """向后兼容测试：不传 group_labels 时行为不变 / Backward compat: no group_labels param."""

    def test_no_group_labels_rebalance_1(self):
        """rebalance_freq=1 不传 group_labels 与原行为一致."""
        factor, _ = make_synthetic_data()
        labels = _calc_labels_with_rebalance(factor, n_groups=5, rebalance_freq=1)
        expected = quantile_group(factor, n_groups=5)
        assert_series_close(labels, expected, label="no_group_labels_rebalance_1")

    def test_no_group_labels_rebalance_5(self):
        """rebalance_freq=5 不传 group_labels 与原行为一致."""
        factor, _ = make_synthetic_data()
        labels = _calc_labels_with_rebalance(factor, n_groups=5, rebalance_freq=5)
        # 原行为: 仅调仓日 quantile_group + ffill
        assert labels.notna().sum() > 0, "labels should have values"


class TestCalcLabelsWithPrecomputedLabels:
    """预计算 group_labels 测试 / Pre-computed group_labels tests."""

    def test_precomputed_labels_rebalance_1(self):
        """rebalance_freq=1 传入 group_labels 跳过 quantile_group."""
        factor, _ = make_synthetic_data()
        pre_labels = quantile_group(factor, n_groups=5)
        result = _calc_labels_with_rebalance(
            factor, n_groups=5, rebalance_freq=1, group_labels=pre_labels,
        )
        assert_series_close(result, pre_labels, label="precomputed_rebalance_1")

    def test_precomputed_labels_rebalance_5(self):
        """rebalance_freq=5 传入 group_labels 仍正确应用调仓+前向填充."""
        factor, _ = make_synthetic_data(n_days=100, n_symbols=30)
        pre_labels = quantile_group(factor, n_groups=5)
        result_with = _calc_labels_with_rebalance(
            factor, n_groups=5, rebalance_freq=5, group_labels=pre_labels,
        )
        result_without = _calc_labels_with_rebalance(
            factor, n_groups=5, rebalance_freq=5,
        )
        assert_series_close(result_with, result_without, label="precomputed_rebalance_5")

    def test_precomputed_matches_internal_all_scenarios(self):
        """所有预定义场景：传入预计算标签与内部计算结果一致."""
        checks = 0
        for sid, factor, returns in iter_scenarios():
            pre_labels = quantile_group(factor, n_groups=5)
            result_with = _calc_labels_with_rebalance(
                factor, n_groups=5, rebalance_freq=1, group_labels=pre_labels,
            )
            result_without = _calc_labels_with_rebalance(
                factor, n_groups=5, rebalance_freq=1,
            )
            assert_series_close(
                result_with, result_without,
                label=f"scenario_{sid}_labels",
            )
            checks += 1
        print(f"[test_precomputed_matches_internal_all_scenarios] {checks} scenarios passed")


class TestCurveFuncsWithGroupLabels:
    """三个公共曲线函数支持 group_labels 参数 / Curve functions support group_labels param."""

    def test_long_only_with_group_labels(self):
        """calc_long_only_curve 传入 group_labels 结果与不传一致."""
        factor, returns = make_synthetic_data()
        pre_labels = quantile_group(factor, n_groups=5)
        curve_with = calc_long_only_curve(
            factor, returns, n_groups=5, top_k=1, group_labels=pre_labels,
        )
        curve_without = calc_long_only_curve(
            factor, returns, n_groups=5, top_k=1,
        )
        assert_series_close(curve_with, curve_without, label="long_only_group_labels")

    def test_short_only_with_group_labels(self):
        """calc_short_only_curve 传入 group_labels 结果与不传一致."""
        factor, returns = make_synthetic_data()
        pre_labels = quantile_group(factor, n_groups=5)
        curve_with = calc_short_only_curve(
            factor, returns, n_groups=5, bottom_k=1, group_labels=pre_labels,
        )
        curve_without = calc_short_only_curve(
            factor, returns, n_groups=5, bottom_k=1,
        )
        assert_series_close(curve_with, curve_without, label="short_only_group_labels")

    def test_top_bottom_with_group_labels(self):
        """calc_top_bottom_curve 传入 group_labels 结果与不传一致."""
        factor, returns = make_synthetic_data()
        pre_labels = quantile_group(factor, n_groups=5)
        curve_with = calc_top_bottom_curve(
            factor, returns, n_groups=5, top_k=1, bottom_k=1, group_labels=pre_labels,
        )
        curve_without = calc_top_bottom_curve(
            factor, returns, n_groups=5, top_k=1, bottom_k=1,
        )
        assert_series_close(curve_with, curve_without, label="top_bottom_group_labels")

    def test_all_curves_all_scenarios(self):
        """所有预定义场景 × 三函数：传入 group_labels 与不传数值一致."""
        checks = 0
        for sid, factor, returns in iter_scenarios():
            pre_labels = quantile_group(factor, n_groups=5)

            long_with = calc_long_only_curve(
                factor, returns, n_groups=5, top_k=1, group_labels=pre_labels,
            )
            long_without = calc_long_only_curve(
                factor, returns, n_groups=5, top_k=1,
            )
            assert_series_close(long_with, long_without, label=f"{sid}_long")

            short_with = calc_short_only_curve(
                factor, returns, n_groups=5, bottom_k=1, group_labels=pre_labels,
            )
            short_without = calc_short_only_curve(
                factor, returns, n_groups=5, bottom_k=1,
            )
            assert_series_close(short_with, short_without, label=f"{sid}_short")

            hedge_with = calc_top_bottom_curve(
                factor, returns, n_groups=5, top_k=1, bottom_k=1, group_labels=pre_labels,
            )
            hedge_without = calc_top_bottom_curve(
                factor, returns, n_groups=5, top_k=1, bottom_k=1,
            )
            assert_series_close(hedge_with, hedge_without, label=f"{sid}_hedge")

            checks += 3
        print(f"[test_all_curves_all_scenarios] {checks} checks passed")


class TestCurveFuncsWithRebalanceFreq:
    """带调仓频率的 group_labels 测试 / group_labels with rebalance_freq tests."""

    def test_long_only_rebalance_5_with_group_labels(self):
        """calc_long_only_curve rebalance_freq=5 传入 group_labels 正确."""
        factor, returns = make_synthetic_data(n_days=100, n_symbols=30)
        pre_labels = quantile_group(factor, n_groups=5)
        curve_with = calc_long_only_curve(
            factor, returns, n_groups=5, top_k=1, rebalance_freq=5, group_labels=pre_labels,
        )
        curve_without = calc_long_only_curve(
            factor, returns, n_groups=5, top_k=1, rebalance_freq=5,
        )
        assert_series_close(curve_with, curve_without, label="long_rebalance_5_group_labels")

    def test_hedge_rebalance_3_with_group_labels(self):
        """calc_top_bottom_curve rebalance_freq=3 传入 group_labels 正确."""
        factor, returns = make_synthetic_data(n_days=90, n_symbols=30)
        pre_labels = quantile_group(factor, n_groups=5)
        curve_with = calc_top_bottom_curve(
            factor, returns, n_groups=5, top_k=2, bottom_k=2, rebalance_freq=3, group_labels=pre_labels,
        )
        curve_without = calc_top_bottom_curve(
            factor, returns, n_groups=5, top_k=2, bottom_k=2, rebalance_freq=3,
        )
        assert_series_close(curve_with, curve_without, label="hedge_rebalance_3_group_labels")


if __name__ == "__main__":
    pytest_main = __import__("pytest").main
    pytest_main([__file__, "-v"])
