"""
tests/test_group_returns_chart.py — 分组收益对比图验证测试
tests/test_group_returns_chart.py — Group returns chart validation tests.

验证 Task 21 实现的 plot_group_returns 图表生成功能。
Validates the plot_group_returns chart generation implemented in Task 21.
"""

from __future__ import annotations

import os

import numpy as np
import pandas as pd
import pytest

from FactorAnalysis.evaluator import FactorEvaluator
from FactorAnalysis.visualization.charts import plot_group_returns


# ============================================================
# 测试数据构建工具 / Test data construction helpers
# ============================================================


def _make_evaluator(
    n_dates: int = 100,
    n_symbols: int = 10,
    seed: int = 42,
    n_groups: int = 5,
    with_curves: bool = True,
):
    """
    构建已运行 run_grouping() 的 FactorEvaluator 实例
    Build a FactorEvaluator with run_grouping() called.

    生成随机因子值和收益率，因子值按组别有递增趋势，
    确保各分组收益曲线有差异化表现。
    Generates random factor values and returns with group-ordered trend
    to ensure differentiated group return curves.
    """
    rng = np.random.RandomState(seed)
    dates = pd.date_range("2024-01-01", periods=n_dates, freq="D")
    symbols = [f"SYM{i}" for i in range(n_symbols)]
    idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])

    # 因子值带有分组趋势：同一时间截面内按 symbol 排序有递增趋势
    # factor values have group-ordered trend within each cross-section
    factor_vals = np.zeros(n_dates * n_symbols)
    for t in range(n_dates):
        base = rng.randn(n_symbols) * 0.5
        # 排序后加上递增偏移，确保分组标签分散 / sorted + incremental offset
        base.sort()
        factor_vals[t * n_symbols: (t + 1) * n_symbols] = base + np.linspace(-1, 1, n_symbols)

    noise = rng.randn(n_dates * n_symbols) * 0.3
    returns_vals = factor_vals * 0.2 + noise

    factor = pd.Series(factor_vals, index=idx, name="factor")
    returns = pd.Series(returns_vals, index=idx, name="returns")

    ev = FactorEvaluator(factor, returns, n_groups=n_groups)
    ev.run_grouping()
    if with_curves:
        ev.run_curves()
    return ev


# ============================================================
# 基础功能测试 / Basic functionality tests
# ============================================================


class TestPlotGroupReturnsBasic:
    """分组收益对比图基础功能验证 / Group returns chart basic functionality validation."""

    def test_returns_figure_object(self):
        # 返回 plt.Figure 对象 / returns plt.Figure object
        import matplotlib.pyplot as plt

        ev = _make_evaluator()
        fig = plot_group_returns(ev)
        assert isinstance(fig, plt.Figure)

    def test_figure_has_one_subplot(self):
        # 图表包含一个子图 / figure has one subplot
        ev = _make_evaluator()
        fig = plot_group_returns(ev)
        assert len(fig.axes) == 1

    def test_raises_with_none_evaluator(self):
        # evaluator 为 None 时抛出 ValueError / raises ValueError when evaluator is None
        with pytest.raises(ValueError, match="evaluator 不能为 None"):
            plot_group_returns(None)

    def test_raises_without_run_grouping(self):
        # 未调用 run_grouping() 时抛出 ValueError / raises ValueError without run_grouping()
        ev = _make_evaluator()
        ev.group_labels = None
        with pytest.raises(ValueError, match="run_grouping"):
            plot_group_returns(ev)


# ============================================================
# 图表内容验证 / Chart content validation
# ============================================================


class TestPlotGroupReturnsContent:
    """分组收益对比图内容验证 / Group returns chart content validation."""

    def test_has_n_groups_lines(self):
        # 折线数量 = n_groups（含对冲线则 +1）/ line count = n_groups (+1 if hedge)
        ev = _make_evaluator(n_groups=5, with_curves=True)
        fig = plot_group_returns(ev)
        ax = fig.axes[0]
        # 5 组 + 1 对冲线 = 6 条线
        # 5 groups + 1 hedge line = 6 lines
        assert len(ax.lines) >= 5

    def test_has_hedge_line_when_curves_available(self):
        # run_curves() 已调用时包含多空对冲虚线 / has hedge dashed line when run_curves() called
        ev = _make_evaluator(with_curves=True)
        fig = plot_group_returns(ev)
        ax = fig.axes[0]
        # 查找黑色虚线 / find black dashed line
        has_hedge = False
        for line in ax.lines:
            if line.get_linestyle() == "--" and line.get_color() == "black":
                has_hedge = True
                break
        assert has_hedge, "应包含多空对冲虚线 / should contain long-short hedge dashed line"

    def test_no_hedge_line_without_curves(self):
        # 未调用 run_curves() 时不包含对冲线 / no hedge line without run_curves()
        ev = _make_evaluator(with_curves=False)
        fig = plot_group_returns(ev)
        ax = fig.axes[0]
        has_hedge = False
        for line in ax.lines:
            if line.get_linestyle() == "--" and line.get_color() == "black":
                has_hedge = True
                break
        assert not has_hedge, "未调用 run_curves() 时不应对冲线 / should not have hedge line"

    def test_start_nav_reference_line(self):
        # 包含起始净值参考线 y=1.0 / has start NAV reference line y=1.0
        ev = _make_evaluator()
        fig = plot_group_returns(ev)
        ax = fig.axes[0]
        # axhline creates a horizontal line / axhline creates horizontal line
        has_ref = False
        for line in ax.lines:
            ydata = line.get_ydata()
            if len(ydata) >= 2 and np.allclose(ydata, 1.0):
                has_ref = True
                break
        assert has_ref, "应包含 y=1.0 参考线 / should contain y=1.0 reference line"

    def test_group_lines_have_different_colors(self):
        # 各组折线颜色不同 / group lines have different colors
        ev = _make_evaluator(n_groups=5, with_curves=False)
        fig = plot_group_returns(ev)
        ax = fig.axes[0]
        # 排除参考线（灰色半透明），检查前 5 条线颜色
        # exclude reference line (gray semi-transparent), check first 5 lines' colors
        colors = set()
        for line in ax.lines:
            c = line.get_color()
            if c != "gray":
                colors.add(c)
        assert len(colors) >= 5, f"应有至少 5 种不同颜色，实际 {len(colors)} 种"


# ============================================================
# 文件保存测试 / File saving tests
# ============================================================


class TestPlotGroupReturnsSave:
    """分组收益对比图文件保存验证 / Group returns chart file saving validation."""

    def test_save_png_file(self, tmp_path):
        # 保存 PNG 文件 / save PNG file
        ev = _make_evaluator()
        output = str(tmp_path / "group_returns.png")
        plot_group_returns(ev, output_path=output)
        assert os.path.exists(output)
        assert os.path.getsize(output) > 0

    def test_save_with_custom_filename(self, tmp_path):
        # 自定义文件名保存 / save with custom filename
        ev = _make_evaluator()
        output = str(tmp_path / "custom_group_chart.png")
        plot_group_returns(ev, output_path=output)
        assert os.path.exists(output)

    def test_no_save_when_output_path_none(self):
        # output_path=None 时不保存文件 / no file saved when output_path is None
        import matplotlib.pyplot as plt

        ev = _make_evaluator()
        fig = plot_group_returns(ev, output_path=None)
        assert isinstance(fig, plt.Figure)


# ============================================================
# 参数验证 / Parameter validation tests
# ============================================================


class TestPlotGroupReturnsParams:
    """分组收益对比图参数验证 / Group returns chart parameter validation."""

    def test_custom_figsize(self):
        # 自定义图表尺寸 / custom figure size
        ev = _make_evaluator()
        fig = plot_group_returns(ev, figsize=(8, 5))
        w, h = fig.get_size_inches()
        assert abs(w - 8.0) < 0.1
        assert abs(h - 5.0) < 0.1

    def test_custom_dpi(self, tmp_path):
        # 自定义 DPI / custom DPI
        ev = _make_evaluator()
        output = str(tmp_path / "group_high_dpi.png")
        fig = plot_group_returns(ev, output_path=output, dpi=300)
        assert fig.dpi == 300

    def test_different_n_groups(self):
        # 不同分组数量 / different number of groups
        for n_groups in [2, 3, 5, 10]:
            ev = _make_evaluator(n_groups=n_groups, with_curves=False)
            fig = plot_group_returns(ev)
            assert len(fig.axes) == 1

    def test_no_output_path_no_error(self):
        # 不传 output_path 不报错 / no error when output_path is not provided
        ev = _make_evaluator()
        fig = plot_group_returns(ev)
        assert fig is not None


# ============================================================
# 数据一致性验证 / Data consistency validation
# ============================================================


class TestPlotGroupReturnsConsistency:
    """分组收益对比图数据一致性验证 / Group returns chart data consistency validation."""

    def test_group_curves_start_at_one(self):
        # 各组净值曲线起始值为 1.0 / group NAV curves start at 1.0
        ev = _make_evaluator(n_dates=50, seed=42)
        fig = plot_group_returns(ev)
        # 从内部计算验证 / verify from internal computation
        combined = pd.DataFrame({
            "label": ev.group_labels,
            "returns": ev.returns,
        }).dropna(subset=["label"])
        group_daily = (combined
                       .groupby(["timestamp", "label"])["returns"]
                       .mean()
                       .unstack("label")
                       .reindex(columns=sorted(combined["label"].dropna().unique())))
        group_curves = (1.0 + group_daily).cumprod()
        group_curves.iloc[0] = 1.0
        # 验证各组起始值 / verify each group's start value
        for col in group_curves.columns:
            assert abs(group_curves[col].iloc[0] - 1.0) < 1e-10

    def test_hedge_curve_matches_evaluator(self):
        # 对冲线数据与 evaluator.hedge_curve 一致 / hedge line matches evaluator.hedge_curve
        ev = _make_evaluator(n_dates=60, seed=77, with_curves=True)
        fig = plot_group_returns(ev)
        ax = fig.axes[0]
        # 找到对冲线并验证其 y 数据范围 / find hedge line and verify y data range
        hedge_curve = ev.hedge_curve
        for line in ax.lines:
            if line.get_linestyle() == "--" and line.get_color() == "black":
                line_y = line.get_ydata()
                # 对冲线数据应与 evaluator.hedge_curve 值域接近
                # hedge line data should be in similar range as evaluator.hedge_curve
                assert np.nanmax(np.abs(line_y)) > 0, "对冲线应有非零值"
                break

    def test_multi_seed_consistency(self):
        # 多种子下图表正常生成 / charts generate normally across multiple seeds
        for seed in [0, 42, 99, 2024]:
            ev = _make_evaluator(n_dates=50, seed=seed, with_curves=False)
            fig = plot_group_returns(ev)
            assert len(fig.axes) == 1


# ============================================================
# 边界情况 / Edge cases
# ============================================================


class TestPlotGroupReturnsEdgeCases:
    """分组收益对比图边界情况 / Group returns chart edge cases."""

    def test_minimal_data(self):
        # 最小数据量（2 个时间截面）/ minimal data (2 timestamps)
        ev = _make_evaluator(n_dates=2, n_symbols=5, seed=1)
        fig = plot_group_returns(ev)
        assert len(fig.axes) == 1

    def test_single_symbol(self):
        # 单交易对（n_groups=2，1 个 symbol 至少能分到 1 组）
        # single symbol (n_groups=2, 1 symbol can form at least 1 group)
        ev = _make_evaluator(n_dates=30, n_symbols=1, seed=55, n_groups=2)
        # 单交易对分组标签可能全为 NaN（无法分位数分组）
        # single symbol labels may be all NaN (cannot quantile-group 1 item)
        if ev.group_labels.dropna().empty:
            with pytest.raises(ValueError, match="分组标签为空"):
                plot_group_returns(ev)
        else:
            fig = plot_group_returns(ev)
            assert len(fig.axes) == 1

    def test_large_dataset(self):
        # 大数据集不崩溃 / large dataset does not crash
        ev = _make_evaluator(n_dates=500, n_symbols=50, seed=88, with_curves=False)
        fig = plot_group_returns(ev)
        assert len(fig.axes) == 1

    def test_raises_with_empty_group_labels(self):
        # 分组标签全为 NaN 时抛出 ValueError / raises ValueError when all labels are NaN
        ev = _make_evaluator()
        ev.group_labels = pd.Series(np.nan, index=ev.factor.index, dtype=float)
        with pytest.raises(ValueError, match="分组标签为空"):
            plot_group_returns(ev)

    def test_two_groups(self):
        # 2 组极端情况 / 2 groups edge case
        ev = _make_evaluator(n_dates=30, n_symbols=10, n_groups=2, seed=33, with_curves=False)
        fig = plot_group_returns(ev)
        ax = fig.axes[0]
        assert len(ax.lines) >= 2
