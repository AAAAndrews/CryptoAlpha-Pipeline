"""
tests/test_portfolio_curves_chart.py — 组合净值曲线图验证测试
tests/test_portfolio_curves_chart.py — Portfolio equity curves chart validation tests.

验证 Task 22 实现的 plot_portfolio_curves 图表生成功能。
Validates the plot_portfolio_curves chart generation implemented in Task 22.
"""

from __future__ import annotations

import os

import numpy as np
import pandas as pd
import pytest

from FactorAnalysis.evaluator import FactorEvaluator
from FactorAnalysis.visualization.charts import plot_portfolio_curves


# ============================================================
# 测试数据构建工具 / Test data construction helpers
# ============================================================


def _make_evaluator(
    n_dates: int = 100,
    n_symbols: int = 10,
    seed: int = 42,
    n_groups: int = 5,
    cost_rate: float = 0.001,
):
    """
    构建已运行 run_curves() 的 FactorEvaluator 实例
    Build a FactorEvaluator with run_curves() called.

    生成随机因子值和收益率，因子值按组别有递增趋势，
    确保多/空/对冲净值曲线有差异化表现。
    Generates random factor values and returns with group-ordered trend
    to ensure differentiated long/short/hedge curves.
    """
    rng = np.random.RandomState(seed)
    dates = pd.date_range("2024-01-01", periods=n_dates, freq="D")
    symbols = [f"SYM{i}" for i in range(n_symbols)]
    idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])

    # 因子值带有分组趋势 / factor values have group-ordered trend
    factor_vals = np.zeros(n_dates * n_symbols)
    for t in range(n_dates):
        base = rng.randn(n_symbols) * 0.5
        base.sort()
        factor_vals[t * n_symbols: (t + 1) * n_symbols] = base + np.linspace(-1, 1, n_symbols)

    noise = rng.randn(n_dates * n_symbols) * 0.3
    returns_vals = factor_vals * 0.2 + noise

    factor = pd.Series(factor_vals, index=idx, name="factor")
    returns = pd.Series(returns_vals, index=idx, name="returns")

    ev = FactorEvaluator(factor, returns, n_groups=n_groups, cost_rate=cost_rate)
    ev.run_curves()
    return ev


# ============================================================
# 基础功能测试 / Basic functionality tests
# ============================================================


class TestPlotPortfolioCurvesBasic:
    """组合净值曲线图基础功能验证 / Portfolio curves chart basic functionality validation."""

    def test_returns_figure_object(self):
        # 返回 plt.Figure 对象 / returns plt.Figure object
        import matplotlib.pyplot as plt

        ev = _make_evaluator()
        fig = plot_portfolio_curves(ev)
        assert isinstance(fig, plt.Figure)

    def test_figure_has_two_subplots(self):
        # 图表包含两个子图（含/不含手续费）/ figure has two subplots
        ev = _make_evaluator()
        fig = plot_portfolio_curves(ev)
        assert len(fig.axes) == 2

    def test_raises_with_none_evaluator(self):
        # evaluator 为 None 时抛出 ValueError / raises ValueError when evaluator is None
        with pytest.raises(ValueError, match="evaluator 不能为 None"):
            plot_portfolio_curves(None)

    def test_raises_without_run_curves(self):
        # 未调用 run_curves() 时抛出 ValueError / raises ValueError without run_curves()
        ev = _make_evaluator()
        ev.long_curve = None
        ev.short_curve = None
        ev.hedge_curve = None
        with pytest.raises(ValueError, match="run_curves"):
            plot_portfolio_curves(ev)


# ============================================================
# 图表内容验证 / Chart content validation
# ============================================================


class TestPlotPortfolioCurvesContent:
    """组合净值曲线图内容验证 / Portfolio curves chart content validation."""

    def test_top_subplot_has_three_lines(self):
        # 上子图含 3 条曲线（long/short/hedge）/ top subplot has 3 lines
        ev = _make_evaluator()
        fig = plot_portfolio_curves(ev)
        ax1 = fig.axes[0]
        assert len(ax1.lines) >= 3

    def test_bottom_subplot_has_three_lines(self):
        # 下子图含 3 条曲线（long/short/hedge after cost）/ bottom subplot has 3 lines
        ev = _make_evaluator()
        fig = plot_portfolio_curves(ev)
        ax2 = fig.axes[1]
        assert len(ax2.lines) >= 3

    def test_top_subplot_before_cost_title(self):
        # 上子图标题包含"不含手续费" / top subplot title contains "before cost"
        ev = _make_evaluator()
        fig = plot_portfolio_curves(ev)
        ax1 = fig.axes[0]
        assert "不含手续费" in ax1.get_title() or "Before Cost" in ax1.get_title()

    def test_bottom_subplot_after_cost_title(self):
        # 下子图标题包含"含手续费" / bottom subplot title contains "after cost"
        ev = _make_evaluator()
        fig = plot_portfolio_curves(ev)
        ax2 = fig.axes[1]
        assert "含手续费" in ax2.get_title() or "After Cost" in ax2.get_title()

    def test_reference_line_y_equals_one(self):
        # 两个子图均包含 y=1.0 参考线 / both subplots have y=1.0 reference line
        ev = _make_evaluator()
        fig = plot_portfolio_curves(ev)
        for ax in fig.axes:
            has_ref = False
            for line in ax.lines:
                ydata = line.get_ydata()
                if len(ydata) >= 2 and np.allclose(ydata, 1.0):
                    has_ref = True
                    break
            assert has_ref, "应包含 y=1.0 参考线 / should contain y=1.0 reference line"

    def test_long_short_hedge_have_different_colors(self):
        # long/short/hedge 使用不同颜色 / long/short/hedge use different colors
        ev = _make_evaluator()
        fig = plot_portfolio_curves(ev)
        ax1 = fig.axes[0]
        # 排除灰色参考线，检查其余线的颜色 / exclude gray reference, check other lines
        colors = set()
        for line in ax1.lines:
            c = line.get_color()
            if c not in ("gray",):
                colors.add(c)
        assert len(colors) >= 3, f"应有至少 3 种不同颜色，实际 {len(colors)} 种"


# ============================================================
# 文件保存测试 / File saving tests
# ============================================================


class TestPlotPortfolioCurvesSave:
    """组合净值曲线图文件保存验证 / Portfolio curves chart file saving validation."""

    def test_save_png_file(self, tmp_path):
        # 保存 PNG 文件 / save PNG file
        ev = _make_evaluator()
        output = str(tmp_path / "portfolio_curves.png")
        plot_portfolio_curves(ev, output_path=output)
        assert os.path.exists(output)
        assert os.path.getsize(output) > 0

    def test_save_with_custom_filename(self, tmp_path):
        # 自定义文件名保存 / save with custom filename
        ev = _make_evaluator()
        output = str(tmp_path / "custom_portfolio_chart.png")
        plot_portfolio_curves(ev, output_path=output)
        assert os.path.exists(output)

    def test_no_save_when_output_path_none(self):
        # output_path=None 时不保存文件 / no file saved when output_path is None
        import matplotlib.pyplot as plt

        ev = _make_evaluator()
        fig = plot_portfolio_curves(ev, output_path=None)
        assert isinstance(fig, plt.Figure)


# ============================================================
# 参数验证 / Parameter validation tests
# ============================================================


class TestPlotPortfolioCurvesParams:
    """组合净值曲线图参数验证 / Portfolio curves chart parameter validation."""

    def test_custom_figsize(self):
        # 自定义图表尺寸 / custom figure size
        ev = _make_evaluator()
        fig = plot_portfolio_curves(ev, figsize=(10, 8))
        w, h = fig.get_size_inches()
        assert abs(w - 10.0) < 0.1
        assert abs(h - 8.0) < 0.1

    def test_custom_dpi(self, tmp_path):
        # 自定义 DPI / custom DPI
        ev = _make_evaluator()
        output = str(tmp_path / "portfolio_high_dpi.png")
        fig = plot_portfolio_curves(ev, output_path=output, dpi=300)
        assert fig.dpi == 300

    def test_different_cost_rates(self):
        # 不同成本率下图表正常生成 / charts generate normally with different cost rates
        for cost in [0.0, 0.001, 0.005, 0.01]:
            ev = _make_evaluator(cost_rate=cost)
            fig = plot_portfolio_curves(ev)
            assert len(fig.axes) == 2

    def test_no_output_path_no_error(self):
        # 不传 output_path 不报错 / no error when output_path is not provided
        ev = _make_evaluator()
        fig = plot_portfolio_curves(ev)
        assert fig is not None


# ============================================================
# 数据一致性验证 / Data consistency validation
# ============================================================


class TestPlotPortfolioCurvesConsistency:
    """组合净值曲线图数据一致性验证 / Portfolio curves chart data consistency validation."""

    def test_curves_start_at_one(self):
        # 净值曲线起始值为 1.0 / equity curves start at 1.0
        ev = _make_evaluator(n_dates=50, seed=42)
        assert abs(ev.long_curve.iloc[0] - 1.0) < 1e-10
        assert abs(ev.short_curve.iloc[0] - 1.0) < 1e-10
        assert abs(ev.hedge_curve.iloc[0] - 1.0) < 1e-10

    def test_hedge_after_cost_lower_than_hedge(self):
        # 含手续费对冲净值 ≤ 不含手续费对冲净值（成本扣除方向正确）
        # hedge after cost ≤ hedge before cost (cost deduction direction is correct)
        ev = _make_evaluator(n_dates=100, seed=42, cost_rate=0.005)
        # 对冲净值扣除成本后应 ≤ 不含成本的对冲净值
        # hedge after cost should be ≤ hedge before cost at each point
        diff = ev.hedge_curve_after_cost - ev.hedge_curve
        # 允许浮点误差 / allow floating point tolerance
        assert np.all(diff <= 1e-10), "含手续费对冲净值应 ≤ 不含手续费 / after cost ≤ before cost"

    def test_multi_seed_consistency(self):
        # 多种子下图表正常生成 / charts generate normally across multiple seeds
        for seed in [0, 42, 99, 2024]:
            ev = _make_evaluator(n_dates=50, seed=seed)
            fig = plot_portfolio_curves(ev)
            assert len(fig.axes) == 2


# ============================================================
# 边界情况 / Edge cases
# ============================================================


class TestPlotPortfolioCurvesEdgeCases:
    """组合净值曲线图边界情况 / Portfolio curves chart edge cases."""

    def test_minimal_data(self):
        # 最小数据量（2 个时间截面）/ minimal data (2 timestamps)
        ev = _make_evaluator(n_dates=2, n_symbols=5, seed=1)
        fig = plot_portfolio_curves(ev)
        assert len(fig.axes) == 2

    def test_large_dataset(self):
        # 大数据集不崩溃 / large dataset does not crash
        ev = _make_evaluator(n_dates=500, n_symbols=50, seed=88)
        fig = plot_portfolio_curves(ev)
        assert len(fig.axes) == 2

    def test_zero_cost_rate(self):
        # 零成本率：含/不含手续费对冲曲线一致 / zero cost: curves identical
        ev = _make_evaluator(n_dates=50, seed=33, cost_rate=0.0)
        fig = plot_portfolio_curves(ev)
        # 零成本时 hedge_curve_after_cost 应与 hedge_curve 几乎一致
        # with zero cost, after_cost should match before_cost
        diff = np.abs(ev.hedge_curve_after_cost.values - ev.hedge_curve.values)
        assert np.all(diff < 1e-10), "零成本时含/不含手续费曲线应一致"

    def test_high_cost_rate(self):
        # 高成本率图表正常生成 / high cost rate chart generates normally
        ev = _make_evaluator(n_dates=50, seed=77, cost_rate=0.05)
        fig = plot_portfolio_curves(ev)
        assert len(fig.axes) == 2
        # 高成本下含手续费对冲净值应显著低于不含手续费
        # high cost: after cost should be notably lower
        diff = ev.hedge_curve - ev.hedge_curve_after_cost
        assert np.mean(diff) > 0, "高成本下含手续费应低于不含手续费"

    def test_different_n_groups(self):
        # 不同分组数量下图表正常 / charts work with different n_groups
        for n_groups in [2, 3, 5, 10]:
            ev = _make_evaluator(n_dates=30, n_symbols=10, seed=55, n_groups=n_groups)
            fig = plot_portfolio_curves(ev)
            assert len(fig.axes) == 2
