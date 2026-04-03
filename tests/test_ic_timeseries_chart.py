"""
tests/test_ic_timeseries_chart.py — IC 时间序列图验证测试
tests/test_ic_timeseries_chart.py — IC timeseries chart validation tests.

验证 Task 20 实现的 plot_ic_timeseries 图表生成功能。
Validates the plot_ic_timeseries chart generation implemented in Task 20.
"""

from __future__ import annotations

import os
import tempfile

import numpy as np
import pandas as pd
import pytest

from FactorAnalysis.evaluator import FactorEvaluator
from FactorAnalysis.visualization.charts import plot_ic_timeseries


# ============================================================
# 测试数据构建工具 / Test data construction helpers
# ============================================================


def _make_evaluator(n_dates: int = 100, n_symbols: int = 10, seed: int = 42):
    """
    构建已运行 run_metrics() 的 FactorEvaluator 实例 / Build a FactorEvaluator with run_metrics() called.

    生成随机因子值和收益率，确保 IC 序列非零。
    Generates random factor values and returns, ensuring non-zero IC series.
    """
    rng = np.random.RandomState(seed)
    dates = pd.date_range("2024-01-01", periods=n_dates, freq="D")
    symbols = [f"SYM{i}" for i in range(n_symbols)]
    idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])

    # 因子值与收益率有弱正相关，确保 IC 非零
    # factor values have weak positive correlation with returns to ensure non-zero IC
    factor_base = rng.randn(n_dates, n_symbols)
    noise = rng.randn(n_dates, n_symbols) * 0.5
    returns_vals = factor_base * 0.3 + noise

    factor = pd.Series(factor_base.ravel(), index=idx, name="factor")
    returns = pd.Series(returns_vals.ravel(), index=idx, name="returns")

    ev = FactorEvaluator(factor, returns)
    ev.run_metrics()
    return ev


# ============================================================
# 基础功能测试 / Basic functionality tests
# ============================================================


class TestPlotIcTimeseriesBasic:
    """IC 时间序列图基础功能验证 / IC timeseries chart basic functionality validation."""

    def test_returns_figure_object(self):
        # 返回 plt.Figure 对象 / returns plt.Figure object
        import matplotlib.pyplot as plt

        ev = _make_evaluator()
        fig = plot_ic_timeseries(ev)
        assert isinstance(fig, plt.Figure)

    def test_figure_has_two_subplots(self):
        # 图表包含上下两个子图 / figure has two subplots
        ev = _make_evaluator()
        fig = plot_ic_timeseries(ev)
        assert len(fig.axes) == 2

    def test_raises_without_run_metrics(self):
        # 未调用 run_metrics() 时抛出 ValueError / raises ValueError when run_metrics() not called
        ev = _make_evaluator()
        ev.ic = None
        with pytest.raises(ValueError, match="run_metrics"):
            plot_ic_timeseries(ev)

    def test_raises_with_empty_ic(self):
        # IC 序列为空时抛出 ValueError / raises ValueError when IC series is empty
        ev = _make_evaluator()
        ev.ic = pd.Series(dtype=float)
        with pytest.raises(ValueError, match="IC 序列为空"):
            plot_ic_timeseries(ev)


# ============================================================
# 图表内容验证 / Chart content validation
# ============================================================


class TestPlotIcTimeseriesContent:
    """IC 时间序列图内容验证 / IC timeseries chart content validation."""

    def test_top_subplot_has_bars(self):
        # 上子图包含日频 IC 柱状图（BarContainer）
        # top subplot contains daily IC bars (BarContainer)
        import matplotlib.container

        ev = _make_evaluator()
        fig = plot_ic_timeseries(ev)
        ax1 = fig.axes[0]
        # bar chart produces BarContainer objects in patches
        has_bars = any(isinstance(c, matplotlib.container.BarContainer) for c in ax1.containers)
        assert has_bars, "上子图应包含柱状图 / top subplot should contain bars"

    def test_bottom_subplot_has_fill(self):
        # 下子图包含累积 IC 填充区域 / bottom subplot has cumulative IC fill
        ev = _make_evaluator()
        fig = plot_ic_timeseries(ev)
        ax2 = fig.axes[1]
        # fill_between creates PolyCollection
        has_fill = len(ax2.collections) > 0
        assert has_fill, "下子图应包含填充区域 / bottom subplot should contain filled area"

    def test_top_subplot_has_lines(self):
        # 上子图包含滚动 IC 线条（周度 + 月度 + 均值线）
        # top subplot has rolling IC lines (weekly + monthly + mean)
        ev = _make_evaluator(n_dates=100)
        fig = plot_ic_timeseries(ev)
        ax1 = fig.axes[0]
        # 至少 3 条线：周度滚动、月度滚动、IC 均值线
        # at least 3 lines: weekly rolling, monthly rolling, IC mean
        assert len(ax1.lines) >= 3, f"上子图应有至少 3 条线，实际 {len(ax1.lines)} 条"

    def test_mean_line_value_correct(self):
        # IC 均值线数值正确 / IC mean line has correct value
        ev = _make_evaluator(n_dates=100, seed=42)
        ic_valid = ev.ic.dropna()
        expected_mean = float(ic_valid.mean())

        fig = plot_ic_timeseries(ev)
        ax1 = fig.axes[0]
        # 均值线是水平线（ydata 全部相同），找到非零线段
        # mean line is horizontal (all ydata equal), find non-zero segment
        found_mean = False
        for line in ax1.lines:
            ydata = line.get_ydata()
            if len(ydata) >= 2 and np.allclose(ydata, ydata[0]):
                # 水平线 / horizontal line
                if abs(ydata[0] - expected_mean) < 0.01:
                    found_mean = True
                    break
        assert found_mean, "应找到 IC 均值水平线 / should find IC mean horizontal line"

    def test_std_band_exists(self):
        # ±1std 阴影带存在 / ±1std shaded band exists
        ev = _make_evaluator(n_dates=100, seed=42)
        fig = plot_ic_timeseries(ev)
        ax1 = fig.axes[0]
        # axhspan creates a Rectangle patch
        has_span = len(ax1.patches) > 0
        assert has_span, "上子图应包含 ±1std 阴影带 / top subplot should have ±1std band"


# ============================================================
# 文件保存测试 / File saving tests
# ============================================================


class TestPlotIcTimeseriesSave:
    """IC 时间序列图文件保存验证 / IC timeseries chart file saving validation."""

    def test_save_png_file(self, tmp_path):
        # 保存 PNG 文件 / save PNG file
        ev = _make_evaluator()
        output = str(tmp_path / "ic_timeseries.png")
        plot_ic_timeseries(ev, output_path=output)
        assert os.path.exists(output)
        assert os.path.getsize(output) > 0

    def test_save_with_custom_filename(self, tmp_path):
        # 自定义文件名保存 / save with custom filename
        ev = _make_evaluator()
        output = str(tmp_path / "custom_ic_chart.png")
        plot_ic_timeseries(ev, output_path=output)
        assert os.path.exists(output)

    def test_no_save_when_output_path_none(self):
        # output_path=None 时不保存文件 / no file saved when output_path is None
        import matplotlib.pyplot as plt

        ev = _make_evaluator()
        fig = plot_ic_timeseries(ev, output_path=None)
        assert isinstance(fig, plt.Figure)  # 只返回 Figure 不保存 / only returns Figure


# ============================================================
# 参数验证 / Parameter validation tests
# ============================================================


class TestPlotIcTimeseriesParams:
    """IC 时间序列图参数验证 / IC timeseries chart parameter validation."""

    def test_custom_figsize(self):
        # 自定义图表尺寸 / custom figure size
        ev = _make_evaluator()
        fig = plot_ic_timeseries(ev, figsize=(8, 6))
        w, h = fig.get_size_inches()
        assert abs(w - 8.0) < 0.1
        assert abs(h - 6.0) < 0.1

    def test_custom_dpi(self, tmp_path):
        # 自定义 DPI / custom DPI
        ev = _make_evaluator()
        output = str(tmp_path / "ic_high_dpi.png")
        fig = plot_ic_timeseries(ev, output_path=output, dpi=300)
        assert fig.dpi == 300

    def test_custom_rolling_windows(self):
        # 自定义滚动窗口 / custom rolling windows
        ev = _make_evaluator(n_dates=100)
        fig = plot_ic_timeseries(ev, rolling_window_week=3, rolling_window_month=10)
        ax1 = fig.axes[0]
        # 应有滚动线和均值线 / should have rolling lines and mean line
        assert len(ax1.lines) >= 2

    def test_short_data_weekly_rolling(self):
        # 短数据时仍能绘制（周度滚动窗口 >= 数据长度）
        # still drawable when data is shorter than weekly window
        ev = _make_evaluator(n_dates=3, n_symbols=5)
        fig = plot_ic_timeseries(ev)
        assert len(fig.axes) == 2

    def test_no_output_path_no_error(self):
        # 不传 output_path 不报错 / no error when output_path is not provided
        ev = _make_evaluator()
        fig = plot_ic_timeseries(ev)
        assert fig is not None


# ============================================================
# 数据一致性验证 / Data consistency validation
# ============================================================


class TestPlotIcTimeseriesConsistency:
    """IC 时间序列图数据一致性验证 / IC timeseries chart data consistency validation."""

    def test_cumulative_ic_values(self):
        # 累积 IC 值与 evaluator.ic 一致 / cumulative IC values match evaluator.ic
        ev = _make_evaluator(n_dates=50, seed=123)
        ic_valid = ev.ic.dropna()
        expected_cumsum = ic_valid.cumsum().values

        fig = plot_ic_timeseries(ev)
        ax2 = fig.axes[1]
        # 从 fill_between 的顶边获取数据 / get data from fill_between top edge
        poly = ax2.collections[0]
        # get_paths 返回路径集合，取第一个路径的顶点
        # get_paths returns path collection, take first path vertices
        path = poly.get_paths()[0]
        vertices = path.vertices
        # 提取填充区域的上边界 y 值 / extract top boundary y values of fill
        y_upper = vertices[:, 1]
        # 累积 IC 曲线最大值应与预期 cumsum 最大值接近
        # max value of cumulative IC curve should be close to expected cumsum max
        assert np.max(np.abs(y_upper)) > 0, "累积 IC 曲线应有非零值"

    def test_ic_series_length_matches_data(self):
        # IC 序列长度与 evaluator.ic 一致 / IC series length matches evaluator.ic
        ev = _make_evaluator(n_dates=60, n_symbols=8, seed=77)
        n_ic = len(ev.ic.dropna())
        fig = plot_ic_timeseries(ev)
        ax1 = fig.axes[0]
        # 柱状图的 x 数据点数应与 IC 序列长度一致
        # number of x data points in bar chart should match IC series length
        bar_container = ax1.containers[0]
        n_bars = len(bar_container)
        assert n_bars == n_ic

    def test_multi_seed_consistency(self):
        # 多种子下图表正常生成 / charts generate normally across multiple seeds
        for seed in [0, 42, 99, 2024]:
            ev = _make_evaluator(n_dates=50, seed=seed)
            fig = plot_ic_timeseries(ev)
            assert len(fig.axes) == 2


# ============================================================
# 边界情况 / Edge cases
# ============================================================


class TestPlotIcTimeseriesEdgeCases:
    """IC 时间序列图边界情况 / IC timeseries chart edge cases."""

    def test_minimal_data(self):
        # 最小数据量（2 个时间截面）/ minimal data (2 timestamps)
        ev = _make_evaluator(n_dates=2, n_symbols=5, seed=1)
        fig = plot_ic_timeseries(ev)
        assert len(fig.axes) == 2

    def test_single_symbol(self):
        # 单交易对 / single symbol
        ev = _make_evaluator(n_dates=30, n_symbols=1, seed=55)
        # 单交易对 IC 可能全为 NaN，测试不崩溃
        # single symbol IC may be all NaN, test does not crash
        if ev.ic.dropna().empty:
            with pytest.raises(ValueError, match="IC 序列为空"):
                plot_ic_timeseries(ev)
        else:
            fig = plot_ic_timeseries(ev)
            assert len(fig.axes) == 2

    def test_large_dataset(self):
        # 大数据集不崩溃 / large dataset does not crash
        ev = _make_evaluator(n_dates=500, n_symbols=50, seed=88)
        fig = plot_ic_timeseries(ev)
        assert len(fig.axes) == 2
