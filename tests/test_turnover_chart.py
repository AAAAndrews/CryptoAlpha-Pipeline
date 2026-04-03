"""
tests/test_turnover_chart.py — 换手率分布图验证测试
tests/test_turnover_chart.py — Turnover distribution chart validation tests.

验证 Task 23 实现的 plot_turnover 图表生成功能。
Validates the plot_turnover chart generation implemented in Task 23.
"""

from __future__ import annotations

import os

import numpy as np
import pandas as pd
import pytest

from FactorAnalysis.evaluator import FactorEvaluator
from FactorAnalysis.visualization.charts import plot_turnover


# ============================================================
# 测试数据构建工具 / Test data construction helpers
# ============================================================


def _make_evaluator(
    n_dates: int = 100,
    n_symbols: int = 10,
    seed: int = 42,
    n_groups: int = 5,
):
    """
    构建已运行 run_turnover() 的 FactorEvaluator 实例
    Build a FactorEvaluator with run_turnover() called.

    生成随机因子值和收益率，因子值带有分组趋势以产生有意义的换手率。
    Generates random factor values and returns with group-ordered trend
    to produce meaningful turnover.
    """
    rng = np.random.RandomState(seed)
    dates = pd.date_range("2024-01-01", periods=n_dates, freq="D")
    symbols = [f"SYM{i}" for i in range(n_symbols)]
    idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])

    # 因子值带有分组趋势 + 时间维度噪声 / factor with group trend + temporal noise
    factor_vals = np.zeros(n_dates * n_symbols)
    for t in range(n_dates):
        base = rng.randn(n_symbols) * 0.5
        base.sort()
        factor_vals[t * n_symbols: (t + 1) * n_symbols] = base + np.linspace(-1, 1, n_symbols)

    noise = rng.randn(n_dates * n_symbols) * 0.3
    returns_vals = factor_vals * 0.2 + noise

    factor = pd.Series(factor_vals, index=idx, name="factor")
    returns = pd.Series(returns_vals, index=idx, name="returns")

    ev = FactorEvaluator(factor, returns, n_groups=n_groups)
    ev.run_turnover()
    return ev


# ============================================================
# 基础功能测试 / Basic functionality tests
# ============================================================


class TestPlotTurnoverBasic:
    """换手率分布图基础功能验证 / Turnover chart basic functionality validation."""

    def test_returns_figure_object(self):
        # 返回 plt.Figure 对象 / returns plt.Figure object
        import matplotlib.pyplot as plt

        ev = _make_evaluator()
        fig = plot_turnover(ev)
        assert isinstance(fig, plt.Figure)

    def test_figure_has_two_subplots(self):
        # 图表包含两个子图（换手率 + 排名自相关）/ figure has two subplots
        ev = _make_evaluator()
        fig = plot_turnover(ev)
        assert len(fig.axes) == 2

    def test_raises_with_none_evaluator(self):
        # evaluator 为 None 时抛出 ValueError / raises ValueError when evaluator is None
        with pytest.raises(ValueError, match="evaluator 不能为 None"):
            plot_turnover(None)

    def test_raises_without_run_turnover(self):
        # 未调用 run_turnover() 时抛出 ValueError / raises ValueError without run_turnover()
        ev = _make_evaluator()
        ev.turnover = None
        with pytest.raises(ValueError, match="run_turnover"):
            plot_turnover(ev)


# ============================================================
# 图表内容验证 / Chart content validation
# ============================================================


class TestPlotTurnoverContent:
    """换手率分布图内容验证 / Turnover chart content validation."""

    def test_top_subplot_has_area(self):
        # 上子图含堆叠面积图（PolyCollection）/ top subplot has stacked area (PolyCollection)
        from matplotlib.collections import PolyCollection

        ev = _make_evaluator()
        fig = plot_turnover(ev)
        ax1 = fig.axes[0]
        # stackplot 生成 PolyCollection / stackplot produces PolyCollection
        has_poly = any(isinstance(c, PolyCollection) for c in ax1.collections)
        assert has_poly, "上子图应包含面积图 / top subplot should contain area chart"

    def test_top_subplot_title(self):
        # 上子图标题包含"换手率" / top subplot title contains "turnover"
        ev = _make_evaluator()
        fig = plot_turnover(ev)
        ax1 = fig.axes[0]
        assert "换手率" in ax1.get_title() or "Turnover" in ax1.get_title()

    def test_bottom_subplot_title(self):
        # 下子图标题包含"自相关" / bottom subplot title contains "autocorrelation"
        ev = _make_evaluator()
        fig = plot_turnover(ev)
        ax2 = fig.axes[1]
        assert "自相关" in ax2.get_title() or "Autocorr" in ax2.get_title()

    def test_top_subplot_mean_line(self):
        # 上子图含均值参考线（虚线）/ top subplot has mean reference line (dashed)
        ev = _make_evaluator()
        fig = plot_turnover(ev)
        ax1 = fig.axes[0]
        has_dashed = any(
            line.get_linestyle() == "--"
            for line in ax1.lines
        )
        assert has_dashed, "上子图应包含均值虚线 / top subplot should contain dashed mean line"

    def test_bottom_subplot_zero_line(self):
        # 下子图含零线 / bottom subplot has zero line
        ev = _make_evaluator()
        fig = plot_turnover(ev)
        ax2 = fig.axes[1]
        has_zero = False
        for line in ax2.lines:
            ydata = line.get_ydata()
            if len(ydata) >= 2 and np.allclose(ydata, 0.0):
                has_zero = True
                break
        assert has_zero, "下子图应包含零线 / bottom subplot should contain zero line"


# ============================================================
# 文件保存测试 / File saving tests
# ============================================================


class TestPlotTurnoverSave:
    """换手率分布图文件保存验证 / Turnover chart file saving validation."""

    def test_save_png_file(self, tmp_path):
        # 保存 PNG 文件 / save PNG file
        ev = _make_evaluator()
        output = str(tmp_path / "turnover.png")
        plot_turnover(ev, output_path=output)
        assert os.path.exists(output)
        assert os.path.getsize(output) > 0

    def test_save_with_custom_filename(self, tmp_path):
        # 自定义文件名保存 / save with custom filename
        ev = _make_evaluator()
        output = str(tmp_path / "custom_turnover_chart.png")
        plot_turnover(ev, output_path=output)
        assert os.path.exists(output)

    def test_no_save_when_output_path_none(self):
        # output_path=None 时不保存文件 / no file saved when output_path is None
        import matplotlib.pyplot as plt

        ev = _make_evaluator()
        fig = plot_turnover(ev, output_path=None)
        assert isinstance(fig, plt.Figure)


# ============================================================
# 参数验证 / Parameter validation tests
# ============================================================


class TestPlotTurnoverParams:
    """换手率分布图参数验证 / Turnover chart parameter validation."""

    def test_custom_figsize(self):
        # 自定义图表尺寸 / custom figure size
        ev = _make_evaluator()
        fig = plot_turnover(ev, figsize=(10, 8))
        w, h = fig.get_size_inches()
        assert abs(w - 10.0) < 0.1
        assert abs(h - 8.0) < 0.1

    def test_custom_dpi(self, tmp_path):
        # 自定义 DPI / custom DPI
        ev = _make_evaluator()
        output = str(tmp_path / "turnover_high_dpi.png")
        fig = plot_turnover(ev, output_path=output, dpi=300)
        assert fig.dpi == 300

    def test_raises_with_empty_turnover(self):
        # 换手率数据全为 NaN 时抛出 ValueError / raises ValueError when turnover is all NaN
        ev = _make_evaluator()
        # 手动将 turnover 全部设为 NaN / manually set all turnover to NaN
        ev.turnover = pd.DataFrame(np.nan, index=ev.turnover.index,
                                   columns=ev.turnover.columns)
        with pytest.raises(ValueError, match="换手率数据为空"):
            plot_turnover(ev)

    def test_no_output_path_no_error(self):
        # 不传 output_path 不报错 / no error when output_path is not provided
        ev = _make_evaluator()
        fig = plot_turnover(ev)
        assert fig is not None


# ============================================================
# 数据一致性验证 / Data consistency validation
# ============================================================


class TestPlotTurnoverConsistency:
    """换手率分布图数据一致性验证 / Turnover chart data consistency validation."""

    def test_turnover_first_period_is_nan(self):
        # 首期换手率为 NaN（无前序截面）/ first period turnover is NaN
        ev = _make_evaluator(n_dates=50, seed=42)
        assert ev.turnover.iloc[0].isna().all(), "首期换手率应全为 NaN / first period should be all NaN"

    def test_turnover_values_in_valid_range(self):
        # 换手率值域 [0, 1] / turnover values in [0, 1]
        ev = _make_evaluator(n_dates=100, seed=42)
        valid = ev.turnover.dropna()
        assert valid.min().min() >= 0.0, "换手率不应为负 / turnover should not be negative"
        assert valid.max().max() <= 1.0 + 1e-10, "换手率不应超过 1 / turnover should not exceed 1"

    def test_rank_autocorr_values_in_valid_range(self):
        # 排名自相关值域 [-1, 1] / rank autocorrelation values in [-1, 1]
        ev = _make_evaluator(n_dates=100, seed=42)
        valid = ev.rank_autocorr.dropna()
        if len(valid) > 0:
            assert valid.min() >= -1.0 - 1e-10, "排名自相关不应 < -1"
            assert valid.max() <= 1.0 + 1e-10, "排名自相关不应 > 1"


# ============================================================
# 边界情况 / Edge cases
# ============================================================


class TestPlotTurnoverEdgeCases:
    """换手率分布图边界情况 / Turnover chart edge cases."""

    def test_minimal_data(self):
        # 最小数据量（3 个时间截面）/ minimal data (3 timestamps)
        ev = _make_evaluator(n_dates=3, n_symbols=5, seed=1)
        fig = plot_turnover(ev)
        assert len(fig.axes) == 2

    def test_large_dataset(self):
        # 大数据集不崩溃 / large dataset does not crash
        ev = _make_evaluator(n_dates=500, n_symbols=50, seed=88)
        fig = plot_turnover(ev)
        assert len(fig.axes) == 2

    def test_two_groups(self):
        # 2 组换手率图表正常 / 2-group turnover chart works
        ev = _make_evaluator(n_dates=50, n_symbols=10, seed=33, n_groups=2)
        fig = plot_turnover(ev)
        assert len(fig.axes) == 2

    def test_many_groups(self):
        # 10 组换手率图表正常 / 10-group turnover chart works
        ev = _make_evaluator(n_dates=50, n_symbols=20, seed=77, n_groups=10)
        fig = plot_turnover(ev)
        assert len(fig.axes) == 2

    def test_multi_seed_consistency(self):
        # 多种子下图表正常生成 / charts generate normally across multiple seeds
        for seed in [0, 42, 99, 2024]:
            ev = _make_evaluator(n_dates=50, seed=seed)
            fig = plot_turnover(ev)
            assert len(fig.axes) == 2
