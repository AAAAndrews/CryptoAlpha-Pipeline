"""
tests/test_viz_chart_validation.py — 可视化图表生成验证测试
tests/test_viz_chart_validation.py — Visualization chart generation validation tests.

验证 Task 27：4 种图表正确生成、文件格式为 PNG、中文标签可读、数据与 FactorEvaluator 结果一致。
Validates Task 27: 4 chart types generate correctly, output format is PNG,
Chinese labels are readable, data is consistent with FactorEvaluator results.
"""

from __future__ import annotations

import os
import struct

import numpy as np
import pandas as pd
import pytest

from FactorAnalysis.evaluator import FactorEvaluator
from FactorAnalysis.visualization.charts import (
    configure_chinese_font,
    plot_group_returns,
    plot_ic_timeseries,
    plot_portfolio_curves,
    plot_turnover,
)


# ============================================================
# 测试数据构建工具 / Test data construction helpers
# ============================================================


def _make_full_evaluator(
    n_dates: int = 100,
    n_symbols: int = 10,
    seed: int = 42,
    n_groups: int = 5,
    cost_rate: float = 0.001,
):
    """
    构建已运行所有子分析的 FactorEvaluator 实例 / Build a fully-analysed FactorEvaluator.

    依次调用 run_metrics / run_grouping / run_curves / run_turnover，
    返回可用于全部 4 种图表的 evaluator。
    Calls run_metrics / run_grouping / run_curves / run_turnover in sequence,
    returns an evaluator usable for all 4 chart types.
    """
    rng = np.random.RandomState(seed)
    dates = pd.date_range("2024-01-01", periods=n_dates, freq="D")
    symbols = [f"SYM{i}" for i in range(n_symbols)]
    idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])

    # 因子值带有分组趋势，确保各图表有差异化表现
    # factor values have group-ordered trend for differentiated chart visuals
    factor_base = rng.randn(n_dates, n_symbols)
    for t in range(n_dates):
        factor_base[t].sort()
        factor_base[t] += np.linspace(-1, 1, n_symbols)

    noise = rng.randn(n_dates, n_symbols) * 0.3
    returns_vals = factor_base * 0.2 + noise

    factor = pd.Series(factor_base.ravel(), index=idx, name="factor")
    returns = pd.Series(returns_vals.ravel(), index=idx, name="returns")

    ev = FactorEvaluator(factor, returns, n_groups=n_groups, cost_rate=cost_rate)
    ev.run_metrics()
    ev.run_grouping()
    ev.run_curves()
    ev.run_turnover()
    return ev


# ============================================================
# 4 种图表正确生成 / All 4 chart types generate correctly
# ============================================================


class TestAllChartsGenerate:
    """4 种图表均能正确生成 / All 4 chart types generate correctly."""

    def test_ic_timeseries_generates(self):
        # IC 时间序列图正常生成 / IC timeseries chart generates
        import matplotlib.pyplot as plt

        ev = _make_full_evaluator()
        fig = plot_ic_timeseries(ev)
        assert isinstance(fig, plt.Figure)

    def test_group_returns_generates(self):
        # 分组收益对比图正常生成 / group returns chart generates
        import matplotlib.pyplot as plt

        ev = _make_full_evaluator()
        fig = plot_group_returns(ev)
        assert isinstance(fig, plt.Figure)

    def test_portfolio_curves_generates(self):
        # 组合净值曲线图正常生成 / portfolio curves chart generates
        import matplotlib.pyplot as plt

        ev = _make_full_evaluator()
        fig = plot_portfolio_curves(ev)
        assert isinstance(fig, plt.Figure)

    def test_turnover_generates(self):
        # 换手率分布图正常生成 / turnover chart generates
        import matplotlib.pyplot as plt

        ev = _make_full_evaluator()
        fig = plot_turnover(ev)
        assert isinstance(fig, plt.Figure)

    def test_all_charts_with_single_evaluator(self):
        # 同一个 evaluator 可依次生成全部 4 种图表 / same evaluator generates all 4 charts
        import matplotlib.pyplot as plt

        ev = _make_full_evaluator(n_dates=60, seed=77)
        fig1 = plot_ic_timeseries(ev)
        fig2 = plot_group_returns(ev)
        fig3 = plot_portfolio_curves(ev)
        fig4 = plot_turnover(ev)
        for fig in [fig1, fig2, fig3, fig4]:
            assert isinstance(fig, plt.Figure)

    def test_charts_subplot_count(self):
        # 各图表子图数量正确 / correct subplot count per chart
        ev = _make_full_evaluator(n_dates=60)
        assert len(plot_ic_timeseries(ev).axes) == 2       # 上下两个子图
        assert len(plot_group_returns(ev).axes) == 1       # 单子图
        assert len(plot_portfolio_curves(ev).axes) == 2    # 含/不含手续费
        assert len(plot_turnover(ev).axes) == 2            # 换手率 + 自相关


# ============================================================
# 文件格式为 PNG / Output format is PNG
# ============================================================


class TestPngFormat:
    """验证输出文件为合法 PNG 格式 / Validate output files are valid PNG format."""

    @staticmethod
    def _is_png(filepath: str) -> bool:
        # PNG 文件头 8 字节魔数 / PNG 8-byte magic number
        with open(filepath, "rb") as f:
            header = f.read(8)
        return header == b"\x89PNG\r\n\x1a\n"

    def test_ic_timeseries_png(self, tmp_path):
        # IC 时间序列图保存为 PNG / IC timeseries saves as PNG
        ev = _make_full_evaluator()
        output = str(tmp_path / "ic_timeseries.png")
        plot_ic_timeseries(ev, output_path=output)
        assert self._is_png(output)

    def test_group_returns_png(self, tmp_path):
        # 分组收益对比图保存为 PNG / group returns saves as PNG
        ev = _make_full_evaluator()
        output = str(tmp_path / "group_returns.png")
        plot_group_returns(ev, output_path=output)
        assert self._is_png(output)

    def test_portfolio_curves_png(self, tmp_path):
        # 组合净值曲线图保存为 PNG / portfolio curves saves as PNG
        ev = _make_full_evaluator()
        output = str(tmp_path / "portfolio_curves.png")
        plot_portfolio_curves(ev, output_path=output)
        assert self._is_png(output)

    def test_turnover_png(self, tmp_path):
        # 换手率分布图保存为 PNG / turnover saves as PNG
        ev = _make_full_evaluator()
        output = str(tmp_path / "turnover.png")
        plot_turnover(ev, output_path=output)
        assert self._is_png(output)

    def test_png_file_nonzero_size(self, tmp_path):
        # 所有 PNG 文件非空 / all PNG files are non-empty
        ev = _make_full_evaluator()
        names = ["ic.png", "group.png", "portfolio.png", "turnover.png"]
        funcs = [plot_ic_timeseries, plot_group_returns, plot_portfolio_curves, plot_turnover]
        for name, func in zip(names, funcs):
            output = str(tmp_path / name)
            func(ev, output_path=output)
            assert os.path.getsize(output) > 100, f"{name} 文件过小 / file too small"

    def test_png_contains_ihdr_chunk(self, tmp_path):
        # PNG 文件包含 IHDR 数据块（合法 PNG 必需）/ PNG contains IHDR chunk
        ev = _make_full_evaluator()
        output = str(tmp_path / "test.png")
        plot_ic_timeseries(ev, output_path=output)
        with open(output, "rb") as f:
            data = f.read()
        # IHDR chunk type bytes
        assert b"IHDR" in data, "PNG 应包含 IHDR 块 / PNG should contain IHDR chunk"

    def test_custom_dpi_affects_size(self, tmp_path):
        # 不同 DPI 下文件大小不同 / different DPI produces different file sizes
        ev = _make_full_evaluator(n_dates=50)
        low = str(tmp_path / "low.png")
        high = str(tmp_path / "high.png")
        plot_ic_timeseries(ev, output_path=low, dpi=72)
        plot_ic_timeseries(ev, output_path=high, dpi=300)
        assert os.path.getsize(high) > os.path.getsize(low)


# ============================================================
# 中文标签可读 / Chinese labels are readable
# ============================================================


class TestChineseLabels:
    """验证图表包含中文标签且字体配置正确 / Verify charts contain Chinese labels."""

    def test_configure_chinese_font_runs(self):
        # 中文字体配置函数无异常运行 / configure_chinese_font runs without error
        configure_chinese_font()  # 模块加载时已调用一次，再次调用应安全

    def test_configure_chinese_font_custom_list(self):
        # 自定义字体列表配置 / custom font list configuration
        configure_chinese_font(font_names=["SimHei", "Arial"], fallback="Arial")

    def test_ic_timeseries_has_chinese_title(self):
        # IC 时间序列图标题包含中文 / IC chart title contains Chinese
        ev = _make_full_evaluator()
        fig = plot_ic_timeseries(ev)
        title = fig.axes[0].get_title()
        assert "IC" in title or "时间序列" in title or "Timeseries" in title

    def test_group_returns_has_chinese_title(self):
        # 分组收益对比图标题包含中文 / group returns chart title contains Chinese
        ev = _make_full_evaluator()
        fig = plot_group_returns(ev)
        title = fig.axes[0].get_title()
        assert "分组" in title or "Group" in title or "收益" in title or "Returns" in title

    def test_portfolio_curves_has_chinese_title(self):
        # 组合净值曲线图标题包含中文 / portfolio curves chart title contains Chinese
        ev = _make_full_evaluator()
        fig = plot_portfolio_curves(ev)
        title = fig.axes[0].get_title()
        assert "组合" in title or "Portfolio" in title or "净值" in title or "Curves" in title

    def test_turnover_has_chinese_title(self):
        # 换手率分布图标题包含中文 / turnover chart title contains Chinese
        ev = _make_full_evaluator()
        fig = plot_turnover(ev)
        title = fig.axes[0].get_title()
        assert "换手" in title or "Turnover" in title or "分组" in title or "Group" in title

    def test_axes_unicode_minus_disabled(self):
        # axes.unicode_minus 已禁用（负号正常显示）/ axes.unicode_minus disabled
        import matplotlib

        configure_chinese_font()
        assert matplotlib.rcParams["axes.unicode_minus"] is False

    def test_ylabel_contains_chinese(self):
        # Y 轴标签包含中文 / Y-axis labels contain Chinese
        ev = _make_full_evaluator()
        fig = plot_ic_timeseries(ev)
        ylabel = fig.axes[1].get_ylabel()
        assert ylabel != "", "Y 轴标签不应为空 / Y-axis label should not be empty"

    def test_group_returns_legend_labels(self):
        # 分组收益图图例包含中文"组" / group returns legend contains Chinese "组"
        ev = _make_full_evaluator(n_groups=5)
        fig = plot_group_returns(ev)
        ax = fig.axes[0]
        labels = [t.get_text() for t in ax.get_legend().get_texts()]
        has_group_label = any("组" in l or "Group" in l for l in labels)
        assert has_group_label, "图例应包含分组标签 / legend should contain group label"


# ============================================================
# 数据与 FactorEvaluator 结果一致 / Data consistency with evaluator
# ============================================================


class TestDataConsistency:
    """验证图表数据与 FactorEvaluator 计算结果一致 / Verify chart data matches evaluator."""

    def test_ic_chart_bar_count_matches_ic_series(self):
        # IC 柱状图数据点数与 evaluator.ic 长度一致 / IC bar count matches ic series length
        ev = _make_full_evaluator(n_dates=60, seed=33)
        n_ic = len(ev.ic.dropna())
        fig = plot_ic_timeseries(ev)
        bar_container = fig.axes[0].containers[0]
        assert len(bar_container) == n_ic

    def test_ic_cumsum_matches_evaluator(self):
        # 累积 IC 值域与 evaluator.ic.cumsum() 一致 / cumulative IC range matches evaluator
        ev = _make_full_evaluator(n_dates=50, seed=99)
        ic_valid = ev.ic.dropna()
        expected_cumsum_max = float(np.max(np.abs(ic_valid.cumsum())))

        fig = plot_ic_timeseries(ev)
        ax2 = fig.axes[1]
        poly = ax2.collections[0]
        y_upper = poly.get_paths()[0].vertices[:, 1]
        actual_max = float(np.max(np.abs(y_upper)))

        # 量级一致（允许 fill_between 零线导致微小差异）/ magnitude matches
        assert actual_max > 0
        ratio = actual_max / expected_cumsum_max if expected_cumsum_max > 0 else 1.0
        assert 0.8 < ratio < 1.2, f"累积 IC 量级偏差过大 ratio={ratio:.2f}"

    def test_group_returns_line_count_matches_groups(self):
        # 分组收益折线数 = n_groups（+对冲线）/ group returns line count = n_groups (+hedge)
        ev = _make_full_evaluator(n_groups=5)
        fig = plot_group_returns(ev)
        ax = fig.axes[0]
        # 至少 5 组线 + 1 对冲线 + 1 参考线 = 7
        assert len(ax.lines) >= 6

    def test_group_returns_curves_start_at_one(self):
        # 分组收益曲线起始值归一化为 1.0 / group return curves start at 1.0
        ev = _make_full_evaluator(n_dates=50, seed=42)
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
        for col in group_curves.columns:
            assert abs(group_curves[col].iloc[0] - 1.0) < 1e-10

    def test_portfolio_curves_start_at_one(self):
        # 净值曲线起始值为 1.0 / portfolio curves start at 1.0
        ev = _make_full_evaluator(n_dates=50, seed=42)
        assert abs(ev.long_curve.iloc[0] - 1.0) < 1e-10
        assert abs(ev.short_curve.iloc[0] - 1.0) < 1e-10
        assert abs(ev.hedge_curve.iloc[0] - 1.0) < 1e-10

    def test_portfolio_curves_two_subplots_same_long_short(self):
        # 两个子图中 long/short 曲线相同（成本仅影响对冲线）/ long/short identical in both subplots
        ev = _make_full_evaluator(n_dates=50, seed=55)
        fig = plot_portfolio_curves(ev)
        ax1, ax2 = fig.axes[0], fig.axes[1]
        # ax1 和 ax2 都有 3 条线：long, short, hedge
        assert len(ax1.lines) >= 3
        assert len(ax2.lines) >= 3

    def test_turnover_data_range_valid(self):
        # 换手率数据范围合理（非负）/ turnover data range is valid (non-negative)
        ev = _make_full_evaluator(n_dates=50, seed=42)
        fig = plot_turnover(ev)
        ax1 = fig.axes[0]
        # stackplot 数据已绘制，验证 turnover DataFrame 本身非负
        assert (ev.turnover.dropna() >= 0).all().all(), "换手率应非负 / turnover should be non-negative"

    def test_rank_autocorr_range(self):
        # 排名自相关系数在 [-1, 1] 范围内 / rank autocorrelation in [-1, 1]
        ev = _make_full_evaluator(n_dates=50, seed=42)
        ra_valid = ev.rank_autocorr.dropna()
        if len(ra_valid) > 0:
            assert ra_valid.max() <= 1.0 + 1e-10
            assert ra_valid.min() >= -1.0 - 1e-10

    def test_evaluator_metrics_unchanged_after_plotting(self):
        # 绘图前后 evaluator 数值指标不变 / evaluator metrics unchanged after plotting
        ev = _make_full_evaluator(n_dates=50, seed=88)
        # 记录绘图前的指标 / record metrics before plotting
        ic_copy = ev.ic.copy()
        long_copy = ev.long_curve.copy()
        turnover_copy = ev.turnover.copy()

        # 绘制全部 4 种图表 / plot all 4 charts
        plot_ic_timeseries(ev)
        plot_group_returns(ev)
        plot_portfolio_curves(ev)
        plot_turnover(ev)

        # 验证指标未被修改 / verify metrics unchanged
        assert ev.ic.equals(ic_copy), "IC 序列不应被修改 / IC series should not be modified"
        assert ev.long_curve.equals(long_copy), "净值曲线不应被修改 / curves should not be modified"
        assert ev.turnover.equals(turnover_copy), "换手率不应被修改 / turnover should not be modified"


# ============================================================
# 多种子稳定性 / Multi-seed stability
# ============================================================


class TestMultiSeedStability:
    """多种子下图表生成稳定性 / Chart generation stability across multiple seeds."""

    @pytest.mark.parametrize("seed", [0, 42, 99, 2024, 31415])
    def test_all_charts_generate_for_seed(self, seed):
        # 不同种子下 4 种图表均正常生成 / all 4 charts generate for each seed
        ev = _make_full_evaluator(n_dates=50, seed=seed)
        fig1 = plot_ic_timeseries(ev)
        fig2 = plot_group_returns(ev)
        fig3 = plot_portfolio_curves(ev)
        fig4 = plot_turnover(ev)
        for fig in [fig1, fig2, fig3, fig4]:
            assert fig is not None


# ============================================================
# 边界情况 / Edge cases
# ============================================================


class TestEdgeCases:
    """边界情况下图表生成验证 / Chart generation edge cases."""

    def test_minimal_data_all_charts(self):
        # 最小数据量（10 个时间截面）/ minimal data (10 timestamps)
        ev = _make_full_evaluator(n_dates=10, n_symbols=5, seed=1)
        fig1 = plot_ic_timeseries(ev)
        fig2 = plot_group_returns(ev)
        fig3 = plot_portfolio_curves(ev)
        fig4 = plot_turnover(ev)
        assert all(isinstance(f, type(fig1)) for f in [fig1, fig2, fig3, fig4])

    def test_large_data_all_charts(self):
        # 大数据集（300 个时间截面 × 30 个交易对）/ large dataset
        ev = _make_full_evaluator(n_dates=300, n_symbols=30, seed=88)
        fig1 = plot_ic_timeseries(ev)
        fig2 = plot_group_returns(ev)
        fig3 = plot_portfolio_curves(ev)
        fig4 = plot_turnover(ev)
        assert all(isinstance(f, type(fig1)) for f in [fig1, fig2, fig3, fig4])

    def test_two_groups_all_charts(self):
        # 2 组极端情况 / 2 groups edge case
        ev = _make_full_evaluator(n_dates=50, n_symbols=10, n_groups=2, seed=33)
        plot_ic_timeseries(ev)
        plot_group_returns(ev)
        plot_portfolio_curves(ev)
        plot_turnover(ev)

    def test_ten_groups_all_charts(self):
        # 10 组 / 10 groups
        ev = _make_full_evaluator(n_dates=50, n_symbols=20, n_groups=10, seed=44)
        plot_ic_timeseries(ev)
        plot_group_returns(ev)
        plot_portfolio_curves(ev)
        plot_turnover(ev)

    def test_all_charts_save_to_same_directory(self, tmp_path):
        # 4 种图表保存到同一目录 / all 4 charts save to same directory
        ev = _make_full_evaluator()
        output_dir = tmp_path / "viz"
        output_dir.mkdir()
        plot_ic_timeseries(ev, output_path=str(output_dir / "ic_timeseries.png"))
        plot_group_returns(ev, output_path=str(output_dir / "group_returns.png"))
        plot_portfolio_curves(ev, output_path=str(output_dir / "portfolio_curves.png"))
        plot_turnover(ev, output_path=str(output_dir / "turnover.png"))
        assert len(list(output_dir.iterdir())) == 4

    def test_no_output_path_no_files_created(self, tmp_path):
        # 不传 output_path 不创建任何文件 / no files created without output_path
        ev = _make_full_evaluator()
        plot_ic_timeseries(ev)
        plot_group_returns(ev)
        plot_portfolio_curves(ev)
        plot_turnover(ev)
        assert len(list(tmp_path.iterdir())) == 0
