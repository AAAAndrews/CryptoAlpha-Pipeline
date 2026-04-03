"""
FactorAnalysis/visualization/charts.py — 图表生成模块
FactorAnalysis/visualization/charts.py — Chart generation module.

提供因子绩效分析的核心图表：IC 时间序列、分组收益、净值曲线、换手率。
Provides core factor performance charts: IC timeseries, group returns,
portfolio curves, and turnover.
"""

from __future__ import annotations

import logging

import matplotlib
import matplotlib.pyplot as plt

logger = logging.getLogger(__name__)


# ============================================================
# 中文字体配置 / Chinese font configuration
# ============================================================

def configure_chinese_font(
    font_names: list[str] | None = None,
    fallback: str = "SimHei",
) -> None:
    """
    配置 matplotlib 中文字体 / Configure matplotlib Chinese font.

    按优先级尝试多个中文字体，找到第一个可用字体后设置 rcParams。
    若全部不可用则使用 fallback 字体。
    Tries multiple Chinese fonts by priority, sets rcParams to the first available.
    Falls back to the fallback font if none are available.

    Parameters / 参数:
        font_names: 候选中文字体名称列表，默认 Windows/Mac/Linux 常见中文字体
                    Candidate Chinese font names, defaults to common fonts
                    across Windows/Mac/Linux
        fallback: 所有候选字体均不可用时的回退字体
                  Fallback font when no candidates are available
    """
    import platform

    if font_names is None:
        # 按平台选择常见中文字体 / common Chinese fonts by platform
        system = platform.system()
        if system == "Windows":
            font_names = ["Microsoft YaHei", "SimHei", "SimSun", "KaiTi"]
        elif system == "Darwin":
            font_names = ["PingFang SC", "Heiti SC", "STHeiti", "Songti SC"]
        else:
            font_names = [
                "WenQuanYi Micro Hei", "WenQuanYi Zen Hei",
                "Noto Sans CJK SC", "Source Han Sans SC",
                "Droid Sans Fallback",
            ]

    # 从 matplotlib 字体缓存中查找可用字体 / find available font from cache
    available_fonts = {f.name for f in matplotlib.font_manager.fontManager.ttflist}
    selected = fallback
    for name in font_names:
        if name in available_fonts:
            selected = name
            break

    # 设置全局字体参数 / set global font parameters
    matplotlib.rcParams["font.sans-serif"] = [selected] + matplotlib.rcParams.get(
        "font.sans-serif", []
    )
    matplotlib.rcParams["axes.unicode_minus"] = False

    logger.debug("matplotlib 中文字体设置为: %s", selected)


# 模块加载时自动配置中文字体 / auto-configure Chinese font on module load
configure_chinese_font()


# ============================================================
# 图表生成函数 / Chart generation functions
# ============================================================

def plot_ic_timeseries(
    evaluator,
    output_path: str | None = None,
    figsize: tuple[float, float] = (12, 5),
    dpi: int = 150,
) -> plt.Figure:
    """
    绘制 IC 时间序列图 / Plot IC timeseries chart.

    包含滚动 IC（月度/周度）和累积 IC 曲线，标注 IC 均值线 ± 1std 阴影带。
    Includes rolling IC (monthly/weekly) and cumulative IC curve,
    with mean line and ± 1std shaded band.

    Parameters / 参数:
        evaluator: 已调用 run_metrics() 的 FactorEvaluator 实例
                   A FactorEvaluator instance that has called run_metrics()
        output_path: 图片保存路径，None 时不保存
                    Image save path, None to skip saving
        figsize: 图表尺寸 / Figure size
        dpi: 图片分辨率 / Image DPI

    Returns / 返回:
        plt.Figure — matplotlib Figure 对象 / matplotlib Figure object

    Raises / 异常:
        ValueError: evaluator 尚未调用 run_metrics()
    """
    raise NotImplementedError("plot_ic_timeseries 将在 Task 20 中实现")


def plot_group_returns(
    evaluator,
    output_path: str | None = None,
    figsize: tuple[float, float] = (12, 6),
    dpi: int = 150,
) -> plt.Figure:
    """
    绘制分组收益对比图 / Plot group returns comparison chart.

    按因子分位数（如 5 组）的累计收益曲线，标注多空对冲收益。
    Cumulative return curves by factor quantile groups, with long-short hedge return.

    Parameters / 参数:
        evaluator: 已调用 run_grouping() + run_curves() 的 FactorEvaluator 实例
                   A FactorEvaluator instance that has called run_grouping() + run_curves()
        output_path: 图片保存路径，None 时不保存
                    Image save path, None to skip saving
        figsize: 图表尺寸 / Figure size
        dpi: 图片分辨率 / Image DPI

    Returns / 返回:
        plt.Figure — matplotlib Figure 对象 / matplotlib Figure object

    Raises / 异常:
        ValueError: evaluator 尚未调用 run_grouping() 或 run_curves()
    """
    raise NotImplementedError("plot_group_returns 将在 Task 21 中实现")


def plot_portfolio_curves(
    evaluator,
    output_path: str | None = None,
    figsize: tuple[float, float] = (12, 6),
    dpi: int = 150,
) -> plt.Figure:
    """
    绘制组合净值曲线图 / Plot portfolio equity curves chart.

    long/short/hedge 三条净值曲线（含/不含手续费两个子图）。
    Three equity curves (long/short/hedge) with two subplots
    (before and after cost).

    Parameters / 参数:
        evaluator: 已调用 run_curves() 的 FactorEvaluator 实例
                   A FactorEvaluator instance that has called run_curves()
        output_path: 图片保存路径，None 时不保存
                    Image save path, None to skip saving
        figsize: 图表尺寸 / Figure size
        dpi: 图片分辨率 / Image DPI

    Returns / 返回:
        plt.Figure — matplotlib Figure 对象 / matplotlib Figure object

    Raises / 异常:
        ValueError: evaluator 尚未调用 run_curves()
    """
    raise NotImplementedError("plot_portfolio_curves 将在 Task 22 中实现")


def plot_turnover(
    evaluator,
    output_path: str | None = None,
    figsize: tuple[float, float] = (12, 4),
    dpi: int = 150,
) -> plt.Figure:
    """
    绘制换手率分布图 / Plot turnover distribution chart.

    按时间序列的换手率柱状图或面积图。
    Turnover bar chart or area chart by time series.

    Parameters / 参数:
        evaluator: 已调用 run_turnover() 的 FactorEvaluator 实例
                   A FactorEvaluator instance that has called run_turnover()
        output_path: 图片保存路径，None 时不保存
                    Image save path, None to skip saving
        figsize: 图表尺寸 / Figure size
        dpi: 图片分辨率 / Image DPI

    Returns / 返回:
        plt.Figure — matplotlib Figure 对象 / matplotlib Figure object

    Raises / 异常:
        ValueError: evaluator 尚未调用 run_turnover()
    """
    raise NotImplementedError("plot_turnover 将在 Task 23 中实现")
