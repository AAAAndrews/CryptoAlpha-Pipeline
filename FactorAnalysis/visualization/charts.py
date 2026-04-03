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
    figsize: tuple[float, float] = (12, 7),
    dpi: int = 150,
    rolling_window_week: int = 5,
    rolling_window_month: int = 22,
) -> plt.Figure:
    """
    绘制 IC 时间序列图 / Plot IC timeseries chart.

    包含滚动 IC（月度/周度）和累积 IC 曲线，标注 IC 均值线 ± 1std 阴影带。
    Includes rolling IC (monthly/weekly) and cumulative IC curve,
    with mean line and ± 1std shaded band.

    上子图：日频 IC 柱状图 + 滚动 IC 均线（周度/月度）+ IC 均值线 + ±1std 阴影带
    下子图：累积 IC 曲线
    Top subplot: daily IC bars + rolling IC lines (weekly/monthly) + mean line + ±1std band
    Bottom subplot: cumulative IC curve

    Parameters / 参数:
        evaluator: 已调用 run_metrics() 的 FactorEvaluator 实例
                   A FactorEvaluator instance that has called run_metrics()
        output_path: 图片保存路径，None 时不保存
                    Image save path, None to skip saving
        figsize: 图表尺寸 / Figure size
        dpi: 图片分辨率 / Image DPI
        rolling_window_week: 周度滚动窗口天数，默认 5
                            Weekly rolling window in days, default 5
        rolling_window_month: 月度滚动窗口天数，默认 22
                             Monthly rolling window in days, default 22

    Returns / 返回:
        plt.Figure — matplotlib Figure 对象 / matplotlib Figure object

    Raises / 异常:
        ValueError: evaluator 尚未调用 run_metrics()
    """
    import numpy as np

    # 校验前置条件 / validate preconditions
    if evaluator is None:
        raise ValueError("evaluator 不能为 None")
    if evaluator.ic is None:
        raise ValueError("evaluator 尚未调用 run_metrics()，请先执行 evaluator.run_metrics()")

    ic = evaluator.ic.dropna()
    if len(ic) == 0:
        raise ValueError("IC 序列为空，无法绘制图表")

    ic_mean = float(ic.mean())
    ic_std = float(ic.std())

    # --- 上子图：日频 IC + 滚动均线 + 均值线 + ±1std 阴影带 ---
    # --- Top subplot: daily IC + rolling lines + mean + ±1std band ---
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=figsize, dpi=dpi,
                                    gridspec_kw={"height_ratios": [3, 2]})
    fig.subplots_adjust(hspace=0.35)

    dates = ic.index

    # 日频 IC 柱状图 / daily IC bar chart
    ax1.bar(dates, ic.values, width=1.0, alpha=0.35, color="steelblue",
            label="日频 IC / Daily IC")

    # 滚动 IC 均线 / rolling IC lines
    ic_series = ic.copy()
    if len(ic) >= rolling_window_week:
        rolling_week = ic_series.rolling(window=rolling_window_week, min_periods=1).mean()
        ax1.plot(dates, rolling_week.values, color="darkorange", linewidth=1.2,
                 label=f"周度滚动 ({rolling_window_week}D) / Weekly rolling")

    if len(ic) >= rolling_window_month:
        rolling_month = ic_series.rolling(window=rolling_window_month, min_periods=1).mean()
        ax1.plot(dates, rolling_month.values, color="crimson", linewidth=1.2,
                 label=f"月度滚动 ({rolling_window_month}D) / Monthly rolling")

    # IC 均值线 / IC mean line
    ax1.axhline(y=ic_mean, color="black", linestyle="--", linewidth=1.0,
                label=f"IC 均值 / Mean = {ic_mean:.4f}")

    # ±1std 阴影带 / ±1std shaded band
    ax1.axhspan(ic_mean - ic_std, ic_mean + ic_std, alpha=0.10, color="gray",
                label=f"±1std ({ic_std:.4f})")

    # 零线 / zero line
    ax1.axhline(y=0, color="gray", linestyle="-", linewidth=0.5, alpha=0.5)

    ax1.set_title("IC 时间序列 / IC Timeseries", fontsize=13, fontweight="bold")
    ax1.set_ylabel("IC", fontsize=10)
    ax1.legend(loc="upper right", fontsize=8)
    ax1.grid(True, alpha=0.3)

    # --- 下子图：累积 IC 曲线 / Bottom subplot: cumulative IC curve ---
    cumulative_ic = ic.cumsum()
    ax2.fill_between(dates, cumulative_ic.values, alpha=0.3, color="steelblue")
    ax2.plot(dates, cumulative_ic.values, color="steelblue", linewidth=1.0)
    ax2.axhline(y=0, color="gray", linestyle="-", linewidth=0.5, alpha=0.5)

    ax2.set_title("累积 IC / Cumulative IC", fontsize=13, fontweight="bold")
    ax2.set_xlabel("日期 / Date", fontsize=10)
    ax2.set_ylabel("累积 IC", fontsize=10)
    ax2.grid(True, alpha=0.3)

    # 保存图片 / save figure
    if output_path is not None:
        fig.savefig(output_path, dpi=dpi, bbox_inches="tight")
        logger.info("IC 时间序列图已保存: %s", output_path)

    return fig


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
