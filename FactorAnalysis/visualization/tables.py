"""
FactorAnalysis/visualization/tables.py — 综合绩效表格模块
FactorAnalysis/visualization/tables.py — Summary performance table module.

将 FactorEvaluator 指标汇总为格式化 HTML 表格，包含信号灯标识。
Summarizes FactorEvaluator metrics into a formatted HTML table
with traffic-light signal indicators.
"""

from __future__ import annotations

import numpy as np


def _signal_color(value: float | None, threshold_good: float = 0.5) -> str:
    """
    根据阈值返回信号灯颜色 CSS 类名 / Return signal-light CSS class by threshold.

    ICIR > threshold_good → 绿色（好）/ green (good)
    ICIR < 0            → 红色（差）/ red (bad)
    其他                → 黄色（一般）/ yellow (neutral)

    Parameters / 参数:
        value: 指标值 / Metric value
        threshold_good: "好"的阈值，默认 0.5 / "Good" threshold, default 0.5

    Returns / 返回:
        str — CSS 颜色类名 / CSS color class name
    """
    if value is None or np.isnan(value):
        return "signal-na"
    if value > threshold_good:
        return "signal-good"
    if value < 0:
        return "signal-bad"
    return "signal-neutral"


def _fmt(value: float | None, decimals: int = 4) -> str:
    """
    格式化浮点数值，None/NaN 显示为 N/A / Format float value, None/NaN shows as N/A.

    Parameters / 参数:
        value: 待格式化的值 / Value to format
        decimals: 小数位数，默认 4 / Decimal places, default 4

    Returns / 返回:
        str — 格式化后的字符串 / Formatted string
    """
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "N/A"
    return f"{value:.{decimals}f}"


def _signal_html(value: float | None, threshold_good: float = 0.5) -> str:
    """
    返回带信号灯 CSS 类的 HTML span / Return HTML span with signal-light CSS class.

    Parameters / 参数:
        value: 指标值 / Metric value
        threshold_good: "好"的阈值，默认 0.5 / "Good" threshold, default 0.5

    Returns / 返回:
        str — <span class="signal-xxx">value</span> HTML 片段
    """
    css_class = _signal_color(value, threshold_good)
    display = _fmt(value)
    return f'<span class="{css_class}">{display}</span>'


def build_summary_table(evaluator) -> str:
    """
    将 FactorEvaluator 指标汇总为 HTML 表格 / Build HTML table from FactorEvaluator metrics.

    表格包含 IC、收益、绩效比率等关键指标，ICIR 和 Sharpe 列使用信号灯标识。
    Table includes IC, return, performance ratio metrics with signal-light
    indicators on ICIR and Sharpe columns.

    Parameters / 参数:
        evaluator: 已调用 run() 的 FactorEvaluator 实例
                   A FactorEvaluator instance that has called run()

    Returns / 返回:
        str — HTML 表格字符串 / HTML table string

    Raises / 异常:
        ValueError: evaluator 为 None 或尚未执行分析步骤
    """
    if evaluator is None:
        raise ValueError("evaluator 不能为 None / evaluator must not be None")
    # 检查是否至少执行了 run_metrics() / check if run_metrics() was at least called
    if evaluator.ic is None:
        raise ValueError(
            "evaluator 尚未调用 run() 或 run_metrics()，无可用指标 / "
            "evaluator has not called run() or run_metrics(), no metrics available"
        )

    # --- 收集指标数据 / Collect metric data ---

    # IC 分析 / IC analysis
    ic_mean = evaluator.ic.mean() if evaluator.ic is not None else None
    ic_std = evaluator.ic.std() if evaluator.ic is not None else None
    rank_ic_mean = evaluator.rank_ic.mean() if evaluator.rank_ic is not None else None
    rank_ic_std = evaluator.rank_ic.std() if evaluator.rank_ic is not None else None

    # IC 统计显著性 / IC statistical significance
    ic_t_stat = evaluator.ic_stats.get("t_stat", None) if evaluator.ic_stats is not None else None
    ic_p_value = evaluator.ic_stats.get("p_value", None) if evaluator.ic_stats is not None else None
    ic_skew = evaluator.ic_stats.get("IC_skew", None) if evaluator.ic_stats is not None else None
    ic_kurt = evaluator.ic_stats.get("IC_kurtosis", None) if evaluator.ic_stats is not None else None

    # 收益 / Returns
    long_ret = (evaluator.long_curve.iloc[-1] - 1.0) if evaluator.long_curve is not None else None
    short_ret = (evaluator.short_curve.iloc[-1] - 1.0) if evaluator.short_curve is not None else None
    hedge_ret = (evaluator.hedge_curve.iloc[-1] - 1.0) if evaluator.hedge_curve is not None else None
    hedge_ret_cost = (evaluator.hedge_curve_after_cost.iloc[-1] - 1.0) if evaluator.hedge_curve_after_cost is not None else None

    # 绩效比率 / Performance ratios
    sharpe = evaluator.sharpe
    calmar = evaluator.calmar
    sortino = evaluator.sortino
    sharpe_ac = evaluator.sharpe_after_cost
    calmar_ac = evaluator.calmar_after_cost
    sortino_ac = evaluator.sortino_after_cost

    # 换手率 / Turnover
    avg_turnover = evaluator.turnover.mean().mean() if evaluator.turnover is not None else None
    avg_rank_ac = evaluator.rank_autocorr.mean() if evaluator.rank_autocorr is not None else None

    # 中性化 / Neutralization
    neutralized_ret = (
        evaluator.neutralized_curve.iloc[-1] - 1.0
        if evaluator.neutralized_curve is not None
        else None
    )

    # --- 构建表格行 / Build table rows ---

    def row(label: str, value: str, signal: bool = False) -> str:
        """生成一行 <tr> / Generate a <tr> row."""
        return f"<tr><td>{label}</td><td>{value}</td></tr>"

    lines: list[str] = []
    # 内联样式（独立使用时无需外部 CSS）/ Inline styles (no external CSS needed when standalone)
    style = (
        '<style>'
        '.signal-good { color: #27ae60; font-weight: bold; }'
        '.signal-bad { color: #e74c3c; font-weight: bold; }'
        '.signal-neutral { color: #f39c12; font-weight: bold; }'
        '.signal-na { color: #999; }'
        '</style>'
    )
    lines.append(style)

    # 表格开始 / Table open
    lines.append('<table>')
    lines.append('<tr><th colspan="2" style="background:#4a90d9;color:white;">IC 分析 / IC Analysis</th></tr>')
    lines.append(row("IC Mean", _fmt(ic_mean)))
    lines.append(row("IC Std", _fmt(ic_std)))
    lines.append(row("RankIC Mean", _fmt(rank_ic_mean)))
    lines.append(row("RankIC Std", _fmt(rank_ic_std)))
    lines.append(row("ICIR", _signal_html(evaluator.icir), signal=True))
    lines.append(row("IC t-stat", _fmt(ic_t_stat)))
    lines.append(row("IC p-value", _fmt(ic_p_value)))
    lines.append(row("IC Skew", _fmt(ic_skew)))
    lines.append(row("IC Kurtosis", _fmt(ic_kurt)))

    lines.append('<tr><th colspan="2" style="background:#4a90d9;color:white;">收益分析 / Return Analysis</th></tr>')
    lines.append(row("Long Return", _fmt(long_ret)))
    lines.append(row("Short Return", _fmt(short_ret)))
    lines.append(row("Hedge Return", _fmt(hedge_ret)))
    lines.append(row("Hedge Return (After Cost)", _fmt(hedge_ret_cost)))

    lines.append('<tr><th colspan="2" style="background:#4a90d9;color:white;">绩效比率 / Performance Ratios</th></tr>')
    lines.append(row("Sharpe", _signal_html(sharpe), signal=True))
    lines.append(row("Calmar", _signal_html(calmar), signal=True))
    lines.append(row("Sortino", _signal_html(sortino), signal=True))
    lines.append(row("Sharpe (After Cost)", _signal_html(sharpe_ac), signal=True))
    lines.append(row("Calmar (After Cost)", _signal_html(calmar_ac), signal=True))
    lines.append(row("Sortino (After Cost)", _signal_html(sortino_ac), signal=True))

    lines.append('<tr><th colspan="2" style="background:#4a90d9;color:white;">换手率 / Turnover</th></tr>')
    lines.append(row("Avg Turnover", _fmt(avg_turnover)))
    lines.append(row("Avg Rank Autocorr", _fmt(avg_rank_ac)))

    lines.append('<tr><th colspan="2" style="background:#4a90d9;color:white;">中性化 / Neutralization</th></tr>')
    lines.append(row("Neutralized Return", _fmt(neutralized_ret)))

    lines.append('</table>')
    return "\n".join(lines)
