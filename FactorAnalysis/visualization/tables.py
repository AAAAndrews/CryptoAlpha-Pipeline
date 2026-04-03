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


def build_summary_table(evaluator) -> str:
    """
    将 FactorEvaluator 指标汇总为 HTML 表格 / Build HTML table from FactorEvaluator metrics.

    表格包含 IC、收益、绩效比率等关键指标，ICIR 列使用信号灯标识。
    Table includes IC, return, performance ratio metrics with signal-light
    indicators on ICIR column.

    Parameters / 参数:
        evaluator: 已调用 run() 的 FactorEvaluator 实例
                   A FactorEvaluator instance that has called run()

    Returns / 返回:
        str — HTML 表格字符串 / HTML table string

    Raises / 异常:
        ValueError: evaluator 尚未调用 run()
    """
    raise NotImplementedError("build_summary_table 将在 Task 24 中实现")
