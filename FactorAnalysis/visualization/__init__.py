"""
FactorAnalysis/visualization — 可视化子包
FactorAnalysis/visualization — Visualization subpackage.

提供因子绩效分析图表、综合绩效表格和 HTML 报告组装功能。
Provides factor performance charts, summary tables, and HTML report assembly.

子模块 / Sub-modules:
    - charts.py:        IC 时间序列 / 分组收益 / 净值曲线 / 换手率图表
    - tables.py:        综合绩效表格（信号灯标识）
    - report_html.py:   Jinja2 HTML 报告组装（base64 内嵌图片）
"""

from .charts import (
    configure_chinese_font,
    plot_ic_timeseries,
    plot_group_returns,
    plot_portfolio_curves,
    plot_turnover,
)
from .tables import build_summary_table
from .report_html import build_html_report

__all__ = [
    # 字体配置 / Font configuration
    "configure_chinese_font",
    # 图表生成 / Chart generation
    "plot_ic_timeseries",
    "plot_group_returns",
    "plot_portfolio_curves",
    "plot_turnover",
    # 绩效表格 / Summary table
    "build_summary_table",
    # HTML 报告 / HTML report
    "build_html_report",
]
