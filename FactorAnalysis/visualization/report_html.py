"""
FactorAnalysis/visualization/report_html.py — HTML 报告组装模块
FactorAnalysis/visualization/report_html.py — HTML report assembly module.

使用 Jinja2 模板将图表和绩效表格组装为单一自包含 HTML 文件。
Assembles charts and performance tables into a single self-contained HTML file
using Jinja2 templates.

图片以 base64 内嵌，无需外部依赖。
Images are embedded as base64, no external dependencies required.
"""

from __future__ import annotations

import base64
import io
import logging
from pathlib import Path

import matplotlib.pyplot as plt

logger = logging.getLogger(__name__)

# Jinja2 HTML 模板（内嵌） / Jinja2 HTML template (inline)
_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{{ title }}</title>
<style>
    body { font-family: "Microsoft YaHei", "PingFang SC", sans-serif; max-width: 1200px; margin: 0 auto; padding: 20px; background: #fafafa; }
    h1 { color: #333; border-bottom: 2px solid #4a90d9; padding-bottom: 10px; }
    h2 { color: #4a90d9; margin-top: 30px; }
    .chart-container { margin: 20px 0; text-align: center; }
    .chart-container img { max-width: 100%; border: 1px solid #ddd; border-radius: 4px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
    .summary-table { margin: 20px 0; }
    .summary-table table { border-collapse: collapse; width: 100%; background: white; }
    .summary-table th, .summary-table td { border: 1px solid #ddd; padding: 8px 12px; text-align: center; }
    .summary-table th { background: #4a90d9; color: white; }
    .summary-table tr:nth-child(even) { background: #f9f9f9; }
    .signal-good { color: #27ae60; font-weight: bold; }
    .signal-bad { color: #e74c3c; font-weight: bold; }
    .signal-neutral { color: #f39c12; font-weight: bold; }
    .signal-na { color: #999; }
</style>
</head>
<body>
<h1>{{ title }}</h1>
<p>Generated: {{ generated_at }}</p>

<h2>绩效概览 / Performance Summary</h2>
<div class="summary-table">
{{ summary_table|safe }}
</div>

{% if ic_chart %}
<h2>IC 时间序列 / IC Timeseries</h2>
<div class="chart-container"><img src="data:image/png;base64,{{ ic_chart }}" alt="IC Timeseries"></div>
{% endif %}

{% if group_chart %}
<h2>分组收益 / Group Returns</h2>
<div class="chart-container"><img src="data:image/png;base64,{{ group_chart }}" alt="Group Returns"></div>
{% endif %}

{% if portfolio_chart %}
<h2>组合净值 / Portfolio Curves</h2>
<div class="chart-container"><img src="data:image/png;base64,{{ portfolio_chart }}" alt="Portfolio Curves"></div>
{% endif %}

{% if turnover_chart %}
<h2>换手率 / Turnover</h2>
<div class="chart-container"><img src="data:image/png;base64,{{ turnover_chart }}" alt="Turnover"></div>
{% endif %}

</body>
</html>
"""


def _fig_to_base64(fig) -> str:
    """
    将 matplotlib Figure 转为 base64 编码字符串 / Convert matplotlib Figure to base64 string.

    Parameters / 参数:
        fig: matplotlib Figure 对象 / matplotlib Figure object

    Returns / 返回:
        str — base64 编码的 PNG 图片字符串 / base64-encoded PNG image string
    """
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


def build_html_report(
    evaluator,
    output_dir: str | None = None,
    title: str | None = None,
    include_charts: bool = True,
) -> str:
    """
    使用 Jinja2 模板组装 HTML 报告 / Assemble HTML report using Jinja2 template.

    将 FactorEvaluator 的图表和绩效指标组装为单一自包含 HTML 文件，
    图片以 base64 内嵌，无需外部依赖。
    Assembles FactorEvaluator charts and metrics into a single self-contained
    HTML file. Images are base64-embedded with no external dependencies.

    Parameters / 参数:
        evaluator: 已调用 run() 的 FactorEvaluator 实例
                   A FactorEvaluator instance that has called run()
        output_dir: 报告输出目录，None 时不保存文件
                    Report output directory, None to skip file saving
        title: 报告标题，None 时自动生成 / Report title, auto-generated if None
        include_charts: 是否包含图表，默认 True / Whether to include charts, default True

    Returns / 返回:
        str — 生成的 HTML 内容 / Generated HTML content

    Raises / 异常:
        ValueError: evaluator 为 None 或尚未执行分析步骤
    """
    from datetime import datetime

    from jinja2 import Template

    from .charts import (
        plot_ic_timeseries,
        plot_group_returns,
        plot_portfolio_curves,
        plot_turnover,
    )
    from .tables import build_summary_table

    # --- 校验前置条件 / validate preconditions ---
    if evaluator is None:
        raise ValueError("evaluator 不能为 None / evaluator must not be None")
    if evaluator.ic is None:
        raise ValueError(
            "evaluator 尚未调用 run() 或 run_metrics()，无可用指标 / "
            "evaluator has not called run() or run_metrics(), no metrics available"
        )

    # --- 报告标题 / Report title ---
    if title is None:
        title = "因子绩效分析报告 / Factor Performance Report"

    # --- 生成时间戳 / Generate timestamp ---
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # --- 综合绩效表格 / Summary performance table ---
    summary_table = build_summary_table(evaluator)

    # --- 图表生成与 base64 编码 / Chart generation and base64 encoding ---
    ic_chart: str | None = None
    group_chart: str | None = None
    portfolio_chart: str | None = None
    turnover_chart: str | None = None

    if include_charts:
        # IC 时间序列图（需要 run_metrics）/ IC timeseries (requires run_metrics)
        if evaluator.ic is not None and len(evaluator.ic.dropna()) > 0:
            try:
                fig = plot_ic_timeseries(evaluator)
                ic_chart = _fig_to_base64(fig)
                plt.close(fig)
            except Exception as e:
                logger.warning("IC 时间序列图生成失败: %s", e)

        # 分组收益图（需要 run_grouping）/ Group returns chart (requires run_grouping)
        if evaluator.group_labels is not None:
            try:
                fig = plot_group_returns(evaluator)
                group_chart = _fig_to_base64(fig)
                plt.close(fig)
            except Exception as e:
                logger.warning("分组收益图生成失败: %s", e)

        # 净值曲线图（需要 run_curves）/ Portfolio curves chart (requires run_curves)
        if (evaluator.long_curve is not None
                and evaluator.short_curve is not None
                and evaluator.hedge_curve is not None):
            try:
                fig = plot_portfolio_curves(evaluator)
                portfolio_chart = _fig_to_base64(fig)
                plt.close(fig)
            except Exception as e:
                logger.warning("净值曲线图生成失败: %s", e)

        # 换手率图（需要 run_turnover）/ Turnover chart (requires run_turnover)
        if evaluator.turnover is not None and len(evaluator.turnover.dropna(how="all")) > 0:
            try:
                fig = plot_turnover(evaluator)
                turnover_chart = _fig_to_base64(fig)
                plt.close(fig)
            except Exception as e:
                logger.warning("换手率图生成失败: %s", e)

    # --- Jinja2 模板渲染 / Jinja2 template rendering ---
    template = Template(_HTML_TEMPLATE)
    html = template.render(
        title=title,
        generated_at=generated_at,
        summary_table=summary_table,
        ic_chart=ic_chart,
        group_chart=group_chart,
        portfolio_chart=portfolio_chart,
        turnover_chart=turnover_chart,
    )

    # --- 保存文件 / Save to file ---
    if output_dir is not None:
        out_path = Path(output_dir)
        out_path.mkdir(parents=True, exist_ok=True)
        report_file = out_path / "report.html"
        report_file.write_text(html, encoding="utf-8")
        logger.info("HTML 报告已保存: %s", report_file)

    return html
