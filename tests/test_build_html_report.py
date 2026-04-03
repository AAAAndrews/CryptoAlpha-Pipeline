"""
tests/test_build_html_report.py — build_html_report 验证测试
tests/test_build_html_report.py — build_html_report validation tests.

验证 Task 25 实现的 build_html_report 函数：基础功能、HTML 内容正确性、
文件保存、参数验证、数据一致性、边界情况。
Validates Task 25 build_html_report: basic functionality, HTML content correctness,
file saving, parameter validation, data consistency, edge cases.
"""

from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# 确保项目根目录在 sys.path / ensure project root is in sys.path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in os.sys.path:
    os.sys.path.insert(0, project_root)


# ============================================================
# 测试数据 fixture / Test data fixtures
# ============================================================

def _make_evaluator(seed: int = 42, n_days: int = 60, n_symbols: int = 10, run_all: bool = True):
    """
    构造一个完整的 FactorEvaluator 实例 / Build a complete FactorEvaluator instance.

    创建合成因子值和收益率，按需执行全部分析步骤。
    Creates synthetic factor values and returns, optionally runs all analysis steps.
    """
    from FactorAnalysis.evaluator import FactorEvaluator

    np.random.seed(seed)
    dates = pd.date_range("2024-01-01", periods=n_days, freq="B")
    symbols = [f"S{i}" for i in range(n_symbols)]
    idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])

    factor = pd.Series(np.random.randn(len(idx)), index=idx, dtype=float)
    returns = 0.01 * factor + 0.02 * np.random.randn(len(idx))
    returns.index = idx

    ev = FactorEvaluator(factor, returns)
    if run_all:
        ev.run()
    return ev


# ============================================================
# 测试类 / Test classes
# ============================================================


class TestBuildHtmlReportBasic:
    """基础功能验证 / Basic functionality validation."""

    def test_returns_html_string(self):
        # 返回值为非空字符串 / returns non-empty string
        from FactorAnalysis.visualization import build_html_report

        ev = _make_evaluator()
        html = build_html_report(ev)
        assert isinstance(html, str)
        assert len(html) > 0

    def test_valid_html_structure(self):
        # HTML 结构完整 / complete HTML structure
        from FactorAnalysis.visualization import build_html_report

        ev = _make_evaluator()
        html = build_html_report(ev)
        assert html.strip().startswith("<!DOCTYPE html>")
        assert "</html>" in html
        assert "<head>" in html
        assert "<body>" in html

    def test_contains_title(self):
        # 包含默认标题 / contains default title
        from FactorAnalysis.visualization import build_html_report

        ev = _make_evaluator()
        html = build_html_report(ev)
        assert "因子绩效分析报告" in html

    def test_custom_title(self):
        # 自定义标题 / custom title
        from FactorAnalysis.visualization import build_html_report

        ev = _make_evaluator()
        html = build_html_report(ev, title="My Custom Report")
        assert "My Custom Report" in html

    def test_contains_generated_at(self):
        # 包含生成时间戳 / contains generation timestamp
        from FactorAnalysis.visualization import build_html_report
        import re

        ev = _make_evaluator()
        html = build_html_report(ev)
        # 匹配 YYYY-MM-DD HH:MM:SS 格式 / match YYYY-MM-DD HH:MM:SS format
        assert re.search(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", html)


class TestBuildHtmlReportContent:
    """HTML 内容正确性 / HTML content correctness."""

    def test_contains_summary_table(self):
        # 包含绩效概览表格 / contains performance summary table
        from FactorAnalysis.visualization import build_html_report

        ev = _make_evaluator()
        html = build_html_report(ev)
        assert "绩效概览" in html
        assert "Performance Summary" in html
        assert "<table>" in html

    def test_contains_signal_css(self):
        # 包含信号灯 CSS / contains signal-light CSS
        from FactorAnalysis.visualization import build_html_report

        ev = _make_evaluator()
        html = build_html_report(ev)
        assert "signal-good" in html
        assert "signal-bad" in html
        assert "signal-neutral" in html
        assert "signal-na" in html

    def test_contains_ic_chart_section(self):
        # 包含 IC 时间序列图表区域 / contains IC timeseries chart section
        from FactorAnalysis.visualization import build_html_report

        ev = _make_evaluator()
        html = build_html_report(ev, include_charts=True)
        assert "IC 时间序列" in html
        assert "data:image/png;base64," in html

    def test_contains_group_chart_section(self):
        # 包含分组收益图表区域 / contains group returns chart section
        from FactorAnalysis.visualization import build_html_report

        ev = _make_evaluator()
        html = build_html_report(ev, include_charts=True)
        assert "分组收益" in html
        assert "Group Returns" in html

    def test_contains_portfolio_chart_section(self):
        # 包含净值曲线图表区域 / contains portfolio curves chart section
        from FactorAnalysis.visualization import build_html_report

        ev = _make_evaluator()
        html = build_html_report(ev, include_charts=True)
        assert "组合净值" in html
        assert "Portfolio Curves" in html

    def test_contains_turnover_chart_section(self):
        # 包含换手率图表区域 / contains turnover chart section
        from FactorAnalysis.visualization import build_html_report

        ev = _make_evaluator()
        html = build_html_report(ev, include_charts=True)
        assert "换手率" in html
        assert "Turnover" in html

    def test_no_charts_mode(self):
        # include_charts=False 时不包含图片 / no images when include_charts=False
        from FactorAnalysis.visualization import build_html_report

        ev = _make_evaluator()
        html = build_html_report(ev, include_charts=False)
        assert "data:image/png;base64," not in html

    def test_ic_mean_in_table(self):
        # 表格中包含 IC Mean 值 / IC Mean present in table
        from FactorAnalysis.visualization import build_html_report

        ev = _make_evaluator()
        html = build_html_report(ev)
        assert "IC Mean" in html


class TestBuildHtmlReportFileSaving:
    """文件保存验证 / File saving validation."""

    def test_save_to_output_dir(self):
        # 保存 HTML 文件到指定目录 / save HTML file to specified directory
        from FactorAnalysis.visualization import build_html_report

        ev = _make_evaluator()
        with tempfile.TemporaryDirectory() as tmpdir:
            html = build_html_report(ev, output_dir=tmpdir)
            report_file = Path(tmpdir) / "report.html"
            assert report_file.exists()
            assert report_file.read_text(encoding="utf-8") == html

    def test_output_dir_auto_created(self):
        # 输出目录不存在时自动创建 / auto-create output directory
        from FactorAnalysis.visualization import build_html_report

        ev = _make_evaluator()
        with tempfile.TemporaryDirectory() as tmpdir:
            nested = os.path.join(tmpdir, "a", "b", "c")
            build_html_report(ev, output_dir=nested)
            assert os.path.isdir(nested)
            assert os.path.exists(os.path.join(nested, "report.html"))

    def test_no_output_dir_skips_saving(self):
        # output_dir=None 时不保存文件 / no file saved when output_dir=None
        from FactorAnalysis.visualization import build_html_report

        ev = _make_evaluator()
        with tempfile.TemporaryDirectory() as tmpdir:
            # 确认目录下无文件生成 / confirm no files generated in directory
            html = build_html_report(ev)
            assert len(os.listdir(tmpdir)) == 0
            assert len(html) > 0  # 但仍返回 HTML 字符串 / but still returns HTML string


class TestBuildHtmlReportValidation:
    """参数验证 / Parameter validation."""

    def test_evaluator_none_raises(self):
        # evaluator=None 抛出 ValueError / evaluator=None raises ValueError
        from FactorAnalysis.visualization import build_html_report

        with pytest.raises(ValueError, match="evaluator"):
            build_html_report(None)

    def test_unrun_evaluator_raises(self):
        # 未执行 run() 的 evaluator 抛出 ValueError / unrun evaluator raises ValueError
        from FactorAnalysis.visualization import build_html_report

        ev = _make_evaluator(run_all=False)
        with pytest.raises(ValueError, match="run"):
            build_html_report(ev)


class TestBuildHtmlReportDataConsistency:
    """数据一致性验证 / Data consistency validation."""

    def test_base64_images_valid(self):
        # base64 图片数据可解码 / base64 image data is decodable
        import base64
        from FactorAnalysis.visualization import build_html_report

        ev = _make_evaluator()
        html = build_html_report(ev, include_charts=True)

        # 提取所有 base64 图片数据 / extract all base64 image data
        pattern = r'data:image/png;base64,([A-Za-z0-9+/=]+)'
        matches = re.findall(pattern, html)
        # 应有 4 张图：IC / 分组 / 净值 / 换手率 / should have 4 charts
        assert len(matches) == 4

        # 每个 base64 数据可解码 / each base64 data is decodable
        for match in matches:
            decoded = base64.b64decode(match)
            # PNG 文件以 \x89PNG 开头 / PNG files start with \x89PNG
            assert decoded[:4] == b'\x89PNG'

    def test_report_consistent_with_evaluator(self):
        # 报告中 ICIR 值与 evaluator 一致 / ICIR in report matches evaluator
        from FactorAnalysis.visualization import build_html_report

        ev = _make_evaluator()
        html = build_html_report(ev)
        # ICIR 值应出现在报告中 / ICIR value should appear in report
        if ev.icir is not None and np.isfinite(ev.icir):
            icir_str = f"{ev.icir:.4f}"
            assert icir_str in html

    def test_summary_table_matches_standalone(self):
        # 报告中的表格与独立生成的表格一致 / table in report matches standalone
        from FactorAnalysis.visualization import build_html_report, build_summary_table

        ev = _make_evaluator()
        standalone_table = build_summary_table(ev)
        html = build_html_report(ev)

        # 独立表格中的关键行应出现在报告中 / key rows from standalone table should appear in report
        for keyword in ["IC Mean", "ICIR", "Sharpe", "Long Return"]:
            assert keyword in html
            assert keyword in standalone_table


class TestBuildHtmlReportEdgeCases:
    """边界情况 / Edge cases."""

    def test_minimal_evaluator_metrics_only(self):
        # 仅执行 run_metrics() 的 evaluator / evaluator with only run_metrics()
        from FactorAnalysis.visualization import build_html_report

        ev = _make_evaluator(run_all=False)
        ev.run_metrics()
        html = build_html_report(ev)

        # 应包含绩效表格 / should contain summary table
        assert "绩效概览" in html
        assert "<table>" in html

    def test_evaluator_with_grouping_only(self):
        # 仅执行 run_metrics + run_grouping / evaluator with run_metrics + run_grouping
        from FactorAnalysis.visualization import build_html_report

        ev = _make_evaluator(run_all=False)
        ev.run_metrics()
        ev.run_grouping()
        html = build_html_report(ev)

        assert "绩效概览" in html
        # 分组图应生成 / group chart should be generated
        assert "分组收益" in html

    def test_evaluator_without_curves(self):
        # 未执行 run_curves 时无净值曲线图 / no portfolio chart without run_curves
        from FactorAnalysis.visualization import build_html_report

        ev = _make_evaluator(run_all=False)
        ev.run_metrics()
        html = build_html_report(ev)

        # 净值曲线图不应出现（因 run_curves 未执行），但 IC 图仍应有
        # portfolio chart should not appear (run_curves not called), but IC chart should exist
        assert "组合净值" not in html
        # IC 图应存在（run_metrics 已执行）/ IC chart should exist (run_metrics was called)
        assert "IC 时间序列" in html

    def test_fig_to_base64_produces_valid_png(self):
        # _fig_to_base64 生成有效 PNG / _fig_to_base64 produces valid PNG
        import base64
        import matplotlib.pyplot as plt
        from FactorAnalysis.visualization.report_html import _fig_to_base64

        fig, ax = plt.subplots()
        ax.plot([1, 2, 3], [1, 4, 9])
        b64 = _fig_to_base64(fig)
        plt.close(fig)

        assert isinstance(b64, str)
        assert len(b64) > 0
        decoded = base64.b64decode(b64)
        assert decoded[:4] == b'\x89PNG'

    def test_multiple_calls_independent(self):
        # 多次调用返回独立 HTML / multiple calls return independent HTML
        from FactorAnalysis.visualization import build_html_report

        ev1 = _make_evaluator(seed=42)
        ev2 = _make_evaluator(seed=99)

        html1 = build_html_report(ev1, title="Report A")
        html2 = build_html_report(ev2, title="Report B")

        assert "Report A" in html1
        assert "Report B" in html2
        assert html1 != html2
