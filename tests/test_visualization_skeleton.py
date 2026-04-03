"""
tests/test_visualization_skeleton.py — 可视化模块骨架验证测试
tests/test_visualization_skeleton.py — Visualization module skeleton validation tests.

验证 Task 19 创建的 FactorAnalysis/visualization/ 子包结构完整性。
Validates the FactorAnalysis/visualization/ subpackage structure created in Task 19.
"""

from __future__ import annotations

import importlib
import os

import pytest


class TestPackageStructure:
    """子包结构完整性验证 / Subpackage structure completeness validation."""

    def test_visualization_package_importable(self):
        # 可视化子包可正常导入 / visualization subpackage is importable
        from FactorAnalysis.visualization import (
            configure_chinese_font,
            plot_ic_timeseries,
            plot_group_returns,
            plot_portfolio_curves,
            plot_turnover,
            build_summary_table,
            build_html_report,
        )
        assert callable(configure_chinese_font)
        assert callable(plot_ic_timeseries)
        assert callable(plot_group_returns)
        assert callable(plot_portfolio_curves)
        assert callable(plot_turnover)
        assert callable(build_summary_table)
        assert callable(build_html_report)

    def test_charts_module_importable(self):
        # charts.py 可独立导入 / charts.py is independently importable
        from FactorAnalysis.visualization.charts import (
            configure_chinese_font,
            plot_ic_timeseries,
            plot_group_returns,
            plot_portfolio_curves,
            plot_turnover,
        )
        assert callable(configure_chinese_font)
        assert callable(plot_ic_timeseries)
        assert callable(plot_group_returns)
        assert callable(plot_portfolio_curves)
        assert callable(plot_turnover)

    def test_tables_module_importable(self):
        # tables.py 可独立导入 / tables.py is independently importable
        from FactorAnalysis.visualization.tables import (
            build_summary_table,
            _signal_color,
        )
        assert callable(build_summary_table)
        assert callable(_signal_color)

    def test_report_html_module_importable(self):
        # report_html.py 可独立导入 / report_html.py is independently importable
        from FactorAnalysis.visualization.report_html import (
            build_html_report,
            _fig_to_base64,
            _HTML_TEMPLATE,
        )
        assert callable(build_html_report)
        assert callable(_fig_to_base64)
        assert isinstance(_HTML_TEMPLATE, str)
        assert "<!DOCTYPE html>" in _HTML_TEMPLATE


class TestChineseFontConfig:
    """中文字体配置验证 / Chinese font configuration validation."""

    def test_configure_chinese_font_runs_without_error(self):
        # 配置中文字体不抛异常 / configuring Chinese font raises no error
        import matplotlib
        from FactorAnalysis.visualization.charts import configure_chinese_font

        # 保存原始配置 / save original config
        original_sans = list(matplotlib.rcParams["font.sans-serif"])
        original_minus = matplotlib.rcParams["axes.unicode_minus"]

        configure_chinese_font()

        # 配置后 sans-serif 非空，unicode_minus 已关闭
        # after config: sans-serif is non-empty, unicode_minus is disabled
        assert len(matplotlib.rcParams["font.sans-serif"]) > 0
        assert matplotlib.rcParams["axes.unicode_minus"] is False

        # 恢复原始配置 / restore original config
        matplotlib.rcParams["font.sans-serif"] = original_sans
        matplotlib.rcParams["axes.unicode_minus"] = original_minus

    def test_configure_chinese_font_custom_list(self):
        # 自定义字体列表 / custom font list
        import matplotlib
        from FactorAnalysis.visualization.charts import configure_chinese_font

        original_sans = list(matplotlib.rcParams["font.sans-serif"])
        configure_chinese_font(font_names=["NonExistentFontA", "NonExistentFontB"])
        # 不可用字体时应回退到 SimHei / should fallback to SimHei when fonts unavailable
        assert "SimHei" in matplotlib.rcParams["font.sans-serif"]
        matplotlib.rcParams["font.sans-serif"] = original_sans

    def test_configure_chinese_font_with_real_font(self):
        # 传入真实可用字体 / passing a real available font
        import matplotlib
        from FactorAnalysis.visualization.charts import configure_chinese_font

        original_sans = list(matplotlib.rcParams["font.sans-serif"])
        # "DejaVu Sans" 是 matplotlib 默认自带的字体
        configure_chinese_font(font_names=["DejaVu Sans"])
        assert "DejaVu Sans" in matplotlib.rcParams["font.sans-serif"]
        matplotlib.rcParams["font.sans-serif"] = original_sans


class TestSignalColor:
    """信号灯颜色逻辑验证 / Signal color logic validation."""

    def test_signal_good(self):
        from FactorAnalysis.visualization.tables import _signal_color
        assert _signal_color(0.8) == "signal-good"
        assert _signal_color(0.51) == "signal-good"
        assert _signal_color(1.0) == "signal-good"
        # 边界值：等于阈值不算好 / boundary: equal to threshold is not "good"
        assert _signal_color(0.5) == "signal-neutral"

    def test_signal_bad(self):
        from FactorAnalysis.visualization.tables import _signal_color
        assert _signal_color(-0.1) == "signal-bad"
        assert _signal_color(-1.0) == "signal-bad"
        assert _signal_color(0.0) == "signal-neutral"  # 0 不算差 / 0 is not bad

    def test_signal_neutral(self):
        from FactorAnalysis.visualization.tables import _signal_color
        assert _signal_color(0.0) == "signal-neutral"
        assert _signal_color(0.3) == "signal-neutral"
        assert _signal_color(0.49) == "signal-neutral"

    def test_signal_na(self):
        from FactorAnalysis.visualization.tables import _signal_color
        assert _signal_color(None) == "signal-na"
        assert _signal_color(float("nan")) == "signal-na"

    def test_signal_custom_threshold(self):
        from FactorAnalysis.visualization.tables import _signal_color
        assert _signal_color(0.31, threshold_good=0.3) == "signal-good"
        # 边界值 / boundary value
        assert _signal_color(0.3, threshold_good=0.3) == "signal-neutral"
        assert _signal_color(0.29, threshold_good=0.3) == "signal-neutral"


class TestHtmlTemplate:
    """HTML 模板验证 / HTML template validation."""

    def test_template_contains_required_placeholders(self):
        from FactorAnalysis.visualization.report_html import _HTML_TEMPLATE

        # 模板包含必要变量占位符 / template contains required variable placeholders
        assert "{{ title }}" in _HTML_TEMPLATE
        assert "{{ generated_at }}" in _HTML_TEMPLATE
        assert "{{ summary_table" in _HTML_TEMPLATE
        assert "{{ ic_chart }}" in _HTML_TEMPLATE
        assert "{{ group_chart }}" in _HTML_TEMPLATE
        assert "{{ portfolio_chart }}" in _HTML_TEMPLATE
        assert "{{ turnover_chart }}" in _HTML_TEMPLATE

    def test_template_contains_signal_css(self):
        from FactorAnalysis.visualization.report_html import _HTML_TEMPLATE

        # 模板包含信号灯 CSS 样式 / template contains signal-light CSS styles
        assert "signal-good" in _HTML_TEMPLATE
        assert "signal-bad" in _HTML_TEMPLATE
        assert "signal-neutral" in _HTML_TEMPLATE
        assert "signal-na" in _HTML_TEMPLATE

    def test_template_is_valid_html(self):
        from FactorAnalysis.visualization.report_html import _HTML_TEMPLATE

        assert _HTML_TEMPLATE.strip().startswith("<!DOCTYPE html>")
        assert _HTML_TEMPLATE.strip().endswith("</html>")


class TestStubFunctionsRaiseNotImplemented:
    """骨架函数抛出 NotImplementedError / Skeleton functions raise NotImplementedError."""

    def test_plot_ic_timeseries_raises(self):
        from FactorAnalysis.visualization.charts import plot_ic_timeseries
        with pytest.raises(NotImplementedError):
            plot_ic_timeseries(None)

    def test_plot_group_returns_raises(self):
        from FactorAnalysis.visualization.charts import plot_group_returns
        with pytest.raises(NotImplementedError):
            plot_group_returns(None)

    def test_plot_portfolio_curves_raises(self):
        from FactorAnalysis.visualization.charts import plot_portfolio_curves
        with pytest.raises(NotImplementedError):
            plot_portfolio_curves(None)

    def test_plot_turnover_raises(self):
        from FactorAnalysis.visualization.charts import plot_turnover
        with pytest.raises(NotImplementedError):
            plot_turnover(None)

    def test_build_summary_table_raises(self):
        from FactorAnalysis.visualization.tables import build_summary_table
        with pytest.raises(NotImplementedError):
            build_summary_table(None)

    def test_build_html_report_raises(self):
        from FactorAnalysis.visualization.report_html import build_html_report
        with pytest.raises(NotImplementedError):
            build_html_report(None)

    def test_fig_to_base64_not_called_yet(self):
        # _fig_to_base64 存在但骨架阶段不测试实际功能
        # _fig_to_base64 exists but actual functionality is not tested in skeleton stage
        from FactorAnalysis.visualization.report_html import _fig_to_base64
        assert callable(_fig_to_base64)


class TestAllExports:
    """__all__ 导出验证 / __all__ exports validation."""

    def test_init_all_exports(self):
        from FactorAnalysis import visualization
        expected = [
            "configure_chinese_font",
            "plot_ic_timeseries",
            "plot_group_returns",
            "plot_portfolio_curves",
            "plot_turnover",
            "build_summary_table",
            "build_html_report",
        ]
        assert set(visualization.__all__) == set(expected)

    def test_init_all_importable(self):
        from FactorAnalysis import visualization
        for name in visualization.__all__:
            assert hasattr(visualization, name), f"{name} not found in visualization module"
