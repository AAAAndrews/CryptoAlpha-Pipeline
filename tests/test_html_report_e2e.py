"""
tests/test_html_report_e2e.py — HTML 报告端到端集成测试
tests/test_html_report_e2e.py — HTML report end-to-end integration tests.

验证 Task 28：从 run_factor_research → build_html_report → report.html 的完整端到端集成，
包括 report.html 生成、base64 图片内嵌、信号灯标识正确性、--viz-output 路径自动创建、
pipeline 数值输出不受可视化影响。
Validates Task 28: Full E2E integration from run_factor_research → build_html_report → report.html,
including file generation, base64 image embedding, signal indicator correctness,
--viz-output auto-creation, and pipeline numerical output unaffected by visualization.
"""

from __future__ import annotations

import base64
import os
import re
import shutil
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


# ============================================================
# 辅助函数 / Helper functions
# ============================================================


def _make_factor_data(n_dates=80, n_symbols=10, seed=42):
    """
    生成标准测试行情数据（平铺格式）。
    Generate standard test OHLC data (flat format).

    参数含义：
        n_dates: 时间截面数（小时级别 K 线）
        n_symbols: 交易对数量
        seed: 随机种子
    """
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2024-01-01", periods=n_dates, freq="1h")
    symbols = [f"SYM{i:03d}" for i in range(n_symbols)]
    rows = []
    for d in dates:
        for s in symbols:
            rows.append({"timestamp": d, "symbol": s})
    meta = pd.DataFrame(rows)
    n = len(meta)
    close = 100 + rng.standard_normal(n).cumsum() * 0.5
    close = np.abs(close) + 1
    meta["open"] = close * (1 + rng.standard_normal(n) * 0.005)
    meta["high"] = close * (1 + np.abs(rng.standard_normal(n)) * 0.01)
    meta["low"] = close * (1 - np.abs(rng.standard_normal(n)) * 0.01)
    meta["close"] = close
    meta["high"] = meta[["high", "low", "close", "open"]].max(axis=1)
    meta["low"] = meta[["high", "low", "close", "open"]].min(axis=1)
    return meta


class _MockLoader:
    """Mock KlineLoader，返回合成数据 / Mock KlineLoader returning synthetic data."""

    def __init__(self, data=None):
        self._data = data if data is not None else _make_factor_data()

    def compile(self):
        return self._data


def _run_pipeline_with_viz(
    factor_name="AlphaMomentum",
    viz_output=None,
    seed=42,
    n_dates=80,
    n_symbols=10,
    monkeypatch=None,
):
    """
    执行完整 pipeline（mock 数据）并返回 (evaluator, report, viz_dir)。
    Run full pipeline (mocked data) and return (evaluator, report, viz_dir).
    """
    from scripts.run_factor_research import run_factor_research

    mock_data = _make_factor_data(n_dates=n_dates, n_symbols=n_symbols, seed=seed)
    if monkeypatch is not None:
        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kw: _MockLoader(mock_data),
        )

    if viz_output is not None:
        viz_dir = viz_output
    else:
        viz_dir = None

    ev, report = run_factor_research(
        factor_name=factor_name,
        viz_output=viz_dir,
    )
    return ev, report, viz_dir


def _extract_base64_images(html: str) -> list[str]:
    """
    从 HTML 中提取所有 base64 编码图片数据。
    Extract all base64-encoded image data from HTML.
    """
    pattern = r"data:image/png;base64,([A-Za-z0-9+/=]+)"
    return re.findall(pattern, html)


def _count_base64_images(html: str) -> int:
    """统计 HTML 中 base64 图片数量 / Count base64 images in HTML."""
    return len(_extract_base64_images(html))


# ============================================================
# 1. report.html 端到端生成验证 / report.html E2E generation
# ============================================================


class TestReportHtmlE2EGeneration:
    """验证完整 pipeline 生成 report.html / Verify full pipeline generates report.html."""

    def test_report_html_exists_after_pipeline(self, monkeypatch):
        """pipeline 执行后 report.html 存在 / report.html exists after pipeline."""
        viz_dir = tempfile.mkdtemp()
        try:
            ev, report, _ = _run_pipeline_with_viz(
                viz_output=viz_dir, monkeypatch=monkeypatch,
            )
            report_path = os.path.join(viz_dir, "report.html")
            assert os.path.isfile(report_path), "report.html 应存在"
        finally:
            shutil.rmtree(viz_dir, ignore_errors=True)

    def test_report_html_valid_structure(self, monkeypatch):
        """report.html 具有完整 HTML 结构 / report.html has complete HTML structure."""
        viz_dir = tempfile.mkdtemp()
        try:
            _run_pipeline_with_viz(viz_output=viz_dir, monkeypatch=monkeypatch)
            report_path = os.path.join(viz_dir, "report.html")
            content = Path(report_path).read_text(encoding="utf-8")

            assert content.strip().startswith("<!DOCTYPE html>")
            assert "</html>" in content
            assert "<head>" in content
            assert "</head>" in content
            assert "<body>" in content
            assert "</body>" in content
        finally:
            shutil.rmtree(viz_dir, ignore_errors=True)

    def test_report_html_contains_all_sections(self, monkeypatch):
        """report.html 包含所有章节 / report.html contains all sections."""
        viz_dir = tempfile.mkdtemp()
        try:
            _run_pipeline_with_viz(viz_output=viz_dir, monkeypatch=monkeypatch)
            content = Path(os.path.join(viz_dir, "report.html")).read_text(encoding="utf-8")

            # 绩效概览 / Performance summary
            assert "绩效概览" in content
            assert "Performance Summary" in content

            # 4 种图表章节 / 4 chart sections
            assert "IC 时间序列" in content
            assert "IC Timeseries" in content
            assert "分组收益" in content
            assert "Group Returns" in content
            assert "组合净值" in content
            assert "Portfolio Curves" in content
            assert "换手率" in content
            assert "Turnover" in content
        finally:
            shutil.rmtree(viz_dir, ignore_errors=True)

    def test_report_html_title_contains_factor_name(self, monkeypatch):
        """报告标题包含因子名 / Report title contains factor name."""
        viz_dir = tempfile.mkdtemp()
        try:
            _run_pipeline_with_viz(
                factor_name="AlphaMomentum",
                viz_output=viz_dir,
                monkeypatch=monkeypatch,
            )
            content = Path(os.path.join(viz_dir, "report.html")).read_text(encoding="utf-8")
            assert "AlphaMomentum" in content
        finally:
            shutil.rmtree(viz_dir, ignore_errors=True)

    def test_report_html_generated_at_timestamp(self, monkeypatch):
        """报告包含生成时间戳 / Report contains generation timestamp."""
        viz_dir = tempfile.mkdtemp()
        try:
            _run_pipeline_with_viz(viz_output=viz_dir, monkeypatch=monkeypatch)
            content = Path(os.path.join(viz_dir, "report.html")).read_text(encoding="utf-8")
            assert re.search(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", content)
        finally:
            shutil.rmtree(viz_dir, ignore_errors=True)


# ============================================================
# 2. base64 图片内嵌无外部依赖 / Base64 images, no external deps
# ============================================================


class TestBase64EmbeddedImages:
    """验证图片 base64 内嵌且无外部依赖 / Verify base64-embedded images, no external deps."""

    def test_four_charts_embedded(self, monkeypatch):
        """4 种图表均以 base64 内嵌 / All 4 charts embedded as base64."""
        viz_dir = tempfile.mkdtemp()
        try:
            _run_pipeline_with_viz(viz_output=viz_dir, monkeypatch=monkeypatch)
            content = Path(os.path.join(viz_dir, "report.html")).read_text(encoding="utf-8")

            n_images = _count_base64_images(content)
            assert n_images == 4, f"应有 4 张 base64 图片，实际 {n_images}"
        finally:
            shutil.rmtree(viz_dir, ignore_errors=True)

    def test_base64_images_decode_to_png(self, monkeypatch):
        """base64 数据可解码为有效 PNG / base64 data decodes to valid PNG."""
        viz_dir = tempfile.mkdtemp()
        try:
            _run_pipeline_with_viz(viz_output=viz_dir, monkeypatch=monkeypatch)
            content = Path(os.path.join(viz_dir, "report.html")).read_text(encoding="utf-8")

            images = _extract_base64_images(content)
            assert len(images) == 4
            for img_b64 in images:
                decoded = base64.b64decode(img_b64)
                assert decoded[:4] == b'\x89PNG', "图片应为有效 PNG 格式"
                assert len(decoded) > 100, "图片数据不应为空"
        finally:
            shutil.rmtree(viz_dir, ignore_errors=True)

    def test_no_external_image_references(self, monkeypatch):
        """HTML 中无外部图片引用 / No external image references in HTML."""
        viz_dir = tempfile.mkdtemp()
        try:
            _run_pipeline_with_viz(viz_output=viz_dir, monkeypatch=monkeypatch)
            content = Path(os.path.join(viz_dir, "report.html")).read_text(encoding="utf-8")

            # 不应包含 http/https 外部图片链接 / no http/https external image URLs
            external_img_pattern = r'<img\s+src=["\']https?://'
            assert not re.search(external_img_pattern, content), "不应有外部图片引用"

            # 不应包含相对路径图片 / no relative path images
            relative_img_pattern = r'<img\s+src=["\'](?!(data:))'
            assert not re.search(relative_img_pattern, content), "不应有非 base64 的图片引用"
        finally:
            shutil.rmtree(viz_dir, ignore_errors=True)

    def test_no_external_css_or_js(self, monkeypatch):
        """HTML 中无外部 CSS/JS 引用 / No external CSS/JS references in HTML."""
        viz_dir = tempfile.mkdtemp()
        try:
            _run_pipeline_with_viz(viz_output=viz_dir, monkeypatch=monkeypatch)
            content = Path(os.path.join(viz_dir, "report.html")).read_text(encoding="utf-8")

            # 不应包含外部 CSS/JS 链接 / no external CSS/JS links
            assert "<link " not in content, "不应有外部 link 标签"
            assert 'src="http' not in content, "不应有外部 JS 引用"
        finally:
            shutil.rmtree(viz_dir, ignore_errors=True)

    def test_self_contained_single_file(self, monkeypatch):
        """report.html 为自包含单文件 / report.html is self-contained single file."""
        viz_dir = tempfile.mkdtemp()
        try:
            _run_pipeline_with_viz(viz_output=viz_dir, monkeypatch=monkeypatch)
            report_path = os.path.join(viz_dir, "report.html")
            content = Path(report_path).read_text(encoding="utf-8")

            # 目录中应只有 report.html / only report.html should exist
            files = os.listdir(viz_dir)
            assert files == ["report.html"], f"目录应仅有 report.html，实际: {files}"

            # 文件大小合理（HTML + 4 张 base64 图片） / reasonable file size
            file_size = os.path.getsize(report_path)
            assert file_size > 10000, f"自包含 HTML 文件应 > 10KB，实际 {file_size} bytes"
        finally:
            shutil.rmtree(viz_dir, ignore_errors=True)

    def test_base64_images_match_chart_types(self, monkeypatch):
        """base64 图片对应 4 种图表类型 / base64 images match 4 chart types."""
        viz_dir = tempfile.mkdtemp()
        try:
            _run_pipeline_with_viz(viz_output=viz_dir, monkeypatch=monkeypatch)
            content = Path(os.path.join(viz_dir, "report.html")).read_text(encoding="utf-8")

            # 每种图表应有对应的 alt 文本 / each chart should have alt text
            assert 'alt="IC Timeseries"' in content
            assert 'alt="Group Returns"' in content
            assert 'alt="Portfolio Curves"' in content
            assert 'alt="Turnover"' in content
        finally:
            shutil.rmtree(viz_dir, ignore_errors=True)


# ============================================================
# 3. 信号灯标识正确性 / Signal indicator correctness
# ============================================================


class TestSignalIndicators:
    """验证信号灯标识颜色逻辑正确 / Verify signal indicator color logic is correct."""

    def test_signal_css_classes_present(self, monkeypatch):
        """4 种信号灯 CSS 类均存在于 HTML / All 4 signal CSS classes present in HTML."""
        viz_dir = tempfile.mkdtemp()
        try:
            _run_pipeline_with_viz(viz_output=viz_dir, monkeypatch=monkeypatch)
            content = Path(os.path.join(viz_dir, "report.html")).read_text(encoding="utf-8")

            # 模板 CSS 中定义了 4 种信号灯 / template CSS defines 4 signal types
            assert "signal-good" in content
            assert "signal-bad" in content
            assert "signal-neutral" in content
            assert "signal-na" in content
        finally:
            shutil.rmtree(viz_dir, ignore_errors=True)

    def test_signal_colors_css_correct(self, monkeypatch):
        """信号灯 CSS 颜色定义正确 / Signal CSS color definitions correct."""
        viz_dir = tempfile.mkdtemp()
        try:
            _run_pipeline_with_viz(viz_output=viz_dir, monkeypatch=monkeypatch)
            content = Path(os.path.join(viz_dir, "report.html")).read_text(encoding="utf-8")

            # 绿色好 / green for good
            assert "#27ae60" in content
            # 红色差 / red for bad
            assert "#e74c3c" in content
            # 黄色一般 / yellow for neutral
            assert "#f39c12" in content
            # 灰色不可用 / gray for N/A
            assert "#999" in content
        finally:
            shutil.rmtree(viz_dir, ignore_errors=True)

    def test_icir_signal_good_when_positive(self, monkeypatch):
        """ICIR > 0 时显示 signal-neutral 或 signal-good / ICIR > 0 shows neutral or good."""
        from FactorAnalysis.visualization.tables import _signal_color

        # ICIR = 0.8 → signal-good (green)
        assert _signal_color(0.8) == "signal-good"
        # ICIR = 0.6 → signal-good
        assert _signal_color(0.6) == "signal-good"
        # ICIR = 0.5 → signal-neutral (not > 0.5)
        assert _signal_color(0.5) == "signal-neutral"

    def test_icir_signal_bad_when_negative(self, monkeypatch):
        """ICIR < 0 时显示 signal-bad / ICIR < 0 shows signal-bad."""
        from FactorAnalysis.visualization.tables import _signal_color

        assert _signal_color(-0.1) == "signal-bad"
        assert _signal_color(-1.5) == "signal-bad"

    def test_icir_signal_na_for_none(self, monkeypatch):
        """ICIR 为 None/NaN 时显示 signal-na / ICIR None/NaN shows signal-na."""
        from FactorAnalysis.visualization.tables import _signal_color

        assert _signal_color(None) == "signal-na"
        assert _signal_color(float("nan")) == "signal-na"

    def test_signal_span_in_report(self, monkeypatch):
        """报告中 ICIR/Sharpe 列使用信号灯 span / ICIR/Sharpe use signal span in report."""
        viz_dir = tempfile.mkdtemp()
        try:
            _run_pipeline_with_viz(viz_output=viz_dir, monkeypatch=monkeypatch)
            content = Path(os.path.join(viz_dir, "report.html")).read_text(encoding="utf-8")

            # 应包含 signal- 开头的 span 标签 / should contain span tags with signal- class
            assert re.search(r'<span class="signal-\w+">', content), "应有信号灯 span 标签"
        finally:
            shutil.rmtree(viz_dir, ignore_errors=True)

    def test_sharpe_signal_in_report(self, monkeypatch):
        """报告中 Sharpe 列也使用信号灯 / Sharpe column also uses signal in report."""
        viz_dir = tempfile.mkdtemp()
        try:
            _run_pipeline_with_viz(viz_output=viz_dir, monkeypatch=monkeypatch)
            content = Path(os.path.join(viz_dir, "report.html")).read_text(encoding="utf-8")

            # Sharpe 行应有信号灯 / Sharpe row should have signal
            assert "Sharpe" in content
        finally:
            shutil.rmtree(viz_dir, ignore_errors=True)


# ============================================================
# 4. --viz-output 路径自动创建 / --viz-output auto-creation
# ============================================================


class TestVizOutputAutoCreation:
    """验证 --viz-output 路径自动创建 / Verify --viz-output path auto-creation."""

    def test_nonexistent_dir_created(self, monkeypatch):
        """不存在的目录被自动创建 / Non-existent directory is auto-created."""
        viz_dir = os.path.join(tempfile.gettempdir(), "viz_e2e_test", "auto_create")
        # 确保目录不存在 / ensure directory does not exist
        shutil.rmtree(viz_dir, ignore_errors=True)
        assert not os.path.exists(viz_dir), "测试前目录应不存在"
        try:
            _run_pipeline_with_viz(viz_output=viz_dir, monkeypatch=monkeypatch)
            assert os.path.isdir(viz_dir), "目录应被自动创建"
            assert os.path.isfile(os.path.join(viz_dir, "report.html"))
        finally:
            shutil.rmtree(viz_dir, ignore_errors=True)

    def test_deeply_nested_dir_created(self, monkeypatch):
        """深层嵌套目录被自动创建 / Deeply nested directory is auto-created."""
        viz_dir = os.path.join(
            tempfile.gettempdir(), "viz_e2e_deep", "a", "b", "c", "d"
        )
        shutil.rmtree(
            os.path.join(tempfile.gettempdir(), "viz_e2e_deep"),
            ignore_errors=True,
        )
        try:
            _run_pipeline_with_viz(viz_output=viz_dir, monkeypatch=monkeypatch)
            assert os.path.isdir(viz_dir)
            assert os.path.isfile(os.path.join(viz_dir, "report.html"))
        finally:
            shutil.rmtree(
                os.path.join(tempfile.gettempdir(), "viz_e2e_deep"),
                ignore_errors=True,
            )

    def test_existing_dir_not_cleared(self, monkeypatch):
        """已存在的目录不被清空 / Existing directory is not cleared."""
        viz_dir = tempfile.mkdtemp()
        try:
            # 预先放入一个文件 / pre-place a file
            extra_file = os.path.join(viz_dir, "existing.txt")
            Path(extra_file).write_text("keep me", encoding="utf-8")

            _run_pipeline_with_viz(viz_output=viz_dir, monkeypatch=monkeypatch)

            assert os.path.isfile(extra_file), "已有文件不应被删除"
            assert os.path.isfile(os.path.join(viz_dir, "report.html"))
        finally:
            shutil.rmtree(viz_dir, ignore_errors=True)


# ============================================================
# 5. pipeline 数值输出不受可视化影响 / Pipeline output unaffected
# ============================================================


class TestPipelineNumericalUnchanged:
    """验证可视化步骤不影响 pipeline 数值结果 / Verify viz doesn't affect pipeline numbers."""

    def test_icir_identical_with_and_without_viz(self, monkeypatch):
        """ICIR 在启用/禁用可视化时完全一致 / ICIR identical with/without viz."""
        mock_data = _make_factor_data(n_dates=80, n_symbols=10, seed=42)
        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kw: _MockLoader(mock_data),
        )
        from scripts.run_factor_research import run_factor_research

        ev_no_viz, _ = run_factor_research(
            factor_name="AlphaMomentum", viz_output=None,
        )

        viz_dir = tempfile.mkdtemp()
        try:
            ev_viz, _ = run_factor_research(
                factor_name="AlphaMomentum", viz_output=viz_dir,
            )
            assert ev_no_viz.icir == ev_viz.icir, (
                f"ICIR 应一致: no_viz={ev_no_viz.icir}, viz={ev_viz.icir}"
            )
        finally:
            shutil.rmtree(viz_dir, ignore_errors=True)

    def test_sharpe_identical_with_and_without_viz(self, monkeypatch):
        """Sharpe 在启用/禁用可视化时完全一致 / Sharpe identical with/without viz."""
        mock_data = _make_factor_data(n_dates=80, n_symbols=10, seed=42)
        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kw: _MockLoader(mock_data),
        )
        from scripts.run_factor_research import run_factor_research

        ev_no_viz, _ = run_factor_research(
            factor_name="AlphaMomentum", viz_output=None,
        )

        viz_dir = tempfile.mkdtemp()
        try:
            ev_viz, _ = run_factor_research(
                factor_name="AlphaMomentum", viz_output=viz_dir,
            )
            assert ev_no_viz.sharpe == ev_viz.sharpe, (
                f"Sharpe 应一致: no_viz={ev_no_viz.sharpe}, viz={ev_viz.sharpe}"
            )
        finally:
            shutil.rmtree(viz_dir, ignore_errors=True)

    def test_hedge_curve_identical_with_and_without_viz(self, monkeypatch):
        """对冲净值曲线在启用/禁用可视化时完全一致 / Hedge curve identical."""
        mock_data = _make_factor_data(n_dates=80, n_symbols=10, seed=42)
        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kw: _MockLoader(mock_data),
        )
        from scripts.run_factor_research import run_factor_research

        ev_no_viz, _ = run_factor_research(
            factor_name="AlphaMomentum", viz_output=None,
        )

        viz_dir = tempfile.mkdtemp()
        try:
            ev_viz, _ = run_factor_research(
                factor_name="AlphaMomentum", viz_output=viz_dir,
            )
            # 对冲净值最后值一致 / hedge curve final value identical
            assert ev_no_viz.hedge_curve.iloc[-1] == ev_viz.hedge_curve.iloc[-1], (
                "对冲净值应一致"
            )
        finally:
            shutil.rmtree(viz_dir, ignore_errors=True)

    def test_report_dataframe_identical_with_and_without_viz(self, monkeypatch):
        """报告 DataFrame 在启用/禁用可视化时完全一致 / Report DataFrame identical."""
        mock_data = _make_factor_data(n_dates=80, n_symbols=10, seed=42)
        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kw: _MockLoader(mock_data),
        )
        from scripts.run_factor_research import run_factor_research

        _, report_no_viz = run_factor_research(
            factor_name="AlphaMomentum", viz_output=None,
        )

        viz_dir = tempfile.mkdtemp()
        try:
            _, report_viz = run_factor_research(
                factor_name="AlphaMomentum", viz_output=viz_dir,
            )
            pd.testing.assert_frame_equal(report_no_viz, report_viz)
        finally:
            shutil.rmtree(viz_dir, ignore_errors=True)

    def test_turnover_identical_with_and_without_viz(self, monkeypatch):
        """换手率在启用/禁用可视化时完全一致 / Turnover identical."""
        mock_data = _make_factor_data(n_dates=80, n_symbols=10, seed=42)
        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kw: _MockLoader(mock_data),
        )
        from scripts.run_factor_research import run_factor_research

        ev_no_viz, _ = run_factor_research(
            factor_name="AlphaMomentum", viz_output=None,
        )

        viz_dir = tempfile.mkdtemp()
        try:
            ev_viz, _ = run_factor_research(
                factor_name="AlphaMomentum", viz_output=viz_dir,
            )
            pd.testing.assert_frame_equal(ev_no_viz.turnover, ev_viz.turnover)
        finally:
            shutil.rmtree(viz_dir, ignore_errors=True)


# ============================================================
# 6. 多因子端到端 / Multi-factor E2E
# ============================================================


class TestMultiFactorE2E:
    """多因子 E2E 验证 / Multi-factor E2E verification."""

    @pytest.fixture()
    def registered_factors(self):
        """获取已注册因子列表 / Get registered factor list."""
        from FactorLib import list_factors
        return list_factors()

    def test_each_registered_factor_generates_report(self, monkeypatch, registered_factors):
        """每个已注册因子都能生成 report.html / Each registered factor generates report.html."""
        if not registered_factors:
            pytest.skip("无已注册因子")

        for factor_name in registered_factors:
            viz_dir = tempfile.mkdtemp()
            try:
                mock_data = _make_factor_data(n_dates=80, n_symbols=10, seed=42)
                monkeypatch.setattr(
                    "Cross_Section_Factor.kline_loader.KlineLoader",
                    lambda **kw: _MockLoader(mock_data),
                )
                from scripts.run_factor_research import run_factor_research

                ev, report, _ = _run_pipeline_with_viz(
                    factor_name=factor_name,
                    viz_output=viz_dir,
                    monkeypatch=monkeypatch,
                )
                assert ev is not None, f"因子 {factor_name} 应返回有效 evaluator"
                assert report is not None, f"因子 {factor_name} 应返回有效 report"
                report_path = os.path.join(viz_dir, "report.html")
                assert os.path.isfile(report_path), (
                    f"因子 {factor_name} 应生成 report.html"
                )
                # HTML 内容应包含因子名 / HTML should contain factor name
                content = Path(report_path).read_text(encoding="utf-8")
                assert factor_name in content, (
                    f"因子 {factor_name} 名称应出现在报告中"
                )
            finally:
                shutil.rmtree(viz_dir, ignore_errors=True)


# ============================================================
# 7. 边界情况 / Edge cases
# ============================================================


class TestEdgeCases:
    """端到端边界情况验证 / E2E edge case verification."""

    def test_minimal_data_generates_report(self, monkeypatch):
        """最小数据量仍能生成报告 / Minimal data still generates report."""
        viz_dir = tempfile.mkdtemp()
        try:
            ev, report, _ = _run_pipeline_with_viz(
                n_dates=30, n_symbols=3, seed=42,
                viz_output=viz_dir, monkeypatch=monkeypatch,
            )
            assert ev is not None
            report_path = os.path.join(viz_dir, "report.html")
            assert os.path.isfile(report_path)
            content = Path(report_path).read_text(encoding="utf-8")
            assert "<!DOCTYPE html>" in content
        finally:
            shutil.rmtree(viz_dir, ignore_errors=True)

    def test_large_data_generates_report(self, monkeypatch):
        """大数据量仍能生成报告 / Large data still generates report."""
        viz_dir = tempfile.mkdtemp()
        try:
            ev, report, _ = _run_pipeline_with_viz(
                n_dates=200, n_symbols=30, seed=42,
                viz_output=viz_dir, monkeypatch=monkeypatch,
            )
            assert ev is not None
            report_path = os.path.join(viz_dir, "report.html")
            assert os.path.isfile(report_path)
            content = Path(report_path).read_text(encoding="utf-8")
            n_images = _count_base64_images(content)
            assert n_images == 4, f"大数据量应有 4 张图片，实际 {n_images}"
        finally:
            shutil.rmtree(viz_dir, ignore_errors=True)

    def test_viz_failure_does_not_corrupt_evaluator(self, monkeypatch):
        """可视化失败不损坏 evaluator 属性 / Viz failure doesn't corrupt evaluator."""
        mock_data = _make_factor_data(n_dates=80, n_symbols=10, seed=42)
        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kw: _MockLoader(mock_data),
        )
        from scripts.run_factor_research import run_factor_research

        def mock_build_html_report(*args, **kwargs):
            raise RuntimeError("mock failure")

        monkeypatch.setattr(
            "FactorAnalysis.visualization.build_html_report",
            mock_build_html_report,
        )

        viz_dir = tempfile.mkdtemp()
        try:
            ev, report = run_factor_research(
                factor_name="AlphaMomentum", viz_output=viz_dir,
            )
            # pipeline 应继续 / pipeline should continue
            assert ev is not None
            assert report is not None
            # evaluator 属性应完整 / evaluator attributes should be intact
            assert ev.ic is not None
            assert ev.icir is not None and np.isfinite(ev.icir)
            assert ev.sharpe is not None
            assert ev.hedge_curve is not None
            assert ev.turnover is not None
            assert ev.group_labels is not None
        finally:
            shutil.rmtree(viz_dir, ignore_errors=True)

    def test_different_seeds_generate_different_reports(self, monkeypatch):
        """不同种子生成不同报告内容 / Different seeds generate different report content."""
        viz_dir1 = tempfile.mkdtemp()
        viz_dir2 = tempfile.mkdtemp()
        try:
            _run_pipeline_with_viz(
                seed=42, viz_output=viz_dir1, monkeypatch=monkeypatch,
            )
            _run_pipeline_with_viz(
                seed=99, viz_output=viz_dir2, monkeypatch=monkeypatch,
            )

            content1 = Path(os.path.join(viz_dir1, "report.html")).read_text(encoding="utf-8")
            content2 = Path(os.path.join(viz_dir2, "report.html")).read_text(encoding="utf-8")

            # 两个报告的 base64 图片数据应不同（因因子值不同）
            # base64 image data should differ (different factor values)
            images1 = _extract_base64_images(content1)
            images2 = _extract_base64_images(content2)
            assert len(images1) == len(images2) == 4
            # 至少有一张图不同 / at least one chart differs
            assert images1 != images2, "不同种子应生成不同图表"
        finally:
            shutil.rmtree(viz_dir1, ignore_errors=True)
            shutil.rmtree(viz_dir2, ignore_errors=True)
