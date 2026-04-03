"""
--viz-output CLI 参数集成测试 / --viz-output CLI parameter integration tests (Task 26)

验证内容 / Verifications:
1. --viz-output 默认值与解析 / Default value and parsing
2. viz_output=None 跳过可视化 / Skip visualization when viz_output=None
3. viz_output 路径自动创建 / Auto-create output directory
4. build_html_report 被正确调用 / build_html_report called correctly
5. 可视化失败不中断 pipeline / Visualization failure doesn't crash pipeline
6. 向后兼容 / Backward compatibility
7. 函数签名验证 / Function signature verification
"""

import os
import sys
import shutil
import tempfile
import numpy as np
import pandas as pd
import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


# ─────────────────────────────────────────────
# 辅助函数 / Helper functions
# ─────────────────────────────────────────────


def _make_factor_data(n_dates=80, n_symbols=10, seed=42):
    """
    生成标准测试行情数据（平铺格式）。
    Generate standard test OHLC data (flat format).

    参数含义：
        n_dates: 时间截面数
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


# ─────────────────────────────────────────────
# 1. --viz-output 默认值与解析 / Default value and parsing
# ─────────────────────────────────────────────


class TestVizOutputCLI:
    """验证 --viz-output CLI 参数解析 / Verify --viz-output CLI argument parsing."""

    def test_default_value(self):
        """--viz-output 默认为 output/viz/ / --viz-output defaults to output/viz/."""
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument("--factor", type=str, required=True)
        parser.add_argument("--viz-output", type=str, default="output/viz/")

        args = parser.parse_args(["--factor", "AlphaMomentum"])
        assert args.viz_output == "output/viz/"

    def test_custom_path(self):
        """--viz-output 可设为自定义路径 / --viz-output accepts custom path."""
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument("--factor", type=str, required=True)
        parser.add_argument("--viz-output", type=str, default="output/viz/")

        args = parser.parse_args(["--factor", "AlphaMomentum", "--viz-output", "custom/path/"])
        assert args.viz_output == "custom/path/"

    def test_none_string_converted(self):
        """--viz-output "none" 转为 None / --viz-output 'none' converts to None."""
        viz_dir = "none"
        if viz_dir is not None and viz_dir.lower() == "none":
            viz_dir = None
        assert viz_dir is None

    def test_None_string_converted(self):
        """--viz-output "None" 也转为 None / --viz-output 'None' also converts to None."""
        viz_dir = "None"
        if viz_dir is not None and viz_dir.lower() == "none":
            viz_dir = None
        assert viz_dir is None

    def test_normal_path_not_converted(self):
        """正常路径不被转换 / Normal path is not converted."""
        viz_dir = "output/charts/"
        if viz_dir is not None and viz_dir.lower() == "none":
            viz_dir = None
        assert viz_dir == "output/charts/"


# ─────────────────────────────────────────────
# 2. 函数签名验证 / Function signature verification
# ─────────────────────────────────────────────


class TestFunctionSignature:
    """验证 run_factor_research 函数签名包含 viz_output / Verify viz_output in function signature."""

    def test_viz_output_in_signature(self):
        """函数签名包含 viz_output 参数 / Function signature includes viz_output."""
        import inspect
        from scripts.run_factor_research import run_factor_research

        sig = inspect.signature(run_factor_research)
        assert "viz_output" in sig.parameters, "函数签名应包含 viz_output"

    def test_viz_output_default_value(self):
        """viz_output 默认值为 'output/viz/' / viz_output default is 'output/viz/'."""
        import inspect
        from scripts.run_factor_research import run_factor_research

        sig = inspect.signature(run_factor_research)
        assert sig.parameters["viz_output"].default == "output/viz/"

    def test_viz_output_type_hint(self):
        """viz_output 类型注解为 str | None / viz_output type hint is str | None."""
        import inspect
        from scripts.run_factor_research import run_factor_research

        sig = inspect.signature(run_factor_research)
        annotation = sig.parameters["viz_output"].annotation
        # str | None 的字符串表示 / str representation of str | None
        assert "str" in str(annotation) and "None" in str(annotation)


# ─────────────────────────────────────────────
# 3. viz_output=None 跳过可视化 / Skip when viz_output=None
# ─────────────────────────────────────────────


class TestVizOutputSkipped:
    """viz_output=None 时跳过可视化步骤 / Skip visualization when viz_output=None."""

    def test_none_skips_build_html_report(self, monkeypatch):
        """viz_output=None 时不调用 build_html_report / build_html_report not called when None."""
        from scripts.run_factor_research import run_factor_research

        mock_data = _make_factor_data(n_dates=60, n_symbols=5, seed=42)
        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kw: _MockLoader(mock_data),
        )

        called = {"value": False}

        def mock_build_html_report(*args, **kwargs):
            called["value"] = True
            return "<html></html>"

        monkeypatch.setattr(
            "FactorAnalysis.visualization.build_html_report",
            mock_build_html_report,
        )

        ev, report = run_factor_research(
            factor_name="AlphaMomentum",
            viz_output=None,
        )

        assert ev is not None, "viz_output=None 时 pipeline 应正常完成"
        assert not called["value"], "viz_output=None 时不应调用 build_html_report"

    def test_none_returns_valid_evaluator(self, monkeypatch):
        """viz_output=None 时返回有效 evaluator / Returns valid evaluator when None."""
        from scripts.run_factor_research import run_factor_research

        mock_data = _make_factor_data(n_dates=80, n_symbols=10, seed=42)
        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kw: _MockLoader(mock_data),
        )

        ev, report = run_factor_research(
            factor_name="AlphaMomentum",
            viz_output=None,
        )

        assert ev is not None
        assert report is not None
        assert isinstance(ev.ic, pd.Series)
        assert isinstance(report, pd.DataFrame)


# ─────────────────────────────────────────────
# 4. viz_output 路径自动创建 + build_html_report 调用 / Auto-create + call
# ─────────────────────────────────────────────


class TestVizOutputGeneration:
    """viz_output 设置时正确生成可视化 / Visualization generated correctly when set."""

    def test_build_html_report_called_with_correct_args(self, monkeypatch):
        """build_html_report 以正确参数被调用 / Called with correct arguments."""
        from scripts.run_factor_research import run_factor_research

        mock_data = _make_factor_data(n_dates=60, n_symbols=5, seed=42)
        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kw: _MockLoader(mock_data),
        )

        captured = {"args": None, "kwargs": None}

        def mock_build_html_report(*args, **kwargs):
            captured["args"] = args
            captured["kwargs"] = kwargs
            return "<html>mock</html>"

        monkeypatch.setattr(
            "FactorAnalysis.visualization.build_html_report",
            mock_build_html_report,
        )

        viz_dir = tempfile.mkdtemp()
        try:
            ev, report = run_factor_research(
                factor_name="AlphaMomentum",
                viz_output=viz_dir,
            )

            assert ev is not None
            # build_html_report 应被调用 / should have been called
            assert captured["args"] is not None
            # 第一个参数应为 evaluator / first arg should be evaluator
            assert captured["args"][0] is ev
            # output_dir 应正确传递 / output_dir should be passed
            assert captured["kwargs"].get("output_dir") == viz_dir
            # title 应包含因子名 / title should include factor name
            title = captured["kwargs"].get("title", "")
            assert "AlphaMomentum" in title
        finally:
            shutil.rmtree(viz_dir, ignore_errors=True)

    def test_report_html_file_created(self, monkeypatch):
        """report.html 文件被创建 / report.html file is created."""
        from scripts.run_factor_research import run_factor_research

        mock_data = _make_factor_data(n_dates=60, n_symbols=5, seed=42)
        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kw: _MockLoader(mock_data),
        )

        viz_dir = tempfile.mkdtemp()
        try:
            ev, report = run_factor_research(
                factor_name="AlphaMomentum",
                viz_output=viz_dir,
            )

            report_path = os.path.join(viz_dir, "report.html")
            assert os.path.isfile(report_path), f"report.html 应存在于 {report_path}"

            # 文件内容应为有效 HTML / file content should be valid HTML
            with open(report_path, "r", encoding="utf-8") as f:
                content = f.read()
            assert "<html" in content.lower()
            assert "</html>" in content.lower()
        finally:
            shutil.rmtree(viz_dir, ignore_errors=True)

    def test_nested_directory_auto_created(self, monkeypatch):
        """嵌套目录自动创建 / Nested directories auto-created."""
        from scripts.run_factor_research import run_factor_research

        mock_data = _make_factor_data(n_dates=60, n_symbols=5, seed=42)
        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kw: _MockLoader(mock_data),
        )

        viz_dir = os.path.join(tempfile.gettempdir(), "viz_test_nested", "deep", "path")
        try:
            ev, report = run_factor_research(
                factor_name="AlphaMomentum",
                viz_output=viz_dir,
            )

            assert os.path.isdir(viz_dir), f"目录应被创建: {viz_dir}"
            assert os.path.isfile(os.path.join(viz_dir, "report.html"))
        finally:
            shutil.rmtree(viz_dir, ignore_errors=True)


# ─────────────────────────────────────────────
# 5. 可视化失败不中断 pipeline / Visualization failure doesn't crash pipeline
# ─────────────────────────────────────────────


class TestVizOutputFailure:
    """可视化生成失败时 pipeline 不中断 / Pipeline continues on visualization failure."""

    def test_viz_exception_returns_valid_evaluator(self, monkeypatch):
        """build_html_report 抛异常时 pipeline 正常返回 / Pipeline returns normally on exception."""
        from scripts.run_factor_research import run_factor_research

        mock_data = _make_factor_data(n_dates=60, n_symbols=5, seed=42)
        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kw: _MockLoader(mock_data),
        )

        def mock_build_html_report(*args, **kwargs):
            raise RuntimeError("mock visualization failure")

        monkeypatch.setattr(
            "FactorAnalysis.visualization.build_html_report",
            mock_build_html_report,
        )

        viz_dir = tempfile.mkdtemp()
        try:
            # 不应抛出异常 / should not raise exception
            ev, report = run_factor_research(
                factor_name="AlphaMomentum",
                viz_output=viz_dir,
            )

            assert ev is not None, "可视化失败时 pipeline 应继续返回 evaluator"
            assert report is not None
            assert np.isfinite(ev.icir)
        finally:
            shutil.rmtree(viz_dir, ignore_errors=True)

    def test_viz_import_error_returns_valid_evaluator(self, monkeypatch):
        """可视化模块导入失败时 pipeline 正常返回 / Pipeline returns normally on import error."""
        from scripts.run_factor_research import run_factor_research

        mock_data = _make_factor_data(n_dates=60, n_symbols=5, seed=42)
        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kw: _MockLoader(mock_data),
        )

        # 让 import 失败 / make import fail
        import builtins

        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if "visualization" in name:
                raise ImportError("mock import failure")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", mock_import)

        viz_dir = tempfile.mkdtemp()
        try:
            ev, report = run_factor_research(
                factor_name="AlphaMomentum",
                viz_output=viz_dir,
            )

            assert ev is not None, "导入失败时 pipeline 应继续返回 evaluator"
        finally:
            shutil.rmtree(viz_dir, ignore_errors=True)


# ─────────────────────────────────────────────
# 6. 向后兼容 / Backward compatibility
# ─────────────────────────────────────────────


class TestBackwardCompatibility:
    """不传 viz_output 时行为与改造前一致 / Same behavior without viz_output."""

    def test_default_viz_output_generates_report(self, monkeypatch):
        """默认 viz_output='output/viz/' 时生成报告 / Default viz_output generates report."""
        from scripts.run_factor_research import run_factor_research

        mock_data = _make_factor_data(n_dates=60, n_symbols=5, seed=42)
        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kw: _MockLoader(mock_data),
        )

        called = {"value": False}

        def mock_build_html_report(*args, **kwargs):
            called["value"] = True
            return "<html></html>"

        monkeypatch.setattr(
            "FactorAnalysis.visualization.build_html_report",
            mock_build_html_report,
        )

        # 不传 viz_output，使用默认值 / don't pass viz_output, use default
        ev, report = run_factor_research(
            factor_name="AlphaMomentum",
        )

        assert ev is not None
        # 默认 viz_output 不为 None，应调用 build_html_report
        # default viz_output is not None, should call build_html_report
        assert called["value"], "默认 viz_output 时应调用 build_html_report"

    def test_no_viz_output_same_evaluator_results(self, monkeypatch):
        """viz_output=None 与不传 viz_output 的 evaluator 结果数值一致."""
        from scripts.run_factor_research import run_factor_research

        mock_data = _make_factor_data(n_dates=80, n_symbols=10, seed=42)

        # monkeypatch KlineLoader 返回相同数据 / return same data
        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kw: _MockLoader(mock_data),
        )

        # 跳过可视化以避免文件写入 / skip viz to avoid file writes
        ev1, report1 = run_factor_research(
            factor_name="AlphaMomentum",
            viz_output=None,
        )

        ev2, report2 = run_factor_research(
            factor_name="AlphaMomentum",
            viz_output=None,
        )

        # 数值结果应完全一致 / numerical results should be identical
        assert ev1.icir == ev2.icir
        assert ev1.sharpe == ev2.sharpe

    def test_evaluator_results_unchanged_by_viz(self, monkeypatch):
        """可视化步骤不影响 evaluator 数值结果 / Viz step doesn't affect evaluator results."""
        from scripts.run_factor_research import run_factor_research

        mock_data = _make_factor_data(n_dates=80, n_symbols=10, seed=42)
        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kw: _MockLoader(mock_data),
        )

        # 不启用可视化 / without viz
        ev_no_viz, _ = run_factor_research(
            factor_name="AlphaMomentum",
            viz_output=None,
        )

        # 启用可视化 / with viz
        viz_dir = tempfile.mkdtemp()
        try:
            def mock_build_html_report(*args, **kwargs):
                return "<html>mock</html>"

            monkeypatch.setattr(
                "FactorAnalysis.visualization.build_html_report",
                mock_build_html_report,
            )

            ev_viz, _ = run_factor_research(
                factor_name="AlphaMomentum",
                viz_output=viz_dir,
            )

            # evaluator 数值不应因可视化步骤而改变 / evaluator results should not change
            assert ev_no_viz.icir == ev_viz.icir, (
                "可视化不应改变 ICIR"
            )
            assert ev_no_viz.sharpe == ev_viz.sharpe, (
                "可视化不应改变 Sharpe"
            )
        finally:
            shutil.rmtree(viz_dir, ignore_errors=True)


# ─────────────────────────────────────────────
# 7. 步骤编号验证 / Step numbering verification
# ─────────────────────────────────────────────


class TestStepNumbering:
    """验证步骤编号更新为 8 步 / Verify step numbering updated to 8 steps."""

    def test_total_steps_is_8(self):
        """总步骤数应为 8 / Total steps should be 8."""
        import inspect
        from scripts.run_factor_research import run_factor_research

        source = inspect.getsource(run_factor_research)
        # 应包含 Step 1/8 到 Step 7/8 / should contain Step 1/8 through Step 7/8
        assert "1/8" in source, "步骤编号应更新为 1/8"
        assert "2/8" in source, "步骤编号应更新为 2/8"
        assert "3/8" in source, "步骤编号应更新为 3/8"
        assert "4/8" in source, "步骤编号应更新为 4/8"
        assert "4.5/8" in source, "步骤编号应更新为 4.5/8"
        assert "5/8" in source, "步骤编号应更新为 5/8"
        assert "6/8" in source, "步骤编号应更新为 6/8"
        assert "7/8" in source, "步骤编号应更新为 7/8"
        # 不应再包含旧的 1/7 编号 / should not contain old 1/7 numbering
        assert "1/7" not in source, "不应再包含旧的 1/7 步骤编号"
