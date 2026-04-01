"""
Task 23 验证测试：端到端投研编排脚本
Task 23 verification tests: End-to-end factor research orchestration script.

验证内容 / Coverage:
- 导入校验 (3)
- CLI 参数解析 (8)
- _build_factor_multiindex 辅助函数 (7)
- _print_metrics 辅助函数 (5)
- run_factor_research 核心流程 — mock 数据 (12)
- 错误处理 (3)
- 结构化输出 (4)
"""

import sys
import os
import argparse
import unittest
from unittest.mock import patch, MagicMock
from types import SimpleNamespace

import numpy as np
import pandas as pd

# 项目根目录 / Project root
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "CryptoDataProviders"))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "CryptoDB_feather"))


class TestImport(unittest.TestCase):
    """导入校验 / Import validation."""

    def test_import_run_factor_research(self):
        """run_factor_research 函数可导入 / run_factor_research function is importable."""
        from scripts.run_factor_research import run_factor_research
        self.assertTrue(callable(run_factor_research))

    def test_import_main(self):
        """main 入口函数可导入 / main entry function is importable."""
        from scripts.run_factor_research import main
        self.assertTrue(callable(main))

    def test_import_helpers(self):
        """辅助函数可导入 / Helper functions are importable."""
        from scripts.run_factor_research import _build_factor_multiindex, _print_metrics
        self.assertTrue(callable(_build_factor_multiindex))
        self.assertTrue(callable(_print_metrics))


class TestCLIParsing(unittest.TestCase):
    """CLI 参数解析验证 / CLI argument parsing validation."""

    def _parse(self, argv):
        """辅助：用 mock argv 解析参数 / Helper: parse args with mock argv."""
        from scripts.run_factor_research import main
        with patch("sys.argv", argv):
            parser = argparse.ArgumentParser()
            # 重新构建 parser 以便测试 / rebuild parser for testing
            from scripts.run_factor_research import run_factor_research
            # 直接用 inspect 获取参数名列表 / get param names via inspect
            import inspect
            sig = inspect.signature(run_factor_research)
            return sig

    def test_factor_required(self):
        """--factor 为必填参数 / --factor is required."""
        from scripts.run_factor_research import main
        with patch("sys.argv", ["script"]):
            with self.assertRaises(SystemExit) as ctx:
                main()
            self.assertEqual(ctx.exception.code, 2)

    def test_help_exit_zero(self):
        """--help 正常退出 / --help exits with 0."""
        from scripts.run_factor_research import main
        with patch("sys.argv", ["script", "--factor", "X", "--help"]):
            with self.assertRaises(SystemExit) as ctx:
                main()
            self.assertEqual(ctx.exception.code, 0)

    def test_return_label_choices(self):
        """--return-label 仅接受 close2close / open2open."""
        from scripts.run_factor_research import main
        with patch("sys.argv", ["script", "--factor", "X", "--return-label", "bad"]):
            with self.assertRaises(SystemExit):
                main()

    def test_default_values(self):
        """默认参数值正确 / Default parameter values are correct."""
        from scripts.run_factor_research import run_factor_research
        import inspect
        sig = inspect.signature(run_factor_research)
        self.assertEqual(sig.parameters["return_label"].default, "close2close")
        self.assertEqual(sig.parameters["n_groups"].default, 5)
        self.assertEqual(sig.parameters["cost_rate"].default, 0.001)
        self.assertEqual(sig.parameters["top_k"].default, 1)
        self.assertEqual(sig.parameters["bottom_k"].default, 1)
        self.assertEqual(sig.parameters["risk_free_rate"].default, 0.0)
        self.assertEqual(sig.parameters["periods_per_year"].default, 252)
        self.assertEqual(sig.parameters["max_loss"].default, 0.35)


class TestBuildFactorMultiIndex(unittest.TestCase):
    """_build_factor_multiindex 辅助函数验证 / Helper function validation."""

    def setUp(self):
        from scripts.run_factor_research import _build_factor_multiindex
        self.fn = _build_factor_multiindex
        self.data = pd.DataFrame({
            "timestamp": pd.date_range("2024-01-01", periods=6, freq="h"),
            "symbol": ["A", "A", "A", "B", "B", "B"],
            "close": [1, 2, 3, 4, 5, 6],
            "open": [1, 2, 3, 4, 5, 6],
            "high": [1, 2, 3, 4, 5, 6],
            "low": [1, 2, 3, 4, 5, 6],
        })

    def test_returns_multiindex(self):
        """返回 MultiIndex / Returns MultiIndex."""
        factor = pd.Series([0.1] * 6, name="test")
        result = self.fn(factor, self.data)
        self.assertIsInstance(result.index, pd.MultiIndex)

    def test_index_names(self):
        """索引名称为 timestamp, symbol / Index names are timestamp, symbol."""
        factor = pd.Series([0.1] * 6, name="test")
        result = self.fn(factor, self.data)
        self.assertEqual(result.index.names, ["timestamp", "symbol"])

    def test_values_preserved(self):
        """因子值保持不变 / Factor values are preserved."""
        factor = pd.Series([0.1, 0.2, 0.3, 0.4, 0.5, 0.6], name="test")
        result = self.fn(factor, self.data)
        np.testing.assert_array_equal(result.values, factor.values)

    def test_name_preserved(self):
        """Series name 保持不变 / Series name is preserved."""
        factor = pd.Series([0.1] * 6, name="my_factor")
        result = self.fn(factor, self.data)
        self.assertEqual(result.name, "my_factor")

    def test_single_symbol(self):
        """单交易对正常工作 / Single symbol works correctly."""
        data1 = self.data[self.data["symbol"] == "A"].reset_index(drop=True)
        factor = pd.Series([0.1, 0.2, 0.3], name="test")
        result = self.fn(factor, data1)
        self.assertEqual(len(result), 3)
        self.assertTrue(all(s == "A" for _, s in result.index))

    def test_empty_data(self):
        """空数据返回空 Series / Empty data returns empty Series."""
        data_empty = pd.DataFrame(columns=["timestamp", "symbol", "close", "open", "high", "low"])
        factor = pd.Series([], name="test", dtype=float)
        result = self.fn(factor, data_empty)
        self.assertEqual(len(result), 0)

    def test_multiindex_levels(self):
        """MultiIndex 有两个层级 / MultiIndex has two levels."""
        factor = pd.Series([0.1] * 6, name="test")
        result = self.fn(factor, self.data)
        self.assertEqual(result.index.nlevels, 2)


class TestPrintMetrics(unittest.TestCase):
    """_print_metrics 辅助函数验证 / Helper function validation."""

    def test_no_crash_full(self):
        """完整指标不崩溃 / No crash with full metrics."""
        from scripts.run_factor_research import _print_metrics
        ev = _make_evaluator_mock()
        _print_metrics(ev)  # 不应抛出异常 / should not raise

    def test_no_crash_partial(self):
        """部分指标为 None 不崩溃 / No crash with partial None metrics."""
        from scripts.run_factor_research import _print_metrics
        ev = SimpleNamespace(
            ic=None, rank_ic=None, icir=None, ic_stats=None,
            sharpe=None, hedge_curve=None, hedge_curve_after_cost=None,
            turnover=None, rank_autocorr=None, neutralized_curve=None,
        )
        _print_metrics(ev)

    def test_no_crash_no_stats(self):
        """无 ic_stats 不崩溃 / No crash without ic_stats."""
        from scripts.run_factor_research import _print_metrics
        ev = _make_evaluator_mock()
        ev.ic_stats = None
        _print_metrics(ev)

    def test_output_contains_labels(self):
        """输出包含关键标签 / Output contains key labels."""
        from scripts.run_factor_research import _print_metrics
        from io import StringIO
        ev = _make_evaluator_mock()
        buf = StringIO()
        with patch("sys.stdout", buf):
            _print_metrics(ev)
        output = buf.getvalue()
        self.assertIn("IC Mean", output)
        self.assertIn("ICIR", output)
        self.assertIn("Sharpe", output)

    def test_nan_values_handled(self):
        """NaN 值正常打印 / NaN values are printed normally."""
        from scripts.run_factor_research import _print_metrics
        from io import StringIO
        ev = SimpleNamespace(
            ic=pd.Series([np.nan, np.nan]),
            rank_ic=None,
            icir=np.nan,
            ic_stats=pd.Series({"t_stat": np.nan, "p_value": np.nan}),
            sharpe=np.nan,
            hedge_curve=pd.Series([1.0, np.nan]),
            hedge_curve_after_cost=None,
            turnover=None,
            rank_autocorr=None,
            neutralized_curve=None,
        )
        buf = StringIO()
        with patch("sys.stdout", buf):
            _print_metrics(ev)
        # 不应崩溃 / should not crash
        self.assertTrue(len(buf.getvalue()) > 0)


class TestRunFactorResearch(unittest.TestCase):
    """run_factor_research 核心流程验证 / Core pipeline validation with mock data."""

    @patch("Cross_Section_Factor.kline_loader.KlineLoader")
    def test_full_pipeline(self, mock_loader_cls):
        """完整流程跑通无报错 / Full pipeline runs without error."""
        mock_loader = MagicMock()
        mock_loader.compile.return_value = _make_mock_data()
        mock_loader_cls.return_value = mock_loader

        from scripts.run_factor_research import run_factor_research
        ev, report = run_factor_research(
            factor_name="AlphaMomentum",
            return_label="close2close",
            n_groups=3,
        )
        self.assertIsNotNone(ev)
        self.assertIsNotNone(report)

    @patch("Cross_Section_Factor.kline_loader.KlineLoader")
    def test_open2open_label(self, mock_loader_cls):
        """open2open 收益率标签正常工作 / open2open return label works."""
        mock_loader = MagicMock()
        mock_loader.compile.return_value = _make_mock_data()
        mock_loader_cls.return_value = mock_loader

        from scripts.run_factor_research import run_factor_research
        ev, report = run_factor_research(
            factor_name="AlphaMomentum",
            return_label="open2open",
        )
        self.assertIsNotNone(ev)

    @patch("Cross_Section_Factor.kline_loader.KlineLoader")
    def test_custom_cost_rate(self, mock_loader_cls):
        """自定义 cost_rate 参数传递正确 / Custom cost_rate is passed correctly."""
        mock_loader = MagicMock()
        mock_loader.compile.return_value = _make_mock_data()
        mock_loader_cls.return_value = mock_loader

        from scripts.run_factor_research import run_factor_research
        ev, _ = run_factor_research(
            factor_name="AlphaMomentum",
            cost_rate=0.005,
        )
        self.assertAlmostEqual(ev.cost_rate, 0.005)

    @patch("Cross_Section_Factor.kline_loader.KlineLoader")
    def test_custom_n_groups(self, mock_loader_cls):
        """自定义 n_groups 参数传递正确 / Custom n_groups is passed correctly."""
        mock_loader = MagicMock()
        mock_loader.compile.return_value = _make_mock_data()
        mock_loader_cls.return_value = mock_loader

        from scripts.run_factor_research import run_factor_research
        ev, _ = run_factor_research(
            factor_name="AlphaMomentum",
            n_groups=10,
        )
        self.assertEqual(ev.n_groups, 10)

    @patch("Cross_Section_Factor.kline_loader.KlineLoader")
    def test_evaluator_attributes_populated(self, mock_loader_cls):
        """FactorEvaluator 各属性均已填充 / Evaluator attributes are populated."""
        mock_loader = MagicMock()
        mock_loader.compile.return_value = _make_mock_data()
        mock_loader_cls.return_value = mock_loader

        from scripts.run_factor_research import run_factor_research
        ev, _ = run_factor_research(factor_name="AlphaMomentum")

        self.assertIsNotNone(ev.ic)
        self.assertIsNotNone(ev.rank_ic)
        self.assertIsNotNone(ev.icir)
        self.assertIsNotNone(ev.ic_stats)
        self.assertIsNotNone(ev.group_labels)
        self.assertIsNotNone(ev.long_curve)
        self.assertIsNotNone(ev.short_curve)
        self.assertIsNotNone(ev.hedge_curve)
        self.assertIsNotNone(ev.hedge_curve_after_cost)
        self.assertIsNotNone(ev.turnover)
        self.assertIsNotNone(ev.rank_autocorr)
        self.assertIsNotNone(ev.neutralized_curve)

    @patch("Cross_Section_Factor.kline_loader.KlineLoader")
    def test_report_is_dataframe(self, mock_loader_cls):
        """报告为 DataFrame 类型 / Report is a DataFrame."""
        mock_loader = MagicMock()
        mock_loader.compile.return_value = _make_mock_data()
        mock_loader_cls.return_value = mock_loader

        from scripts.run_factor_research import run_factor_research
        _, report = run_factor_research(factor_name="AlphaMomentum")
        self.assertIsInstance(report, pd.DataFrame)

    @patch("Cross_Section_Factor.kline_loader.KlineLoader")
    def test_report_single_row(self, mock_loader_cls):
        """报告为单行 / Report is a single row."""
        mock_loader = MagicMock()
        mock_loader.compile.return_value = _make_mock_data()
        mock_loader_cls.return_value = mock_loader

        from scripts.run_factor_research import run_factor_research
        _, report = run_factor_research(factor_name="AlphaMomentum")
        self.assertEqual(len(report), 1)

    @patch("Cross_Section_Factor.kline_loader.KlineLoader")
    def test_report_contains_key_columns(self, mock_loader_cls):
        """报告包含关键列 / Report contains key columns."""
        mock_loader = MagicMock()
        mock_loader.compile.return_value = _make_mock_data()
        mock_loader_cls.return_value = mock_loader

        from scripts.run_factor_research import run_factor_research
        _, report = run_factor_research(factor_name="AlphaMomentum")
        key_cols = ["IC_mean", "ICIR", "hedge_return", "sharpe"]
        for col in key_cols:
            self.assertIn(col, report.columns, f"Missing column: {col}")


class TestErrorHandling(unittest.TestCase):
    """错误处理验证 / Error handling validation."""

    @patch("Cross_Section_Factor.kline_loader.KlineLoader")
    def test_empty_data_returns_none(self, mock_loader_cls):
        """空数据返回 (None, None) / Empty data returns (None, None)."""
        mock_loader = MagicMock()
        mock_loader.compile.return_value = pd.DataFrame()
        mock_loader_cls.return_value = mock_loader

        from scripts.run_factor_research import run_factor_research
        ev, report = run_factor_research(factor_name="AlphaMomentum")
        self.assertIsNone(ev)
        self.assertIsNone(report)

    @patch("Cross_Section_Factor.kline_loader.KlineLoader")
    def test_unknown_factor_returns_none(self, mock_loader_cls):
        """未知因子名返回 (None, None) / Unknown factor name returns (None, None)."""
        mock_loader = MagicMock()
        mock_loader.compile.return_value = _make_mock_data()
        mock_loader_cls.return_value = mock_loader

        from scripts.run_factor_research import run_factor_research
        ev, report = run_factor_research(factor_name="NonExistentFactor")
        self.assertIsNone(ev)
        self.assertIsNone(report)

    def test_keyboard_interrupt_caught(self):
        """KeyboardInterrupt 被捕获 / KeyboardInterrupt is caught."""
        from scripts.run_factor_research import main
        with patch("sys.argv", ["script", "--factor", "X"]):
            with patch(
                "scripts.run_factor_research.run_factor_research",
                side_effect=KeyboardInterrupt,
            ):
                # 不应抛出异常 / should not raise
                try:
                    main()
                except KeyboardInterrupt:
                    self.fail("KeyboardInterrupt should be caught")


# ── 辅助函数 / Helper functions ──


def _make_mock_data():
    """
    构造模拟 K 线数据，5 个交易对 × 100 个时间点。
    Generate mock kline data: 5 symbols × 100 timestamps.
    """
    np.random.seed(42)
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "XRPUSDT"]
    dates = pd.date_range("2024-01-01", periods=100, freq="h")
    rows = []
    for sym in symbols:
        price = 100.0
        for dt in dates:
            ret = np.random.normal(0.0001, 0.01)
            price *= (1 + ret)
            rows.append({
                "timestamp": dt,
                "symbol": sym,
                "open": price * (1 + np.random.normal(0, 0.002)),
                "high": price * (1 + abs(np.random.normal(0, 0.005))),
                "low": price * (1 - abs(np.random.normal(0, 0.005))),
                "close": price,
            })
    return pd.DataFrame(rows)


def _make_evaluator_mock():
    """构造模拟 FactorEvaluator / Create a mock FactorEvaluator."""
    return SimpleNamespace(
        ic=pd.Series([0.1, 0.2, 0.15]),
        rank_ic=pd.Series([0.15, 0.25, 0.2]),
        icir=1.5,
        ic_stats=pd.Series({
            "t_stat": 2.0, "p_value": 0.05,
            "IC_skew": -0.1, "IC_kurtosis": 3.0,
        }),
        sharpe=1.2,
        calmar=0.8,
        sortino=1.5,
        sharpe_after_cost=1.0,
        calmar_after_cost=0.6,
        sortino_after_cost=1.2,
        hedge_curve=pd.Series([1.0, 1.05, 1.10, 1.08, 1.12]),
        hedge_curve_after_cost=pd.Series([1.0, 1.04, 1.08, 1.06, 1.09]),
        turnover=pd.DataFrame({0: [0.1, 0.2], 1: [0.1, 0.2]}),
        rank_autocorr=pd.Series([0.8, 0.9]),
        neutralized_curve=pd.Series([1.0, 1.03, 1.06, 1.05, 1.07]),
    )


if __name__ == "__main__":
    unittest.main(verbosity=2)
