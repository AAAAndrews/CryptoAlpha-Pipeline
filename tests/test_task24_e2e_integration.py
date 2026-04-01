"""
Task 24 验证测试：端到端投研流程集成测试
Task 24 verification tests: End-to-end factor research pipeline integration tests.

验证内容 / Coverage:
- 完整流程端到端运行 — 真实模块串联，非 mock (4)
- 两种收益率标签分别运行 (2)
- 结构化结果输出正确 — evaluator 属性 + report DataFrame (6)
- CLI 参数解析集成 (4)
- 不同因子端到端运行 (2)
- 参数传递验证 (3)
- 边界情况 (3)
"""

import sys
import os
import unittest
from unittest.mock import patch, MagicMock
from io import StringIO

import numpy as np
import pandas as pd

# 项目根目录 / Project root
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "CryptoDataProviders"))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "CryptoDB_feather"))


# ── 辅助函数 / Helper functions ──


def _make_realistic_data(n_symbols=5, n_periods=100, seed=42):
    """
    构造仿真 K 线数据，模拟真实价格走势。
    Generate realistic mock kline data with simulated price dynamics.
    """
    np.random.seed(seed)
    symbols = [f"SYM{i}USDT" for i in range(n_symbols)]
    dates = pd.date_range("2024-01-01", periods=n_periods, freq="h")
    rows = []
    for sym in symbols:
        price = 100.0 + np.random.uniform(0, 500)
        for dt in dates:
            ret = np.random.normal(0.0001, 0.01)
            price *= (1 + ret)
            h = price * (1 + abs(np.random.normal(0, 0.005)))
            l = price * (1 - abs(np.random.normal(0, 0.005)))
            o = l + np.random.random() * (h - l)
            rows.append({
                "timestamp": dt,
                "symbol": sym,
                "open": o,
                "high": h,
                "low": l,
                "close": price,
            })
    return pd.DataFrame(rows)


def _run_full_pipeline(factor_name="AlphaMomentum", return_label="close2close",
                       n_groups=5, cost_rate=0.001, data=None, **kwargs):
    """
    使用真实模块执行完整端到端流程（数据加载步骤 mock）。
    Execute the full E2E pipeline using real modules (mock data loading only).
    """
    from scripts.run_factor_research import run_factor_research

    mock_data = data if data is not None else _make_realistic_data()

    with patch("Cross_Section_Factor.kline_loader.KlineLoader") as mock_cls:
        mock_loader = MagicMock()
        mock_loader.compile.return_value = mock_data
        mock_cls.return_value = mock_loader
        ev, report = run_factor_research(
            factor_name=factor_name,
            return_label=return_label,
            n_groups=n_groups,
            cost_rate=cost_rate,
            **kwargs,
        )
    return ev, report


class TestE2EPipelineClose2Close(unittest.TestCase):
    """close2close 收益率标签端到端流程 / E2E pipeline with close2close label."""

    def test_pipeline_completes(self):
        """完整流程跑通无报错 / Full pipeline runs without error."""
        ev, report = _run_full_pipeline("AlphaMomentum", "close2close")
        self.assertIsNotNone(ev)
        self.assertIsNotNone(report)

    def test_all_evaluator_attributes_populated(self):
        """FactorEvaluator 全部 16 项属性已填充 / All 16 evaluator attributes are populated."""
        ev, _ = _run_full_pipeline("AlphaMomentum", "close2close")
        # IC 指标 / IC metrics
        self.assertIsNotNone(ev.ic)
        self.assertIsInstance(ev.ic, pd.Series)
        self.assertIsNotNone(ev.rank_ic)
        self.assertIsInstance(ev.rank_ic, pd.Series)
        self.assertIsNotNone(ev.icir)
        self.assertIsNotNone(ev.ic_stats)
        self.assertIsInstance(ev.ic_stats, pd.Series)
        # 分组 / Grouping
        self.assertIsNotNone(ev.group_labels)
        # 净值曲线 / Equity curves
        self.assertIsNotNone(ev.long_curve)
        self.assertIsNotNone(ev.short_curve)
        self.assertIsNotNone(ev.hedge_curve)
        self.assertIsNotNone(ev.hedge_curve_after_cost)
        # 绩效指标 / Performance ratios
        self.assertIsNotNone(ev.sharpe)
        self.assertIsNotNone(ev.calmar)
        self.assertIsNotNone(ev.sortino)
        self.assertIsNotNone(ev.sharpe_after_cost)
        self.assertIsNotNone(ev.calmar_after_cost)
        self.assertIsNotNone(ev.sortino_after_cost)
        # 换手率 / Turnover
        self.assertIsNotNone(ev.turnover)
        self.assertIsNotNone(ev.rank_autocorr)
        # 中性化 / Neutralization
        self.assertIsNotNone(ev.neutralized_curve)


class TestE2EPipelineOpen2Open(unittest.TestCase):
    """open2open 收益率标签端到端流程 / E2E pipeline with open2open label."""

    def test_pipeline_completes(self):
        """open2open 完整流程跑通 / open2open full pipeline runs."""
        ev, report = _run_full_pipeline("AlphaMomentum", "open2open")
        self.assertIsNotNone(ev)
        self.assertIsNotNone(report)

    def test_evaluator_attributes_populated(self):
        """open2open 下各属性已填充 / Attributes populated with open2open."""
        ev, _ = _run_full_pipeline("AlphaMomentum", "open2open")
        self.assertIsNotNone(ev.ic)
        self.assertIsNotNone(ev.hedge_curve)
        self.assertIsNotNone(ev.turnover)
        self.assertIsNotNone(ev.neutralized_curve)


class TestStructuredOutput(unittest.TestCase):
    """结构化结果输出验证 / Structured output validation."""

    def test_report_is_single_row_dataframe(self):
        """报告为单行 DataFrame / Report is a single-row DataFrame."""
        _, report = _run_full_pipeline()
        self.assertIsInstance(report, pd.DataFrame)
        self.assertEqual(len(report), 1)

    def test_report_contains_ic_columns(self):
        """报告包含 IC 相关列 / Report contains IC-related columns."""
        _, report = _run_full_pipeline()
        ic_cols = ["IC_mean", "ICIR", "RankIC_mean"]
        for col in ic_cols:
            self.assertIn(col, report.columns, f"Missing IC column: {col}")

    def test_report_contains_performance_columns(self):
        """报告包含绩效相关列 / Report contains performance columns."""
        _, report = _run_full_pipeline()
        perf_cols = ["hedge_return", "sharpe"]
        for col in perf_cols:
            self.assertIn(col, report.columns, f"Missing performance column: {col}")

    def test_report_no_nan_columns(self):
        """报告列中无全 NaN 列 / No all-NaN columns in report."""
        _, report = _run_full_pipeline()
        nan_cols = [c for c in report.columns if report[c].isna().all()]
        self.assertEqual(len(nan_cols), 0, f"All-NaN columns: {nan_cols}")

    def test_ic_series_has_multiindex(self):
        """IC 序列索引为 timestamp / IC series index is timestamp."""
        ev, _ = _run_full_pipeline()
        self.assertIsInstance(ev.ic.index, pd.DatetimeIndex)

    def test_hedge_curve_starts_at_one(self):
        """多空净值曲线起始值为 1.0 / Hedge curve starts at 1.0."""
        ev, _ = _run_full_pipeline()
        self.assertAlmostEqual(ev.hedge_curve.iloc[0], 1.0, places=6)


class TestCLIParsingIntegration(unittest.TestCase):
    """CLI 参数解析集成验证 / CLI argument parsing integration."""

    def test_factor_required(self):
        """--factor 为必填 / --factor is required."""
        from scripts.run_factor_research import main
        with patch("sys.argv", ["script"]):
            with self.assertRaises(SystemExit) as ctx:
                main()
            self.assertEqual(ctx.exception.code, 2)

    def test_invalid_return_label_rejected(self):
        """非法收益率标签被拒绝 / Invalid return label is rejected."""
        from scripts.run_factor_research import main
        with patch("sys.argv", ["script", "--factor", "X", "--return-label", "bad_label"]):
            with self.assertRaises(SystemExit):
                main()

    def test_all_data_params_accepted(self):
        """所有数据加载参数被接受 / All data loading params are accepted."""
        from scripts.run_factor_research import main
        argv = [
            "script", "--factor", "X",
            "--start-time", "2024-01-01",
            "--end-time", "2024-06-01",
            "--symbols", "BTCUSDT", "ETHUSDT",
            "--exchange", "binance",
            "--kline-type", "swap",
            "--interval", "4h",
        ]
        with patch("sys.argv", argv):
            with patch("scripts.run_factor_research.run_factor_research", return_value=(None, None)):
                # 不应抛出参数解析错误 / should not raise parse error
                try:
                    main()
                except SystemExit:
                    self.fail("main() raised SystemExit unexpectedly")

    def test_all_analysis_params_accepted(self):
        """所有分析参数被接受 / All analysis params are accepted."""
        from scripts.run_factor_research import main
        argv = [
            "script", "--factor", "X",
            "--n-groups", "10",
            "--cost-rate", "0.002",
            "--top-k", "2",
            "--bottom-k", "2",
            "--risk-free-rate", "0.03",
            "--periods-per-year", "365",
            "--max-loss", "0.2",
        ]
        with patch("sys.argv", argv):
            with patch("scripts.run_factor_research.run_factor_research", return_value=(None, None)):
                try:
                    main()
                except SystemExit:
                    self.fail("main() raised SystemExit unexpectedly")


class TestMultipleFactorsE2E(unittest.TestCase):
    """不同因子端到端运行 / E2E with different factors."""

    @patch("Cross_Section_Factor.kline_loader.KlineLoader")
    def test_alpha_volatility(self, mock_cls):
        """AlphaVolatility 因子端到端运行 / AlphaVolatility factor E2E."""
        mock_loader = MagicMock()
        mock_loader.compile.return_value = _make_realistic_data()
        mock_cls.return_value = mock_loader

        ev, report = _run_full_pipeline("AlphaVolatility")
        self.assertIsNotNone(ev)
        self.assertIsNotNone(report)
        self.assertIsNotNone(ev.icir)

    @patch("Cross_Section_Factor.kline_loader.KlineLoader")
    def test_alpha_price_range(self, mock_cls):
        """AlphaPriceRange 因子端到端运行 / AlphaPriceRange factor E2E."""
        mock_loader = MagicMock()
        mock_loader.compile.return_value = _make_realistic_data()
        mock_cls.return_value = mock_loader

        ev, report = _run_full_pipeline("AlphaPriceRange")
        self.assertIsNotNone(ev)
        self.assertIsNotNone(report)
        self.assertIsNotNone(ev.hedge_curve)


class TestParameterPassing(unittest.TestCase):
    """参数传递验证 / Parameter passing validation."""

    def test_n_groups_passed_to_evaluator(self):
        """n_groups 正确传递到 evaluator / n_groups passed correctly."""
        ev, _ = _run_full_pipeline(n_groups=8)
        self.assertEqual(ev.n_groups, 8)

    def test_cost_rate_passed_to_evaluator(self):
        """cost_rate 正确传递到 evaluator / cost_rate passed correctly."""
        ev, _ = _run_full_pipeline(cost_rate=0.003)
        self.assertAlmostEqual(ev.cost_rate, 0.003)

    def test_custom_risk_free_rate(self):
        """自定义 risk_free_rate 正确传递 / Custom risk_free_rate passed."""
        ev, _ = _run_full_pipeline(risk_free_rate=0.05)
        self.assertAlmostEqual(ev.risk_free_rate, 0.05)


class TestEdgeCases(unittest.TestCase):
    """边界情况 / Edge cases."""

    def test_large_dataset(self):
        """大数据量 (20 symbols × 500 periods) 正常运行 / Large dataset runs."""
        data = _make_realistic_data(n_symbols=20, n_periods=500)
        ev, report = _run_full_pipeline(data=data)
        self.assertIsNotNone(ev)
        self.assertIsNotNone(report)

    def test_minimum_data(self):
        """最小数据量正常处理 / Minimum data runs."""
        # AlphaPriceRange 无 lookback 依赖，适合小数据量 / No lookback dependency
        data = _make_realistic_data(n_symbols=2, n_periods=10)
        ev, report = _run_full_pipeline(
            factor_name="AlphaPriceRange", n_groups=2, data=data,
        )
        self.assertIsNotNone(ev)
        self.assertIsNotNone(report)

    @patch("Cross_Section_Factor.kline_loader.KlineLoader")
    def test_unknown_factor_returns_none(self, mock_cls):
        """未知因子返回 (None, None) / Unknown factor returns (None, None)."""
        mock_loader = MagicMock()
        mock_loader.compile.return_value = _make_realistic_data()
        mock_cls.return_value = mock_loader

        from scripts.run_factor_research import run_factor_research
        ev, report = run_factor_research(factor_name="NonExistentFactor")
        self.assertIsNone(ev)
        self.assertIsNone(report)


if __name__ == "__main__":
    unittest.main(verbosity=2)
