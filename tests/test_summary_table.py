"""
tests/test_summary_table.py — 综合绩效表格验证测试
tests/test_summary_table.py — Summary performance table validation tests.

验证 Task 24 实现的 build_summary_table 绩效表格生成功能。
Validates the build_summary_table performance table generation implemented in Task 24.
"""

from __future__ import annotations

import re

import numpy as np
import pandas as pd
import pytest

from FactorAnalysis.evaluator import FactorEvaluator
from FactorAnalysis.visualization.tables import (
    build_summary_table,
    _signal_color,
    _signal_html,
    _fmt,
)


# ============================================================
# 测试数据构建工具 / Test data construction helpers
# ============================================================


def _make_evaluator(
    n_dates: int = 100,
    n_symbols: int = 10,
    seed: int = 42,
    run_all: bool = True,
):
    """
    构建已运行分析的 FactorEvaluator 实例 / Build a FactorEvaluator with analysis run.

    生成随机因子值和收益率，确保 IC 序列非零。
    Generates random factor values and returns, ensuring non-zero IC series.

    Parameters / 参数:
        n_dates: 日期数 / Number of dates
        n_symbols: 交易对数 / Number of symbols
        seed: 随机种子 / Random seed
        run_all: 是否运行 run_all()，False 时仅 run_metrics()
                 Whether to run run_all(), False means only run_metrics()
    """
    rng = np.random.RandomState(seed)
    dates = pd.date_range("2024-01-01", periods=n_dates, freq="D")
    symbols = [f"SYM{i}" for i in range(n_symbols)]
    idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])

    # 因子值与收益率有弱正相关，确保 IC 非零
    # factor values have weak positive correlation with returns to ensure non-zero IC
    factor_base = rng.randn(n_dates, n_symbols)
    noise = rng.randn(n_dates, n_symbols) * 0.5
    returns_vals = factor_base * 0.3 + noise

    factor = pd.Series(factor_base.ravel(), index=idx, name="factor")
    returns = pd.Series(returns_vals.ravel(), index=idx, name="returns")

    ev = FactorEvaluator(factor, returns)
    if run_all:
        ev.run_all()
    else:
        ev.run_metrics()
    return ev


def _make_partial_evaluator():
    """
    构建仅执行 run_metrics() 的 evaluator / Build an evaluator with only run_metrics() called.
    """
    return _make_evaluator(run_all=False)


# ============================================================
# 基础功能测试 / Basic functionality tests
# ============================================================


class TestBuildSummaryTableBasic:
    """综合绩效表格基础功能验证 / Summary table basic functionality validation."""

    def test_returns_string(self):
        """返回值为字符串 / Returns a string."""
        ev = _make_evaluator()
        result = build_summary_table(ev)
        assert isinstance(result, str)

    def test_contains_table_tag(self):
        """包含 <table> 标签 / Contains <table> tag."""
        ev = _make_evaluator()
        result = build_summary_table(ev)
        assert "<table>" in result
        assert "</table>" in result

    def test_contains_css_styles(self):
        """包含内联 CSS 信号灯样式 / Contains inline CSS signal-light styles."""
        ev = _make_evaluator()
        result = build_summary_table(ev)
        assert "signal-good" in result
        assert "signal-bad" in result
        assert "signal-neutral" in result
        assert "signal-na" in result

    def test_partial_evaluator_works(self):
        """仅 run_metrics() 也能生成表格 / Works with only run_metrics() called."""
        ev = _make_partial_evaluator()
        result = build_summary_table(ev)
        assert "<table>" in result
        assert "IC Mean" in result


# ============================================================
# 表格内容测试 / Table content tests
# ============================================================


class TestBuildSummaryTableContent:
    """表格内容完整性验证 / Table content completeness validation."""

    def test_ic_section_present(self):
        """IC 分析板块存在 / IC analysis section present."""
        ev = _make_evaluator()
        result = build_summary_table(ev)
        assert "IC 分析 / IC Analysis" in result
        assert "IC Mean" in result
        assert "IC Std" in result
        assert "RankIC Mean" in result
        assert "RankIC Std" in result
        assert "ICIR" in result
        assert "IC t-stat" in result
        assert "IC p-value" in result
        assert "IC Skew" in result
        assert "IC Kurtosis" in result

    def test_return_section_present(self):
        """收益分析板块存在 / Return analysis section present."""
        ev = _make_evaluator()
        result = build_summary_table(ev)
        assert "收益分析 / Return Analysis" in result
        assert "Long Return" in result
        assert "Short Return" in result
        assert "Hedge Return" in result
        assert "Hedge Return (After Cost)" in result

    def test_performance_section_present(self):
        """绩效比率板块存在 / Performance ratios section present."""
        ev = _make_evaluator()
        result = build_summary_table(ev)
        assert "绩效比率 / Performance Ratios" in result
        assert "Sharpe" in result
        assert "Calmar" in result
        assert "Sortino" in result
        assert "Sharpe (After Cost)" in result
        assert "Calmar (After Cost)" in result
        assert "Sortino (After Cost)" in result

    def test_turnover_section_present(self):
        """换手率板块存在 / Turnover section present."""
        ev = _make_evaluator()
        result = build_summary_table(ev)
        assert "换手率 / Turnover" in result
        assert "Avg Turnover" in result
        assert "Avg Rank Autocorr" in result

    def test_neutralize_section_present(self):
        """中性化板块存在 / Neutralization section present."""
        ev = _make_evaluator()
        result = build_summary_table(ev)
        assert "中性化 / Neutralization" in result
        assert "Neutralized Return" in result

    def test_all_rows_are_tr(self):
        """所有数据行使用 <tr> 标签 / All data rows use <tr> tags."""
        ev = _make_evaluator()
        result = build_summary_table(ev)
        # 排除 header 行（<th 开头），检查数据行 / exclude header rows, check data rows
        tr_count = result.count("<tr><td>")
        assert tr_count >= 10  # 至少 10 个数据指标 / at least 10 data metrics


# ============================================================
# 信号灯标识测试 / Signal-light indicator tests
# ============================================================


class TestSignalLightIndicators:
    """信号灯标识逻辑验证 / Signal-light indicator logic validation."""

    def test_icir_signal_good(self):
        """ICIR > 0.5 时显示绿色信号 / ICIR > 0.5 shows green signal."""
        ev = _make_evaluator()
        ev.icir = 1.2  # 强制设为高值 / force high value
        result = build_summary_table(ev)
        assert 'class="signal-good"' in result

    def test_icir_signal_bad(self):
        """ICIR < 0 时显示红色信号 / ICIR < 0 shows red signal."""
        ev = _make_evaluator()
        ev.icir = -0.5
        result = build_summary_table(ev)
        assert 'class="signal-bad"' in result

    def test_icir_signal_neutral(self):
        """ICIR 在 0~0.5 之间显示黄色信号 / ICIR in 0~0.5 shows yellow signal."""
        ev = _make_evaluator()
        ev.icir = 0.3
        result = build_summary_table(ev)
        assert 'class="signal-neutral"' in result

    def test_sharpe_signal_good(self):
        """Sharpe > 0.5 时显示绿色信号 / Sharpe > 0.5 shows green signal."""
        ev = _make_evaluator()
        ev.sharpe = 2.0
        result = build_summary_table(ev)
        assert 'class="signal-good"' in result

    def test_sharpe_signal_bad(self):
        """Sharpe < 0 时显示红色信号 / Sharpe < 0 shows red signal."""
        ev = _make_evaluator()
        ev.sharpe = -1.0
        result = build_summary_table(ev)
        assert 'class="signal-bad"' in result

    def test_sharpe_na_in_partial_evaluator(self):
        """仅 run_metrics() 时 Sharpe 为 N/A / Sharpe is N/A when only run_metrics()."""
        ev = _make_partial_evaluator()
        result = build_summary_table(ev)
        assert "N/A" in result
        assert 'class="signal-na"' in result


# ============================================================
# 参数验证测试 / Parameter validation tests
# ============================================================


class TestBuildSummaryTableValidation:
    """参数验证 / Parameter validation."""

    def test_none_evaluator_raises(self):
        """evaluator 为 None 抛出 ValueError / None evaluator raises ValueError."""
        with pytest.raises(ValueError, match="evaluator"):
            build_summary_table(None)

    def test_uninitialized_evaluator_raises(self):
        """未执行 run() 的 evaluator 抛出 ValueError / Uninitialized evaluator raises ValueError."""
        rng = np.random.RandomState(42)
        dates = pd.date_range("2024-01-01", periods=50, freq="D")
        symbols = ["SYM0", "SYM1"]
        idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])
        factor = pd.Series(rng.randn(100), index=idx)
        returns = pd.Series(rng.randn(100), index=idx)
        ev = FactorEvaluator(factor, returns)
        # 未调用任何 run 方法 / no run method called
        with pytest.raises(ValueError, match="run"):
            build_summary_table(ev)


# ============================================================
# 数据一致性测试 / Data consistency tests
# ============================================================


class TestBuildSummaryTableConsistency:
    """表格数据与 evaluator 属性一致性 / Table data consistency with evaluator attributes."""

    def test_ic_mean_matches(self):
        """表格中 IC Mean 值与 evaluator.ic.mean() 一致 / IC Mean matches evaluator."""
        ev = _make_evaluator()
        result = build_summary_table(ev)
        expected = f"{ev.ic.mean():.4f}"
        assert expected in result

    def test_icir_matches(self):
        """表格中 ICIR 值与 evaluator.icir 一致 / ICIR matches evaluator."""
        ev = _make_evaluator()
        result = build_summary_table(ev)
        expected = f"{ev.icir:.4f}"
        assert expected in result

    def test_hedge_return_matches(self):
        """表格中 Hedge Return 与 evaluator 一致 / Hedge Return matches evaluator."""
        ev = _make_evaluator()
        result = build_summary_table(ev)
        expected = f"{ev.hedge_curve.iloc[-1] - 1.0:.4f}"
        assert expected in result

    def test_sharpe_matches(self):
        """表格中 Sharpe 与 evaluator.sharpe 一致 / Sharpe matches evaluator."""
        ev = _make_evaluator()
        result = build_summary_table(ev)
        expected = f"{ev.sharpe:.4f}"
        assert expected in result

    def test_avg_turnover_matches(self):
        """表格中 Avg Turnover 与 evaluator 一致 / Avg Turnover matches evaluator."""
        ev = _make_evaluator()
        result = build_summary_table(ev)
        expected = f"{ev.turnover.mean().mean():.4f}"
        assert expected in result

    def test_neutralized_return_matches(self):
        """表格中 Neutralized Return 与 evaluator 一致 / Neutralized Return matches evaluator."""
        ev = _make_evaluator()
        result = build_summary_table(ev)
        expected = f"{ev.neutralized_curve.iloc[-1] - 1.0:.4f}"
        assert expected in result


# ============================================================
# 辅助函数测试 / Helper function tests
# ============================================================


class TestHelperFunctions:
    """_fmt / _signal_html / _signal_color 辅助函数验证."""

    def test_fmt_normal(self):
        """正常值格式化 / Normal value formatting."""
        assert _fmt(0.123456) == "0.1235"
        assert _fmt(1.0) == "1.0000"
        assert _fmt(-0.5) == "-0.5000"

    def test_fmt_none(self):
        """None 返回 N/A / None returns N/A."""
        assert _fmt(None) == "N/A"

    def test_fmt_nan(self):
        """NaN 返回 N/A / NaN returns N/A."""
        assert _fmt(float("nan")) == "N/A"

    def test_fmt_custom_decimals(self):
        """自定义小数位数 / Custom decimal places."""
        assert _fmt(0.123456, decimals=2) == "0.12"

    def test_signal_color_good(self):
        """_signal_color 返回 good / _signal_color returns good."""
        assert _signal_color(1.0) == "signal-good"

    def test_signal_color_bad(self):
        """_signal_color 返回 bad / _signal_color returns bad."""
        assert _signal_color(-0.5) == "signal-bad"

    def test_signal_color_neutral(self):
        """_signal_color 返回 neutral / _signal_color returns neutral."""
        assert _signal_color(0.3) == "signal-neutral"

    def test_signal_color_na(self):
        """_signal_color 返回 na（None 或 NaN）/ _signal_color returns na."""
        assert _signal_color(None) == "signal-na"
        assert _signal_color(float("nan")) == "signal-na"

    def test_signal_color_custom_threshold(self):
        """自定义阈值 / Custom threshold."""
        assert _signal_color(0.3, threshold_good=0.2) == "signal-good"

    def test_signal_html_good(self):
        """_signal_html 包含 signal-good 类 / _signal_html contains signal-good class."""
        html = _signal_html(1.0)
        assert 'class="signal-good"' in html
        assert "1.0000" in html

    def test_signal_html_none(self):
        """_signal_html 对 None 返回 N/A / _signal_html returns N/A for None."""
        html = _signal_html(None)
        assert 'class="signal-na"' in html
        assert "N/A" in html


# ============================================================
# 边界情况测试 / Edge case tests
# ============================================================


class TestBuildSummaryTableEdgeCases:
    """边界情况验证 / Edge case validation."""

    def test_small_dataset(self):
        """小数据集（20 日期 × 3 交易对）/ Small dataset (20 dates x 3 symbols)."""
        ev = _make_evaluator(n_dates=20, n_symbols=3, seed=99)
        result = build_summary_table(ev)
        assert "<table>" in result
        assert "IC Mean" in result

    def test_large_dataset(self):
        """大数据集（300 日期 × 50 交易对）/ Large dataset (300 dates x 50 symbols)."""
        ev = _make_evaluator(n_dates=300, n_symbols=50, seed=123)
        result = build_summary_table(ev)
        assert "<table>" in result
        assert "ICIR" in result

    def test_multiple_seeds(self):
        """多种子稳定性 / Multi-seed stability."""
        results = []
        for seed in [1, 42, 100, 200, 999]:
            ev = _make_evaluator(seed=seed)
            html = build_summary_table(ev)
            results.append(html)
        # 每个种子都应产生有效 HTML / each seed should produce valid HTML
        for html in results:
            assert "<table>" in html
            assert "</table>" in html

    def test_evaluator_with_nan_ic(self):
        """IC 序列含 NaN 时表格正常生成 / Table works with NaN in IC series."""
        ev = _make_evaluator()
        # 在 IC 序列中注入 NaN / inject NaN into IC series
        ev.ic.iloc[::10] = np.nan
        result = build_summary_table(ev)
        assert "<table>" in result

    def test_zero_icir(self):
        """ICIR = 0 时显示黄色信号 / ICIR = 0 shows yellow signal."""
        ev = _make_evaluator()
        ev.icir = 0.0
        result = build_summary_table(ev)
        assert 'class="signal-neutral"' in result
