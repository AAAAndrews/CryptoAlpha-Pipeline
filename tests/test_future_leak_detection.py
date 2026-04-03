"""
未来函数检测验证测试 / Future Leak Detection Verification Tests (Task 16)

验证内容：
1. 现有因子输出 PASS / Existing factors output PASS
2. 人为注入泄露用例输出 FAIL / Injected leak cases output FAIL
3. 边界情况：空数据/全 NaN/单交易对 / Edge cases: empty/all-NaN/single pair
"""

import os
import warnings

import numpy as np
import pandas as pd
import pytest

from FactorLib.alpha_momentum import AlphaMomentum
from FactorLib.alpha_volatility import AlphaVolatility
from FactorLib.alpha_price_range import AlphaPriceRange
from FactorLib.base import BaseFactor
from FactorAnalysis.returns import calc_returns
from FactorAnalysis.alignment import align_factor_returns

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# ─────────────────────────────────────────────
# 辅助函数 / Helper functions
# ─────────────────────────────────────────────


def _make_factor_data(n_dates=100, n_symbols=20, seed=42):
    """
    生成标准测试行情数据（平铺格式，timestamp/symbol 为列）。
    Generate standard test OHLC data (flat format, timestamp/symbol as columns).

    参数含义：
        n_dates: 时间截面数
        n_symbols: 交易对数量
        seed: 随机种子
    """
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2024-01-01", periods=n_dates, freq="1h")
    symbols = [f"SYM{i:03d}" for i in range(n_symbols)]
    # 生成所有 (date, symbol) 组合 / generate all (date, symbol) pairs
    rows = []
    for d in dates:
        for s in symbols:
            rows.append({"timestamp": d, "symbol": s})
    meta = pd.DataFrame(rows)
    n = len(meta)
    close = 100 + rng.standard_normal(n).cumsum() * 0.5
    close = np.abs(close) + 1  # 确保正价格 / ensure positive prices
    meta["open"] = close * (1 + rng.standard_normal(n) * 0.005)
    meta["high"] = close * (1 + np.abs(rng.standard_normal(n)) * 0.01)
    meta["low"] = close * (1 - np.abs(rng.standard_normal(n)) * 0.01)
    meta["close"] = close
    # 确保 high >= low / ensure high >= low
    meta["high"] = meta[["high", "low", "close", "open"]].max(axis=1)
    meta["low"] = meta[["high", "low", "close", "open"]].min(axis=1)
    return meta


# ── 人为注入的未来泄露因子 / Artificially injected leaky factors ──


class _LeakyZScoreFactor(BaseFactor):
    """
    使用全样本统计量的泄露因子：z = (close - mean_all) / std_all。
    Leaky factor using full-sample stats: z = (close - mean_all) / std_all.

    截断数据后 mean/std 改变，重叠部分因子值不同 → 检测为 FAIL。
    Truncating data changes mean/std, overlapping portion differs → detected as FAIL.
    """

    def __init__(self):
        super().__init__()
        self.name = "LeakyZScoreFactor"

    def calculate(self, data):
        grouped = data.groupby("symbol")["close"]
        mean = grouped.transform("mean")
        std = grouped.transform("std")
        leaky = (data["close"] - mean) / std
        leaky.name = self.name
        return leaky


class _LeakyFutureMeanFactor(BaseFactor):
    """
    使用未来均值作为信号的泄露因子：factor = mean(close[t+1:]) / close[t] - 1。
    Leaky factor using future mean: factor = mean(close[t+1:]) / close[t] - 1.

    截断末尾数据后未来均值改变 → 检测为 FAIL。
    Truncating end changes future mean → detected as FAIL.
    """

    def __init__(self):
        super().__init__()
        self.name = "LeakyFutureMeanFactor"

    def calculate(self, data):
        # 对每个 symbol，从后向前累积未来均值 / cumulate future mean per symbol
        result = pd.Series(np.nan, index=data.index, name=self.name)
        for sym, grp in data.groupby("symbol"):
            close = grp["close"].values
            # 未来均值：从 t 到末尾的均值 / future mean from t to end
            cumsum_rev = np.cumsum(close[::-1])[::-1]
            count_rev = np.arange(len(close), 0, -1, dtype=float)
            future_mean = cumsum_rev / count_rev
            leaky = future_mean / close - 1
            result.loc[grp.index] = leaky
        return result


# ─────────────────────────────────────────────
# 1. 静态代码扫描测试 / Static code scanning tests
# ─────────────────────────────────────────────


class TestStaticScanning:
    """静态代码扫描：验证现有代码库无泄露 / Static scanning: verify no leaks in codebase."""

    def test_factorlib_no_shift_negative(self):
        """FactorLib 中不存在 shift(-N) / No shift(-N) in FactorLib."""
        from scripts.check_future_leak import check_no_shift_in_factorlib

        results = check_no_shift_in_factorlib()
        assert len(results) == 1
        assert results[0].status == "PASS", f"FactorLib 应无 shift(-N): {results[0].details}"

    def test_shift_only_in_allowed_files(self):
        """shift(-N) 仅在允许文件中 / shift(-N) only in allowed files."""
        from scripts.check_future_leak import check_shift_only_in_allowed_files

        results = check_shift_only_in_allowed_files()
        assert len(results) == 1
        assert results[0].status == "PASS", f"shift(-N) 不应在非允许文件中: {results[0].details}"

    def test_kline_loader_no_shift(self):
        """KlineLoader 无 shift 操作 / No shift in KlineLoader."""
        from scripts.check_future_leak import check_kline_loader_no_shift

        results = check_kline_loader_no_shift()
        assert len(results) == 1
        assert results[0].status == "PASS", f"KlineLoader 应无 shift: {results[0].details}"


# ─────────────────────────────────────────────
# 2. 动态数据验证 - 现有因子 PASS / Dynamic checks - existing factors PASS
# ─────────────────────────────────────────────


class TestExistingFactorsPass:
    """现有因子应全部 PASS / All existing factors should PASS."""

    @pytest.mark.parametrize("factor_cls", [AlphaMomentum, AlphaVolatility, AlphaPriceRange])
    def test_factor_independence(self, factor_cls):
        """因子值不依赖未来数据 / Factor values don't depend on future data."""
        from scripts.check_future_leak import check_factor_independence

        data = _make_factor_data(n_dates=100, n_symbols=5, seed=42)
        factor_inst = factor_cls()
        results = check_factor_independence(data, factor_cls.__name__, factor_inst)

        assert len(results) == 1
        assert results[0].status == "PASS", f"{factor_cls.__name__} 应独立: {results[0].details}"

    @pytest.mark.parametrize("factor_cls", [AlphaMomentum, AlphaVolatility, AlphaPriceRange])
    def test_alignment_checks(self, factor_cls):
        """对齐检测全部 PASS / All alignment checks PASS."""
        from scripts.check_future_leak import check_factor_returns_alignment

        data = _make_factor_data(n_dates=100, n_symbols=5, seed=42)
        factor_inst = factor_cls()
        results = check_factor_returns_alignment(
            data, factor_cls.__name__, factor_inst, "close2close"
        )

        # 应有 3 项检测：最后一期 NaN、对齐剔除最后时间戳、对齐后无 NaN
        assert len(results) == 3
        for r in results:
            assert r.status == "PASS", f"{factor_cls.__name__} {r.name} 应 PASS: {r.details}"


# ─────────────────────────────────────────────
# 3. 人为注入泄露用例 FAIL / Injected leak cases FAIL
# ─────────────────────────────────────────────


class TestInjectedLeaksFail:
    """人为注入的泄露因子应被检测到 FAIL / Injected leaky factors should FAIL."""

    @pytest.mark.parametrize("factor_cls", [_LeakyZScoreFactor, _LeakyFutureMeanFactor])
    def test_leaky_factor_independence(self, factor_cls):
        """泄露因子独立性检测应 FAIL / Leaky factor independence check should FAIL."""
        from scripts.check_future_leak import check_factor_independence

        data = _make_factor_data(n_dates=100, n_symbols=5, seed=42)
        factor_inst = factor_cls()
        results = check_factor_independence(data, factor_cls.__name__, factor_inst)

        assert len(results) == 1
        assert results[0].status == "FAIL", (
            f"{factor_cls.__name__} 是泄露因子，应输出 FAIL: {results[0].details}"
        )

    def test_leaky_factor_details_contain_warning(self):
        """泄露因子的 details 应包含警告信息 / FAIL details should contain warning."""
        from scripts.check_future_leak import check_factor_independence

        data = _make_factor_data(n_dates=100, n_symbols=5, seed=42)
        factor_inst = _LeakyZScoreFactor()
        results = check_factor_independence(data, "LeakyZScoreFactor", factor_inst)

        assert results[0].status == "FAIL"
        # 详情中应包含关键信息 / details should contain key info
        assert "未来数据" in results[0].details or "future" in results[0].details.lower()

    def test_leaky_factor_diff_exceeds_tolerance(self):
        """泄露因子的差异值应超过 1e-12 阈值 / Leaky factor diff should exceed tolerance."""
        from scripts.check_future_leak import check_factor_independence

        data = _make_factor_data(n_dates=100, n_symbols=5, seed=42)
        factor_inst = _LeakyZScoreFactor()
        results = check_factor_independence(data, "LeakyZScoreFactor", factor_inst)

        assert results[0].status == "FAIL"
        # details 中应包含具体差异数值 / details should contain actual diff value
        assert "差异" in results[0].details and "1e-12" in results[0].details


# ─────────────────────────────────────────────
# 4. 边界情况 / Edge cases
# ─────────────────────────────────────────────


class TestEdgeCases:
    """边界情况测试 / Edge case tests."""

    def test_empty_data_factor_independence(self):
        """空数据：因子独立性检测应处理 gracefully / Empty data should be handled."""
        from scripts.check_future_leak import check_factor_independence

        # 构建空 DataFrame / Build empty DataFrame
        empty = pd.DataFrame(columns=["timestamp", "symbol", "open", "high", "low", "close"])
        factor_inst = AlphaMomentum()

        # 空数据不会崩溃 / empty data should not crash
        results = check_factor_independence(empty, "AlphaMomentum", factor_inst)
        assert len(results) == 1
        # 空数据可能因无法比较而 PASS 或 FAIL，但不应抛异常
        assert results[0].status in ("PASS", "FAIL")

    def test_single_symbol_factor_independence(self):
        """单交易对：因子独立性应正常工作 / Single symbol should work normally."""
        from scripts.check_future_leak import check_factor_independence

        data = _make_factor_data(n_dates=50, n_symbols=1, seed=123)
        factor_inst = AlphaMomentum()
        results = check_factor_independence(data, "AlphaMomentum", factor_inst)

        assert len(results) == 1
        assert results[0].status == "PASS", f"单交易对应 PASS: {results[0].details}"

    def test_single_symbol_alignment(self):
        """单交易对：对齐检测应正常工作 / Single symbol alignment should work."""
        from scripts.check_future_leak import check_factor_returns_alignment

        data = _make_factor_data(n_dates=50, n_symbols=1, seed=123)
        factor_inst = AlphaMomentum()
        results = check_factor_returns_alignment(
            data, "AlphaMomentum", factor_inst, "close2close"
        )

        assert len(results) == 3
        for r in results:
            assert r.status == "PASS", f"单交易对 {r.name} 应 PASS: {r.details}"

    def test_all_nan_factor_values(self):
        """全 NaN 因子值：对齐后应剔除所有行 / All-NaN factor should result in empty alignment."""
        from scripts.check_future_leak import _build_factor_multiindex

        data = _make_factor_data(n_dates=50, n_symbols=3, seed=42)

        # 构建全 NaN 的因子值 / Build all-NaN factor values
        factor_raw = pd.Series(np.nan, index=data.index, name="NaNFactor")
        factor_values = _build_factor_multiindex(factor_raw, data)

        # 对齐应产生空结果 / alignment should produce empty result
        returns = calc_returns(data, label="close2close")
        clean = align_factor_returns(factor_values, returns)

        assert len(clean) == 0, "全 NaN 因子对齐后应为空"

    def test_two_dates_only(self):
        """仅两个时间截面：最小数据量应正常工作 / Two dates minimum should work."""
        from scripts.check_future_leak import check_factor_independence

        data = _make_factor_data(n_dates=2, n_symbols=3, seed=42)
        factor_inst = AlphaMomentum()
        results = check_factor_independence(data, "AlphaMomentum", factor_inst)

        assert len(results) == 1
        # 不应崩溃 / should not crash
        assert results[0].status in ("PASS", "FAIL")

    def test_very_few_dates_alignment(self):
        """极少日期：对齐应仍能处理 / Very few dates should still be handled."""
        from scripts.check_future_leak import check_factor_returns_alignment

        data = _make_factor_data(n_dates=5, n_symbols=3, seed=42)
        factor_inst = AlphaMomentum()
        results = check_factor_returns_alignment(
            data, "AlphaMomentum", factor_inst, "close2close"
        )

        # 至少有 3 项结果 / at least 3 results
        assert len(results) == 3
        # 最后一期收益应为 NaN / last period returns should be NaN
        last_period_result = [r for r in results if r.name == "最后一期收益为 NaN"][0]
        assert last_period_result.status == "PASS", last_period_result.details


# ─────────────────────────────────────────────
# 5. DetectionReport 数据结构测试 / DetectionReport structure tests
# ─────────────────────────────────────────────


class TestDetectionReport:
    """DetectionReport 属性验证 / DetectionReport property verification."""

    def test_all_passed_when_all_pass(self):
        """所有检测 PASS 时 all_passed=True / all_passed=True when all PASS."""
        from scripts.check_future_leak import CheckResult, DetectionReport

        report = DetectionReport(checks=[
            CheckResult(name="check1", status="PASS"),
            CheckResult(name="check2", status="PASS"),
        ])
        assert report.all_passed is True
        assert report.n_pass == 2
        assert report.n_fail == 0

    def test_all_passed_when_any_fail(self):
        """任一检测 FAIL 时 all_passed=False / all_passed=False when any FAIL."""
        from scripts.check_future_leak import CheckResult, DetectionReport

        report = DetectionReport(checks=[
            CheckResult(name="check1", status="PASS"),
            CheckResult(name="check2", status="FAIL"),
            CheckResult(name="check3", status="PASS"),
        ])
        assert report.all_passed is False
        assert report.n_pass == 2
        assert report.n_fail == 1

    def test_empty_report(self):
        """空报告 all_passed=True / Empty report all_passed=True."""
        from scripts.check_future_leak import DetectionReport

        report = DetectionReport()
        assert report.all_passed is True
        assert report.n_pass == 0
        assert report.n_fail == 0

    def test_check_result_defaults(self):
        """CheckResult 默认值 / CheckResult default values."""
        from scripts.check_future_leak import CheckResult

        cr = CheckResult(name="test", status="PASS")
        assert cr.details == ""
        assert cr.file == ""
        assert cr.line == 0


# ─────────────────────────────────────────────
# 6. FutureLeakDetector 集成测试 / FutureLeakDetector integration tests
# ─────────────────────────────────────────────


class TestFutureLeakDetector:
    """FutureLeakDetector 完整检测流程 / Full detector workflow tests."""

    def test_to_markdown_all_passed(self):
        """Markdown 报告：全 PASS 时生成正确内容 / Markdown report for all PASS."""
        from scripts.check_future_leak import CheckResult, DetectionReport, FutureLeakDetector

        detector = FutureLeakDetector()
        detector.report = DetectionReport(checks=[
            CheckResult(name="check1", status="PASS", details="all good"),
            CheckResult(name="check2", status="PASS", details="also good"),
        ])
        detector.report.start_time = "2026-04-04 12:00:00"
        detector.report.elapsed = 1.5

        md = detector.to_markdown()
        assert "ALL PASSED" in md
        assert "2/2 passed" in md
        assert "check1" in md
        assert "check2" in md

    def test_to_markdown_with_failure(self):
        """Markdown 报告：含 FAIL 时生成失败详情 / Markdown report with failures."""
        from scripts.check_future_leak import CheckResult, DetectionReport, FutureLeakDetector

        detector = FutureLeakDetector()
        detector.report = DetectionReport(checks=[
            CheckResult(name="ok_check", status="PASS"),
            CheckResult(name="bad_check", status="FAIL", details="发现泄露 / leak found"),
        ])
        detector.report.start_time = "2026-04-04 12:00:00"
        detector.report.elapsed = 0.5

        md = detector.to_markdown()
        assert "FAILED" in md
        assert "1/2 passed" in md
        assert "bad_check" in md
        assert "Failed Checks" in md
        assert "发现泄露" in md

    def test_to_markdown_table_format(self):
        """Markdown 报告：表格格式正确 / Markdown table format is correct."""
        from scripts.check_future_leak import CheckResult, DetectionReport, FutureLeakDetector

        detector = FutureLeakDetector()
        detector.report = DetectionReport(checks=[
            CheckResult(name="test_check", status="PASS", details="detail text"),
        ])
        detector.report.start_time = "2026-04-04 12:00:00"

        md = detector.to_markdown()
        # 验证表格结构 / verify table structure
        assert "| # | Status | Check | Details |" in md
        assert "|---|" in md
        assert "PASS" in md

    def test_static_checks_produce_results(self):
        """静态检测产生有效结果 / Static checks produce valid results."""
        from scripts.check_future_leak import FutureLeakDetector
        from scripts.check_future_leak import (
            check_no_shift_in_factorlib,
            check_shift_only_in_allowed_files,
            check_kline_loader_no_shift,
        )

        detector = FutureLeakDetector()
        for check_fn in [check_no_shift_in_factorlib, check_shift_only_in_allowed_files, check_kline_loader_no_shift]:
            for r in check_fn():
                detector.report.checks.append(r)

        # 静态扫描应有 3 项结果 / static scan should have 3 results
        assert len(detector.report.checks) >= 3
        # 所有静态检测应 PASS / all static checks should PASS
        assert detector.report.all_passed is True

    def test_full_detector_with_mock_data(self, monkeypatch):
        """完整检测器：使用 mock 数据 / Full detector with mock data."""
        from scripts.check_future_leak import FutureLeakDetector

        mock_data = _make_factor_data(n_dates=50, n_symbols=3, seed=42)

        class MockLoader:
            def compile(self):
                return mock_data

        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kwargs: MockLoader(),
        )

        detector = FutureLeakDetector()
        report = detector.run(
            factor_name="AlphaMomentum",
            start_time="2024-01-01",
            end_time="2024-02-01",
        )

        # 静态 3 项 + 动态 4 项（独立性 + 3 项对齐）= 7 项
        assert len(report.checks) >= 7
        # 现有因子应全部 PASS / existing factors should all PASS
        assert report.all_passed is True

    def test_detector_with_leaky_factor_mock(self, monkeypatch):
        """完整检测器：泄露因子应被检出 FAIL / Detector should catch leaky factor."""
        from scripts.check_future_leak import FutureLeakDetector
        import FactorLib

        mock_data = _make_factor_data(n_dates=50, n_symbols=3, seed=42)

        class MockLoader:
            def compile(self):
                return mock_data

        # monkeypatch FactorLib 的 list_factors/get 以包含泄露因子
        # patch FactorLib's list_factors/get to include the leaky factor
        _orig_list = FactorLib.list_factors
        _orig_get = FactorLib.get

        def _patched_list():
            return _orig_list() + ["LeakyZScoreFactor"]

        def _patched_get(name):
            if name == "LeakyZScoreFactor":
                return _LeakyZScoreFactor
            return _orig_get(name)

        monkeypatch.setattr(FactorLib, "list_factors", _patched_list)
        monkeypatch.setattr(FactorLib, "get", _patched_get)

        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kwargs: MockLoader(),
        )

        detector = FutureLeakDetector()
        report = detector.run(
            factor_name="LeakyZScoreFactor",
            start_time="2024-01-01",
            end_time="2024-02-01",
        )

        # 应包含 FAIL 结果 / should contain FAIL result
        assert report.all_passed is False
        fail_results = [c for c in report.checks if c.status == "FAIL"]
        assert len(fail_results) >= 1
        # 独立性检测应 FAIL / independence check should FAIL
        fail_names = [c.name for c in fail_results]
        assert any("LeakyZScoreFactor" in n for n in fail_names)

    def test_detector_empty_data_handling(self, monkeypatch):
        """完整检测器：空数据时 graceful 处理 / Detector handles empty data gracefully."""
        from scripts.check_future_leak import FutureLeakDetector

        class EmptyLoader:
            def compile(self):
                return pd.DataFrame()

        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kwargs: EmptyLoader(),
        )

        detector = FutureLeakDetector()
        report = detector.run()

        # 空数据时动态检测应添加 FAIL 占位 / empty data should add FAIL placeholders
        dynamic_names = {"因子独立性", "最后一期收益为 NaN", "对齐剔除最后时间戳", "对齐后无 NaN"}
        dynamic_results = [c for c in report.checks if c.name in dynamic_names]
        assert len(dynamic_results) >= 4
        # 空数据时 overall 应 FAIL / empty data means overall FAIL
        assert report.all_passed is False

    def test_detector_all_nan_data_handling(self, monkeypatch):
        """完整检测器：全 NaN 数据处理 / Detector handles all-NaN data."""
        from scripts.check_future_leak import FutureLeakDetector

        # 构建 close 全为 NaN 的数据（平铺格式）/ build flat data with all-NaN close
        dates = pd.date_range("2024-01-01", periods=20, freq="1h")
        symbols = ["SYM001", "SYM002"]
        rows = []
        for d in dates:
            for s in symbols:
                rows.append({"timestamp": d, "symbol": s})
        nan_data = pd.DataFrame(rows)
        nan_data["open"] = np.nan
        nan_data["high"] = np.nan
        nan_data["low"] = np.nan
        nan_data["close"] = np.nan

        class NanLoader:
            def compile(self):
                return nan_data

        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kwargs: NanLoader(),
        )

        detector = FutureLeakDetector()
        report = detector.run(factor_name="AlphaMomentum")

        # 不应崩溃 / should not crash
        assert len(report.checks) >= 3  # 至少静态 3 项 / at least 3 static checks

    def test_detector_single_symbol_mock(self, monkeypatch):
        """完整检测器：单交易对 mock / Detector with single symbol mock."""
        from scripts.check_future_leak import FutureLeakDetector

        mock_data = _make_factor_data(n_dates=30, n_symbols=1, seed=42)

        class SingleLoader:
            def compile(self):
                return mock_data

        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kwargs: SingleLoader(),
        )

        detector = FutureLeakDetector()
        report = detector.run(factor_name="AlphaMomentum")

        # 单交易对应能正常完成检测 / single symbol should complete detection
        assert len(report.checks) >= 7


# ─────────────────────────────────────────────
# 7. 多种子稳定性 / Multi-seed stability
# ─────────────────────────────────────────────


class TestMultiSeedStability:
    """多种子下检测结果应一致 / Detection results should be consistent across seeds."""

    @pytest.mark.parametrize("seed", [0, 42, 123, 999, 2024])
    def test_existing_factor_pass_across_seeds(self, seed):
        """现有因子在多种子下均 PASS / Existing factors PASS across seeds."""
        from scripts.check_future_leak import check_factor_independence

        data = _make_factor_data(n_dates=80, n_symbols=5, seed=seed)
        factor_inst = AlphaMomentum()
        results = check_factor_independence(data, "AlphaMomentum", factor_inst)

        assert results[0].status == "PASS", f"seed={seed} 时应 PASS: {results[0].details}"

    @pytest.mark.parametrize("seed", [0, 42, 123])
    def test_leaky_factor_fail_across_seeds(self, seed):
        """泄露因子在多种子下均 FAIL / Leaky factors FAIL across seeds."""
        from scripts.check_future_leak import check_factor_independence

        data = _make_factor_data(n_dates=80, n_symbols=5, seed=seed)
        factor_inst = _LeakyZScoreFactor()
        results = check_factor_independence(data, "LeakyZScoreFactor", factor_inst)

        assert results[0].status == "FAIL", f"seed={seed} 时应 FAIL: {results[0].details}"


# ─────────────────────────────────────────────
# 8. CheckResult 数据完整性 / CheckResult data integrity
# ─────────────────────────────────────────────


class TestCheckResultIntegrity:
    """CheckResult 字段完整性验证 / CheckResult field integrity verification."""

    def test_static_check_result_has_name_and_status(self):
        """静态检测结果有 name 和 status / Static check results have name and status."""
        from scripts.check_future_leak import check_no_shift_in_factorlib

        results = check_no_shift_in_factorlib()
        for r in results:
            assert r.name, "name 不应为空"
            assert r.status in ("PASS", "FAIL"), f"status 应为 PASS/FAIL，收到 {r.status}"

    def test_dynamic_check_result_has_details(self):
        """动态检测结果有 details / Dynamic check results have details."""
        from scripts.check_future_leak import check_factor_independence

        data = _make_factor_data(n_dates=50, n_symbols=3, seed=42)
        factor_inst = AlphaMomentum()
        results = check_factor_independence(data, "AlphaMomentum", factor_inst)

        for r in results:
            assert r.details, f"{r.name} 的 details 不应为空"
            assert r.name, "name 不应为空"

    def test_alignment_check_names(self):
        """对齐检测项名称正确 / Alignment check names are correct."""
        from scripts.check_future_leak import check_factor_returns_alignment

        data = _make_factor_data(n_dates=50, n_symbols=3, seed=42)
        factor_inst = AlphaMomentum()
        results = check_factor_returns_alignment(
            data, "AlphaMomentum", factor_inst, "close2close"
        )

        names = [r.name for r in results]
        assert "最后一期收益为 NaN" in names
        assert "对齐剔除最后时间戳" in names
        assert "对齐后无 NaN" in names
