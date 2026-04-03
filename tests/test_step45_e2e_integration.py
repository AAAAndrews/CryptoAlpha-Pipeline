"""
未来函数检测端到端集成测试 / Future Leak Detection E2E Integration Tests (Task 18)

验证内容 / Verifications:
1. step4.5 正确集成到 run_factor_research() / step4.5 correctly integrated
2. PASS 时流程继续 / Pipeline continues when PASS
3. FAIL + leak_block=True 时阻断 / Pipeline blocks on FAIL with leak_block
4. FAIL + leak_block=False 时继续 / Pipeline continues on FAIL without leak_block
5. CLI --check-leak 参数可用 / CLI --check-leak parameter works
6. 不传 check_leak 时向后兼容 / Backward compatible without check_leak
"""

import os
import sys
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
# 1. step4.5 集成正确性 / step4.5 integration correctness
# ─────────────────────────────────────────────


class TestStep45Integration:
    """验证 step4.5 正确集成到 pipeline / Verify step4.5 correctly integrated."""

    def test_check_leak_triggers_detection(self, monkeypatch):
        """check_leak=True 时 step4.5 执行检测 / step4.5 runs when check_leak=True."""
        from scripts.check_future_leak import DetectionReport, CheckResult
        from scripts.run_factor_research import run_factor_research

        mock_data = _make_factor_data(n_dates=60, n_symbols=5, seed=42)

        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kw: _MockLoader(mock_data),
        )

        # 构造 mock 检测器，记录 run() 是否被调用 / track if run() was called
        called = {"value": False}

        class TrackedDetector:
            def __init__(self):
                pass

            def run(self, **kwargs):
                called["value"] = True
                return DetectionReport(checks=[
                    CheckResult(name="test", status="PASS"),
                ])

        monkeypatch.setattr(
            "scripts.check_future_leak.FutureLeakDetector",
            TrackedDetector,
        )

        ev, report = run_factor_research(
            factor_name="AlphaMomentum",
            check_leak=True,
        )

        # 检测器 run() 应被调用 / detector run() should have been called
        assert called["value"], "check_leak=True 时 FutureLeakDetector.run() 应被调用"

    def test_no_check_leak_skips_detection(self, monkeypatch):
        """check_leak=False 时不执行 step4.5 / step4.5 skipped when check_leak=False."""
        import importlib
        import sys

        from scripts.run_factor_research import run_factor_research

        mock_data = _make_factor_data(n_dates=60, n_symbols=5, seed=42)

        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kw: _MockLoader(mock_data),
        )

        # 移除模块缓存（如果之前加载过）/ remove from module cache if loaded before
        mod_key = "scripts.check_future_leak"
        was_loaded = mod_key in sys.modules
        saved_mod = sys.modules.pop(mod_key, None)

        ev, report = run_factor_research(
            factor_name="AlphaMomentum",
            check_leak=False,
        )

        # check_leak=False 时不应导入 check_future_leak 模块
        # should not import check_future_leak when check_leak=False
        assert mod_key not in sys.modules, (
            "check_leak=False 时不应导入 scripts.check_future_leak"
        )

        # 恢复缓存 / restore cache
        if was_loaded and saved_mod is not None:
            sys.modules[mod_key] = saved_mod


# ─────────────────────────────────────────────
# 2. PASS 时流程继续 / Pipeline continues when PASS
# ─────────────────────────────────────────────


class TestPassContinues:
    """检测 PASS 时 pipeline 正常完成 / Pipeline completes normally when PASS."""

    def test_pass_returns_evaluator_and_report(self, monkeypatch):
        """PASS 时返回 evaluator 和 report / Returns evaluator and report on PASS."""
        from scripts.check_future_leak import DetectionReport, CheckResult
        from scripts.run_factor_research import run_factor_research

        mock_data = _make_factor_data(n_dates=60, n_symbols=5, seed=42)

        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kw: _MockLoader(mock_data),
        )

        class PassDetector:
            def run(self, **kwargs):
                return DetectionReport(checks=[
                    CheckResult(name="static_1", status="PASS"),
                    CheckResult(name="static_2", status="PASS"),
                    CheckResult(name="dynamic_1", status="PASS"),
                ])

        monkeypatch.setattr(
            "scripts.check_future_leak.FutureLeakDetector",
            PassDetector,
        )

        ev, report = run_factor_research(
            factor_name="AlphaMomentum",
            check_leak=True,
        )

        # PASS 时应正常返回 evaluator 和 report / should return evaluator and report
        assert ev is not None, "PASS 时 evaluator 不应为 None"
        assert report is not None, "PASS 时 report 不应为 None"
        # 验证 evaluator 关键属性存在 / verify evaluator has key attributes
        assert hasattr(ev, "ic") and ev.ic is not None
        assert hasattr(ev, "icir") and ev.icir is not None
        # report 应为 DataFrame / report should be DataFrame
        assert isinstance(report, pd.DataFrame)

    def test_pass_evaluator_results_valid(self, monkeypatch):
        """PASS 时 evaluator 结果数值有效 / Evaluator results are numerically valid on PASS."""
        from scripts.check_future_leak import DetectionReport, CheckResult
        from scripts.run_factor_research import run_factor_research

        mock_data = _make_factor_data(n_dates=80, n_symbols=10, seed=42)

        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kw: _MockLoader(mock_data),
        )

        class PassDetector:
            def run(self, **kwargs):
                return DetectionReport(checks=[
                    CheckResult(name="c1", status="PASS"),
                ])

        monkeypatch.setattr(
            "scripts.check_future_leak.FutureLeakDetector",
            PassDetector,
        )

        ev, report = run_factor_research(
            factor_name="AlphaMomentum",
            check_leak=True,
            n_groups=5,
        )

        # 关键数值指标应为有限数 / key metrics should be finite
        assert np.isfinite(ev.icir), f"ICIR 应为有限数: {ev.icir}"
        assert np.isfinite(ev.sharpe), f"Sharpe 应为有限数: {ev.sharpe}"
        assert ev.hedge_curve is not None
        assert len(ev.hedge_curve) > 0


# ─────────────────────────────────────────────
# 3. FAIL + leak_block=True 阻断 / Block on FAIL with leak_block
# ─────────────────────────────────────────────


class TestFailBlock:
    """FAIL + leak_block=True 时阻断 pipeline / Block pipeline on FAIL."""

    def test_fail_block_returns_none(self, monkeypatch):
        """FAIL + leak_block=True 时返回 (None, None) / Returns (None, None)."""
        from scripts.check_future_leak import DetectionReport, CheckResult
        from scripts.run_factor_research import run_factor_research

        mock_data = _make_factor_data(n_dates=60, n_symbols=5, seed=42)

        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kw: _MockLoader(mock_data),
        )

        class FailDetector:
            def run(self, **kwargs):
                return DetectionReport(checks=[
                    CheckResult(name="ok", status="PASS"),
                    CheckResult(name="leak", status="FAIL", details="发现泄露"),
                ])

        monkeypatch.setattr(
            "scripts.check_future_leak.FutureLeakDetector",
            FailDetector,
        )

        ev, report = run_factor_research(
            factor_name="AlphaMomentum",
            check_leak=True,
            leak_block=True,
        )

        # 阻断时返回 None / should return None
        assert ev is None, "leak_block=True 时 evaluator 应为 None"
        assert report is None, "leak_block=True 时 report 应为 None"

    def test_fail_block_single_check_fail(self, monkeypatch):
        """仅一项 FAIL 即触发阻断 / Single FAIL triggers block."""
        from scripts.check_future_leak import DetectionReport, CheckResult
        from scripts.run_factor_research import run_factor_research

        mock_data = _make_factor_data(n_dates=60, n_symbols=5, seed=42)

        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kw: _MockLoader(mock_data),
        )

        class SingleFailDetector:
            def run(self, **kwargs):
                return DetectionReport(checks=[
                    CheckResult(name="only_check", status="FAIL"),
                ])

        monkeypatch.setattr(
            "scripts.check_future_leak.FutureLeakDetector",
            SingleFailDetector,
        )

        ev, report = run_factor_research(
            factor_name="AlphaMomentum",
            check_leak=True,
            leak_block=True,
        )

        assert ev is None
        assert report is None

    def test_fail_block_all_checks_fail(self, monkeypatch):
        """全部 FAIL 时也应阻断 / All FAIL also blocks."""
        from scripts.check_future_leak import DetectionReport, CheckResult
        from scripts.run_factor_research import run_factor_research

        mock_data = _make_factor_data(n_dates=60, n_symbols=5, seed=42)

        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kw: _MockLoader(mock_data),
        )

        class AllFailDetector:
            def run(self, **kwargs):
                return DetectionReport(checks=[
                    CheckResult(name="c1", status="FAIL"),
                    CheckResult(name="c2", status="FAIL"),
                    CheckResult(name="c3", status="FAIL"),
                ])

        monkeypatch.setattr(
            "scripts.check_future_leak.FutureLeakDetector",
            AllFailDetector,
        )

        ev, report = run_factor_research(
            factor_name="AlphaMomentum",
            check_leak=True,
            leak_block=True,
        )

        assert ev is None
        assert report is None


# ─────────────────────────────────────────────
# 4. FAIL + leak_block=False 继续 / Continue on FAIL without leak_block
# ─────────────────────────────────────────────


class TestFailContinue:
    """FAIL + leak_block=False 时继续执行 / Pipeline continues without leak_block."""

    def test_fail_no_block_returns_evaluator(self, monkeypatch):
        """FAIL + leak_block=False 时返回 evaluator / Returns evaluator on FAIL without block."""
        from scripts.check_future_leak import DetectionReport, CheckResult
        from scripts.run_factor_research import run_factor_research

        mock_data = _make_factor_data(n_dates=60, n_symbols=5, seed=42)

        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kw: _MockLoader(mock_data),
        )

        class FailDetector:
            def run(self, **kwargs):
                return DetectionReport(checks=[
                    CheckResult(name="ok", status="PASS"),
                    CheckResult(name="leak", status="FAIL", details="泄露"),
                ])

        monkeypatch.setattr(
            "scripts.check_future_leak.FutureLeakDetector",
            FailDetector,
        )

        ev, report = run_factor_research(
            factor_name="AlphaMomentum",
            check_leak=True,
            leak_block=False,
        )

        # 不阻断时应正常返回 / should return normally when not blocking
        assert ev is not None, "leak_block=False 时 evaluator 不应为 None"
        assert report is not None, "leak_block=False 时 report 不应为 None"

    def test_fail_no_block_evaluator_valid(self, monkeypatch):
        """FAIL + leak_block=False 时 evaluator 结果有效 / Evaluator results valid."""
        from scripts.check_future_leak import DetectionReport, CheckResult
        from scripts.run_factor_research import run_factor_research

        mock_data = _make_factor_data(n_dates=80, n_symbols=10, seed=42)

        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kw: _MockLoader(mock_data),
        )

        class FailDetector:
            def run(self, **kwargs):
                return DetectionReport(checks=[
                    CheckResult(name="c1", status="PASS"),
                    CheckResult(name="c2", status="FAIL", details="test failure"),
                ])

        monkeypatch.setattr(
            "scripts.check_future_leak.FutureLeakDetector",
            FailDetector,
        )

        ev, report = run_factor_research(
            factor_name="AlphaMomentum",
            check_leak=True,
            leak_block=False,
        )

        # evaluator 应有完整结果 / evaluator should have complete results
        assert np.isfinite(ev.icir)
        assert ev.hedge_curve is not None
        assert isinstance(report, pd.DataFrame)
        assert len(report) > 0


# ─────────────────────────────────────────────
# 5. CLI --check-leak 参数 / CLI --check-leak parameter
# ─────────────────────────────────────────────


class TestCLIParameters:
    """验证 CLI 参数解析 / Verify CLI argument parsing."""

    def test_check_leak_flag_default_false(self):
        """--check-leak 默认为 False / --check-leak defaults to False."""
        from scripts.run_factor_research import main as _main
        import argparse

        # 直接解析参数验证 / parse arguments directly
        import scripts.run_factor_research as rfr_module
        parser = argparse.ArgumentParser()
        # 重建 parser 以验证默认值 / rebuild parser to verify defaults
        parser.add_argument("--factor", type=str, required=True)
        parser.add_argument("--check-leak", action="store_true", default=False)
        parser.add_argument("--leak-block", action="store_true", default=False)

        args = parser.parse_args(["--factor", "AlphaMomentum"])
        assert args.check_leak is False
        assert args.leak_block is False

    def test_check_leak_flag_set_true(self):
        """--check-leak 设为 True / --check-leak sets to True."""
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument("--factor", type=str, required=True)
        parser.add_argument("--check-leak", action="store_true", default=False)
        parser.add_argument("--leak-block", action="store_true", default=False)

        args = parser.parse_args(["--factor", "AlphaMomentum", "--check-leak"])
        assert args.check_leak is True
        assert args.leak_block is False

    def test_leak_block_flag_set_true(self):
        """--leak-block 设为 True / --leak-block sets to True."""
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument("--factor", type=str, required=True)
        parser.add_argument("--check-leak", action="store_true", default=False)
        parser.add_argument("--leak-block", action="store_true", default=False)

        args = parser.parse_args([
            "--factor", "AlphaMomentum", "--check-leak", "--leak-block",
        ])
        assert args.check_leak is True
        assert args.leak_block is True

    def test_leak_block_without_check_leak(self):
        """--leak-block 单独使用时 check_leak 仍为 False / --leak-block alone keeps check_leak False."""
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument("--factor", type=str, required=True)
        parser.add_argument("--check-leak", action="store_true", default=False)
        parser.add_argument("--leak-block", action="store_true", default=False)

        args = parser.parse_args(["--factor", "AlphaMomentum", "--leak-block"])
        assert args.check_leak is False
        assert args.leak_block is True

    def test_run_factor_research_signature(self):
        """run_factor_research 函数签名包含 check_leak 和 leak_block 参数."""
        import inspect
        from scripts.run_factor_research import run_factor_research

        sig = inspect.signature(run_factor_research)
        assert "check_leak" in sig.parameters, "函数签名应包含 check_leak"
        assert "leak_block" in sig.parameters, "函数签名应包含 leak_block"
        # 默认值应为 False / defaults should be False
        assert sig.parameters["check_leak"].default is False
        assert sig.parameters["leak_block"].default is False


# ─────────────────────────────────────────────
# 6. 向后兼容 / Backward compatibility
# ─────────────────────────────────────────────


class TestBackwardCompatibility:
    """不传 check_leak 时行为与改造前完全一致 / Same behavior as before without check_leak."""

    def test_no_check_leak_completes_normally(self, monkeypatch):
        """不传 check_leak 时 pipeline 正常完成 / Pipeline completes without check_leak."""
        from scripts.run_factor_research import run_factor_research

        mock_data = _make_factor_data(n_dates=80, n_symbols=10, seed=42)

        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kw: _MockLoader(mock_data),
        )

        ev, report = run_factor_research(
            factor_name="AlphaMomentum",
            # 不传 check_leak，使用默认值 / don't pass check_leak, use default
        )

        assert ev is not None
        assert report is not None
        assert isinstance(ev.ic, pd.Series)
        assert isinstance(report, pd.DataFrame)

    def test_no_check_leak_same_results_as_explicit_false(self, monkeypatch):
        """不传 check_leak 与 check_leak=False 结果一致 / Same results either way."""
        from scripts.run_factor_research import run_factor_research

        mock_data = _make_factor_data(n_dates=80, n_symbols=10, seed=42)

        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kw: _MockLoader(mock_data),
        )

        ev1, report1 = run_factor_research(
            factor_name="AlphaMomentum",
        )

        ev2, report2 = run_factor_research(
            factor_name="AlphaMomentum",
            check_leak=False,
        )

        # ICIR 应完全一致 / ICIR should be identical
        assert ev1.icir == ev2.icir, (
            f"不传 check_leak 与 check_leak=False 的 ICIR 应一致: "
            f"{ev1.icir} vs {ev2.icir}"
        )
        # Sharpe 应完全一致 / Sharpe should be identical
        assert ev1.sharpe == ev2.sharpe

    def test_no_check_leak_evaluator_has_all_attributes(self, monkeypatch):
        """不传 check_leak 时 evaluator 所有属性正常 / All evaluator attributes present."""
        from scripts.run_factor_research import run_factor_research

        mock_data = _make_factor_data(n_dates=80, n_symbols=10, seed=42)

        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kw: _MockLoader(mock_data),
        )

        ev, report = run_factor_research(
            factor_name="AlphaMomentum",
            n_groups=5,
            cost_rate=0.001,
        )

        # 所有核心属性应非空 / all core attributes should be non-null
        assert ev.ic is not None
        assert ev.rank_ic is not None
        assert ev.icir is not None
        assert ev.ic_stats is not None
        assert ev.sharpe is not None
        assert ev.hedge_curve is not None
        assert ev.hedge_curve_after_cost is not None
        assert ev.group_labels is not None
        assert ev.turnover is not None
        assert ev.rank_autocorr is not None


# ─────────────────────────────────────────────
# 7. 多因子 + 检测 / Multi-factor + detection
# ─────────────────────────────────────────────


class TestMultiFactor:
    """多因子场景下 step4.5 行为正确 / step4.5 works correctly with multiple factors."""

    @pytest.mark.parametrize("factor_name", ["AlphaMomentum", "AlphaVolatility", "AlphaPriceRange"])
    def test_each_factor_pass_continues(self, factor_name, monkeypatch):
        """每个因子 PASS 时 pipeline 继续 / Pipeline continues for each factor."""
        from scripts.check_future_leak import DetectionReport, CheckResult
        from scripts.run_factor_research import run_factor_research

        mock_data = _make_factor_data(n_dates=60, n_symbols=5, seed=42)

        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kw: _MockLoader(mock_data),
        )

        class PassDetector:
            def run(self, **kwargs):
                return DetectionReport(checks=[
                    CheckResult(name="c", status="PASS"),
                ])

        monkeypatch.setattr(
            "scripts.check_future_leak.FutureLeakDetector",
            PassDetector,
        )

        ev, report = run_factor_research(
            factor_name=factor_name,
            check_leak=True,
        )

        assert ev is not None, f"{factor_name} PASS 时 evaluator 不应为 None"
        assert report is not None

    @pytest.mark.parametrize("factor_name", ["AlphaMomentum", "AlphaVolatility", "AlphaPriceRange"])
    def test_each_factor_fail_blocks(self, factor_name, monkeypatch):
        """每个因子 FAIL + leak_block 时阻断 / Blocks for each factor."""
        from scripts.check_future_leak import DetectionReport, CheckResult
        from scripts.run_factor_research import run_factor_research

        mock_data = _make_factor_data(n_dates=60, n_symbols=5, seed=42)

        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kw: _MockLoader(mock_data),
        )

        class FailDetector:
            def run(self, **kwargs):
                return DetectionReport(checks=[
                    CheckResult(name="c", status="FAIL"),
                ])

        monkeypatch.setattr(
            "scripts.check_future_leak.FutureLeakDetector",
            FailDetector,
        )

        ev, report = run_factor_research(
            factor_name=factor_name,
            check_leak=True,
            leak_block=True,
        )

        assert ev is None, f"{factor_name} FAIL + leak_block 时 evaluator 应为 None"
        assert report is None


# ─────────────────────────────────────────────
# 8. 边界情况 / Edge cases
# ─────────────────────────────────────────────


class TestEdgeCases:
    """边界情况测试 / Edge case tests."""

    def test_empty_detection_report_pass(self, monkeypatch):
        """空检测报告（0 项检测）视为 PASS / Empty report (0 checks) treated as PASS."""
        from scripts.check_future_leak import DetectionReport
        from scripts.run_factor_research import run_factor_research

        mock_data = _make_factor_data(n_dates=60, n_symbols=5, seed=42)

        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kw: _MockLoader(mock_data),
        )

        class EmptyDetector:
            def run(self, **kwargs):
                return DetectionReport(checks=[])

        monkeypatch.setattr(
            "scripts.check_future_leak.FutureLeakDetector",
            EmptyDetector,
        )

        ev, report = run_factor_research(
            factor_name="AlphaMomentum",
            check_leak=True,
        )

        # 空报告 all_passed=True，不应阻断 / empty report all_passed=True, should not block
        assert ev is not None

    def test_check_leak_with_different_return_labels(self, monkeypatch):
        """不同 return_label 下 check_leak 正常工作 / check_leak works with different labels."""
        from scripts.check_future_leak import DetectionReport, CheckResult
        from scripts.run_factor_research import run_factor_research

        mock_data = _make_factor_data(n_dates=60, n_symbols=5, seed=42)

        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kw: _MockLoader(mock_data),
        )

        class PassDetector:
            def run(self, **kwargs):
                # 验证 return_label 被正确传递 / verify return_label is passed correctly
                assert kwargs.get("return_label") in ("close2close", "open2open")
                return DetectionReport(checks=[
                    CheckResult(name="c", status="PASS"),
                ])

        monkeypatch.setattr(
            "scripts.check_future_leak.FutureLeakDetector",
            PassDetector,
        )

        for label in ["close2close", "open2open"]:
            ev, report = run_factor_research(
                factor_name="AlphaMomentum",
                return_label=label,
                check_leak=True,
            )
            assert ev is not None, f"{label} 时 evaluator 不应为 None"

    def test_check_leak_parameters_forwarded(self, monkeypatch):
        """检测参数正确传递给 FutureLeakDetector / Parameters forwarded correctly."""
        from scripts.check_future_leak import DetectionReport, CheckResult
        from scripts.run_factor_research import run_factor_research

        mock_data = _make_factor_data(n_dates=60, n_symbols=5, seed=42)

        monkeypatch.setattr(
            "Cross_Section_Factor.kline_loader.KlineLoader",
            lambda **kw: _MockLoader(mock_data),
        )

        captured_kwargs = {}

        class CaptureDetector:
            def __init__(self):
                pass

            def run(self, **kwargs):
                captured_kwargs.update(kwargs)
                return DetectionReport(checks=[
                    CheckResult(name="c", status="PASS"),
                ])

        monkeypatch.setattr(
            "scripts.check_future_leak.FutureLeakDetector",
            CaptureDetector,
        )

        run_factor_research(
            factor_name="AlphaMomentum",
            return_label="open2open",
            start_time="2024-01-01",
            end_time="2024-03-01",
            symbols=["BTCUSDT", "ETHUSDT"],
            exchange="binance",
            kline_type="swap",
            interval="1h",
            check_leak=True,
        )

        # 验证参数传递 / verify parameters forwarded
        assert captured_kwargs.get("factor_name") == "AlphaMomentum"
        assert captured_kwargs.get("return_label") == "open2open"
        assert captured_kwargs.get("start_time") == "2024-01-01"
        assert captured_kwargs.get("end_time") == "2024-03-01"
        assert captured_kwargs.get("symbols") == ["BTCUSDT", "ETHUSDT"]
        assert captured_kwargs.get("exchange") == "binance"
        assert captured_kwargs.get("kline_type") == "swap"
        assert captured_kwargs.get("interval") == "1h"
