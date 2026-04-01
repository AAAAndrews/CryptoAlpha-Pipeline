"""
Task 22 — FactorAnalysis 公共导出完整性验证测试
FactorAnalysis public export completeness verification tests

验证所有新增函数已正确导出到 FactorAnalysis 包的公共 API。
Verify all new functions are correctly exported in the FactorAnalysis package public API.
"""

import importlib
import inspect

import pytest


class TestPublicImports:
    """验证所有新增函数可通过 FactorAnalysis 包导入 / Verify new functions importable from package"""

    def test_import_calc_ic_stats(self):
        from FactorAnalysis import calc_ic_stats
        assert callable(calc_ic_stats)

    def test_import_calc_returns(self):
        from FactorAnalysis import calc_returns
        assert callable(calc_returns)

    def test_import_align_factor_returns(self):
        from FactorAnalysis import align_factor_returns
        assert callable(align_factor_returns)

    def test_import_calc_turnover(self):
        from FactorAnalysis import calc_turnover
        assert callable(calc_turnover)

    def test_import_calc_rank_autocorr(self):
        from FactorAnalysis import calc_rank_autocorr
        assert callable(calc_rank_autocorr)

    def test_import_check_data_quality(self):
        from FactorAnalysis import check_data_quality
        assert callable(check_data_quality)

    def test_import_calc_neutralized_curve(self):
        from FactorAnalysis import calc_neutralized_curve
        assert callable(calc_neutralized_curve)


class TestAllExport:
    """验证 __all__ 列表完整性与正确性 / Verify __all__ list completeness and correctness"""

    EXPECTED_NAMES = {
        # 核心指标 / Core metrics
        "calc_ic", "calc_rank_ic", "calc_icir",
        "calc_sharpe", "calc_calmar", "calc_sortino",
        "calc_ic_stats",
        # 分组分析 / Grouping
        "quantile_group",
        # 净值曲线 / Portfolio curves
        "calc_long_only_curve", "calc_short_only_curve", "calc_top_bottom_curve",
        # 交易成本 / Transaction costs
        "deduct_cost",
        # 收益矩阵 / Return matrix
        "calc_returns",
        # 因子对齐 / Alignment
        "align_factor_returns",
        # 换手率 / Turnover
        "calc_turnover", "calc_rank_autocorr",
        # 数据质量 / Data quality
        "check_data_quality",
        # 中性化 / Neutralization
        "calc_neutralized_curve",
        # 编排器 / Orchestrator
        "FactorEvaluator",
        # 报告输出 / Report
        "generate_report",
    }

    def test_all_contains_expected(self):
        import FactorAnalysis
        all_list = set(FactorAnalysis.__all__)
        for name in self.EXPECTED_NAMES:
            assert name in all_list, f"{name} missing from __all__"

    def test_all_count(self):
        import FactorAnalysis
        assert len(FactorAnalysis.__all__) == len(self.EXPECTED_NAMES)

    def test_all_entries_are_importable(self):
        import FactorAnalysis
        for name in FactorAnalysis.__all__:
            assert hasattr(FactorAnalysis, name), f"{name} in __all__ but not importable"

    def test_new_exports_in_all(self):
        """确认任务 22 要求的 6 个新导出全部在 __all__ 中"""
        import FactorAnalysis
        required = [
            "calc_ic_stats",
            "calc_turnover",
            "calc_rank_autocorr",
            "check_data_quality",
            "calc_neutralized_curve",
            "calc_returns",
            "align_factor_returns",
        ]
        for name in required:
            assert name in FactorAnalysis.__all__, f"{name} not in __all__"


class TestSourceModuleConsistency:
    """验证导出函数来自正确的子模块 / Verify exported functions come from correct submodules"""

    def test_calc_ic_stats_from_metrics(self):
        from FactorAnalysis.metrics import calc_ic_stats as _orig
        from FactorAnalysis import calc_ic_stats as _reexport
        assert _orig is _reexport

    def test_calc_turnover_from_turnover(self):
        from FactorAnalysis.turnover import calc_turnover as _orig
        from FactorAnalysis import calc_turnover as _reexport
        assert _orig is _reexport

    def test_calc_rank_autocorr_from_turnover(self):
        from FactorAnalysis.turnover import calc_rank_autocorr as _orig
        from FactorAnalysis import calc_rank_autocorr as _reexport
        assert _orig is _reexport

    def test_check_data_quality_from_data_quality(self):
        from FactorAnalysis.data_quality import check_data_quality as _orig
        from FactorAnalysis import check_data_quality as _reexport
        assert _orig is _reexport

    def test_calc_neutralized_curve_from_neutralize(self):
        from FactorAnalysis.neutralize import calc_neutralized_curve as _orig
        from FactorAnalysis import calc_neutralized_curve as _reexport
        assert _orig is _reexport

    def test_calc_returns_from_returns(self):
        from FactorAnalysis.returns import calc_returns as _orig
        from FactorAnalysis import calc_returns as _reexport
        assert _orig is _reexport

    def test_align_factor_returns_from_alignment(self):
        from FactorAnalysis.alignment import align_factor_returns as _orig
        from FactorAnalysis import align_factor_returns as _reexport
        assert _orig is _reexport


class TestExistingExportsUnchanged:
    """验证已有导出未受影响 / Verify existing exports are not affected"""

    def test_existing_metrics(self):
        from FactorAnalysis import calc_ic, calc_rank_ic, calc_icir
        assert all(callable(f) for f in [calc_ic, calc_rank_ic, calc_icir])

    def test_existing_portfolio(self):
        from FactorAnalysis import calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve
        assert all(callable(f) for f in [calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve])

    def test_existing_grouping(self):
        from FactorAnalysis import quantile_group
        assert callable(quantile_group)

    def test_existing_cost(self):
        from FactorAnalysis import deduct_cost
        assert callable(deduct_cost)

    def test_existing_evaluator(self):
        from FactorAnalysis import FactorEvaluator
        assert inspect.isclass(FactorEvaluator)

    def test_existing_report(self):
        from FactorAnalysis import generate_report
        assert callable(generate_report)
