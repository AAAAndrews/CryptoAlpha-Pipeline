"""Task 16 验证脚本 — FactorAnalysis 模块结构"""
import os
import sys

# 确保项目根目录在 Python 路径中
# Ensure project root is in Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# 1. 模块可导入
import FactorAnalysis
print("1. Import OK")

# 2. __all__ 已定义且非空
assert hasattr(FactorAnalysis, "__all__"), "__all__ missing"
assert len(FactorAnalysis.__all__) > 0, "__all__ is empty"
print(f"2. __all__ has {len(FactorAnalysis.__all__)} exports")

# 3. __all__ 包含所有预期的公共 API 名称
expected_names = {
    "calc_ic", "calc_rank_ic", "calc_icir",
    "calc_sharpe", "calc_calmar", "calc_sortino", "calc_ic_stats",
    "quantile_group",
    "calc_long_only_curve", "calc_short_only_curve", "calc_top_bottom_curve",
    "calc_portfolio_curves",
    "deduct_cost",
    "calc_returns",
    "align_factor_returns",
    "calc_turnover", "calc_rank_autocorr",
    "check_data_quality",
    "calc_neutralized_curve",
    "FactorEvaluator",
    "generate_report",
}
assert set(FactorAnalysis.__all__) == expected_names, \
    f"Mismatch: got {set(FactorAnalysis.__all__)}, expected {expected_names}"
print("3. __all__ matches expected names")

# 4. 所有子模块文件存在
expected_files = [
    "FactorAnalysis/__init__.py",
    "FactorAnalysis/metrics.py",
    "FactorAnalysis/grouping.py",
    "FactorAnalysis/portfolio.py",
    "FactorAnalysis/cost.py",
    "FactorAnalysis/evaluator.py",
    "FactorAnalysis/report.py",
]
for f in expected_files:
    assert os.path.isfile(f), f"Missing: {f}"
print("4. All submodule files exist")

print("ALL CHECKS PASSED")
