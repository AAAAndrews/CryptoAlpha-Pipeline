"""
FactorAnalysis/report.py 验证测试 / report.py validation tests

验证 generate_report 函数的正确性：导入、类型、列完整性、数值合理性。
Validates generate_report: import, type, column completeness, numerical sanity.
"""

import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import numpy as np
import pandas as pd

passed = 0
failed = 0


def check(name: str, condition: bool) -> None:
    global passed, failed
    if condition:
        passed += 1
        print(f"  [PASS] {name}")
    else:
        failed += 1
        print(f"  [FAIL] {name}")


# --- 构造合成数据 / Build synthetic data ---

np.random.seed(42)
dates = pd.date_range("2024-01-01", periods=100, freq="B")
symbols = [f"S{i}" for i in range(20)]
idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])

factor = pd.Series(np.random.randn(len(idx)), index=idx, dtype=float)
# 带一定预测能力的因子 / factor with some predictive power
returns = 0.01 * factor + 0.02 * np.random.randn(len(idx))
returns.index = idx

# --- 测试开始 / Tests ---

print("=== report.py validation ===")

# 1. 导入 / Import
from FactorAnalysis.report import generate_report
check("import generate_report", callable(generate_report))

from FactorAnalysis import generate_report as gr_public
check("public export generate_report", gr_public is generate_report)

# 2. 公共导出 / __all__ export
import FactorAnalysis
check("__all__ contains generate_report", "generate_report" in FactorAnalysis.__all__)

# 3. 未调用 run() 抛出 ValueError
from FactorAnalysis.evaluator import FactorEvaluator
ev = FactorEvaluator(factor, returns)
try:
    generate_report(ev)
    check("ValueError on unrun evaluator", False)
except ValueError:
    check("ValueError on unrun evaluator", True)

# 4. 正常运行 / Normal run
ev.run()
report = generate_report(ev)

# 5. 返回类型 / Return type
check("returns DataFrame", isinstance(report, pd.DataFrame))
check("single row", len(report) == 1)

# 6. 期望列 / Expected columns
expected_cols = {
    "IC_mean", "IC_std", "RankIC_mean", "RankIC_std", "ICIR",
    "long_return", "short_return", "hedge_return", "hedge_return_after_cost",
    "sharpe", "calmar", "sortino",
    "sharpe_after_cost", "calmar_after_cost", "sortino_after_cost",
    "n_days",
}
check("all expected columns present", expected_cols.issubset(set(report.columns)))
check("column count correct", len(report.columns) == len(expected_cols))

# 7. 数值类型 / Value types
check("n_days is int-like", report["n_days"].iloc[0] == 100)
check("ICIR is float", isinstance(report["ICIR"].iloc[0], (float, np.floating)))

# 8. 数值有限 / Values are finite
for col in expected_cols - {"n_days"}:
    val = report[col].iloc[0]
    check(f"{col} is finite", np.isfinite(val))

# 9. 逻辑一致性 / Logical consistency
# 成本后收益 <= 成本前收益 / after-cost <= before-cost
check("hedge_return_after_cost <= hedge_return",
      report["hedge_return_after_cost"].iloc[0] <= report["hedge_return"].iloc[0])
check("sharpe_after_cost <= sharpe",
      report["sharpe_after_cost"].iloc[0] <= report["sharpe"].iloc[0])

# 10. 自定义参数 / Custom params
ev2 = FactorEvaluator(factor, returns, n_groups=3, top_k=1, bottom_k=1, cost_rate=0.01)
ev2.run()
report2 = generate_report(ev2)
check("custom params report ok", isinstance(report2, pd.DataFrame) and len(report2) == 1)
check("custom params columns ok", expected_cols.issubset(set(report2.columns)))
# 更高成本 → 更低收益 / higher cost → lower return
check("higher cost → lower hedge return",
      report2["hedge_return_after_cost"].iloc[0] <= report["hedge_return_after_cost"].iloc[0])

print(f"\n=== Results: {passed} passed, {failed} failed ===")
sys.exit(1 if failed else 0)
