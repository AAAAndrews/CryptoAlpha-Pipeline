"""
FactorAnalysis/report.py — 绩效报告汇总输出
Performance report summary output.

生成包含所有关键指标的摘要 DataFrame，支持按板块选择性输出。
向后兼容：select=None 时保持原有全量行为。
Generates a summary DataFrame containing all key metrics.
Supports selective output by section. Backward compatible: select=None gives full report.
"""

import pandas as pd


def generate_report(evaluator, select=None) -> pd.DataFrame:
    """
    从 FactorEvaluator 实例生成摘要报告 DataFrame，支持按板块筛选。
    Generate a summary report DataFrame from a FactorEvaluator instance, with optional section filtering.

    委托给 evaluator.generate_report(select=...)，确保模块级函数与实例方法行为一致。
    Delegates to evaluator.generate_report(select=...), ensuring module-level function
    stays consistent with the instance method.

    Parameters / 参数:
        evaluator: 已调用 run_all() 或 run_quick() 的 FactorEvaluator 实例
                   A FactorEvaluator instance that has called run_all() or run_quick()
        select: 要包含的板块列表，None 表示全部。
                可选值: "metrics", "grouping", "curves", "turnover", "neutralize"
                Sections to include, None for all.
                Valid values: "metrics", "grouping", "curves", "turnover", "neutralize"

    Returns / 返回:
        pd.DataFrame — 单行，列为指标名 / single-row, columns are metric names

    Raises / 异常:
        ValueError: evaluator 尚未调用任何 run 方法 / evaluator not yet run
    """
    return evaluator.generate_report(select=select)
