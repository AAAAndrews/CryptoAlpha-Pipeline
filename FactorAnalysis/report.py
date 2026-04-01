"""
FactorAnalysis/report.py — 绩效报告汇总输出
Performance report summary output.

生成包含所有关键指标的摘要 DataFrame。
向后兼容：保持原有列集合不变。
Generates a summary DataFrame containing all key metrics.
Backward compatible: preserves original column set.
"""

import pandas as pd


def generate_report(evaluator) -> pd.DataFrame:
    """
    从 FactorEvaluator 实例生成摘要报告 DataFrame。
    Generate a summary report DataFrame from a FactorEvaluator instance.

    消费 evaluator 上全部已计算的属性，汇总为一个单行 DataFrame，
    列为各关键指标名，值为对应数值。
    Consumes all computed attributes on the evaluator, summarises into
    a single-row DataFrame with metric names as columns.

    Parameters / 参数:
        evaluator: 已调用 run() 的 FactorEvaluator 实例
                   A FactorEvaluator instance that has already called run()

    Returns / 返回:
        pd.DataFrame — 单行，列为指标名 / single-row, columns are metric names

    Raises / 异常:
        ValueError: evaluator 尚未调用 run() / evaluator.run() not yet called
    """
    # 校验 / Validation
    if evaluator.icir is None:
        raise ValueError(
            "FactorEvaluator has not been run yet. "
            "Please call evaluator.run() before generating the report."
        )

    rows = {}

    # IC 指标 / IC metrics
    ic = evaluator.ic
    rank_ic = evaluator.rank_ic
    rows["IC_mean"] = ic.mean()
    rows["IC_std"] = ic.std()
    rows["RankIC_mean"] = rank_ic.mean()
    rows["RankIC_std"] = rank_ic.std()
    rows["ICIR"] = evaluator.icir

    # 净值曲线期末收益 / Equity curve terminal returns
    rows["long_return"] = evaluator.long_curve.iloc[-1] - 1.0
    rows["short_return"] = evaluator.short_curve.iloc[-1] - 1.0
    rows["hedge_return"] = evaluator.hedge_curve.iloc[-1] - 1.0
    rows["hedge_return_after_cost"] = evaluator.hedge_curve_after_cost.iloc[-1] - 1.0

    # 绩效比率 / Performance ratios
    rows["sharpe"] = evaluator.sharpe
    rows["calmar"] = evaluator.calmar
    rows["sortino"] = evaluator.sortino
    rows["sharpe_after_cost"] = evaluator.sharpe_after_cost
    rows["calmar_after_cost"] = evaluator.calmar_after_cost
    rows["sortino_after_cost"] = evaluator.sortino_after_cost

    # 分析天数 / Number of analysis days
    rows["n_days"] = len(evaluator.hedge_curve)

    # 构造单行 DataFrame / Build single-row DataFrame
    return pd.DataFrame([rows])
