"""
FactorAnalysis — 因子绩效检验模块 / Factor performance analysis module

独立于 DEAP，使用 pandas DataFrame 作为主要数据格式，兼容 FactorLib 输出。
Decoupled from DEAP, uses pandas DataFrame as primary data format, compatible with FactorLib output.
"""

from .metrics import calc_ic, calc_rank_ic, calc_icir, calc_sharpe, calc_calmar, calc_sortino
from .grouping import quantile_group
from .portfolio import calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve
from .cost import deduct_cost
from .evaluator import FactorEvaluator

__all__ = [
    # 核心指标 / Core metrics
    "calc_ic",
    "calc_rank_ic",
    "calc_icir",
    "calc_sharpe",
    "calc_calmar",
    "calc_sortino",
    # 分组分析 / Grouping analysis
    "quantile_group",
    # 净值曲线 / Portfolio equity curves
    "calc_long_only_curve",
    "calc_short_only_curve",
    "calc_top_bottom_curve",
    # 交易成本 / Transaction costs
    "deduct_cost",
    # 编排器 / Orchestrator
    "FactorEvaluator",
    # 报告输出 / Report output
    "generate_report",
]
