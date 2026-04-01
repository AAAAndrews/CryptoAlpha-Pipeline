"""
FactorAnalysis — 因子绩效检验模块 / Factor performance analysis module

独立于 DEAP，使用 pandas DataFrame 作为主要数据格式，兼容 FactorLib 输出。
Decoupled from DEAP, uses pandas DataFrame as primary data format, compatible with FactorLib output.
"""

from .metrics import calc_ic, calc_rank_ic, calc_icir, calc_sharpe, calc_calmar, calc_sortino, calc_ic_stats
from .grouping import quantile_group
from .portfolio import calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve
from .cost import deduct_cost
from .returns import calc_returns
from .alignment import align_factor_returns
from .turnover import calc_turnover, calc_rank_autocorr
from .data_quality import check_data_quality
from .neutralize import calc_neutralized_curve
from .evaluator import FactorEvaluator
from .report import generate_report

__all__ = [
    # 核心指标 / Core metrics
    "calc_ic",
    "calc_rank_ic",
    "calc_icir",
    "calc_sharpe",
    "calc_calmar",
    "calc_sortino",
    "calc_ic_stats",
    # 分组分析 / Grouping analysis
    "quantile_group",
    # 净值曲线 / Portfolio equity curves
    "calc_long_only_curve",
    "calc_short_only_curve",
    "calc_top_bottom_curve",
    # 交易成本 / Transaction costs
    "deduct_cost",
    # 收益矩阵 / Return matrix
    "calc_returns",
    # 因子对齐 / Factor-return alignment
    "align_factor_returns",
    # 换手率 / Turnover
    "calc_turnover",
    "calc_rank_autocorr",
    # 数据质量 / Data quality
    "check_data_quality",
    # 中性化 / Neutralization
    "calc_neutralized_curve",
    # 编排器 / Orchestrator
    "FactorEvaluator",
    # 报告输出 / Report output
    "generate_report",
]
