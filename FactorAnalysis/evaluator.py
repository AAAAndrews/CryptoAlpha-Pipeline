"""
FactorAnalysis/evaluator.py — FactorEvaluator 主类

编排因子绩效检验全流程：IC 分析、分组收益、净值曲线、成本扣除、绩效比率。
Orchestrates full factor evaluation pipeline: IC analysis, grouping returns,
equity curves, cost deduction, performance ratios.
"""

import pandas as pd

from .metrics import calc_ic, calc_rank_ic, calc_icir, calc_sharpe, calc_calmar, calc_sortino
from .grouping import quantile_group
from .portfolio import calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve
from .cost import deduct_cost


class FactorEvaluator:
    """
    因子绩效检验编排器 / Factor performance evaluation orchestrator.

    依次执行 IC 分析、分组、净值曲线构建、成本扣除、绩效比率计算，
    将所有结果存储在实例属性中，供 generate_report 消费。
    Runs IC analysis, grouping, equity curve construction, cost deduction,
    and performance ratio calculation; stores all results as instance attributes
    for generate_report to consume.

    Parameters / 参数:
        factor: 因子值，MultiIndex (timestamp, symbol)
                Factor values, MultiIndex (timestamp, symbol)
        returns: 前向收益率，MultiIndex (timestamp, symbol)
                 Forward returns, MultiIndex (timestamp, symbol)
        n_groups: 分组数量，默认 5
                  Number of quantile groups, default 5
        top_k: 做多最高的几组，默认 1
               Number of top groups to long, default 1
        bottom_k: 做空最低的几组，默认 1
                  Number of bottom groups to short, default 1
        cost_rate: 每次换仓的交易成本比例，默认 0.001
                   Transaction cost rate per rebalance, default 0.001
        risk_free_rate: 年化无风险利率，默认 0.0
                        Annualized risk-free rate, default 0.0
        periods_per_year: 年化交易日数，默认 252
                          Trading periods per year, default 252
    """

    def __init__(
        self,
        factor: pd.Series,
        returns: pd.Series,
        n_groups: int = 5,
        top_k: int = 1,
        bottom_k: int = 1,
        cost_rate: float = 0.001,
        risk_free_rate: float = 0.0,
        periods_per_year: int = 252,
    ):
        self.factor = factor
        self.returns = returns
        self.n_groups = n_groups
        self.top_k = top_k
        self.bottom_k = bottom_k
        self.cost_rate = cost_rate
        self.risk_free_rate = risk_free_rate
        self.periods_per_year = periods_per_year

        # IC 指标 / IC metrics
        self.ic: pd.Series | None = None
        self.rank_ic: pd.Series | None = None
        self.icir: float | None = None

        # 分组标签 / Group labels
        self.group_labels: pd.Series | None = None

        # 净值曲线 / Equity curves
        self.long_curve: pd.Series | None = None
        self.short_curve: pd.Series | None = None
        self.hedge_curve: pd.Series | None = None

        # 成本扣除后净值曲线 / Cost-adjusted equity curves
        self.hedge_curve_after_cost: pd.Series | None = None

        # 绩效比率 / Performance ratios
        self.sharpe: float | None = None
        self.calmar: float | None = None
        self.sortino: float | None = None
        self.sharpe_after_cost: float | None = None
        self.calmar_after_cost: float | None = None
        self.sortino_after_cost: float | None = None

    def run(self) -> "FactorEvaluator":
        """
        执行全部分析步骤并返回自身 / Run all analysis steps and return self.

        按顺序执行：IC 分析 → 分组 → 净值曲线 → 成本扣除 → 绩效比率。
        Executes in order: IC analysis → grouping → equity curves → cost deduction → ratios.

        Returns / 返回:
            self，支持链式调用 / self, for method chaining
        """
        self._compute_ic()
        self._compute_grouping()
        self._compute_portfolio_curves()
        self._compute_cost_adjusted()
        self._compute_performance_ratios()
        return self

    # --- 分析步骤 / Analysis steps ---

    def _compute_ic(self) -> None:
        """计算 IC / Rank IC / ICIR / Compute IC / Rank IC / ICIR."""
        self.ic = calc_ic(self.factor, self.returns)
        self.rank_ic = calc_rank_ic(self.factor, self.returns)
        self.icir = calc_icir(self.factor, self.returns)

    def _compute_grouping(self) -> None:
        """计算分位数分组标签 / Compute quantile group labels."""
        self.group_labels = quantile_group(self.factor, n_groups=self.n_groups)

    def _compute_portfolio_curves(self) -> None:
        """计算多/空/对冲净值曲线 / Compute long/short/hedge equity curves."""
        self.long_curve = calc_long_only_curve(
            self.factor, self.returns,
            n_groups=self.n_groups, top_k=self.top_k,
        )
        self.short_curve = calc_short_only_curve(
            self.factor, self.returns,
            n_groups=self.n_groups, bottom_k=self.bottom_k,
        )
        self.hedge_curve = calc_top_bottom_curve(
            self.factor, self.returns,
            n_groups=self.n_groups, top_k=self.top_k, bottom_k=self.bottom_k,
        )

    def _compute_cost_adjusted(self) -> None:
        """对对冲净值曲线扣除交易成本 / Deduct transaction cost from hedge equity curve."""
        # 从对冲净值曲线反推日收益率，再扣除成本
        # Derive daily returns from hedge curve, then deduct cost
        hedge_daily = self.hedge_curve.pct_change().fillna(0.0)
        self.hedge_curve_after_cost = deduct_cost(hedge_daily, cost_rate=self.cost_rate)

    def _compute_performance_ratios(self) -> None:
        """计算绩效比率（成本前 + 成本后）/ Compute performance ratios (before & after cost)."""
        rf = self.risk_free_rate
        pp = self.periods_per_year

        # 成本前 / before cost
        self.sharpe = calc_sharpe(self.hedge_curve, risk_free_rate=rf, periods_per_year=pp)
        self.calmar = calc_calmar(self.hedge_curve, periods_per_year=pp)
        self.sortino = calc_sortino(self.hedge_curve, risk_free_rate=rf, periods_per_year=pp)

        # 成本后 / after cost
        self.sharpe_after_cost = calc_sharpe(
            self.hedge_curve_after_cost, risk_free_rate=rf, periods_per_year=pp,
        )
        self.calmar_after_cost = calc_calmar(self.hedge_curve_after_cost, periods_per_year=pp)
        self.sortino_after_cost = calc_sortino(
            self.hedge_curve_after_cost, risk_free_rate=rf, periods_per_year=pp,
        )
