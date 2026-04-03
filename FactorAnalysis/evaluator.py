"""
FactorAnalysis/evaluator.py — FactorEvaluator Tear Sheet 分层编排器
FactorEvaluator — Tear Sheet layered evaluation orchestrator.

将因子绩效检验拆分为独立子分析步骤，支持选择性执行和灵活组合。
Splits factor performance evaluation into independent sub-analysis steps,
supporting selective execution and flexible combination.

子模块 / Sub-modules:
    - run_metrics():     IC / RankIC / ICIR / IC 统计显著性
    - run_grouping():    分位数分组标签
    - run_curves():      净值曲线 + 成本扣除 + 绩效比率
    - run_turnover():    分组换手率 + 排名自相关
    - run_neutralize():  分组中性化净值曲线
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from .metrics import (
    calc_ic, calc_rank_ic, calc_icir, calc_sharpe, calc_calmar, calc_sortino,
    calc_ic_stats,
)
from .grouping import quantile_group
from .portfolio import calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve
from .cost import deduct_cost
from .turnover import calc_turnover, calc_rank_autocorr
from .neutralize import calc_neutralized_curve


class FactorEvaluator:
    """
    因子绩效检验编排器（Tear Sheet 分层模式）/ Factor evaluation orchestrator (Tear Sheet layered mode).

    支持按需执行子分析步骤，各步骤可独立调用，也可通过 run_all() 一次性完成。
    Supports on-demand sub-analysis steps; each can be called independently
    or via run_all() for a one-shot full pipeline.

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
        chunk_size: 分块大小（每个块的时间截面数），默认 None（全量模式）。
                    设为正整数时启用分块处理以降低内存峰值。
                    Chunk size (number of timestamps per chunk), default None (full mode).
                    Set to a positive integer to enable chunked processing for lower memory peak.
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
        chunk_size: int | None = None,
    ):
        self.factor = factor
        self.returns = returns
        self.n_groups = n_groups
        self.top_k = top_k
        self.bottom_k = bottom_k
        self.cost_rate = cost_rate
        self.risk_free_rate = risk_free_rate
        self.periods_per_year = periods_per_year
        self.chunk_size = self._validate_chunk_size(chunk_size)

        # IC 指标 / IC metrics
        self.ic: pd.Series | None = None
        self.rank_ic: pd.Series | None = None
        self.icir: float | None = None
        self.ic_stats: pd.Series | None = None

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

        # 换手率指标 / Turnover metrics
        self.turnover: pd.DataFrame | None = None
        self.rank_autocorr: pd.Series | None = None

        # 中性化净值曲线 / Neutralized equity curve
        self.neutralized_curve: pd.Series | None = None

    # --- 内部校验 / Internal validation ---

    @staticmethod
    def _validate_chunk_size(chunk_size: int | None) -> int | None:
        """
        校验 chunk_size 参数合法性 / Validate chunk_size parameter.

        - None: 合法，表示全量模式 / Valid, means full (non-chunked) mode
        - 正整数: 合法 / Valid positive integer
        - 0、负数、浮点数: 抛出 ValueError / Raises ValueError for 0, negative, or non-integer

        Parameters / 参数:
            chunk_size: 待校验的分块大小 / Chunk size to validate

        Returns / 返回:
            int | None: 校验后的值 / Validated value

        Raises / 异常:
            ValueError: chunk_size 不是正整数或 None
            TypeError: chunk_size 是浮点数（非整数值）
        """
        if chunk_size is None:
            return None
        # 浮点数类型检查：排除 3.0 这类隐式整数 / reject float even if value is integer-like
        if isinstance(chunk_size, float):
            if not chunk_size.is_integer():
                raise ValueError(
                    f"chunk_size 必须为正整数或 None，当前值: {chunk_size}"
                )
            # 隐式整数转正 / convert implicit integer to int
            chunk_size = int(chunk_size)
        if not isinstance(chunk_size, int) or isinstance(chunk_size, bool):
            raise TypeError(
                f"chunk_size 必须为 int 或 None，当前类型: {type(chunk_size).__name__}"
            )
        if chunk_size < 1:
            raise ValueError(
                f"chunk_size 必须 >= 1 或为 None，当前值: {chunk_size}"
            )
        return chunk_size

    # --- 子分析步骤 / Sub-analysis steps ---

    def run_metrics(self) -> "FactorEvaluator":
        """
        IC 分析：计算 IC / Rank IC / ICIR / IC 统计显著性。
        IC analysis: compute IC / Rank IC / ICIR / IC statistical significance.

        Returns / 返回:
            self，支持链式调用 / self, for method chaining
        """
        self.ic = calc_ic(self.factor, self.returns)
        self.rank_ic = calc_rank_ic(self.factor, self.returns)
        self.icir = calc_icir(self.factor, self.returns)
        self.ic_stats = calc_ic_stats(self.factor, self.returns)
        return self

    def run_grouping(self) -> "FactorEvaluator":
        """
        计算分位数分组标签 / Compute quantile group labels.

        Returns / 返回:
            self，支持链式调用 / self, for method chaining
        """
        self.group_labels = quantile_group(self.factor, n_groups=self.n_groups)
        return self

    def run_curves(self) -> "FactorEvaluator":
        """
        净值曲线 + 成本扣除 + 绩效比率 / Equity curves + cost deduction + performance ratios.

        依次计算：多/空/对冲净值 → 成本扣除 → Sharpe/Calmar/Sortino（成本前+成本后）。
        Computes: long/short/hedge curves → cost deduction → Sharpe/Calmar/Sortino (before & after cost).

        Returns / 返回:
            self，支持链式调用 / self, for method chaining
        """
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
        # 对冲净值曲线扣除交易成本 / deduct cost from hedge curve
        hedge_daily = self.hedge_curve.pct_change().fillna(0.0)
        self.hedge_curve_after_cost = deduct_cost(hedge_daily, cost_rate=self.cost_rate)

        # 绩效比率 / performance ratios
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
        return self

    def run_turnover(self) -> "FactorEvaluator":
        """
        换手率指标：分组换手率 + 因子排名自相关。
        Turnover metrics: quantile group turnover + factor rank autocorrelation.

        Returns / 返回:
            self，支持链式调用 / self, for method chaining
        """
        self.turnover = calc_turnover(self.factor, n_groups=self.n_groups)
        self.rank_autocorr = calc_rank_autocorr(self.factor)
        return self

    def run_neutralize(
        self,
        groups: "pd.Series | int | None" = None,
        demeaned: bool = True,
        group_adjust: bool = False,
        n_groups: int | None = None,
    ) -> "FactorEvaluator":
        """
        分组中性化净值曲线 / Group-neutralized equity curve.

        Parameters / 参数:
            groups: 中性化分组，None 时使用 self.n_groups
                    Neutralization groups, defaults to self.n_groups when None
            demeaned: 是否组内因子去均值，默认 True
                      Whether to demean factor within groups, default True
            group_adjust: 是否组内收益去均值，默认 False
                          Whether to adjust returns within groups, default False
            n_groups: 中性化后排名分组数，None 时使用 self.n_groups
                      Ranking groups after neutralization, defaults to self.n_groups when None

        Returns / 返回:
            self，支持链式调用 / self, for method chaining
        """
        if groups is None:
            groups = self.n_groups
        if n_groups is None:
            n_groups = self.n_groups
        self.neutralized_curve = calc_neutralized_curve(
            self.factor, self.returns,
            groups=groups, demeaned=demeaned, group_adjust=group_adjust, n_groups=n_groups,
        )
        return self

    # --- 编排方法 / Orchestration methods ---

    def run_all(self) -> "FactorEvaluator":
        """
        执行完整分析流程 / Run the full analysis pipeline.

        按顺序执行所有子分析步骤：
        metrics → grouping → curves → turnover → neutralize
        Executes all sub-analysis steps in order:
        metrics → grouping → curves → turnover → neutralize

        Returns / 返回:
            self，支持链式调用 / self, for method chaining
        """
        return (self.run_metrics()
                    .run_grouping()
                    .run_curves()
                    .run_turnover()
                    .run_neutralize())

    def run(self) -> "FactorEvaluator":
        """
        向后兼容：等同 run_all() / Backward compatible: equivalent to run_all().

        Returns / 返回:
            self，支持链式调用 / self, for method chaining
        """
        return self.run_all()

    def generate_report(self, select=None) -> pd.DataFrame:
        """
        生成选择性摘要报告 / Generate selective summary report.

        根据 select 参数选择包含的分析板块，未计算的板块字段填 NaN。
        Selects which analysis sections to include; uncomputed sections are filled with NaN.

        Parameters / 参数:
            select: 要包含的板块列表，None 表示全部。
                    可选值: "metrics", "grouping", "curves", "turnover", "neutralize"
                    Sections to include, None for all.
                    Valid values: "metrics", "grouping", "curves", "turnover", "neutralize"

        Returns / 返回:
            pd.DataFrame: 单行摘要报告，列为指标名 / Single-row summary, columns are metric names

        Raises / 异常:
            ValueError: select 包含无效的板块名称 / select contains invalid section names
        """
        valid_sections = {"metrics", "grouping", "curves", "turnover", "neutralize"}

        if select is None:
            sections = valid_sections
        else:
            sections = set(select)
            invalid = sections - valid_sections
            if invalid:
                raise ValueError(
                    f"Invalid sections: {invalid}. "
                    f"Valid values: {sorted(valid_sections)}"
                )

        rows = {}

        if "metrics" in sections:
            if self.ic is not None:
                rows["IC_mean"] = self.ic.mean()
                rows["IC_std"] = self.ic.std()
            if self.rank_ic is not None:
                rows["RankIC_mean"] = self.rank_ic.mean()
                rows["RankIC_std"] = self.rank_ic.std()
            if self.icir is not None:
                rows["ICIR"] = self.icir
            if self.ic_stats is not None:
                rows["IC_t_stat"] = self.ic_stats.get("t_stat", np.nan)
                rows["IC_p_value"] = self.ic_stats.get("p_value", np.nan)
                rows["IC_skew"] = self.ic_stats.get("IC_skew", np.nan)
                rows["IC_kurtosis"] = self.ic_stats.get("IC_kurtosis", np.nan)

        if "grouping" in sections:
            if self.group_labels is not None:
                rows["n_groups_used"] = int(self.group_labels.dropna().nunique())

        if "curves" in sections:
            if self.long_curve is not None:
                rows["long_return"] = self.long_curve.iloc[-1] - 1.0
            if self.short_curve is not None:
                rows["short_return"] = self.short_curve.iloc[-1] - 1.0
            if self.hedge_curve is not None:
                rows["hedge_return"] = self.hedge_curve.iloc[-1] - 1.0
            if self.hedge_curve_after_cost is not None:
                rows["hedge_return_after_cost"] = self.hedge_curve_after_cost.iloc[-1] - 1.0
            if self.sharpe is not None:
                rows["sharpe"] = self.sharpe
            if self.calmar is not None:
                rows["calmar"] = self.calmar
            if self.sortino is not None:
                rows["sortino"] = self.sortino
            if self.sharpe_after_cost is not None:
                rows["sharpe_after_cost"] = self.sharpe_after_cost
            if self.calmar_after_cost is not None:
                rows["calmar_after_cost"] = self.calmar_after_cost
            if self.sortino_after_cost is not None:
                rows["sortino_after_cost"] = self.sortino_after_cost
            if self.hedge_curve is not None:
                rows["n_days"] = len(self.hedge_curve)

        if "turnover" in sections:
            if self.turnover is not None:
                rows["avg_turnover"] = self.turnover.mean().mean()
            if self.rank_autocorr is not None:
                rows["avg_rank_autocorr"] = self.rank_autocorr.mean()

        if "neutralize" in sections:
            if self.neutralized_curve is not None:
                rows["neutralized_return"] = self.neutralized_curve.iloc[-1] - 1.0

        return pd.DataFrame([rows])
