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

import warnings

import numpy as np
import pandas as pd
from scipy import stats

from .metrics import (
    calc_ic, calc_rank_ic, calc_icir, calc_sharpe, calc_calmar, calc_sortino,
    calc_ic_stats,
)
from .grouping import quantile_group
from .portfolio import calc_long_only_curve, calc_short_only_curve, calc_top_bottom_curve
from .cost import deduct_cost
from .turnover import calc_turnover, calc_rank_autocorr
from .neutralize import calc_neutralized_curve
from .chunking import split_into_chunks, merge_chunk_results, ChunkMemoryTracker


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

        当 chunk_size 已设置时，按时间分块逐块计算 IC 序列，再汇总聚合。
        IC 值在各时间截面上独立，分块拼接结果与全量计算数值一致（差异 < 1e-8）。
        When chunk_size is set, compute IC series per time chunk, then aggregate.
        IC values are independent per timestamp; merged chunked results match full
        calculation within 1e-8 tolerance.

        Returns / 返回:
            self，支持链式调用 / self, for method chaining
        """
        if self.chunk_size is None:
            # 全量模式：原有逻辑 / full mode: original logic
            self.ic = calc_ic(self.factor, self.returns)
            self.rank_ic = calc_rank_ic(self.factor, self.returns)
            self.icir = calc_icir(self.factor, self.returns)
            self.ic_stats = calc_ic_stats(self.factor, self.returns)
        else:
            # 分块模式 / chunked mode
            factor_chunks = split_into_chunks(self.factor, self.chunk_size)
            returns_chunks = split_into_chunks(self.returns, self.chunk_size)
            n_chunks = len(factor_chunks)

            # 逐块计算 IC / RankIC，带内存监控 / compute IC per chunk with memory tracking
            ic_chunks = []
            rank_ic_chunks = []
            for i, (fc, rc) in enumerate(zip(factor_chunks, returns_chunks)):
                with ChunkMemoryTracker(i, n_chunks, description="run_metrics"):
                    ic_chunks.append(calc_ic(fc, rc))
                    rank_ic_chunks.append(calc_rank_ic(fc, rc))

            # 汇总 IC 序列（各时间截面独立，直接拼接）/ merge IC series
            self.ic = merge_chunk_results(ic_chunks, "ic")
            self.rank_ic = merge_chunk_results(rank_ic_chunks, "ic")

            # ICIR / IC 统计量从合并后的 IC 序列计算
            # ICIR / IC stats computed from merged IC series
            self.icir = _icir_from_series(self.ic)
            self.ic_stats = _ic_stats_from_series(self.ic)

        return self

    def run_grouping(self) -> "FactorEvaluator":
        """
        计算分位数分组标签 / Compute quantile group labels.

        当 chunk_size 已设置时，按时间分块逐块执行 quantile_group，
        确保分块内截面完整性。各时间截面的分组标签相互独立，
        分块拼接结果与全量计算数值一致。
        When chunk_size is set, execute quantile_group per time chunk,
        ensuring cross-sectional completeness within each chunk. Group labels
        are independent per timestamp; merged results match full calculation.

        Returns / 返回:
            self，支持链式调用 / self, for method chaining
        """
        if self.chunk_size is None:
            # 全量模式：原有逻辑 / full mode: original logic
            self.group_labels = quantile_group(self.factor, n_groups=self.n_groups)
        else:
            # 分块模式：逐块计算分组标签，带内存监控 / chunked mode with memory tracking
            factor_chunks = split_into_chunks(self.factor, self.chunk_size)
            n_chunks = len(factor_chunks)
            chunk_labels = []
            for i, fc in enumerate(factor_chunks):
                with ChunkMemoryTracker(i, n_chunks, description="run_grouping"):
                    chunk_labels.append(
                        quantile_group(fc, n_groups=self.n_groups)
                    )
            # 分组标签按时间截面独立，直接拼接 / labels independent per timestamp, concat
            self.group_labels = merge_chunk_results(chunk_labels, "ic")

        return self

    def run_curves(self) -> "FactorEvaluator":
        """
        净值曲线 + 成本扣除 + 绩效比率 / Equity curves + cost deduction + performance ratios.

        依次计算：多/空/对冲净值 → 成本扣除 → Sharpe/Calmar/Sortino（成本前+成本后）。
        Computes: long/short/hedge curves → cost deduction → Sharpe/Calmar/Sortino (before & after cost).

        当 chunk_size 已设置时，按时间分块逐块构建净值曲线，使用 raw 曲线（不覆写起始值）
        合并后统一覆写起始值为 1.0，确保与全量计算数值一致。
        When chunk_size is set, build equity curves per time chunk using raw curves
        (without overwriting start value), merge, then overwrite start to 1.0,
        ensuring numerical consistency with full calculation.

        Returns / 返回:
            self，支持链式调用 / self, for method chaining
        """
        if self.chunk_size is None:
            # 全量模式：原有逻辑 / full mode: original logic
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
        else:
            # 分块模式：逐块计算 raw 曲线后合并，带内存监控 / chunked mode with memory tracking
            factor_chunks = split_into_chunks(self.factor, self.chunk_size)
            returns_chunks = split_into_chunks(self.returns, self.chunk_size)
            n_chunks = len(factor_chunks)

            # 逐块计算 raw 净值曲线 / compute raw equity curves per chunk
            long_chunks = []
            short_chunks = []
            hedge_chunks = []
            for i, (fc, rc) in enumerate(zip(factor_chunks, returns_chunks)):
                with ChunkMemoryTracker(i, n_chunks, description="run_curves"):
                    long_chunks.append(
                        calc_long_only_curve(fc, rc, n_groups=self.n_groups, top_k=self.top_k, _raw=True)
                    )
                    short_chunks.append(
                        calc_short_only_curve(fc, rc, n_groups=self.n_groups, bottom_k=self.bottom_k, _raw=True)
                    )
                    hedge_chunks.append(
                        calc_top_bottom_curve(fc, rc, n_groups=self.n_groups,
                                              top_k=self.top_k, bottom_k=self.bottom_k, _raw=True)
                    )

            # 合并 raw 曲线并覆写起始值 / merge raw curves and overwrite start
            self.long_curve = _merge_raw_curves(long_chunks)
            self.short_curve = _merge_raw_curves(short_chunks)
            self.hedge_curve = _merge_raw_curves(hedge_chunks)

            if len(self.long_curve) > 0:
                self.long_curve.iloc[0] = 1.0
            if len(self.short_curve) > 0:
                self.short_curve.iloc[0] = 1.0
            if len(self.hedge_curve) > 0:
                self.hedge_curve.iloc[0] = 1.0
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

        当 chunk_size 已设置时，按时间分块逐段计算换手率和排名自相关。
        跨块边界的换手率/自相关标记为 NaN（无法跨块比较前序截面）。
        When chunk_size is set, compute turnover and rank autocorrelation per time chunk.
        Cross-chunk boundary values are marked as NaN (predecessor cross-section unavailable).

        Returns / 返回:
            self，支持链式调用 / self, for method chaining
        """
        if self.chunk_size is None:
            # 全量模式：原有逻辑 / full mode: original logic
            self.turnover = calc_turnover(self.factor, n_groups=self.n_groups)
            self.rank_autocorr = calc_rank_autocorr(self.factor)
        else:
            # 分块模式：逐块计算换手率和排名自相关，带内存监控 / chunked mode with memory tracking
            factor_chunks = split_into_chunks(self.factor, self.chunk_size)
            n_chunks = len(factor_chunks)

            # 逐块计算换手率和排名自相关 / compute turnover and rank autocorrelation per chunk
            turnover_chunks = []
            autocorr_chunks = []
            for i, fc in enumerate(factor_chunks):
                with ChunkMemoryTracker(i, n_chunks, description="run_turnover"):
                    turnover_chunks.append(calc_turnover(fc, n_groups=self.n_groups))
                    autocorr_chunks.append(calc_rank_autocorr(fc))

            # 汇总：跨块边界首行设为 NaN / merge: boundary rows set to NaN
            self.turnover = merge_chunk_results(turnover_chunks, "turnover")
            self.rank_autocorr = merge_chunk_results(autocorr_chunks, "rank_autocorr")

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

        当 chunk_size 已设置时，按时间分块逐块执行中性化处理，
        使用 raw 曲线（不覆写起始值）合并后统一覆写起始值为 1.0，
        确保与全量计算数值一致。
        When chunk_size is set, execute neutralization per time chunk,
        using raw curves (without overwriting start value), merge, then
        overwrite start to 1.0, ensuring numerical consistency with full calculation.

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

        if self.chunk_size is None:
            # 全量模式：原有逻辑 / full mode: original logic
            self.neutralized_curve = calc_neutralized_curve(
                self.factor, self.returns,
                groups=groups, demeaned=demeaned, group_adjust=group_adjust, n_groups=n_groups,
            )
        else:
            # 分块模式：逐块计算 raw 中性化曲线后合并，带内存监控 / chunked mode with memory tracking
            factor_chunks = split_into_chunks(self.factor, self.chunk_size)
            returns_chunks = split_into_chunks(self.returns, self.chunk_size)

            # 当 groups 为 pd.Series 时也需要分块 / split groups Series if provided
            if isinstance(groups, pd.Series):
                groups_chunks = split_into_chunks(groups, self.chunk_size)
            else:
                groups_chunks = [groups] * len(factor_chunks)

            n_chunks = len(factor_chunks)
            neutralized_chunks = []
            for i, (fc, rc, gc) in enumerate(zip(factor_chunks, returns_chunks, groups_chunks)):
                with ChunkMemoryTracker(i, n_chunks, description="run_neutralize"):
                    neutralized_chunks.append(
                        calc_neutralized_curve(
                            fc, rc,
                            groups=gc, demeaned=demeaned, group_adjust=group_adjust,
                            n_groups=n_groups, _raw=True,
                        )
                    )

            # 合并 raw 曲线并覆写起始值 / merge raw curves and overwrite start
            self.neutralized_curve = _merge_raw_curves(neutralized_chunks)
            if len(self.neutralized_curve) > 0:
                self.neutralized_curve.iloc[0] = 1.0

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


# ============================================================
# 模块级辅助函数 / Module-level helpers
# ============================================================


def _merge_raw_curves(chunk_results: list[pd.Series]) -> pd.Series:
    """
    缩放拼接 raw 净值曲线（未覆写起始值），保持 cumprod 连续性 / Scale and concat raw equity curves.

    与 _merge_curves 不同，此函数不跳过后续块的首元素，因为 raw 曲线的首元素
    是有效 cumprod 值（非覆写的 1.0）。合并后由调用方统一覆写起始值为 1.0。
    Unlike _merge_curves, this function does NOT skip the first element of subsequent
    chunks because raw curves have valid cumprod values (not overwritten 1.0).
    The caller is responsible for overwriting the start value to 1.0 after merging.
    """
    if not chunk_results:
        return pd.Series(dtype=float)

    merged = chunk_results[0].copy()
    for i in range(1, len(chunk_results)):
        # 前一块末尾的净值 / last equity value of previous chunk
        scale = merged.iloc[-1]
        chunk = chunk_results[i]
        # 缩放全部元素（含首元素）/ scale ALL elements (including first)
        scaled = chunk * scale
        merged = pd.concat([merged, scaled])
    return merged


def _icir_from_series(ic_series: pd.Series) -> float:
    """
    从 IC 序列计算 ICIR / Compute ICIR from IC series.

    ICIR = mean(IC) / std(IC)，与 calc_icir 的数值逻辑一致（ddof=1）。
    ICIR = mean(IC) / std(IC), numerically consistent with calc_icir (ddof=1).
    """
    ic_valid = ic_series.dropna()
    if len(ic_valid) == 0:
        return 0.0
    mean_ic = ic_valid.mean()
    std_ic = ic_valid.std()
    if std_ic == 0:
        return 0.0
    return mean_ic / std_ic


def _ic_stats_from_series(ic_series: pd.Series) -> pd.Series:
    """
    从 IC 序列计算统计显著性指标 / Compute IC stats from IC series.

    与 calc_ic_stats 输出字段完全一致，但直接接受 IC 序列而非 factor/returns，
    适用于分块合并后的 IC 序列场景。
    Output fields are fully consistent with calc_ic_stats, but accepts an IC series
    directly instead of factor/returns, suitable for merged chunked IC series.
    """
    ic_valid = ic_series.dropna()

    if len(ic_valid) < 3:
        warnings.warn(
            f"IC series has only {len(ic_valid)} valid observations (need >= 3), "
            "returning NaN stats",
            UserWarning,
        )
        return pd.Series({
            "IC_mean": np.nan,
            "IC_std": np.nan,
            "ICIR": np.nan,
            "t_stat": np.nan,
            "p_value": np.nan,
            "IC_skew": np.nan,
            "IC_kurtosis": np.nan,
        })

    ic_mean = float(ic_valid.mean())
    ic_std = float(ic_valid.std(ddof=1))
    icir = ic_mean / ic_std if ic_std != 0 else np.nan

    t_stat, p_value = stats.ttest_1samp(ic_valid.values, 0.0)
    ic_skew = float(stats.skew(ic_valid.values, bias=False))
    ic_kurtosis = float(stats.kurtosis(ic_valid.values, bias=False))

    return pd.Series({
        "IC_mean": ic_mean,
        "IC_std": ic_std,
        "ICIR": icir,
        "t_stat": float(t_stat),
        "p_value": float(p_value),
        "IC_skew": ic_skew,
        "IC_kurtosis": ic_kurtosis,
    })
