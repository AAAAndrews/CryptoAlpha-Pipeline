"""
Script: 端到端因子投研编排脚本 / End-to-end factor research orchestration script.
Purpose: 一键串联完整因子投研流程：
         数据加载 → 收益率计算 → 因子计算 → 因子对齐 → [未来函数检测] → 绩效检验 → 报告输出
         One-command execution of the full factor research pipeline:
         Data loading → Returns calculation → Factor calculation → Alignment → [Future leak check] → Evaluation → Report

依赖 / Dependencies:
- Cross_Section_Factor: KlineLoader 数据加载 / Data loading
- FactorLib: 因子计算 / Factor calculation
- FactorAnalysis: 绩效检验 / Factor performance evaluation
"""

import sys
import os
import argparse
import time
from datetime import datetime

# 添加项目根目录到路径 / Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

for submodule in ["CryptoDataProviders", "CryptoDB_feather"]:
    submodule_path = os.path.join(project_root, submodule)
    if submodule_path not in sys.path:
        sys.path.insert(0, submodule_path)


def run_factor_research(
    factor_name: str,
    return_label: str = "close2close",
    n_groups: int = 5,
    cost_rate: float = 0.001,
    top_k: int = 1,
    bottom_k: int = 1,
    risk_free_rate: float = 0.0,
    periods_per_year: int = 252,
    max_loss: float = 0.35,
    # 数据加载参数 / Data loading parameters
    start_time: str = None,
    end_time: str = None,
    symbols: list = None,
    exchange: str = "binance",
    kline_type: str = "swap",
    interval: str = "1h",
    # 未来函数检测参数 / Future leak detection parameters
    check_leak: bool = False,
    leak_block: bool = False,
):
    """
    执行完整因子投研流程 / Execute the full factor research pipeline.

    Parameters / 参数:
        factor_name: 因子名称，需已在 FactorLib 中注册 / Registered factor name in FactorLib
        return_label: 收益率标签，close2close 或 open2open / Return label, close2close or open2open
        n_groups: 分组数量 / Number of quantile groups
        cost_rate: 每次换仓的交易成本比例 / Transaction cost rate per rebalance
        top_k: 做多最高的几组 / Number of top groups to long
        bottom_k: 做空最低的几组 / Number of bottom groups to short
        risk_free_rate: 年化无风险利率 / Annualized risk-free rate
        periods_per_year: 年化交易日数 / Trading periods per year
        max_loss: 数据质量容忍阈值 / Data quality tolerance threshold
        start_time: 起始时间 / Start time filter
        end_time: 结束时间 / End time filter
        symbols: 交易对列表 / List of trading pairs
        exchange: 交易所名称 / Exchange name
        kline_type: K 线类型 / Kline type
        interval: K 线周期 / Kline interval
        check_leak: 是否在评估前执行未来函数检测 / Run future leak detection before evaluation
        leak_block: 检测 FAIL 时是否阻断 pipeline / Block pipeline on detection failure

    Returns / 返回:
        tuple: (evaluator, report) — FactorEvaluator 实例和摘要报告 DataFrame
    """
    pipeline_start = time.time()

    # 头部信息 / Header
    print("=" * 70)
    print(f"  CryptoAlpha Factor Research Pipeline")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  factor={factor_name}  label={return_label}  "
          f"n_groups={n_groups}  cost={cost_rate}")
    print("=" * 70)

    # ── Step 1: 数据加载 / Data Loading ──────────────────────────
    print(f"\n{'─' * 70}")
    print(f"  [Step 1/7] Data Loading")
    print(f"{'─' * 70}")

    from Cross_Section_Factor.kline_loader import KlineLoader

    loader = KlineLoader(
        start_time=start_time,
        end_time=end_time,
        symbols=symbols,
        exchange=exchange,
        kline_type=kline_type,
        interval=interval,
    )
    step_start = time.time()
    data = loader.compile()
    n_symbols = data["symbol"].nunique() if not data.empty else 0
    print(f"  Loaded {len(data)} rows, {n_symbols} symbols in {time.time() - step_start:.1f}s")

    if data.empty:
        print("  ERROR: No data loaded. Aborting.")
        return None, None

    # ── Step 2: 因子计算 / Factor Calculation ────────────────────
    print(f"\n{'─' * 70}")
    print(f"  [Step 2/7] Factor Calculation — {factor_name}")
    print(f"{'─' * 70}")

    from FactorLib import list_factors, get

    available = list_factors()
    if factor_name not in available:
        print(f"  ERROR: Factor '{factor_name}' not found.")
        print(f"  Available factors: {available}")
        return None, None

    factor_cls = get(factor_name)
    factor_inst = factor_cls()

    step_start = time.time()
    factor_raw = factor_inst.calculate(data)
    # 构建 MultiIndex (timestamp, symbol) / Build MultiIndex from raw data columns
    factor_values = _build_factor_multiindex(factor_raw, data)
    print(f"  Factor '{factor_inst.name}' computed, "
          f"{factor_values.notna().sum()} valid values in {time.time() - step_start:.1f}s")

    # ── Step 3: 收益率计算 / Returns Calculation ─────────────────
    print(f"\n{'─' * 70}")
    print(f"  [Step 3/7] Returns Calculation — {return_label}")
    print(f"{'─' * 70}")

    from FactorAnalysis.returns import calc_returns

    step_start = time.time()
    returns = calc_returns(data, label=return_label)
    n_valid_returns = returns.notna().sum()
    print(f"  {return_label} returns computed, "
          f"{n_valid_returns} valid values in {time.time() - step_start:.1f}s")

    # ── Step 4: 因子对齐 + 数据质量 / Alignment + Quality ────────
    print(f"\n{'─' * 70}")
    print(f"  [Step 4/7] Factor-Returns Alignment + Data Quality")
    print(f"{'─' * 70}")

    from FactorAnalysis.alignment import align_factor_returns
    from FactorAnalysis.data_quality import check_data_quality

    step_start = time.time()
    clean = align_factor_returns(factor_values, returns)
    print(f"  Aligned: {len(clean)} clean pairs in {time.time() - step_start:.1f}s")

    coverage = check_data_quality(
        clean["factor"], clean["returns"], max_loss=max_loss,
    )
    print(f"  Data quality coverage: {coverage:.1%}")

    # ── Step 4.5: 未来函数检测 / Future Leak Detection (optional) ─
    if check_leak:
        print(f"\n{'─' * 70}")
        print(f"  [Step 4.5/7] Future Leak Detection — {factor_name}")
        print(f"{'─' * 70}")

        step_start = time.time()

        # 延迟导入避免循环依赖 / Lazy import to avoid circular dependency
        from scripts.check_future_leak import FutureLeakDetector

        detector = FutureLeakDetector()
        leak_report = detector.run(
            factor_name=factor_name,
            return_label=return_label,
            start_time=start_time,
            end_time=end_time,
            symbols=symbols,
            exchange=exchange,
            kline_type=kline_type,
            interval=interval,
        )

        print(f"  Detection completed in {time.time() - step_start:.1f}s")

        # 检测 FAIL 且阻断模式 / FAIL with block mode
        if not leak_report.all_passed:
            n_fail = leak_report.n_fail
            print(f"\n  WARNING: Future leak detection FAILED ({n_fail} checks)")
            if leak_block:
                print("  Pipeline blocked by --leak-block. Aborting.")
                return None, None
            else:
                print("  Continuing pipeline (--leak-block not set).")
        else:
            print(f"  All {leak_report.n_pass} checks PASSED.")
    else:
        leak_report = None

    # ── Step 5: 绩效检验 / Factor Evaluation ─────────────────────
    print(f"\n{'─' * 70}")
    print(f"  [Step 5/7] Factor Evaluation (Tear Sheet)")
    print(f"{'─' * 70}")

    from FactorAnalysis.evaluator import FactorEvaluator

    ev = FactorEvaluator(
        clean["factor"],
        clean["returns"],
        n_groups=n_groups,
        top_k=top_k,
        bottom_k=bottom_k,
        cost_rate=cost_rate,
        risk_free_rate=risk_free_rate,
        periods_per_year=periods_per_year,
    )

    step_start = time.time()
    ev.run_all()
    print(f"  Evaluation completed in {time.time() - step_start:.1f}s")

    # ── Step 6: 报告输出 / Report Output ─────────────────────────
    print(f"\n{'─' * 70}")
    print(f"  [Step 6/7] Report Generation")
    print(f"{'─' * 70}")

    report = ev.generate_report()
    elapsed = time.time() - pipeline_start

    # 摘要打印 / Summary printout
    print(f"\n{'=' * 70}")
    print(f"  Factor Research Report")
    print(f"{'=' * 70}")
    print(f"  Factor:         {factor_inst.name}")
    print(f"  Return Label:   {return_label}")
    print(f"  Data:           {len(data)} rows, {n_symbols} symbols")
    print(f"  Aligned Pairs:  {len(clean)}")
    print(f"  Coverage:       {coverage:.1%}")
    print(f"  Total Time:     {elapsed:.1f}s")
    print(f"{'─' * 70}")

    # 关键指标 / Key metrics
    _print_metrics(ev)

    # 完整报告表 / Full report table
    print(f"\n  Full Report DataFrame:")
    print(report.to_string(index=False))
    print(f"{'=' * 70}")

    return ev, report


def _build_factor_multiindex(factor_raw, data):
    """
    将因子值从 RangeIndex 转换为 MultiIndex (timestamp, symbol)。
    Convert factor values from RangeIndex to MultiIndex (timestamp, symbol).

    因子计算在扁平 DataFrame 上执行，返回 RangeIndex 的 Series。
    需要从原始数据中提取 timestamp 和 symbol 构建 MultiIndex。
    """
    import pandas as pd

    n = len(factor_raw)
    return pd.Series(
        factor_raw.values[:n],
        index=pd.MultiIndex.from_arrays(
            [data["timestamp"].values[:n], data["symbol"].values[:n]],
            names=["timestamp", "symbol"],
        ),
        name=factor_raw.name,
    )


def _print_metrics(ev):
    """
    打印关键绩效指标摘要 / Print key performance metrics summary.
    """
    if ev.icir is not None:
        print(f"  IC Mean:        {ev.ic.mean():.4f}")
        print(f"  ICIR:           {ev.icir:.4f}")
    if ev.rank_ic is not None:
        print(f"  RankIC Mean:    {ev.rank_ic.mean():.4f}")
    if ev.ic_stats is not None:
        print(f"  IC t-stat:      {ev.ic_stats.get('t_stat', float('nan')):.4f}")
        print(f"  IC p-value:     {ev.ic_stats.get('p_value', float('nan')):.4f}")
    if ev.sharpe is not None:
        print(f"  Sharpe:         {ev.sharpe:.4f}")
    if ev.hedge_curve is not None:
        print(f"  Hedge Return:   {ev.hedge_curve.iloc[-1] - 1.0:.4f}")
    if ev.hedge_curve_after_cost is not None:
        print(f"  Hedge (cost):   {ev.hedge_curve_after_cost.iloc[-1] - 1.0:.4f}")
    if ev.turnover is not None:
        print(f"  Avg Turnover:   {ev.turnover.mean().mean():.4f}")
    if ev.rank_autocorr is not None:
        print(f"  Avg RankAuto:   {ev.rank_autocorr.mean():.4f}")
    if ev.neutralized_curve is not None:
        print(f"  Neutralized:    {ev.neutralized_curve.iloc[-1] - 1.0:.4f}")


def main():
    """CLI 入口函数 / CLI entry point."""
    parser = argparse.ArgumentParser(
        description="CryptoAlpha 端到端因子投研流程 / End-to-end factor research pipeline"
    )

    # 因子分析参数 / Factor analysis parameters
    parser.add_argument(
        "--factor", type=str, required=True,
        help="因子名称，需已在 FactorLib 中注册 / Registered factor name",
    )
    parser.add_argument(
        "--return-label", type=str, default="close2close",
        choices=["close2close", "open2open"],
        help="收益率标签 (default: close2close)",
    )
    parser.add_argument(
        "--n-groups", type=int, default=5,
        help="分组数量 (default: 5)",
    )
    parser.add_argument(
        "--cost-rate", type=float, default=0.001,
        help="每次换仓的交易成本比例 (default: 0.001)",
    )
    parser.add_argument(
        "--top-k", type=int, default=1,
        help="做多最高的几组 (default: 1)",
    )
    parser.add_argument(
        "--bottom-k", type=int, default=1,
        help="做空最低的几组 (default: 1)",
    )
    parser.add_argument(
        "--risk-free-rate", type=float, default=0.0,
        help="年化无风险利率 (default: 0.0)",
    )
    parser.add_argument(
        "--periods-per-year", type=int, default=252,
        help="年化交易日数 (default: 252)",
    )
    parser.add_argument(
        "--max-loss", type=float, default=0.35,
        help="数据质量容忍阈值 (default: 0.35)",
    )

    # 数据加载参数 / Data loading parameters
    parser.add_argument(
        "--start-time", type=str, default=None,
        help="起始时间，如 '2024-01-01' / Start time filter",
    )
    parser.add_argument(
        "--end-time", type=str, default=None,
        help="结束时间，如 '2024-06-01' / End time filter",
    )
    parser.add_argument(
        "--symbols", type=str, nargs="*", default=None,
        help="交易对列表，如 BTCUSDT ETHUSDT / Trading pair list",
    )
    parser.add_argument(
        "--exchange", type=str, default="binance",
        help="交易所名称 (default: binance)",
    )
    parser.add_argument(
        "--kline-type", type=str, default="swap",
        help="K 线类型 (default: swap)",
    )
    parser.add_argument(
        "--interval", type=str, default="1h",
        help="K 线周期 (default: 1h)",
    )

    # 未来函数检测参数 / Future leak detection parameters
    parser.add_argument(
        "--check-leak", action="store_true", default=False,
        help="在评估前执行未来函数检测 / Run future leak detection before evaluation",
    )
    parser.add_argument(
        "--leak-block", action="store_true", default=False,
        help="检测 FAIL 时阻断 pipeline / Block pipeline on detection failure",
    )

    args = parser.parse_args()

    try:
        run_factor_research(
            factor_name=args.factor,
            return_label=args.return_label,
            n_groups=args.n_groups,
            cost_rate=args.cost_rate,
            top_k=args.top_k,
            bottom_k=args.bottom_k,
            risk_free_rate=args.risk_free_rate,
            periods_per_year=args.periods_per_year,
            max_loss=args.max_loss,
            start_time=args.start_time,
            end_time=args.end_time,
            symbols=args.symbols,
            exchange=args.exchange,
            kline_type=args.kline_type,
            interval=args.interval,
            check_leak=args.check_leak,
            leak_block=args.leak_block,
        )
    except KeyboardInterrupt:
        print("\n\nUser interrupts and exits the program...")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
