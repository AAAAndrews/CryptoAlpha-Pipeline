# Progress Log

> Append newest entries to the top in this format:
> `[YYYY-MM-DD HH:MM] summary`

### [2026-04-01] Task 28 — 项目级冒烟测试 / Project-level Smoke Tests

Created `scarffold/.agent/test_smoke.py` with 52 validation checks across 3 areas:

1. **requirements.txt 解析** — 文件存在、非空、所有依赖行可解析为 Requirement、无重复包名、关键包已安装
2. **模块导入** — CryptoDataProviders（5 checks）、CryptoDB_feather.config（3 checks）、CryptoDB_feather.core（8 checks）、FactorLib（8 checks）、FactorAnalysis（14 checks）、scripts（4 checks）全部导入正常，__all__ 导出完整
3. **pipeline --dry-run 空跑** — 使用 `--skip-bulk --skip-cleanup` 避免网络请求，验证退出码为 0、输出包含 "Pipeline Summary" 和 "ALL STEPS PASSED"

All 52 checks PASSED. **所有 28 个任务已完成 / All 28 tasks completed.**

**Usage**: `python scarffold/.agent/test_smoke.py`

---

### [2026-04-01] Task 27 — 根目录 requirements.txt 更新 / Root requirements.txt Update

Updated root `requirements.txt` from 9 to 11 packages by scanning all project Python files for third-party imports:

**Added packages:**
1. **`matplotlib>=3.7.0`** — used by `Cross_Section_Factor/crypto_gp_1h.py` and `deap_alpha/utils.py` for visualization
2. **`joblib>=1.3.0`** — used by `Cross_Section_Factor/crypto_gp_1h.py` and `deap_alpha/utils.py` for parallel computing

**Complete dependency list (11 packages):**
- 数据处理: pandas, numpy, pyarrow, scipy, numba
- 网络请求: requests
- 终端 UI: tqdm, rich
- 可视化: matplotlib
- 并行计算: joblib
- 遗传编程: deap

**Verification**: All 11 packages parse correctly via `pkg_resources.Requirement.parse()`. AST scan of all `.py` files confirms no missing third-party imports.

**Usage**: `pip install -r requirements.txt`

---

### [2026-04-01] Task 26 — FactorAnalysis 端到端验证测试 / E2E Validation Tests

Created `scarffold/.agent/test_factoranalysis_e2e.py` with 94 validation checks across 13 areas:

1. **合成数据生成** — factor/returns MultiIndex (timestamp, symbol) 类型、形状、有限值
2. **FactorEvaluator 默认运行** — run() 返回 self、ic/rank_ic/icir 类型
3. **IC/RankIC/ICIR 数值合理性** — 正 IC 强度因子 IC > 0、IC < 0.5、ICIR > 0、IC ∈ [-1,1]
4. **分组标签** — 类型、MultiIndex、唯一值范围 0-4、无 NaN
5. **分组收益单调性** — 因子值越高收益越高，单调比例 = 1.00
6. **净值曲线** — long/short/hedge 曲线：类型、DatetimeIndex、起始 1.0、无 NaN、有限值
7. **交易成本扣除** — after_cost 类型、起始 1.0、无 NaN、after_cost 收益 <= 原始收益
8. **绩效比率** — 6 个比率（sharpe/calmar/sortino × before/after cost）类型和有限值、after <= before
9. **generate_report** — DataFrame 类型、单行、16 列全部存在、所有值有限
10. **自定义参数** — n_groups=3、cost_rate=0.005 正常运行、更高成本 → 更低收益
11. **错误处理** — 未调用 run() 时 generate_report 抛出 ValueError
12. **弱因子场景** — 独立噪声因子 ICIR ≈ 0、report 正常输出
13. **公共导出** — `__all__` 有 13 个导出

All 94 checks PASSED.

**Usage**: `python scarffold/.agent/test_factoranalysis_e2e.py`

---

### [2026-04-01] Task 25 — FactorAnalysis/report.py 绩效报告汇总 / Summary Report Generator

Implemented `generate_report(evaluator)` in `FactorAnalysis/report.py`:

1. **`generate_report(evaluator) -> pd.DataFrame`** — 从已调用 `run()` 的 FactorEvaluator 实例生成单行摘要 DataFrame，包含 16 个关键指标列。
2. **IC 指标列**: `IC_mean`, `IC_std`, `RankIC_mean`, `RankIC_std`, `ICIR`
3. **净值曲线期末收益列**: `long_return`, `short_return`, `hedge_return`, `hedge_return_after_cost`
4. **绩效比率列**: `sharpe`, `calmar`, `sortino`, `sharpe_after_cost`, `calmar_after_cost`, `sortino_after_cost`
5. **元信息列**: `n_days` (分析天数)
6. 未调用 `run()` 时抛出 `ValueError`。
7. 更新 `__init__.py` 添加 `from .report import generate_report`。

**Verification**: 30 checks passed — import OK, public export OK, __all__ export OK, ValueError on unrun evaluator, DataFrame type, single row, 16 expected columns present, n_days int-like, ICIR float, all values finite, logical consistency (after_cost <= before_cost for hedge return and sharpe), custom params run, higher cost → lower return.

**Usage**:
```python
from FactorAnalysis import FactorEvaluator, generate_report

ev = FactorEvaluator(factor_series, returns_series, cost_rate=0.001)
ev.run()

report = generate_report(ev)
# report: pd.DataFrame (1 row × 16 columns)
print(report.T)  # 以列形式查看所有指标 / view all metrics as rows
```

---

### [2026-04-01] Task 24 — FactorAnalysis/evaluator.py FactorEvaluator 编排器 / FactorEvaluator Orchestrator

Implemented `FactorEvaluator` class in `FactorAnalysis/evaluator.py`:

1. **`FactorEvaluator(factor, returns, n_groups=5, top_k=1, bottom_k=1, cost_rate=0.001, risk_free_rate=0.0, periods_per_year=252)`** — 因子绩效检验编排器，按顺序执行全部分析步骤并将结果存储在实例属性中。
2. **`run()`** — 依次执行：IC 分析 (ic/rank_ic/icir) → 分组 (group_labels) → 净值曲线 (long/short/hedge) → 成本扣除 (hedge_curve_after_cost) → 绩效比率 (sharpe/calmar/sortino, 含成本后版本)。返回 self 支持链式调用。
3. 所有结果属性初始化为 None，run() 后可直接访问，供 `generate_report` 消费。

更新 `__init__.py` 添加 `from .evaluator import FactorEvaluator`。

**Verification**: 72 checks passed — import OK, instantiation with default/custom params, run() returns self, IC metrics (Series type/length/index/finite), group labels (Series/index/range), equity curves (long/short/hedge: type/index/length/start/no-NaN/finite), cost-adjusted curve (type/start/no-NaN/<=hedge), performance ratios (6 ratios: type/finite, sharpe_after_cost<=sharpe), custom params run, __all__ export, all-NaN edge case, non-zero risk_free_rate edge case.

**Usage**:
```python
from FactorAnalysis import FactorEvaluator

ev = FactorEvaluator(factor_series, returns_series, n_groups=5, cost_rate=0.001)
ev.run()

# IC 指标
print(ev.icir)         # float

# 净值曲线
print(ev.hedge_curve)  # pd.Series

# 绩效比率（成本前 / 成本后）
print(ev.sharpe, ev.sharpe_after_cost)
print(ev.calmar, ev.calmar_after_cost)
print(ev.sortino, ev.sortino_after_cost)
```

---

### [2026-04-01] Task 23 — FactorAnalysis/metrics.py Sharpe/Calmar/Sortino 比率 / Performance Ratios

Implemented three annualized performance ratio functions in `FactorAnalysis/metrics.py`:

1. **`calc_sharpe(equity, risk_free_rate=0.0, periods_per_year=252)`** — 年化 Sharpe 比率。从净值曲线计算日收益率，Sharpe = (mean(excess_ret) / std(excess_ret)) * sqrt(P)。
2. **`calc_calmar(equity, periods_per_year=252)`** — 年化 Calmar 比率。Calmar = annualized_return / abs(max_drawdown)。年化收益率使用复合收益公式 `(1+total)^(1/years)-1`。
3. **`calc_sortino(equity, risk_free_rate=0.0, periods_per_year=252)`** — 年化 Sortino 比率。下行偏差仅考虑低于 rf_daily 的收益率，Sortino = mean(excess) / downside_dev * sqrt(P)。

边界处理：数据不足（<2 天）返回 0.0；标准差/下行偏差为 0 返回 0.0；NaN/Inf 自动过滤。
更新 `__init__.py` 添加 `from .metrics import calc_sharpe, calc_calmar, calc_sortino`。

**Verification**: 30 checks passed — import OK, __all__ OK, type/finite, positive/negative/flat drift sign correctness, risk-free rate monotonicity, edge cases (1-day/2-day/NaN), periods_per_year sensitivity, all-positive-returns sortino=0.

**Usage**:
```python
from FactorAnalysis import calc_sharpe, calc_calmar, calc_sortino

# equity: pd.Series, cumulative equity curve starting at 1.0
sharpe = calc_sharpe(equity, risk_free_rate=0.03)     # annualized Sharpe
calmar = calc_calmar(equity)                           # annualized Calmar
sortino = calc_sortino(equity, risk_free_rate=0.03)   # annualized Sortino
```

---

> Append newest entries to the top in this format:
> `[YYYY-MM-DD HH:MM] summary`

### [2026-04-01] Task 22 — FactorAnalysis/cost.py 交易成本向量化扣除 / Transaction Cost Deduction

Implemented `deduct_cost(daily_returns, cost_rate=0.001)` in `FactorAnalysis/cost.py`:

1. **`deduct_cost(daily_returns, cost_rate=0.001)`** — 假设每个截面均换仓，对日收益率向量化扣除固定比例滑差成本，重新计算累积净值曲线。`adjusted_returns = daily_returns - cost_rate`，再 `(1 + adjusted_returns).cumprod()`。
2. 参数校验：`cost_rate < 0` 或 `cost_rate >= 1` 抛出 `ValueError`。
3. 更新 `__init__.py` 添加 `from .cost import deduct_cost`。

**Verification**: 20 checks passed — import OK, public export OK, shape/type/DatetimeIndex/float dtype, start value 1.0, no NaN/all finite, cost reduces equity, higher cost = lower equity, default cost_rate=0.001, ValueError on negative/>=1/1.0 cost_rate, zero returns → declining, single day, negative returns, cost_rate=0 matches manual cumprod.

**Usage**:
```python
from FactorAnalysis import deduct_cost

# daily_returns: pd.Series indexed by timestamp
adjusted_equity = deduct_cost(daily_returns, cost_rate=0.001)
# adjusted_equity: pd.Series, cumulative equity curve after slippage deduction
```

---

### [2026-04-01] Task 21 — FactorAnalysis/portfolio.py 多空对冲净值曲线 / Long-Short Hedged Equity Curve

Implemented `calc_top_bottom_curve(factor, returns, n_groups=5, top_k=1, bottom_k=1)` in `FactorAnalysis/portfolio.py`:

1. **`calc_top_bottom_curve(factor, returns, n_groups=5, top_k=1, bottom_k=1)`** — 每个截面选取因子值最高的 top_k 组做多、最低的 bottom_k 组做空，日收益 = 多头平均收益 + 空头平均收益（空头已取反），计算累积净值曲线。内部调用 `quantile_group` 获取分组标签，按截面 groupby 分别计算多空收益后合并。
2. 参数校验：`top_k < 1` 或 `bottom_k < 1` 或 `top_k + bottom_k > n_groups` 抛出 `ValueError`。
3. 全 NaN 因子 → 日收益为 0 → 平坦净值 1.0。
4. 更新 `__init__.py` 添加 `from .portfolio import calc_top_bottom_curve`。

**Verification**: 22 checks passed — import OK, __all__ export OK, module-level import OK, shape/type/DatetimeIndex/float dtype, start value 1.0, no NaN/all finite, positive factor → hedge > long & > short, top_k=2/bottom_k=2/n_groups=5 variants, ValueError on invalid top_k (0) / bottom_k (0) / top_k+bottom_k > n_groups, all-NaN → flat curve, single-symbol edge case.

**Usage**:
```python
from FactorAnalysis import calc_top_bottom_curve

curve = calc_top_bottom_curve(factor_series, returns_series, n_groups=5, top_k=1, bottom_k=1)
# curve: pd.Series indexed by timestamp, starting at 1.0
# Long the top_k highest factor groups, short the bottom_k lowest groups
# top_k + bottom_k must not exceed n_groups
```

---

### [2026-03-31] Task 20 — FactorAnalysis/portfolio.py 仅空组净值曲线 / Short-Only Equity Curve

Implemented `calc_short_only_curve(factor, returns, n_groups=5, bottom_k=1)` in `FactorAnalysis/portfolio.py`:

1. **`calc_short_only_curve(factor, returns, n_groups=5, bottom_k=1)`** — 每个截面选取因子值最低的 bottom_k 组，等权做空（收益取反），计算累积净值曲线。内部调用 `quantile_group` 获取分组标签，按截面 groupby 计算等权平均收益后取反，再 cumprod 得到净值。
2. 参数校验：`bottom_k < 1` 或 `bottom_k > n_groups` 抛出 `ValueError`。
3. 全 NaN 因子 → 日收益为 0 → 平坦净值 1.0。
4. 更新 `__init__.py` 添加 `from .portfolio import calc_short_only_curve`。

**Verification**: 24 checks passed — import OK, __all__ export OK, module-level import OK, shape/type/DatetimeIndex/float dtype, start value 1.0, no NaN/all finite, bottom_k=2/5 variants, ValueError on invalid bottom_k (0 and 6), good factor → positive short curve, all-NaN → flat curve, single-symbol edge case, short != long symmetry check.

**Usage**:
```python
from FactorAnalysis import calc_short_only_curve

curve = calc_short_only_curve(factor_series, returns_series, n_groups=5, bottom_k=1)
# curve: pd.Series indexed by timestamp, starting at 1.0
# Returns are negated (short selling) for the bottom_k groups
```

---

### [2026-03-31] Task 19 — FactorAnalysis/portfolio.py 仅多组净值曲线 / Long-Only Equity Curve

Implemented `calc_long_only_curve(factor, returns, n_groups=5, top_k=1)` in `FactorAnalysis/portfolio.py`:

1. **`calc_long_only_curve(factor, returns, n_groups=5, top_k=1)`** — 每个截面选取因子值最高的 top_k 组，等权持有，计算累积净值曲线。内部调用 `quantile_group` 获取分组标签，按截面 groupby 计算等权平均收益，再 cumprod 得到净值。
2. 参数校验：`top_k < 1` 或 `top_k > n_groups` 抛出 `ValueError`。
3. 全 NaN 因子 → 日收益为 0 → 平坦净值 1.0。
4. 更新 `__init__.py` 添加 `from .portfolio import calc_long_only_curve`。

**Verification**: 13 checks passed — import OK, __all__ export OK, shape/type/length, start value 1.0, no NaN, top_k=2/5 variants, ValueError on invalid top_k, good factor → positive curve, all-NaN → flat curve, single-symbol edge case, module-level import.

**Usage**:
```python
from FactorAnalysis import calc_long_only_curve

curve = calc_long_only_curve(factor_series, returns_series, n_groups=5, top_k=1)
# curve: pd.Series indexed by timestamp, starting at 1.0
```

---

### [2026-03-31] Task 18 — FactorAnalysis/grouping.py 分位数分组 / Quantile Grouping

Implemented `quantile_group(factor, n_groups=5)` in `FactorAnalysis/grouping.py`:

1. **`quantile_group(factor, n_groups=5)`** — 每个时间截面上按因子值分位数分成 n_groups 组，组标签 0（最低）到 n_groups-1（最高）。使用 `pd.qcut` + `groupby(level=0)` 实现横截面分组。NaN/Inf 值保留 NaN 标签。
2. 参数校验：`n_groups < 2` 抛出 `ValueError`。
3. 更新 `__init__.py` 添加 `from .grouping import quantile_group`。

**Verification**: 21 checks passed — import OK, public export OK, 5/3/10 组标签正确, 分组均匀性, 因子均值单调递增, NaN/Inf/全 NaN/单截面边界情况, 参数校验。

**Usage**:
```python
from FactorAnalysis import quantile_group

labels = quantile_group(factor_series, n_groups=5)
# labels: pd.Series, same MultiIndex, values 0..4 (lowest to highest quantile)
```

---

### [2026-03-31] Task 17 — FactorAnalysis/metrics.py IC/RankIC/ICIR

Implemented three core factor evaluation metrics in `FactorAnalysis/metrics.py`:

1. **`calc_ic(factor, returns)`** — Daily Pearson IC via `groupby(level=0)` + `Series.corr()`. Returns `pd.Series` indexed by timestamp.
2. **`calc_rank_ic(factor, returns)`** — Daily Spearman Rank IC via `Series.corr(method='spearman')`. Returns `pd.Series` indexed by timestamp.
3. **`calc_icir(factor, returns)`** — ICIR = mean(IC) / std(IC). Returns `float`.

All functions accept `pd.Series` with MultiIndex `(timestamp, symbol)`, matching FactorLib output format. Handles NaN/Inf filtering, single-asset edge case, constant-factor zero-variance case.

Updated `__init__.py` with `from .metrics import calc_ic, calc_rank_ic, calc_icir`.

**Verification**: 31 checks passed — import OK, IC/RankIC series type/length/range, positive/no/perfect correlation scenarios, edge cases (single asset, all NaN, constant factor), public exports.

**Usage**:
```python
from FactorAnalysis import calc_ic, calc_rank_ic, calc_icir

ic = calc_ic(factor_series, returns_series)       # pd.Series (daily IC)
rank_ic = calc_rank_ic(factor_series, returns_series)  # pd.Series (daily Rank IC)
icir = calc_icir(factor_series, returns_series)    # float
```

---

### [2026-03-31] Task 16 — FactorAnalysis 模块结构 / FactorAnalysis Module Structure

Created `FactorAnalysis/` module directory with 7 files:

1. **`__init__.py`** — module docstring + `__all__` declaring 13 public API exports:
   - `calc_ic`, `calc_rank_ic`, `calc_icir` — IC 指标
   - `calc_sharpe`, `calc_calmar`, `calc_sortino` — 绩效比率
   - `quantile_group` — 分位数分组
   - `calc_long_only_curve`, `calc_short_only_curve`, `calc_top_bottom_curve` — 净值曲线
   - `deduct_cost` — 交易成本扣除
   - `FactorEvaluator` — 编排器主类
   - `generate_report` — 报告输出
2. **Stub files** — `metrics.py`, `grouping.py`, `portfolio.py`, `cost.py`, `evaluator.py`, `report.py` (docstring-only, implementations in tasks 17-25)

**Verification**: 4 checks passed — import OK, `__all__` has 13 exports, all expected names present, all submodule files exist.

**Usage**:
```python
import FactorAnalysis
print(FactorAnalysis.__all__)  # 13 public API names
# Implementations will be added by tasks 17-25
```

---

### [2026-03-31] Task 15 — FactorLib 集成验证测试 / FactorLib Integration Tests

Created `scarffold/.agent/test_factorlib_integration.py` with 38 validation checks across 5 areas:

1. **BaseFactor ABC** — import OK, cannot instantiate (TypeError)
2. **Alpha1 Momentum** — inheritance, repr, Series shape, NaN count (lookback × symbols), finite values
3. **Alpha2 Volatility** — inheritance, repr, Series shape, NaN count (lookback × symbols), finite, non-negative
4. **Registry** — clear → empty, single/multiple register, get by name, get missing → None, duplicate warning, non-BaseFactor TypeError, clear
5. **Public exports** — all 7 names accessible via `FactorLib.*`, `__all__` matches

All 38 checks PASSED.

**Usage**: `python scarffold/.agent/test_factorlib_integration.py`

---

### [2026-03-31] Task 14 — 因子注册表 / Factor Registry

Created `FactorLib/registry.py` with a global factor registry:

- `register(factor_cls)` — register a BaseFactor subclass by class name; warns on duplicate
- `list_factors()` — returns sorted list of registered factor names
- `get(name)` — retrieve a registered factor class by name, returns None if not found
- `clear()` — empty the registry (useful for testing)
- `TypeError` raised when registering non-BaseFactor classes
- Updated `FactorLib/__init__.py` to export all 4 registry functions

**Verification**: 9 checks passed — import, single register, multiple register + sorted list, get by name, get missing → None, duplicate warning, non-BaseFactor TypeError, clear, public exports.

**Usage**:
```python
from FactorLib import register, list_factors, get

register(AlphaMomentum)
register(AlphaVolatility)
print(list_factors())  # ['AlphaMomentum', 'AlphaVolatility']

cls = get('AlphaMomentum')
factor = cls(lookback=10)
```

---

### [2026-03-31] Task 13 — Alpha2 波动率因子 / Alpha2 Volatility Factor

Created `FactorLib/alpha_volatility.py` with `AlphaVolatility(BaseFactor)` class:

- Calculates return standard deviation over a configurable `lookback` window per symbol
- Formula: `close.pct_change().rolling(lookback).std()` via `groupby().transform()`
- Default `lookback=20`, name auto-updated to reflect parameter
- Constant price yields zero volatility; all values are non-negative
- Updated `FactorLib/__init__.py` to export `AlphaVolatility`

**Verification**: 8 checks passed — inheritance, instantiation/repr, default lookback, correct Series shape, NaN count (lookback × symbols), non-negative values, constant-price zero volatility, public export.

**Usage**:
```python
from FactorLib.alpha_volatility import AlphaVolatility

factor = AlphaVolatility(lookback=20)
vol = factor.calculate(data)  # data: DataFrame with timestamp, symbol, close
# Returns pd.Series of volatility values (high = volatile, low = stable)
```

---

### [2026-03-31] Task 12 — Alpha1 动量因子 / Alpha1 Momentum Factor

Created `FactorLib/alpha_momentum.py` with `AlphaMomentum(BaseFactor)` class:

- Calculates cumulative return over a configurable `lookback` window: `close.pct_change(lookback)` per symbol
- Default `lookback=10`, name auto-updated to reflect parameter (e.g. `AlphaMomentum(lookback=10)`)
- Bilingual docstrings with parameter/return documentation
- Updated `FactorLib/__init__.py` to export `AlphaMomentum`

**Verification**: 6 checks passed — inheritance, default NaN count, custom lookback NaN count, numerical correctness (0.025000), public export, repr.

**Usage**:
```python
from FactorLib.alpha_momentum import AlphaMomentum

factor = AlphaMomentum(lookback=10)
momentum = factor.calculate(data)  # data: DataFrame with timestamp, symbol, close
# Returns pd.Series of momentum values (positive = uptrend, negative = downtrend)
```

---

### [2026-03-31] Task 11 — BaseFactor 抽象基类 / BaseFactor Abstract Base Class

Created `FactorLib/` module directory with two files:

1. **`FactorLib/base.py`** — `BaseFactor(ABC)` abstract base class:
   - Inherits from `ABC`, enforces `calculate(data: pd.DataFrame) -> pd.Series` via `@abstractmethod`
   - `name` attribute defaults to class name
   - Bilingual docstrings with parameter/return documentation for `calculate()`
   - `__repr__` for readable string representation

2. **`FactorLib/__init__.py`** — public exports: `BaseFactor`

**Verification**: 6 checks passed — abstract (cannot instantiate), inherits ABC, has calculate method, docstring present, subclass works and returns Series, repr correct.

**Usage**:
```python
from FactorLib.base import BaseFactor

class MyFactor(BaseFactor):
    def calculate(self, data: pd.DataFrame) -> pd.Series:
        # data: long-format DataFrame with timestamp, symbol, OHLC
        return data.groupby('timestamp')['close'].pct_change()
```

---

### [2026-03-31] Task 10 — 短期数据加载器验证测试 / Short-Term Loader Validation Tests

Created `scarffold/.agent/test_short_term_loader.py` with 4 validation tests:

1. **Import and Inheritance** — ShortTermDataLoader imports and correctly inherits BaseDataLoader; required methods (receive, compile, dataset) exist
2. **Data Shape** — Required columns (timestamp, symbol, open, high, low, close) present; OHLC columns are numeric; loaded 80 symbols, 320 rows (lookback=1d)
3. **No Missing Data** — Zero NaN in all critical columns; no duplicate (timestamp, symbol) pairs; all rows satisfy high >= low
4. **Compile Validation** — Missing columns raises ValueError with descriptive message; dataset property auto-triggers receive() → compile() chain

All 4 tests PASSED.

**Usage**: `python scarffold/.agent/test_short_term_loader.py`

---

### [2026-03-31] Task 9 — 短期数据加载器 / Short-Term Data Loader

Created `Cross_Section_Factor/short_term_loader.py` with `ShortTermDataLoader` class:
- Extends `BaseDataLoader` for compatibility with `MultiAssetDataHandler`
- Wraps `CryptoDB_feather.core.reader.load_multi_klines` with short-term window defaults
- `lookback_days` parameter (default 30) controls time window from current UTC time
- `compile()` validates required columns: `timestamp`, `symbol`, `open`, `high`, `low`, `close`
- Supports custom `symbols` list, `exchange`, `kline_type`, `interval`, `num_workers`

**Bug fix**: Fixed pre-existing broken imports in `market_api.py` and `utils.py` — `from utils.common` → `from CryptoDataProviders.utils.common` (absolute import path).

**Verification**: Import OK, dataset loading OK (80 symbols, 320 rows for 1-day lookback), BaseDataLoader inheritance OK, column validation OK.

**Usage**:
```python
from Cross_Section_Factor.short_term_loader import ShortTermDataLoader

loader = ShortTermDataLoader(lookback_days=30)
df = loader.dataset  # long-format DataFrame with all pairs
```

---

### [2026-03-31] Task 8 — 管道摘要日志 / Pipeline Summary Logging

Added pipeline summary logging to `scripts/pipeline.py`. Enhanced `_print_summary()` to display per-step statistics:

- **Bulk download**: updated pairs, intervals, kline type
- **Cleanup**: scanned pairs, active pairs, cleaned pairs
- **Error count** + final status (`ALL STEPS PASSED` / `COMPLETED WITH ERRORS`)

Minimal changes to sub-functions:
- `update_bulk.main()` now returns `{pairs, intervals, kline_type}`
- `cleanup_fake_data.run_cleanup()` now returns `{scanned, active, cleaned}`

**Verification**: syntax OK (all 3 files), summary output correct in 3 scenarios (all success, bulk failed, all skipped), CLI help OK.

**Usage**: Pipeline output now ends with a structured summary block showing key metrics from each step.

---

### [2026-03-31] Task 7 — 统一管道脚本 / Unified Pipeline Script

Created `scripts/pipeline.py`. Runs two steps sequentially:

1. Bulk historical download via S3 (`update_bulk.main`)
2. Cleanup fake data of delisted trading pairs (`cleanup_fake_data.run_cleanup`)

CLI options: `--kline-type`, `--interval`, `--dry-run` (default), `--execute`, `--skip-bulk`, `--skip-cleanup`. Error handling: if bulk download fails, cleanup is skipped.

**Usage**: `python scripts/pipeline.py` (dry run) or `python scripts/pipeline.py --execute` (real execution).

---

### [2026-03-31] Task 6 — 数据管道端到端验证 / Pipeline E2E Validation

Created `scarffold/.agent/test_validation.py` covering 4 validation areas:

1. **Retry decorator** — import OK, retries on expected failures with exponential backoff
2. **Active trading pairs validator** — returns correct types (`Set[str]`), valid_pairs is proper subset of active ∩ local (541 active, 819 local, 540 valid)
3. **update_api.py import chain** — all dependencies import successfully
4. **db_manager retry wrapping** — retry parameters and exception types correctly configured

All 4 tests PASSED.

**Usage**: `python scarffold/.agent/test_validation.py`

---

### [2026-03-31] Task 5 — 活跃交易对过滤器集成 / Active Pair Filter Integration

Integrated `validate_active_trading_pairs` into `update_api.py`. Only `valid_pairs` (intersection of API active AND local DB) are passed to `run_binance_rest_updater()`, preventing wasted API calls on delisted pairs.

**Usage**: `python scripts/update_api.py` — automatically filters out delisted pairs before fetching.

---

### [2026-03-31] Task 4 — 活跃交易对校验工具 / Active Pair Validator

Created in `trading_pairs.py`:

- `get_active_trading_pairs_from_api()` — fetches from Binance `/fapi/v1/exchangeInfo`
- `get_local_trading_pairs()` — scans local database directory
- `validate_active_trading_pairs()` — returns `(active_pairs, local_pairs, valid_pairs)` tuple

**Usage**: `validate_active_trading_pairs(db_root_path, exchange='binance', proxy=None)`

---

### [2026-03-31] Task 3 — DB Manager 重试集成 / DB Manager Retry Integration

Integrated retry into `db_manager.py`'s `run_binance_rest_updater`. Extracted per-pair logic into `_fetch_and_save_pair()` with `retry_with_backoff(max_retries=2, base_delay=5.0, max_delay=30.0, exponential_base=5.0)`. Second retry layer on top of market_api's per-request retry.

**Usage**: `run_binance_rest_updater` behavior unchanged externally — retry is transparent.

---

### [2026-03-31] Task 2 — Market API 重试集成 / Market API Retry Integration

Integrated `retry_with_backoff` into `market_api.py`. Extracted inline retry logic into `_request_kline_batch()` decorated with `@retry_with_backoff`. Handles 502/429/rate-limit errors with exponential backoff + jitter.

**Usage**: `fetch_klines` works the same externally — retry is transparent.

---

### [2026-03-31] Task 1 — 重试工具模块 / Retry Utility Module

Created `CryptoDataProviders/utils/retry.py` with `retry_with_backoff` decorator (exponential backoff, jitter, max_delay cap, exception filtering, on_retry callback). Updated `utils/__init__.py` exports.

> NOTE: shell/WSL unavailable — verified logic statically, no runtime tests.

---

### [2026-03-31 00:00] Planning Phase

Generated `architecture.md` (tech stack, file layout, core flow), `tasks.json` (28 fine-grained tasks across 4 phases), root `requirements.txt`.
