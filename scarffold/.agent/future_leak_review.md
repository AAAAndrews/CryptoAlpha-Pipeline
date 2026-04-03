# 未来函数审查报告 / Future Function (Lookahead Bias) Review Report

> 审查日期 / Review Date: 2026-04-04
> 审查范围 / Scope: FactorLib/, FactorAnalysis/, Cross_Section_Factor/
> 目的 / Purpose: 逐文件审查信号生成逻辑，识别未来数据泄露风险

---

## 1. 总体结论 / Executive Summary

**当前系统不存在实质性未来函数泄露风险。**

系统中出现的 `shift(-1)` 操作均用于 **T+1 前向收益率计算**（这是因子评估的标准设计：用 T 时刻的因子值预测 T→T+1 的收益率），而非用于因子信号生成。所有因子计算仅使用历史或当期数据。

唯一需要关注的边界问题是：**最后一个时间截面的前向收益为 NaN，但因子值仍然保留**——需确保对齐步骤正确剔除这些无效配对。

---

## 2. 模块逐文件审查 / Per-Module Review

### 2.1 FactorLib/ — 因子计算模块

| 文件 | 信号时点 | 价格字段 | Shift 操作 | T+1 对齐 | 风险等级 |
|------|----------|----------|------------|----------|----------|
| `base.py` | N/A (抽象基类) | N/A | 无 | 无 | LOW |
| `alpha_momentum.py` | T (当期) | close | `pct_change(lookback)` — 纯回看 | 无 | **LOW** |
| `alpha_volatility.py` | T (当期) | close | `pct_change().rolling(lookback).std()` — 纯回看 | 无 | **LOW** |
| `alpha_price_range.py` | T (当期) | open, high, low, close | 无 | 无 | **LOW** |
| `registry.py` | N/A (注册表) | N/A | 无 | 无 | LOW |
| `__init__.py` | N/A (模块导出) | N/A | 无 | 无 | LOW |

#### AlphaMomentum (alpha_momentum.py)
- **公式**: `momentum = close.pct_change(lookback)` → 等价于 `close / close.shift(lookback) - 1`
- **分析**: `pct_change(n)` 是纯回看操作，使用 T-n 期到 T 期的价格变化。`groupby("symbol")` 确保每个交易对独立计算。
- **结论**: 无未来数据依赖。

#### AlphaVolatility (alpha_volatility.py)
- **公式**: `volatility = close.pct_change().rolling(lookback).std()`
- **分析**: 先计算日收益率（回看 1 期），再取滚动窗口标准差。`rolling` 窗口仅包含历史数据。
- **结论**: 无未来数据依赖。

#### AlphaPriceRange (alpha_price_range.py)
- **公式**: `price_range = (open - close) / (high - low)`
- **分析**: 逐行独立计算，仅使用同一根 K 线的 OHLC 数据，无任何时序操作。
- **结论**: 无未来数据依赖。

---

### 2.2 FactorAnalysis/ — 因子绩效检验模块

| 文件 | 信号时点 | 价格字段 | Shift 操作 | T+1 对齐 | 风险等级 |
|------|----------|----------|------------|----------|----------|
| `returns.py` | T (因子) → T+1 (收益) | close / open | `shift(-1)` | **有 (设计如此)** | **LOW** |
| `alignment.py` | N/A (对齐工具) | N/A | 无 | 剔除 NaN | **LOW** |
| `metrics.py` | N/A (IC 计算) | N/A | 无 | 使用已对齐数据 | **LOW** |
| `grouping.py` | N/A (截面分组) | N/A | 无 | 无 | **LOW** |
| `portfolio.py` | N/A (净值构建) | N/A | 无 | 使用已对齐数据 | **LOW** |
| `turnover.py` | N/A (换手率) | N/A | `shift(1)` 回看 | 无 | **LOW** |
| `neutralize.py` | N/A (中性化) | N/A | 无 | 无 | **LOW** |
| `chunking.py` | N/A (分块工具) | N/A | 无 | 无 | **LOW** |
| `data_quality.py` | N/A (质量检查) | N/A | 无 | 无 | **LOW** |
| `cost.py` | N/A (成本扣除) | N/A | 无 | 无 | **LOW** |
| `report.py` | N/A (报告输出) | N/A | 无 | 无 | **LOW** |
| `evaluator.py` | N/A (编排器) | N/A | 无 | 依赖上游 | **LOW** |

#### returns.py — T+1 前向收益计算 (核心关注点)
- **公式**: `forward_return = next_price / current_price - 1`，其中 `next_price = prices.groupby(level=1).shift(-1)`
- **分析**:
  - `shift(-1)` 将价格向前移动一期，使得每个时间戳 T 的收益值为 `(price[T+1] - price[T]) / price[T]`
  - 这是 **标准的前向收益率定义**，不是未来函数
  - 每个交易对的最后一期收益自动为 NaN（无 T+1 数据）
  - 因子值在 T 时刻计算，收益率反映 T→T+1 的变化，因子与收益的时间关系正确
- **关键设计**: 因子不使用未来数据，收益使用 T+1 价格是评估因子预测能力的标准做法
- **结论**: 设计正确，无泄露风险。

#### alignment.py — 因子-收益对齐
- **分析**:
  - 执行内连接（inner join），仅保留两侧索引均存在的 (timestamp, symbol)
  - 剔除 factor 和 returns 中任何 NaN / inf 的行
  - 由于 returns 最后一个时间截面为 NaN，对齐后最后一个截面会被自动剔除
- **结论**: 对齐逻辑正确，有效防止末尾无效配对。

#### turnover.py — 换手率计算
- **分析**: 使用 `shift(1)` 比较当前与前一期的因子排名差异。`shift(1)` 是回看操作，安全。
- **结论**: 无未来数据依赖。

---

### 2.3 Cross_Section_Factor/ — DEAP 因子挖掘模块

| 文件 | 信号时点 | 价格字段 | Shift 操作 | T+1 对齐 | 风险等级 |
|------|----------|----------|------------|----------|----------|
| `datapreprocess.py` | T (因子) → T+1 (收益) | open | `shift(-1)`, `shift(-period-1)` | **有 (设计如此)** | **LOW** |
| `kline_loader.py` | N/A (数据加载) | OHLC | 无 | 无 | **LOW** |
| `short_term_loader.py` | N/A (数据加载) | OHLC | 无 | 无 | **LOW** |
| `crypto_gp_1h.py` | N/A (GP 入口) | N/A | 无 | 无 | **LOW** |
| `deap_alpha/ops/timeseries_ops.py` | T (当期) | N/A | `shift(d)` (正 d，回看) | 无 | **LOW** |
| `deap_alpha/ops/cross_section_ops.py` | T (当期) | N/A | 无 | 无 | **LOW** |
| `deap_alpha/ops/arithmetic_ops.py` | T (当期) | N/A | 无 | 无 | **LOW** |
| `deap_alpha/ops/worldquant_ops.py` | T (当期) | N/A | 无 | 无 | **LOW** |
| `deap_alpha/fitness_funcs/metrics/factor.py` | N/A (IC 计算) | N/A | 无 | 使用已对齐数据 | **LOW** |

#### datapreprocess.py — MultiAssetDataHandler.to_3d_array() (核心关注点)
- **公式**:
  ```python
  future_opens = single_symbol_data['open'].shift(-period-1)  # T+period+1 的 open
  current_opens = single_symbol_data['open'].shift(-1)        # T+1 的 open
  returns = (future_opens - current_opens) / current_opens     # T+1 到 T+period+1 的收益
  ```
- **分析**:
  - `shift(-1)` 和 `shift(-period-1)` 用于计算 **未来收益窗口**
  - 收益矩阵的时间语义: 第 i 行的收益 = 从 T[i]+1 到 T[i]+period+1 的收益率
  - 因子值（data_3d）使用当前时间戳的数据，不包含未来信息
  - **关键保护**: 第 149-154 行在非 update_mode 下截断尾部无效数据:
    ```python
    valid_len = len(timestamps) - period - 1
    data_3d = data_3d[:, :, :valid_len]
    returns_matrix = returns_matrix[:valid_len, :]
    timestamps = timestamps[:valid_len]
    ```
  - 这确保因子矩阵和收益矩阵在时间上正确对齐，尾部因 shift 产生的 NaN 被截断
- **结论**: 设计正确。shift(-N) 仅用于收益计算，因子数据被同步截断，无泄露。

#### timeseries_ops.py — 时间序列算子库
- **分析**:
  - `ts_delay(x, d)`: 使用 `pd.Series.shift(d)`，其中 d > 0（正整数），为回看操作
  - `ts_delta(x, d)`: `x - ts_delay(x, d)`，差分也是回看
  - `ts_min/ts_max/ts_sum/ts_std_dev/ts_mean/ts_rank/ts_zscore`: 均使用 `rolling(d)` 窗口，纯回看
  - `ts_corr/ts_cov`: 使用 `rolling(d).corr/cov`，纯回看
  - 所有算子的 d 参数均为正整数，方向为向后看（历史数据）
- **结论**: 所有时间序列算子均为纯回看操作，无未来数据依赖。

#### kline_loader.py — K 线数据加载器
- **分析**:
  - 从 feather 数据库加载历史 K 线数据
  - 支持时间范围过滤（start_time, end_time）
  - 校验逻辑: OHLC 无 NaN、无重复行、high >= low
  - 无任何时序操作，纯数据读取和校验
- **结论**: 数据加载器不引入未来数据。

---

## 3. 重点排查项检查 / Key Audit Items

| # | 排查项 | 结果 | 说明 |
|---|--------|------|------|
| 1 | 因子计算是否使用当日 close 后数据 | **PASS** | FactorLib 三个因子仅使用 OHLC 当期/历史数据 |
| 2 | KlineLoader 是否返回未来时间戳行 | **PASS** | KlineLoader 从 feather 读取历史数据，无时序操作 |
| 3 | align_factor_and_returns() 是否正确 drop 最后一行 | **PASS** | returns.py 中 shift(-1) 使最后截面收益为 NaN，alignment.py 的 NaN 过滤自动剔除 |
| 4 | shift(-1) 使用位置确认无反向 shift | **PASS** | shift(-1) 仅出现在 returns.py 和 datapreprocess.py 的收益计算中，不用于因子信号生成 |

---

## 4. shift 操作完整清单 / Complete Shift Operation Inventory

| 位置 | 操作 | 方向 | 用途 | 是否安全 |
|------|------|------|------|----------|
| `FactorAnalysis/returns.py:74` | `prices.groupby(level=1).shift(-1)` | 前看 1 期 | T+1 前向收益计算 | **安全** (设计如此) |
| `Cross_Section_Factor/datapreprocess.py:144` | `single_symbol_data['open'].shift(-period-1)` | 前看 N+1 期 | 收益矩阵计算 | **安全** (设计如此，尾部已截断) |
| `Cross_Section_Factor/datapreprocess.py:145` | `single_symbol_data['open'].shift(-1)` | 前看 1 期 | 收益矩阵计算 | **安全** (设计如此，尾部已截断) |
| `FactorAnalysis/turnover.py` | `shift(1)` | 回看 1 期 | 换手率计算 | **安全** |
| `FactorLib/alpha_momentum.py:57` | `pct_change(lookback)` 隐含 `shift(lookback)` | 回看 N 期 | 动量因子 | **安全** |
| `deap_alpha/ops/timeseries_ops.py:11` | `pd.Series.shift(d)` (d > 0) | 回看 d 期 | ts_delay 算子 | **安全** |

---

## 5. 风险评估汇总 / Risk Assessment Summary

| 风险等级 | 模块 | 数量 | 说明 |
|----------|------|------|------|
| **LOW** | FactorLib (全部) | 3/3 | 所有因子使用纯历史/当期数据 |
| **LOW** | FactorAnalysis (全部) | 12/12 | shift(-1) 仅用于收益计算，非因子生成 |
| **LOW** | Cross_Section_Factor (全部) | 9/9 | shift(-N) 仅用于收益矩阵，因子算子均为回看 |
| **MEDIUM** | (无) | 0 | — |
| **HIGH** | (无) | 0 | — |

---

## 6. 建议 / Recommendations

1. **当前状态良好**: 系统的因子-收益时间对齐设计正确，无未来函数泄露。
2. **保持警惕**: 未来新增因子时，确保 `calculate()` 方法中不使用 `shift(-N)` 等前看操作。
3. **自动化检测**: 建议在 Task 15 中实现 `check_future_leak.py` 自动化脚本，作为 CI 级别的防护层。
4. **文档维护**: 建议在因子开发指南中明确标注"因子计算禁止使用未来数据"的规范。
