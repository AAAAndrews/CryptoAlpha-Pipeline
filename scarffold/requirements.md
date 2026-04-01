# 跑通第一个投研流程

## 1. 通用数据加载器 — 扩展 ShortTermDataLoader 为通用 K 线加载器

- 基于现有 `Cross_Section_Factor/short_term_loader.py` 中的 `ShortTermDataLoader`，扩展为一个通用数据加载器（可重命名或新建类）
- 支持参数化过滤：`start_time`、`end_time`、`symbols`（交易对列表）、`exchange`、`kline_type`、`interval` 等
- 全样本加载 = 该通用加载器的宽约束调用（不传时间范围/不传 symbols 过滤）
- 保持与 `FactorLib` / `FactorAnalysis` 的 MultiIndex `(timestamp, symbol)` 格式兼容
- 数据验证：格式校验、无缺失值、无重复行、high >= low

## 2. 因子案例编写 — 实现 (open-close)/(high-low) 因子

- 在 `FactorLib/` 中新增因子文件（如 `alpha_price_range.py`），继承 `BaseFactor`
- 因子公式：`(open - close) / (high - low)`，衡量当日价格振幅中收盘相对于开盘的偏离程度
- 需处理边界情况：`high == low` 时（振幅为零）因子值为 NaN 或 0
- 注册到 `FactorLib` 的 registry 中，与现有 Alpha1/Alpha2 一致

## 3. 收益矩阵计算 — 基于全样本数据构建截面收益

- 使用全样本 K 线数据计算日收益率，产出 `pd.Series` with MultiIndex `(timestamp, symbol)`
- 这是 FactorAnalysis 所有组件的统一输入（IC/RankIC/分组/净值曲线均需要因子值 + 收益率）
- 需注意：每个截面（时间点）的收益是 T+1 收益（即当日因子值对应次日收益），这是标准做法
- 收益率矩阵需要与因子值对齐：相同的时间戳和交易对索引
- 验证：无全 NaN 截面、截面内至少有一定数量的有效交易对

## 4. 因子计算与对齐 — 计算因子值并与收益矩阵配对

- 使用通用加载器加载全样本数据
- 调用 `FactorLib` 中的因子计算因子值
- **收益率计算支持多种标签**：
  - `close2close`：`close.shift(-1) / close - 1`（经典日收益率）
  - `open2open`：`open.shift(-1) / open - 1`（开盘到次日开盘）
- 因子值与收益矩阵按 `(timestamp, symbol)` 索引对齐
- 关键对齐逻辑：因子值在 T 时刻计算，收益使用 T+1 的前向收益，确保**不存在未来函数**
- 对齐后剔除任一侧为 NaN 的行，产出干净的 `(factor_value, forward_return)` 配对数据

## 5. IC/RankIC/ICIR 分析 — 因子预测能力检验（跑通验证）

- 使用 `FactorAnalysis.calc_ic`、`calc_rank_ic`、`calc_icir` 对每个因子进行 IC 系列分析
- 对两种收益率标签（close2close、open2open）分别计算
- **目的为验证功能可正常跑通**：确认 IC 系列能正确产出 Series、ICIR 能产出 float，数值不作为判断因子好坏的标准
- 简要检查 IC 均值在合理范围内（|IC| < 0.5），仅作为未来函数的基本筛查，不做因子有效性判断

## 6. 分组收益分析 — 分位数分组 + 分组平均收益

- 使用 `FactorAnalysis.quantile_group` 对因子值进行横截面分位数分组（如 5 组）
- 计算每组的平均收益（分别 close2close / open2open），验证分组收益的单调性
- **目的为验证功能跑通**：确认分组标签正确产出、分组收益计算无误
- 输出：各组平均收益表，可观察因子是否具有排序能力

## 7. 净值曲线回测 — 纯多/纯空/多空对冲

- 使用 `FactorAnalysis.calc_long_only_curve`、`calc_short_only_curve`、`calc_top_bottom_curve` 分别构建净值曲线
- 分别对两种收益率标签（close2close、open2open）回测
- **目的为验证功能跑通**：确认三条净值曲线能正确产出、起始值为 1.0、无 NaN
- 不对曲线的盈利能力做判断

## 8. 交易成本扣除 — 滑点成本模拟

- 使用 `FactorAnalysis.deduct_cost`（Task 22 待实现）对净值曲线进行交易成本扣除
- **目的为验证功能跑通**：确认扣费后净值低于扣费前、无异常值
- 成本参数使用合理的默认值（如 0.1% 滑点）

## 9. 绩效指标计算 — Sharpe/Calmar/Sortino 比率

- 使用 `FactorAnalysis` 中的 Sharpe、Calmar、Sortino 比率计算（Task 23 待实现）对净值曲线评估
- 分别对扣费前/扣费后的净值曲线计算绩效指标
- **目的为验证功能跑通**：确认三个比率能正确产出数值、年化处理正确
- 不对指标高低做判断

## 10. 端到端流程编排 — 一键运行完整投研流程

- 创建一个端到端运行脚本（如 `scripts/run_factor_research.py`），串联上述所有步骤：
  1. 数据加载（通用加载器）
  2. 收益率计算（支持 close2close / open2open 标签）
  3. 因子计算（FactorLib 中所有已注册因子）
  4. 因子与收益对齐
  5. IC/RankIC/ICIR 分析
  6. 分组收益分析
  7. 净值曲线构建
  8. 交易成本扣除
  9. 绩效指标计算
  10. 结果汇总输出（print 或 DataFrame）
- 脚本支持 CLI 参数：指定因子、收益率标签、分组数、成本参数等
- 运行完毕后输出结构化结果，确认全流程跑通无报错



# Alphalens 借鉴建议 / Suggestions from alphalens

> 基于 [quantopian/alphalens](https://github.com/quantopian/alphalens) 源码分析，对 FactorAnalysis 模块的改进建议。

## 1. 增加换手率指标 / Add Turnover Metrics

alphalens 实现了两个关键的换手率指标：

- `quantile_turnover()`: 衡量每个分组每期有多少"新面孔"进入（0.0 = 完全稳定, 1.0 = 全部替换）
- `factor_rank_autocorrelation()`: 因子排名的自相关系数，衡量排名稳定性

当前 `FactorAnalysis` 没有任何换手率度量。一个因子 IC 很高但换手率极高时，收益会被交易成本吞噬，这在加密货币市场（高波动、高手续费）尤其关键。alphalens 的经验判断标准：

| 自相关系数 | 含义 |
|---|---|
| > 0.95 | 高度稳定，预期交易成本低 |
| 0.80–0.95 | 中等稳定 |
| 0.50–0.80 | 不稳定，实现成本高 |
| < 0.50 | 接近随机，因子可能是噪声主导 |

**建议**: 在 `metrics.py` 中新增 `calc_turnover(factor, n_groups)` 和 `calc_rank_autocorr(factor)`，在 `FactorEvaluator` 编排中默认计算并输出到 report。

## 2. 增加 IC 统计显著性分析 / Add IC Statistical Significance

alphalens 的 IC 信息表报告了远比当前 `FactorAnalysis` 更丰富的 IC 统计量：

| 指标 | 说明 | 当前状态 |
|---|---|---|
| IC Mean | 日均 IC | 有（ICIR 间接体现） |
| IC Std. | IC 波动率 | 有（ICIR 间接体现） |
| Risk-Adjusted IC | IC Mean / IC Std. | 即 ICIR，已有 |
| **t-stat(IC)** | 单样本 t 检验，IC 是否显著不为零 | **缺失** |
| **p-value(IC)** | 统计显著性水平 | **缺失** |
| **IC Skew** | IC 分布偏度，衡量极端值方向 | **缺失** |
| **IC Kurtosis** | IC 分布峰度，衡量尾部风险 | **缺失** |

t-stat 和 p-value 能直接判断因子的预测能力是否具有统计显著性（而非随机噪声）。IC Skew 和 Kurtosis 能帮助识别 IC 是否由少数极端截面驱动。当前 `calc_icir` 只返回一个 float，信息量有限。

**建议**: 新增 `calc_ic_stats(factor, returns)` 函数，返回包含 mean、std、icir、t_stat、p_value、skew、kurtosis 的 `pd.Series` 或 dict，替代或补充当前单一的 `calc_icir`。

## 3. 增加零值感知分组 / Add Zero-Aware Quantization

alphalens 的 `quantize_factor` 函数有一个 `zero_aware=True` 模式：将因子值先按正/负分为两部分，各自独立做分位数分组，然后统一标记为 1 到 N。负值一侧标记为最做空组到最不空组，正值一侧标记为最不多组到最多多组。

这在加密货币因子分析中特别有价值：许多技术因子（如动量、波动率）自然以零为分界线——正动量 = 看多，负动量 = 看空。使用 `zero_aware` 分组可以确保零值附近的资产不会被随机分配到多空两侧，避免分组边界穿越零点导致的信号失真。

当前 `quantile_group` 使用 `pd.qcut` 做简单等频分组，不考虑因子的正负结构。

**建议**: 在 `grouping.py` 的 `quantile_group` 中增加 `zero_aware: bool = False` 参数。当 `zero_aware=True` 时，将因子按正负拆分后各自做分位数分组，最终标签仍然为 0 到 n_groups-1。

## 4. 增加数据质量追踪与 max_loss 机制 / Add Data Quality Tracking

alphalens 在数据准备阶段有一个精巧的 `max_loss` 机制：在合并因子值、前向收益率、分组标签时，逐阶段追踪 NaN 丢弃比例，如果总数据损失超过阈值（默认 35%），抛出 `MaxLossExceededError` 并打印各阶段丢失详情。

当前 `FactorAnalysis` 的各函数内部静默过滤 NaN/Inf，不报告丢弃了多少数据。如果因子和收益率对齐后 80% 的数据都丢失了，分析结果毫无意义但不会有任何警告。

**建议**: 在 `evaluator.py` 的 `FactorEvaluator` 初始化阶段增加数据质量检查步骤，计算因子与收益率对齐后的数据覆盖率（非 NaN 比例），低于阈值时发出 `UserWarning` 或抛出异常。可以作为一个可选的 `max_loss: float = 0.35` 参数。

## 5. 引入分组中性化权重构建 / Add Group-Neutral Weight Construction

alphalens 的 `factor_weights()` 函数通过 `demeaned` 和 `group_adjust` 两个正交标志位，支持 5 种权重构建模式：

| demeaned | group_adjust | 效果 |
|---|---|---|
| False | False | 原始因子值归一化，多头偏斜 |
| True | False | 去均值后归一化，金额中性多空 |
| False | True | 组内归一化后跨组再归一化，组中性 |
| True | True | 组中性 + 金额中性 |

当前 `FactorAnalysis` 的净值曲线只做简单等权分组，不考虑行业/板块中性化。在加密货币市场中，BTC 生态、DeFi、Layer2 等不同板块可能有系统性收益差异，不做中性化会导致因子收益被板块效应污染。

**建议**: 在 `portfolio.py` 中增加 `calc_neutralized_curve(factor, returns, groups, ...)` 函数，或在现有净值曲线函数中增加 `groups` 参数（行业标签 Series），支持组内去均值后再构建多空组合。`groups` 可以通过交易对名称前缀自动推断（如 `BTC*` = BTC 生态）。

## 6. 支持多调仓频率衰减分析 / Add Multi-Holding-Period Decay Analysis

alphalens 原生支持多周期前向收益率（1D/5D/10D），但其设计有一个问题：不同周期的收益率在量纲上不可直接对比（日收益率 vs 5 日收益率），导致画图和报告缺乏可比性。

更好的方式是**保持统一的基础收益率（日频），通过改变调仓频率来分析信号衰减**。例如：
- **调仓周期 = 1 天**: 每天根据最新因子值重新分组，产生每日调仓净值曲线
- **调仓周期 = 5 天**: 每 5 天根据最新因子值重新分组，中间 4 天持仓不变
- **调仓周期 = 10 天**: 每 10 天重新分组，中间 9 天持仓不变

这样三条净值曲线都基于相同的日收益率，可以直接在同一张图上对比，观察"多调仓 vs 少调仓"的收益差异。差异越小，说明因子信号持续性越好（低频调仓也不会损失太多 alpha）。

当前 `FactorAnalysis` 的净值曲线函数每天都重新分组，等效于调仓周期 = 1 天，无法评估更长持有期的效果。

**建议**: 在 `portfolio.py` 的净值曲线函数中增加 `rebalance_freq: int = 1` 参数，表示每多少天重新调仓。在非调仓日，沿用上一个调仓日的分组结果计算当日收益。在 `FactorEvaluator` 中默认对 `rebalance_freq=[1, 5, 10]` 分别计算并输出到 report，方便对比信号衰减特征。

## 7. 采用 Tear Sheet 分层编排模式 / Adopt Tear Sheet Composition Pattern

alphalens 的核心设计模式是将分析分为四个正交子报告（tear sheets），每个子报告独立可调用：

| 子报告 | 内容 | 对应 FactorAnalysis |
|---|---|---|
| Summary | 收益表 + IC 表 + 换手率表 | report.py 的 `generate_report` |
| Returns | 分组收益 + 净值曲线 + 多空价差 | portfolio.py + metrics.py (Sharpe等) |
| Information | IC 时序 + 直方图 + QQ图 + 月度热力图 | metrics.py (IC/RankIC) |
| Turnover | 分组换手率 + 排名自相关 | 待新增 |

alphalens 通过 `create_full_tear_sheet()` 组合所有子报告，但也允许单独调用 `create_returns_tear_sheet()` 等。这种分层模式的好处是：用户可以按需运行部分分析（节省计算时间），也可以自定义组合。

当前 `FactorAnalysis` 计划中的 `FactorEvaluator` 是一个"全有或全无"的编排器。建议参考 alphalens 的分层思路，将 `FactorEvaluator` 的分析步骤拆分为可独立调用的方法，`generate_report` 可以选择性组合。

**建议**: 在 `evaluator.py` 中将 `FactorEvaluator` 设计为分阶段执行：`run_metrics()` → `run_grouping()` → `run_curves()` → `run_turnover()` → `generate_report(select=None)`。用户可以通过 `select=["returns", "information"]` 只运行感兴趣的子分析，也可以调用 `run_all()` 执行完整流程。
