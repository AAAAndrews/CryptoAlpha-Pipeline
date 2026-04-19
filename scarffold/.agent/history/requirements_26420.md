# 评估环节性能优化需求 (Evaluation Performance Optimization)

> 基准测试日期: 2026-04-09
> 测试环境: Windows 11, 15.4 GB RAM, Anaconda Python 3.10
> 数据规模: 722 symbols × 54,759 timestamps × 13,065,981 rows (全量永续合约, 1h K线, ~6.3年)

---

## 1. 基准测试结果

### 1.1 全流程耗时

使用 `scripts/run_factor_research.py --factor AlphaMomentum --interval 1h --kline-type swap` 全量回测:

| 步骤 | 耗时 | 占比 |
|------|------|------|
| Data Loading | 37.5s | 3.6% |
| Factor Calc | 2.6s | 0.2% |
| Returns Calc | 3.3s | 0.3% |
| Alignment | 11.0s | 1.1% |
| **Evaluation (run_all)** | **990.0s** | **94.8%** |
| **总计** | **1044.3s (17.4min)** | 100% |

> 全量模式 OOM, 使用 chunk_size=200 完成。评估环节占绝对瓶颈。

### 1.2 评估环节细粒度计时 (chunk_size=500)

使用 `scripts/bench_eval_profiling.py` 对 run_all() 内部每个函数逐一计时:

#### 五大步骤耗时

| 步骤 | 耗时 | 占比 |
|------|------|------|
| run_neutralize | 172.4s | 20.1% |
| split_into_chunks (重复调用开销) | 174.5s | 20.3% |
| run_curves | 109.9s | 12.8% |
| run_grouping | 55.9s | 6.5% |
| run_metrics | 43.3s | 5.0% |
| run_turnover | 35.8s | 4.2% |
| 其他 (merge/ratios/cost 等) | 266.8s | 31.1% |

#### 内部操作瓶颈排名

| 排名 | 操作 | 耗时 | 占比 | 调用位置 |
|------|------|------|------|---------|
| **1** | **`groupby.apply` (portfolio)** | **136.6s** | **32.7%** | run_curves + run_neutralize |
| **2** | **`groupby.apply` (quantile_group)** | **83.0s** | **19.9%** | run_grouping + run_neutralize |
| **3** | **`split_into_chunks` (重复调用)** | **174.5s** | **20.3%** | 每个 step 重复调用 |
| 4 | `groupby.transform` (demean) | 17.0s | 4.1% | run_neutralize |
| 5 | `unstack` (all) | 16.5s | 3.9% | calc_ic / calc_rank_ic / turnover / autocorr |
| 6 | `groupby.rank` (autocorr) | 2.3s | 0.5% | run_turnover |
| 7 | `rank(axis=1)` (RankIC) | 1.5s | 0.4% | calc_rank_ic |
| 8 | `numpy Pearson` (all) | 2.1s | 0.5% | calc_ic / calc_rank_ic / autocorr |

---

## 2. 根因分析

### 2.1 瓶颈 #1: `split_into_chunks` 重复调用 (20.3%, 174.5s)

**问题**: `run_metrics`, `run_grouping`, `run_curves`, `run_turnover`, `run_neutralize` 五个步骤各自独立调用 `split_into_chunks`，每次都对 factor/returns/group_labels 做一次 `isin()` 布尔索引过滤。

- 每个 step 调用 1~3 次 split_into_chunks
- 每次 split 遍历全部 1300 万行做 `index.get_level_values(0).isin(chunk_ts)`
- 总计约 1650 次 isin 过滤，完全冗余

**代码位置**: `FactorAnalysis/evaluator.py` 各 run_* 方法内的 `split_into_chunks(self.factor, self.chunk_size)` 调用。

### 2.2 瓶颈 #2: `groupby.apply` (portfolio) (32.7%, 136.6s)

**问题**: `_portfolio_curves_core()` 中 `df.groupby(level=0).apply(_portfolio_returns)` 对每个时间截面 (54,759 个) 调用一次纯 Python 函数，每次内部做:
1. `g["returns"].notna() & np.isfinite(g["returns"])` — 构建布尔 mask
2. `g["label"].isin(top_labels)` — 构建 long/short mask
3. `.mean()` — 计算均值

虽然单个截面数据量不大 (~722 symbols)，但 Python 函数调用开销 × 54,759 次累积极大。run_curves 调一次 (67.3s)，run_neutralize 内的 calc_portfolio_curves 再调一次 (69.3s)。

**代码位置**: `FactorAnalysis/portfolio.py:206` — `daily = df.groupby(level=0).apply(_portfolio_returns)`

### 2.3 瓶颈 #3: `groupby.apply` (quantile_group) (19.9%, 83.0s)

**问题**: `quantile_group()` 中 `factor_valid.groupby(level=0, group_keys=False).apply(_assign_group)` 对每个截面调用一次 `_assign_group`，内部执行 `pd.qcut()`。

- run_grouping 调一次 (41.1s)
- run_neutralize 内对中性化因子再调一次 (41.9s)
- 总计 110 chunks × 2 × ~500 截面 = ~110,000 次 Python 函数调用

**代码位置**: `FactorAnalysis/grouping.py:106` — `group_result = factor_valid.groupby(level=0, group_keys=False).apply(_fn)`

---

## 3. 优化方案

### P0: split_into_chunks 一次性计算 (预期 -170s, ↓20%)

**方案**: 在 `run_all()` 入口处一次性对 factor/returns/group_labels 执行 `split_into_chunks`，将 chunk 列表以参数形式传入各 run_* 方法，消除重复的 isin 过滤。

**修改范围**:
- `evaluator.py`: `run_all()` 增加 chunk 列表缓存逻辑
- `evaluator.py`: 各 `run_*` 方法签名增加可选 `chunk_list` 参数
- 各方法内部检查 `chunk_list` 是否已提供，有则跳过 split

**向后兼容**: `chunk_list=None` 时行为不变 (仍内部 split)。

### P1: portfolio groupby.apply → 向量化 (预期 -100s, ↓12%)

**方案**: 将 `_portfolio_curves_core()` 中的 `groupby.apply(_portfolio_returns)` 替换为向量化操作:
1. `labels.unstack()` → 2D 矩阵 (timestamp × symbol)
2. `returns.unstack()` → 2D 矩阵
3. 用 numpy boolean mask 按组取均值: `np.where(labels_mat == g, returns_mat, np.nan).mean(axis=1)`
4. 避免 Python 函数调用，单次 numpy 批量计算

**修改范围**:
- `FactorAnalysis/portfolio.py`: 重写 `_portfolio_curves_core()`
- 保持公共 API (calc_portfolio_curves / calc_long_only_curve / calc_short_only_curve / calc_top_bottom_curve) 签名不变

**风险**: 需验证 NaN 处理、数值一致性 (diff < 1e-8)、_raw 模式兼容。

### P2: quantile_group groupby.apply → 向量化 (预期 -60s, ↓7%)

**方案**: 将 `quantile_group()` 中的 `groupby.apply(_assign_group)` 替换为:
1. `factor.unstack()` → 2D 矩阵 (timestamp × symbol)
2. `numpy.percentile` 或 `scipy.stats.rankdata` 按行计算分位数
3. 用 `pd.cut` 或直接 `np.searchsorted` 分组

**修改范围**:
- `FactorAnalysis/grouping.py`: 重写 `quantile_group()` 核心逻辑
- 保持公共 API 签名不变，包括 zero_aware 模式

**风险**: `pd.qcut` 的 `duplicates='drop'` 行为需用 numpy 等价实现，处理重复值边界情况。

### P3: neutralize 内部操作合并 (预期 -40s, ↓5%)

**方案**: run_neutralize 内部当前执行: demean → quantile_group → calc_portfolio_curves，其中 demean 和 quantile_group 各自独立做一次 groupby 操作。可合并为:
1. demean + re-rank 在单次 unstack 矩阵上完成 (避免两次 groupby)
2. portfolio curves 复用 P1 的向量化实现

**修改范围**:
- `FactorAnalysis/neutralize.py`: 重写 `calc_neutralized_curve()` 内部流程

---

## 4. 预期效果

| 阶段 | 当前耗时 | 优化后预期 | 加速比 |
|------|---------|-----------|--------|
| split_into_chunks | 174.5s | ~5s | 35× |
| groupby.apply (portfolio) | 136.6s | ~20s | 7× |
| groupby.apply (quantile_group) | 83.0s | ~15s | 5.5× |
| groupby.transform (demean) | 17.0s | ~5s | 3.4× |
| 其他 (已向量化) | 447.9s | ~447.9s | 1× |
| **评估总计** | **858.5s** | **~493s** | **1.7×** |
| **全流程总计** | **~1044s** | **~530s** | **2.0×** |

> 注: 全流程耗时从 ~17min 降至 ~9min。若后续增大 chunk_size (内存允许时) 可进一步缩短。

---

## 5. 硬约束

- 所有优化数值差异 < 1e-8
- 既有测试不回归 (1179 passed)
- 公共 API 签名不变
- chunk_size 分块模式正常工作
- _raw 模式兼容
- zero_aware 分组模式兼容

---

## 6. 执行顺序建议

```
P0 (split_into_chunks) → P1 (portfolio 向量化) → P2 (quantile_group 向量化) → P3 (neutralize 合并)
```

每个优化独立可交付，P0 无需修改核心计算逻辑，风险最低，应最先实施。

---

## 7. 快速回测 vs 全量回测分级方案

### 7.1 设计思路

因子投研存在天然的两阶段工作流:

```
Stage 1: 因子筛选 (Factor Screening)
  └─ 从数百个候选因子中快速淘汰无效因子
  └─ 关键问题: "这个因子有没有预测能力?" "是否统计显著?" "信号衰减快吗?"
  └─ 需要跑 100~1000 次, 对单次耗敏感

Stage 2: 因子深度分析 (Factor Deep Dive)
  └─ 对 10~20 个通过筛选的因子做完整绩效检验
  └─ 关键问题: "PnL 曲线如何?" "换手成本多高?" "是否存在风格偏差?"
  └─ 只跑少量因子, 可接受较长耗时
```

当前 run_all() 把所有指标打包在一起, 无法区分这两个阶段。需要拆分为两条管道。

### 7.2 指标依赖链分析

每个指标的计算依赖关系决定了它属于哪条管道:

```
Layer 0 (纯向量化, 无 groupby.apply):
  ├─ IC (Pearson)               ─ unstack + numpy Pearson
  ├─ Rank IC (Spearman)         ─ unstack + rank(axis=1) + numpy Pearson
  ├─ ICIR                       ─ IC 序列的 mean/std, 零成本
  ├─ IC Stats (t-stat/p-value)  ─ IC 序列的 scipy 统计, 零成本
  └─ Rank Autocorrelation       ─ groupby.rank + unstack + numpy Pearson

Layer 1 (依赖 quantile_group, 有 1 次 groupby.apply):
  ├─ Quantile Group Labels      ─ groupby.apply(qcut)
  └─ Turnover                   ─ labels.unstack + boolean ops (向量化)

Layer 2 (依赖 group labels + portfolio groupby.apply):
  ├─ Portfolio Curves           ─ groupby.apply (long/short/hedge)
  ├─ Sharpe / Calmar / Sortino  ─ 曲线衍生指标, 零成本
  └─ Cost-Adjusted Returns      ─ deduct_cost, 零成本

Layer 3 (依赖 demean + re-ranking + portfolio):
  └─ Neutralized Curve          ─ demean + quantile_group + portfolio
```

**Layer 0 是天然的快速回测候选** — 全部向量化, 不依赖 quantile_group, 不依赖 portfolio 计算。

### 7.3 指标价值/成本排序

按 "对因子筛选的决策价值" 排序, 标注计算成本:

| 排名 | 指标 | 决策价值 | 计算成本 | 依赖 | 管道 |
|------|------|---------|---------|------|------|
| 1 | **ICIR** | ★★★★★ 判断因子质量的单一最佳指标 | ~0s (IC 衍生) | IC | Quick |
| 2 | **Rank IC Mean** | ★★★★★ 比 IC 更鲁棒, 捕捉非线性关系 | ~2s | — | Quick |
| 3 | **IC Mean** | ★★★★ 因子预测方向和强度 | ~2s | — | Quick |
| 4 | **IC t-stat / p-value** | ★★★★ 预测能力的统计显著性 | ~0s (IC 衍生) | IC | Quick |
| 5 | **Rank Autocorrelation** | ★★★ 信号衰减速度, 决定调仓频率 | ~5s | — | Quick |
| 6 | **IC Skew / Kurtosis** | ★★★ IC 分布形态, 尾部风险信号 | ~0s (IC 衍生) | IC | Quick |
| 7 | **Turnover** | ★★★ 信号稳定性, 实际换手成本 | ~6s | quantile_group | Standard |
| 8 | **Hedge Return** | ★★★ 多空对冲绝对收益 | ~67s | group + portfolio | Full |
| 9 | **Sharpe** | ★★★ 风险调整后收益 | ~0s (曲线衍生) | portfolio | Full |
| 10 | **Cost-Adjusted Hedge** | ★★☆ 扣费后真实收益 | ~0s (曲线衍生) | portfolio + cost | Full |
| 11 | **Calmar / Sortino** | ★★☆ 回撤/下行风险特征 | ~0s (曲线衍生) | portfolio | Full |
| 12 | **Neutralized Return** | ★★☆ 去风格偏差后的纯 alpha | ~128s | demean + qg + portfolio | Full |
| 13 | **Long/Short Return** | ★☆ 分侧收益拆解 | ~0s (曲线衍生) | portfolio | Full |

> 成本数据基于 722 symbols × 54,759 timestamps, chunk_size=500 实测。

### 7.4 两级管道定义

#### Quick Screen (快速回测)

```
目标: 秒级完成单因子筛选
受众: 因子挖掘 (GP) / 批量因子巡检

计算内容 (Layer 0 only):
  [1] IC / Rank IC / ICIR / IC Stats
  [2] Rank Autocorrelation

输出:
  - IC_mean, IC_std, RankIC_mean, RankIC_std
  - ICIR, IC_t_stat, IC_p_value
  - IC_skew, IC_kurtosis
  - avg_rank_autocorr

预估耗时 (当前, chunk_size=500):
  评估环节: ~13s
  全流程:   ~67s (含数据加载 37.5s + 因子计算 2.6s + 对齐 11s + 评估 13s)

预估耗时 (P0 优化后):
  评估环节: ~8s
  全流程:   ~62s
```

#### Full Analysis (全量回测)

```
目标: 完整绩效检验, 生成 Tear Sheet
受众: 因子深度分析 / 投研报告

计算内容 (Layer 0 + 1 + 2 + 3):
  [1] IC / Rank IC / ICIR / IC Stats           (Layer 0)
  [2] Rank Autocorrelation                      (Layer 0)
  [3] Quantile Group Labels                     (Layer 1)
  [4] Turnover                                  (Layer 1)
  [5] Portfolio Curves + Cost + Ratios          (Layer 2)
  [6] Neutralized Curve                         (Layer 3)

输出:
  - Quick Screen 的全部指标
  - n_groups_used
  - long_return, short_return, hedge_return
  - hedge_return_after_cost
  - sharpe, calmar, sortino (cost 前/后)
  - avg_turnover
  - neutralized_return
  - n_days

预估耗时 (当前, chunk_size=500):
  评估环节: ~858s
  全流程:   ~912s

预估耗时 (P0-P3 全部优化后):
  评估环节: ~250s
  全流程:   ~305s (~5min)
```

#### 对比

| | Quick Screen | Full Analysis |
|--|-------------|---------------|
| 指标数 | 8 | 20 |
| 评估耗时 (当前) | ~13s | ~858s |
| 评估耗时 (优化后) | ~8s | ~250s |
| 加速比 vs 当前全量 | **66×** | **3.4×** |
| groupby.apply 调用 | 0 次 | ~330,000 次 |
| 适用场景 | 因子筛选 / GP 挖掘 | 深度分析 / 报告 |

### 7.5 实现方案

在 `FactorAnalysis/evaluator.py` 中新增 `run_quick()` 方法:

```python
class FactorEvaluator:
    # 现有
    def run_all(self): ...       # 全量分析 (Layer 0-3)
    def run(self): ...           # 向后兼容 = run_all()

    # 新增
    def run_quick(self): ...     # 快速筛选 (Layer 0 only)
        """仅计算 IC/RankIC/ICIR/IC Stats/Rank Autocorrelation"""
        self.run_metrics()       # IC/RankIC/ICIR/IC_stats (向量化)
        self.run_rank_autocorr() # rank autocorrelation (向量化)
        return self
```

对应 `run_factor_research.py` 增加 `--mode` 参数:

```bash
# 快速筛选 (默认)
python scripts/run_factor_research.py --factor AlphaMomentum --mode quick

# 全量分析
python scripts/run_factor_research.py --factor AlphaMomentum --mode full
```

`generate_report()` 支持 `select` 参数按需输出:

```python
# 快速模式只输出 Layer 0 指标
ev.run_quick().generate_report(select=["metrics", "rank_autocorr"])

# 全量模式输出全部
ev.run_all().generate_report()
```


### 7.7 执行优先级

快速回测本身不需要任何性能优化即可工作 (Layer 0 已全部向量化), 因此:

```
1. [立即可做] 新增 run_quick() + --mode quick    ← 零风险, 即刻可用
2. [P0] split_into_chunks 优化                     ← 同时加速两条管道
3. [P1] portfolio 向量化                           ← 加速 Full 管道
4. [P2] quantile_group 向量化                      ← 加速 Full 管道, 也使 Turnover 可纳入 Quick
5. [P3] neutralize 合并                             ← 加速 Full 管道
6. [可选] P2 完成后, 将 Turnover 纳入 Quick 管道    ← 进一步丰富快速筛选指标
```

---

## 8. 辅助脚本

- `scripts/bench_full_backtest.py` — 全量回测基准测试 (全流程计时)
- `scripts/bench_eval_profiling.py` — 评估环节细粒度计时 (函数级 + 内部操作级)
