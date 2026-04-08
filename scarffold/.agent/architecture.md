# Architecture

## Tech Stack
- **Language**: Python 3.10+
- **Data Storage**: Feather format (pyarrow), stored on local filesystem
- **Data Source**: Binance REST API + Binance S3 bulk download
- **Core Libraries**: pandas, numpy, scipy, numba, pyarrow, requests, rich, tqdm
- **Factor Framework**: DEAP (genetic programming for factor discovery)
- **Performance Evaluation**: Custom implementation (alphalens-style)
- **Memory Monitoring**: psutil, tracemalloc (分块处理内存追踪)
- **Visualization**: matplotlib (图表生成), jinja2 (HTML 报告模板渲染)

## Project Structure

```
CryptoAlpha-Pipeline/
├── requirements.txt                 # 项目级依赖
├── CryptoDataProviders/             # 数据源接口层
│   ├── config.py                    # 代理、超时、支持的交易所
│   ├── requirements.txt
│   ├── providers/
│   │   ├── binance_api/             # REST API 逐条拉取
│   │   │   ├── cons.py              # API endpoint 常量
│   │   │   ├── market_api.py        # K线拉取核心
│   │   │   └── utils.py
│   │   └── binance_bulk/            # S3 批量下载
│   │       ├── bulk_fetcher.py
│   │       ├── downloader.py
│   │       └── exceptions.py
│   └── utils/
│       ├── common.py                # 路径构建、时间解析、错误日志
│       ├── retry.py                 # 指数退避重试装饰器
│       └── trading_pairs.py         # 获取活跃交易对列表
│
├── CryptoDB_feather/                # Feather 存储层
│   ├── config.py                    # DB_ROOT_PATH, PROXY
│   └── core/
│       ├── storage.py               # 读写 feather, upsert, 去重
│       ├── bulk_manager.py          # S3 批量下载管理
│       ├── db_manager.py            # REST API 增量更新管理
│       └── reader.py                # 多交易对并发读取, load_multi_klines
│
├── scripts/                         # 运维脚本
│   ├── update_api.py                # 增量更新 (REST API)
│   ├── update_bulk.py               # 历史批量下载 (S3)
│   ├── cleanup_fake_data.py         # 清理下架交易对假数据
│   ├── pipeline.py                  # 统一管道: bulk + cleanup
│   ├── run_factor_research.py       # [增强] 端到端投研编排脚本 (--mode quick/full)
│   └── check_future_leak.py         # [新增] 未来函数检测脚本
│
├── Cross_Section_Factor/            # 因子挖掘
│   ├── datapreprocess.py            # 数据预处理, BaseDataLoader, MultiAssetDataHandler
│   ├── short_term_loader.py         # 短期数据加载器 (lookback_days)
│   ├── kline_loader.py              # [新增] 通用 K 线数据加载器 (参数化过滤)
│   ├── crypto_gp_1h.py              # 遗传编程因子挖掘入口
│   └── deap_alpha/                  # DEAP 因子框架
│       ├── fitness_funcs/
│       │   ├── core.py              # 评估核心, base_evaluate
│       │   └── metrics/
│       │       ├── factor.py        # IC, RankIC, ICIR, turnover
│       │       ├── performance.py   # Sharpe, Calmar, Sortino
│       │       └── utils.py         # high_minus_low, preprocess_data_jit
│       └── ops/                     # 算子库
│           ├── arithmetic_ops.py
│           ├── cross_section_ops.py
│           ├── timeseries_ops.py
│           └── worldquant_ops.py
│
├── FactorLib/                       # 独立因子库模块
│   ├── __init__.py
│   ├── base.py                      # BaseFactor 抽象基类
│   ├── alpha_momentum.py            # Alpha1 动量因子
│   ├── alpha_volatility.py          # Alpha2 波动率因子
│   ├── alpha_price_range.py         # [新增] Alpha3 价格振幅因子 (open-close)/(high-low)
│   └── registry.py                  # 因子注册表
│
├── FactorAnalysis/                  # 因子绩效检验模块
│   ├── __init__.py                  # 公共 API 导出
│   ├── metrics.py                   # IC/RankIC/ICIR/Sharpe/Calmar/Sortino + [新增] calc_ic_stats
│   ├── grouping.py                  # 分组分析 + [增强] zero_aware 参数
│   ├── portfolio.py                 # 净值曲线 + [增强] rebalance_freq 参数
│   ├── cost.py                      # 交易成本向量化扣除
│   ├── returns.py                   # [新增] 收益矩阵计算 (close2close/open2open)
│   ├── alignment.py                 # [新增] 因子值与收益矩阵对齐
│   ├── turnover.py                  # [新增] 换手率指标 (quantile_turnover, rank_autocorr)
│   ├── data_quality.py              # [新增] 数据质量追踪 (max_loss 机制)
│   ├── neutralize.py                # [新增] 分组中性化权重构建
│   ├── chunking.py                  # [新增] 分块处理核心逻辑 (时间分块/结果汇总)
│   ├── evaluator.py                 # [增强] FactorEvaluator → Tear Sheet 分层 + run_quick() 快速筛选
│   ├── report.py                    # 绩效报告汇总输出
│   └── visualization/               # [新增] 可视化子包
│       ├── __init__.py              # 公共导出
│       ├── charts.py                # 图表生成 (IC/分组/净值/换手率)
│       ├── tables.py                # 绩效表格 (信号灯标识)
│       └── report_html.py           # HTML 报告组装 (Jinja2 + base64 内嵌)
│
└── scarffold/.agent/                # Agent 脚手架
    ├── architecture.md
    ├── progress.md
    ├── tasks.json
    └── tasks.schema.json
```

## Core Flow

### 数据管道流程 (Data Pipeline Flow)
```
1. 获取活跃交易对列表 (trading_pairs.py → Binance API)
       ↓
2. 过滤校验: 仅保留正在交易的交易对 (active_symbols validator)
       ↓
3. 批量历史下载 (update_bulk.py → S3 bulk_fetcher → feather storage)
       ↓
4. 增量更新 (update_api.py → REST API fetch_klines → feather upsert)
       ↓
5. 清理下架交易对假数据 (cleanup_fake_data.py → truncate fake OHLC)
       ↓
6. 统一管道: 步骤 3 → 5 一键执行 (pipeline.py)
```

### 投研分析流程 (Factor Research Flow) — [新增]
```
1. 数据加载 (kline_loader.KlineLoader → 参数化过滤 → DataFrame)
       ↓
2. 收益矩阵计算 (returns.calc_returns → close2close / open2open T+1 前向收益)
       ↓
3. 因子计算 (FactorLib.BaseFactor.calculate → factor_values Series)
       ↓
4. 因子-收益对齐 (alignment.align_factor_returns → 干净配对数据)
       ↓
5. 数据质量检查 (data_quality.check_data_quality → max_loss 告警)
       ↓
6. 绩效检验 (FactorAnalysis.FactorEvaluator — Tear Sheet 分层)
   ├── Returns: 分组收益 + 净值曲线 + 多空价差 (含 rebalance_freq 衰减)
   ├── Information: IC/RankIC/ICIR/IC统计显著性 (t-stat, p-value, skew, kurtosis)
   ├── Turnover: 分组换手率 + 排名自相关
   └── Neutralize: 分组中性化权重 (可选)
       ↓
7. 报告输出 (report.py → summary DataFrame + metrics)
       ↓
8. 端到端编排 (run_factor_research.py → CLI 一键运行)
```

### 分块处理流程 (Chunked Processing Flow) — [新增]
```
1. FactorEvaluator(chunk_size=100) 初始化
       ↓
2. 按 rebalance 频率对齐的时间戳分块 (chunking.split_into_chunks)
       ↓
3. 逐块执行子分析 (IC/分组/净值/换手率/中性化)
   - 块间持仓连续性: 上一块末尾持仓 → 下一块初始持仓
   - 分块边界对齐 rebalance 频率, 避免跨块信号丢失
       ↓
4. 汇总聚合 (chunking.merge_chunk_results)
   - IC: 加权平均 (按样本量加权)
   - 净值曲线: 块间拼接 (cumprod 衔接)
       ↓
5. 内存监控日志 (每块峰值内存输出)
       ↓
6. chunk_size=None 时行为与改造前完全一致 (向后兼容)
```

### 未来函数检测流程 (Future Function Detection Flow) — [新增]
```
1. 加载因子数据 + 收益率数据
       ↓
2. 检查 factor 列与 forward_return 列的时间对齐
       ↓
3. 验证每个时间戳的 factor 值不依赖当日之后的任何数据字段
       ↓
4. 检查 shift(-1) 使用位置, 确认无反向 shift
       ↓
5. 排查 KlineLoader 数据边界 / align 对齐逻辑 / shift 位置
       ↓
6. 输出 PASS/FAIL 报告 (Markdown)
       ↓
7. 集成到 Pipeline step4.5 (可选) 或独立 CI 检查项
```

### 可视化流程 (Visualization Flow) — [新增]
```
1. FactorEvaluator 结果 → 图表生成
   ├── IC 时间序列图 (ic_timeseries.png): 滚动 IC + 累积 IC ± 1std
   ├── 分组收益对比图 (group_returns.png): 分位数累计收益 + 多空对冲
   ├── 组合净值曲线图 (portfolio_curves.png): long/short/hedge 含/不含手续费
   └── 换手率分布图 (turnover.png): 按时间序列换手率
       ↓
2. 综合绩效表格 (信号灯标识: ICIR > 0.5 绿 / < 0 红)
       ↓
3. Jinja2 模板组装 → report.html (图片 base64 内嵌, 无外部依赖)
       ↓
4. --viz-output 路径配置 + 自动创建目录
```

### Quick Screen / Full Analysis 两级管道 (Two-Tier Pipeline) — [新增]
```
Quick Screen (快速筛选):
  目标: 秒级完成单因子筛选, 适用于因子挖掘 (GP) / 批量巡检
  计算: Layer 0 only (IC/RankIC/ICIR/IC Stats/Rank Autocorrelation)
  耗时: ~8s (评估环节), 全流程 ~62s
  调用: ev.run_quick().generate_report(select=["metrics", "rank_autocorr"])
  CLI:  python scripts/run_factor_research.py --factor X --mode quick

Full Analysis (全量分析):
  目标: 完整绩效检验, 生成 Tear Sheet
  计算: Layer 0 + 1 + 2 + 3 (全部指标)
  耗时: ~250s (评估环节优化后), 全流程 ~305s
  调用: ev.run_all().generate_report()
  CLI:  python scripts/run_factor_research.py --factor X --mode full

指标层级依赖:
  Layer 0 (纯向量化): IC, RankIC, ICIR, IC Stats, Rank Autocorrelation
  Layer 1 (依赖 quantile_group): Quantile Group Labels, Turnover
  Layer 2 (依赖 portfolio): Portfolio Curves, Sharpe/Calmar/Sortino, Cost-Adjusted
  Layer 3 (依赖 demean+portfolio): Neutralized Curve
```

## Key Design Decisions
- FactorLib 与 deap_alpha 解耦: FactorLib 面向手动定义因子, deap_alpha 面向遗传编程自动挖掘
- FactorAnalysis 独立于 DEAP: 使用 pandas DataFrame 作为主要数据格式, 兼容 FactorLib 输出
- 重试机制放在 API 调用层 (market_api.py), 对上层透明
- 活跃交易对校验放在脚本入口层 (scripts/), 避免深度侵入核心模块
- **Tear Sheet 分层模式**: FactorEvaluator 按子分析拆分 (Returns/Information/Turnover/Neutralize), 支持选择性执行
- **多收益率标签**: 支持 close2close 和 open2open 两种前向收益, 因子分析可分别评估
- **零值感知分组**: zero_aware=True 时按正负拆分后各自做分位数分组, 避免零点附近信号失真
- **调仓频率衰减**: rebalance_freq 参数支持 1D/5D/10D 等多周期对比, 评估信号持续性
- **数据质量追踪**: max_loss 机制在因子-收益对齐后检查数据覆盖率, 低于阈值时告警
- **分块处理 (Chunking)**: FactorEvaluator 新增 chunk_size 参数, 按时间戳分块处理以控制内存峰值, 块间保持持仓连续性, chunk_size=None 时向后兼容
- **未来函数检测**: 自动化脚本检查 factor/forward_return 时间对齐、shift(-1) 使用位置, 输出 PASS/FAIL 报告
- **可视化模块**: 独立子包 FactorAnalysis/visualization/, 支持 IC 时间序列/分组收益/净值曲线/换手率图表 + 综合绩效表格 + Jinja2 HTML 报告组装
- **quantile_group 缓存复用**: FactorEvaluator 内缓存一次分组结果, 下游函数接受预计算 group_labels, 消除 run_all() 中 6-7 次冗余排序
- **portfolio 三函数合并**: calc_long_only/short_only/top_bottom_curve 合并为 calc_portfolio_curves, 单次 groupby.apply 同时输出三条日收益序列, 原函数保留为薄包装
- **IC/RankIC 向量化**: unstack 为 2D 矩阵后使用 numpy 行级 Pearson/Spearman 批量计算, 避免 groupby.apply 逐截面 Python 函数调用
- **rank_autocorr 向量化**: unstack 为 2D 矩阵后 numpy 批量计算相邻行相关性, 消除逐截面 xs+corr 纯 Python 循环
- **turnover unstack 去重**: unstack 一次后向量化计算所有组换手率, 替代循环内重复 unstack
- **neutralize 复用 portfolio**: 中性化对冲收益复用 calc_portfolio_curves, 减少 groupby.apply 冗余调用
- **数值一致性保障**: 所有优化通过 mock 数据逐元素对比 (diff < 1e-8) + 小批量真实数据端到端回归验证
- **split_into_chunks 一次性计算 (P0)**: run_all() 入口处一次性 split factor/returns/group_labels, chunk 列表以参数传入各 run_* 方法, 消除 ~1650 次冗余 isin 过滤, chunk_list=None 时向后兼容
- **portfolio numpy 向量化 (P1)**: _portfolio_curves_core 使用 unstack + numpy boolean mask 替代 groupby.apply, 单次 numpy 批量计算 long/short/hedge 日收益, 预期 7× 加速
- **quantile_group numpy 向量化 (P2)**: quantile_group 使用 unstack + numpy percentile + searchsorted 替代 groupby.apply, 含 zero_aware 模式支持, 预期 5.5× 加速
- **neutralize 内部操作合并 (P3)**: demean + re-rank 在单次 unstack 矩阵上完成, 避免两次独立 groupby 操作, 复用 P1 向量化 portfolio
- **两级管道 (Quick Screen / Full Analysis)**: run_quick() 仅计算 Layer 0 指标 (全部向量化), 零 groupby.apply 调用, 适用于因子筛选; run_all() 计算全部层级, 适用于深度分析

## Performance Optimization Flow (性能优化流程)

```
run_all() v2 优化执行路径:
0. split_into_chunks 一次性计算 → chunk 列表缓存, 传入各 run_* 方法 (P0)
1. run_metrics()         — 向量化 IC/RankIC (numpy 2D 矩阵批量计算)
2. run_grouping()        — quantile_group numpy 向量化 (P2), 结果缓存
3. run_curves()          — numpy 向量化 portfolio (P1) + 缓存 group_labels
4. run_turnover()        — 使用缓存 group_labels + unstack 一次向量化计算
5. run_neutralize()      — demean + re-rank 合并 (P3) + 复用 P1 向量化 portfolio

run_quick() 快速筛选路径:
1. run_metrics()         — 向量化 IC/RankIC/ICIR/IC Stats
2. run_rank_autocorr()   — 向量化 rank autocorrelation
→ 零 groupby.apply, 纯 numpy 计算
```

### 优化前后对比
| 指标 | 优化前 (v1) | v1 迭代后 | v2 迭代后 (目标) |
|------|-------------|-----------|-----------------|
| split_into_chunks | ~1650 次 isin | ~1650 次 isin | 1 次 (P0) |
| quantile_group 调用次数 | 6-7 次 | 1 次 | 0 次 (numpy 向量化, P2) |
| groupby.apply (portfolio) | 3 次 | 1 次 | 0 次 (numpy 向量化, P1) |
| neutralize groupby | 3 次独立操作 | 复用 portfolio | 合并为单次 unstack (P3) |
| 评估环节耗时 | ~990s | ~858s | ~250s |
| 全流程耗时 | ~1044s (17.4min) | ~912s | ~305s (~5min) |

### 向量化计算模式
```
IC/RankIC 向量化:
  factor.unstack()  → 2D DataFrame (timestamp × symbol)
  returns.unstack() → 2D DataFrame (timestamp × symbol)
  numpy 批量行级 Pearson/Spearman → IC Series

portfolio numpy 向量化 (P1):
  labels.unstack()  → 2D DataFrame (timestamp × symbol)
  returns.unstack() → 2D DataFrame (timestamp × symbol)
  np.where(labels_mat == g, returns_mat, np.nan).mean(axis=1) → 日收益

quantile_group numpy 向量化 (P2):
  factor.unstack() → 2D DataFrame (timestamp × symbol)
  numpy 按行计算分位数边界 → np.searchsorted 分组

neutralize 合并 (P3):
  factor/returns unstack → demean + re-rank 单次矩阵操作
  复用 P1 向量化 portfolio 计算对冲收益

rank_autocorr 向量化:
  ranks = factor.groupby(level=0).rank()
  ranks.unstack() → 2D DataFrame (timestamp × symbol)
  numpy 相邻行 Pearson 相关 → rank_autocorr Series

turnover 向量化:
  labels.unstack() → 2D DataFrame (timestamp × symbol)
  numpy 比较相邻行变化 → turnover DataFrame (所有组)
```
