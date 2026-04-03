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
│   ├── run_factor_research.py       # [新增] 端到端投研编排脚本
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
│   ├── evaluator.py                 # [重构] FactorEvaluator → Tear Sheet 分层编排
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
