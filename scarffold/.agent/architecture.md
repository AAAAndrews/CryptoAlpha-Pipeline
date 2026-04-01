# Architecture

## Tech Stack
- **Language**: Python 3.10+
- **Data Storage**: Feather format (pyarrow), stored on local filesystem
- **Data Source**: Binance REST API + Binance S3 bulk download
- **Core Libraries**: pandas, numpy, scipy, numba, pyarrow, requests, rich, tqdm
- **Factor Framework**: DEAP (genetic programming for factor discovery)
- **Performance Evaluation**: Custom implementation (alphalens-style)

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
│   └── run_factor_research.py       # [新增] 端到端投研编排脚本
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
│   ├── evaluator.py                 # [重构] FactorEvaluator → Tear Sheet 分层编排
│   └── report.py                    # 绩效报告汇总输出
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
