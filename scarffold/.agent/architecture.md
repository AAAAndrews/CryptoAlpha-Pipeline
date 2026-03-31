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
│   └── pipeline.py                  # [新增] 统一管道: bulk + cleanup
│
├── Cross_Section_Factor/            # 因子挖掘
│   ├── datapreprocess.py            # 数据预处理, BaseDataLoader, MultiAssetDataHandler
│   ├── crypto_gp_1h.py              # 遗传编程因子挖掘入口
│   └── deap_alpha/                  # DEAP 因子框架
│       ├── fitness_funcs/           # 适应度评估
│       │   ├── core.py              # 评估核心, base_evaluate
│       │   └── metrics/
│       │       ├── factor.py        # IC, RankIC, ICIR, turnover
│       │       ├── performance.py   # Sharpe, Calmar, Sortino, top_k_returns, monotonicity
│       │       └── utils.py         # high_minus_low, preprocess_data_jit
│       └── ops/                     # 算子库
│           ├── arithmetic_ops.py
│           ├── cross_section_ops.py
│           ├── timeseries_ops.py
│           └── worldquant_ops.py
│
├── FactorLib/                       # [新增] 独立因子库模块
│   ├── __init__.py
│   ├── base.py                      # BaseFactor 抽象基类
│   ├── alpha_momentum.py            # Alpha1 动量因子
│   ├── alpha_volatility.py          # Alpha2 波动率因子
│   └── registry.py                  # 因子注册表
│
├── FactorAnalysis/                  # [新增] 因子绩效检验模块
│   ├── __init__.py
│   ├── evaluator.py                 # FactorEvaluator 主类
│   ├── metrics.py                   # IC/RankIC/ICIR 重新实现
│   ├── grouping.py                  # 分组分析, 可调分组数
│   ├── portfolio.py                 # 净值曲线: 纯多/纯空/top多-top空
│   ├── cost.py                      # 交易成本向量化扣除
│   └── report.py                    # 绩效报告汇总输出
│
└── scarffold/.agent/                # Agent 脚手架
    ├── architecture.md
    ├── progress.txt
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
   - 带重试机制 (exponential backoff)
   - 失败记录到 errors/ 目录
       ↓
5. 清理下架交易对假数据 (cleanup_fake_data.py → truncate fake OHLC)
       ↓
6. 统一管道: 步骤 3 → 5 一键执行 (pipeline.py)
```

### 因子分析流程 (Factor Analysis Flow)
```
1. 数据加载 (CryptoDB_feather reader → load_multi_klines → DataFrame)
       ↓
2. 数据预处理 (MultiAssetDataHandler → 3D array + returns matrix)
       ↓
3. 因子计算 (FactorLib.BaseFactor.calculate(data) → factor_values series)
       ↓
4. 绩效检验 (FactorAnalysis.FactorEvaluator)
   ├── IC / RankIC / ICIR
   ├── 分组收益分析 (可调 quantile 数)
   ├── 净值曲线 (纯多 / 纯空 / top多-bottom空)
   ├── 交易成本扣除 (向量化按比例)
   └── Sharpe / Calmar / Sortino
       ↓
5. 报告输出 (report.py → summary DataFrame + metrics)
```

## Key Design Decisions
- FactorLib 与 deap_alpha 解耦: FactorLib 面向手动定义因子, deap_alpha 面向遗传编程自动挖掘
- FactorAnalysis 独立于 DEAP: 使用 pandas DataFrame 作为主要数据格式, 兼容 FactorLib 输出
- 重试机制放在 API 调用层 (market_api.py), 对上层透明
- 活跃交易对校验放在脚本入口层 (scripts/), 避免深度侵入核心模块
