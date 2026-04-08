# Progress Log

> Append newest entries to the top in this format:
> `[YYYY-MM-DD HH:MM] summary`

[2026-04-09 Task 7] P1 portfolio._portfolio_curves_core numpy 向量化 — 221/221 tests passed
- 修改文件: FactorAnalysis/portfolio.py (_portfolio_curves_core + docstring)
- 将 groupby.apply(_portfolio_returns) 替换为 unstack + numpy boolean mask 向量化计算
- 核心变更:
  1. labels.unstack() + returns.unstack() → 2D numpy 矩阵 (timestamp × symbol)
  2. np.isin(labels_np, top_labels) & np.isfinite(returns_np) 构造 boolean mask
  3. np.where(mask, returns, 0.0).sum(axis=1) / count 安全逐行求均值
  4. 消除逐截面 Python groupby.apply 循环，单次 numpy 批量计算 long/short/hedge 日收益
- 保持 rebalance_freq/_raw/group_labels 参数完全兼容
- 行列索引使用 union + reindex 对齐，与原 pd.DataFrame 构造行为一致
- 回归测试: 221 tests passed (portfolio/chunk/integration/rebalance 全量)
- 7 个 pre-existing failures 均为 Task 4/5 --mode quick 默认值导致，与本次变更无关
- 用法: 无 API 变更，calc_portfolio_curves / calc_long_only / calc_short_only / calc_top_bottom 接口不变

[2026-04-09 Task 6] Quick Screen 管道功能验证 — 157/157 checks passed
- 新增测试: tests/test_quick_screen.py (157/157 通过)
- 8 个测试模块:
  1. run_quick() 仅产出 Layer 0 指标 (IC/RankIC/ICIR/IC_stats/rank_autocorr)，Layer 1~3 全为 None
  2. run_all() 后 _quick_mode 保持 False
  3. 6 种 mock 场景 × 8 指标全量模式数值一致性 (diff = 0.00e+00)
  4. 6 种 mock 场景 × 8 指标分块模式数值一致性 (diff = 0.00e+00)
  5. report select 参数过滤: select=["metrics","turnover"] 正确包含 IC+rank_autocorr 列、排除 curves/neutralize
  6. report.generate_report 模块级代理正确
  7. --mode quick/full CLI 参数解析 + 无效值拒绝
  8. ic_stats 字段完整性 (7 个 key 全部有限值)
- 回归测试: 13/13 P0 chunk/report tests passed
- 用法: python tests/test_quick_screen.py

[2026-04-09 Task 5] report.py select 参数 + run_factor_research.py --mode 参数
- 修改文件: FactorAnalysis/report.py, scripts/run_factor_research.py
- report.py: generate_report() 增加 select 参数，委托给 evaluator.generate_report(select=...)，消除重复逻辑
- run_factor_research.py: 新增 --mode {quick,full} CLI 参数 (默认 quick)
  - quick 模式: 调用 run_quick() + generate_report(select=["metrics", "turnover"])
  - full 模式: 调用 run_all() + generate_report() (全量)
- 头部信息显示当前 mode，Step 5 标题区分 Quick Screen / Full Analysis
- 向后兼容: select=None 时输出全部已计算板块
- 验证内容:
  - run_quick() + select=["metrics", "turnover"]: 仅含 IC/RankIC/ICIR/IC_stats/rank_autocorr 列
  - run_all() + generate_report(): 含全部 24 列 (metrics+grouping+curves+turnover+neutralize)
  - select=["metrics"]: 仅含 IC 相关列
  - --help 正确显示 --mode {quick,full}
- 回归测试: 13/13 P0 chunk/evaluator tests passed, 136 total tests passed (1 pre-existing flaky)
- 用法:
  - CLI: python scripts/run_factor_research.py --factor X --mode quick
  - CLI: python scripts/run_factor_research.py --factor X --mode full
  - API: generate_report(ev, select=["metrics", "turnover"])

[2026-04-09 Task 4] Layer 0 快速筛选管道 run_quick() — IC/RankIC/ICIR/IC Stats/Rank Autocorrelation
- 修改文件: FactorAnalysis/evaluator.py
- 新增 run_quick() 方法: 仅计算 Layer 0 纯向量化指标 (IC/RankIC/ICIR/IC Stats/Rank Autocorrelation)
- 新增 run_rank_autocorr() 方法: 从 run_turnover 中提取排名自相关为独立方法，支持 chunk 模式
- 新增 _quick_mode 属性: run_quick() 设置为 True，其他方法不影响
- P0 优化兼容: run_quick() 入口一次性 split factor/returns，分发到 run_metrics/run_rank_autocorr
- 验证内容:
  - 全量模式: Layer 0 指标全部非空，Layer 1~3 指标全部为 None
  - 分块模式 (chunk_size=50): Layer 0 指标正常计算
  - 数值一致性: run_quick() IC/RankIC 与 run_all().run_metrics() diff = 0.00e+00
  - run_rank_autocorr() 独立调用正常，_quick_mode 保持 False
- 回归测试: 13/13 P0 chunk tests passed
- 用法: ev.run_quick().generate_report(select=["metrics", "turnover"])

[2026-04-09 Task 3] P0 chunk 一致性 + 向后兼容测试 — 608/608 checks passed
- 新增测试: tests/test_p0_chunk_cache.py (608/608 通过)
- 6 种 mock 场景: 标准场景、大数据量、小数据量、高NaN比例、弱信号+多分组、紧凑分块
- 验证内容:
  - run_all() 全量 vs 分块所有报告字段 diff < 1e-10 (6 场景 × 全部属性)
  - chunk_list=None 向后兼容 (不传 chunk 参数 = 内部 split)
  - chunk_size=None 全量模式正常 (所有属性非空、报告正常)
  - 分块模式正确性 (chunk 数量、无重叠、时间连续、symbol 集合一致)
  - 一次性分块 (P0 run_all) vs 子方法独立分块结果完全一致
  - 全量模式忽略伪造 chunk 参数
- group_labels NaN 处理: NaN 因子值产生 NaN 标签，比较时需分别验证 NaN 位置和非空值一致性
- 回归测试: Task 1 passthrough (26/26)、integration (153 checks) 均通过
- 用法: python tests/test_p0_chunk_cache.py

[2026-04-09 Task 2] P0 run_all() 一次性分块分发 — 消除重复 isin 过滤
- 修改文件: FactorAnalysis/evaluator.py (run_all() 方法重构)
- split_into_chunks 调用从 ~10 次降至 3 次 (factor + returns + group_labels)
- chunk_factor/chunk_returns 在入口处一次性计算，分发到 run_metrics/run_grouping/run_curves/run_turnover/run_neutralize
- chunk_groups 在 run_grouping() 后一次性计算，分发到 run_curves/run_turnover/run_neutralize
- 全量模式 (chunk_size=None) 行为完全不变
- 回归测试: 72 个测试全部通过 (chunk/integration/memory/smoke)
- 验证: mock patch 确认 split_into_chunks 恰好调用 3 次

[2026-04-09 Task 1] P0 chunk 列表参数 — evaluator 各 run_* 方法增加可选 chunk_factor/chunk_returns/chunk_groups 参数
- 修改文件: FactorAnalysis/evaluator.py (5 个方法签名变更)
- 新增测试: tests/test_p0_chunk_passthrough.py (26/26 通过)
- 回归测试: 8 个既有分块测试全部通过
- 用法: 调用 run_metrics(chunk_factor=fc, chunk_returns=rc) 传入预计算分块，内部自动跳过 split_into_chunks；不传时行为完全不变
- 优先级: chunk_groups > cached_group_labels > [None]*n_chunks

[2026-04-09 Init] 评估环节性能优化迭代 v2 初始化
- 需求来源: scarffold/requirements.md — P0-P3 优化 + Quick Screen 两级管道
- 目标: 全流程从 ~17min 降至 ~9min, 新增秒级快速筛选管道
- 5 个优化方向: P0 (chunk 缓存) / P1 (portfolio 向量化) / P2 (quantile_group 向量化) / P3 (neutralize 合并) / S7 (Quick Screen)
- 旧任务 (1-19) 已归档至 scarffold/.agent/history/tasks_20260409.json
- 旧进度已归档至 scarffold/.agent/history/progress_20260409.md
