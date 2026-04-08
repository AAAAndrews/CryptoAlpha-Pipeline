# Progress Log

> Append newest entries to the top in this format:
> `[YYYY-MM-DD HH:MM] summary`

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
