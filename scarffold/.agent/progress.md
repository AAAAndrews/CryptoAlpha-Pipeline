# Progress Log

> Append newest entries to the top in this format:
> `[YYYY-MM-DD HH:MM] summary`

[2026-04-04 16:00] Task 2 完成 — FactorEvaluator group_labels 缓存机制
- 修改文件: FactorAnalysis/evaluator.py
- 新增属性: `self._cached_group_labels` (初始化为 None)
- 新增方法: `_set_group_cache(group_labels)` 设置缓存, `_clear_group_cache()` 清除缓存
- run_grouping() 全量模式和分块模式执行后均自动调用 `_set_group_cache`
- 缓存与 group_labels 为同一引用（非拷贝），下游可直接复用
- 验证测试: tests/test_perf_group_cache.py (12 checks passed)
  - 初始化缓存为 None / 全量+分块模式自动缓存 / 同一引用验证 / 6 种场景覆盖
  - set/clear 方法正确性 / chunk_size 独立性 / 分块 vs 全量一致性
- 用法: 下游 run_curves/run_turnover/run_neutralize 检查 `_cached_group_labels` 复用，跳过冗余 quantile_group 调用

[2026-04-04 15:30] Task 1 完成 — 共享性能测试 fixtures
- 新增文件: tests/mutual_components/conftest_perf.py
- make_synthetic_data(): 生成 MultiIndex (timestamp, symbol) 的可控 mock 因子+收益数据，支持 n_days/n_symbols/seed/nan_frac/corr 参数
- 6 种预定义场景 (SCENARIOS): basic / high_ic / neg_ic / with_nan / large(500×100) / small(10×5)
- iter_scenarios(): 遍历所有场景的生成器，用于参数化测试
- benchmark(): 计时上下文管理器，measure_time(): 函数计时辅助
- assert_scalar_close / assert_series_close / assert_frame_close: 数值一致性断言 (默认容差 1e-10)
- 验证测试: tests/test_perf_conftest.py (38 checks passed)
- 用法: 下游 test_perf_*.py 统一从 `from tests.mutual_components.conftest_perf import ...` 导入

[2026-04-04 Init] 评估环节性能优化迭代初始化
- 需求来源: scarffold/requirements.md — 评估环节性能优化 (Evaluation Performance Optimization)
- 目标: 消除 run_all() 中的计算冗余，将评估环节总耗时降低 50% 以上
- 6 个子需求: quantile_group 缓存 (4.1) / portfolio 合并 (4.2) / rank_autocorr 向量化 (4.3) / IC 向量化 (4.4) / turnover 去重 (4.5) / neutralize 复用 (4.6)
- 建议执行顺序: 4.1 → 4.2 → 4.4 → 4.3 → 4.6 → 4.5
- 硬约束: 所有优化数值差异 < 1e-8, 既有测试不回归
- 旧任务 (1-28) 已归档至 scarffold/.agent/history/


