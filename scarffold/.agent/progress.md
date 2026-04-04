# Progress Log

> Append newest entries to the top in this format:
> `[YYYY-MM-DD HH:MM] summary`

[2026-04-04 18:30] Task 4 完成 — group_labels 缓存接入 run_curves/run_turnover/run_neutralize
- 修改文件: FactorAnalysis/evaluator.py, FactorAnalysis/turnover.py
- evaluator.py 变更:
  - run_curves(): 检查 `_cached_group_labels`，存在时传递给 calc_long_only_curve/calc_short_only_curve/calc_top_bottom_curve 的 `group_labels` 参数
  - run_turnover(): 检查 `_cached_group_labels`，存在时传递给 calc_turnover 的 `group_labels` 参数
  - run_neutralize(): 当 `groups == self.n_groups`（默认值）且缓存存在时，使用缓存替代 int 参数；groups 不同时跳过缓存
  - 三个方法均同步适配 chunk_size 分块模式：缓存标签通过 split_into_chunks 分块后逐块传递
- turnover.py 变更:
  - 新增 `from __future__ import annotations` (Python 3.9 兼容)
  - calc_turnover 新增 `group_labels: pd.Series | None = None` 参数，传入时跳过内部 quantile_group 调用
- 验证测试: tests/test_perf_cache_pass_through.py (9 checks passed)
  - 向后兼容: 缓存为 None 时行为不变
  - 全量模式: 有/无缓存 run_curves 三条曲线 diff < 1e-10
  - 全量模式: 有/无缓存 run_turnover DataFrame diff < 1e-10
  - 全量模式: 有/无缓存 run_neutralize 曲线 diff < 1e-10
  - neutralize groups!=n_groups 时正确跳过缓存
  - 分块模式: 三方法有/无缓存 chunk_size=50 diff < 1e-10
  - run_all() 全流程有/无缓存 6 种场景 × 6 指标 diff < 1e-10
- 用法: run_all() 中 run_grouping() 后自动缓存，下游 run_curves/run_turnover/run_neutralize 均复用缓存，消除冗余 quantile_group 调用

> Append newest entries to the top in this format:
> `[YYYY-MM-DD HH:MM] summary`

[2026-04-04 17:30] Task 3 完成 — portfolio 预计算 group_labels 支持
- 修改文件: FactorAnalysis/portfolio.py
- 新增 `from __future__ import annotations` (兼容 Python 3.9 的 `X | None` 语法)
- `_calc_labels_with_rebalance` 新增 `group_labels: pd.Series | None = None` 可选参数
  - rebalance_freq=1 时: 传入 group_labels 直接返回，跳过 quantile_group
  - rebalance_freq>1 时: 传入 group_labels 仅在调仓日取值 + ffill，跳过 quantile_group
  - 不传时保持原有行为（向后兼容）
- `calc_long_only_curve` / `calc_short_only_curve` / `calc_top_bottom_curve` 均新增 `group_labels` 参数并透传
- 验证测试: tests/test_perf_portfolio_group_labels.py (11 checks passed)
  - 向后兼容: 不传 group_labels 行为不变
  - 预计算标签: rebalance_freq=1 和 >1 均与内部计算结果一致
  - 三函数: long/short/hedge 传入 group_labels 结果一致
  - 6 种 mock 场景全覆盖
- 用法: 下游 run_curves() 可传入 evaluator._cached_group_labels 跳过冗余 quantile_group 调用

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


