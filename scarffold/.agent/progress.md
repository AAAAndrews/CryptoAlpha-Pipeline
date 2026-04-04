# Progress Log

> Append newest entries to the top in this format:
> `[YYYY-MM-DD HH:MM] summary`

[2026-04-05 01:30] Task 13 完成 — rank_autocorr 向量化数值一致性测试
- 测试文件: tests/test_perf_rank_autocorr.py (已有，Task 12 同步创建)
- 验证内容: 44 checks passed
  - 6 种 mock 场景 × rank_autocorr Series diff < 1e-10 (6 checks)
  - 极端信号 autocorr=1.0: 向量化与参考实现一致 + 均值验证 (2 checks)
  - 极端信号 autocorr=-1.0: 振荡因子排名反转一致 + 均值验证 (2 checks)
  - NaN 数据处理: 高比例 NaN + 全 NaN 输入 (3 checks)
  - 边界情况: 最小数据/单期/单资产 (3 checks)
  - lag 参数: lag=2 和 lag=5 一致性 + 前期 NaN (4 checks)
  - 返回类型和形状: 6 场景 × 3 属性 (18 checks)
  - 既有回归: 稳定/振荡因子/值域/参数校验 (6 checks)
- 参考实现: 逐截面 xs+corr 的 calc_rank_autocorr_reference (Pearson)
- 回归: 0 项回归，所有既有测试保持通过
- 用法: 此测试验证 Task 12 (calc_rank_autocorr 向量化) 的数值一致性，是后续 E2E 基准测试 (Task 18) 的前置依赖

[2026-04-05 01:00] Task 12 完成 — 向量化 calc_rank_autocorr (turnover.py)
- 修改文件: FactorAnalysis/turnover.py (已有向量化实现，本次完成验证)
- 核心实现: calc_rank_autocorr 使用 unstack 2D 矩阵 + numpy 批量行级 Pearson 相关，替代逐截面 xs+corr 纯 Python 循环
- 实现方式: factor.groupby(level=0).rank() 横截面排名 → unstack 为 (timestamp × symbol) 矩阵 → shift(lag) 构建滞后矩阵 → numpy Pearson 公式 r=(n*Σxy-Σx*Σy)/sqrt((n*Σx²-(Σx)²)(n*Σy²-(Σy)²)) 批量计算
- NaN 处理: valid 掩码过滤非有限值，n_valid<2 或分母为零时返回 NaN，前 lag 期强制 NaN
- 公共 API 完全保留: 函数签名 (factor, lag=1)、返回类型 (pd.Series indexed by timestamp) 均不变
- 验证测试: tests/test_perf_rank_autocorr.py (44 checks passed)
  - 6 种 mock 场景 × rank_autocorr Series diff < 1e-10 (6 checks)
  - 极端信号 autocorr=1.0: 向量化与参考实现一致 + 均值验证 (2 checks)
  - 极端信号 autocorr=-1.0: 振荡因子排名反转一致 + 均值验证 (2 checks)
  - NaN 数据处理: 高比例 NaN + 全 NaN 输入 (3 checks)
  - 边界情况: 最小数据/单期/单资产 (3 checks)
  - lag 参数: lag=2 和 lag=5 一致性 + 前期 NaN (4 checks)
  - 返回类型和形状: 6 场景 × 3 属性 (18 checks)
  - 既有回归: 稳定/振荡因子/值域/参数校验 (6 checks)
- 回归: test_task12_turnover (23 checks) 全部通过，无回归
- 用法: 下游 Task 13 将用统一测试验证 rank_autocorr 向量化数值一致性

[2026-04-05 00:30] Task 11 完成 — IC/RankIC 向量化整体数值一致性统一测试
- 新增文件: tests/test_perf_ic_rankic_unified.py
- 验证内容:
  1. 6 种 mock 场景 × IC/RankIC/ICIR/IC_stats 全字段 diff < 1e-10 (24 checks: 6 场景 × 4 指标)
  2. 极端信号 IC=1.0: IC/RankIC Series 一致性 + ICIR 方向验证 + IC_stats 4 稳定字段 (4 checks)
  3. 极端信号 IC=-1.0: IC/RankIC Series 一致性 + ICIR 方向验证 (3 checks)
  4. NaN 数据处理: 高比例 NaN × IC/RankIC/ICIR/IC_stats + 全 NaN 输入 IC/RankIC/ICIR (7 checks)
  5. 多种子稳定性: 5 个种子 × IC/RankIC/ICIR/IC_stats 全字段 (20 checks)
  6. IC_stats 数据不足: 全 NaN 返回 + 警告触发 (2 checks)
- 总计 60 checks passed
- 技术细节: 极端信号 (IC=±1.0) 时 IC_std≈0 导致 ICIR/t_stat/p_value 数值爆炸，此类场景仅验证稳定字段 (IC_mean/IC_std/IC_skew/IC_kurtosis)；其余场景全字段 1e-10 容差
- 参考实现: 逐截面 groupby.apply 的 calc_ic_reference (Pearson) 和 calc_rank_ic_reference (Spearman)
- 回归: test_metrics_ic (8 checks) 全部通过，无回归
- 用法: 此测试综合验证 Task 9 (calc_ic 向量化) + Task 10 (calc_rank_ic 向量化) 的整体正确性，是后续 E2E 基准测试 (Task 18) 的前置依赖

[2026-04-04 23:30] Task 10 完成 — 向量化 calc_rank_ic (metrics.py)
- 修改文件: FactorAnalysis/metrics.py
- 核心变更: calc_rank_ic 从 groupby.apply 逐截面 _spearman 内部函数改为 unstack 2D 矩阵 + pandas 行级排名 + numpy 向量化 Pearson 批量计算
- 实现方式: factor/returns unstack 为 (timestamp × symbol) 矩阵，先用 valid 掩码屏蔽无效值，再用 pandas.DataFrame.rank(axis=1, method='average') 按行排名（等价于 scipy.stats.rankdata 平均秩法），最后复用与 calc_ic 相同的 numpy Pearson 公式
- NaN 处理: valid 掩码过滤非有限值，先 where(valid) 再 rank 确保排名范围与参考实现一致（仅对 factor 和 returns 同时有效的样本排名），n_valid<2 或分母为零时返回 NaN
- 公共 API 完全保留: 函数签名、返回类型 (pd.Series indexed by timestamp) 均不变
- 验证测试: tests/test_perf_rank_ic_vectorized.py (37 checks passed)
  - 6 种 mock 场景 × Rank IC Series diff < 1e-10 (6 checks)
  - 极端信号 Rank IC=1.0/-1.0: 向量化与参考实现一致 + 均值验证 (2 checks)
  - NaN 数据处理: 高比例 NaN + 全 NaN 输入 (2 checks)
  - 边界情况: 单资产/常数因子/两资产/含大量平值(ties) (4 checks)
  - 返回类型和形状: 6 场景 × 3 属性 (18 checks)
  - 既有回归: test_calc_rank_ic 的 5 项核心断言 (5 checks)
- 回归: test_metrics_ic (31 checks) 全部通过，无回归
- 用法: 下游 calc_icir 和 calc_ic_stats 均调用 calc_ic（非 calc_rank_ic），Rank IC 独立使用；Task 11 将用统一测试验证 IC/RankIC 向量化整体正确性

[2026-04-04 23:00] Task 9 完成 — 向量化 calc_ic (metrics.py)
- 修改文件: FactorAnalysis/metrics.py
- 核心变更: calc_ic 从 groupby.apply 逐截面 Python 循环改为 unstack 2D 矩阵 + numpy 向量化 Pearson 批量计算
- 实现方式: factor/returns unstack 为 (timestamp × symbol) 矩阵，使用 Pearson 公式 r=(n*Σxy-Σx*Σy)/sqrt((n*Σx²-(Σx)²)(n*Σy²-(Σy)²)) 按行批量计算
- NaN 处理: valid 掩码过滤非有限值，n_valid<2 或分母为零时返回 NaN
- 公共 API 完全保留: 函数签名、返回类型 (pd.Series indexed by timestamp) 均不变
- 验证测试: tests/test_perf_ic_vectorized.py (36 checks passed)
  - 6 种 mock 场景 × IC Series diff < 1e-10 (6 checks)
  - 极端信号 IC=1.0/-1.0: 向量化与参考实现一致 + 均值验证 (2 checks)
  - NaN 数据处理: 高比例 NaN + 全 NaN 输入 (2 checks)
  - 边界情况: 单资产/常数因子/两资产 (3 checks)
  - 返回类型和形状: 6 场景 × 3 属性 (18 checks)
  - 既有回归: test_metrics_ic 的 5 项核心断言 (5 checks)
- 回归: test_metrics_ic (31 checks), test_task08, test_task09, test_chunking 全部通过，无回归
- 用法: 下游 calc_icir 和 calc_ic_stats 均调用 calc_ic，自动受益于向量化加速；Task 10 将用同样方式向量化 calc_rank_ic

[2026-04-04 22:00] Task 8 完成 — portfolio 合并数值一致性 + 分块兼容测试
- 新增文件: tests/test_perf_portfolio_merge.py
- 验证内容:
  1. 三薄包装 (calc_long_only/short_only/top_bottom_curve) 与 calc_portfolio_curves 对应元素一致 (6 场景 × 3 曲线 = 18 checks)
  2. calc_portfolio_curves 直接调用正确性: 起始值 1.0、曲线长度匹配、终值非平凡 (36 checks)
  3. evaluator.run_curves() 与直接调用 calc_portfolio_curves 一致 (6 场景 × 3 曲线 = 18 checks)
  4. 不同 top_k/bottom_k 组合: (1,1)/(2,1)/(1,2)/(2,2) × 3 曲线 = 12 checks
  5. _raw 模式一致性: 6 场景 × 3 曲线 = 18 checks
  6. rebalance_freq 非默认值: rebal=3,5 × 3 曲线 = 6 checks
  7. chunk_size 模式两次独立运行一致性 (5 场景 × 3 曲线 = 15 checks)
  8. chunk_size vs 全量模式: 比较日收益率避免 cumprod 浮点累积 (5 场景 × 3 曲线 + 起始值 = 17 checks)
  9. evaluator group_labels 缓存传递 run_curves 一致 (6 场景 × 3 曲线 = 18 checks)
  10. chunk_size + 缓存组合一致性 (5 场景 × 3 曲线 = 15 checks)
- 总计 10 项测试全部通过
- 技术细节: chunk vs full 模式比较日收益率 (pct_change) 而非累积净值曲线，因为 cumprod 在长序列上浮点累积误差会被放大到绝对值 > 1e-10（但相对误差在 float64 epsilon 内 ~5.8e-16）
- 回归: Task 6 的 11 项 calc_portfolio_curves 测试全部通过，无回归
- 用法: 此测试验证 Task 7 的薄包装重构整体正确性，是后续 E2E 基准测试 (Task 18) 的前置依赖

[2026-04-04 21:00] Task 7 完成 — 重构三独立函数为薄包装 + evaluator 单次调用
- 修改文件: FactorAnalysis/portfolio.py, FactorAnalysis/evaluator.py
- 核心重构: 提取 `_portfolio_curves_core()` 内部函数（无参数校验），承载全部计算逻辑
- `calc_long_only_curve` → 薄包装: 校验参数后调用 `_portfolio_curves_core(top_k=X, bottom_k=0)`，仅返回 long 曲线
- `calc_short_only_curve` → 薄包装: 校验参数后调用 `_portfolio_curves_core(top_k=0, bottom_k=X)`，仅返回 short 曲线
- `calc_top_bottom_curve` → 薄包装: 校验参数后调用 `_portfolio_curves_core(top_k=X, bottom_k=Y)`，仅返回 hedge 曲线
- `calc_portfolio_curves` → 校验层 + 委托 `_portfolio_curves_core`（行为不变，Task 6 测试全部通过）
- evaluator.py: `run_curves()` 全量模式改为单次 `calc_portfolio_curves()` 调用替代三次独立调用；分块模式同理
- evaluator.py import: 移除三独立函数导入，改为 `from .portfolio import calc_portfolio_curves`
- 公共 API 完全保留: 三函数签名、返回类型、参数校验行为均不变
- 验证: 34 项关键测试全部通过（含 Task 6 的 11 项 calc_portfolio_curves 测试 + 缓存/分块集成测试）
- 回归: 0 项回归，所有既有测试保持通过
- 用法: 下游代码无需任何修改，三函数仍可独立调用；evaluator.run_curves() 内部自动走统一路径

[2026-04-04 20:00] Task 6 完成 — 创建统一 calc_portfolio_curves 函数
- 修改文件: FactorAnalysis/portfolio.py, FactorAnalysis/__init__.py
- 新增函数: `calc_portfolio_curves(factor, returns, n_groups, top_k, bottom_k, rebalance_freq, _raw, group_labels)` → tuple[long, short, hedge]
- 核心实现: 单次 groupby.apply 的 `_portfolio_returns` 内部函数同时计算 long/short/hedge 三种日收益，一次构建 DataFrame 和标签，消除三次独立调用的冗余
- 支持全部参数: rebalance_freq 调仓频率、_raw 原始模式、group_labels 预计算标签缓存复用
- 完整参数校验: rebalance_freq 类型/范围、top_k/bottom_k 范围、top_k+bottom_k ≤ n_groups
- __init__.py 新增 `calc_portfolio_curves` 导出
- 验证测试: tests/test_perf_calc_portfolio_curves.py (11 checks passed)
  - 6 场景默认参数 × 3 曲线 diff < 1e-10 (18 checks)
  - rebalance_freq > 1 一致性 (9 checks)
  - 预计算 group_labels 一致性 (18 checks)
  - _raw=True 模式一致性 (3 checks)
  - 自定义 top_k/bottom_k 一致性 (9 checks)
  - _raw 起始值行为 (1 check)
  - 参数校验 (5 checks: TypeError/ValueError)
- 回归测试: 76 项既有测试全部通过，无回归
- 用法: `from FactorAnalysis.portfolio import calc_portfolio_curves; long, short, hedge = calc_portfolio_curves(factor, returns)` — 后续 Task 7 将重构三独立函数为薄包装

[2026-04-04 19:00] Task 5 完成 — quantile_group 缓存数值一致性 + 分块兼容
- 新增文件: tests/test_perf_quantile_cache.py
- 验证内容:
  1. quantile_group 调用次数: 全量模式 2 次 (1 主分组 + 1 neutralize ranking)，优化前 6-7 次
  2. quantile_group 调用次数: 分块模式 2×n_chunks 次，每块主分组仅 1 次
  3. 6 场景 × 12 标量报告指标: 两次独立 run_all() diff < 1e-10 (72 checks)
  4. 6 场景 × 9 核心数据对象: ic/rank_ic/long/short/hedge/hedge_cost/turnover/rank_autocorr/neutralized (54 checks)
  5. 6 场景 × 5 单独方法: run_curves/run_turnover/run_neutralize 有缓存 vs 无缓存一致 (30 checks)
  6. 5 场景 × 6 分块模式: chunk_size=50 结果一致性 (30 checks)
- 总计 197 checks passed
- 技术细节: 使用 unittest.mock.patch 同步 patch 4 个模块的 quantile_group 引用 (evaluator/portfolio/turnover/neutralize) 进行调用计数
- 用法: 此测试验证 tasks 1-4 的缓存优化整体正确性，是后续优化任务 (IC 向量化/portfolio 合并等) 的回归基线

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


