# Progress Log

> Append newest entries to the top in this format:
> `[YYYY-MM-DD HH:MM] summary`

[2026-04-04 Task 10] feat: 分块净值曲线持仓连续性验证测试 (128 checks passed)
- 新增 `tests/test_chunked_curve_continuity.py`，验证分块净值曲线的连续性、cumprod 衔接点无跳变、rebalance_freq 与分块交互
- 10 个场景：隐含日收益率一致性（long/short/hedge）、21 个块边界比值零跳变、7 种素数 chunk_size 连续性、5 种 rebalance_freq 与分块交互（数值 diff < 1e-14）、4 种不对齐 chunk_size 自动对齐验证、chunk_size=1 极端连续性、5 种子稳定性、NaN 因子连续性、4 种 cost_rate × 3 种 chunk_size 成本曲线连续性、8 种 rebalance_freq+分组参数组合
- 关键结论：分块曲线隐含日收益率与全量完全一致（diff ≤ 2.22e-16 = machine epsilon），cumprod 衔接点比值 diff=0.00e+00
- 用法：`python tests/test_chunked_curve_continuity.py` → 128 checks 全部 PASS

[2026-04-04 Task 9] feat: 分块 IC/IR 数值一致性验证测试 (718 checks passed)
- 新增 `tests/test_chunked_ic_consistency.py`，验证分块 vs 全量 IC/RankIC/ICIR/IC_stats 数值一致性（差异 < 1e-8）
- 覆盖 10 个场景：多种子稳定性（10 seeds）、多 chunk_size（15 种）、多数据规模（6 种）、含 NaN 数据（4 种比例）、NaN+种子组合、极端信号强度、chunk_size>n_dates 退化、极少量时间截面、IC 序列长度一致性、索引顺序一致性
- 所有 diff 均为 0.00e+00（IC 序列在各时间截面独立，分块拼接与全量计算完全一致）
- IC_stats 全字段验证：IC_mean/IC_std/ICIR/t_stat/p_value/IC_skew/IC_kurtosis
- 用法：`python tests/test_chunked_ic_consistency.py` → 718 checks 全部 PASS

[2026-04-04 Task 8] feat: 内存监控日志集成 ChunkMemoryTracker (8 tests passed)

[2026-04-04 Task 8] feat: 内存监控日志集成 ChunkMemoryTracker (8 tests passed)
- `chunking.py` 新增 `ChunkMemoryTracker` 上下文管理器：使用 `tracemalloc` 追踪每块 Python 堆内存峰值，可选 `psutil` 获取进程 RSS
- 日志输出格式：`[chunk i/N] description | peak_alloc=X.XX MB, RSS=X.XX MB`，写入 `FactorAnalysis.chunking` logger
- `__enter__` 自动启动 tracemalloc 并重置峰值，`__exit__` 获取峰值并记录日志，异常时仍正常输出
- `evaluator.py` 五个分块方法（run_metrics/run_grouping/run_curves/run_turnover/run_neutralize）全部集成内存监控
- 分块循环从列表推导式改为显式 for 循环 + `ChunkMemoryTracker`，每块迭代输出一条内存日志
- psutil 为可选依赖，不可用时仅输出 tracemalloc 峰值，不报错
- 用法：`with ChunkMemoryTracker(0, 5, description="run_metrics"): process_chunk(data)` → logger.info 输出内存信息
- 调优 chunk_size：观察各块 peak_alloc 日志，选择使峰值内存保持在目标范围内的 chunk_size 值

[2026-04-04 Task 7] feat: run_neutralize() 分块计算 (39 checks passed)
- `run_neutralize()` 新增分块模式：当 `chunk_size` 已设置时，按时间戳分块逐块执行中性化处理
- `calc_neutralized_curve` 新增 `_raw=False` 内部参数，`_raw=True` 时不覆写起始值为 1.0（用于分块合并）
- 使用 `_merge_raw_curves` 缩放拼接各块 raw 曲线后统一覆写起始值为 1.0，与全量计算数值一致（diff < 1e-14）
- 当 `groups` 参数为 pd.Series 时，同步分块 groups 数据以保持截面完整性
- 覆盖 demeaned/group_adjust 组合、5 种 chunk_size、多种子、含 NaN、小数据集、chunk_size=1、不同 n_groups、自定义 groups、向后兼容等场景
- 用法：`ev = FactorEvaluator(factor, returns, chunk_size=30); ev.run_neutralize()` → `ev.neutralized_curve` 与全量一致

[2026-04-04 Task 6] feat: run_turnover() 分块计算 (283 checks passed)
- `run_turnover()` 新增分块模式：当 `chunk_size` 已设置时，按时间戳分块逐段计算换手率和排名自相关
- 分块内换手率/排名自相关与全量计算完全一致（diff = 0），跨块边界首行标记为 NaN
- 使用 `split_into_chunks` 分块 + `merge_chunk_results(..., "turnover"/"rank_autocorr")` 汇总拼接
- `_merge_turnover`: 拼接换手率 DataFrame，后续块首行设为 NaN（跨块无前序截面）
- `_merge_rank_autocorr`: 拼接排名自相关 Series，后续块首值设为 NaN（跨块排名不可比）
- 覆盖 5 种 chunk_size、多种子、含 NaN、小数据集、chunk_size=1、不同 n_groups、向后兼容等场景
- 用法：`ev = FactorEvaluator(factor, returns, chunk_size=30); ev.run_turnover()` → `ev.turnover`/`ev.rank_autocorr` 与全量一致（边界除外为 NaN）

[2026-04-04 Task 5] feat: run_curves() 分块计算 (68 checks passed)
- `run_curves()` 新增分块模式：当 `chunk_size` 已设置时，按时间戳分块逐块构建净值曲线
- 关键设计：使用 `_raw=True` 参数获取未覆写起始值的 raw cumprod 曲线，通过 `_merge_raw_curves` 缩放拼接后统一覆写起始值为 1.0
- 与 `_merge_curves` 不同，`_merge_raw_curves` 不跳过后续块首元素，保留所有时间戳，确保与全量计算数值一致（diff < 1e-10）
- 成本扣除（hedge_curve_after_cost）和绩效比率（Sharpe/Calmar/Sortino）在合并后的曲线上计算，与全量模式一致
- `portfolio.py` 三函数（calc_long_only_curve / calc_short_only_curve / calc_top_bottom_curve）新增 `_raw=False` 内部参数，向后兼容
- 覆盖 5 种 chunk_size、多种子、含 NaN、小数据集、chunk_size=1、不同 top_k/bottom_k/n_groups/cost_rate、向后兼容等场景
- 用法：`ev = FactorEvaluator(factor, returns, chunk_size=30); ev.run_curves()` → long/short/hedge 曲线与全量一致

[2026-04-04 Task 4] feat: run_grouping() 分块计算 (37 tests passed)
- `run_grouping()` 新增分块模式：当 `chunk_size` 已设置时，按时间戳分块逐块执行 `quantile_group`
- 分组标签在各时间截面上独立（截面内分位数计算），分块拼接结果与全量计算完全一致（diff = 0）
- 使用 `split_into_chunks` 分块 + `merge_chunk_results(..., "ic")` 汇总拼接
- 截面完整性：每个分块内所有 symbol 在每个时间戳均存在，分组标签范围正确
- 覆盖 5 种 chunk_size、多种子、含 NaN、小数据集、chunk_size=1、不同 n_groups、向后兼容等场景
- 用法：`ev = FactorEvaluator(factor, returns, chunk_size=30); ev.run_grouping()` → `ev.group_labels` 与全量一致

[2026-04-04 Task 3] feat: run_metrics() IC/IR 分块计算 (61 tests passed)
- `run_metrics()` 新增分块模式：当 `chunk_size` 已设置时，按时间戳分块逐块计算 IC/RankIC 序列
- 使用 `split_into_chunks` 分块 + `merge_chunk_results(..., "ic")` 汇总拼接
- 新增模块级辅助函数 `_icir_from_series()` 和 `_ic_stats_from_series()`，从合并后的 IC 序列计算 ICIR 和统计显著性
- IC 值在各时间截面上独立，分块拼接结果与全量计算完全一致（diff < 1e-15）
- 覆盖 5 种 chunk_size、多种子、含 NaN、小数据集、向后兼容等场景
- 用法：`ev = FactorEvaluator(factor, returns, chunk_size=30); ev.run_metrics()` → IC/RankIC/ICIR/ic_stats 与全量一致

[2026-04-04 Task 2] feat: FactorEvaluator.__init__ 新增 chunk_size 参数与 _validate_chunk_size 校验 (29 tests passed)
- `__init__` 新增 `chunk_size: int | None = None` 参数，默认 None（全量模式），正整数启用分块模式
- 新增 `_validate_chunk_size()` 静态方法：校验 None/正整数/浮点整数(自动转 int)，拒绝 0、负数、非整数浮点、bool、字符串等
- chunk_size 属性可在实例化后访问：`ev.chunk_size`
- 向后兼容：不传 chunk_size 时行为与改造前完全一致（72 项既有测试全部通过）
- 用法示例：`ev = FactorEvaluator(factor, returns, chunk_size=100)` → `ev.chunk_size == 100`

[2026-04-04 Task 1] feat: 创建 FactorAnalysis/chunking.py 分块工具函数 (42 tests passed)
- 新增 `split_into_chunks(data, chunk_size, rebalance_freq=1)` 按时间戳分块，支持 rebalance 频率边界对齐
- 新增 `merge_chunk_results(chunk_results, metric_type)` 汇总聚合，支持 5 种指标类型：ic/ic_stats/curve/turnover/rank_autocorr
- IC 合并：直接拼接（各时间截面独立）；净值曲线合并：缩放拼接保持 cumprod 连续性
- 换手率/排名自相关合并：拼接，跨块边界设为 NaN；IC 统计量合并：按样本量加权平均（pooled variance）
- 用法示例：`chunks = split_into_chunks(data, chunk_size=100, rebalance_freq=5)` → `merged = merge_chunk_results(chunk_ics, "ic")`

