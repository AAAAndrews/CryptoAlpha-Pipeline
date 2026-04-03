# Progress Log

> Append newest entries to the top in this format:
> `[YYYY-MM-DD HH:MM] summary`

[2026-04-04 Task 16] feat: 未来函数检测验证测试 (43 checks passed)
- 新增 `tests/test_future_leak_detection.py`，8 个测试类 43 项检测全面覆盖未来函数检测脚本
- 静态扫描验证 (3 tests)：FactorLib 无 shift(-N)、shift(-N) 仅在允许文件中、KlineLoader 无 shift
- 现有因子 PASS 验证 (6 tests)：3 因子 × 2 检测（独立性 + 对齐），全部 PASS
- 泄露因子 FAIL 验证 (3 tests)：_LeakyZScoreFactor（全样本 z-score）和 _LeakyFutureMeanFactor（未来均值）均被正确检出 FAIL
- 边界情况 (6 tests)：空数据 graceful 处理、单交易对正常工作、全 NaN 因子对齐后为空、2 日期最小数据量、5 日期对齐检测
- DetectionReport 结构验证 (4 tests)：all_passed/n_pass/n_fail 属性、空报告、CheckResult 默认值
- FutureLeakDetector 集成 (7 tests)：Markdown 报告生成（全 PASS/含 FAIL/表格格式）、mock 数据完整流程、泄露因子检出、空数据处理、全 NaN 数据、单交易对
- 多种子稳定性 (8 tests)：5 种子 × 现有因子 PASS + 3 种子 × 泄露因子 FAIL
- CheckResult 完整性 (3 tests)：静态结果 name/status、动态结果 details、对齐检测项名称
- 关键设计：泄露因子使用全样本统计量（z-score）和未来均值，截断数据后重叠部分因子值差异 > 1e-12
- 用法：`python -m pytest tests/test_future_leak_detection.py -v` → 43 checks 全部 PASS


[2026-04-04 Task 15] feat: 未来函数自动化检测脚本 check_future_leak.py (15 checks passed)
- 新增 `scripts/check_future_leak.py`，包含静态代码扫描 + 动态数据验证两层检测机制
- 静态扫描（Phase 1）：AST + 正则双重扫描，3 项检测——FactorLib 无 shift(-N)、shift(-N) 仅在 returns.py/datapreprocess.py、KlineLoader 无 shift
- 动态验证（Phase 2）：4 项检测 × 每个已注册因子——因子独立性（截断未来数据对比 diff < 1e-12）、最后一期收益为 NaN、对齐剔除最后时间戳、对齐后无 NaN
- FutureLeakDetector 类：run() 方法串联全部检测，to_markdown() 生成结构化 Markdown 报告
- CLI 参数：--factor（指定因子）、--return-label（收益标签）、--start-time/--end-time（数据范围）、--symbols（交易对）、--report-path（报告输出路径）
- 退出码：0 = ALL PASSED，1 = FAILED，支持 CI 集成
- 关键结论：3 个已注册因子（AlphaMomentum/AlphaPriceRange/AlphaVolatility）全部 15 项检测 PASS
- 用法：`python scripts/check_future_leak.py --start-time 2024-01-01 --end-time 2024-02-01 --symbols BTCUSDT ETHUSDT` → 15 checks ALL PASSED
- 报告输出：`python scripts/check_future_leak.py --report-path output/report.md` → 生成 Markdown 格式报告

[2026-04-04 Task 14] feat: 重点排查项检查 (17 checks passed)
- 新增 `tests/test_future_leak_audit.py`，四项排查全部 PASS
- 排查项 ① 因子计算不使用当日 close 后数据：4 个测试（动量/波动率/价格振幅独立性验证 + 截断未来数据对比），截断前后因子值 diff < 1e-12
- 排查项 ② KlineLoader 不返回未来时间戳行：3 个测试（AST 源码无 shift 操作、无前瞻关键字、仅调用 load_multi_klines 读取历史数据）
- 排查项 ③ align_factor_and_returns() 正确 drop 最后一行：4 个测试（末尾时间戳被剔除、输出无 NaN、剔除行数精确匹配 lookback NaN + 末尾 NaN、有效数据保留）
- 排查项 ④ shift(-1) 使用位置确认无反向 shift：5 个测试（AST 扫描全项目 shift(-N) 仅出现在 returns.py/datapreprocess.py、正向 shift 安全、returns.py 仅 shift(-1)、datapreprocess.py shift(-N) 用于收益矩阵、FactorLib 无 shift(-N)）
- 关键结论：系统无未来函数泄露风险，shift(-N) 严格限定在收益计算文件中
- 用法：`python -m pytest tests/test_future_leak_audit.py -v` → 17 checks 全部 PASS

[2026-04-04 Task 13] feat: 未来函数审查报告 — 逐文件审查信号生成逻辑
- 新增 `scarffold/.agent/future_leak_review.md`，覆盖 FactorLib/（3 因子）、FactorAnalysis/（12 模块）、Cross_Section_Factor/（9 模块）共 24 个文件的完整审查
- 总体结论：当前系统不存在实质性未来函数泄露风险，所有 shift(-1) 均用于 T+1 前向收益计算（标准设计），因子信号生成仅使用历史/当期数据
- 重点排查 4 项全部 PASS：因子计算不使用 close 后数据、KlineLoader 无未来时间戳、alignment 正确剔除末尾 NaN、shift(-1) 位置无反向误用
- 完整 shift 操作清单：6 处 shift 调用全部安全（returns.py/datapreprocess.py 的前看 shift 用于收益计算，timeseries_ops.py/turnover.py/alpha_momentum.py 的正 shift 用于回看）
- 用法：查阅 `scarffold/.agent/future_leak_review.md` 获取每个模块的信号时点、价格字段、T+1 对齐关系和风险等级

[2026-04-04 Task 12] feat: 分块处理集成测试 (153 checks passed)
- 新增 `tests/test_chunked_integration.py`，验证分块处理模式的完整集成行为
- 8 个场景：chunk_size=None 向后兼容（36 项属性/类型/起始值检查）、run_all() 分块模式完整流程（IC/净值/绩效比率全字段对比）、5 个子方法分块独立可用（metrics/grouping/curves/turnover/neutralize）、generate_report() 分块/全量模式对比（含选择性板块报告 + 无效板块异常）、run() 向后兼容别名（全量+分块+run vs run_all 一致性）、4 种子 × 3 chunk_size 多种子稳定性、含 NaN 数据集成、链式调用混合模式
- 关键结论：分块模式 run_all() 完整流程与全量完全一致（IC diff=0, hedge diff<1e-13），换手率/排名自相关在分块边界处为 NaN（设计行为），generate_report() 除换手率聚合指标外差异 < 1e-14
- 用法：`python tests/test_chunked_integration.py` → 153 checks 全部 PASS

[2026-04-04 Task 11] feat: 分块内存占用对比验证测试 (30 checks passed)
- 新增 `tests/test_chunked_memory.py`，验证分块模式峰值内存显著低于全量模式
- 7 个场景：chunk_size=100 峰值内存 < 全量 40%（500×200 数据：32.47%）、多种 chunk_size 内存递减趋势（50/100/250 vs full）、ChunkMemoryTracker 属性记录正确（peak_mb/rss_mb）、内存日志格式验证（peak_alloc/RSS/chunk index）、多块追踪一致性（4 块独立追踪）、run_all() 分块模式下 5 个子方法全部产生内存日志（20 条）、不同数据规模下 chunk_size=100 内存比均 < 40%（500 日期 32.83%，800 日期 25.60%）
- 关键结论：分块模式内存节省显著，chunk_size=100 时峰值内存约为全量模式的 25-33%
- 用法：`python tests/test_chunked_memory.py` → 30 checks 全部 PASS

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

