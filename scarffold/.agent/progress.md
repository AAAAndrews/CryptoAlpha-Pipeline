# Progress Log

> Append newest entries to the top in this format:
> `[YYYY-MM-DD HH:MM] summary`

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

