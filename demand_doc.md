# CryptoAlpha-Pipeline 需求细化文档

> 优先级排序：需求2（内存优化） > 需求1（未来函数检查） > 需求3（可视化模块）
> 创建日期：2026-04-04

---

## 需求2：Step5 内存优化（分块处理 Chunking）

**背景**：`scripts/run_factor_research.py` 的 step5 调用 `FactorEvaluator.run_all()` 时，在全量标的×全量时间维度上执行 IC 计算、分组、组合曲线、换手率等操作，中间产生大量临时 DataFrame 导致内存溢出。

**目标**：在保证计算结果一致的前提下，通过分块处理使内存占用可控。

### 子需求

| ID | 子任务 | 说明 |
|----|--------|------|
| 2.1 | `FactorEvaluator` 新增 `chunk_size` 参数 | 默认 `None`（全量），设为整数时按时间戳分块处理。分块边界以 rebalance 频率为基准对齐，避免跨块信号丢失。 |
| 2.2 | IC/IR 指标分块计算 | `run_metrics()` 按时间 chunk 逐块计算 pearson/spearman 相关性，最后汇总 `IC_mean`、`IC_std`、`ICIR` 等聚合统计量。分块间结果需与全量计算数值一致（允许浮点精度差异 <1e-8）。 |
| 2.3 | 分组回测分块计算 | `run_grouping()` / `run_curves()` 按时间 chunk 逐块构建组合权重与收益曲线，块间衔接需保持持仓连续性（上一块末尾持仓作为下一块初始持仓）。 |
| 2.4 | 换手率 & 自相关分块计算 | `run_turnover()` 按分块逐段计算，首块无前置持仓时换手率标记为 NaN。 |
| 2.5 | 中性化分块支持 | `run_neutralize()` 若涉及行业/市值中性化，需确保分块内中性化因子覆盖完整截面。 |
| 2.6 | 内存监控日志 | 分块处理时输出每块的峰值内存（通过 `psutil` 或 `tracemalloc`），写入日志便于调优 `chunk_size`。 |

### 验收标准

- 设定 `chunk_size=100`（约100个交易日）时，峰值内存 < 全量模式的 40%。
- 分块结果与全量结果的数值差异 < 1e-8。
- `chunk_size=None` 时行为与改造前完全一致（向后兼容）。

---

## 需求1：未来函数检查（Future Function Detection）

**背景**：因子信号在 T 日生成，交易必须在 T+1 日（下一个K线）执行。需确认 `close2close` 和 `open2open` 收益率的计算方式不会引入未来信息。当前实现在 `FactorAnalysis/returns.py` 中使用 `shift(-1)` 计算前瞻收益，但需排查信号端是否存在时间泄露。

### 子需求

| ID | 子任务 | 说明 |
|----|--------|------|
| 1.1 | 人工审查报告 | 逐文件审查 `FactorLib/`、`FactorAnalysis/`、`Cross_Section_Factor/` 中的信号生成逻辑，输出 Markdown 报告，列出：每个因子/模块的信号时点、使用的价格字段、与 T+1 交易的对齐关系、风险等级（安全/疑似/危险）。 |
| 1.2 | 重点排查项 | ①因子计算中是否使用了当日 close 后的数据（如 volume 的高频聚合）；② `KlineLoader` 返回的数据是否包含未来时间戳的行；③ 对齐步骤 `align_factor_and_returns()` 是否正确 drop 了最后一行（无前瞻收益的行）。 |
| 1.3 | 自动化检测脚本 | 编写 `scripts/check_future_leak.py`，自动执行：① 加载因子数据，检查 `factor` 列与 `forward_return` 列的时间对齐；② 验证每个时间戳的 factor 值不依赖当日之后的任何数据字段；③ 检查 `shift(-1)` 的使用位置，确认无反向 shift（即无 `shift(+1)` 后再参与信号计算的情况）。输出 PASS/FAIL 报告。 |
| 1.4 | 集成到 Pipeline | 将 `check_future_leak.py` 作为 `run_factor_research.py` step4.5（对齐之后、评估之前）的可选步骤，或作为独立 CI 检查项。 |

### 验收标准

- 审查报告覆盖所有已注册因子和关键模块。
- 自动化脚本对所有现有因子输出 PASS（当前代码无未来函数），对人为注入的泄露用例输出 FAIL。
- CI 集成后，新因子若存在泄露会阻断 pipeline。

---

## 需求3：可视化模块（Visualization Module）

**背景**：当前 `FactorEvaluator` 输出为纯数值 DataFrame，缺乏直观的图表展示。需新增可视化模块，将回测绩效指标以静态图表 + HTML 报告的形式输出。

### 子需求

| ID | 子任务 | 说明 |
|----|--------|------|
| 3.1 | 创建 `FactorAnalysis/visualization/` 模块 | 新增独立子包，包含图表生成和 HTML 报告组装逻辑。依赖 `matplotlib`（已在 requirements 中）和 `jinja2`（HTML 模板渲染）。 |
| 3.2 | 输出路径配置 | 在 `run_factor_research.py` 中新增 `--viz-output` 参数（默认 `output/viz/`），所有图表和 HTML 报告输出到该路径。 |
| 3.3 | IC 时间序列图 | 绘制滚动 IC（月度/周度）和累积 IC 曲线，标注 IC 均值线 ± 1std 阴影带。保存为 `ic_timeseries.png`。 |
| 3.4 | 分组收益对比图 | 绘制按因子分位数（如5组）的累计收益曲线，标注多空对冲收益。保存为 `group_returns.png`。 |
| 3.5 | 组合净值曲线图 | 绘制 long / short / hedge 三条净值曲线（含/不含手续费两个子图）。保存为 `portfolio_curves.png`。 |
| 3.6 | 换手率分布图 | 绘制按时间序列的换手率柱状图或面积图。保存为 `turnover.png`。 |
| 3.7 | 综合绩效表格 | 将 `FactorEvaluator` 的指标汇总为格式化 HTML 表格，包含信号灯标识（如 ICIR > 0.5 绿色，< 0 红色）。 |
| 3.8 | HTML 报告组装 | 使用 Jinja2 模板将上述图表和表格组装为单一自包含 HTML 文件 `report.html`（图片 base64 内嵌，无需外部依赖）。 |

### 验收标准

- 所有图表清晰可读，支持中文标签（配置 `matplotlib` 中文字体）。
- `report.html` 可独立打开，不依赖外部图片文件。
- `--viz-output` 路径不存在时自动创建。
- 不影响现有 pipeline 的数值输出（纯增量功能）。

---

## 附录：实施顺序建议

```
需求2（内存优化） → 需求1（未来函数检查） → 需求3（可视化模块）
```

