# Progress Log

> Append newest entries to the top in this format:
> `[YYYY-MM-DD HH:MM] summary`

[2026-04-04 Task 27] feat: 可视化图表生成验证测试 (42 checks passed)
- 新增 `tests/test_viz_chart_validation.py`，7 个测试类 42 项检测全面覆盖 4 种图表的生成验证
- 图表正确生成 (6 tests)：IC 时间序列 / 分组收益 / 净值曲线 / 换手率均返回 plt.Figure，同一 evaluator 可依次生成全部图表，子图数量正确
- PNG 格式验证 (7 tests)：4 种图表输出均为合法 PNG（魔数 `\x89PNG\r\n\x1a\n` + IHDR chunk），文件非空，不同 DPI 下文件大小不同
- 中文标签可读 (9 tests)：`configure_chinese_font()` 无异常、自定义字体列表、4 种图表标题包含中英文、`axes.unicode_minus=False`、Y 轴标签非空、分组图例包含"组"
- 数据一致性 (9 tests)：IC 柱数 = IC 序列长度、累积 IC 量级匹配、分组折线数 = n_groups、分组/净值曲线起始值 1.0、两个子图 long/short 相同、换手率非负、自相关 ∈ [-1,1]、绘图前后 evaluator 指标不变
- 多种子稳定性 (5 tests)：5 种子 × 4 图表全部正常生成
- 边界情况 (6 tests)：最小数据（10 时间截面）、大数据（300×30）、2 组 / 10 组、4 图表保存同目录、无 output_path 不创建文件
- 关键设计：`_make_full_evaluator()` 辅助函数依次调用 run_metrics/grouping/curves/turnover，返回可用于全部 4 种图表的完整 evaluator
- 用法：`python -m pytest tests/test_viz_chart_validation.py -v` → 42 checks 全部 PASS


[2026-04-04 Task 26] feat: --viz-output CLI 参数集成可视化到 pipeline (19 checks passed)
- `run_factor_research.py` 新增 `viz_output` 参数（默认 `output/viz/`），CLI 新增 `--viz-output` 参数
- 新增 Step 7/8 可视化生成步骤：调用 `build_html_report(ev, output_dir, title)` 生成 report.html
- 路径不存在时自动递归创建（由 `build_html_report` 内部 `mkdir(parents=True, exist_ok=True)` 处理）
- `--viz-output none` / `--viz-output None` 可跳过可视化（CLI 层转为 `None`）
- 可视化失败不中断 pipeline（try/except 捕获异常打印 WARNING）
- 步骤编号从 1/7 更新为 1/8（Step 7 为可视化生成）
- 添加 `from __future__ import annotations` 支持 Python 3.9 的 `str | None` 类型注解
- 不影响 evaluator 数值结果（viz_output=None vs 启用可视化的 ICIR/Sharpe 完全一致）
- 19 项测试：CLI 解析 (5) + 函数签名 (3) + 跳过逻辑 (2) + 生成验证 (3) + 异常处理 (2) + 向后兼容 (3) + 步骤编号 (1)
- 用法：`python scripts/run_factor_research.py --factor AlphaMomentum --viz-output output/viz/`
- 用法：`python scripts/run_factor_research.py --factor AlphaMomentum --viz-output none` → 跳过可视化

[2026-04-04 Task 25] feat: HTML 报告组装 build_html_report (26 checks passed)
- 实现 `report_html.py` 中的 `build_html_report()` 函数，替换 NotImplementedError 骨架
- 使用 Jinja2 Template 渲染内嵌 HTML 模板，将图表和绩效表格组装为单一自包含 HTML 文件
- 图片以 base64 编码内嵌（`_fig_to_base64()`），无需外部依赖，单文件即可在浏览器中查看
- 自动检测 evaluator 已执行的子步骤，按需生成 4 种图表：IC 时间序列 / 分组收益 / 净值曲线 / 换手率
- 未执行的子步骤跳过对应图表（不报错），仅生成绩效表格
- 参数支持：`output_dir`（保存 report.html）、`title`（自定义标题）、`include_charts`（是否包含图表）
- `output_dir` 路径不存在时自动递归创建（`mkdir(parents=True, exist_ok=True)`）
- 校验：evaluator 为 None 或未执行 run_metrics() 均抛出 ValueError
- 更新骨架测试：`test_build_html_report_raises` 从 NotImplementedError 改为 ValueError（功能已实现）
- 26 项测试：基础功能 (5) + HTML 内容 (8) + 文件保存 (3) + 参数验证 (2) + 数据一致性 (3) + 边界情况 (5)
- 关键设计：图表生成包裹在 try/except 中，单个图表失败不影响其余图表和报告生成
- 用法：`from FactorAnalysis.visualization import build_html_report` → `html = build_html_report(ev, output_dir="output/viz/")`

[2026-04-04 Task 24] feat: 综合绩效表格 build_summary_table (40 checks passed)
- 实现 `tables.py` 中的 `build_summary_table()` 函数，替换 NotImplementedError 骨架
- 5 个板块：IC 分析（9 项指标）、收益分析（4 项）、绩效比率（6 项）、换手率（2 项）、中性化（1 项）
- ICIR / Sharpe / Calmar / Sortino 列使用信号灯标识：> 0.5 绿色 / < 0 红色 / 其他黄色 / N/A 灰色
- 内联 CSS 样式，独立使用时无需外部依赖
- 新增辅助函数：`_fmt()`（浮点格式化，None/NaN → "N/A"）、`_signal_html()`（信号灯 span 标签）
- 校验：evaluator 为 None 或未执行 run_metrics() 均抛出 ValueError
- 更新骨架测试：`test_build_summary_table_raises` 从 NotImplementedError 改为 ValueError（功能已实现）
- 40 项测试：基础功能 (4) + 表格内容 (6) + 信号灯标识 (6) + 参数验证 (2) + 数据一致性 (6) + 辅助函数 (11) + 边界情况 (5)
- 关键设计：表格独立于 Jinja2 模板，可直接嵌入任意 HTML；信号灯颜色与 report_html.py CSS 类名保持一致
- 用法：`from FactorAnalysis.visualization import build_summary_table` → `html = build_summary_table(evaluator)`

[2026-04-04 Task 23] feat: 换手率分布图 plot_turnover (24 checks passed)
- 实现 `charts.py` 中的 `plot_turnover()` 函数，替换 NotImplementedError 骨架
- 上子图：分组换手率堆叠面积图（stackplot），各组使用 Set2 colormap 着色 + 均值虚线参考线
- 下子图：因子排名自相关时间序列线图 + 均值参考线 + 零线
- 校验：evaluator 为 None / turnover 为 None / 换手率全 NaN 均抛出 ValueError
- 更新骨架测试：`test_plot_turnover_raises` 从 NotImplementedError 改为 ValueError（功能已实现）
- 24 项测试：基础功能 (4) + 图表内容 (5) + 文件保存 (3) + 参数验证 (4) + 数据一致性 (3) + 边界情况 (5)
- 关键设计：使用 stackplot 绘制各组换手率叠加面积图，直观展示整体换手水平变化；排名自相关线图展示因子衰减趋势
- 用法：`from FactorAnalysis.visualization import plot_turnover` → `fig = plot_turnover(evaluator, output_path="turnover.png")`

[2026-04-04 Task 22] feat: 组合净值曲线图 plot_portfolio_curves (25 checks passed)
- 实现 `charts.py` 中的 `plot_portfolio_curves()` 函数，替换 NotImplementedError 骨架
- 上子图：不含手续费 — long_curve（蓝色）/ short_curve（红色）/ hedge_curve（绿色）三条净值曲线
- 下子图：含手续费 — long/short 同上 + hedge_curve_after_cost（绿色，标注"含手续费"）
- 无成本数据时下子图对冲线退化为虚线 + 提示"无成本数据"
- 两个子图均含 y=1.0 灰色参考线 + 图例 + 网格 + 中英双语标题
- 参数支持：`output_path`（PNG 保存）、`figsize`、`dpi`
- 校验：evaluator 为 None / long_curve/short_curve/hedge_curve 任一为 None 均抛出 ValueError
- 更新骨架测试：`test_plot_portfolio_curves_raises` 从 NotImplementedError 改为 ValueError（功能已实现）
- 25 项测试：基础功能 (4) + 图表内容 (6) + 文件保存 (3) + 参数验证 (4) + 数据一致性 (3) + 边界情况 (5)
- 关键设计：long/short 曲线在两个子图中相同（成本仅扣除 hedge），下子图展示成本对对冲净值的实际影响
- 用法：`from FactorAnalysis.visualization import plot_portfolio_curves` → `fig = plot_portfolio_curves(evaluator, output_path="portfolio_curves.png")`

[2026-04-04 Task 21] feat: 分组收益对比图 plot_group_returns (24 checks passed)
- 实现 `charts.py` 中的 `plot_group_returns()` 函数，替换 NotImplementedError 骨架
- 从 evaluator.group_labels + evaluator.returns 计算各组等权平均日收益，cumprod 累计净值曲线
- 颜色映射：RdYlGn colormap，低组（红色）→ 高组（绿色），直观展示因子单调性
- 若 evaluator 已调用 run_curves()，叠加多空对冲净值曲线（黑色虚线）
- 起始净值归一化（1.0）+ y=1.0 灰色参考线 + 图例 + 网格
- 参数支持：`output_path`（PNG 保存）、`figsize`、`dpi`
- 校验：evaluator 为 None / group_labels 为 None / 分组标签全为 NaN 均抛出 ValueError
- 更新骨架测试：`test_plot_group_returns_raises` 从 NotImplementedError 改为 ValueError（功能已实现）
- 24 项测试：基础功能 (4) + 图表内容 (5) + 文件保存 (3) + 参数验证 (4) + 数据一致性 (3) + 边界情况 (5)
- 关键设计：使用 groupby(["timestamp", "label"]).mean().unstack("label") 计算截面内等权收益
- 用法：`from FactorAnalysis.visualization import plot_group_returns` → `fig = plot_group_returns(evaluator, output_path="group_returns.png")`

[2026-04-04 Task 20] feat: IC 时间序列图 plot_ic_timeseries (23 checks passed)
- 实现 `charts.py` 中的 `plot_ic_timeseries()` 函数，替换 NotImplementedError 骨架
- 上子图：日频 IC 柱状图 + 周度滚动 IC (5D) + 月度滚动 IC (22D) + IC 均值虚线 + ±1std 灰色阴影带 + 零线
- 下子图：累积 IC 曲线（cumsum）+ 半透明填充区域 + 零线
- 参数支持：`output_path`（PNG 保存）、`figsize`、`dpi`、`rolling_window_week`、`rolling_window_month`
- 校验：evaluator 为 None / ic 为 None / IC 序列为空均抛出 ValueError
- 更新骨架测试：`test_plot_ic_timeseries_raises` 从 NotImplementedError 改为 ValueError（功能已实现）
- 23 项测试：基础功能 (4) + 图表内容 (5) + 文件保存 (3) + 参数验证 (5) + 数据一致性 (3) + 边界情况 (3)
- 关键设计：使用 `axhline` 画均值线和零线，`axhspan` 画 ±1std 阴影带，`fill_between` 画累积 IC
- 用法：`from FactorAnalysis.visualization import plot_ic_timeseries` → `fig = plot_ic_timeseries(evaluator, output_path="ic_timeseries.png")`

[2026-04-04 Task 19] feat: FactorAnalysis/visualization/ 模块骨架 (24 checks passed)
- 新增 `FactorAnalysis/visualization/` 子包：`__init__.py` + `charts.py` + `tables.py` + `report_html.py`
- `charts.py`：`configure_chinese_font()` 自动配置中文字体（按 Windows/Mac/Linux 平台选择候选字体列表），模块加载时自动执行；4 个图表函数骨架（`plot_ic_timeseries`/`plot_group_returns`/`plot_portfolio_curves`/`plot_turnover`），后续 Task 20-23 逐步实现
- `tables.py`：`_signal_color()` 信号灯颜色逻辑（ICIR > 0.5 绿色 / < 0 红色 / 其他黄色 / None 灰色），`build_summary_table()` 骨架
- `report_html.py`：内嵌 Jinja2 HTML 模板（含信号灯 CSS），`_fig_to_base64()` 图表转 base64 工具函数，`build_html_report()` 骨架
- `__init__.py`：统一导出 7 个公共 API（configure_chinese_font + 4 图表 + 表格 + HTML 报告）
- 8 个测试类 24 项检测：子包结构完整性（4）、中文字体配置（3）、信号灯逻辑（5）、HTML 模板验证（3）、骨架函数 NotImplementedError（7）、__all__ 导出（2）
- 关键设计：图表函数骨架抛出 NotImplementedError，避免未实现功能被误调用
- 用法：`from FactorAnalysis.visualization import plot_ic_timeseries, build_html_report` → Task 20-25 实现后可用

[2026-04-04 Task 18] feat: 未来函数检测端到端集成测试 (26 checks passed)
- 新增 `tests/test_step45_e2e_integration.py`，8 个测试类 26 项检测全面覆盖 step4.5 端到端集成
- step4.5 集成正确性 (2 tests)：check_leak=True 时 FutureLeakDetector.run() 被调用、check_leak=False 时不导入 check_future_leak 模块
- PASS 时流程继续 (2 tests)：返回 evaluator 和 report、evaluator 关键指标（ICIR/Sharpe/hedge_curve）数值有效
- FAIL + leak_block=True 阻断 (3 tests)：返回 (None, None)、单项 FAIL 即阻断、全部 FAIL 也阻断
- FAIL + leak_block=False 继续 (2 tests)：返回有效 evaluator、evaluator 结果完整
- CLI 参数 (5 tests)：--check-leak 默认 False、设为 True、--leak-block 组合使用、单独 --leak-block、函数签名验证
- 向后兼容 (3 tests)：不传 check_leak 正常完成、与 check_leak=False 结果一致、evaluator 所有属性正常
- 多因子 (6 tests)：3 因子 × 2 场景（PASS 继续 + FAIL 阻断）
- 边界情况 (3 tests)：空检测报告视为 PASS、不同 return_label 下正常工作、检测参数正确传递给 FutureLeakDetector
- 关键设计：使用 monkeypatch mock KlineLoader 和 FutureLeakDetector，避免真实数据访问和网络依赖
- 用法：`python -m pytest tests/test_step45_e2e_integration.py -v` → 26 checks 全部 PASS


[2026-04-04 Task 17] feat: 未来函数检测集成到 run_factor_research.py step4.5
- `run_factor_research.py` 在 step4（对齐）之后、step5（评估）之前新增可选的 step4.5 未来函数检测步骤
- 新增 `check_leak: bool = False` 参数：启用后执行 FutureLeakDetector 完整检测（静态扫描 + 动态验证）
- 新增 `leak_block: bool = False` 参数：检测 FAIL 时阻断 pipeline（返回 None, None），不设则仅打印 WARNING 继续
- CLI 新增 `--check-leak`（store_true）和 `--leak-block`（store_true）两个参数
- 步骤编号从 1/6 更新为 1/7（step 4.5 插入后总步骤 +1）
- 导入方式：延迟导入 `from scripts.check_future_leak import FutureLeakDetector`，避免循环依赖
- 检测通过时打印 PASSED 信息；失败时根据 leak_block 决定阻断或继续
- 向后兼容：不传 --check-leak 时行为与改造前完全一致
- 用法：`python scripts/run_factor_research.py --factor AlphaMomentum --check-leak` → step4.5 执行检测
- 用法：`python scripts/run_factor_research.py --factor AlphaMomentum --check-leak --leak-block` → 检测 FAIL 时阻断 pipeline

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

