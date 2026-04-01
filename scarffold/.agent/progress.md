# Progress Log

> Append newest entries to the top in this format:
> `[YYYY-MM-DD HH:MM] summary`

[2026-04-02 12:30] feat: complete task 17 — 分组中性化权重深度验证测试
- 新增 `tests/test_task17_neutralize_verify.py`，63 项测试全部通过
- 覆盖范围：返回结构与类型 (7)、groups 参数类型校验 (11)、四种组合模式 (9)、中性化效果 (3)、与原始曲线对比 (3)、边界情况 (12)、多种子稳定性 (18)
- 关键验证点：cumprod 一致性验证、datetime 索引类型、 demeaned 确实减小组内因子方差、group_adjust 确实减小组内收益方差、
  四种模式两两互不相同、双重中性化不同于单一模式、含 NaN/inf 数据正常、相同种子确定性输出、8 种子×4 模式全部通过
- 用法：`python -m pytest tests/test_task17_neutralize_verify.py -v`

[2026-04-03 00:30] feat: complete task 16 — 分组中性化净值曲线 (neutralize)
- 新增 `FactorAnalysis/neutralize.py`，实现 `calc_neutralized_curve(factor, returns, groups, demeaned=True, group_adjust=False)`
- 支持组内因子去均值（demeaned）和组内收益去均值（group_adjust）两种中性化模式
- groups 参数支持 int（自动分位数分组）和 pd.Series（自定义分组标签）两种类型
- 中性化后按处理后的因子值排名，构建 top1-bottom1 多空对冲净值曲线
- 完整参数校验：类型检查、groups 范围、n_groups 范围
- 新增 `tests/test_task16_neutralize.py`，35 项测试全部通过
- 覆盖范围：导入校验 (2)、返回结构与类型 (4)、groups 参数校验 (7)、四种组合模式 (5)、中性化效果 (3)、边界情况 (6)、多种子稳定性 (8)
- 用法：
  ```python
  from FactorAnalysis.neutralize import calc_neutralized_curve
  # groups=int: 自动分位数分组
  curve = calc_neutralized_curve(factor, returns, groups=4, demeaned=True)
  # groups=pd.Series: 自定义分组标签
  curve = calc_neutralized_curve(factor, returns, groups=industry_labels, demeaned=True, group_adjust=True)
  # 返回 pd.Series, index=timestamp, 起始值 1.0
  ```

[2026-04-02 23:30] feat: complete task 15 — 数据质量追踪深度验证测试
- 新增 `tests/test_task15_data_quality_verify.py`，74 项测试全部通过
- 覆盖范围：返回结构与类型 (6)、正常覆盖率无告警 (7)、低覆盖率告警 (6)、极低覆盖率异常 (5)、max_loss 参数校验 (13)、输入类型校验 (3)、索引对齐 (4)、inf/NaN 混合无效值 (7)、独立 NaN (3)、边界情况 (7)、覆盖率单调性 (3)
- 关键验证点：多种子稳定性、参数化 NaN 比例告警/异常行为、max_loss 阈值控制、部分重叠索引覆盖率计算、inf/-inf 视为无效、因子与收益率独立 NaN 取并集、30% 边界精确验证
- 用法：`python -m pytest tests/test_task15_data_quality_verify.py -v`

[2026-04-02 23:00] feat: complete task 14 — 数据质量追踪 (data_quality)
- 新增 `FactorAnalysis/data_quality.py`，实现 `check_data_quality(factor, returns, max_loss=0.35)`
- 先对因子值和收益率做索引内连接对齐，再计算有效数据（非 NaN、非 inf）的覆盖率
- 覆盖率低于 (1 - max_loss) 时发出 UserWarning，低于 30% 时抛出 ValueError
- 完整参数校验：类型检查、max_loss 范围 [0, 1)、无重叠索引检测
- 已导出到 `FactorAnalysis/__init__.py` 的公共 API
- 新增 `tests/test_task14_data_quality.py`，29 项测试全部通过
- 覆盖范围：导入校验 (2)、正常覆盖率 (5)、低覆盖率告警 (4)、极低覆盖率异常 (2)、max_loss 参数校验 (6)、输入类型校验 (3)、边界情况 (5)、覆盖率精度 (2)
- 用法：
  ```python
  from FactorAnalysis.data_quality import check_data_quality
  coverage = check_data_quality(factor_values, forward_returns, max_loss=0.35)
  # 返回 float 覆盖率，低覆盖率发出 UserWarning，极低抛出 ValueError
  ```

[2026-04-02 22:00] feat: complete task 13 — 换手率指标深度验证测试
- 新增 `tests/test_task13_turnover_verify.py`，51 项测试全部通过
- 覆盖范围：返回结构与类型 (8)、值域验证 (6)、稳定因子行为 (5)、振荡因子行为 (2)、弱/随机因子行为 (6)、边界情况 (11)、参数校验 (8)、大数据量 (1)
- 关键验证点：参数化 n_groups/lag 组合值域正确、多种子稳定性、振荡因子自相关=-1.0 且换手率=1.0、
  带噪声因子自相关 > 0、变化资产集合正常处理、资产数少于组数退化处理、含 inf 值正常、高 NaN 比例正常
- 用法：`python -m pytest tests/test_task13_turnover_verify.py -v`

[2026-04-02 21:00] feat: complete task 12 — 分组换手率与因子排名自相关 (turnover)
- 新增 `FactorAnalysis/turnover.py`，实现 `calc_turnover(factor, n_groups)` 和 `calc_rank_autocorr(factor, lag=1)`
- calc_turnover: 基于分位数分组，计算相邻截面各分组内成员变动比例，返回 pd.DataFrame（columns=分组标签）
- calc_rank_autocorr: 对横截面排名计算 Pearson 自相关，衡量因子排名持续性，返回 pd.Series
- 新增 `tests/test_task12_turnover.py`，23 项测试全部通过
- 覆盖范围：导入 (2)、calc_turnover 基础功能 (5)、calc_rank_autocorr 基础功能 (5)、强/弱因子行为 (2)、边界情况 (9)
- 关键验证点：稳定因子换手率=0、稳定因子自相关=1.0、振荡因子自相关=-1.0、首期 NaN、值域正确、参数校验
- 用法：
  ```python
  from FactorAnalysis.turnover import calc_turnover, calc_rank_autocorr
  turnover_df = calc_turnover(factor, n_groups=5)  # pd.DataFrame, columns=0~4
  autocorr = calc_rank_autocorr(factor, lag=1)     # pd.Series, index=timestamp
  ```

[2026-04-02 20:00] feat: complete task 11 — 零值感知分组验证测试 (zero_aware)
- 新增 `tests/test_task11_zero_aware_grouping_verify.py`，24 项测试全部通过
- 覆盖范围：向后兼容性 (2)、基本功能 (5)、分组比例分配 (3)、边界情况 (9)、多截面一致性 (3)、输出结构 (2)
- 关键验证点：zero_aware=False 与默认参数结果一致、标签范围 [0, n_groups-1]、负值标签 < 正值标签、
  零值归入负值侧、全正/全负/全零退化为标准分组、NaN 保留、n_groups<2 抛出 ValueError、
  多截面独立分组、大数据量正常工作、非 NaN 标签数等于有效因子值数
- 用法：`python -m pytest tests/test_task11_zero_aware_grouping_verify.py -v`

[2026-04-02 19:00] feat: complete task 10 — quantile_group zero_aware 零值感知分组
- 修改 `FactorAnalysis/grouping.py`，为 `quantile_group` 新增 `zero_aware: bool = False` 参数
- zero_aware=True 时按正负拆分后各自做分位数分组：负值（含零）获得较低标签，正值获得较高标签
- 按两侧样本量比例分配分组数（`round(n_groups * n_neg / total)`），每侧至少 1 组
- 某一侧为空时自动退化为标准分组；保持与原有 `duplicates='drop'` 和异常处理一致
- 21 项已有测试全部通过，无回归
- 用法：
  ```python
  from FactorAnalysis.grouping import quantile_group
  labels = quantile_group(factor, n_groups=5, zero_aware=True)
  # 负值标签 < 正值标签，标签范围 [0, n_groups-1]
  ```

[2026-04-02 18:00] feat: complete task 9 — calc_ic_stats IC 统计显著性深度验证测试
- 新增 `tests/test_task09_ic_stats_verify.py`，52 项测试全部通过
- 覆盖范围：返回类型与字段完整性 (5)、强因子 t_stat/p_value 合理性 (18)、弱因子统计合理性 (9)、负相关因子 (4)、skew/kurtosis 分布统计 (5)、边界情况 (11)
- 关键验证点：多种子参数化强因子（0.3/0.5/0.7）下 IC_mean/t_stat/p_value/ICIR 均合理、
  t_stat 手动计算与 scipy 输出一致（IC_mean / (IC_std / sqrt(n))）、
  弱因子（0.01/0.02/0.03）下 p_value 不显著、负相关因子各项指标符号正确、
  多种子（10 seeds）下 skew/kurtosis 稳定性、NaN 数据/常量因子/单交易对等边界情况
- 用法：`python -m pytest tests/test_task09_ic_stats_verify.py -v`

[2026-04-02 17:00] feat: complete task 8 — calc_ic_stats IC 统计显著性指标
- 新增 `FactorAnalysis/metrics.py` 中的 `calc_ic_stats(factor, returns)` 函数
- 返回 pd.Series 包含 7 个字段：IC_mean, IC_std, ICIR, t_stat, p_value, IC_skew, IC_kurtosis
- 复用已有的 `calc_ic()` 计算日频 Pearson IC 序列，再用 scipy.stats 进行 t 检验和分布统计
- 参数校验：类型检查、空数据检查；数据不足（<3 个有效 IC）时返回全 NaN 并发出 UserWarning
- 新增 `tests/test_task08_calc_ic_stats.py`，22 项测试全部通过
- 覆盖范围：导入与基础检查 (3)、强因子统计合理性 (5)、弱因子统计合理性 (2)、ICIR 计算 (2)、分布统计 (3)、边界情况 (7)
- 用法：
  ```python
  from FactorAnalysis.metrics import calc_ic_stats
  stats = calc_ic_stats(factor_values, forward_returns)
  # 返回 pd.Series: IC_mean, IC_std, ICIR, t_stat, p_value, IC_skew, IC_kurtosis
  ```

[2026-04-02 16:00] feat: complete task 7 — 收益矩阵与对齐集成验证测试
- 新增 `tests/test_task07_returns_alignment_verify.py`，20 项测试全部通过
- 覆盖范围：两种收益率标签端到端管道 (3)、T+1 无未来函数验证 (3)、对齐后无 NaN/inf (3)、索引一致性 (4)、边界情况 (7)
- 关键验证点：close2close/open2open 通过完整管道（calc_returns → align_factor_returns）结果正确、
  T+1 前向收益仅依赖下一期价格不含未来信息、对齐后输出完全无缺失值、
  多交易对索引按 (timestamp, symbol) 正确保留、零价格/空输入/不匹配索引等边界情况
- 备注：calc_returns 中 expected_nan 计算逻辑存在已知问题（使用总行数而非交易对数），不影响对齐结果正确性
- 用法：`python -m pytest tests/test_task07_returns_alignment_verify.py -v`

[2026-04-02 15:00] feat: complete task 6 — 因子值与收益矩阵对齐
- 新增 `FactorAnalysis/alignment.py`，实现 `align_factor_returns(factor, returns)`
- 按 (timestamp, symbol) MultiIndex 内连接，仅保留两侧共有的行
- 剔除因子值或收益率为 NaN / inf 的行，输出无缺失的干净 DataFrame
- 数据丢失超过 50% 时发出 UserWarning 告警
- 完整参数校验：类型检查、MultiIndex 检查
- 新增 `tests/test_task06_alignment.py`，24 项测试全部通过
- 覆盖范围：导入 (2)、内连接对齐 (3)、NaN/inf 剔除 (5)、输出结构 (4)、告警 (2)、边界情况 (4)、参数校验 (4)
- 用法：
  ```python
  from FactorAnalysis.alignment import align_factor_returns
  clean = align_factor_returns(factor_values, forward_returns)
  # 返回 pd.DataFrame, columns=['factor', 'returns'], MultiIndex (timestamp, symbol)
  ```

[2026-04-02 14:00] feat: complete task 5 — T+1 前向收益矩阵计算
- 新增 `FactorAnalysis/returns.py`，实现 `calc_returns(data, label='close2close')`
- 支持两种收益率标签：close2close（(next_close/current_close)-1）和 open2open（(next_open/current_open)-1）
- 输入支持普通 DataFrame（含 timestamp/symbol 列）和已设 MultiIndex 的 DataFrame
- 输出为 pd.Series，MultiIndex (timestamp, symbol)，每组最后一期为 NaN
- 包含分母为零保护、参数校验、额外 NaN 告警
- 新增 `tests/test_task05_returns.py`，22 项测试全部通过
- 覆盖范围：导入校验 (3)、close2close 正确性 (3)、open2open 正确性 (2)、返回值结构 (5)、多交易对独立性 (2)、边界情况 (7)
- 用法：
  ```python
  from FactorAnalysis.returns import calc_returns
  returns = calc_returns(data, label='close2close')  # 或 'open2open'
  # 返回 pd.Series, MultiIndex (timestamp, symbol)
  ```

[2026-04-02 13:00] feat: complete task 4 — Alpha3 价格振幅因子验证测试
- 新增 `tests/test_task04_alpha_price_range_verify.py`，24 项测试全部通过
- 覆盖范围：继承关系 (5)、因子值形状 (4)、计算正确性 (5)、边界情况 (4)、注册表集成 (3)、公共导出 (3)
- 关键验证点：BaseFactor 抽象类不可实例化、公式 (open-close)/(high-low) 精确计算、high==low 返回 NaN、多交易对混合数据、全局注册表 get/list 正确、FactorLib 包级 __all__ 导出
- 用法：`python -m pytest tests/test_task04_alpha_price_range_verify.py -v`

[2026-04-02 12:00] feat: complete task 3 — Alpha3 价格振幅因子
- 新增 `FactorLib/alpha_price_range.py`，AlphaPriceRange 继承 BaseFactor
- 公式：price_range = (open - close) / (high - low)
- high == low 时返回 NaN（分母为零保护）
- 使用 numpy.where 向量化计算，避免逐行循环
- 已注册到全局因子注册表（FactorLib.__init__ 中调用 register）
- 同时将 AlphaMomentum、AlphaVolatility 一并注册到 registry
- 4 项验证全部通过：基础计算、边界情况、继承关系、注册表集成
- 用法：
  ```python
  from FactorLib.alpha_price_range import AlphaPriceRange
  factor = AlphaPriceRange()
  result = factor.calculate(data)  # data 需包含 open, high, low, close 列
  ```

[2026-04-02 00:10] feat: complete task 2 — KlineLoader 通用 K 线数据加载器验证测试
- 新增 `tests/test_task02_kline_loader_verify.py`，47 项测试全部通过
- 覆盖范围：导入校验 (3)、参数化过滤 (10)、全样本加载 (6)、数据验证 (13)、排序 (2)、继承接口 (6)、边界情况 (7)
- 关键验证点：datetime/毫秒时间戳双格式、单/多交易对过滤、OHLC 各列 NaN 检测、high==low DOJI 边界、validate=False 跳过全部校验、compile 幂等性
- 用法：`python -m pytest tests/test_task02_kline_loader_verify.py -v`

[2026-04-02 00:00] feat: complete task 1 — KlineLoader 通用 K 线数据加载器
- 新增 `Cross_Section_Factor/kline_loader.py`，继承 BaseDataLoader
- 支持参数化过滤：start_time, end_time, symbols, exchange, kline_type, interval
- 数据校验：必需列检查、NaN 检查、重复行检查、high >= low 逻辑检查
- 支持 validate=False 跳过校验，全样本加载（不传时间范围/symbols）
- 18 项单元测试全部通过 (tests/test_task01_kline_loader.py)
- 用法示例:
  ```python
  from Cross_Section_Factor.kline_loader import KlineLoader
  loader = KlineLoader(start_time="2024-01-01", end_time="2024-06-01", symbols=["BTCUSDT"])
  df = loader.compile()  # 加载 + 校验 + 排序
  ```
