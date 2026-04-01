# Progress Log

> Append newest entries to the top in this format:
> `[YYYY-MM-DD HH:MM] summary`

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
