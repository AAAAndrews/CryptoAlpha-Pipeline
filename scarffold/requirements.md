# FactorLib 统一 deap_alpha 算子 + 3D 数据支持 — 需求文档

> 日期: 2026-04-20
> 状态: 待实施

## 1. 背景与目标

当前项目中存在两套独立的因子计算体系：

- **FactorLib**: 面向手动定义因子，3 个硬编码因子类，输入 `pd.DataFrame`，输出 `pd.Series`
- **deap_alpha/ops**: 面向遗传编程自动挖掘，~50 个纯函数算子（算术/时序/截面/WQ），输入 `np.ndarray (fields, assets, time)`

两套系统数据格式不兼容、算子无法复用，导致：
1. FactorLib 中新增因子需要从零编写 pandas 逻辑，无法利用已有算子
2. FactorLib 无法接受 3D ndarray 数据格式，与 deap_alpha 数据流脱节
3. 算子能力仅限于 GP 自动挖掘场景，无法服务于手动因子研发

**目标**：让 FactorLib 复用 deap_alpha 算子来组合定义因子，同时支持 3D ndarray 数据直接输入，保持与现有 FactorAnalysis 模块的完全兼容。

## 2. 约束条件

| 约束 | 说明 |
|------|------|
| BaseFactor 接口不变 | `calculate(data) → pd.Series` 签名保持，FactorAnalysis 零修改 |
| DEAP 兼容 | deap_alpha 算子字典格式 `(函数, 参数类型列表, 返回类型)` 不变，GP 功能继续可用 |
| 现有测试不受影响 | 1350+ 既存测试零回归 |
| 表达式解析 | 第一期不做字符串表达式解析（如 `rank(ts_mean(close, 10))`），后续迭代 |
| 代码规范 | 遵循 CLAUDE.md 中的 Code Style / 代码规范 |

## 3. 功能需求

### FR-1: 数据格式转换层

新增 `FactorLib/converter.py`，提供 DataFrame 与 3D ndarray 之间的双向转换。

#### FR-1.1: DataFrame → 3D ndarray

```python
def df_to_3d(
    data: pd.DataFrame,
    fields: List[str] = ['open', 'high', 'low', 'close'],
) -> Tuple[np.ndarray, List[str], List[str], np.ndarray]:
    """
    将标准化 DataFrame 转换为 3D ndarray。

    输入:
        data: pd.DataFrame，至少包含 timestamp, symbol 列和指定的 fields 列
        fields: 需要提取的字段列表

    输出:
        array_3d: np.ndarray, shape (len(fields), n_assets, n_timestamps)
        fields: 字段名列表（顺序与 array_3d 第 0 轴对应）
        symbols: 交易对列表（顺序与 array_3d 第 1 轴对应）
        timestamps: 时间戳数组（顺序与 array_3d 第 2 轴对应）
    """
```

**要求**：
- 按 symbol 排序保证跨次调用的一致性
- 正确处理 NaN 值（DataFrame 中缺失值在 ndarray 中保持 NaN）
- 去除不含任何指定 fields 的 symbol
- timestamp 按升序排列

#### FR-1.2: 2D ndarray → pd.Series

```python
def ndarray_to_series(
    values: np.ndarray,
    symbols: List[str],
    timestamps: np.ndarray,
    name: str = "",
) -> pd.Series:
    """
    将 2D 因子值 ndarray 转换为 pd.Series。

    输入:
        values: np.ndarray, shape (n_assets, n_timestamps)，与 deap_alpha 算子输出格式一致
        symbols: 交易对列表
        timestamps: 时间戳数组
        name: Series 名称

    输出:
        pd.Series，MultiIndex (timestamp, symbol)，dtype float64
    """
```

**要求**：
- 输出格式与现有 `AlphaMomentum.calculate()` 返回值完全一致
- NaN 值正确传播

### FR-2: OperatorFactor 类

新增 `FactorLib/operator_factor.py`，提供基于算子组合的因子定义方式。

#### FR-2.1: 类定义

```python
class OperatorFactor(BaseFactor):
    """
    基于 deap_alpha 算子组合定义的因子。

    Parameters:
        fields_mapping: dict
            字段名到标识的映射，如 {'close': 'close', 'volume': 'volume'}
            用于从 3D ndarray 中按字段名提取对应的 2D 切片
        compute_fn: Callable
            签名为 compute_fn(field_2d_1, field_2d_2, ...) -> np.ndarray (assets, time)
            接收一个或多个 2D ndarray，返回 2D 因子值
        name: str, optional
            因子名称，默认自动生成
    """
```

#### FR-2.2: 输入兼容性

`calculate()` 方法必须同时支持两种输入格式：

1. **pd.DataFrame**：内部调用 `df_to_3d()` 转换后计算
2. **np.ndarray (fields, assets, time)**：直接使用，跳过转换

自动检测输入类型：`isinstance(data, np.ndarray)` 判断。

#### FR-2.3: 字段提取

从 3D ndarray 中按字段名提取 2D 切片，传给 `compute_fn`：

```python
# 内部实现示意
array_3d, fields, symbols, timestamps = df_to_3d(data, fields=list(self.fields_mapping.keys()))
close_2d = array_3d[fields.index('close')]  # shape (n_assets, n_timestamps)
result_2d = self.compute_fn(close_2d)
series = ndarray_to_series(result_2d, symbols, timestamps, name=self.name)
```

#### FR-2.4: 使用示例

```python
from FactorLib import OperatorFactor
from FactorLib.ops import ts_mean, ts_std_dev, divide, rank

# 例: rank(ts_mean(close, 10)) / rank(ts_std_dev(close, 10))
def icir_factor(close_2d):
    mean_part = rank(ts_mean(close_2d, 10))
    std_part = rank(ts_std_dev(close_2d, 10))
    return divide(mean_part, std_part)

factor = OperatorFactor(
    fields_mapping={'close': 'close'},
    compute_fn=icir_factor,
    name="ICIRFactor",
)
result = factor.calculate(dataframe)        # DataFrame 输入
result = factor.calculate(array_3d)         # 3D ndarray 输入
```

### FR-3: 算子便捷导入

新增 `FactorLib/ops.py`，作为 deap_alpha 算子的统一导入入口。

```python
# FactorLib/ops.py
# 从 deap_alpha 算子库重新导出所有公共算子

# 算术算子
from Cross_Section_Factor.deap_alpha.ops.arithmetic_ops import (
    add, subtract, multiply, divide, abs, inverse, sqrt, log,
    sign, reverse, maximum, minimum, signed_power, s_log_1p,
)

# 时序算子
from Cross_Section_Factor.deap_alpha.ops.timeseries_ops import (
    ts_delay, ts_corr, ts_cov, ts_delta, ts_min, ts_max,
    ts_arg_min, ts_arg_max, ts_rank, ts_sum, ts_std_dev,
    ts_mean, ts_zscore, days_from_last_change, hump, kth_element,
)

# 截面算子
from Cross_Section_Factor.deap_alpha.ops.cross_section_ops import (
    rank, normalize, quantile, scale, winsorize, zscore,
)
```

**要求**：
- 仅做 re-export，不修改算子实现
- 命名冲突时以截面算子优先（如 `rank` 来自 cross_section_ops）

### FR-4: 注册表集成

更新 `FactorLib/__init__.py`，将新组件纳入公共 API：

```python
from FactorLib.operator_factor import OperatorFactor
from FactorLib.converter import df_to_3d, ndarray_to_series
```

`OperatorFactor` 不自动注册到全局注册表（因为是实例级参数化因子，非固定类），但支持手动注册。

### FR-5: 现有因子算子化验证（可选）

用算子组合重写现有 3 个因子，验证数值一致性：

| 现有因子 | 算子等价表达 |
|---------|-------------|
| `AlphaMomentum(lookback=N)` | `close / ts_delay(close, N) - 1`，即 `divide(close, ts_delay(close, N))` 后 `subtract(result, 1)` |
| `AlphaVolatility(lookback=N)` | `ts_std_dev(divide(close, ts_delay(close, 1)), N)` |
| `AlphaPriceRange` | `divide(subtract(open, close), subtract(high, low))` |

**要求**：
- 新增算子化因子类，不替换原有类
- 数值一致性测试：mock 数据 diff < 1e-8

## 4. 非功能需求

| 项 | 要求 |
|-----|------|
| 测试覆盖 | converter.py 和 operator_factor.py 各自独立测试，覆盖率 ≥ 90% |
| 性能 | DataFrame → 3D 转换不应成为瓶颈（大数据量场景下可跳过转换，直接接受 ndarray） |
| 向后兼容 | `from FactorLib import *` 导出不变，现有代码无需修改 |
| import 依赖 | FactorLib 依赖 Cross_Section_Factor（单向），不引入循环依赖 |

## 5. 文件变更清单

| 操作 | 文件路径 | 说明 |
|------|---------|------|
| 新增 | `FactorLib/converter.py` | 数据格式转换层 (FR-1) |
| 新增 | `FactorLib/operator_factor.py` | 算子组合因子类 (FR-2) |
| 新增 | `FactorLib/ops.py` | 算子便捷导入入口 (FR-3) |
| 修改 | `FactorLib/__init__.py` | 导出新组件 (FR-4) |
| 新增 | `tests/test_converter.py` | 转换层测试 |
| 新增 | `tests/test_operator_factor.py` | OperatorFactor 测试 |
| 新增 | `tests/test_ops_reexport.py` | 算子导入测试 (FR-3) |
| 新增 | `tests/test_factor_ops_equivalence.py` | 算子化因子一致性测试 (FR-5, 可选) |

## 6. 实施顺序

```
Phase 1: converter.py (FR-1)
    ↓
Phase 2: operator_factor.py (FR-2)
    ↓
Phase 3: ops.py + __init__.py 更新 (FR-3, FR-4)
    ↓
Phase 4: 测试用例 (全部 FR)
    ↓
Phase 5: 算子化验证 (FR-5, 可选)
```

每个 Phase 独立可测试，Phase 1-2 是核心，Phase 3-4 是收尾，Phase 5 是增强。
