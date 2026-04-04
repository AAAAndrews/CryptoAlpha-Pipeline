"""
tests/mutual_components/conftest_perf.py — 性能优化测试共享 fixtures
Shared fixtures for performance optimization test suite.

提供可控 mock 数据生成、计时辅助、数值一致性断言。
Provides controllable mock data generation, timing helpers, numerical consistency assertions.

所有下游 perf 测试 (test_perf_*.py) 统一从此模块导入。
All downstream perf tests (test_perf_*.py) import from this module.
"""

import time
from contextlib import contextmanager

import numpy as np
import pandas as pd


# ============================================================
# Mock 数据生成 / Mock data generation
# ============================================================

# 预定义场景 ID / Predefined scenario IDs
SCENARIO_BASIC = "basic"               # 标准正态因子 + 弱相关收益 / standard normal + weak correlation
SCENARIO_HIGH_IC = "high_ic"           # 高正相关 (IC ≈ 1.0) / high positive correlation
SCENARIO_NEG_IC = "neg_ic"             # 负相关 (IC ≈ -1.0) / negative correlation
SCENARIO_WITH_NAN = "with_nan"         # 含 NaN 数据 / with NaN values
SCENARIO_LARGE = "large"               # 大数据集 (500×100) / large dataset
SCENARIO_SMALL = "small"               # 小数据集边界 / small dataset edge case

# 场景配置表 / Scenario configuration table
SCENARIOS = {
    SCENARIO_BASIC: dict(n_days=200, n_symbols=50, seed=42, nan_frac=0.02, corr=0.3),
    SCENARIO_HIGH_IC: dict(n_days=200, n_symbols=50, seed=100, nan_frac=0.01, corr=0.9),
    SCENARIO_NEG_IC: dict(n_days=200, n_symbols=50, seed=200, nan_frac=0.01, corr=-0.8),
    SCENARIO_WITH_NAN: dict(n_days=200, n_symbols=50, seed=300, nan_frac=0.10, corr=0.3),
    SCENARIO_LARGE: dict(n_days=500, n_symbols=100, seed=42, nan_frac=0.02, corr=0.3),
    SCENARIO_SMALL: dict(n_days=10, n_symbols=5, seed=42, nan_frac=0.0, corr=0.3),
}


def make_synthetic_data(
    n_days: int = 200,
    n_symbols: int = 50,
    seed: int = 42,
    nan_frac: float = 0.02,
    corr: float = 0.3,
) -> tuple[pd.Series, pd.Series]:
    """
    生成可控 mock 因子值 + 前向收益率 / Generate controllable mock factor + returns.

    因子值服从标准正态分布，收益率 = corr * factor + noise，按 nan_frac 比例注入 NaN。
    Factor values ~ N(0,1), returns = corr * factor + noise, NaN injected at nan_frac rate.

    Parameters / 参数:
        n_days: 时间截面数 / Number of time cross-sections
        n_symbols: 资产数 / Number of assets (symbols)
        seed: 随机种子 / Random seed
        nan_frac: NaN 注入比例 (0~1) / NaN injection rate (0~1)
        corr: 因子-收益线性相关系数 / Factor-returns linear correlation coefficient

    Returns / 返回:
        tuple: (factor, returns)，均为 MultiIndex (timestamp, symbol) 的 pd.Series
               Both are pd.Series with MultiIndex (timestamp, symbol)
    """
    rng = np.random.default_rng(seed)
    timestamps = pd.date_range("2025-01-01", periods=n_days, freq="D")
    symbols = [f"SYM_{i:03d}" for i in range(n_symbols)]

    # 因子值：标准正态 / Factor values: standard normal
    factor_values = rng.standard_normal((n_days, n_symbols))

    # 收益率 = corr * factor + noise / Returns = corr * factor + noise
    noise = rng.standard_normal((n_days, n_symbols)) * np.sqrt(1 - corr ** 2)
    returns_values = corr * factor_values + noise

    # 注入 NaN / Inject NaN
    if nan_frac > 0:
        nan_mask = rng.random((n_days, n_symbols)) < nan_frac
        factor_values[nan_mask] = np.nan
        returns_values[nan_mask] = np.nan

    # 构建 MultiIndex Series / Build MultiIndex Series
    idx = pd.MultiIndex.from_product([timestamps, symbols], names=["timestamp", "symbol"])
    factor_s = pd.Series(factor_values.ravel(), index=idx, name="factor")
    returns_s = pd.Series(returns_values.ravel(), index=idx, name="returns")

    return factor_s, returns_s


def iter_scenarios():
    """
    遍历所有预定义场景，生成 (scenario_id, factor, returns) 元组。
    Iterate all predefined scenarios, yielding (scenario_id, factor, returns) tuples.

    用于参数化测试 / For parametrized tests:
        for sid, factor, returns in iter_scenarios():
            ...
    """
    for sid, params in SCENARIOS.items():
        factor, returns = make_synthetic_data(**params)
        yield sid, factor, returns


# ============================================================
# 计时辅助 / Timing helpers
# ============================================================

@contextmanager
def benchmark(label: str = ""):
    """
    计时上下文管理器 / Timing context manager.

    Usage / 用法:
        with benchmark("calc_ic"):
            result = calc_ic(factor, returns)
        # prints: [benchmark] calc_ic: 0.123s
    """
    t0 = time.perf_counter()
    yield
    elapsed = time.perf_counter() - t0
    tag = f" {label}" if label else ""
    print(f"[benchmark]{tag}: {elapsed:.4f}s")


def measure_time(func, *args, n_runs: int = 1, **kwargs) -> tuple:
    """
    测量函数执行时间 / Measure function execution time.

    Parameters / 参数:
        func: 待测函数 / Function to measure
        *args: 位置参数 / Positional arguments
        n_runs: 重复运行次数 / Number of repeated runs
        **kwargs: 关键字参数 / Keyword arguments

    Returns / 返回:
        tuple: (result, elapsed_seconds) / (结果, 耗时秒数)
    """
    t0 = time.perf_counter()
    result = None
    for _ in range(n_runs):
        result = func(*args, **kwargs)
    elapsed = time.perf_counter() - t0
    return result, elapsed


# ============================================================
# 数值一致性断言 / Numerical consistency assertions
# ============================================================

def assert_scalar_close(a, b, tol: float = 1e-10, label: str = ""):
    """
    断言两个标量数值接近 / Assert two scalars are close.

    双方均为 NaN 时视为通过。比较 NaN 时使用 math.isnan，容差范围内视为通过。
    Both-NaN passes. Within tolerance passes.

    Raises / 异常:
        AssertionError: 数值差异超过容差 / Difference exceeds tolerance
    """
    # 双方 NaN 视为相等 / both NaN treated as equal
    if np.isnan(a) and np.isnan(b):
        return
    # 单方 NaN / one-side NaN
    if np.isnan(a) or np.isnan(b):
        raise AssertionError(
            f"{'[' + label + '] ' if label else ''}"
            f"scalar mismatch: one is NaN (a={a}, b={b})"
        )
    diff = abs(float(a) - float(b))
    if diff > tol:
        raise AssertionError(
            f"{'[' + label + '] ' if label else ''}"
            f"scalar mismatch: diff={diff:.2e} > tol={tol:.2e} (a={a}, b={b})"
        )


def assert_series_close(
    a: pd.Series,
    b: pd.Series,
    tol: float = 1e-10,
    label: str = "",
    check_names: bool = False,
):
    """
    断言两个 Series 数值接近 / Assert two Series are numerically close.

    逐元素比较，容差 tol。NaN 视为相等。索引不一致时报告差异。
    Compare element-wise within tolerance. NaN treated as equal. Index mismatch reported.

    Parameters / 参数:
        a, b: 待比较的 Series / Series to compare
        tol: 元素级容差 / Element-wise tolerance
        label: 断言标签，用于错误信息 / Label for error messages
        check_names: 是否校验 Series.name / Whether to check Series.name
    """
    prefix = f"[{label}] " if label else ""

    # 长度检查（对齐前） / Length check (before alignment)
    if len(a) != len(b):
        raise AssertionError(
            f"{prefix}length mismatch: {len(a)} vs {len(b)}"
        )

    # 索引对齐 / Align indices
    if not a.index.equals(b.index):
        a_aligned, b_aligned = a.align(b, join="inner")
        missing = len(a) - len(a_aligned)
        if missing > 0:
            raise AssertionError(
                f"{prefix}index mismatch: {missing} elements only in one Series"
            )
        a, b = a_aligned, b_aligned

    # NaN 位置一致性 / NaN position consistency
    a_nan = a.isna()
    b_nan = b.isna()
    if not (a_nan == b_nan).all():
        diff_mask = a_nan != b_nan
        n_diff = diff_mask.sum()
        raise AssertionError(
            f"{prefix}NaN position mismatch at {n_diff} positions"
        )

    # 非NaN元素数值比较 / Non-NaN element comparison
    valid = ~a_nan
    if valid.sum() > 0:
        diff = (a[valid] - b[valid]).abs()
        max_diff = diff.max()
        if max_diff > tol:
            worst_idx = diff.idxmax()
            raise AssertionError(
                f"{prefix}Series element mismatch: max_diff={max_diff:.2e} > tol={tol:.2e} "
                f"at index={worst_idx} (a={a[worst_idx]}, b={b[worst_idx]})"
            )


def assert_frame_close(
    a: pd.DataFrame,
    b: pd.DataFrame,
    tol: float = 1e-10,
    label: str = "",
):
    """
    断言两个 DataFrame 数值接近 / Assert two DataFrames are numerically close.

    逐元素比较，容差 tol。NaN 视为相等。索引/列不一致时报告差异。
    Compare element-wise within tolerance. NaN treated as equal. Index/column mismatch reported.

    Parameters / 参数:
        a, b: 待比较的 DataFrame / DataFrame to compare
        tol: 元素级容差 / Element-wise tolerance
        label: 断言标签，用于错误信息 / Label for error messages
    """
    prefix = f"[{label}] " if label else ""

    # 索引和列对齐 / Align index and columns
    if not a.index.equals(b.index) or not a.columns.equals(b.columns):
        a_aligned, b_aligned = a.align(b, join="inner")
        if a_aligned.shape != a.shape or b_aligned.shape != b.shape:
            raise AssertionError(
                f"{prefix}shape mismatch: a={a.shape} vs b={b.shape}"
            )
        a, b = a_aligned, b_aligned

    if a.shape != b.shape:
        raise AssertionError(
            f"{prefix}shape mismatch after alignment: a={a.shape} vs b={b.shape}"
        )

    # NaN 位置一致性 / NaN position consistency
    a_nan = a.isna()
    b_nan = b.isna()
    if not (a_nan == b_nan).all().all():
        diff_mask = a_nan != b_nan
        n_diff = diff_mask.sum().sum()
        raise AssertionError(
            f"{prefix}NaN position mismatch at {n_diff} cells"
        )

    # 非NaN元素数值比较 / Non-NaN element comparison
    valid = ~a_nan
    if valid.sum().sum() > 0:
        diff = (a[valid] - b[valid]).abs()
        max_diff = diff.max().max()
        if max_diff > tol:
            # 找到最大差异位置 / Find worst difference location
            full_diff = (a.fillna(0) - b.fillna(0)).abs()
            worst_loc = full_diff.stack().idxmax()
            raise AssertionError(
                f"{prefix}DataFrame element mismatch: max_diff={max_diff:.2e} > tol={tol:.2e} "
                f"at {worst_loc} (a={a.loc[worst_loc]}, b={b.loc[worst_loc]})"
            )
