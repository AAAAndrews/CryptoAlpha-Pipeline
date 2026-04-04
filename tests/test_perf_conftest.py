"""
tests/test_perf_conftest.py — 验证 conftest_perf.py 共享 fixtures 功能正确性
Verify conftest_perf.py shared fixtures work correctly.
"""

import numpy as np
import pandas as pd
import pytest

from tests.mutual_components.conftest_perf import (
    make_synthetic_data,
    iter_scenarios,
    SCENARIOS,
    SCENARIO_BASIC,
    SCENARIO_HIGH_IC,
    SCENARIO_NEG_IC,
    SCENARIO_WITH_NAN,
    SCENARIO_LARGE,
    SCENARIO_SMALL,
    benchmark,
    measure_time,
    assert_scalar_close,
    assert_series_close,
    assert_frame_close,
)


# ============================================================
# 1. make_synthetic_data 基础功能 / make_synthetic_data basics
# ============================================================

class TestMakeSyntheticData:
    """make_synthetic_data 生成器测试 / make_synthetic_data generator tests."""

    def test_output_types(self):
        """返回值类型正确 / Output types are correct."""
        factor, returns = make_synthetic_data()
        assert isinstance(factor, pd.Series)
        assert isinstance(returns, pd.Series)

    def test_multiindex_structure(self):
        """MultiIndex 结构正确 / MultiIndex structure is correct."""
        factor, returns = make_synthetic_data(n_days=50, n_symbols=20)
        assert factor.index.names == ["timestamp", "symbol"]
        assert returns.index.names == ["timestamp", "symbol"]
        assert len(factor) == 50 * 20
        assert len(returns) == 50 * 20

    def test_custom_params(self):
        """自定义参数生效 / Custom parameters take effect."""
        factor, returns = make_synthetic_data(n_days=10, n_symbols=5, seed=0, nan_frac=0.0, corr=0.5)
        timestamps = factor.index.get_level_values(0).unique()
        symbols = factor.index.get_level_values(1).unique()
        assert len(timestamps) == 10
        assert len(symbols) == 5
        assert factor.notna().all()
        assert returns.notna().all()

    def test_nan_injection(self):
        """NaN 注入比例正确 / NaN injection rate is correct."""
        factor, returns = make_synthetic_data(nan_frac=0.15, seed=99)
        nan_ratio_f = factor.isna().mean()
        nan_ratio_r = returns.isna().mean()
        # 注入比例应在合理范围内 (允许随机波动) / Rate should be within reasonable range
        assert 0.05 < nan_ratio_f < 0.25
        assert 0.05 < nan_ratio_r < 0.25

    def test_no_nan_when_frac_zero(self):
        """nan_frac=0 时无 NaN / No NaN when nan_frac=0."""
        factor, returns = make_synthetic_data(nan_frac=0.0)
        assert factor.notna().all()
        assert returns.notna().all()

    def test_correlation_direction(self):
        """相关方向正确 / Correlation direction is correct."""
        # 高正相关 / High positive correlation
        f1, r1 = make_synthetic_data(corr=0.95, nan_frac=0.0, n_days=500, n_symbols=50)
        df1 = pd.DataFrame({"f": f1, "r": r1}).dropna()
        ic = df1["f"].corr(df1["r"])
        assert ic > 0.8, f"Expected high positive IC, got {ic:.4f}"

        # 负相关 / Negative correlation
        f2, r2 = make_synthetic_data(corr=-0.8, nan_frac=0.0, n_days=500, n_symbols=50)
        df2 = pd.DataFrame({"f": f2, "r": r2}).dropna()
        ic2 = df2["f"].corr(df2["r"])
        assert ic2 < -0.6, f"Expected negative IC, got {ic2:.4f}"

    def test_reproducibility(self):
        """同 seed 可复现 / Same seed is reproducible."""
        f1, r1 = make_synthetic_data(seed=42)
        f2, r2 = make_synthetic_data(seed=42)
        assert_series_close(f1, f2, label="reproducibility_factor")
        assert_series_close(r1, r2, label="reproducibility_returns")

    def test_large_dataset(self):
        """大数据集维度正确 / Large dataset dimensions correct."""
        factor, returns = make_synthetic_data(n_days=500, n_symbols=100, nan_frac=0.02)
        assert len(factor) == 500 * 100
        assert len(returns) == 500 * 100


# ============================================================
# 2. iter_scenarios 场景遍历 / iter_scenarios iteration
# ============================================================

class TestIterScenarios:
    """iter_scenarios 场景遍历测试 / iter_scenarios iteration tests."""

    def test_all_scenarios_yielded(self):
        """6 个场景全部产出 / All 6 scenarios yielded."""
        ids = [sid for sid, _, _ in iter_scenarios()]
        assert set(ids) == set(SCENARIOS.keys())
        assert len(ids) == 6

    def test_each_scenario_types(self):
        """每个场景产出类型正确 / Each scenario yields correct types."""
        for sid, factor, returns in iter_scenarios():
            assert isinstance(factor, pd.Series), f"{sid}: factor not Series"
            assert isinstance(returns, pd.Series), f"{sid}: returns not Series"
            assert len(factor) > 0, f"{sid}: empty factor"
            assert len(returns) > 0, f"{sid}: empty returns"

    def test_large_scenario_dimensions(self):
        """large 场景维度正确 / large scenario dimensions correct."""
        _, factor, returns = next(
            (sid, f, r) for sid, f, r in iter_scenarios() if sid == SCENARIO_LARGE
        )
        assert len(factor) == 500 * 100

    def test_small_scenario_dimensions(self):
        """small 场景维度正确 / small scenario dimensions correct."""
        _, factor, returns = next(
            (sid, f, r) for sid, f, r in iter_scenarios() if sid == SCENARIO_SMALL
        )
        assert len(factor) == 10 * 5


# ============================================================
# 3. benchmark 计时 / benchmark timing
# ============================================================

class TestBenchmark:
    """benchmark 计时辅助测试 / benchmark timing helper tests."""

    def test_benchmark_runs(self, capsys):
        """benchmark 上下文管理器正常执行 / benchmark context manager executes."""
        import time
        with benchmark("test_sleep"):
            time.sleep(0.01)
        captured = capsys.readouterr()
        assert "[benchmark] test_sleep:" in captured.out

    def test_measure_time_returns_result(self):
        """measure_time 返回结果和耗时 / measure_time returns result and elapsed."""
        def _add(a, b):
            return a + b
        result, elapsed = measure_time(_add, 1, 2)
        assert result == 3
        assert elapsed >= 0

    def test_measure_time_n_runs(self):
        """measure_time n_runs 多次运行 / measure_time n_runs multiple runs."""
        counter = {"n": 0}
        def _inc():
            counter["n"] += 1
            return counter["n"]
        result, elapsed = measure_time(_inc, n_runs=5)
        assert result == 5
        assert counter["n"] == 5


# ============================================================
# 4. assert_scalar_close / assert_scalar_close
# ============================================================

class TestAssertScalarClose:
    """assert_scalar_close 断言测试 / assert_scalar_close assertion tests."""

    def test_equal_scalars(self):
        """相等标量通过 / Equal scalars pass."""
        assert_scalar_close(1.0, 1.0)

    def test_within_tolerance(self):
        """容差内通过 / Within tolerance pass."""
        assert_scalar_close(1.0, 1.0 + 1e-11, tol=1e-10)

    def test_exceeds_tolerance(self):
        """超容差失败 / Exceeds tolerance fails."""
        with pytest.raises(AssertionError, match="scalar mismatch"):
            assert_scalar_close(1.0, 1.0 + 1e-9, tol=1e-10)

    def test_both_nan_passes(self):
        """双方 NaN 通过 / Both NaN pass."""
        assert_scalar_close(np.nan, np.nan)

    def test_one_nan_fails(self):
        """单方 NaN 失败 / One NaN fails."""
        with pytest.raises(AssertionError, match="NaN"):
            assert_scalar_close(1.0, np.nan)

    def test_with_label(self):
        """带标签的错误信息 / Error message with label."""
        with pytest.raises(AssertionError, match="\\[my_label\\]"):
            assert_scalar_close(0.0, 1.0, label="my_label")

    def test_zero_tolerance(self):
        """零容差完全相等 / Zero tolerance exact match."""
        assert_scalar_close(1.0, 1.0, tol=0.0)
        with pytest.raises(AssertionError):
            assert_scalar_close(1.0, 1.0 + 1e-15, tol=0.0)


# ============================================================
# 5. assert_series_close / assert_series_close
# ============================================================

class TestAssertSeriesClose:
    """assert_series_close 断言测试 / assert_series_close assertion tests."""

    def test_identical_series(self):
        """相同 Series 通过 / Identical Series pass."""
        s = pd.Series([1.0, 2.0, 3.0])
        assert_series_close(s, s)

    def test_within_tolerance(self):
        """容差内通过 / Within tolerance pass."""
        s1 = pd.Series([1.0, 2.0, 3.0])
        s2 = pd.Series([1.0 + 1e-11, 2.0, 3.0])
        assert_series_close(s1, s2, tol=1e-10)

    def test_exceeds_tolerance(self):
        """超容差失败 / Exceeds tolerance fails."""
        s1 = pd.Series([1.0, 2.0, 3.0])
        s2 = pd.Series([1.0 + 1e-9, 2.0, 3.0])
        with pytest.raises(AssertionError, match="Series element mismatch"):
            assert_series_close(s1, s2, tol=1e-10)

    def test_both_nan_passes(self):
        """NaN 位置一致通过 / Consistent NaN positions pass."""
        s1 = pd.Series([1.0, np.nan, 3.0])
        s2 = pd.Series([1.0, np.nan, 3.0])
        assert_series_close(s1, s2)

    def test_nan_mismatch_fails(self):
        """NaN 位置不一致失败 / Inconsistent NaN positions fail."""
        s1 = pd.Series([1.0, np.nan, 3.0])
        s2 = pd.Series([1.0, 2.0, 3.0])
        with pytest.raises(AssertionError, match="NaN position mismatch"):
            assert_series_close(s1, s2)

    def test_length_mismatch(self):
        """长度不一致失败 / Length mismatch fails."""
        s1 = pd.Series([1.0, 2.0])
        s2 = pd.Series([1.0, 2.0, 3.0])
        with pytest.raises(AssertionError, match="length mismatch"):
            assert_series_close(s1, s2)

    def test_index_alignment(self):
        """索引对齐后比较 / Compare after index alignment."""
        s1 = pd.Series([1.0, 2.0, 3.0], index=[0, 1, 2])
        s2 = pd.Series([1.0, 2.0, 3.0], index=[0, 1, 2])
        assert_series_close(s1, s2)

    def test_with_label(self):
        """带标签的错误信息 / Error message with label."""
        s1 = pd.Series([1.0, 2.0])
        s2 = pd.Series([1.0, 3.0])
        with pytest.raises(AssertionError, match="\\[my_series\\]"):
            assert_series_close(s1, s2, label="my_series")

    def test_multiindex_series(self):
        """MultiIndex Series 比较 / MultiIndex Series comparison."""
        idx = pd.MultiIndex.from_product(
            [["2025-01-01", "2025-01-02"], ["A", "B"]],
            names=["timestamp", "symbol"],
        )
        s1 = pd.Series([1.0, 2.0, 3.0, 4.0], index=idx)
        s2 = pd.Series([1.0, 2.0, 3.0, 4.0], index=idx)
        assert_series_close(s1, s2)


# ============================================================
# 6. assert_frame_close / assert_frame_close
# ============================================================

class TestAssertFrameClose:
    """assert_frame_close 断言测试 / assert_frame_close assertion tests."""

    def test_identical_frame(self):
        """相同 DataFrame 通过 / Identical DataFrame pass."""
        df = pd.DataFrame({"a": [1.0, 2.0], "b": [3.0, 4.0]})
        assert_frame_close(df, df)

    def test_within_tolerance(self):
        """容差内通过 / Within tolerance pass."""
        df1 = pd.DataFrame({"a": [1.0, 2.0], "b": [3.0, 4.0]})
        df2 = pd.DataFrame({"a": [1.0 + 1e-11, 2.0], "b": [3.0, 4.0]})
        assert_frame_close(df1, df2, tol=1e-10)

    def test_exceeds_tolerance(self):
        """超容差失败 / Exceeds tolerance fails."""
        df1 = pd.DataFrame({"a": [1.0, 2.0], "b": [3.0, 4.0]})
        df2 = pd.DataFrame({"a": [1.0 + 1e-9, 2.0], "b": [3.0, 4.0]})
        with pytest.raises(AssertionError, match="DataFrame element mismatch"):
            assert_frame_close(df1, df2, tol=1e-10)

    def test_both_nan_passes(self):
        """NaN 位置一致通过 / Consistent NaN positions pass."""
        df1 = pd.DataFrame({"a": [1.0, np.nan], "b": [3.0, 4.0]})
        df2 = pd.DataFrame({"a": [1.0, np.nan], "b": [3.0, 4.0]})
        assert_frame_close(df1, df2)

    def test_nan_mismatch_fails(self):
        """NaN 位置不一致失败 / Inconsistent NaN positions fail."""
        df1 = pd.DataFrame({"a": [1.0, np.nan], "b": [3.0, 4.0]})
        df2 = pd.DataFrame({"a": [1.0, 2.0], "b": [3.0, 4.0]})
        with pytest.raises(AssertionError, match="NaN position mismatch"):
            assert_frame_close(df1, df2)

    def test_shape_mismatch(self):
        """形状不一致失败 / Shape mismatch fails."""
        df1 = pd.DataFrame({"a": [1.0, 2.0]})
        df2 = pd.DataFrame({"a": [1.0, 2.0], "b": [3.0, 4.0]})
        with pytest.raises(AssertionError, match="shape mismatch"):
            assert_frame_close(df1, df2)

    def test_with_label(self):
        """带标签的错误信息 / Error message with label."""
        df1 = pd.DataFrame({"a": [1.0, 2.0]})
        df2 = pd.DataFrame({"a": [1.0, 3.0]})
        with pytest.raises(AssertionError, match="\\[my_frame\\]"):
            assert_frame_close(df1, df2, label="my_frame")
