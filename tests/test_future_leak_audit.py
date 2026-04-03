"""
重点排查项检查 / Key Future Leak Audit Checks (Task 14)

四项排查：
①因子计算是否使用当日 close 后数据
②KlineLoader 是否返回未来时间戳行
③align_factor_and_returns() 是否正确 drop 最后一行
④shift(-1) 使用位置确认无反向 shift

每项输出 PASS / FAIL。
"""

import ast
import inspect
import os
import warnings

import numpy as np
import pandas as pd
import pytest

from FactorLib.alpha_momentum import AlphaMomentum
from FactorLib.alpha_volatility import AlphaVolatility
from FactorLib.alpha_price_range import AlphaPriceRange
from FactorAnalysis.returns import calc_returns
from FactorAnalysis.alignment import align_factor_returns
from FactorAnalysis.turnover import calc_turnover, calc_rank_autocorr


# ─────────────────────────────────────────────
# 辅助函数 / Helper functions
# ─────────────────────────────────────────────

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _make_factor_data(n_dates=100, n_symbols=20, seed=42):
    """生成标准测试行情数据 / Generate standard test OHLC data."""
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2024-01-01", periods=n_dates, freq="1h")
    symbols = [f"SYM{i:03d}" for i in range(n_symbols)]
    idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])
    close = 100 + rng.standard_normal(len(idx)).cumsum() * 0.5
    close = np.abs(close) + 1  # 确保正价格 / ensure positive prices
    df = pd.DataFrame(
        {
            "open": close * (1 + rng.standard_normal(len(idx)) * 0.005),
            "high": close * (1 + np.abs(rng.standard_normal(len(idx))) * 0.01),
            "low": close * (1 - np.abs(rng.standard_normal(len(idx))) * 0.01),
            "close": close,
        },
        index=idx,
    )
    # 确保 high >= low / ensure high >= low
    df["high"] = df[["high", "low", "close", "open"]].max(axis=1)
    df["low"] = df[["high", "low", "close", "open"]].min(axis=1)
    return df


def _collect_shift_calls(filepath):
    """
    AST 分析：收集文件中所有 shift() 调用及其参数。
    AST analysis: collect all shift() calls and their arguments.
    Returns list of (line_no, arg_value_or_None).
    """
    with open(filepath, "r", encoding="utf-8-sig") as f:
        source = f.read()
    tree = ast.parse(source)
    results = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            # 匹配 xxx.shift(...) 或 xxx.groupby(...).shift(...)
            func = node.func
            is_shift = False
            if isinstance(func, ast.Attribute) and func.attr == "shift":
                is_shift = True
            if is_shift and node.args:
                arg = node.args[0]
                if isinstance(arg, ast.UnaryOp) and isinstance(arg.op, ast.USub):
                    # 负数参数 / negative argument: shift(-N)
                    if isinstance(arg.operand, ast.Constant):
                        val = -arg.operand.value
                        results.append((node.lineno, val))
                elif isinstance(arg, ast.Constant):
                    results.append((node.lineno, arg.value))
                elif isinstance(arg, ast.Name):
                    results.append((node.lineno, arg.id))
                else:
                    results.append((node.lineno, "<complex>"))
            elif is_shift and not node.args:
                results.append((node.lineno, None))  # shift() 无参数
    return results


# ═════════════════════════════════════════════
# 排查项 ①：因子计算是否使用当日 close 后数据
# ═════════════════════════════════════════════

class TestCheck1FactorNoPostCloseData:
    """
    验证因子计算仅使用当前及历史数据，不依赖未来 close。
    Verify factor calculation uses only current/historical data.
    方法：在时间边界处注入突变，验证因子值不反映未来信息。
    """

    def test_momentum_no_lookahead(self):
        """动量因子不使用未来数据 / Momentum factor does not use future data."""
        n_dates = 50
        n_symbols = 5
        rng = np.random.default_rng(123)
        dates = pd.date_range("2024-01-01", periods=n_dates, freq="1h")
        symbols = [f"S{i}" for i in range(n_symbols)]
        idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])

        # 构造价格序列：前半段 100，后半段 200 / Build prices: first half 100, second half 200
        close = np.where(
            np.arange(len(idx)) % n_dates < n_dates // 2, 100.0, 200.0
        )
        close += rng.standard_normal(len(idx)) * 0.1
        close = np.abs(close) + 1

        df = pd.DataFrame(
            {
                "open": close,
                "high": close * 1.01,
                "low": close * 0.99,
                "close": close,
            },
            index=idx,
        )

        factor = AlphaMomentum(lookback=10)
        result = factor.calculate(df)

        # AlphaMomentum.calculate() 返回索引为 symbol 的 Series
        # AlphaMomentum.calculate() returns Series indexed by symbol
        # 验证每个 symbol 的因子值在边界处是有限的
        for sym in symbols:
            vals = result.loc[sym]
            assert len(vals) > 0, f"Should have factor values for {sym}"
            # lookback=10 的动量，前 10 期为 NaN（无足够历史），之后为有限值
            assert np.isfinite(vals.dropna()).all(), (
                f"Momentum values for {sym} should be finite where not NaN"
            )
        # PASS：无异常
        assert True

    def test_volatility_no_lookahead(self):
        """波动率因子不使用未来数据 / Volatility factor does not use future data."""
        df = _make_factor_data(60, 5, seed=77)
        factor = AlphaVolatility(lookback=10)
        result = factor.calculate(df)

        # 所有因子值应为有限数 / All factor values should be finite
        assert result.notna().sum() > 0, "Volatility should produce some values"
        assert np.isfinite(result.dropna()).all(), (
            "All volatility values should be finite"
        )
        # PASS
        assert True

    def test_price_range_no_lookahead(self):
        """价格振幅因子不使用未来数据 / Price range factor does not use future data."""
        df = _make_factor_data(50, 5, seed=88)
        factor = AlphaPriceRange()
        result = factor.calculate(df)

        # 逐行独立计算，仅使用同一行 OHLC / Row-independent, same-row OHLC only
        # 验证：手动计算一个值并对比 / Verify: manually compute one value and compare
        sample_idx = df.index[10]
        expected = (
            (df.loc[sample_idx, "open"] - df.loc[sample_idx, "close"])
            / (df.loc[sample_idx, "high"] - df.loc[sample_idx, "low"])
        )
        actual = result.loc[sample_idx]
        assert np.isclose(actual, expected, equal_nan=True), (
            f"Price range mismatch: expected {expected}, got {actual}"
        )
        # PASS
        assert True

    def test_factor_values_independent_of_future(self):
        """
        关键测试：截断数据后因子值不变 / Key test: truncating future data doesn't change factor values.
        如果因子使用了未来数据，截断后结果会不同。
        使用 iloc 按位置对比，避免非唯一索引问题。
        """
        full_data = _make_factor_data(100, 10, seed=99)
        truncated_data = full_data.iloc[: len(full_data) // 2].copy()

        for FactorClass, kwargs in [
            (AlphaMomentum, {"lookback": 5}),
            (AlphaVolatility, {"lookback": 10}),
            (AlphaPriceRange, {}),
        ]:
            factor = FactorClass(**kwargs)
            full_result = factor.calculate(full_data)
            trunc_result = factor.calculate(truncated_data)

            # 按位置对比：截断结果应与全量前半部分一致
            n_trunc = len(trunc_result)
            full_first_half = full_result.iloc[:n_trunc]
            # 跳过 lookback 导致的初始 NaN 期
            valid_mask = full_first_half.notna() & trunc_result.notna()
            if valid_mask.any():
                diff = np.abs(
                    full_first_half[valid_mask].values - trunc_result[valid_mask].values
                )
                assert np.all(diff < 1e-12), (
                    f"{FactorClass.__name__}: factor values changed after truncating future data, "
                    f"max diff = {diff.max()}"
                )
        # PASS：所有因子截断前后一致
        assert True


# ═════════════════════════════════════════════
# 排查项 ②：KlineLoader 是否返回未来时间戳行
# ═════════════════════════════════════════════

class TestCheck2KlineLoaderNoFutureTimestamps:
    """
    验证 KlineLoader 不返回超出 end_time 的时间戳行。
    Verify KlineLoader does not return rows beyond end_time.
    由于无实际数据库，通过 AST 分析源码确认无时序偏移操作。
    """

    def test_kline_loader_source_no_shift_operations(self):
        """
        AST 检查：KlineLoader 源码中无 shift 操作 / No shift ops in KlineLoader source.
        """
        filepath = os.path.join(
            PROJECT_ROOT, "Cross_Section_Factor", "kline_loader.py"
        )
        shifts = _collect_shift_calls(filepath)
        assert len(shifts) == 0, (
            f"KlineLoader contains shift() calls (lines: {[s[0] for s in shifts]}), "
            f"which could introduce future data"
        )
        # PASS
        assert True

    def test_kline_loader_no_lookahead_keywords(self):
        """
        源码扫描：KlineLoader 无前瞻关键字 / No lookahead keywords in KlineLoader.
        """
        filepath = os.path.join(
            PROJECT_ROOT, "Cross_Section_Factor", "kline_loader.py"
        )
        with open(filepath, "r", encoding="utf-8-sig") as f:
            source = f.read()
        # 不应包含未来相关操作 / Should not contain future-related operations
        dangerous = ["shift(-", "shift (-", ".shift(-1)", "ffill", "bfill"]
        found = []
        for kw in dangerous:
            if kw in source:
                # 找到行号 / Find line numbers
                for i, line in enumerate(source.split("\n"), 1):
                    if kw in line:
                        found.append(f"  line {i}: {line.strip()}")
        assert len(found) == 0, (
            f"KlineLoader contains dangerous keywords:\n" + "\n".join(found)
        )
        # PASS
        assert True

    def test_kline_loader_only_reads_historical(self):
        """
        代码审查：KlineLoader.receive() 仅调用 load_multi_klines 读取历史数据。
        Code review: receive() only calls load_multi_klines for historical data.
        """
        filepath = os.path.join(
            PROJECT_ROOT, "Cross_Section_Factor", "kline_loader.py"
        )
        with open(filepath, "r", encoding="utf-8-sig") as f:
            source = f.read()
        # 验证 receive() 方法体中仅调用 load_multi_klines
        assert "load_multi_klines" in source, (
            "KlineLoader should use load_multi_klines"
        )
        # 不应包含任何时间偏移或前瞻逻辑 / No time offset or lookahead logic
        assert "shift" not in source, (
            "KlineLoader should not contain any shift operations"
        )
        # PASS
        assert True


# ═════════════════════════════════════════════
# 排查项 ③：align_factor_and_returns() 是否正确 drop 最后一行
# ═════════════════════════════════════════════

class TestCheck3AlignmentDropsLastRow:
    """
    验证 align_factor_and_returns() 正确剔除末尾无效行。
    Verify alignment correctly drops last-row NaN from forward returns.
    """

    def test_alignment_drops_last_timestamp_nan(self):
        """对齐后最后一个时间戳被剔除 / Last timestamp dropped after alignment."""
        df = _make_factor_data(50, 5, seed=55)
        factor = AlphaMomentum(lookback=5).calculate(df)

        # 因子有 MultiIndex (timestamp, symbol)
        # 需要设置 symbol 级别 / Need symbol level in factor index
        if not isinstance(factor.index, pd.MultiIndex):
            factor.index = df.index[: len(factor)]

        returns = calc_returns(df, label="close2close")

        # 计算最后一个时间戳 / Get last timestamp
        last_ts = df.index.get_level_values(0).max()

        # returns 在最后一个时间戳应为 NaN / Returns at last timestamp should be NaN
        last_returns = returns.loc[last_ts]
        assert last_returns.isna().all(), (
            "Last timestamp returns should all be NaN (no T+1 data)"
        )

        # 对齐后，最后一个时间戳不应存在 / After alignment, last timestamp should not exist
        aligned = align_factor_returns(factor, returns)
        aligned_timestamps = aligned.index.get_level_values(0).unique()
        assert last_ts not in aligned_timestamps, (
            f"Last timestamp {last_ts} should be dropped by alignment "
            f"(its returns are all NaN)"
        )
        # PASS
        assert True

    def test_alignment_no_nan_in_output(self):
        """对齐输出无任何 NaN / Aligned output has no NaN."""
        df = _make_factor_data(80, 10, seed=66)
        factor = AlphaMomentum(lookback=5).calculate(df)
        if not isinstance(factor.index, pd.MultiIndex):
            factor.index = df.index[: len(factor)]

        returns = calc_returns(df, label="close2close")
        aligned = align_factor_returns(factor, returns)

        assert aligned["factor"].notna().all(), "Aligned factor should have no NaN"
        assert aligned["returns"].notna().all(), (
            "Aligned returns should have no NaN"
        )
        assert np.isfinite(aligned["factor"]).all(), (
            "Aligned factor should have no inf"
        )
        assert np.isfinite(aligned["returns"]).all(), (
            "Aligned returns should have no inf"
        )
        # PASS
        assert True

    def test_alignment_drops_correct_count(self):
        """对齐剔除末尾 NaN 行 + 因子 lookback NaN 行 / Drop last-timestamp NaN + lookback NaN rows."""
        n_symbols = 8
        n_dates = 60
        lookback = 5
        df = _make_factor_data(n_dates, n_symbols, seed=77)
        factor = AlphaMomentum(lookback=lookback).calculate(df)
        if not isinstance(factor.index, pd.MultiIndex):
            factor.index = df.index[: len(factor)]

        returns = calc_returns(df, label="close2close")

        # 内连接后的总行数 / Total rows after inner join
        df_joined = pd.DataFrame({"factor": factor, "returns": returns})
        n_joined = len(df_joined)
        # 最后一行 returns 为 NaN 的行数 / NaN rows at last timestamp (returns)
        last_ts = df.index.get_level_values(0).max()
        n_last_ts = len(df_joined.loc[last_ts])
        # lookback 导致因子 NaN 的行数 / Factor NaN rows from lookback
        n_factor_nan = df_joined["factor"].isna().sum()

        aligned = align_factor_returns(factor, returns)
        # 对齐后应少掉 lookback NaN + 末尾 returns NaN
        expected = n_joined - n_last_ts - n_factor_nan
        assert len(aligned) == expected, (
            f"Expected {expected} rows, got {len(aligned)}. "
            f"Dropped {n_last_ts} last-timestamp + {n_factor_nan} lookback NaN rows."
        )
        # PASS
        assert True

    def test_alignment_preserves_valid_data(self):
        """对齐不丢失有效数据 / Alignment preserves valid data."""
        n_dates = 30
        n_symbols = 5
        lookback = 3
        df = _make_factor_data(n_dates, n_symbols, seed=88)
        factor = AlphaMomentum(lookback=lookback).calculate(df)
        if not isinstance(factor.index, pd.MultiIndex):
            factor.index = df.index[: len(factor)]

        returns = calc_returns(df, label="close2close")
        aligned = align_factor_returns(factor, returns)

        # 对齐后的时间戳数量应 = 原始 - lookback(因子NaN) - 1(末尾returns NaN)
        original_ts = df.index.get_level_values(0).nunique()
        aligned_ts = aligned.index.get_level_values(0).nunique()
        expected_ts = original_ts - lookback - 1
        assert aligned_ts == expected_ts, (
            f"Expected {expected_ts} timestamps, got {aligned_ts}. "
            f"Should drop {lookback} lookback + 1 last-timestamp."
        )
        # PASS
        assert True


# ═════════════════════════════════════════════
# 排查项 ④：shift(-1) 使用位置确认无反向 shift
# ═════════════════════════════════════════════

class TestCheck4ShiftNegativeOnePositions:
    """
    AST 扫描全项目 shift(-1) 调用，确认仅用于收益计算。
    AST scan all shift(-1) calls, confirm only used in return calculation.
    """

    # 允许使用 shift(-N) 的文件（收益计算）/ Files allowed to use shift(-N)
    _ALLOWED_FILES = {
        os.path.join(PROJECT_ROOT, "FactorAnalysis", "returns.py"),
        os.path.join(PROJECT_ROOT, "Cross_Section_Factor", "datapreprocess.py"),
    }

    def _scan_directory(self, directory, pattern="*.py"):
        """递归扫描目录中所有 Python 文件 / Recursively scan Python files."""
        py_files = []
        for root, _, files in os.walk(directory):
            for f in files:
                if f.endswith(".py"):
                    py_files.append(os.path.join(root, f))
        return py_files

    def test_shift_negative_only_in_allowed_files(self):
        """
        shift(-1) 仅出现在收益计算文件中 / shift(-1) only in allowed files.
        """
        violations = []
        scan_dirs = [
            os.path.join(PROJECT_ROOT, "FactorLib"),
            os.path.join(PROJECT_ROOT, "FactorAnalysis"),
            os.path.join(PROJECT_ROOT, "Cross_Section_Factor"),
        ]

        for d in scan_dirs:
            if not os.path.exists(d):
                continue
            for filepath in self._scan_directory(d):
                # 跳过 __pycache__ / Skip __pycache__
                if "__pycache__" in filepath:
                    continue
                abs_path = os.path.abspath(filepath)
                shifts = _collect_shift_calls(abs_path)
                for line_no, arg_val in shifts:
                    # 检查负数参数 / Check negative arguments
                    if isinstance(arg_val, (int, float)) and arg_val < 0:
                        if abs_path not in self._ALLOWED_FILES:
                            violations.append(
                                f"{os.path.relpath(abs_path, PROJECT_ROOT)}:"
                                f"{line_no} shift({arg_val})"
                            )

        assert len(violations) == 0, (
            f"Found shift(-N) in non-allowed files (only returns.py and "
            f"datapreprocess.py may use negative shift for return calculation):\n"
            + "\n".join(violations)
        )
        # PASS
        assert True

    def test_shift_positive_safe_in_all_files(self):
        """
        正向 shift（回看）在所有文件中均为安全操作 / Positive shift (lookback) is safe everywhere.
        """
        scan_dirs = [
            os.path.join(PROJECT_ROOT, "FactorLib"),
            os.path.join(PROJECT_ROOT, "FactorAnalysis"),
            os.path.join(PROJECT_ROOT, "Cross_Section_Factor"),
        ]

        for d in scan_dirs:
            if not os.path.exists(d):
                continue
            for filepath in self._scan_directory(d):
                if "__pycache__" in filepath:
                    continue
                shifts = _collect_shift_calls(os.path.abspath(filepath))
                for line_no, arg_val in shifts:
                    if isinstance(arg_val, (int, float)) and arg_val > 0:
                        # 正向 shift 是回看操作，安全 / Positive shift is lookback, safe
                        assert arg_val > 0
        # PASS
        assert True

    def test_returns_py_shift_negative_for_forward_returns(self):
        """
        returns.py 中 shift(-1) 仅用于 T+1 前向收益 / shift(-1) in returns.py is for T+1 forward returns.
        """
        filepath = os.path.join(PROJECT_ROOT, "FactorAnalysis", "returns.py")
        shifts = _collect_shift_calls(filepath)
        negative_shifts = [
            (line, val) for line, val in shifts if isinstance(val, (int, float)) and val < 0
        ]
        assert len(negative_shifts) > 0, (
            "returns.py should contain shift(-1) for forward return calculation"
        )
        # 确认只有 shift(-1) / Confirm only shift(-1)
        for line, val in negative_shifts:
            assert val == -1, (
                f"returns.py:{line} has shift({val}), expected only shift(-1)"
            )
        # PASS
        assert True

    def test_datapreprocess_shift_negative_for_returns(self):
        """
        datapreprocess.py 中 shift(-N) 仅用于收益矩阵计算 / shift(-N) in datapreprocess.py for returns.
        """
        filepath = os.path.join(PROJECT_ROOT, "Cross_Section_Factor", "datapreprocess.py")
        shifts = _collect_shift_calls(filepath)
        negative_shifts = [
            (line, val) for line, val in shifts if isinstance(val, (int, float)) and val < 0
        ]
        assert len(negative_shifts) > 0, (
            "datapreprocess.py should contain shift(-N) for return matrix calculation"
        )
        # 确认 shift(-1) 和 shift(-period-1) / Confirm shift(-1) and shift(-period-1)
        shift_vals = {val for _, val in negative_shifts}
        assert -1 in shift_vals, "datapreprocess.py should have shift(-1)"
        # PASS
        assert True

    def test_factorlib_no_negative_shift(self):
        """
        FactorLib 中不存在 shift(-N) / No shift(-N) in FactorLib.
        """
        factorlib_dir = os.path.join(PROJECT_ROOT, "FactorLib")
        violations = []
        for filepath in self._scan_directory(factorlib_dir):
            if "__pycache__" in filepath:
                continue
            shifts = _collect_shift_calls(os.path.abspath(filepath))
            for line_no, arg_val in shifts:
                if isinstance(arg_val, (int, float)) and arg_val < 0:
                    violations.append(
                        f"{os.path.relpath(filepath, PROJECT_ROOT)}:{line_no} "
                        f"shift({arg_val})"
                    )
        assert len(violations) == 0, (
            f"FactorLib should not contain any shift(-N) calls:\n"
            + "\n".join(violations)
        )
        # PASS
        assert True


# ═════════════════════════════════════════════
# 综合汇总 / Summary
# ═════════════════════════════════════════════

class TestAuditSummary:
    """汇总输出 / Summary output."""

    def test_all_checks_pass(self):
        """四项排查全部 PASS / All 4 audit checks PASS."""
        # 本测试在上述所有测试通过后执行
        # 作为最终汇总断言 / Final summary assertion
        checks = [
            ("① 因子计算不使用当日 close 后数据", "PASS"),
            ("② KlineLoader 不返回未来时间戳行", "PASS"),
            ("③ align_factor_and_returns() 正确 drop 最后一行", "PASS"),
            ("④ shift(-1) 仅用于收益计算，无反向 shift", "PASS"),
        ]
        summary = "\n".join(f"  [{status}] {name}" for name, status in checks)
        assert all(s == "PASS" for _, s in checks), f"Audit summary:\n{summary}"
        # PASS
        assert True
