"""
scripts/check_future_leak.py — 未来函数自动化检测脚本 / Automated future leak detection script

对因子投研管道执行完整的未来函数泄露检测，包含静态代码扫描和动态数据验证。
Performs comprehensive future leak detection on the factor research pipeline,
including static code scanning and dynamic data verification.

检测项 / Checks:
1. FactorLib 中无 shift(-N) 操作 / No shift(-N) in FactorLib
2. shift(-N) 仅出现在允许的文件中 / shift(-N) only in allowed files
3. KlineLoader 源码无 shift 操作 / No shift operations in KlineLoader
4. 因子值不依赖未来数据 / Factor values independent of future data
5. 因子-收益时间对齐正确 / Factor-returns time alignment is correct
6. 最后一期收益为 NaN 且被对齐剔除 / Last period NaN dropped by alignment

用法 / Usage:
    python scripts/check_future_leak.py                          # 使用默认参数
    python scripts/check_future_leak.py --factor AlphaMomentum    # 指定因子
    python scripts/check_future_leak.py --report-path report.md   # 输出到文件
"""

import ast
import argparse
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

# 添加项目根目录到路径 / Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

for submodule in ["CryptoDataProviders", "CryptoDB_feather"]:
    submodule_path = os.path.join(project_root, submodule)
    if submodule_path not in sys.path:
        sys.path.insert(0, submodule_path)


# ── 数据结构 / Data structures ────────────────────────────────────────────────

@dataclass
class CheckResult:
    """单项检测结果 / Single check result."""
    name: str                           # 检测项名称 / Check name
    status: str                         # PASS / FAIL
    details: str = ""                   # 详细说明 / Details
    file: str = ""                      # 相关文件 / Related file
    line: int = 0                       # 相关行号 / Related line number


@dataclass
class DetectionReport:
    """检测报告 / Detection report."""
    checks: List[CheckResult] = field(default_factory=list)
    start_time: str = ""
    end_time: str = ""
    elapsed: float = 0.0

    @property
    def all_passed(self) -> bool:
        return all(c.status == "PASS" for c in self.checks)

    @property
    def n_pass(self) -> int:
        return sum(1 for c in self.checks if c.status == "PASS")

    @property
    def n_fail(self) -> int:
        return sum(1 for c in self.checks if c.status == "FAIL")


# ── 允许使用 shift(-N) 的文件白名单 / Allowed shift(-N) file whitelist ───────

# 这些文件中的 shift(-N) 用于收益计算，是标准设计，不算泄露
# shift(-N) in these files is for return calculation (standard design, not a leak)
_ALLOWED_SHIFT_FILES = {
    "returns.py",
    "datapreprocess.py",
}


# ── 静态代码扫描 / Static code scanning ───────────────────────────────────────

def _find_python_files(directory: str) -> List[Path]:
    """
    递归查找目录下所有 .py 文件 / Recursively find all .py files in directory.
    """
    return list(Path(directory).rglob("*.py"))


def _scan_shift_negative(file_path: Path) -> List[Tuple[int, str]]:
    """
    使用 AST 扫描文件中所有 shift(-N) 调用，返回 [(行号, 代码片段)]。
    Use AST to scan all shift(-N) calls in file, return [(line_no, code_snippet)].
    """
    results = []
    try:
        source = file_path.read_text(encoding="utf-8")
        tree = ast.parse(source)
    except (SyntaxError, UnicodeDecodeError):
        return results

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        # 匹配 expr.shift(n) 形式 / Match expr.shift(n) pattern
        if (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "shift"
            and len(node.args) == 1
            and isinstance(node.args[0], (ast.UnaryOp, ast.Constant))
        ):
            # 提取 shift 参数值 / Extract shift argument value
            if isinstance(node.args[0], ast.UnaryOp) and isinstance(
                node.args[0].op, ast.USub
            ):
                if isinstance(node.args[0].operand, ast.Constant):
                    shift_val = -node.args[0].operand.value
                else:
                    continue
            elif isinstance(node.args[0], ast.Constant):
                shift_val = node.args[0].value
            else:
                continue

            if shift_val < 0:
                snippet = ast.get_source_segment(source, node) or ""
                results.append((node.lineno, snippet))

    return results


def _scan_shift_negative_regex(file_path: Path) -> List[Tuple[int, str]]:
    """
    使用正则扫描文件中 shift(-N) 作为补充（AST 可能遗漏动态调用）。
    Regex scan as fallback for dynamic shift(-N) calls that AST may miss.
    """
    results = []
    try:
        source = file_path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return results

    # 匹配 .shift(-N) 模式 / Match .shift(-N) pattern
    pattern = r"\.shift\(\s*-\s*(\d+)\s*\)"
    for match in re.finditer(pattern, source):
        line_no = source[: match.start()].count("\n") + 1
        results.append((line_no, match.group(0)))

    return results


def check_no_shift_in_factorlib() -> List[CheckResult]:
    """
    检测 1: FactorLib 中不存在 shift(-N) 操作。
    Check 1: No shift(-N) operations in FactorLib.
    """
    results: List[CheckResult] = []
    factorlib_dir = os.path.join(project_root, "FactorLib")

    if not os.path.isdir(factorlib_dir):
        results.append(CheckResult(
            name="FactorLib 无 shift(-N)",
            status="FAIL",
            details=f"FactorLib 目录不存在: {factorlib_dir}",
        ))
        return results

    all_violations: List[Tuple[str, int, str]] = []

    for py_file in _find_python_files(factorlib_dir):
        # AST 扫描 / AST scan
        violations = _scan_shift_negative(py_file)
        for line_no, snippet in violations:
            all_violations.append((str(py_file.relative_to(project_root)), line_no, snippet))

        # 正则补充扫描 / Regex fallback scan
        regex_violations = _scan_shift_negative_regex(py_file)
        for line_no, snippet in regex_violations:
            # 去重：排除已被 AST 扫描捕获的 / Deduplicate: exclude already captured by AST
            if not any(v[0] == str(py_file.relative_to(project_root)) and v[1] == line_no
                       for v in all_violations):
                all_violations.append((str(py_file.relative_to(project_root)), line_no, snippet))

    if all_violations:
        detail_lines = [f"  - {f}:{line}: {snippet}" for f, line, snippet in all_violations]
        results.append(CheckResult(
            name="FactorLib 无 shift(-N)",
            status="FAIL",
            details="FactorLib 中发现 shift(-N) 操作:\n" + "\n".join(detail_lines),
        ))
    else:
        results.append(CheckResult(
            name="FactorLib 无 shift(-N)",
            status="PASS",
            details="FactorLib 中未发现任何 shift(-N) 操作",
        ))

    return results


def check_shift_only_in_allowed_files() -> List[CheckResult]:
    """
    检测 2: shift(-N) 仅出现在允许的文件中（returns.py, datapreprocess.py）。
    Check 2: shift(-N) only appears in allowed files.
    """
    results: List[CheckResult] = []
    scan_dirs = ["FactorLib", "FactorAnalysis", "Cross_Section_Factor"]
    violations: List[Tuple[str, int, str]] = []

    for scan_dir in scan_dirs:
        dir_path = os.path.join(project_root, scan_dir)
        if not os.path.isdir(dir_path):
            continue

        for py_file in _find_python_files(dir_path):
            rel_path = py_file.relative_to(project_root)
            file_name = rel_path.name

            # 允许的文件跳过检测 / Skip allowed files
            if file_name in _ALLOWED_SHIFT_FILES:
                continue

            shift_calls = _scan_shift_negative(py_file)
            shift_calls += _scan_shift_negative_regex(py_file)

            # 去重 / Deduplicate
            seen = set()
            for line_no, snippet in shift_calls:
                key = (str(rel_path), line_no)
                if key not in seen:
                    seen.add(key)
                    violations.append((str(rel_path), line_no, snippet))

    if violations:
        detail_lines = [f"  - {f}:{line}: {snippet}" for f, line, snippet in violations]
        results.append(CheckResult(
            name="shift(-N) 仅在允许文件中",
            status="FAIL",
            details="以下非允许文件中发现 shift(-N):\n" + "\n".join(detail_lines),
        ))
    else:
        results.append(CheckResult(
            name="shift(-N) 仅在允许文件中",
            status="PASS",
            details="shift(-N) 仅出现在允许的文件中: " + ", ".join(sorted(_ALLOWED_SHIFT_FILES)),
        ))

    return results


def check_kline_loader_no_shift() -> List[CheckResult]:
    """
    检测 3: KlineLoader 源码中不存在 shift 操作。
    Check 3: No shift operations in KlineLoader source code.
    """
    results: List[CheckResult] = []
    loader_path = os.path.join(project_root, "Cross_Section_Factor", "kline_loader.py")

    if not os.path.isfile(loader_path):
        results.append(CheckResult(
            name="KlineLoader 无 shift 操作",
            status="FAIL",
            details=f"kline_loader.py 不存在: {loader_path}",
        ))
        return results

    source = Path(loader_path).read_text(encoding="utf-8")

    # 检查 AST 级别的 shift 调用 / Check AST-level shift calls
    tree = ast.parse(source)
    shift_found = False
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr == "shift":
                shift_found = True
                snippet = ast.get_source_segment(source, node) or ""
                results.append(CheckResult(
                    name="KlineLoader 无 shift 操作",
                    status="FAIL",
                    details=f"kline_loader.py 中发现 shift 调用: {snippet}",
                    file="Cross_Section_Factor/kline_loader.py",
                    line=node.lineno,
                ))
                break

    if not shift_found:
        # 正则补充 / Regex fallback
        if re.search(r"\.shift\s*\(", source):
            results.append(CheckResult(
                name="KlineLoader 无 shift 操作",
                status="FAIL",
                details="kline_loader.py 中发现 .shift() 正则匹配",
                file="Cross_Section_Factor/kline_loader.py",
            ))
        else:
            results.append(CheckResult(
                name="KlineLoader 无 shift 操作",
                status="PASS",
                details="kline_loader.py 中未发现任何 shift 操作",
            ))

    return results


# ── 动态数据验证 / Dynamic data verification ──────────────────────────────────

def _build_factor_multiindex(factor_raw, data):
    """
    将因子值从 RangeIndex 转换为 MultiIndex (timestamp, symbol)。
    Convert factor values from RangeIndex to MultiIndex (timestamp, symbol).
    """
    import pandas as pd

    n = len(factor_raw)
    return pd.Series(
        factor_raw.values[:n],
        index=pd.MultiIndex.from_arrays(
            [data["timestamp"].values[:n], data["symbol"].values[:n]],
            names=["timestamp", "symbol"],
        ),
        name=factor_raw.name,
    )


def check_factor_independence(
    data, factor_name: str, factor_inst,
) -> List[CheckResult]:
    """
    检测 4: 因子值不依赖未来数据——截断未来数据后因子值应完全一致。
    Check 4: Factor values don't depend on future data —
    truncating future data should produce identical factor values.
    """
    import numpy as np

    results: List[CheckResult] = []

    # 使用完整数据计算因子 / Compute factor with full data
    factor_full = factor_inst.calculate(data)

    # 截断数据：去掉最后 10% 的行 / Truncate data: remove last 10% rows
    n_total = len(data)
    n_truncate = max(1, n_total // 10)
    data_truncated = data.iloc[: n_total - n_truncate].copy()

    # 用截断数据计算因子 / Compute factor with truncated data
    factor_truncated = factor_inst.calculate(data_truncated)

    # 比较重叠部分 / Compare overlapping portion
    n_overlap = len(factor_truncated)
    diff = np.abs(factor_full.values[:n_overlap] - factor_truncated.values)
    max_diff = np.nanmax(diff) if diff.size > 0 else 0.0

    tolerance = 1e-12
    if max_diff < tolerance:
        results.append(CheckResult(
            name=f"因子独立性 ({factor_name})",
            status="PASS",
            details=(
                f"截断 {n_truncate} 行后因子值差异 < {tolerance} "
                f"(max_diff={max_diff:.2e})"
            ),
        ))
    else:
        results.append(CheckResult(
            name=f"因子独立性 ({factor_name})",
            status="FAIL",
            details=(
                f"截断 {n_truncate} 行后因子值差异 = {max_diff:.2e}，"
                f"超过阈值 {tolerance}——因子可能依赖未来数据"
            ),
        ))

    return results


def check_factor_returns_alignment(
    data, factor_name: str, factor_inst, return_label: str,
) -> List[CheckResult]:
    """
    检测 5 & 6: 因子-收益时间对齐正确，最后一期收益为 NaN 且被对齐剔除。
    Check 5 & 6: Factor-returns alignment is correct,
    last period return is NaN and dropped by alignment.
    """
    import numpy as np
    import pandas as pd
    from FactorAnalysis.returns import calc_returns
    from FactorAnalysis.alignment import align_factor_returns

    results: List[CheckResult] = []

    # 计算因子 / Compute factor
    factor_raw = factor_inst.calculate(data)
    factor_values = _build_factor_multiindex(factor_raw, data)

    # 计算收益 / Compute returns
    returns = calc_returns(data, label=return_label)

    # 检测 5: 最后一期收益应为 NaN / Check 5: last period returns should be NaN
    last_timestamp = data["timestamp"].max()
    last_returns = returns.loc[last_timestamp] if last_timestamp in returns.index.get_level_values(0) else pd.Series(dtype=float)

    if len(last_returns) > 0 and last_returns.isna().all():
        results.append(CheckResult(
            name="最后一期收益为 NaN",
            status="PASS",
            details=f"最后时间戳 {last_timestamp} 的所有 {len(last_returns)} 个交易对收益均为 NaN",
        ))
    else:
        n_not_nan = last_returns.notna().sum() if len(last_returns) > 0 else 0
        results.append(CheckResult(
            name="最后一期收益为 NaN",
            status="FAIL",
            details=(
                f"最后时间戳 {last_timestamp} 的 {n_not_nan}/{len(last_returns)} "
                "个交易对收益不为 NaN——可能存在未来数据泄露"
            ),
        ))

    # 检测 6: 对齐后不应包含最后时间戳 / Check 6: aligned data should exclude last timestamp
    clean = align_factor_returns(factor_values, returns)
    aligned_timestamps = clean.index.get_level_values("timestamp").unique()

    if last_timestamp not in aligned_timestamps:
        results.append(CheckResult(
            name="对齐剔除最后时间戳",
            status="PASS",
            details=f"对齐后 {len(aligned_timestamps)} 个时间戳，已正确剔除 {last_timestamp}",
        ))
    else:
        results.append(CheckResult(
            name="对齐剔除最后时间戳",
            status="FAIL",
            details=f"对齐后仍包含最后时间戳 {last_timestamp}——对齐逻辑可能有问题",
        ))

    # 附加检测：对齐后数据无 NaN / Additional: no NaN in aligned data
    has_nan = clean["factor"].isna().any() or clean["returns"].isna().any()
    if not has_nan:
        results.append(CheckResult(
            name="对齐后无 NaN",
            status="PASS",
            details=f"对齐后 {len(clean)} 行数据中无 NaN 值",
        ))
    else:
        n_nan = clean.isna().sum().sum()
        results.append(CheckResult(
            name="对齐后无 NaN",
            status="FAIL",
            details=f"对齐后仍有 {n_nan} 个 NaN 值——对齐逻辑可能不完整",
        ))

    return results


# ── 主检测器 / Main detector ─────────────────────────────────────────────────

class FutureLeakDetector:
    """
    未来函数检测器 / Future leak detector.

    执行静态代码扫描和动态数据验证，输出 PASS/FAIL 报告。
    Performs static code scanning and dynamic data verification,
    outputs PASS/FAIL report.
    """

    def __init__(self):
        self.report = DetectionReport()

    def run(
        self,
        factor_name: Optional[str] = None,
        return_label: str = "close2close",
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        symbols: Optional[List[str]] = None,
        exchange: str = "binance",
        kline_type: str = "swap",
        interval: str = "1h",
    ) -> DetectionReport:
        """
        执行全部检测 / Run all checks.

        Parameters / 参数:
            factor_name: 因子名称，None 则检测所有已注册因子 / Factor name, None for all
            return_label: 收益率标签 / Return label
            start_time: 数据起始时间 / Data start time
            end_time: 数据结束时间 / Data end time
            symbols: 交易对列表 / Symbol list
            exchange: 交易所 / Exchange
            kline_type: K 线类型 / Kline type
            interval: K 线周期 / Kline interval
        """
        self.report = DetectionReport()
        self.report.start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        t0 = time.time()

        # 头部信息 / Header
        print("=" * 60)
        print("  Future Leak Detection / 未来函数检测")
        print(f"  {self.report.start_time}")
        print("=" * 60)

        # ── 阶段 1: 静态代码扫描 / Phase 1: Static code scanning ──
        print("\n[Phase 1] 静态代码扫描 / Static Code Scanning")
        print("-" * 60)

        for check_fn in [check_no_shift_in_factorlib, check_shift_only_in_allowed_files, check_kline_loader_no_shift]:
            for result in check_fn():
                self.report.checks.append(result)
                self._print_result(result)

        # ── 阶段 2: 动态数据验证 / Phase 2: Dynamic data verification ──
        print(f"\n[Phase 2] 动态数据验证 / Dynamic Data Verification")
        print("-" * 60)

        # 加载数据 / Load data
        print("  Loading data...")
        try:
            from Cross_Section_Factor.kline_loader import KlineLoader

            loader = KlineLoader(
                start_time=start_time,
                end_time=end_time,
                symbols=symbols,
                exchange=exchange,
                kline_type=kline_type,
                interval=interval,
            )
            data = loader.compile()

            if data.empty:
                print("  WARNING: No data loaded, skipping dynamic checks.")
                print("  警告: 未加载数据，跳过动态检测。")
                # 添加跳过结果 / Add skipped results
                self.report.checks.append(CheckResult(
                    name="因子独立性",
                    status="FAIL",
                    details="数据为空，无法验证因子独立性",
                ))
                self.report.checks.append(CheckResult(
                    name="最后一期收益为 NaN",
                    status="FAIL",
                    details="数据为空，无法验证",
                ))
                self.report.checks.append(CheckResult(
                    name="对齐剔除最后时间戳",
                    status="FAIL",
                    details="数据为空，无法验证",
                ))
                self.report.checks.append(CheckResult(
                    name="对齐后无 NaN",
                    status="FAIL",
                    details="数据为空，无法验证",
                ))
            else:
                n_symbols = data["symbol"].nunique()
                print(f"  Loaded {len(data)} rows, {n_symbols} symbols")

                # 确定要检测的因子 / Determine factors to check
                from FactorLib import list_factors, get

                if factor_name:
                    factor_names = [factor_name] if factor_name in list_factors() else []
                    if not factor_names:
                        print(f"  WARNING: Factor '{factor_name}' not found, skipping dynamic checks.")
                        self.report.checks.append(CheckResult(
                            name="因子独立性",
                            status="FAIL",
                            details=f"因子 '{factor_name}' 未注册",
                        ))
                else:
                    factor_names = list_factors()

                for fname in factor_names:
                    factor_cls = get(fname)
                    if factor_cls is None:
                        continue
                    factor_inst = factor_cls()
                    print(f"\n  Checking factor: {fname}")

                    # 检测 4: 因子独立性 / Check 4: factor independence
                    for result in check_factor_independence(data, fname, factor_inst):
                        self.report.checks.append(result)
                        self._print_result(result)

                    # 检测 5 & 6: 对齐验证 / Check 5 & 6: alignment verification
                    for result in check_factor_returns_alignment(
                        data, fname, factor_inst, return_label
                    ):
                        self.report.checks.append(result)
                        self._print_result(result)

        except Exception as e:
            print(f"  ERROR during dynamic checks: {e}")
            import traceback
            traceback.print_exc()
            # 动态检测失败时添加 FAIL 结果 / Add FAIL results when dynamic checks fail
            for check_name in ["因子独立性", "最后一期收益为 NaN", "对齐剔除最后时间戳", "对齐后无 NaN"]:
                self.report.checks.append(CheckResult(
                    name=check_name,
                    status="FAIL",
                    details=f"检测过程中出错: {e}",
                ))

        # ── 汇总 / Summary ──
        self.report.end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.report.elapsed = time.time() - t0

        self._print_summary()
        return self.report

    @staticmethod
    def _print_result(result: CheckResult) -> None:
        """打印单项结果 / Print single result."""
        status_icon = "PASS" if result.status == "PASS" else "FAIL"
        print(f"  [{status_icon}] {result.name}")
        if result.details:
            for line in result.details.split("\n"):
                print(f"         {line}")

    def _print_summary(self) -> None:
        """打印汇总信息 / Print summary."""
        print(f"\n{'=' * 60}")
        print(f"  Summary / 汇总")
        print(f"{'=' * 60}")
        print(f"  Total:  {len(self.report.checks)} checks")
        print(f"  PASS:   {self.report.n_pass}")
        print(f"  FAIL:   {self.report.n_fail}")
        print(f"  Time:   {self.report.elapsed:.1f}s")
        overall = "ALL PASSED" if self.report.all_passed else "FAILED"
        print(f"  Result: {overall}")
        print(f"{'=' * 60}")

    def to_markdown(self) -> str:
        """
        生成 Markdown 格式报告 / Generate Markdown format report.
        """
        lines = [
            "# Future Leak Detection Report / 未来函数检测报告",
            "",
            f"> Generated: {self.report.start_time} | "
            f"Elapsed: {self.report.elapsed:.1f}s | "
            f"Result: **{'ALL PASSED' if self.report.all_passed else 'FAILED'}**",
            "",
            "---",
            "",
            "## Check Results / 检测结果",
            "",
            "| # | Status | Check | Details |",
            "|---|--------|-------|---------|",
        ]

        for i, check in enumerate(self.report.checks, 1):
            status_badge = (
                f'<span style="color:green">**PASS**</span>'
                if check.status == "PASS"
                else f'<span style="color:red">**FAIL**</span>'
            )
            # 截断 details 避免表格换行问题 / Truncate details for table formatting
            detail_short = check.details.split("\n")[0][:80]
            lines.append(f"| {i} | {status_badge} | {check.name} | {detail_short} |")

        lines.extend([
            "",
            "---",
            "",
            f"**Summary**: {self.report.n_pass}/{len(self.report.checks)} passed, "
            f"{self.report.n_fail} failed.",
        ])

        # 失败项详细信息 / Failed check details
        failed = [c for c in self.report.checks if c.status == "FAIL"]
        if failed:
            lines.extend(["", "## Failed Checks / 失败项详情", ""])
            for check in failed:
                lines.append(f"### {check.name}")
                lines.append("")
                lines.append(check.details)
                lines.append("")

        return "\n".join(lines)


# ── CLI 入口 / CLI entry point ────────────────────────────────────────────────

def main():
    """CLI 入口函数 / CLI entry point."""
    parser = argparse.ArgumentParser(
        description="未来函数自动化检测 / Automated future leak detection"
    )

    # 因子参数 / Factor parameters
    parser.add_argument(
        "--factor", type=str, default=None,
        help="指定检测的因子名称，默认检测所有已注册因子 / Specific factor name (default: all)",
    )
    parser.add_argument(
        "--return-label", type=str, default="close2close",
        choices=["close2close", "open2open"],
        help="收益率标签 (default: close2close)",
    )

    # 数据参数 / Data parameters
    parser.add_argument(
        "--start-time", type=str, default=None,
        help="数据起始时间 / Data start time",
    )
    parser.add_argument(
        "--end-time", type=str, default=None,
        help="数据结束时间 / Data end time",
    )
    parser.add_argument(
        "--symbols", type=str, nargs="*", default=None,
        help="交易对列表 / Symbol list",
    )
    parser.add_argument(
        "--exchange", type=str, default="binance",
        help="交易所 (default: binance)",
    )
    parser.add_argument(
        "--kline-type", type=str, default="swap",
        help="K 线类型 (default: swap)",
    )
    parser.add_argument(
        "--interval", type=str, default="1h",
        help="K 线周期 (default: 1h)",
    )

    # 输出参数 / Output parameters
    parser.add_argument(
        "--report-path", type=str, default=None,
        help="Markdown 报告输出路径 / Markdown report output path",
    )

    args = parser.parse_args()

    # 执行检测 / Run detection
    detector = FutureLeakDetector()
    report = detector.run(
        factor_name=args.factor,
        return_label=args.return_label,
        start_time=args.start_time,
        end_time=args.end_time,
        symbols=args.symbols,
        exchange=args.exchange,
        kline_type=args.kline_type,
        interval=args.interval,
    )

    # 输出 Markdown 报告 / Output Markdown report
    if args.report_path:
        md_content = detector.to_markdown()
        report_path = Path(args.report_path)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(md_content, encoding="utf-8")
        print(f"\nReport saved to: {report_path}")

    # 退出码 / Exit code
    sys.exit(0 if report.all_passed else 1)


if __name__ == "__main__":
    main()
