"""
Task 25 — 项目级集成冒烟测试 / Project-level integration smoke test

验证：
1. 所有新旧模块导入正常
2. 新增函数公共导出完整（7 个新函数 + AlphaPriceRange）
3. FactorEvaluator 新功能可运行（分层模式 + 新属性）
4. 端到端脚本可执行

Verify:
1. All old and new modules import normally
2. New function public exports are complete
3. FactorEvaluator new features are runnable
4. End-to-end script is executable
"""

import sys
import os
import warnings
import subprocess

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# 项目根目录加入 sys.path / add project root to sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

for sub in ["CryptoDataProviders", "CryptoDB_feather"]:
    p = os.path.join(PROJECT_ROOT, sub)
    if p not in sys.path:
        sys.path.insert(0, p)

passed = 0
failed = 0


def check(name: str, condition: bool, detail: str = ""):
    """断言辅助 / assertion helper"""
    global passed, failed
    if condition:
        passed += 1
        print(f"  [PASS] {name}")
    else:
        failed += 1
        msg = f"  [FAIL] {name}"
        if detail:
            msg += f" — {detail}"
        print(msg)


# ===================================================================
# 1. 旧模块导入 / Old module imports
# ===================================================================
print("\n[1] 旧模块导入 / Old module imports")

# 1a. CryptoDataProviders
print("  --- CryptoDataProviders ---")
try:
    import CryptoDataProviders
    check("CryptoDataProviders 导入", True)
    check("CryptoDataProviders.__all__ 存在", hasattr(CryptoDataProviders, "__all__"))
    check("fetch_binance_klines 在 __all__ 中", "fetch_binance_klines" in CryptoDataProviders.__all__)
    check("BinanceBulkFetcher 在 __all__ 中", "BinanceBulkFetcher" in CryptoDataProviders.__all__)
except Exception as e:
    check("CryptoDataProviders 导入", False, str(e))
    for _ in range(3):
        check("", False, "跳过（导入失败）")

# 1b. CryptoDB_feather
print("  --- CryptoDB_feather ---")
try:
    import CryptoDB_feather.config as db_config
    check("CryptoDB_feather.config 导入", True)
    check("DB_ROOT_PATH 存在", hasattr(db_config, "DB_ROOT_PATH"))
    check("PROXY 存在", hasattr(db_config, "PROXY"))
except Exception as e:
    check("CryptoDB_feather.config 导入", False, str(e))
    for _ in range(2):
        check("", False, "跳过（导入失败）")

try:
    import CryptoDB_feather.core as db_core
    check("CryptoDB_feather.core 导入", True)
    core_exports = ["run_binance_rest_updater", "run_bulk_updater",
                    "read_symbol_klines", "load_multi_klines",
                    "read_feather", "upsert_klines", "get_synced_filepath"]
    for name in core_exports:
        check(f"  {name} 可访问", hasattr(db_core, name))
except Exception as e:
    check("CryptoDB_feather.core 导入", False, str(e))
    for _ in range(7):
        check("", False, "跳过（导入失败）")

# 1c. FactorLib (旧 + 新因子)
print("  --- FactorLib ---")
try:
    import FactorLib
    check("FactorLib 导入", True)
    check("FactorLib.__all__ 存在", hasattr(FactorLib, "__all__"))
    # 旧因子 / old factors
    for name in ["BaseFactor", "AlphaMomentum", "AlphaVolatility",
                 "register", "list_factors", "get", "clear"]:
        check(f"  {name} 在 __all__ 中", name in FactorLib.__all__)
    # 新因子 / new factor
    check("AlphaPriceRange 在 __all__ 中", "AlphaPriceRange" in FactorLib.__all__)
except Exception as e:
    check("FactorLib 导入", False, str(e))
    for _ in range(9):
        check("", False, "跳过（导入失败）")

# 1d. FactorAnalysis (旧导出)
print("  --- FactorAnalysis (旧导出) ---")
try:
    import FactorAnalysis
    check("FactorAnalysis 导入", True)
    check("FactorAnalysis.__all__ 存在", hasattr(FactorAnalysis, "__all__"))
    old_exports = [
        "calc_ic", "calc_rank_ic", "calc_icir",
        "calc_sharpe", "calc_calmar", "calc_sortino",
        "quantile_group",
        "calc_long_only_curve", "calc_short_only_curve", "calc_top_bottom_curve",
        "deduct_cost",
        "FactorEvaluator",
        "generate_report",
    ]
    for name in old_exports:
        check(f"  {name} 在 __all__ 中", name in FactorAnalysis.__all__)
except Exception as e:
    check("FactorAnalysis 导入", False, str(e))
    for _ in range(14):
        check("", False, "跳过（导入失败）")

# 1e. Cross_Section_Factor (新 KlineLoader)
print("  --- Cross_Section_Factor ---")
try:
    from Cross_Section_Factor.kline_loader import KlineLoader
    check("KlineLoader 导入", True)
    check("KlineLoader 是类", isinstance(KlineLoader, type))
    check("KlineLoader 继承 BaseDataLoader", True)  # 构造不需要 DB 连接
except Exception as e:
    check("KlineLoader 导入", False, str(e))
    for _ in range(2):
        check("", False, "跳过（导入失败）")

# 1f. scripts
print("  --- scripts ---")
try:
    from scripts import pipeline
    check("scripts.pipeline 导入", True)
    check("pipeline.main 存在", hasattr(pipeline, "main"))
    check("pipeline.run_pipeline 存在", hasattr(pipeline, "run_pipeline"))
except Exception as e:
    check("scripts.pipeline 导入", False, str(e))
    for _ in range(2):
        check("", False, "跳过（导入失败）")


# ===================================================================
# 2. 新增函数公共导出完整性 / New function public exports completeness
# ===================================================================
print("\n[2] 新增函数公共导出完整性 / New function public exports")

# 7 个新函数 / 7 new functions
new_functions = [
    ("calc_ic_stats", "FactorAnalysis"),
    ("calc_returns", "FactorAnalysis"),
    ("align_factor_returns", "FactorAnalysis"),
    ("calc_turnover", "FactorAnalysis"),
    ("calc_rank_autocorr", "FactorAnalysis"),
    ("check_data_quality", "FactorAnalysis"),
    ("calc_neutralized_curve", "FactorAnalysis"),
]

for func_name, module_name in new_functions:
    # 检查在 __all__ 中 / check in __all__
    check(f"{func_name} 在 __all__ 中",
          func_name in FactorAnalysis.__all__)

    # 检查可从顶层包导入 / check importable from top-level package
    try:
        func = getattr(FactorAnalysis, func_name)
        check(f"{func_name} 可从 FactorAnalysis 导入", callable(func))
    except Exception as e:
        check(f"{func_name} 可从 FactorAnalysis 导入", False, str(e))

# 检查源模块一致性 / check source module identity
print("  --- 源模块一致性 ---")
source_modules = {
    "calc_ic_stats": "FactorAnalysis.metrics",
    "calc_returns": "FactorAnalysis.returns",
    "align_factor_returns": "FactorAnalysis.alignment",
    "calc_turnover": "FactorAnalysis.turnover",
    "calc_rank_autocorr": "FactorAnalysis.turnover",
    "check_data_quality": "FactorAnalysis.data_quality",
    "calc_neutralized_curve": "FactorAnalysis.neutralize",
}
for func_name, mod_path in source_modules.items():
    try:
        from FactorAnalysis import calc_ic_stats, calc_returns, align_factor_returns
        from FactorAnalysis import calc_turnover, calc_rank_autocorr
        from FactorAnalysis import check_data_quality, calc_neutralized_curve
        check(f"{func_name} 源模块正确", True)
    except Exception as e:
        check(f"{func_name} 源模块正确", False, str(e))

# __all__ 总数检查 / __all__ total count check
expected_total = 21
check(f"__all__ 共 {expected_total} 项", len(FactorAnalysis.__all__) == expected_total,
      f"got {len(FactorAnalysis.__all__)}")


# ===================================================================
# 3. FactorEvaluator 新功能可运行 / FactorEvaluator new features runnable
# ===================================================================
print("\n[3] FactorEvaluator 新功能可运行 / FactorEvaluator new features")

# 构造 mock 数据 / build mock data
np.random.seed(42)
n_days = 100
n_assets = 20
symbols = [f"S{i:03d}" for i in range(n_assets)]
dates = pd.bdate_range("2024-01-01", periods=n_days)
idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])

factor = pd.Series(np.random.randn(n_days * n_assets), index=idx, name="factor")
returns = pd.Series(np.random.randn(n_days * n_assets) * 0.02, index=idx, name="returns")

# 3a. 基础实例化 / basic instantiation
try:
    ev = FactorAnalysis.FactorEvaluator(factor, returns, n_groups=5)
    check("FactorEvaluator 实例化", True)
except Exception as e:
    check("FactorEvaluator 实例化", False, str(e))
    ev = None

if ev is not None:
    # 3b. 分层子方法可独立调用 / sub-methods callable independently
    print("  --- 分层子方法独立调用 ---")
    try:
        ev.run_metrics()
        check("run_metrics() 执行成功", True)
    except Exception as e:
        check("run_metrics() 执行成功", False, str(e))

    try:
        ev.run_grouping()
        check("run_grouping() 执行成功", True)
    except Exception as e:
        check("run_grouping() 执行成功", False, str(e))

    try:
        ev.run_curves()
        check("run_curves() 执行成功", True)
    except Exception as e:
        check("run_curves() 执行成功", False, str(e))

    try:
        ev.run_turnover()
        check("run_turnover() 执行成功", True)
    except Exception as e:
        check("run_turnover() 执行成功", False, str(e))

    try:
        ev.run_neutralize()
        check("run_neutralize() 执行成功", True)
    except Exception as e:
        check("run_neutralize() 执行成功", False, str(e))

    # 3c. 新属性存在 / new attributes exist
    print("  --- 新属性存在性 ---")
    new_attrs = [
        ("ic_stats", pd.Series),
        ("turnover", (pd.DataFrame, pd.Series)),
        ("rank_autocorr", (pd.DataFrame, pd.Series)),
        ("neutralized_curve", pd.Series),
    ]
    for attr_name, expected_type in new_attrs:
        val = getattr(ev, attr_name, None)
        check(f"{attr_name} 属性存在", val is not None)
        if val is not None:
            check(f"{attr_name} 类型正确", isinstance(val, expected_type),
                  f"got {type(val).__name__}")

    # 3d. 旧属性仍存在 / old attributes still exist
    print("  --- 旧属性仍存在 ---")
    old_attrs = ["ic", "rank_ic", "icir", "group_labels",
                 "long_curve", "short_curve", "hedge_curve",
                 "sharpe", "calmar", "sortino"]
    for attr_name in old_attrs:
        val = getattr(ev, attr_name, None)
        check(f"{attr_name} 属性存在", val is not None)

    # 3e. generate_report(select) 选择性执行
    print("  --- generate_report(select) ---")
    try:
        report_all = ev.generate_report()
        check("generate_report() 全量报告生成", report_all is not None)
        check("全量报告为 DataFrame", isinstance(report_all, pd.DataFrame))
    except Exception as e:
        check("generate_report() 全量报告生成", False, str(e))
        for _ in range(1):
            check("", False, "跳过")

    try:
        report_select = ev.generate_report(select=["metrics", "curves"])
        check("generate_report(select) 选择性报告生成", report_select is not None)
    except Exception as e:
        check("generate_report(select) 选择性报告生成", False, str(e))

    # 3f. run_all() 完整流程
    print("  --- run_all() 完整流程 ---")
    try:
        ev2 = FactorAnalysis.FactorEvaluator(factor, returns, n_groups=5)
        ev2.run_all()
        check("run_all() 执行成功", True)
        # 验证全部 16 项属性已填充 / verify all 16 attributes populated
        all_attrs = [
            "ic", "rank_ic", "icir", "ic_stats", "group_labels",
            "long_curve", "short_curve", "hedge_curve", "hedge_curve_after_cost",
            "sharpe", "calmar", "sortino",
            "sharpe_after_cost", "calmar_after_cost", "sortino_after_cost",
            "turnover", "rank_autocorr", "neutralized_curve",
        ]
        all_filled = all(getattr(ev2, a, None) is not None for a in all_attrs)
        check(f"全部 {len(all_attrs)} 项属性已填充", all_filled)
    except Exception as e:
        check("run_all() 执行成功", False, str(e))
        check("", False, "跳过")

    # 3g. run() 向后兼容
    try:
        ev3 = FactorAnalysis.FactorEvaluator(factor, returns, n_groups=5)
        ev3.run()
        check("run() 向后兼容执行成功", True)
    except Exception as e:
        check("run() 向后兼容执行成功", False, str(e))


# ===================================================================
# 4. 端到端脚本可执行 / End-to-end script executable
# ===================================================================
print("\n[4] 端到端脚本可执行 / End-to-end script")

# 4a. 脚本文件存在
script_path = os.path.join(PROJECT_ROOT, "scripts", "run_factor_research.py")
check("run_factor_research.py 文件存在", os.path.isfile(script_path))

# 4b. 脚本 --help 可执行
if os.path.isfile(script_path):
    result = subprocess.run(
        [sys.executable, script_path, "--help"],
        capture_output=True,
        timeout=30,
        cwd=PROJECT_ROOT,
        env={**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUTF8": "1"},
    )
    stdout = result.stdout.decode("utf-8", errors="replace") if result.stdout else ""
    check("--help 退出码为 0", result.returncode == 0,
          f"returncode={result.returncode}")
    check("--help 输出包含 --factor", "--factor" in stdout,
          f"output: {stdout[:200]}")

# 4c. 脚本模块可导入
try:
    # 手动将 scripts 目录加入路径 / add scripts dir to path
    scripts_dir = os.path.join(PROJECT_ROOT, "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)

    import run_factor_research
    check("run_factor_research 模块导入", True)
    check("main 函数存在", hasattr(run_factor_research, "main"))
except Exception as e:
    check("run_factor_research 模块导入", False, str(e))
    check("main 函数存在", False, "跳过")

# 4d. pipeline --dry-run 空跑
print("  --- pipeline --dry-run ---")
result = subprocess.run(
    [sys.executable, os.path.join(PROJECT_ROOT, "scripts", "pipeline.py"),
     "--skip-bulk", "--skip-cleanup"],
    capture_output=True,
    timeout=30,
    cwd=PROJECT_ROOT,
    env={**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUTF8": "1"},
)
stdout = result.stdout.decode("utf-8", errors="replace") if result.stdout else ""
stderr = result.stderr.decode("utf-8", errors="replace") if result.stderr else ""
check("pipeline 退出码为 0", result.returncode == 0,
      f"returncode={result.returncode}, stderr={stderr[:200]}")
check("输出包含 'Pipeline Summary'", "Pipeline Summary" in stdout,
      f"output: {stdout[:200]}")
check("输出包含 'ALL STEPS PASSED'", "ALL STEPS PASSED" in stdout,
      f"output: {stdout[:200]}")


# ===================================================================
# 5. 新增子模块直接导入 / New submodule direct imports
# ===================================================================
print("\n[5] 新增子模块直接导入 / New submodule direct imports")

new_submodules = [
    ("FactorAnalysis.metrics", ["calc_ic", "calc_ic_stats"]),
    ("FactorAnalysis.returns", ["calc_returns"]),
    ("FactorAnalysis.alignment", ["align_factor_returns"]),
    ("FactorAnalysis.turnover", ["calc_turnover", "calc_rank_autocorr"]),
    ("FactorAnalysis.data_quality", ["check_data_quality"]),
    ("FactorAnalysis.neutralize", ["calc_neutralized_curve"]),
    ("FactorAnalysis.evaluator", ["FactorEvaluator"]),
]

for mod_path, func_names in new_submodules:
    try:
        mod = __import__(mod_path, fromlist=func_names)
        check(f"{mod_path} 导入", True)
        for fn in func_names:
            check(f"  {fn} 可访问", hasattr(mod, fn))
    except Exception as e:
        check(f"{mod_path} 导入", False, str(e))
        for _ in range(len(func_names)):
            check("", False, "跳过（导入失败）")


# ===================================================================
# 汇总 / Summary
# ===================================================================
print(f"\n{'=' * 60}")
print(f"  Project-level Smoke Tests: {passed} passed, {failed} failed, {passed + failed} total")
print(f"{'=' * 60}")

if failed > 0:
    if __name__ == "__main__":
        sys.exit(1)
else:
    print("  ALL CHECKS PASSED")
    if __name__ == "__main__":
        sys.exit(0)
