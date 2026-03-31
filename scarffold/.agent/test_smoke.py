"""
项目级冒烟测试 / Project-level Smoke Tests

验证：
1. requirements.txt 可解析无冲突
2. 所有模块导入正常 (CryptoDataProviders, CryptoDB_feather, FactorLib, FactorAnalysis, scripts)
3. pipeline --dry-run 或等效空跑不报错

Verify:
1. requirements.txt parses without conflicts
2. All modules import normally
3. pipeline --dry-run or equivalent runs without error
"""

import sys
import os
import warnings

import pkg_resources

warnings.filterwarnings("ignore")

# 项目根目录加入 sys.path / add project root to sys.path
# test_smoke.py 位于 scarffold/.agent/ 下，需要向上两级 / file is 2 levels deep
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# CryptoDB_feather 需要额外加入路径（无顶层 __init__.py）
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
# 1. requirements.txt 解析 / requirements.txt parsing
# ===================================================================
print("\n[1] requirements.txt 解析 / requirements.txt parsing")

req_path = os.path.join(PROJECT_ROOT, "requirements.txt")
check("requirements.txt 文件存在", os.path.isfile(req_path))

# 读取并解析每一行 / read and parse each line
req_lines = []
with open(req_path, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        req_lines.append(line)

check("requirements.txt 非空（有依赖声明）", len(req_lines) > 0, f"got {len(req_lines)}")

# 逐行解析为 Requirement / parse each line as Requirement
parsed_ok = True
bad_lines = []
for line in req_lines:
    try:
        pkg_resources.Requirement.parse(line)
    except Exception:
        parsed_ok = False
        bad_lines.append(line)

check("所有依赖行可解析为 Requirement", parsed_ok,
      f"bad lines: {bad_lines}" if bad_lines else "")

# 检查是否有重复包名 / check for duplicate package names
pkg_names = []
for line in req_lines:
    try:
        req = pkg_resources.Requirement.parse(line)
        pkg_names.append(req.name.lower())
    except Exception:
        pass

dup_names = [n for n in pkg_names if pkg_names.count(n) > 1]
check("无重复包名", len(dup_names) == 0,
      f"duplicates: {set(dup_names)}" if dup_names else "")

# 检查关键包是否已安装 / check key packages are installed
key_packages = ["pandas", "numpy", "pyarrow", "scipy", "requests", "tqdm", "rich"]
installed_missing = []
for pkg in key_packages:
    try:
        pkg_resources.get_distribution(pkg)
    except pkg_resources.DistributionNotFound:
        installed_missing.append(pkg)

check("关键包已安装", len(installed_missing) == 0,
      f"missing: {installed_missing}" if installed_missing else "")

# ===================================================================
# 2. 模块导入 / Module imports
# ===================================================================
print("\n[2] 模块导入 / Module imports")

# 2a. CryptoDataProviders
print("  --- CryptoDataProviders ---")
try:
    import CryptoDataProviders
    check("CryptoDataProviders 导入", True)
    check("CryptoDataProviders.__all__ 存在", hasattr(CryptoDataProviders, "__all__"))
    check("CryptoDataProviders.__all__ 非空", len(CryptoDataProviders.__all__) > 0)
    check("fetch_binance_klines 在 __all__ 中", "fetch_binance_klines" in CryptoDataProviders.__all__)
    check("BinanceBulkFetcher 在 __all__ 中", "BinanceBulkFetcher" in CryptoDataProviders.__all__)
except Exception as e:
    check("CryptoDataProviders 导入", False, str(e))
    for _ in range(4):
        check("", False, "跳过（导入失败）")

# 2b. CryptoDB_feather
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

# 2c. FactorLib
print("  --- FactorLib ---")
try:
    import FactorLib
    check("FactorLib 导入", True)
    check("FactorLib.__all__ 存在", hasattr(FactorLib, "__all__"))
    expected_factor_exports = [
        "BaseFactor", "AlphaMomentum", "AlphaVolatility",
        "register", "list_factors", "get", "clear",
    ]
    for name in expected_factor_exports:
        check(f"  {name} 在 __all__ 中", name in FactorLib.__all__)
except Exception as e:
    check("FactorLib 导入", False, str(e))
    for _ in range(8):
        check("", False, "跳过（导入失败）")

# 2d. FactorAnalysis
print("  --- FactorAnalysis ---")
try:
    import FactorAnalysis
    check("FactorAnalysis 导入", True)
    check("FactorAnalysis.__all__ 存在", hasattr(FactorAnalysis, "__all__"))
    expected_fa_exports = [
        "calc_ic", "calc_rank_ic", "calc_icir",
        "calc_sharpe", "calc_calmar", "calc_sortino",
        "quantile_group",
        "calc_long_only_curve", "calc_short_only_curve", "calc_top_bottom_curve",
        "deduct_cost",
        "FactorEvaluator",
        "generate_report",
    ]
    for name in expected_fa_exports:
        check(f"  {name} 在 __all__ 中", name in FactorAnalysis.__all__)
except Exception as e:
    check("FactorAnalysis 导入", False, str(e))
    for _ in range(14):
        check("", False, "跳过（导入失败）")

# 2e. scripts
print("  --- scripts ---")
try:
    import scripts
    check("scripts 导入", True)
except Exception as e:
    check("scripts 导入", False, str(e))

# scripts 子模块需要额外路径配置 / scripts submodules need extra path setup
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
# 3. pipeline --dry-run 空跑 / pipeline --dry-run dry run
# ===================================================================
print("\n[3] pipeline --dry-run 空跑 / pipeline --dry-run dry run")

# 使用 --skip-bulk --skip-cleanup 避免网络请求 / skip both steps to avoid network
import subprocess

result = subprocess.run(
    [sys.executable, os.path.join(PROJECT_ROOT, "scripts", "pipeline.py"),
     "--skip-bulk", "--skip-cleanup"],
    capture_output=True,
    timeout=30,
    cwd=PROJECT_ROOT,
    env={**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUTF8": "1"},
)
# 手动解码输出，兼容 Windows GBK 环境 / decode output manually, Windows GBK safe
stdout = result.stdout.decode("utf-8", errors="replace") if result.stdout else ""
stderr = result.stderr.decode("utf-8", errors="replace") if result.stderr else ""

check("pipeline 退出码为 0", result.returncode == 0,
      f"returncode={result.returncode}, stderr={stderr[:200]}")

# 检查输出中包含预期关键字 / check output contains expected keywords
check("输出包含 'Pipeline Summary'", "Pipeline Summary" in stdout,
      f"output: {stdout[:200]}")
check("输出包含 'ALL STEPS PASSED'", "ALL STEPS PASSED" in stdout,
      f"output: {stdout[:200]}")

# ===================================================================
# 汇总 / Summary
# ===================================================================
print(f"\n{'=' * 60}")
print(f"  Smoke Tests: {passed} passed, {failed} failed, {passed + failed} total")
print(f"{'=' * 60}")

if failed > 0:
    sys.exit(1)
else:
    print("  ALL CHECKS PASSED")
    sys.exit(0)
