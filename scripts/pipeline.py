"""
Script: Unified data pipeline — run bulk download then cleanup in sequence.
Purpose: One-command execution of the full data pipeline:
         Step 1: Bulk historical download (S3)
         Step 2: Cleanup fake data of delisted trading pairs

依赖 / Dependencies:
- CryptoDataProviders: 数据源接口 / Data source interface
- CryptoDB_feather: 存储层 / Storage layer
"""
import sys
import os
import argparse
import time
from datetime import datetime

# Add project root directory to path / 添加项目根目录到路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

for submodule in ["CryptoDataProviders", "CryptoDB_feather"]:
    submodule_path = os.path.join(project_root, submodule)
    if submodule_path not in sys.path:
        sys.path.insert(0, submodule_path)


def run_pipeline(kline_type: str, interval: str, dry_run: bool, skip_bulk: bool, skip_cleanup: bool):
    """
    Execute the unified data pipeline in order.
    按顺序执行统一数据管道。

    Parameters:
        kline_type: K线类型 / Kline type ('spot' or 'swap')
        interval: 时间间隔 / Time interval (e.g. '1h', '4h', '1d')
        dry_run: 试运行模式，不实际修改数据 / Dry run mode, no actual data modification
        skip_bulk: 跳过批量下载步骤 / Skip bulk download step
        skip_cleanup: 跳过清理步骤 / Skip cleanup step
    """
    pipeline_start = time.time()
    errors = []

    # Header / 头部信息
    print("=" * 70)
    print(f"  CryptoAlpha Data Pipeline")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  kline_type={kline_type}  interval={interval}  dry_run={dry_run}")
    print("=" * 70)

    # Step 1: Bulk download / 步骤1: 批量下载
    if not skip_bulk:
        print(f"\n{'─' * 70}")
        print(f"  [Step 1/2] Bulk Historical Download (S3)")
        print(f"{'─' * 70}")
        try:
            from scripts.update_bulk import main as run_bulk
            step_start = time.time()
            run_bulk()
            print(f"\n  Bulk download finished in {time.time() - step_start:.1f}s")
        except Exception as e:
            errors.append(f"Bulk download failed: {e}")
            print(f"\n  ERROR: Bulk download failed — {e}")
            print("  Skipping cleanup since bulk download did not complete.")
            _print_summary(pipeline_start, errors)
            return
    else:
        print(f"\n  [Step 1/2] Bulk download — SKIPPED")

    # Step 2: Cleanup fake data / 步骤2: 清理假数据
    if not skip_cleanup:
        print(f"\n{'─' * 70}")
        print(f"  [Step 2/2] Cleanup Fake Data (delisted pairs)")
        print(f"{'─' * 70}")
        try:
            from scripts.cleanup_fake_data import run_cleanup
            step_start = time.time()
            run_cleanup(
                kline_type=kline_type,
                interval=interval,
                dry_run=dry_run,
                max_workers=10
            )
            print(f"\n  Cleanup finished in {time.time() - step_start:.1f}s")
        except Exception as e:
            errors.append(f"Cleanup failed: {e}")
            print(f"\n  ERROR: Cleanup failed — {e}")
    else:
        print(f"\n  [Step 2/2] Cleanup — SKIPPED")

    _print_summary(pipeline_start, errors)


def _print_summary(pipeline_start: float, errors: list):
    """Print pipeline execution summary / 打印管道执行摘要"""
    elapsed = time.time() - pipeline_start
    print("\n" + "=" * 70)
    print(f"  Pipeline finished in {elapsed:.1f}s")
    if errors:
        print(f"  Errors: {len(errors)}")
        for err in errors:
            print(f"    - {err}")
    else:
        print("  All steps completed successfully.")
    print("=" * 70)


def main():
    """Entry point / 入口函数"""
    parser = argparse.ArgumentParser(
        description="CryptoAlpha unified data pipeline: bulk download + cleanup"
    )
    parser.add_argument(
        "--kline-type", default="swap", choices=["spot", "swap"],
        help="K线类型 (default: swap)"
    )
    parser.add_argument(
        "--interval", default="1h",
        help="时间间隔 (default: 1h)"
    )
    parser.add_argument(
        "--dry-run", action="store_true", default=True,
        help="试运行模式，不实际修改数据 (default: True)"
    )
    parser.add_argument(
        "--execute", action="store_true", default=False,
        help="实际执行模式，会修改数据 (overrides --dry-run)"
    )
    parser.add_argument(
        "--skip-bulk", action="store_true", default=False,
        help="跳过批量下载步骤"
    )
    parser.add_argument(
        "--skip-cleanup", action="store_true", default=False,
        help="跳过清理步骤"
    )

    args = parser.parse_args()

    # --execute overrides --dry-run / --execute 覆盖 --dry-run
    dry_run = False if args.execute else args.dry_run

    run_pipeline(
        kline_type=args.kline_type,
        interval=args.interval,
        dry_run=dry_run,
        skip_bulk=args.skip_bulk,
        skip_cleanup=args.skip_cleanup,
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nUser interrupts and exits the program...")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
