"""
Task 18 验证测试 — FactorAnalysis/grouping.py quantile_group
"""

import sys
import traceback

import numpy as np
import pandas as pd

passed = 0
failed = 0


def check(name, condition, detail=""):
    global passed, failed
    if condition:
        passed += 1
        print(f"  [PASS] {name}")
    else:
        failed += 1
        print(f"  [FAIL] {name} {detail}")


def main():
    # --- 1. 导入测试 / Import test ---
    print("=== 1. Import ===")
    try:
        from FactorAnalysis.grouping import quantile_group
        check("import quantile_group", True)
    except Exception as e:
        check("import quantile_group", False, str(e))
        return

    # 公共导出 / public export
    try:
        import FactorAnalysis
        check("FactorAnalysis.quantile_group accessible", hasattr(FactorAnalysis, "quantile_group"))
    except Exception as e:
        check("FactorAnalysis.quantile_group accessible", False, str(e))

    # --- 2. 基本功能 / Basic functionality ---
    print("\n=== 2. Basic functionality ===")

    # 构造合成数据: 100 天, 50 个标的, 正态分布因子值
    # Synthetic data: 100 days, 50 assets, normally distributed factor values
    np.random.seed(42)
    dates = pd.date_range("2025-01-01", periods=100, freq="D")
    symbols = [f"S{i:03d}" for i in range(50)]
    idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])
    factor = pd.Series(np.random.randn(len(idx)), index=idx, name="factor")

    labels = quantile_group(factor, n_groups=5)
    check("返回类型为 pd.Series", isinstance(labels, pd.Series))
    check("索引与输入一致", labels.index.equals(factor.index))
    check("无 NaN 标签（输入无 NaN）", labels.notna().all())

    valid_labels = labels.dropna().astype(int)
    check("组标签最小值为 0", valid_labels.min() == 0)
    check("组标签最大值为 4", valid_labels.max() == 4)
    unique_groups = sorted(valid_labels.unique())
    check("恰好 5 个组", unique_groups == [0, 1, 2, 3, 4])

    # --- 3. 分组均匀性 / Group uniformity ---
    print("\n=== 3. Group uniformity ===")

    # 每个截面上各组的资产数应大致相等（10个/组）
    # Each cross-section should have roughly equal group sizes (~10 per group)
    group_counts = labels.groupby([labels.index.get_level_values(0), labels]).count()
    counts_per_ts = group_counts.unstack()
    # 每组大约 10 个资产（50 / 5 = 10）
    mean_counts = counts_per_ts.mean()
    check("各组平均资产数接近 10", all(abs(mean_counts - 10) <= 1),
          f"mean counts: {mean_counts.to_dict()}")

    # --- 4. 分位数单调性 / Quantile monotonicity ---
    print("\n=== 4. Quantile monotonicity ===")

    # 同一截面内，高组标签的因子均值应 >= 低组标签的因子均值
    # Within same cross-section, higher group should have >= mean factor value
    df = pd.DataFrame({"factor": factor, "group": labels})
    mean_by_group = df.groupby("group")["factor"].mean()
    monotonic = all(mean_by_group.iloc[i] <= mean_by_group.iloc[i + 1] for i in range(len(mean_by_group) - 1))
    check("各组因子均值单调递增", monotonic, f"means: {mean_by_group.to_dict()}")

    # --- 5. 可调分组数 / Adjustable n_groups ---
    print("\n=== 5. Adjustable n_groups ===")

    labels_3 = quantile_group(factor, n_groups=3)
    unique_3 = sorted(labels_3.dropna().astype(int).unique())
    check("3 组: 标签为 0,1,2", unique_3 == [0, 1, 2])

    labels_10 = quantile_group(factor, n_groups=10)
    unique_10 = sorted(labels_10.dropna().astype(int).unique())
    check("10 组: 标签为 0..9", unique_10 == list(range(10)))

    # --- 6. 参数校验 / Parameter validation ---
    print("\n=== 6. Parameter validation ===")

    try:
        quantile_group(factor, n_groups=1)
        check("n_groups=1 抛出 ValueError", False, "no exception")
    except ValueError:
        check("n_groups=1 抛出 ValueError", True)
    except Exception as e:
        check("n_groups=1 抛出 ValueError", False, f"wrong exception: {type(e).__name__}")

    try:
        quantile_group(factor, n_groups=0)
        check("n_groups=0 抛出 ValueError", False, "no exception")
    except ValueError:
        check("n_groups=0 抛出 ValueError", True)
    except Exception as e:
        check("n_groups=0 抛出 ValueError", False, f"wrong exception: {type(e).__name__}")

    # --- 7. NaN 处理 / NaN handling ---
    print("\n=== 7. NaN handling ===")

    factor_nan = factor.copy()
    # 设置 10% 为 NaN / set 10% to NaN
    nan_idx = np.random.choice(len(factor_nan), size=len(factor_nan) // 10, replace=False)
    factor_nan.iloc[nan_idx] = np.nan

    labels_nan = quantile_group(factor_nan, n_groups=5)
    check("NaN 因子值对应 NaN 标签", labels_nan.iloc[nan_idx].isna().all())
    check("非 NaN 因子值有有效标签", labels_nan.dropna().notna().all())
    valid_count = labels_nan.dropna().shape[0]
    expected_valid = len(factor_nan) - len(nan_idx)
    check("有效标签数量正确", valid_count == expected_valid,
          f"got {valid_count}, expected {expected_valid}")

    # --- 8. Inf 处理 / Inf handling ---
    print("\n=== 8. Inf handling ===")

    factor_inf = factor.copy()
    factor_inf.iloc[0] = np.inf
    factor_inf.iloc[1] = -np.inf

    labels_inf = quantile_group(factor_inf, n_groups=5)
    check("Inf 值得到 NaN 标签", pd.isna(labels_inf.iloc[0]) and pd.isna(labels_inf.iloc[1]))
    check("非 Inf 值有有效标签", labels_inf.iloc[2:].dropna().shape[0] > 0)

    # --- 9. 全 NaN 输入 / All-NaN input ---
    print("\n=== 9. Edge cases ===")

    all_nan = pd.Series(np.nan, index=factor.index)
    labels_all_nan = quantile_group(all_nan, n_groups=5)
    check("全 NaN 输入 → 全 NaN 输出", labels_all_nan.isna().all())

    # 单个截面 / single cross-section
    single_ts = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0],
                          index=pd.MultiIndex.from_tuples(
                              [(dates[0], f"S{i}") for i in range(5)],
                              names=["timestamp", "symbol"]))
    labels_single = quantile_group(single_ts, n_groups=5)
    check("单截面 5 个资产 5 组 → 每组 1 个", sorted(labels_single.dropna().astype(int).unique()) == [0, 1, 2, 3, 4])

    # --- 汇总 / Summary ---
    print(f"\n{'='*40}")
    print(f"Results: {passed} passed, {failed} failed")
    if failed > 0:
        sys.exit(1)
    else:
        print("ALL PASSED")


if __name__ == "__main__":
    main()
