"""
FactorAnalysis/data_quality.py — 数据质量追踪 / Data Quality Tracking

在因子与收益率对齐后检查数据覆盖率（非 NaN 比例），低于阈值时发出告警。
Check data coverage (non-NaN ratio) after factor-returns alignment,
emit warnings when coverage falls below threshold.
"""

import warnings

import numpy as np
import pandas as pd


def check_data_quality(
    factor: pd.Series,
    returns: pd.Series,
    max_loss: float = 0.35,
) -> float:
    """
    检查因子与收益率对齐后的数据覆盖率 / Check data coverage after alignment.

    先对因子值和前向收益率做索引对齐，再计算有效数据（非 NaN、非 inf）的覆盖率。
    覆盖率低于 (1 - max_loss) 时发出 UserWarning，低于 0.3 时抛出 ValueError。
    Align factor and returns by index, then compute valid (non-NaN, non-inf) coverage.
    Emit UserWarning when coverage < (1 - max_loss); raise ValueError when coverage < 0.3.

    Parameters / 参数:
        factor: 因子值，MultiIndex (timestamp, symbol) / Factor values, MultiIndex (timestamp, symbol)
        returns: 前向收益率，MultiIndex (timestamp, symbol) / Forward returns, MultiIndex (timestamp, symbol)
        max_loss: 允许的最大数据丢失比例，默认 0.35（即覆盖率阈值 65%）
                  Maximum allowed data loss ratio, default 0.35 (coverage threshold 65%)

    Returns / 返回:
        float: 数据覆盖率，范围 [0.0, 1.0] / Data coverage ratio, range [0.0, 1.0]

    Raises / 异常:
        TypeError: 输入非 pd.Series / Input is not pd.Series
        ValueError: max_loss 不在 [0, 1) 范围内 / max_loss out of [0, 1) range
        ValueError: 对齐后无任何有效数据 / No valid data after alignment
        ValueError: 覆盖率极低（< 0.3），数据不可信 / Coverage critically low (< 0.3)
    """
    # ---- 参数类型校验 / Input type validation ----
    if not isinstance(factor, pd.Series):
        raise TypeError(
            f"'factor' must be pd.Series, got {type(factor).__name__}."
            f"\n'factor' 必须是 pd.Series，收到 {type(factor).__name__}。"
        )
    if not isinstance(returns, pd.Series):
        raise TypeError(
            f"'returns' must be pd.Series, got {type(returns).__name__}."
            f"\n'returns' 必须是 pd.Series，收到 {type(returns).__name__}。"
        )

    # ---- max_loss 参数校验 / Validate max_loss ----
    if not isinstance(max_loss, (int, float)):
        raise TypeError(
            f"'max_loss' must be numeric, got {type(max_loss).__name__}."
            f"\n'max_loss' 必须是数值类型，收到 {type(max_loss).__name__}。"
        )
    if max_loss < 0 or max_loss >= 1:
        raise ValueError(
            f"'max_loss' must be in [0, 1), got {max_loss}."
            f"\n'max_loss' 必须在 [0, 1) 范围内，收到 {max_loss}。"
        )

    # ---- 索引对齐 / Index alignment ----
    # 检查是否有重叠索引 / Check for overlapping indices
    common_idx = factor.index.intersection(returns.index)
    if len(common_idx) == 0:
        raise ValueError(
            "No overlapping (timestamp, symbol) pairs between factor and returns."
            "\n因子与收益率之间无共有的 (timestamp, symbol) 对。"
        )

    # 内连接：仅保留两侧共有的 (timestamp, symbol) / Inner join on common (timestamp, symbol)
    df = pd.DataFrame({
        "factor": factor.loc[common_idx],
        "returns": returns.loc[common_idx],
    })
    n_aligned = len(df)

    # ---- 计算有效覆盖率 / Compute valid coverage ----
    mask = (
        df["factor"].notna()
        & df["returns"].notna()
        & np.isfinite(df["factor"])
        & np.isfinite(df["returns"])
    )
    n_valid = int(mask.sum())
    coverage = n_valid / n_aligned

    # ---- 覆盖率检查 / Coverage checks ----
    min_coverage = 1.0 - max_loss

    # 极低覆盖率：直接抛出异常 / Critically low coverage: raise immediately
    if coverage < 0.3:
        raise ValueError(
            f"Data coverage critically low: {coverage:.2%} ({n_valid}/{n_aligned} valid rows). "
            f"Minimum acceptable is 30%."
            f"\n数据覆盖率极低：{coverage:.2%}（{n_valid}/{n_aligned} 行有效），最低可接受 30%。"
        )

    # 低覆盖率：发出警告 / Low coverage: emit warning
    if coverage < min_coverage:
        warnings.warn(
            f"Data coverage {coverage:.2%} is below threshold {min_coverage:.2%} "
            f"(max_loss={max_loss}). {n_valid}/{n_aligned} valid rows."
            f"\n数据覆盖率 {coverage:.2%} 低于阈值 {min_coverage:.2%} "
            f"（max_loss={max_loss}）。{n_valid}/{n_aligned} 行有效。",
            UserWarning,
            stacklevel=2,
        )

    return coverage
