"""
FactorAnalysis/alignment.py — 因子值与收益矩阵对齐 / Factor-Returns Alignment

将因子值与前向收益率按 (timestamp, symbol) 索引对齐，剔除任一侧为 NaN 的行，
产出干净的配对数据供后续 IC/分组/净值分析使用。
Align factor values with forward returns on (timestamp, symbol) index, dropping
rows where either side is NaN, producing clean paired data for downstream analysis.
"""

import warnings
import numpy as np
import pandas as pd


def align_factor_returns(
    factor: pd.Series,
    returns: pd.Series,
) -> pd.DataFrame:
    """
    按索引对齐因子值与前向收益，剔除 NaN 行 / Align factor with returns, drop NaN rows.

    执行内连接（inner join），仅保留两侧索引均存在的 (timestamp, symbol) 对，
    再剔除因子值或收益率为 NaN / inf 的行。
    Inner join on MultiIndex (timestamp, symbol), then drop rows where factor
    or returns is NaN or infinite.

    Parameters / 参数:
        factor: 因子值，MultiIndex (timestamp, symbol) / Factor values, MultiIndex (timestamp, symbol)
        returns: 前向收益率，MultiIndex (timestamp, symbol) / Forward returns, MultiIndex (timestamp, symbol)

    Returns / 返回:
        pd.DataFrame: 两列 ['factor', 'returns']，索引为 (timestamp, symbol)，无 NaN / inf。
                      DataFrame with ['factor', 'returns'] columns, MultiIndex, no NaN / inf.

    Raises / 异常:
        ValueError: factor 或 returns 不是 pd.Series，或索引非 MultiIndex。
    """
    # 参数类型校验 / Validate input types
    if not isinstance(factor, pd.Series):
        raise ValueError(
            f"'factor' must be pd.Series, got {type(factor).__name__}."
            f"\n'factor' 必须是 pd.Series，收到 {type(factor).__name__}。"
        )
    if not isinstance(returns, pd.Series):
        raise ValueError(
            f"'returns' must be pd.Series, got {type(returns).__name__}."
            f"\n'returns' 必须是 pd.Series，收到 {type(returns).__name__}。"
        )

    # 索引校验 / Validate MultiIndex
    if not isinstance(factor.index, pd.MultiIndex):
        raise ValueError(
            "'factor' index must be a MultiIndex (timestamp, symbol)."
            "\n'factor' 索引必须是 MultiIndex (timestamp, symbol)。"
        )
    if not isinstance(returns.index, pd.MultiIndex):
        raise ValueError(
            "'returns' index must be a MultiIndex (timestamp, symbol)."
            "\n'returns' 索引必须是 MultiIndex (timestamp, symbol)。"
        )

    # 记录原始数据量 / Record original sizes
    n_factor = len(factor)
    n_returns = len(returns)

    # 内连接：仅保留两侧共有的 (timestamp, symbol) / Inner join on common (timestamp, symbol)
    df = pd.DataFrame({"factor": factor, "returns": returns})

    # 剔除 NaN 和 inf / Drop NaN and infinite values
    mask = (
        df["factor"].notna()
        & df["returns"].notna()
        & np.isfinite(df["factor"])
        & np.isfinite(df["returns"])
    )
    clean = df[mask].copy()

    n_after = len(clean)
    n_dropped = len(df) - n_after

    # 数据丢失告警 / Warn on significant data loss
    if len(df) > 0:
        loss_ratio = n_dropped / len(df)
        if loss_ratio > 0.5:
            warnings.warn(
                f"Alignment dropped {n_dropped}/{len(df)} rows ({loss_ratio:.1%}). "
                f"Factor had {n_factor} entries, returns had {n_returns}."
                f"\n对齐丢弃 {n_dropped}/{len(df)} 行 ({loss_ratio:.1%})。"
                f"因子有 {n_factor} 条，收益有 {n_returns} 条。",
                UserWarning,
                stacklevel=2,
            )

    return clean
