"""
FactorAnalysis/returns.py — T+1 前向收益矩阵计算 / T+1 Forward Returns Calculation

支持 close2close 和 open2open 两种收益率标签。
Supports close2close and open2open return labels.
"""

import warnings
import numpy as np
import pandas as pd

# 支持的收益率标签 / Supported return labels
_VALID_LABELS = {"close2close", "open2open"}


def calc_returns(data: pd.DataFrame, label: str = "close2close") -> pd.Series:
    """
    计算 T+1 前向收益 / Calculate T+1 forward returns.

    对于每个交易对，计算下一期价格与当前价格的百分比变化。
    For each symbol, compute the percentage change from current to next period price.

    Parameters / 参数:
        data: 包含 OHLC 数据的 DataFrame，需有 timestamp, symbol 列及对应的 price 列。
              支持以下格式：
              - 已有 MultiIndex (timestamp, symbol) 的 DataFrame
              - 包含 timestamp, symbol 列的普通 DataFrame（将自动设置索引）
              DataFrame with OHLC data. Supports both MultiIndex and flat column formats.
        label: 收益率标签 / Return label.
            'close2close': (next_close / current_close) - 1
            'open2open':   (next_open / current_open) - 1

    Returns / 返回:
        pd.Series: T+1 前向收益，MultiIndex (timestamp, symbol)。
                   每个交易对的最后一期收益为 NaN（无下一期数据）。
                   T+1 forward returns with MultiIndex (timestamp, symbol).
                   Last period for each symbol is NaN (no next period data).
    """
    if label not in _VALID_LABELS:
        raise ValueError(
            f"Invalid label '{label}'. Must be one of {_VALID_LABELS}."
            f"\n无效标签 '{label}'，必须是 {_VALID_LABELS} 之一。"
        )

    # 确定使用哪一列价格 / Determine which price column to use
    price_col = "close" if label == "close2close" else "open"

    # 检查所需列是否存在 / Check required columns exist
    missing = {price_col} - set(data.columns)
    if missing:
        raise ValueError(
            f"Missing columns: {missing}. Data must contain '{price_col}'."
            f"\n缺少列: {missing}，数据必须包含 '{price_col}'。"
        )

    # 构建工作副本 / Build working copy
    df = data.copy()

    # 处理索引：确保 (timestamp, symbol) MultiIndex / Ensure MultiIndex (timestamp, symbol)
    if not isinstance(df.index, pd.MultiIndex):
        if "timestamp" in df.columns and "symbol" in df.columns:
            df = df.set_index(["timestamp", "symbol"])
        else:
            raise ValueError(
                "Data must have (timestamp, symbol) MultiIndex or contain "
                "'timestamp' and 'symbol' columns."
                "\n数据必须有 (timestamp, symbol) MultiIndex 或包含 "
                "'timestamp' 和 'symbol' 列。"
            )

    # 按 symbol 分组，计算 T+1 前向收益 / Group by symbol, compute T+1 forward returns
    # shift(-1) 取下一期的价格 / shift(-1) gets next period's price
    prices = df[price_col]
    next_prices = prices.groupby(level=1).shift(-1)

    # 前向收益 = (下一期价格 / 当前价格) - 1 / Forward return = (next_price / current_price) - 1
    # 分母为零保护 / Division-by-zero protection
    forward_returns = np.where(
        prices == 0,
        np.nan,
        next_prices / prices - 1,
    )

    result = pd.Series(
        forward_returns,
        index=df.index,
        name=f"{label}_return",
    )

    # 统计无效值比例 / Report invalid ratio
    total = len(result)
    nan_count = result.isna().sum()
    # 最后一期的 NaN 是预期的 / NaN at last period is expected
    expected_nan = df.groupby(level=1).size().sum()  # 每组一个预期 NaN

    if nan_count > expected_nan + 0:
        actual_extra = nan_count - expected_nan
        warnings.warn(
            f"Extra NaN in returns: {actual_extra}/{total} "
            f"({actual_extra / total:.1%}). Check for zero prices or data gaps."
            f"\n收益中额外 NaN: {actual_extra}/{total} "
            f"({actual_extra / total:.1%})。请检查零价格或数据缺失。",
            UserWarning,
            stacklevel=2,
        )

    return result
