"""验证 AlphaMomentum 因子 / Validate AlphaMomentum factor"""

import pandas as pd
import numpy as np
from FactorLib.alpha_momentum import AlphaMomentum
from FactorLib.base import BaseFactor


def test_all():
    # 构造合成数据 / build synthetic data
    dates = pd.date_range("2025-01-01", periods=20, freq="h")
    symbols = ["BTCUSDT", "ETHUSDT"]
    rows = []
    for sym in symbols:
        base = 100.0 if sym == "BTCUSDT" else 10.0
        prices = base + np.arange(20) * 0.5
        for i, d in enumerate(dates):
            rows.append({
                "timestamp": d, "symbol": sym,
                "open": prices[i], "high": prices[i] + 1,
                "low": prices[i] - 1, "close": prices[i],
            })
    df = pd.DataFrame(rows)

    # 1. 继承 / inheritance
    assert issubclass(AlphaMomentum, BaseFactor), "not subclass of BaseFactor"
    print("OK: AlphaMomentum is subclass of BaseFactor")

    # 2. 默认参数 / default params (lookback=10 → 前10行NaN/symbol)
    f1 = AlphaMomentum()
    out1 = f1.calculate(df)
    assert isinstance(out1, pd.Series), "not Series"
    expected_nan = 10 * len(symbols)  # lookback * num_symbols
    assert out1.isna().sum() == expected_nan, f"NaN count wrong: {out1.isna().sum()}, expected {expected_nan}"
    assert "AlphaMomentum" in f1.name, f"name wrong: {f1.name}"
    print(f"OK: AlphaMomentum default, NaN={out1.isna().sum()}, non-NaN={out1.notna().sum()}")

    # 3. 自定义 lookback / custom lookback (lookback=5 → 前5行NaN/symbol)
    f2 = AlphaMomentum(lookback=5)
    out2 = f2.calculate(df)
    expected_nan5 = 5 * len(symbols)
    assert out2.isna().sum() == expected_nan5, f"NaN count wrong: {out2.isna().sum()}, expected {expected_nan5}"
    print(f"OK: AlphaMomentum(lookback=5), NaN={out2.isna().sum()}, non-NaN={out2.notna().sum()}")

    # 4. 数值正确性 / numerical correctness
    # 数据顺序: BTC 0-19, ETH 20-39. BTC close: 100.0, 100.5, ..., 109.5
    # pct_change(5) at BTC index 5: (102.5 - 100.0) / 100.0 = 0.025
    btc_closes = df[df["symbol"] == "BTCUSDT"]["close"]
    expected_at_5 = (btc_closes.iloc[5] - btc_closes.iloc[0]) / btc_closes.iloc[0]
    actual = out2.iloc[5]
    assert abs(actual - expected_at_5) < 1e-10, f"value mismatch: {actual} vs {expected_at_5}"
    print(f"OK: Numerical correct: {actual:.6f} == {expected_at_5:.6f}")

    # 5. 公共导出 / public export
    from FactorLib import AlphaMomentum as AM
    assert AM is AlphaMomentum, "export mismatch"
    print("OK: FactorLib public export correct")

    # 6. repr
    r = repr(f2)
    assert "AlphaMomentum" in r, f"repr wrong: {r}"
    print(f"OK: repr = {r}")

    print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    test_all()
