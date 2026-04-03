"""
验证测试 — Task 2: FactorEvaluator chunk_size 参数与 _validate_chunk_size 校验
"""

import sys
import traceback
import numpy as np
import pandas as pd

checks = 0


def ok(label: str, condition: bool):
    global checks
    checks += 1
    status = "PASS" if condition else "FAIL"
    print(f"  [{status}] {label}")
    if not condition:
        traceback.print_stack()


def make_synthetic(n_dates=60, n_symbols=50, seed=42):
    """生成合成因子和收益率 / Generate synthetic factor and returns."""
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2025-01-01", periods=n_dates, freq="B")
    symbols = [f"S{i:03d}" for i in range(n_symbols)]
    idx = pd.MultiIndex.from_product([dates, symbols], names=["timestamp", "symbol"])

    true_signal = rng.standard_normal((n_dates, n_symbols))
    noise = rng.standard_normal((n_dates, n_symbols)) * 0.5
    factor_values = true_signal + noise
    returns_values = true_signal * 0.02 + rng.standard_normal((n_dates, n_symbols)) * 0.03

    factor = pd.Series(factor_values.ravel(), index=idx, dtype=np.float64)
    returns = pd.Series(returns_values.ravel(), index=idx, dtype=np.float64)
    return factor, returns


try:
    print("=== Task 2: FactorEvaluator chunk_size 参数 ===\n")

    from FactorAnalysis import FactorEvaluator

    factor, returns = make_synthetic()

    # 1. 默认值 None（全量模式）/ default None (full mode)
    print("1. 默认值 None（全量模式）")
    ev = FactorEvaluator(factor, returns)
    ok("chunk_size 默认 None", ev.chunk_size is None)
    ok("chunk_size 属性可访问", hasattr(ev, "chunk_size"))

    # 2. 正整数 chunk_size / positive integer chunk_size
    print("\n2. 正整数 chunk_size")
    ev_chunk = FactorEvaluator(factor, returns, chunk_size=10)
    ok("chunk_size=10 存储正确", ev_chunk.chunk_size == 10)
    ev_chunk1 = FactorEvaluator(factor, returns, chunk_size=1)
    ok("chunk_size=1 允许", ev_chunk1.chunk_size == 1)
    ev_large = FactorEvaluator(factor, returns, chunk_size=10000)
    ok("chunk_size=10000 允许（大于数据量）", ev_large.chunk_size == 10000)

    # 3. 与其他参数共存 / coexists with other params
    print("\n3. 与其他参数共存")
    ev_combo = FactorEvaluator(
        factor, returns,
        n_groups=3, top_k=2, bottom_k=2,
        cost_rate=0.005, risk_free_rate=0.02,
        periods_per_year=365, chunk_size=20,
    )
    ok("chunk_size=20 存储正确", ev_combo.chunk_size == 20)
    ok("n_groups=3 存储正确", ev_combo.n_groups == 3)
    ok("cost_rate=0.005 存储正确", ev_combo.cost_rate == 0.005)

    # 4. 向后兼容：不传 chunk_size 时行为不变 / backward compatible
    print("\n4. 向后兼容")
    ev_compat = FactorEvaluator(factor, returns)
    ok("不传 chunk_size 时为 None", ev_compat.chunk_size is None)
    # run() 应正常工作 / run() should work normally
    ev_compat.run()
    ok("run() 正常完成", ev_compat.ic is not None)
    ok("run() icir 有值", ev_compat.icir is not None)

    # 5. _validate_chunk_size 静态方法 / static method
    print("\n5. _validate_chunk_size 静态方法")
    ok("None → None", FactorEvaluator._validate_chunk_size(None) is None)
    ok("10 → 10", FactorEvaluator._validate_chunk_size(10) == 10)
    ok("1 → 1", FactorEvaluator._validate_chunk_size(1) == 1)

    # 6. 浮点整数（如 5.0）应被接受并转为 int / float-like integer accepted
    print("\n6. 浮点整数转换")
    ok("5.0 → 5", FactorEvaluator._validate_chunk_size(5.0) == 5)
    ok("1.0 → 1", FactorEvaluator._validate_chunk_size(1.0) == 1)
    ev_float = FactorEvaluator(factor, returns, chunk_size=5.0)
    ok("chunk_size=5.0 实例化成功", ev_float.chunk_size == 5)

    # 7. 非法值：0 / invalid: 0
    print("\n7. 非法值校验")
    try:
        FactorEvaluator._validate_chunk_size(0)
        ok("chunk_size=0 应抛出 ValueError", False)
    except ValueError:
        ok("chunk_size=0 抛出 ValueError", True)
    except Exception as e:
        ok(f"chunk_size=0 抛出意外异常: {type(e).__name__}", False)

    # 8. 非法值：负数 / invalid: negative
    try:
        FactorEvaluator._validate_chunk_size(-5)
        ok("chunk_size=-5 应抛出 ValueError", False)
    except ValueError:
        ok("chunk_size=-5 抛出 ValueError", True)
    except Exception as e:
        ok(f"chunk_size=-5 抛出意外异常: {type(e).__name__}", False)

    # 9. 非法值：非整数浮点 / invalid: non-integer float
    try:
        FactorEvaluator._validate_chunk_size(3.5)
        ok("chunk_size=3.5 应抛出 ValueError", False)
    except ValueError:
        ok("chunk_size=3.5 抛出 ValueError", True)
    except Exception as e:
        ok(f"chunk_size=3.5 抛出意外异常: {type(e).__name__}", False)

    # 10. 非法值：字符串 / invalid: string
    try:
        FactorEvaluator._validate_chunk_size("10")
        ok('chunk_size="10" 应抛出 TypeError', False)
    except TypeError:
        ok('chunk_size="10" 抛出 TypeError', True)
    except Exception as e:
        ok(f'chunk_size="10" 抛出意外异常: {type(e).__name__}', False)

    # 11. 非法值：bool / invalid: bool (bool 是 int 的子类，应拒绝)
    try:
        FactorEvaluator._validate_chunk_size(True)
        ok("chunk_size=True 应抛出 TypeError", False)
    except TypeError:
        ok("chunk_size=True 抛出 TypeError", True)
    except Exception as e:
        ok(f"chunk_size=True 抛出意外异常: {type(e).__name__}", False)

    # 12. 非法值：空列表 / invalid: empty list
    try:
        FactorEvaluator._validate_chunk_size([])
        ok("chunk_size=[] 应抛出 TypeError", False)
    except TypeError:
        ok("chunk_size=[] 抛出 TypeError", True)
    except Exception as e:
        ok(f"chunk_size=[] 抛出意外异常: {type(e).__name__}", False)

    # 13. 构造函数中的非法值传播 / constructor propagates validation errors
    print("\n13. 构造函数非法值传播")
    try:
        FactorEvaluator(factor, returns, chunk_size=0)
        ok("构造函数 chunk_size=0 应抛出 ValueError", False)
    except ValueError:
        ok("构造函数 chunk_size=0 抛出 ValueError", True)

    try:
        FactorEvaluator(factor, returns, chunk_size=-1)
        ok("构造函数 chunk_size=-1 应抛出 ValueError", False)
    except ValueError:
        ok("构造函数 chunk_size=-1 抛出 ValueError", True)

    try:
        FactorEvaluator(factor, returns, chunk_size="abc")
        ok('构造函数 chunk_size="abc" 应抛出 TypeError', False)
    except TypeError:
        ok('构造函数 chunk_size="abc" 抛出 TypeError', True)

    # 14. chunk_size 不影响全量模式的计算结果 / chunk_size=None doesn't affect results
    print("\n14. chunk_size=None 不影响计算结果")
    ev_a = FactorEvaluator(factor, returns)
    ev_a.run()
    ev_b = FactorEvaluator(factor, returns, chunk_size=None)
    ev_b.run()
    ok("IC 一致", np.allclose(ev_a.ic.values, ev_b.ic.values, equal_nan=True))
    ok("ICIR 一致", ev_a.icir == ev_b.icir or (
        np.isnan(ev_a.icir) and np.isnan(ev_b.icir)))
    ok("hedge_curve 一致", np.allclose(
        ev_a.hedge_curve.values, ev_b.hedge_curve.values, equal_nan=True))

    print(f"\n{'='*40}")
    print(f"Total: {checks} checks")

except Exception as e:
    print(f"\nFATAL: {e}")
    traceback.print_exc()
    sys.exit(1)
