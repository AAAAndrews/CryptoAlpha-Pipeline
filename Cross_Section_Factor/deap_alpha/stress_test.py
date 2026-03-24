import numpy as np
import pandas as pd
import sys
import os

# Add path for import
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from ops import arithmetic_ops, cross_section_ops, timeseries_ops
from fitness_funcs.metrics import factor, performance

def generate_stress_data(shape=(50, 100)):
    """
    Generate test data containing extreme cases such as NaN, Inf, -Inf, Zero, etc.
    shape: (assets, time)
    """
    data = np.random.randn(*shape)
    
    # Inject NaN
    nan_indices = np.random.choice(data.size, size=int(data.size * 0.2), replace=False)
    data.flat[nan_indices] = np.nan
    
    # Inject Inf/-Inf
    inf_indices = np.random.choice(data.size, size=int(data.size * 0.05), replace=False)
    data.flat[inf_indices] = np.inf
    neginf_indices = np.random.choice(data.size, size=int(data.size * 0.05), replace=False)
    data.flat[neginf_indices] = -np.inf
    
    # Inject Zero
    zero_indices = np.random.choice(data.size, size=int(data.size * 0.1), replace=False)
    data.flat[zero_indices] = 0.0
    
    # Inject large areas of identical data (Constant values)
    # 1. Simulates the same values ​​resulting from a market-wide suspension or missing data
    data[2:5, :] = 1.23  # Consecutive rows (assets) have the same value at all points in time
    # 2. Simulate that all assets have the same value at a certain point in time
    data[:, 10:15] = 0.0  # All asset values ​​are 0 for several consecutive time points
    # 3. Random large areas of the same value
    data[10:20, 20:40] = -0.5
    # 4. Minimal difference data (testing stability under floating point precision)
    data[25, 50:60] = 1.000000000000001
    data[26, 50:60] = 1.000000000000002
    
    # 5. Simulate illiquid assets (value is 0 or a fixed value most of the time)
    data[30:40, :] = 0.0
    # 6. Simulate a large number of assets having exactly the same factor values ​​over certain time periods (e.g. due to a certain classification)
    t_start = int(data.shape[1] * 0.7)
    t_end = int(data.shape[1] * 0.8)
    for t in range(t_start, t_end):
        data[10:40, t] = np.random.choice([0.1, 0.2, 0.3])
    
    # Simulate delisting (full row of NaN)
    data[0, :] = np.nan
    # Simulate new listing (NaN in first half)
    data[1, :50] = np.nan
    
    return data

def test_ops():
    print("=== Testing Arithmetic Ops ===")
    data1 = generate_stress_data()
    data2 = generate_stress_data()
    
    ops_to_test = [
        (arithmetic_ops.add, (data1, data2)),
        (arithmetic_ops.subtract, (data1, data2)),
        (arithmetic_ops.multiply, (data1, data2)),
        (arithmetic_ops.divide, (data1, data2)),
        (arithmetic_ops.abs, (data1,)),
        (arithmetic_ops.inverse, (data1,)),
        (arithmetic_ops.sqrt, (np.abs(data1),)), # sqrt needs non-negative
        (arithmetic_ops.log, (np.abs(data1) + 1e-5,)),
        (arithmetic_ops.sign, (data1,)),
        (arithmetic_ops.s_log_1p, (data1,)),
    ]
    
    for op, args in ops_to_test:
        try:
            res = op(*args)
            print(f"SUCCESS: {op.__name__}, Output shape: {res.shape}, Has NaN: {np.isnan(res).any()}")
        except Exception as e:
            print(f"FAILED: {op.__name__}, Error: {e}")

    print("\n=== Testing Cross Section Ops ===")
    cs_ops = [
        (cross_section_ops.rank, (data1,)),
        (cross_section_ops.normalize, (data1,)),
        (cross_section_ops.quantile, (data1,)),
        (cross_section_ops.scale, (data1,)),
        (cross_section_ops.winsorize, (data1,)),
        (cross_section_ops.zscore, (data1,)),
    ]
    for op, args in cs_ops:
        try:
            res = op(*args)
            print(f"SUCCESS: {op.__name__}, Output shape: {res.shape}, Has NaN: {np.isnan(res).any()}")
        except Exception as e:
            print(f"FAILED: {op.__name__}, Error: {e}")

    print("\n=== Testing Time Series Ops ===")
    ts_ops_to_test = [
        (timeseries_ops.ts_delay, (data1, 5)),
        (timeseries_ops.ts_delta, (data1, 5)),
        (timeseries_ops.ts_min, (data1, 10)),
        (timeseries_ops.ts_max, (data1, 10)),
        (timeseries_ops.ts_arg_min, (data1, 10)),
        (timeseries_ops.ts_arg_max, (data1, 10)),
        (timeseries_ops.ts_rank, (data1, 10)),
        (timeseries_ops.ts_corr, (data1, data2, 10)),
        (timeseries_ops.ts_cov, (data1, data2, 10)),
    ]
    for op, args in ts_ops_to_test:
        try:
            res = op(*args)
            print(f"SUCCESS: {op.__name__}, Output shape: {res.shape}, Has NaN: {np.isnan(res).any()}")
        except Exception as e:
            print(f"FAILED: {op.__name__}, Error: {e}")

def test_metrics():
    print("\n=== Testing Metrics ===")
    # Metrics expect (time, assets)
    factor_data = generate_stress_data((100, 50))
    returns_data = generate_stress_data((100, 50))
    
    metrics = [
        (factor.ic, (factor_data, returns_data)),
        (factor.rankic, (factor_data, returns_data)),
        (factor.icir, (factor_data, returns_data)),
        (performance.top_k_returns, (factor_data, returns_data)),
        (performance.calculate_monotonicity, (factor_data, returns_data)),
        (factor.turnover, (factor_data,)),
    ]
    
    for op, args in metrics:
        try:
            res = op(*args)
            print(f"SUCCESS: {op.__name__}, Result: {res}")
        except Exception as e:
            print(f"FAILED: {op.__name__}, Error: {e}")

if __name__ == "__main__":
    # Ignore some expected warnings
    import warnings
    warnings.filterwarnings('ignore')
    
    test_ops()
    test_metrics()
