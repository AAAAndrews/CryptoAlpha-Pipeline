import numpy as np
from scipy.stats import spearmanr
from numba import jit
import warnings


def ic(factor_values, returns):
    """
    Calculate the daily IC (information coefficient) between factor values ​​and future returns.

    parameter:
        factor_values (np.ndarray): Factor value matrix, shape is (timestamp, target name).
        returns (np.ndarray): Percentage rate of return matrix, shape is (timestamp, target name).
    return:
        ic_values (np.ndarray): Daily IC value array, the shape is (timestamp,).
    """
    n_days = factor_values.shape[0]
    ic_values = np.full(n_days, np.nan)
    for t in range(n_days):
        f = factor_values[t]
        r = returns[t]
        # Filter both NaN and Inf
        mask = np.isfinite(f) & np.isfinite(r)
        if np.sum(mask) > 1:
            # Calculate the Pearson correlation coefficient
            ic_values[t] = np.corrcoef(f[mask], r[mask])[0, 1]
    return ic_values


def rankic(factor_values, returns):
    """
    Calculate the daily rank correlation IC (information coefficient) between factor values ​​and future returns.

    parameter:
        factor_values (np.ndarray): Factor value matrix, shape is (timestamp, target name).
        returns (np.ndarray): Percentage rate of return matrix, shape is (timestamp, target name).
    return:
        rank_ic_values (np.ndarray): Array of daily rank correlation IC values ​​with shape (timestamp,).
    """
    n_days = factor_values.shape[0]
    rank_ic_values = np.full(n_days, np.nan)
    for t in range(n_days):
        f = factor_values[t]
        r = returns[t]
        # Filter both NaN and Inf
        mask = np.isfinite(f) & np.isfinite(r)
        if np.sum(mask) > 1:
            # Calculate Spearman's rank correlation coefficient
            res = spearmanr(f[mask], r[mask])
            rank_ic_values[t] = res.correlation if hasattr(res, 'correlation') else res[0]
    return rank_ic_values


def icir(factor_values, returns):
    """
    Calculate the ICIR (Information Coefficient Information Ratio) of the IC.

    parameter:
        factor_values (np.ndarray): Factor value matrix, shape is (timestamp, target name).
        returns (np.ndarray): Percentage rate of return matrix, shape is (timestamp, target name).
    return:
        icir_value (float): ICIRvalue.
    """
    ic_vals = ic(factor_values, returns)
    ic_vals = ic_vals[~np.isnan(ic_vals)]
    if len(ic_vals) == 0:
        return 0.0
    mean_ic = np.mean(ic_vals)
    std_ic = np.std(ic_vals)
    if std_ic == 0:
        return 0.0
    return mean_ic / std_ic


def turnover(factor_values):
    """
    Calculate the turnover rate of a factor.

    parameter:
        factor_values (np.ndarray): Factor value matrix, shape is (timestamp, target name).

    return:
        turnover_rate (float): The turnover rate of the factor.
    """
    n_days, n_assets = factor_values.shape
    if n_days < 2:
        return np.nan

    # Calculate daily turnover rate
    daily_turnover = np.zeros(n_days - 1)
    for t in range(1, n_days):
        prev_factors = factor_values[t - 1]
        curr_factors = factor_values[t]

        # Eliminate invalid values ​​(NaN and Inf)
        mask = np.isfinite(prev_factors) & np.isfinite(curr_factors)
        if np.sum(mask) == 0:
            daily_turnover[t - 1] = np.nan
            continue

        changes = np.abs(curr_factors[mask] - prev_factors[mask])
        prev_sum = np.sum(np.abs(prev_factors[mask]))
        if prev_sum == 0:
            daily_turnover[t - 1] = 0.0
        else:
            daily_turnover[t - 1] = np.sum(changes) / prev_sum

    # Calculate average turnover rate
    valid_turnover = daily_turnover[~np.isnan(daily_turnover)]
    if len(valid_turnover) == 0:
        return np.nan

    turnover_rate = np.mean(valid_turnover)
    return turnover_rate
