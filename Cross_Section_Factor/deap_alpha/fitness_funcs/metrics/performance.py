import numpy as np
from scipy.stats import spearmanr
from numba import jit
import warnings
from .utils import high_minus_low



def top_k_returns(factor_values, returns, k_percent=0.1):
    """
    Calculate top return (Top_R).
    Top_R = max(TopR - mean(totalR), FlopR - mean(totalR))
    Among them, TopR is the average return of the highest-ranked group, FlopR is the average return of the lowest-ranked group, and mean(totalR) is the average return of all targets.

    parameter:
        factor_values (np.ndarray): Factor value matrix, shape is (timestamp, target name).
        returns (np.ndarray): Percentage rate of return matrix, shape is (timestamp, target name).
        k_percent (float): Percentage used to select Top and Flop groups, default is 10%.

    return:
        top_k_score (float): Top return score (average over all periods).
    """
    n_days = factor_values.shape[0]
    daily_scores = np.full(n_days, np.nan)

    for t in range(n_days):
        f = factor_values[t]
        r = returns[t]
        
        # Eliminate invalid values ​​(NaN and Inf)
        mask = ~np.isnan(f) & ~np.isnan(r) & ~np.isinf(f) & ~np.isinf(r)
        f_valid = f[mask]
        r_valid = r[mask]
        
        n = len(f_valid)
        if n < 10:  # Skip when the number of samples is too small
            continue
            
        # Sort by factor value
        sorted_indices = np.argsort(f_valid)
        top_n = max(1, int(n * k_percent))
        
        # Get the highest and lowest ranked groups
        top_indices = sorted_indices[-top_n:]
        flop_indices = sorted_indices[:top_n]
        
        # Calculate the average income of each group and the average income of the entire sample
        top_r = np.mean(r_valid[top_indices])
        flop_r = np.mean(r_valid[flop_indices])
        mean_total_r = np.mean(r_valid)
        
        # Calculate the score for the day based on the formula
        daily_scores[t] = max(top_r - mean_total_r, flop_r - mean_total_r)
        
    # Remove invalid scores and return the mean
    valid_scores = daily_scores[~np.isnan(daily_scores)]
    if len(valid_scores) == 0:
        return np.nan
        
    return np.mean(valid_scores)


def calculate_monotonicity(factor_values, returns, n_groups=10):
    """
    Compute Monotonicity.
    Monotonicity = max(1/N * sum(max(0, Sign(R_k - R_{k+1}))), 1/N * sum(max(0, Sign(R_{k+1} - R_k))))
    where R_k represents the average return of the k-th group, and N is the number of groups.

    parameter:
        factor_values (np.ndarray): Factor value matrix, shape is (timestamp, target name).
        returns (np.ndarray): Percentage rate of return matrix, shape is (timestamp, target name).
        n_groups (int): Number of groups, default is 10.

    return:
        monotonicity_score (float): Monotonicity score (average over the entire period).
    """
    n_days = factor_values.shape[0]
    daily_scores = np.full(n_days, np.nan)

    for t in range(n_days):
        f = factor_values[t]
        r = returns[t]
        
        # Eliminate invalid values ​​(NaN and Inf)
        mask = ~np.isnan(f) & ~np.isnan(r) & ~np.isinf(f) & ~np.isinf(r)
        f_valid = f[mask]
        r_valid = r[mask]
        
        n_assets = len(f_valid)
        if n_assets < n_groups:  # The number of samples must be at least able to be divided into groups
            continue
            
        # Sort by factor value
        sorted_indices = np.argsort(f_valid)
        
        # Divide the target into n_groups groups
        indices_groups = np.array_split(sorted_indices, n_groups)
        
        group_returns = []
        for group_idx in indices_groups:
            if len(group_idx) > 0:
                group_returns.append(np.mean(r_valid[group_idx]))
        
        N = len(group_returns)
        if N < 2:
            continue
            
        group_returns = np.array(group_returns)
        
        # Calculate the difference between adjacent group returns
        # diffs[k] = R_{k+1} - R_k
        diffs = np.diff(group_returns)
        
        # Calculate forward monotonicity count: sum(max(0, Sign(R_{k+1} - R_k)))
        pos_mono_count = np.sum(diffs > 0)
        # Calculate the negative monotonicity count: sum(max(0, Sign(R_k - R_{k+1})))
        neg_mono_count = np.sum(diffs < 0)
        
        # Take the maximum value according to the formula and divide it by the number of groups N
        daily_scores[t] = max(neg_mono_count, pos_mono_count) / n_groups
        
    # Remove invalid scores and return the mean
    valid_scores = daily_scores[~np.isnan(daily_scores)]
    if len(valid_scores) == 0:
        return np.nan
        
    return np.mean(valid_scores)


def calculate_high_low_sharpe_ratio(factor_values, returns, risk_free_rate=0.01, top_bottom_percent=0.1):
    """
    Calculate the Sharpe ratio of log returns for the High-Low portfolio.

    parameter:
        factor_values (np.ndarray): Factor value matrix, shape is (timestamp, target name).
        returns (np.ndarray): Percentage rate of return matrix, shape is (timestamp, target name).
        risk_free_rate (float): Risk-free interest rate, defaults to 1%.
        top_bottom_percent (float): The percentage used to select the High and Low groups, default is 10%.

    return:
        sharpe_ratio (float): High-Low Sharpe ratio of the portfolio's log return.
    """

    log_returns = high_minus_low(factor_values, returns, top_bottom_percent)
    if len(log_returns)<750:
        return np.nan
    else:
        # Calculate the average log return and standard deviation of a portfolio
        avg_return = np.mean(log_returns)*np.sqrt(252)
        std_return = np.std(log_returns, ddof=1)*np.sqrt(252)  # Use sample standard deviation

        # Calculate Sharpe Ratio
        sharpe_ratio = (avg_return - np.log(risk_free_rate+1)) / std_return

        return sharpe_ratio
    

def calculate_high_low_calmar_ratio(factor_values, returns, top_bottom_percent=0.1):

    log_returns = high_minus_low(factor_values, returns, top_bottom_percent)
    # Remove NaN values
   
    if len(log_returns) < 750:
        return np.nan

    # Calculate annualized compound return (based on logarithmic return)
    avg_return = np.mean(log_returns)*np.sqrt(252)# annualized log return

    # Calculate maximum drawdown (cumulative equity based on log return)
    cumulative_log_returns = np.cumsum(log_returns)  # cumulative log return
    peak = np.maximum.accumulate(cumulative_log_returns)
    drawdown = cumulative_log_returns - peak
    max_drawdown = np.min(drawdown)

    # Avoid dividing by zero
    if max_drawdown == 0:
        return np.nan

    # Calculate Karma Ratio
    calmar_ratio = avg_return / abs(max_drawdown)
 
    return calmar_ratio



def calculate_high_low_sortino_ratio(factor_values, returns, risk_free_rate=0.01, top_bottom_percent=0.1):


    log_returns = high_minus_low(factor_values, returns, top_bottom_percent)
    if len(log_returns) < 750:
        return np.nan
    else:
        # Calculate the average logarithmic return of a portfolio
        avg_return = np.mean(log_returns) * np.sqrt(252)
        
        # Calculate downside standard deviation
        downside_returns = log_returns[log_returns < np.log(risk_free_rate+1)]
        downside_std = np.std(downside_returns, ddof=1) * np.sqrt(252)  # Use sample standard deviation

        # Calculate Sortino Ratio
        sortino_ratio = (avg_return - np.log(risk_free_rate+1)) / downside_std

        return sortino_ratio