import numpy as np
import pandas as pd

from scipy.stats import rankdata
from numpy.lib.stride_tricks import sliding_window_view

def ts_delay(x, d):
    row_list = []
    for st in range(np.size(x, 0)):
        row = x[st, :]
        row = pd.Series(row.flatten()).shift(d).to_numpy()
        row_list.append(row)
    return np.asarray(row_list)

def ts_corr(x, y, d):
    row_list = []
    for st in range(np.size(x, 0)):
        x_row = x[st, :]
        y_row = y[st, :]
        row = pd.Series(x_row.flatten()).rolling(d).corr(pd.Series(y_row)).to_numpy()
        row_list.append(row)
    return np.round(np.asarray(row_list),5)

def ts_cov(x, y, d):
    row_list = []
    for st in range(np.size(x, 0)):
        x_row = x[st, :]
        y_row = y[st, :]
        row = pd.Series(x_row.flatten()).rolling(d).cov(pd.Series(y_row)).to_numpy()
        row_list.append(row)
    return np.round(np.asarray(row_list),5)

def ts_delta(x, d):
    value = x - ts_delay(x, d)
    return value


def ts_min(x, d):
    row_list = []
    for st in range(np.size(x, 0)):
        row = x[st, :]
        row = pd.Series(row.flatten()).rolling(d).min().to_numpy()
        row_list.append(row)
    return np.asarray(row_list)


def ts_max(x, d):
    row_list = []
    for st in range(np.size(x, 0)):
        row = x[st, :]
        row = pd.Series(row.flatten()).rolling(d).max().to_numpy()
        row_list.append(row)
    return np.asarray(row_list)


def ts_arg_min(x, d):
    # Create a sliding window view and perform sliding window operations only in the column direction
    windowed_view = sliding_window_view(x, window_shape=d, axis=1)
    # Calculate argmin for each window and adjust index
    result = np.argmin(windowed_view, axis=-1) + 1
    # Create an output array with the same shape as the input array x, filled with NaN
    output = np.full_like(x, fill_value=np.nan, dtype=float)
    # Populate the calculation results into the output array, starting at column d
    output[:, d - 1:] = result
    return output


def ts_arg_max(x, d):
    # Create a sliding window view and perform sliding window operations only in the column direction
    windowed_view = sliding_window_view(x, window_shape=d, axis=1)
    # Calculate argmin for each window and adjust index
    result = np.argmax(windowed_view, axis=-1) + 1
    # Create an output array with the same shape as the input array x, filled with NaN
    output = np.full_like(x, fill_value=np.nan, dtype=float)
    # Populate the calculation results into the output array, starting at column d
    output[:, d - 1:] = result
    return output

def ts_rank(x, d):
    # Create a sliding window view and perform sliding window operations only in the column direction
    windowed_view = sliding_window_view(x, window_shape=d, axis=1)
    # Initialize the output array with the same shape as the input array x and fill it with NaN
    output = np.full_like(x, fill_value=np.nan, dtype=float)
    # Iterate through each row
    for i in range(x.shape[0]):
        # Rank the values ​​within each window
        ranks = np.argsort(np.argsort(windowed_view[i], axis=-1), axis=-1) + 1
        # Extract the ranking of the last element of each window
        output[i, d - 1:] = ranks[:, -1]
    return output

def rolling_rank(x):
    value = rankdata(x)[-1]
    return value


# def rolling_prod(x):
#     return np.prod(x)


def ts_sum(x, d):
    row_list = []
    for st in range(np.size(x, 0)):
        row = x[st, :]
        row = pd.Series(row.flatten()).rolling(d).sum().to_numpy()
        row_list.append(row)
    return np.asarray(row_list)


# def ts_prod(x, d):
#     row_list = []
#     for st in range(np.size(x, 0)):
#         row = x[st, :]
#         row = pd.Series(row.flatten()).rolling(d).apply(rolling_prod).to_numpy()
#         row_list.append(row)
#     return np.asarray(row_list)


def ts_std_dev(x, d):
    row_list = []
    for st in range(np.size(x, 0)):
        row = x[st, :]
        row = pd.Series(row.flatten()).rolling(d).std().to_numpy()
        row_list.append(row)
    return np.asarray(row_list)


def ts_zscore(x, d):
    row_list = []
    for st in range(np.size(x, 0)):
        row = x[st, :]
        mean = pd.Series(row.flatten()).rolling(d).mean()
        std = pd.Series(row.flatten()).rolling(d).std()
        row = (row - mean) / std
        row_list.append(row.to_numpy())
    return np.asarray(row_list)




def rolling_mean(x, d):
    row_list = []
    for st in range(np.size(x, 0)):
        row = x[st, :]
        row = pd.Series(row.flatten()).rolling(d).mean().to_numpy()
        row_list.append(row)
    return np.asarray(row_list)

def ts_mean(x, d):
    return rolling_mean(x, d)

def days_from_last_change(x):
    """
    Calculate the number of days since the last change
    Speed ​​up calculations using vectorized operations
    """
    rows, cols = x.shape
    result = np.zeros((rows, cols), dtype=int)
    
    for i in range(rows):
        # Vectorize each row
        row = x[i, :]
        
        # Detect change points: where the current value differs from the previous value
        changes = np.concatenate(([True], row[1:] != row[:-1]))
        
        # Get the index of the change point
        change_indices = np.where(changes)[0]
        
        # Use searchsorted to quickly find the nearest change point for each position
        nearest_change = np.searchsorted(change_indices, np.arange(cols), side='right') - 1
        
        # Calculate distance
        result[i, :] = np.arange(cols) - change_indices[nearest_change]
    
    return result   


def hump(x: np.ndarray, hump: float = 0.01) -> np.ndarray:
    """
    Fully vectorized version (using numba or pure numpy iteration strategy)
    Note: Due to the existence of recursive dependencies, complete vectorization is difficult. It is recommended to use numba.jit for acceleration.
    """
    from numba import jit
    
    @jit(nopython=True)
    def _hump_core(x, hump):
        result = np.empty_like(x)
        result[:, 0] = x[:, 0]
        
        for i in range(1, x.shape[1]):
            diffs = x[:, i] - result[:, i-1]
            # Vectorization limitations
            clipped = np.where(diffs > hump, hump, 
                              np.where(diffs < -hump, -hump, diffs))
            result[:, i] = result[:, i-1] + clipped
        
        return result
    
    x = np.asarray(x)
    if x.ndim == 1:
        x = x.reshape(1, -1)
        return _hump_core(x, hump).ravel()
    return _hump_core(x, hump)

def kth_element(x: np.ndarray, d: int, k: int = 1) -> np.ndarray:
    """
    Returns the kth value within the past d days
    
    parameter:
    - x: input sequence
    - d: Number of days to look back
    - k: The kth value (1 represents the first non-NaN value, used for data backfill)
    
    return:
    - An array consisting of the kth value of each position in the past d days
    """
    x = np.asarray(x)
    n = len(x)
    result = np.full(n, np.nan)
    
    for i in range(n):
        # Confirm lookback window
        start_idx = max(0, i - d + 1)
        window = x[start_idx:i+1]
        
        # Remove NaN values
        valid_values = window[~np.isnan(window)]
        
        if len(valid_values) >= k:
            # Get the kth value (k starts from 1)
            result[i] = valid_values[k-1]
        elif len(valid_values) > 0:
            # If the number of valid values ​​is less than k, return the last valid value
            result[i] = valid_values[-1]
        else:
            # If there is no valid value, remain NaN
            result[i] = np.nan
    
    return result

def last_diff_value(x: np.ndarray, d: int) -> np.ndarray:
    """
    Returns the last x value within the past d days that is different from the current value
    Implementing height vectorization using sliding_window_view
    
    parameter:
    - x: Input sequence (1D or 2D array)
    - d: Number of days to look back
    
    return:
    - The last value of each location in the past d days that is different from the current value
    """
    x = np.asarray(x, dtype=float)
    
    if x.ndim == 1:
        n = len(x)
        result = np.full(n, np.nan)
        
        # Extend array to handle bounds
        padded = np.pad(x, (d, 0), mode='constant', constant_values=np.nan)
        
        # Create sliding window view (n, d)
        windows = sliding_window_view(padded, window_shape=d + 1)[:-1]
        
        # The current value is the last element of each window
        current_vals = windows[:, -1]
        # The historical value is the previous element
        hist_vals = windows[:, :-1]
        
        # Vectorized comparison of each window
        for i in range(n):
            current = current_vals[i]
            history = hist_vals[i]
            
            # find different values
            if np.isnan(current):
                mask = ~np.isnan(history)
            else:
                mask = np.isnan(history) | (~np.isclose(history, current, equal_nan=False))
            
            # Find the first one that meets the conditions from back to front
            if np.any(mask):
                indices = np.where(mask)[0]
                result[i] = history[indices[-1]]
        
        return result
    
    elif x.ndim == 2:
        rows, cols = x.shape
        result = np.full((rows, cols), np.nan)
        
        for row_idx in range(rows):
            result[row_idx, :] = last_diff_value(x[row_idx, :], d)
        
        return result
    
    else:
        raise ValueError(f"Unsupported array dimensions:{x.ndim}")

def last_diff_value_numba(x: np.ndarray, d: int) -> np.ndarray:
    """
    Returns the last x value within the past d days that is different from the current value
    Use numba JIT compilation acceleration
    """
    from numba import jit
    
    @jit(nopython=True)
    def _last_diff_1d(x, d):
        n = len(x)
        result = np.full(n, np.nan)
        
        for i in range(n):
            current_val = x[i]
            current_is_nan = np.isnan(current_val)
            
            # Search from back to front
            for j in range(i - 1, max(-1, i - d - 1), -1):
                if j < 0:
                    break
                
                hist_val = x[j]
                hist_is_nan = np.isnan(hist_val)
                
                # Determine whether it is different
                if current_is_nan and hist_is_nan:
                    continue
                elif current_is_nan or hist_is_nan:
                    result[i] = hist_val
                    break
                elif abs(hist_val - current_val) > 1e-9:  # Not equal
                    result[i] = hist_val
                    break
        
        return result
    
    @jit(nopython=True)
    def _last_diff_2d(x, d):
        rows, cols = x.shape
        result = np.full((rows, cols), np.nan)
        
        for row in range(rows):
            for i in range(cols):
                current_val = x[row, i]
                current_is_nan = np.isnan(current_val)
                
                for j in range(i - 1, max(-1, i - d - 1), -1):
                    if j < 0:
                        break
                    
                    hist_val = x[row, j]
                    hist_is_nan = np.isnan(hist_val)
                    
                    if current_is_nan and hist_is_nan:
                        continue
                    elif current_is_nan or hist_is_nan:
                        result[row, i] = hist_val
                        break
                    elif abs(hist_val - current_val) > 1e-9:
                        result[row, i] = hist_val
                        break
        
        return result
    
    x = np.asarray(x, dtype=np.float64)
    
    if x.ndim == 1:
        return _last_diff_1d(x, d)
    elif x.ndim == 2:
        return _last_diff_2d(x, d)
    else:
        raise ValueError(f"Unsupported array dimensions:{x.ndim}")
    

from numpy import ndarray
timeseries_function = {
    "ts_delay": [ts_delay, [ndarray, int], ndarray],
    "ts_cov": [ts_cov, [ndarray, ndarray, int], ndarray],
    "ts_corr":[ts_corr, [ndarray, ndarray, int], ndarray],
    "ts_min": [ts_min, [ndarray, int], ndarray],
    "ts_max": [ts_max, [ndarray, int], ndarray],
    "ts_argmin": [ts_arg_min, [ndarray, int], ndarray],
    "ts_argmax": [ts_arg_max, [ndarray, int], ndarray],
    "ts_rank": [ts_rank, [ndarray, int], ndarray],
    "ts_sum": [ts_sum, [ndarray, int], ndarray],
    "ts_stddev": [ts_std_dev, [ndarray, int], ndarray],
    "ts_mean": [ts_mean, [ndarray, int], ndarray],
    "ts_zscore": [ts_zscore, [ndarray, int], ndarray],
}