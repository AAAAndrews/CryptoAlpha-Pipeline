import numpy as np
import pandas as pd

from scipy.stats import rankdata
from numpy.lib.stride_tricks import sliding_window_view


def rank(x):
    col_list = []
    for t in range(np.size(x, 1)):
        col = x[:, t]
        col = pd.Series(col.flatten()).rank().to_numpy()
        col_list.append(col)
    return np.asarray(col_list).transpose((1, 0))

def add(x,y):
    return np.add(x,y)

def multiply(x,y):
    return np.multiply(x,y)

def subtract(x,y):
    return np.subtract(x,y)

def fabs(x):
    return np.fabs(x)

def divide(x,y):
    """
    Safe division function to avoid dividing by zero.
    If there are zero elements in b, a default value (such as 0 or NaN) is returned.
    """
    # Use np.where to determine whether the denominator is zero
    return np.where(y == 0, 1e16, np.divide(x, y))  # If b is zero, return 1e16

def sqrt(x):
    return np.sqrt(x)


def delay(x, d):
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

def delta(x, d):
    value = x - delay(x, d)
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


def ts_argmin(x, d):
    # Create a sliding window view and perform sliding window operations only in the column direction
    windowed_view = sliding_window_view(x, window_shape=d, axis=1)
    # Calculate argmin for each window and adjust index
    result = np.argmin(windowed_view, axis=-1) + 1
    # Create an output array with the same shape as the input array x, filled with NaN
    output = np.full_like(x, fill_value=np.nan, dtype=float)
    # Populate the calculation results into the output array, starting at column d
    output[:, d - 1:] = result
    return output


def ts_argmax(x, d):
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


def ts_stddev(x, d):
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

def get3():
    return 3

def get6():
    return 6

def get9():
    return 9

def get12():
    return 12

def get18():
    return 18

def get24():
    return 24

def get5():
    return 5

def get10():
    return 10

def get15():
    return 15

def get20():
    return 20

def get30():
    return 30

def get60():
    return 60

def get90():
    return 90

def get120():
    return 120


from numpy import ndarray

cross_section_function={
    "rank":[rank, [ndarray], ndarray],
    "add":[add, [ndarray, ndarray], ndarray],
    "multiply":[multiply, [ndarray, ndarray], ndarray],
    "fabs":[fabs,[ndarray], ndarray],
    "subtract":[subtract,[ndarray, ndarray], ndarray]
}

timeseries_function = {
    "delay": [delay, [ndarray, int], ndarray],
    "ts_cov": [ts_cov, [ndarray, ndarray, int], ndarray],
    "ts_corr":[ts_corr, [ndarray, ndarray, int], ndarray],
    "ts_min": [ts_min, [ndarray, int], ndarray],
    "ts_max": [ts_max, [ndarray, int], ndarray],
    "ts_argmin": [ts_argmin, [ndarray, int], ndarray],
    "ts_argmax": [ts_argmax, [ndarray, int], ndarray],
    "ts_rank": [ts_rank, [ndarray, int], ndarray],
    "ts_sum": [ts_sum, [ndarray, int], ndarray],
    "ts_stddev": [ts_stddev, [ndarray, int], ndarray],
    "rolling_mean": [rolling_mean, [ndarray, int], ndarray],
    "ts_zscore": [ts_zscore, [ndarray, int], ndarray],
}



# Completion Dictionary
constant_function = {
    "3": [get3, [], int],
    "6": [get6, [], int],
    "9": [get9, [], int],
    "12": [get12, [], int],
    "18": [get18, [], int],
    "24": [get24, [], int],
    "5": [get5, [], int],
    "10": [get10, [], int],
    "15": [get15, [], int],
    "20": [get20, [], int],
    "30": [get30, [], int],
    "60": [get60, [], int],
    "90": [get90, [], int],
    "120": [get120, [], int],
}