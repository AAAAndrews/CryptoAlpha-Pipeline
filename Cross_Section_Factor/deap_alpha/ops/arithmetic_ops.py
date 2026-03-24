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

def abs(x):
    return np.abs(x)

def inverse(x):
    """
    Safely take the reciprocal function to avoid dividing by zero.
    If there are zero elements in x, a default value (such as 0 or NaN) is returned.
    """
    # Use np.where to determine whether the denominator is zero
    return np.where(x == 0, 1e6, np.divide(1, x))  # If x is zero, return 1e6


def divide(x,y):
    """
    Safe division function to avoid dividing by zero.
    If there are zero elements in b, a default value (such as 0 or NaN) is returned.
    """
    # Use np.where to determine whether the denominator is zero
    return np.where(y == 0, 1e6, np.divide(x, y))  # If b is zero, return 1e6

def sqrt(x):
    return np.sqrt(x)

def reverse(x):
    return np.negative(x)

def maximum(*X):
    """
    Stack multiple vectors and take the maximum value
    """
    stacked = np.stack(X, axis=0)
    return np.max(stacked, axis=0)

def minimum(*X):
    """
    Stack multiple vectors and find the minimum value
    """
    stacked = np.stack(X, axis=0)
    return np.min(stacked, axis=0)

def s_log_1p(x):
    """
    Logarithmic scaling
    """
    return 2 / (1 + np.exp(-np.log1p(np.abs(x)))) - 1

def log(x):
    return np.log(x)

def sign(x):
    return np.sign(x)

def signed_power(x, a):
    return np.sign(x) * (np.abs(x) ** a)





from numpy import ndarray

arithmetic_function={

    "add":[add, [ndarray, ndarray], ndarray],
    "multiply":[multiply, [ndarray, ndarray], ndarray],
    "abs":[abs,[ndarray], ndarray],
    "subtract":[subtract,[ndarray, ndarray], ndarray],
    "inverse":[inverse,[ndarray], ndarray],
    "divide":[divide,[ndarray, ndarray], ndarray],
    "sqrt":[sqrt,[ndarray], ndarray],
    "reverse":[reverse,[ndarray], ndarray],
    "maximum":[maximum,[ndarray, ndarray], ndarray],
    "minimum":[minimum,[ndarray, ndarray], ndarray],
    "s_log_1p":[s_log_1p,[ndarray], ndarray],
    "log":[log,[ndarray], ndarray],
    "sign":[sign,[ndarray], ndarray],
    "signed_power":[signed_power,[ndarray, float], ndarray],

}





