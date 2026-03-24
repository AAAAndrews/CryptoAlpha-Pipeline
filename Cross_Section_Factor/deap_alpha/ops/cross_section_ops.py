import numpy as np
import pandas as pd

from scipy.stats import rankdata, norm, cauchy
from numpy.lib.stride_tricks import sliding_window_view
from numpy import ndarray

def rank(x):
    """
    Sort all tool inputs.
    Note: This function remains as is, returning the original ranking.
    """
    col_list = []
    for t in range(np.size(x, 1)):
        col = x[:, t]
        col = pd.Series(col.flatten()).rank().to_numpy()
        col_list.append(col)
    return np.asarray(col_list).transpose((1, 0))


def normalize(x, useStd=False, limit=0.0):
    """
    Calculate the mean of all valid alpha values ​​for a day and subtract the mean from each element.
    
    Args:
        x: Input array (assets, time).
        useStd: Whether to divide by the standard deviation.
        limit: Truncation limit for results.
    """
    col_list = []
    for t in range(np.size(x, 1)):
        col = x[:, t]
        mu = np.nanmean(col)
        res = col - mu
        if useStd:
            std = np.nanstd(col)
            if std > 0:
                res = res / std
        if limit > 0:
            res = np.clip(res, -limit, limit)
        col_list.append(res)
    return np.asarray(col_list).transpose((1, 0))


def quantile(x, driver='gaussian', sigma=1.0):
    """
    Sort the original vectors, translate the sorted alpha vectors, and apply the distribution.
    
    Args:
        x: Input array (assets, time).
        driver: distribution type ('gaussian', 'cauchy', 'uniform')。
        sigma: The standard deviation or scaling parameter of the distribution.
    """
    col_list = []
    for t in range(np.size(x, 1)):
        col = x[:, t]
        if driver == 'uniform':
            # If the driver is uniform, the mean value of the Alpha vector is directly subtracted.
            res = col - np.nanmean(col)
        else:
            n = np.count_nonzero(~np.isnan(col))
            if n == 0:
                res = col
            else:
                r = pd.Series(col).rank(method='average', pct=True).to_numpy()
                # Avoid inf in ppf
                r = np.clip(r, 1/(2*n), 1 - 1/(2*n))
                if driver == 'gaussian':
                    res = norm.ppf(r, scale=sigma)
                elif driver == 'cauchy':
                    res = cauchy.ppf(r, scale=sigma)
                else:
                    res = r
        col_list.append(res)
    return np.asarray(col_list).transpose((1, 0))

def scale(x, scale=1, longscale=1, shortscale=1):
    """
    Scale the input to the target size.
    
    Args:
        x: Input array (assets, time).
        scale: Total size scaling factor.
        longscale: Long position scaling factor.
        shortscale: Short position scaling factor.
    """
    col_list = []
    for t in range(np.size(x, 1)):
        col = x[:, t]
        if longscale != 1 or shortscale != 1:
            res = col.copy()
            pos_mask = col > 0
            neg_mask = col < 0
            pos_sum = np.nansum(col[pos_mask])
            neg_sum = np.nansum(np.abs(col[neg_mask]))
            if pos_sum > 0:
                res[pos_mask] = col[pos_mask] / pos_sum * longscale
            if neg_sum > 0:
                res[neg_mask] = col[neg_mask] / neg_sum * shortscale
        else:
            abs_sum = np.nansum(np.abs(col))
            if abs_sum > 0:
                res = col / abs_sum * scale
            else:
                res = col
        col_list.append(res)
    return np.asarray(col_list).transpose((1, 0))


def winsorize(x, std=4):
    """
    Winch x so that its value lies between the upper and lower bounds (the upper and lower bounds are multiples of std).
    """
    col_list = []
    for t in range(np.size(x, 1)):
        col = x[:, t]
        mu = np.nanmean(col)
        sigma = np.nanstd(col)
        res = np.clip(col, mu - std * sigma, mu + std * sigma)
        col_list.append(res)
    return np.asarray(col_list).transpose((1, 0))


def zscore(x):
    """
    Calculate the cross-sectional z-score.
    """
    col_list = []
    for t in range(np.size(x, 1)):
        col = x[:, t]
        mu = np.nanmean(col)
        std = np.nanstd(col)
        if std > 0:
            res = (col - mu) / std
        else:
            res = col - mu
        col_list.append(res)
    return np.asarray(col_list).transpose((1, 0))

cross_section_function={
    "rank":[rank, [ndarray], ndarray],
    "quantile":[quantile, [ndarray], ndarray],
    "normalize":[normalize, [ndarray], ndarray],
    "scale":[scale, [ndarray], ndarray],
    "winsorize":[winsorize, [ndarray], ndarray],
}