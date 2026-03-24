import numpy as np
from scipy.stats import spearmanr
from numba import jit
import warnings

@jit(nopython=True)
def preprocess_data_jit(x, y):
    """
    Use numba to speed up preprocessing data and remove NaN and Inf values.
    """
    n = len(x)
    valid_x = []
    valid_y = []
    
    for i in range(n):
        if not np.isnan(x[i]) and not np.isnan(y[i]) and \
           not np.isinf(x[i]) and not np.isinf(y[i]):
            valid_x.append(x[i])
            valid_y.append(y[i])
    
    # Convert list to NumPy array
    valid_x = np.array(valid_x)
    valid_y = np.array(valid_y)
    return valid_x, valid_y


def spearman_corr(x, y):
    """
    To calculate the Spearman correlation coefficient, use scipy.stats.spearmanr.
    """
    # Preprocess data
    valid_x, valid_y = preprocess_data_jit(x, y)
    
    # If there are no valid data points, return NaN
    if len(valid_x) == 0 or len(valid_y) == 0:
        return np.nan
    
    # If there is only one unique value in the data, return NaN
    # if len(np.unique(valid_x)) <= 1 or len(np.unique(valid_y)) <= 1:
    #     return np.nan
    
    # Calculate Spearman correlation coefficient using scipy
    corr, _ = spearmanr(valid_x, valid_y)
    if _<0.05:
        return corr
    else:
        return 0

def high_minus_low(factor_values, returns, top_bottom_percent=0.5):
    # Initialize the result array
    high_low_returns = np.zeros(factor_values.shape[0])


    # Iterate through each timestamp
    for t in range(factor_values.shape[0]):
        # Get the factor value and return for the current timestamp
        current_factors = factor_values[t]
        current_returns = returns[t]
        current_factors,current_returns = preprocess_data_jit(current_factors,current_returns)

        #Two situations that lead to serious bias in historical calculation results
        # if (len(current_factors)>(1/top_bottom_percent)) and\
        #    (len(np.unique(current_factors))<(1/top_bottom_percent)):#The sorting fails and the loop breaks out. For example, it is divided into five groups, but there are only 4 distinct values.

        if len(current_factors)<10:#If it is always very small, it will be meaningless to calculate it in the end.
            warnings.warn("The calculation found that the number of cross-section samples is too small. If it persists, the result will be null.")
            high_low_returns[t] = np.nan
            continue

        elif (len(current_factors)>(10)) and\
           (len(np.unique(current_factors))<(5)):#The sorting fails and the loop breaks out. It should be divided into at least five groups.
              
            high_low_returns=np.array([np.nan])
            warnings.warn("The factor value has the risk of outputting too many duplicate values ​​and is judged to be invalid.")
            break
        
        if np.isinf(current_factors).any():#Sorting fails, jumping out of the loop
            high_low_returns=np.array([np.nan])
            warnings.warn("The factor value contains an infinite value and is judged to be invalid.")
            break

        # sort index
        sorted_indices = np.argsort(current_factors)
        
        
        # Calculate the index of the High and Low groups
        n = len(sorted_indices)
        top_n = int(n * top_bottom_percent)
        
        high_indices = sorted_indices[-top_n:]
        low_indices = sorted_indices[:top_n]
        
        # Calculate log return
        high_returns = np.log(1 + current_returns[high_indices]).mean()
        low_returns = np.log(1 + current_returns[low_indices]).mean()
        
        # Calculate High-Low Portfolio Log Return
        high_low_returns[t] = high_returns - low_returns
    
    log_returns = high_low_returns[~np.isnan(high_low_returns)] 
    return log_returns