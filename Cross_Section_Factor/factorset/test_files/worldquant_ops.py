from numpy import ndarray
# from .enum_ops import constant_function

def abs(x):
    return ndarray()

def subtract(x, y):
    return ndarray()

def add(x, y):
    return ndarray()

def rank(x):
    return ndarray()

def multiply(x, y):
    return ndarray()

def add(x, y):
    return ndarray()

def ts_delay(x, d):
    return ndarray()

def ts_covariance(x, y, d):
    return ndarray()

def ts_corr(x, y, d):
    return ndarray()

def ts_min(x, d):
    return ndarray()

def ts_max(x, d):
    return ndarray()

def ts_arg_min(x, d):
    return ndarray()

def ts_arg_max(x, d):
    return ndarray()

def ts_sum(x, d):
    return ndarray()    

def ts_std_dev(x, d):
    return ndarray()

def rolling_mean(x, d):
    return ndarray()

def ts_mean(x, d):
    return ndarray()

def reverse(x):
    return ndarray()

def ts_zscore(x, d):
    return ndarray()

def ts_rank(x, d):
    return ndarray()


def divide(x,y):
    return ndarray()

def inverse(x):
    return ndarray()

def log(x):
    return ndarray()

def max(x):
    return ndarray()

def min(x):
    return ndarray()

def power(x,d):
    return ndarray()

def reverse(x):
    return ndarray()

def sign(x):
    return ndarray()

def signed_power(x, d):
    return ndarray()

def sqrt(x):
    return ndarray()

def normalize(x):
    return ndarray()

def quantile(x):
    return ndarray()

def scale(x):
    return ndarray()

def winsorize(x):
    return ndarray()

def zscore(x):
    return ndarray()


def ts_decay_linear(x, d):
    return ndarray()

def ts_delta(x, d):
    return ndarray()

def ts_product(x, d):
    return ndarray()

def ts_quantile(x, d):
    return ndarray()

def ts_regression(x, d):
    return ndarray()

def ts_scale(x, d):
    return ndarray()


class Anytype:
    pass


# wq_legal_operator = {
#     "add": [add, [ndarray, ndarray], ndarray, "add"],  # Supports ndarray or int
#     "multiply": [multiply, [ndarray, ndarray], ndarray, "multiply"],
#     ""
#     "abs": [abs, [ndarray], ndarray, "abs"],
#     "subtract": [subtract, [ndarray, ndarray], ndarray, "subtract"],
#     "reverse": [reverse, [ndarray], ndarray, "reverse"],
#     "divide": [divide, [ndarray, ndarray], ndarray, "divide"],
#     "inverse": [inverse, [ndarray], ndarray, "inverse"],
#     "ts_delay": [ts_delay, [ndarray, int], ndarray],
#     "ts_covariance": [ts_covariance, [ndarray, ndarray, int], ndarray],
#     "ts_corr": [ts_corr, [ndarray, ndarray, int], ndarray],
#     "ts_min": [ts_min, [ndarray, int], ndarray],
#     "ts_max": [ts_max, [ndarray, int], ndarray],
#     "ts_arg_min": [ts_arg_min, [ndarray, int], ndarray],
#     "ts_arg_max": [ts_arg_max, [ndarray, int], ndarray],
#     "ts_rank": [ts_rank, [ndarray, int], ndarray],
#     "ts_sum": [ts_sum, [ndarray, int], ndarray],
#     "ts_std_dev": [ts_std_dev, [ndarray, int], ndarray],
#     "ts_mean": [ts_mean, [ndarray, int], ndarray],
#     "ts_zscore": [ts_zscore, [ndarray, int], ndarray],
#     "ts_product": [ts_product, [ndarray, int], ndarray],
#     "rank": [rank, [ndarray], ndarray, "rank"],
#     "log": [log, [ndarray], ndarray, "log"],
#     "max": [max, [ndarray, ndarray], ndarray, "max"],
#     "min": [min, [ndarray, ndarray], ndarray, "min"],
#     "power": [power, [ndarray, int], ndarray, "power"],
#     "sign": [sign, [ndarray], ndarray, "sign"],
#     "signed_power": [signed_power, [ndarray, int], ndarray, "signed_power"],
#     "sqrt": [sqrt, [ndarray], ndarray, "sqrt"],
#     "normalize": [normalize, [ndarray], ndarray, "normalize"],
#     "quantile": [quantile, [ndarray], ndarray, "quantile"],
#     "scale": [scale, [ndarray], ndarray, "scale"],
#     "winsorize": [winsorize, [ndarray], ndarray, "winsorize"],
#     "zscore": [zscore, [ndarray], ndarray, "zscore"],
#     "ts_decay_linear": [ts_decay_linear, [ndarray, int], ndarray],
#     "ts_delta": [ts_delta, [ndarray, int], ndarray],
#     "ts_quantile": [ts_quantile, [ndarray, int], ndarray],
#     "ts_regression": [ts_regression, [ndarray, ndarray], ndarray],
#     "ts_scale": [ts_scale, [ndarray, int], ndarray],
# }



wq_legal_operator = {
    "add": [add, [ndarray, ndarray], ndarray, "add"],
    "add_nf": [add, [ndarray, float], ndarray, "add_nf"],
    "add_fn": [add, [float, ndarray], ndarray, "add_fn"],
    "add_ff": [add, [float, float], float, "add_ff"],

    "multiply": [multiply, [ndarray, ndarray], ndarray, "multiply"],
    "multiply_nf": [multiply, [ndarray, float], ndarray, "multiply_nf"],
    "multiply_fn": [multiply, [float, ndarray], ndarray, "multiply_fn"],
    "multiply_ff": [multiply, [float, float], float, "multiply_ff"],

    "subtract": [subtract, [ndarray, ndarray], ndarray, "subtract"],
    "subtract_nf": [subtract, [ndarray, float], ndarray, "subtract_nf"],
    "subtract_fn": [subtract, [float, ndarray], ndarray, "subtract_fn"],
    "subtract_ff": [subtract, [float, float], float, "subtract_ff"],

    "divide": [divide, [ndarray, ndarray], ndarray, "divide"],
    "divide_nf": [divide, [ndarray, float], ndarray, "divide_nf"],
    "divide_fn": [divide, [float, ndarray], ndarray, "divide_fn"],
    "divide_ff": [divide, [float, float], float, "divide_ff"],

    "max": [max, [ndarray, ndarray], ndarray, "max"],
    "max_nf": [max, [ndarray, float], ndarray, "max_nf"],
    "max_fn": [max, [float, ndarray], ndarray, "max_fn"],
    "max_ff": [max, [float, float], float, "max_ff"],

    "min": [min, [ndarray, ndarray], ndarray, "min"],
    "min_nf": [min, [ndarray, float], ndarray, "min_nf"],
    "min_fn": [min, [float, ndarray], ndarray, "min_fn"],
    "min_ff": [min, [float, float], float, "min_ff"],

    "abs": [abs, [ndarray], ndarray, "abs"],
    "reverse": [reverse, [ndarray], ndarray, "reverse"],
    "reverse_f": [reverse, [float], float, "reverse_f"],
    "inverse": [inverse, [ndarray], ndarray, "inverse"],
    "inverse_f": [inverse, [float], float, "inverse_f"],

    "ts_delay": [ts_delay, [ndarray, int], ndarray],
    "ts_covariance": [ts_covariance, [ndarray, ndarray, int], ndarray],
    "ts_corr": [ts_corr, [ndarray, ndarray, int], ndarray],
    "ts_min": [ts_min, [ndarray, int], ndarray],
    "ts_max": [ts_max, [ndarray, int], ndarray],
    "ts_arg_min": [ts_arg_min, [ndarray, int], ndarray],
    "ts_arg_max": [ts_arg_max, [ndarray, int], ndarray],
    "ts_rank": [ts_rank, [ndarray, int], ndarray],
    "ts_sum": [ts_sum, [ndarray, int], ndarray],
    "ts_std_dev": [ts_std_dev, [ndarray, int], ndarray],
    "ts_mean": [ts_mean, [ndarray, int], ndarray],
    "ts_zscore": [ts_zscore, [ndarray, int], ndarray],
    "ts_product": [ts_product, [ndarray, int], ndarray],
    "rank": [rank, [ndarray], ndarray, "rank"],
    "log": [log, [ndarray], ndarray, "log"],
    "power": [power, [ndarray, int], ndarray, "power"],
    "sign": [sign, [ndarray], ndarray, "sign"],
    "signed_power": [signed_power, [ndarray, int], ndarray, "signed_power"],
    "sqrt": [sqrt, [ndarray], ndarray, "sqrt"],
    "normalize": [normalize, [ndarray], ndarray, "normalize"],
    "quantile": [quantile, [ndarray], ndarray, "quantile"],
    "scale": [scale, [ndarray], ndarray, "scale"],
    "winsorize": [winsorize, [ndarray], ndarray, "winsorize"],
    "zscore": [zscore, [ndarray], ndarray, "zscore"],
    "ts_decay_linear": [ts_decay_linear, [ndarray, int], ndarray],
    "ts_delta": [ts_delta, [ndarray, int], ndarray],
    "ts_quantile": [ts_quantile, [ndarray, int], ndarray],
    "ts_regression": [ts_regression, [ndarray, ndarray], ndarray],
    "ts_scale": [ts_scale, [ndarray, int], ndarray],
}



# wq_legal_operator = {
    
#     "add_nn": [add, [ndarray, ndarray], ndarray,"add_nn"],
#     "add_ni": [add, [ndarray, int], ndarray,"add_ni"],
#     "add_in": [add, [int, ndarray], ndarray,"add_in"],
#     "add_ii": [add, [int, int], int,"add_ii"],

#     "multiply_nn": [multiply, [ndarray, ndarray], ndarray,"multiply_nn"],
#     "multiply_ni": [multiply, [ndarray, int], ndarray,"multiply_ni"],
#     "multiply_in": [multiply, [int, ndarray], ndarray,"multiply_in"],
#     "multiply_ii": [multiply, [int, int], int,"multiply_ii"],

#     "abs_nn": [abs, [ndarray], ndarray,"abs_nn"],
#     # "abs2": [abs, [int], int,"abs"],

#     "subtract_nn": [subtract, [ndarray, ndarray], ndarray,"subtract_nn"],
#     "subtract_ni": [subtract, [ndarray, int], ndarray,"subtract_ni"],
#     "subtract_in": [subtract, [int, ndarray], ndarray,"subtract_in"],

#     "reverse_n": [reverse, [ndarray], ndarray,"reverse_n"],
#     "reverse_i": [reverse, [int], int,"reverse_i"],

#     "divide_nn": [divide, [ndarray, ndarray], ndarray,"divide_nn"],
#     "divide_ni": [divide, [ndarray, int], ndarray,"divide_ni"],
#     "divide_in": [divide, [int, ndarray], ndarray,"divide_in"],

#     "inverse_n": [inverse, [ndarray], ndarray,"inverse_n"],
#     "inverse_i": [inverse, [int], int,"inverse_i"],

#     "ts_delay": [ts_delay, [ndarray, int], ndarray],
#     "ts_covariance": [ts_covariance, [ndarray, ndarray, int], ndarray],
#     "ts_corr": [ts_corr, [ndarray, ndarray, int], ndarray],
#     # "ts_min": [ts_min, [ndarray, int], ndarray],
#     # "ts_max": [ts_max, [ndarray, int], ndarray],
#     "ts_arg_min": [ts_arg_min, [ndarray, int], ndarray],
#     "ts_arg_max": [ts_arg_max, [ndarray, int], ndarray],
#     "ts_rank": [ts_rank, [ndarray, int], ndarray],
#     "ts_sum": [ts_sum, [ndarray, int], ndarray],
#     "ts_std_dev": [ts_std_dev, [ndarray, int], ndarray],
#     "ts_mean": [ts_mean, [ndarray, int], ndarray],
#     "ts_zscore": [ts_zscore, [ndarray, int], ndarray],
#     "ts_product": [ts_product, [ndarray, int], ndarray],

#     "rank": [rank, [ndarray], ndarray],
#     "log": [log, [ndarray], ndarray],

#     "max_nn": [max, [ndarray,ndarray], ndarray,"max_nn"],
#     "max_ni": [max, [ndarray, int], ndarray,"max_ni"],
#     "max_in": [max, [int, ndarray], ndarray,"max_in"],

#     "min_nn": [min, [ndarray,ndarray], ndarray,"min_nn"],
#     "min_ni": [min, [ndarray, int], ndarray,"min_ni"],
#     "min_in": [min, [int, ndarray], ndarray,"min_in"],
    
#     "power": [power, [ndarray, int], ndarray],
#     "sign": [sign, [ndarray], ndarray],
#     "signed_power": [signed_power, [ndarray, int], ndarray],
#     "sqrt": [sqrt, [ndarray], ndarray],
#     "normalize": [normalize, [ndarray], ndarray],
#     "quantile": [quantile, [ndarray], ndarray],
#     "scale": [scale, [ndarray], ndarray],
#     "winsorize": [winsorize, [ndarray], ndarray],
#     "zscore": [zscore, [ndarray], ndarray],
#     "ts_decay_linear": [ts_decay_linear, [ndarray, int], ndarray],
#     "ts_delta": [ts_delta, [ndarray, int], ndarray],
#     "ts_quantile": [ts_quantile, [ndarray, int], ndarray],
#     "ts_regression": [ts_regression, [ndarray, ndarray], ndarray],
#     "ts_scale": [ts_scale, [ndarray, int], ndarray],
# }

# import numpy as np
# class AnyType:
#     # def __instancecheck__(cls, instance):
#     #     # Check if it is an instance of AnyType
#     #     if type(instance) == AnyType:
#     #         return True
#     #     # Check if it should be treated as int
#     #     if hasattr(instance, '_value'):
#     #         return isinstance(instance._value, (int, np.integer, np.ndarray))
#     #     return False
#     pass

# class self_ndarray:
#     pass
#     def __instancecheck__(cls, instance):
#         # Check if it is an instance of AnyType
#         if type(instance) == AnyType:
#             return True
#         # Check if it should be treated as int
#         if hasattr(instance, '_value'):
#             return isinstance(instance._value, (AnyType))
#         return False


# class self_int:
#     def __instancecheck__(cls, instance):
#         # Check if it is an instance of AnyType
#         if type(instance) == AnyType:
#             return True
#         # Check if it should be treated as int
#         if hasattr(instance, '_value'):
#             return isinstance(instance._value, (AnyType))
#         return False





# wq_legal_operator = {
    
#     "add": [add, [AnyType, AnyType], AnyType,"add"],


#     "multiply": [multiply, [AnyType, AnyType], AnyType,"multiply"],



#     "abs": [abs, [AnyType], AnyType,"abs"],
#     # "abs2": [abs, [AnyType], AnyType,"abs"],

#     "subtract": [subtract, [AnyType, AnyType], AnyType,"subtract"],


#     "reverse": [reverse, [AnyType], AnyType,"reverse"],
#     # "reverse2": [reverse, [AnyType], AnyType,"reverse"],

#     "divide": [divide, [AnyType, AnyType], AnyType,"divide"],


#     "inverse": [inverse, [AnyType], AnyType,"inverse"],
#     # "inverse2": [inverse, [AnyType], AnyType,"inverse"],

#     "ts_delay": [ts_delay, [ndarray,int], ndarray],
#     "ts_covariance": [ts_covariance, [ndarray,int], ndarray],
#     "ts_corr": [ts_corr, [ndarray,int], ndarray],
#     # "ts_min": [ts_min, [AnyType, AnyType], AnyType],
#     # "ts_max": [ts_max, [AnyType, AnyType], AnyType],
#     "ts_arg_min": [ts_arg_min, [ndarray,int], ndarray],
#     "ts_arg_max": [ts_arg_max, [ndarray,int], ndarray],
#     "ts_rank": [ts_rank, [ndarray,int], ndarray],
#     "ts_sum": [ts_sum, [ndarray,int], ndarray],
#     "ts_std_dev": [ts_std_dev, [ndarray,int], ndarray],
#     "ts_mean": [ts_mean, [ndarray,int], ndarray],
#     "ts_zscore": [ts_zscore, [ndarray,int], ndarray],
#     "ts_product": [ts_product, [ndarray,int], ndarray],

#     "rank": [rank, [AnyType], AnyType],
#     "log": [log, [AnyType], AnyType],

#     "max": [max, [AnyType,AnyType], AnyType,"max"],


#     "min": [min, [ndarray,ndarray], ndarray,"min"],

    
#     "power": [power, [AnyType,int],AnyType],
#     "sign": [sign, [AnyType,int], AnyType],
#     "signed_power": [signed_power, [AnyType,int], AnyType],
#     "sqrt": [sqrt, [AnyType], AnyType],

#     "normalize": [normalize, [ndarray], ndarray],
#     "quantile": [quantile, [ndarray],ndarray],
#     "scale": [scale, [ndarray], ndarray],
#     "winsorize": [winsorize, [ndarray], ndarray],
#     "zscore": [zscore, [ndarray], ndarray],
#     "ts_decay_linear": [ts_decay_linear, [ndarray,int], ndarray],
#     "ts_delta": [ts_delta, [ndarray,int], ndarray],
#     "ts_quantile": [ts_quantile, [ndarray,int], ndarray],
#     "ts_regression": [ts_regression, [ndarray,ndarray], ndarray],
#     "ts_scale": [ts_scale, [ndarray,int], ndarray],
# }

