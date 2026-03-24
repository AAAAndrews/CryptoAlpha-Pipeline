#jupyter notebookMultiprocessing cannot run normally in
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from deap_alpha import Context
from deap_alpha import MyPset,MyFitness,MyIndividual,MyToolBox,spearman_corr_jit,Statistic,calculate_high_low_sharpe_ratio,calculate_high_low_calmar_ratio,calculate_high_low_sortino_ratio
from deap import tools,algorithms,gp
from datapreprocess import CSVDataLoader,MultiAssetDataHandler
import warnings
import os
import joblib
from statsmodels.tsa.stattools import adfuller
from Cross_Section_Factor.deap_alpha.ops.old_version_ops import *

warnings.simplefilter("ignore")
context = Context()

os.chdir(r"D:\Trading\Macro Investment\[Research] Single asset timing model-data event driven\TimingModelV4.2X Genetic planning technology route\scoreboard")

folder_path = "FactorLab/lab2"
train_set_path = os.path.join(folder_path,"trainset_spx_with_econ.csv")

handler = MultiAssetDataHandler(context=context,multi=True)
handler.add_loader(CSVDataLoader, [train_set_path])


data_3d, fields, stocks, dates, returns_matrix,context =\
      handler.to_3d_array(period=1,fields=[
        'open','high','low','close','volume','amount'
      ]) 




data, y_list = data_3d,returns_matrix
# fitness = MyFitness()
individual = MyIndividual
MAX_TREE_DEPTH=3
MIN_TREE_DEPTH=1


pset = MyPset(
    context=context,
    use_cross_section=False
)
toolbox = MyToolBox(
    pset=pset,
    individual=individual,
    feature_set=data_3d,
    ret_set=returns_matrix,
    min_depths=MIN_TREE_DEPTH,
    max_depths=MAX_TREE_DEPTH,
    tournsize=50,
    mutate_max=MAX_TREE_DEPTH-MIN_TREE_DEPTH,
    mate_max=MAX_TREE_DEPTH-MIN_TREE_DEPTH,
    expr_mute=(0, MAX_TREE_DEPTH-MIN_TREE_DEPTH),  
)
mstats = Statistic()


hof = tools.HallOfFame(50)
pop = toolbox.population(n=1000)




import time


def eval_rolling_ts_ic(individual, window=24):
    """
    Calculate the rolling window timing IC of a single asset, and perform an adfuller stationarity test on the signal sequence.
    :param individual: GPindividual
    :param window: rolling window length
    :return: average IC (tuple)
    """
    data = data_3d
    y_list = returns_matrix

    func = toolbox.compile(expr=individual)
    res = (func(*data)).transpose((1, 0))  # (time, assets)
    res_flat = res.flatten()

    # 1. Signal sparsity penalty
    zero_ratio = np.sum(res_flat == 0) / len(res_flat)
    if zero_ratio > 0.7:
        return -0.001,

    # 2. Signal variance filter
    if np.nanstd(res_flat) < 1e-6:
        return -0.001,

    # 3. Signal NaN distribution check (only NaN allowed at the beginning)
    signal = res[:, 0]
    n = len(signal)
    nan_mask = np.isnan(signal)
    nan_count = np.sum(nan_mask)
    # Only consecutive NaNs at the beginning are allowed
    first_valid = np.argmax(~nan_mask) if np.any(~nan_mask) else n
    # Check if there are NaN in the middle and at the end
    if np.any(nan_mask[first_valid:]):
        return -0.001,
    # Total NaN cannot exceed half
    if nan_count > n // 2:
        return -0.001,

    # 4. Single asset adfuller unit root test
    signal_valid = signal[first_valid:]
    if len(signal_valid) >= 10:
        try:
            pvalue = adfuller(signal_valid)[1]
        except Exception:
            pvalue = 1.0
        if pvalue > 0.05:
            return -0.001,

    # 5. original filter
    if np.isnan(res_flat).all() or np.nanmax(np.abs(res_flat)) > 1e16:
        return -0.001,

    ic_list = []
    for asset_idx in range(res.shape[1]):
        signal = res[:, asset_idx]
        ret = y_list[:, asset_idx]
        for start in range(0, len(signal) - window + 1):
            end = start + window
            sig_win = signal[start:end]
            ret_win = ret[start:end]
            if np.nanstd(sig_win) < 1e-6 or np.nanstd(ret_win) < 1e-6:
                ic = np.nan
            else:
                ic = pd.Series(sig_win).corr(pd.Series(ret_win), method='spearman')
            ic_list.append(ic)
    # 6. Effective window ratio filtering
    valid_ic_count = np.sum(~np.isnan(ic_list))
    if valid_ic_count < 0.5 * len(ic_list):
        return -0.001,
    ic_mean = pd.Series(ic_list).mean(skipna=True)
    if np.isnan(ic_mean):
        ic_mean = -0.001

    return ic_mean,

# Register to toolbox
# toolbox.register("evaluate", eval_rolling_ts_ic)
# Or use lambda to pass the window length
# toolbox.register("evaluate", lambda ind: eval_rolling_ts_ic(ind, window=12))

toolbox.register("evaluate",eval_rolling_ts_ic)

import multiprocessing
if __name__=="__main__":
    count = 0
    while count<6:
        with multiprocessing.Pool(processes=12) as pool:
            try:
                toolbox.register("map",pool.map)
                print("Successfully registered multi-process")
                pop,log = algorithms.eaSimple(pop, toolbox, cxpb=0.6, mutpb=0.6, ngen=8, stats=mstats, halloffame=hof, verbose=True)
                pool.close()
                joblib.dump(hof,os.path.join(folder_path,f"hof_multi_{time.time()}.pkl"))
                print("Saved successfully")
                count+=1
            except:
                pool.close()
                raise Exception

