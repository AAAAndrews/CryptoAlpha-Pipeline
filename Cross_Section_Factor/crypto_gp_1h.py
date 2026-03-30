import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from deap_alpha import Context
from deap_alpha import *
from datapreprocess import CSVDataLoader,DataFrameLoader,MultiAssetDataHandler
import warnings
import os
import re
import joblib
from Cross_Section_Factor.deap_alpha.ops.old_version_ops import *

from copy import deepcopy
from deap import algorithms
warnings.simplefilter("ignore")

MAX_TREE_DEPTH=6
MIN_TREE_DEPTH=2

context = Context()
# df = pd.read_feather(r"F:\MyCryptoTrading\CryptoTradingSystem_allin1\crypto_1h_klines_binancecryptoderivative_20251123_010959.feather")


def generate_mock_crypto_dataframe(
    symbols=("BTCUSDT", "ETHUSDT", "SOLUSDT"),
    start="2024-01-01 00:00:00",
    periods=10000,
    freq="1H",
    seed=42,
):
    """Create a reproducible synthetic OHLCV panel for multiple assets."""
    rng = np.random.default_rng(seed)
    timeline = pd.date_range(start=start, periods=periods, freq=freq)
    freq_delta = pd.to_timedelta(freq)
    rows = []

    for symbol in symbols:
        base_price = rng.uniform(50, 50000)
        price_walk = base_price + rng.normal(scale=base_price * 0.002, size=periods).cumsum()
        close_noise = rng.normal(scale=base_price * 0.001, size=periods)
        opens = price_walk
        closes = price_walk + close_noise
        highs = np.maximum(opens, closes) + rng.random(periods) * base_price * 0.0015
        lows = np.minimum(opens, closes) - rng.random(periods) * base_price * 0.0015
        volumes = rng.lognormal(mean=9, sigma=0.6, size=periods)
        taker_buy_base = volumes * rng.uniform(0.35, 0.75, size=periods)

        for idx, ts in enumerate(timeline):
            avg_price = 0.5 * (opens[idx] + closes[idx])
            quote_volume = volumes[idx] * avg_price
            taker_buy_quote = taker_buy_base[idx] * avg_price
            rows.append(
                {
                    "symbol": symbol,
                    "open_time": ts,
                    "open": float(opens[idx]),
                    "high": float(highs[idx]),
                    "low": float(lows[idx]),
                    "close": float(closes[idx]),
                    "volume": float(volumes[idx]),
                    "close_time": ts + freq_delta,
                    "quote_volume": float(quote_volume),
                    "trades": int(rng.integers(90, 1200)),
                    "taker_buy_base": float(taker_buy_base[idx]),
                    "taker_buy_quote": float(taker_buy_quote),
                }
            )

    df = pd.DataFrame(rows)
    df.sort_values(["symbol", "open_time"], inplace=True, ignore_index=True)
    return df


df = generate_mock_crypto_dataframe()
print(df.head())

handler = MultiAssetDataHandler(context=context,multi=True)
handler.add_loader(
    DataFrameLoader,
    [df]
)

data_3d, fields, stocks, dates, returns_matrix,context =\
      handler.to_3d_array(period=1,fields=
        ['open', 'high', 'low', 'close', 'volume',
       'quote_volume', 'trades', 'taker_buy_base',
       'taker_buy_quote']
      ) 


def compile_factor_matrix(individual):
    global data_3d
    func = toolbox.compile(expr=individual)
    # Calculate factor values
    factor_matrix = (func(*data_3d)).transpose((1, 0))
    return factor_matrix


def evaluate_function(individual):
    global returns_matrix
    factor_matrix = compile_factor_matrix(individual)
    # Calculate IC mean
    ic_mean = pd.DataFrame(factor_matrix).corrwith(
        pd.DataFrame(returns_matrix), method='spearman',axis=1).mean()
    # Calculate factor values
    if ic_mean is None or np.isnan(ic_mean):
        ic_mean = -np.inf
    
    return ic_mean,


settings_dict = easy_initialize_gpsettings(
    context=context,
    min_depths=MIN_TREE_DEPTH,
    max_depths=MAX_TREE_DEPTH,
    fitness_weights=(1.0,),
    tournsize=5,
    mutate_max=MAX_TREE_DEPTH - MIN_TREE_DEPTH,
    mate_max=MAX_TREE_DEPTH - MIN_TREE_DEPTH,
    expr_mut_range=(0, MAX_TREE_DEPTH - MIN_TREE_DEPTH),
    use_cross_section=True,
    use_timeseries=True,
    wq_operators=False,
)

toolbox = settings_dict["toolbox"]
hof = settings_dict["hof"]
mstats = settings_dict["mstats"]
pset = settings_dict["pset"]
creator = settings_dict["creator"]

pop = toolbox.population(n=20)



# pop = deepcopy(alpha_success_to_transform)
print(f"Total found{len(pop)}compilable factors as the initial population")
for expr in pop:
    print(expr)
import time


toolbox.register("evaluate",evaluate_function)
folder_path = r".\deap_results"
import multiprocessing
if __name__=="__main__":
    # count = 0
    with multiprocessing.Pool(processes=16) as pool:
        try:
            toolbox.register("map",pool.map)
            print("Successfully registered multi-process")
            pop,log = algorithms.eaSimple(pop, toolbox, cxpb=0.4, mutpb=0.4, 
                                          ngen=5, stats=mstats, halloffame=hof, verbose=1)
            pool.close()
            # joblib.dump(hof,f"hof_{time.time()}.pkl")
            print("Saved successfully")
            # count+=1
        except:
            pool.close()
            raise Exception("Error, terminate the operation")

