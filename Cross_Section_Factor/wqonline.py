#jupyter notebookMultiprocessing cannot run normally in
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from deap_alpha import Context
from deap_alpha import *
from datapreprocess import CSVDataLoader,MultiAssetDataHandler
import warnings
import os
import re
import joblib
from statsmodels.tsa.stattools import adfuller
from Cross_Section_Factor.deap_alpha.ops.old_version_ops import *
from worldquant_utils.wqbrain_client import BrainBatchAlpha
from copy import deepcopy
from deap import algorithms
warnings.simplefilter("ignore")
context = Context()

ALPHA_RECORDS = []

MAX_TREE_DEPTH=6
MIN_TREE_DEPTH=2
TOURNSIZE=10
CREDENTIALS_FILE = r'.\worldquant_utils\brain_credentials.txt'
FIELDS = ['open', 'high', 'low', 'close', 'volume', 'returns','vwap','adv20']
CLIENT = BrainBatchAlpha(CREDENTIALS_FILE)
ALPHA_BASIC_CONFIG = {
            'type': 'REGULAR',
            'settings': {
                'instrumentType': 'EQUITY',
                'region': 'USA',
                'universe': 'TOP1000',
                'delay': 1,
                'decay': 3,
                'neutralization': 'SUBINDUSTRY',
                'truncation': 0.08,
                'pasteurization': 'ON',
                'unitHandling': 'VERIFY',
                'nanHandling': 'ON',
                'language': 'FASTEXPR',
                'visualization': False,
            },
            # 'regular': expr
        }
context.params["fields"] = FIELDS

def simulate_with_auto_reauth(client, alpha, credentials_file='brain_credentials.txt', max_retry=1):
    for attempt in range(max_retry + 1):
        result = client._simulate_single_alpha(alpha)
        # Check whether 401/403 is caused by authentication failure
        if result is None and hasattr(client, 'last_status_code'):
            if client.last_status_code in [401, 403]:
                print("⚠️ Authentication failure detected, logging in again and trying again...")
                client.reauthenticate(credentials_file)
                continue
        return result
    print("❌ Failed after multiple attempts")
    return None

def check_all_passes(result):
    """
    Check whether the results of all items in the checks of the result are PASS or PENDING.

    parameter:
        result: APIDictionary of results returned

    return:
        bool: Returns True if all checks are PASS or PENDING, False otherwise
        dict: dictionary containing detailed information
    """
    if not result:
        return False, {"error": "resultIs None or empty"}

    if 'metrics' not in result:
        return False, {"error": "resultThere is no metrics field in"}

    if 'checks' not in result['metrics']:
        return False, {"error": "result['metrics']There is no checks field in"}

    checks = result['metrics']['checks']
    failed_checks = []
    passed_checks = []
    pending_checks = []

    for i, check in enumerate(checks):
        if 'result' not in check:
            failed_checks.append(f"examine{i}: No result field")
            continue

        check_result = check['result']
        check_name = check.get('name', f'examine{i}')

        if check_result == 'PASS':
            passed_checks.append(check_name)
        elif check_result == 'PENDING':
            pending_checks.append(check_name)
        else:
            failed_checks.append(f"{check_name}: {check_result}")

    all_pass = len(failed_checks) == 0

    summary = {
        "all_pass": all_pass,
        "total_checks": len(checks),
        "passed": len(passed_checks),
        "pending": len(pending_checks),
        "failed": len(failed_checks),
        "passed_checks": passed_checks,
        "pending_checks": pending_checks,
        "failed_checks": failed_checks
    }

    return all_pass, summary




def worldquant_online_eval(individual):
    expr = str(individual)
    expr = re.sub(r'\bget_f', '', expr)
    expr = re.sub(r'\bget_', '', expr)
    expr = re.sub(r'\(\)', '', expr)
    expr = re.sub(r'_nf\b', '', expr)
    expr = re.sub(r'_fn\b', '', expr)
    expr = re.sub(r'_ff\b', '', expr)
    # print(expr)
    #Remove the get prefix to comply with brain legal parameters
    #deapIn addition to the terminal, it is required to provide generation functions for all input types, so the integer generation function getxx needs to be defined separately, but brain does not require the get prefix

    alpha_config = deepcopy(ALPHA_BASIC_CONFIG)
    alpha_config['regular'] = expr
    result = simulate_with_auto_reauth(CLIENT, alpha_config, CREDENTIALS_FILE)

    if result is None:
        return float('-inf')
      # Use new check function
    all_pass, check_details = check_all_passes(result)

    # Count the number of failed checks
    failed_count = sum(1 for check in result['metrics']['checks'] if check.get('result') not in ['PASS', 'PENDING'])

    # Output the inspection result statistics for each factor
    passed_count = sum(1 for check in result['metrics']['checks'] if check.get('result') == 'PASS')
    pending_count = sum(1 for check in result['metrics']['checks'] if check.get('result') == 'PENDING')
    total_count = len(result['metrics']['checks'])

    print(f"factor:{expr[:50]}{'...' if len(expr) > 50 else ''}")
    print(f"  Check result: passed={passed_count}, To be determined={pending_count}, fail={failed_count}, total={total_count}")
    print(f"  Fitness index: Sharpe={result['metrics']['sharpe']:.4f}, Fitness={result['metrics']['fitness']:.4f}")

    # Number of storage failed checks<=1factor information
    if failed_count <= 1:
        print(f"  ✓ Saved to record (failed check≤1)")
        alpha_info = {
            'expression': expr,
            'alpha_id': result.get('alpha_id', 'N/A'),  # Secure access and avoid KeyError
            'sharpe': result['metrics']['sharpe'],
            'fitness': result['metrics']['fitness'],
            'margin': result['metrics']['margin'],
            'turnover': result['metrics']['turnover'],
            'longCount': result['metrics']['longCount'],
            'shortCount': result['metrics']['shortCount'],
            'concentration': result['metrics']['checks'][4]['result'] if len(
                result['metrics']['checks']) > 4 else 'N/A',
            'sub_uni_sharpe': result['metrics']['checks'][5].get('value', 'ERROR') if len(
                result['metrics']['checks']) > 5 else 'N/A',
            'sub_uni_limit': result['metrics']['checks'][5].get('limit', 'ERROR') if len(
                result['metrics']['checks']) > 5 else 'N/A',
            'all_pass': all_pass,
            'total_checks': total_count,
            'passed_checks': passed_count,
            'pending_checks': pending_count,
            'failed_checks': failed_count,
        }
        ALPHA_RECORDS.append(alpha_info)
        with open("alpha_records.pkl", "wb") as f:
            joblib.dump(ALPHA_RECORDS, f)
        print("Alpha saved successfully, current number of records:", len(ALPHA_RECORDS))
    else:
        print(f"  ✗ not saved (failed check={failed_count}>1)")

    print()  # Blank line separated

    # Calculate fitness score
    sharpe = result['metrics']['sharpe']
    fitness = result['metrics']['fitness']
    turnover = result['metrics']['turnover']

    if (sharpe is None) or (fitness is None) or (turnover is None):
        return float('-inf')

    # Overall rating
    score = (abs(sharpe) * 0.4 +
                abs(fitness) * 0.4 +
                (1 / (1 + turnover)) * 0.2)

    return (score**2,)

settings_dict = easy_initialize_gpsettings(context,
                                           min_depths=MIN_TREE_DEPTH, 
                                           fitness_weights=(1.0,), 
                                           max_depths=MAX_TREE_DEPTH, 
                                           tournsize=5, 
                                           mutate_max=5, 
                                           mate_max=5, 
                                           expr_mut_range=(0,2),
                                           wq_operators=True,
                                           use_cross_section=False,
                                           use_timeseries=False)


toolbox = settings_dict["toolbox"]
hof = settings_dict["hof"]
mstats = settings_dict["mstats"]
pset = settings_dict["pset"]
creator = settings_dict["creator"]
# pop = toolbox.population(n=50)


from deap_alpha.utils import check_if_the_pset_can_compile
alphaset = pd.read_excel(r".\factorset\test_files\numeric_alpha191.xlsx")

alpha_success_to_transform = []
for i in alphaset['expression_']:
    if check_if_the_pset_can_compile(i,pset):
        expr = gp.PrimitiveTree.from_string(i, pset)
        alpha_success_to_transform.append(creator.Individual(expr))

pop = deepcopy(alpha_success_to_transform)
print(f"Total found{len(pop)}compilable factors as the initial population")
for expr in pop:
    print(expr)
import time


toolbox.register("evaluate",worldquant_online_eval)
folder_path = r".\deap_results"
import multiprocessing
if __name__=="__main__":
    # count = 0
    with multiprocessing.Pool(processes=3) as pool:
        try:
            toolbox.register("map",pool.map)
            print("Successfully registered multi-process")
            pop,log = algorithms.eaSimple(pop, toolbox, cxpb=0.4, mutpb=0.4, ngen=50, stats=mstats, halloffame=hof, verbose=False)
            pool.close()
            joblib.dump(hof,f"hof_{time.time()}.pkl")
            print("Saved successfully")
            # count+=1
        except:
            pool.close()
            raise Exception("Error, terminate the operation")

