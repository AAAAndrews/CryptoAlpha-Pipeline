import numpy as np

from .metrics import rankic, top_k_returns, calculate_monotonicity

def base_evaluate(individual, compile_func, features, returns, weights=None):
    """
    Common evaluation logic core.
    
    Args:
        individual: DEAP Individual (expression tree).
        compile_func: toolbox.compile method.
        features: Input feature list [array1, array2, ...].
        returns: Profit rate matrix (time, assets).
        weights: Dictionary of weights for each indicator.
    """
    if weights is None:
        weights = {'rankic': 1.0}

    try:
        # 1. Compile individuals into executable functions
        func = compile_func(expr=individual)
        
        # 2. Calculate factor values
        # Assume that the input features are a list in the order defined by pset
        factor_values = func(*features)
        
        # Check result validity
        if factor_values is None or np.all(np.isnan(factor_values)):
            return -1.0,

        # 3. Calculate various indicators
        # Note: Make sure the dimensions of factor_values ​​and returns are consistent (time, assets)
        # If your operator output is (assets, time), you may need to transpose here
        if factor_values.shape != returns.shape:
            factor_values = factor_values.T
            
        score = 0.0
        if 'rankic' in weights:
            ric = np.nanmean(rankic(factor_values, returns))
            score += ric * weights['rankic']
            
        if 'top_r' in weights:
            tr = top_k_returns(factor_values, returns)
            score += tr * weights['top_r']
            
        if 'mono' in weights:
            mono = calculate_monotonicity(factor_values, returns)
            score += mono * weights['mono']

        # DEAP Request to return tuple
        return score,

    except Exception as e:
        # Capture errors during calculation (such as exceptions caused by dividing by 0, etc.)
        return -1.0,

def make_fitness_func(toolbox, dataset, weights=None):
    """
    Adapter factory: binds datasets and toolboxes to evaluation functions.
    
    Args:
        toolbox: Configured DEAP toolbox.
        dataset: Include'features' (list of ndarray) and'returns' (ndarray) dictionary.
        weights: Indicator weight.
    
    Returns:
        function: Functions that can be registered directly to toolbox.evaluate.
    """
    features = dataset['features']
    returns = dataset['returns']
    
    # Returns a closure that accepts only individual
    def evaluate(individual):
        return base_evaluate(
            individual=individual,
            compile_func=toolbox.compile,
            features=features,
            returns=returns,
            weights=weights
        )
    
    return evaluate

