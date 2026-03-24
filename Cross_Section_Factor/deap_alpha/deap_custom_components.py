import random
import itertools
import operator
import numpy as np
import pandas as pd
import pickle
from numpy import ndarray
from deap import base
from deap import creator
from deap import tools
from deap import gp
from deap import algorithms
from .ops.enum_ops import get_all_constants,get_usual_constants
from .ops.arithmetic_ops import arithmetic_function
from .ops.timeseries_ops import timeseries_function
from .ops.cross_section_ops import cross_section_function
# from .old_version_ops import cross_section_function,timeseries_function
from .ops.worldquant_ops import wq_legal_operator
from copy import deepcopy

class Context:
    def __init__(self):
        """
        Initialize the context object, used to store configuration parameters.
        """
        self.params={}

def initialize_primitiveset(context,array_type,use_cross_section=True,use_timeseries=True,wq_operators=False):
    """
    Initialize DEAP's PrimitiveSetTyped and define operators and terminal nodes.

    Args:
        context: Context object containing configuration parameters, which needs to be included"fields" key.
        array_type: Data type (such as np.ndarray).
        use_cross_section: Whether to use the section operator.
        use_timeseries: Whether to use timing operators.
        wq_operators: Whether to use the WorldQuant operator.

    Returns:
        gp.PrimitiveSetTyped: Configured operator set.
    """
    fields = context.params["fields"]
    n_features = len(fields)
    #Define the input format as a three-dimensional array
    #The return format is a two-dimensional array
    pset = gp.PrimitiveSetTyped("MAIN", in_types=itertools.repeat(array_type, n_features), ret_type=array_type)

    #Custom parameter name
    pset.renameArguments(**{"ARG"+str(_):fields[_] for _ in range(n_features)})
    global cross_section_function, timeseries_function, get_usual_constants,get_all_constants

    #Add operator
    

    if wq_operators:
        constant_function = get_all_constants()
        operators = deepcopy(constant_function)
        
        operators.update(wq_legal_operator)
    
    else:
        constant_function = dict(get_usual_constants())
        operators = deepcopy(constant_function)
        operators.update(arithmetic_function)

        if use_cross_section:
            operators.update(cross_section_function)

        if use_timeseries:
            operators.update(timeseries_function)
    # print(operators.values())
    for params in operators.values():
        # print(params)
        pset.addPrimitive(*params)

    #Add constant endpoint
    periods = [int(_) for _ in constant_function.keys() if _.isdigit()]
    for item in periods:
        pset.addTerminal(item, int)
        pset.addTerminal(float(item), float)

    return pset

def initialize_individual_and_fitness_settings(fitness_weights=(1.0,)):
    """
    Call creator to create or override Individual and Fitness classes.

    Args:
        fitness_weights: Fitness weight tuple, default is (1.0,).
    """
    creator.create('Fitness', base.Fitness, weights=fitness_weights)
    creator.create('Individual', gp.PrimitiveTree, fitness=creator.Fitness)
    pass

def initialize_toolbox(
        pset,
        min_depths=2,
        max_depths=7,
        tournsize=20,
        mutate_max=17,
        mate_max=17,
        expr_mut_range=(0,2),
        **kwargs
        ):
    """
    Initialize DEAP's Toolbox and register genetic algorithm-related operations.

    Args:
        pset: operator set.
        min_depths: The minimum depth of the tree.
        max_depths: The maximum depth of the tree.
        tournsize: Tournament selection size.
        mutate_max: The maximum height limit after mutation.
        mate_max: Maximum height limit after crossing.
        expr_mut_range: The depth range of the mutating expression.
        **kwargs: other parameters.

    Returns:
        base.Toolbox: Configured toolbox.
    """
    
    toolbox = base.Toolbox()

    #Set up genetic programming related methods
    toolbox.register("expr", gp.genHalfAndHalf, pset=pset, min_=min_depths, max_=max_depths)

    # Set Individual to provide preset individual classes
    if "Individual" not in creator.__dict__ or "Fitness" not in creator.__dict__:
        initialize_individual_and_fitness_settings()
    toolbox.register("individual", tools.initIterate, creator.Individual, toolbox.expr)

    # Register a population
    toolbox.register("population", tools.initRepeat, list, toolbox.individual)

    # Register the compilation method, including data type and other information
    toolbox.register("compile", gp.compile, pset=pset)

    #Register evolution parameters
    toolbox.register("select", tools.selTournament, tournsize=tournsize)
    toolbox.register("expr_mut", gp.genFull, min_=expr_mut_range[0], max_=expr_mut_range[1])

    toolbox.register("mutate", gp.mutUniform, expr=toolbox.expr_mut, pset=pset) #uniform mutation
    toolbox.decorate("mutate", gp.staticLimit(key=operator.attrgetter("height"), max_value=mutate_max))

    toolbox.register("mate", gp.cxOnePoint)# single point crossover
    toolbox.decorate("mate", gp.staticLimit(key=operator.attrgetter("height"), max_value=mate_max))

    return toolbox


def initialize_statistics(stat_fit=tools.Statistics(lambda ind: ind.fitness.values),stat_size=tools.Statistics(len)):
    """
    Initialize multiple statistical objects for recording data during the evolution process.

    Args:
        stat_fit: Fitness statistics object.
        stat_size: Individual size statistics object.

    Returns:
        tools.MultiStatistics: Configured statistical objects.
    """
    mstats = tools.MultiStatistics(stat_fit=stat_fit,stat_size=stat_size)
    mstats.register("avg", np.mean)
    mstats.register("std", np.std)
    mstats.register("min", np.min)
    mstats.register("max", np.max)
    return mstats

def initialize_halloffame(maxsize=50):
    """
    Initialize the Hall of Fame object, which is used to save the optimal individuals that appear during the evolution process.

    Args:
        maxsize: The maximum capacity of the Hall of Fame.

    Returns:
        tools.HallOfFame: Hall of Fame object.
    """
    hof = tools.HallOfFame(maxsize)
    return hof

def easy_initialize_gpsettings(context, min_depths=2, fitness_weights=(1.0,), 
                          max_depths=5, tournsize=20, mutate_max=17, mate_max=17, expr_mut_range=(0,2),
                          use_cross_section=True,use_timeseries=True,wq_operators=False,
                          **kwargs):
    """
    Initialize all settings for genetic programming with one click.

    Args:
        context: context object.
        min_depths: The minimum depth of the tree.
        fitness_weights: Fitness weight.
        max_depths: The maximum depth of the tree.
        tournsize: Tournament selection size.
        mutate_max: Variation maximum height.
        mate_max: Maximum cross height.
        expr_mut_range: Variation depth range.
        use_cross_section: Whether to use the section operator.
        use_timeseries: Whether to use timing operators.
        wq_operators: Whether to use the WorldQuant operator.
        **kwargs: other parameters.

    Returns:
        dict: Dictionary containing pset, toolbox, hof, mstats and creator.
    """
    
    initialize_individual_and_fitness_settings(fitness_weights=fitness_weights)

    pset = initialize_primitiveset(context=context, array_type=ndarray, wq_operators=wq_operators,use_cross_section=use_cross_section, use_timeseries=use_timeseries)
    toolbox = initialize_toolbox(
        pset=pset,
        min_depths=min_depths,
        max_depths=max_depths,
        tournsize=tournsize,
        mutate_max=mutate_max,
        mate_max=mate_max,
        expr_mut_range=expr_mut_range,
        **kwargs
    )
    hof = initialize_halloffame(50)
    mstats = initialize_statistics()

    return {
        "pset": pset,
        "toolbox": toolbox,
        "hof": hof,
        "mstats": mstats,
        "creator":creator
    }
