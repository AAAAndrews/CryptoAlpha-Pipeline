from deap import gp
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import joblib

def check_if_the_pset_can_compile(expr,pset):
    success_to_transform=[]
    try:
        gp.PrimitiveTree.from_string(expr, pset)
    except:
        return False

    if str(gp.PrimitiveTree.from_string(expr, pset)).replace(" ","")!=expr.replace(" ",""):
        return False

    elif str(gp.PrimitiveTree.from_string(expr, pset)).replace(" ","")==expr.replace(" ",""):
        return True