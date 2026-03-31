"""
FactorLib — 独立因子库模块 / Standalone factor library module

面向手动定义因子，与 deap_alpha（遗传编程自动挖掘）解耦。
For manually defined factors, decoupled from deap_alpha (GP auto-discovery).
"""

from FactorLib.base import BaseFactor

__all__ = ["BaseFactor"]
