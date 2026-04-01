"""
FactorLib — 独立因子库模块 / Standalone factor library module

面向手动定义因子，与 deap_alpha（遗传编程自动挖掘）解耦。
For manually defined factors, decoupled from deap_alpha (GP auto-discovery).
"""

from FactorLib.alpha_momentum import AlphaMomentum
from FactorLib.alpha_price_range import AlphaPriceRange
from FactorLib.alpha_volatility import AlphaVolatility
from FactorLib.base import BaseFactor
from FactorLib.registry import clear, get, list_factors, register

# 将所有内置因子注册到全局注册表 / register all built-in factors
register(AlphaMomentum)
register(AlphaPriceRange)
register(AlphaVolatility)

__all__ = [
    "BaseFactor",
    "AlphaMomentum",
    "AlphaPriceRange",
    "AlphaVolatility",
    "register",
    "list_factors",
    "get",
    "clear",
]
