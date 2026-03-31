"""
因子注册表模块 / Factor registry module

提供全局因子注册、列出和获取功能，支持按名称管理因子实例。
Provides global factor registration, listing, and retrieval by name.
"""

from typing import Dict, List, Optional, Type

from FactorLib.base import BaseFactor

# 全局注册表：name → factor class / global registry: name → factor class
_global_registry: Dict[str, Type[BaseFactor]] = {}


def register(factor_cls: Type[BaseFactor]) -> None:
    """
    注册一个因子类到全局注册表 / Register a factor class into the global registry.

    使用类名作为键，重复注册同名因子会覆盖旧条目并打印警告。
    Uses the class name as key; re-registering the same name overwrites
    the previous entry with a warning.

    Parameters / 参数:
        factor_cls (Type[BaseFactor]): 继承自 BaseFactor 的因子类。
            A factor class that inherits from BaseFactor.
    """
    if not (isinstance(factor_cls, type) and issubclass(factor_cls, BaseFactor)):
        raise TypeError(
            f"只能注册 BaseFactor 子类，收到 {factor_cls!r} / "
            f"can only register BaseFactor subclasses, got {factor_cls!r}"
        )
    name = factor_cls.__name__
    if name in _global_registry:
        import warnings
        warnings.warn(
            f"因子 '{name}' 已存在，将被覆盖 / "
            f"factor '{name}' already registered, will be overwritten",
            UserWarning,
            stacklevel=2,
        )
    _global_registry[name] = factor_cls


def list_factors() -> List[str]:
    """
    列出所有已注册的因子名称 / List all registered factor names.

    Returns / 返回:
        List[str]: 已注册因子类名列表 / List of registered factor class names.
    """
    return list(_global_registry.keys())


def get(name: str) -> Optional[Type[BaseFactor]]:
    """
    按名称获取已注册的因子类 / Get a registered factor class by name.

    Parameters / 参数:
        name (str): 因子类名 / Factor class name.

    Returns / 返回:
        Optional[Type[BaseFactor]]: 对应的因子类，未找到则返回 None。
            The corresponding factor class, or None if not found.
    """
    return _global_registry.get(name)


def clear() -> None:
    """
    清空全局注册表 / Clear the global registry.

    主要用于测试场景 / Primarily used in test scenarios.
    """
    _global_registry.clear()
