"""
retry.py — 指数退避重试装饰器 / Exponential backoff retry decorator

提供通用的重试机制，用于包装可能因网络抖动或限流而失败的函数。
Provides a generic retry mechanism for wrapping functions that may fail
due to network jitter or rate limiting.
"""

import time
import random
import functools
import logging
from typing import Callable, Optional, Tuple, Type

logger = logging.getLogger(__name__)

# 默认捕获的异常类型 / Default exceptions to catch
_DEFAULT_EXCEPTIONS: Tuple[Type[Exception], ...] = (Exception,)


def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retryable_exceptions: Optional[Tuple[Type[Exception], ...]] = None,
    on_retry: Optional[Callable[[int, Exception, float], None]] = None,
) -> Callable:
    """
    指数退避重试装饰器 / Exponential backoff retry decorator.

    当被装饰函数抛出指定异常时，按指数退避策略延迟后重试。
    Retries the decorated function with exponential backoff delay
    upon specified exceptions.

    参数 / Parameters:
        max_retries: 最大重试次数（不含首次调用） / Max retries (excluding the first call).
        base_delay: 首次重试的基础等待秒数 / Base delay in seconds for the first retry.
        max_delay: 单次重试的最大等待秒数上限 / Maximum delay cap per retry.
        exponential_base: 退避指数基数 / Exponential growth base for backoff.
        jitter: 是否在退避时间上添加随机抖动以避免惊群 / Add random jitter to avoid thundering herd.
        retryable_exceptions: 触发重试的异常类型元组；默认为 (Exception,) / Exception types that trigger retry.
        on_retry: 每次重试前的回调 (attempt, exception, delay) / Callback before each retry.
    """

    exceptions = retryable_exceptions or _DEFAULT_EXCEPTIONS

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    # 最后一次尝试失败后不再等待，直接抛出 / No delay on final failure, raise immediately
                    if attempt >= max_retries:
                        logger.warning(
                            "%s failed after %d retries: %s",
                            func.__name__, max_retries, exc,
                        )
                        raise

                    # 计算退避时间 / Calculate backoff delay
                    delay = min(base_delay * (exponential_base ** attempt), max_delay)
                    if jitter:
                        delay = delay * (0.5 + random.random())

                    logger.info(
                        "%s attempt %d/%d failed (%s), retrying in %.1fs",
                        func.__name__, attempt + 1, max_retries + 1, exc, delay,
                    )

                    # 执行用户回调 / Invoke user callback
                    if on_retry is not None:
                        on_retry(attempt + 1, exc, delay)

                    time.sleep(delay)

            # 理论上不会到达此处 / Should never reach here
            raise last_exc  # type: ignore[misc]

        return wrapper

    return decorator
