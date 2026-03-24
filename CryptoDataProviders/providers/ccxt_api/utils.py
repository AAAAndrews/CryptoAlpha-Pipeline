"""Utility helpers for the ccxt market API wrapper."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, Optional, Union
import math
from utils.common import parse_time, ProgressTracker

Number = Union[int, float]
TimeLike = Union[str, Number, datetime]

_QUOTE_HINTS: tuple[str, ...] = (
    "USDT",
    "BUSD",
    "USDC",
    "USD",
    "BTC",
    "ETH",
)

_UNIT_TO_MS: Dict[str, int] = {
    "s": 1_000,
    "m": 60_000,
    "h": 3_600_000,
    "d": 86_400_000,
    "w": 604_800_000,
}


def normalize_symbol(symbol: str) -> str:
    """
    Convert trading pair names to CCXT standard BASE/QUOTE format.
    
    parameter:
        symbol (str): Original trading pair name (e.g.'BTCUSDT')。
        
    return:
        str: CCXT Standard format (e.g.'BTC/USDT')。
    """
    cleaned = symbol.upper().replace("-", "").replace("_", "").replace(" ", "")
    if "/" in symbol:
        parts = symbol.upper().split("/")
        return f"{parts[0]}/{parts[1]}"

    for quote in _QUOTE_HINTS:
        if cleaned.endswith(quote) and len(cleaned) > len(quote):
            base = cleaned[: -len(quote)]
            return f"{base}/{quote}"

    raise ValueError(
        "Unable to infer quote asset from symbol. Use explicit BASE/QUOTE notation."
    )


def apply_symbol_transform(symbol: str, transform: Optional[str]) -> str:
    """Apply exchange-specific symbol tweaks."""

    if transform is None:
        return symbol
    if transform == "binance_swap":
        if ":" in symbol:
            return symbol
        base, quote = symbol.split("/")
        return f"{base}/{quote}:{quote}"
    raise ValueError(f"Unknown symbol transform '{transform}'.")


def normalize_timeframe(interval: str, supported: Optional[Dict[str, str]]) -> str:
    """
    Insert user-friendly time intervals such as'1 hour', '15 mins'）Convert to CCXT standard time interval string (e.g.'1h', '15m'）。
    
    parameter:
        interval (str): The input time interval string.
        supported (Optional[Dict]): Dictionary of time intervals supported by the exchange for verification.
        
    return:
        str: The verified CCXT standard time interval string.
    """
    norm = interval.strip().lower().replace("minutes", "min").replace("seconds", "s")
    norm = norm.replace("minute", "min").replace("second", "s")
    norm = norm.replace("hours", "h").replace("hour", "h")
    replacements = {
        "sec": "s",
        "mins": "m",
        "min": "m",
        "hrs": "h",
    }
    for old, new in replacements.items():
        if norm.endswith(old):
            norm = norm[: -len(old)] + new
    norm = norm.replace(" ", "")

    if norm and norm[-1] in _UNIT_TO_MS:
        ccxt_tf = norm
    else:
        raise ValueError(
            "Interval must include a unit suffix such as s/m/h/d (e.g. '1m', '1h')."
        )

    if supported and ccxt_tf not in supported:
        raise ValueError(
            f"Exchange does not support timeframe '{ccxt_tf}'. Available: {list(supported)}"
        )
    return ccxt_tf


def timeframe_to_milliseconds(timeframe: str) -> int:
    """
    Convert the CCXT time interval string (e.g.'1h'）Convert to milliseconds.
    
    parameter:
        timeframe (str): Time interval string.
        
    return:
        int: The corresponding number of milliseconds.
    """
    unit = timeframe[-1]
    if unit not in _UNIT_TO_MS:
        raise ValueError(f"Unsupported timeframe unit '{unit}'.")
    value = int(timeframe[:-1])
    return value * _UNIT_TO_MS[unit]


def estimate_total_bars(start_ms: int, end_ms: int, tf_ms: int) -> int:
    """Roughly estimate the number of bars in a range."""

    if end_ms <= start_ms:
        return 0
    return math.ceil((end_ms - start_ms) / tf_ms)
