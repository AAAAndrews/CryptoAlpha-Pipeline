"""Exchange-specific configuration for the ccxt-based market API."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass(frozen=True)
class KlineTypeProfile:
    """Describes how to talk to a specific exchange/kline-type pair."""

    exchange_id: str
    params: Dict[str, object] = field(default_factory=dict)
    symbol_transform: Optional[str] = None


@dataclass(frozen=True)
class ExchangeProfile:
    """High-level configuration per logical exchange handle."""

    default_type: str
    kline_types: Dict[str, KlineTypeProfile]


EXCHANGE_PROFILES: Dict[str, ExchangeProfile] = {
    "binance": ExchangeProfile(
        default_type="spot",
        kline_types={
            "spot": KlineTypeProfile(
                exchange_id="binance",
                symbol_transform="binance_spot",
                                     ),
            "usdmswap": KlineTypeProfile(
                exchange_id="binanceusdm",
                symbol_transform="binance_swap",
            ),
            "usdmindex": KlineTypeProfile(
                exchange_id="binanceusdm",
                params={"price": "index"},

            ),
            "usdmmark": KlineTypeProfile(
                exchange_id="binanceusdm",
                params={"price": "mark"},

            ),
        },
    ),
}

ALIAS_TO_EXCHANGE: Dict[str, str] = {
    "binanceusdm": "binance",
    "binance-futures": "binance",
    "binance-perp": "binance",
}


def resolve_exchange_profile(exchange: str) -> ExchangeProfile:
    """Return the ExchangeProfile for an exchange alias."""

    norm = exchange.lower()
    logical = ALIAS_TO_EXCHANGE.get(norm, norm)
    print(logical)
    if logical not in EXCHANGE_PROFILES:
        raise ValueError(f"Exchange '{exchange}' has no registered profile yet.")
    return EXCHANGE_PROFILES[logical]


def resolve_kline_profile(exchange: str, kline_type: Optional[str]) -> KlineTypeProfile:
    """Retrieve the profile for the requested kline type."""

    profile = resolve_exchange_profile(exchange)
    target_type = (kline_type or profile.default_type).lower()
    if target_type not in profile.kline_types:
        raise ValueError(
            f"Exchange '{exchange}' does not support kline_type '{target_type}'."
        )
    return profile.kline_types[target_type]
