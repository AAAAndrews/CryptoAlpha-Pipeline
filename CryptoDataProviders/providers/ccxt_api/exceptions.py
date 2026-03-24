"""Custom exceptions for the ccxt market API wrapper."""
from __future__ import annotations


class MarketAPIError(Exception):
    """Generic error raised by the market API wrapper."""


class ExchangeNotSupported(MarketAPIError):
    """Raised when the requested exchange is not yet configured."""


class TimeframeNotSupported(MarketAPIError):
    """Raised when the requested timeframe/interval is not available."""
