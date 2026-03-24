"""
CryptoDataProviders - Cryptocurrency data source unified interface

Provides data capture interfaces for multiple exchanges, supporting:
- Binance REST API (spot, swap, mark, index)
- Binance Bulk Download (Historical data batch download)
- CCXT Unified interface for multiple exchanges

Main functions:
- Get K-line data (OHLCV)
- Get list of trading pairs
- Support multiple time granularities
- Support proxy configuration
"""

__version__ = "1.0.0"
__author__ = "Your Name"

from .providers.binance_api.market_api import fetch_klines as fetch_binance_klines
from .providers.ccxt_api.fetcher import CCXTKlineFetcher
from .providers.binance_bulk.bulk_fetcher import BinanceBulkFetcher
from .utils.trading_pairs import get_trading_pairs

__all__ = [
    "fetch_binance_klines",
    "CCXTKlineFetcher",
    "BinanceBulkFetcher",
    "get_trading_pairs",
]
