"""Core ccxt-based kline fetching logic."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Union
import time
import json
import os
from datetime import datetime

import ccxt

from .config import resolve_kline_profile
from .exceptions import ExchangeNotSupported, MarketAPIError, TimeframeNotSupported
from .utils import (
    apply_symbol_transform,
    estimate_total_bars,
    normalize_symbol,
    normalize_timeframe,
    timeframe_to_milliseconds,
)
from utils.common import parse_time, ProgressTracker, log_error_to_json


def log_error(exchange: str, symbol: str, interval: str, kline_type: str, error_msg: str):
    """Log error information for failed operations in market_api_ccxt."""
    error_dir = os.path.join(os.path.dirname(__file__), "errors")
    error_entry = {
        "exchange": exchange,
        "symbol": symbol,
        "interval": interval,
        "kline_type": kline_type,
        "error_message": str(error_msg)
    }
    log_error_to_json(error_dir, error_entry)


ProxyConfig = Union[str, Dict[str, str]]


class CCXTKlineFetcher:
    """
    A high-level CCXT encapsulation class used to abstract the differences between different exchanges.
    Supports automatic conversion of trading pair formats, processing time granularity, and batch capture.
    
    parameter:
        exchange (str): Exchange abbreviation.
        kline_type (Optional[str]): KLine type.
        **client_kwargs (Any): Parameters passed to the ccxt client constructor.
    """

    def __init__(self, exchange: str, kline_type: Optional[str], **client_kwargs: Any) -> None:
        try:
            self.kline_profile = resolve_kline_profile(exchange, kline_type)
        except ValueError as exc:
            raise ExchangeNotSupported(str(exc)) from exc

        exchange_id = self.kline_profile.exchange_id
        try:
            exchange_cls = getattr(ccxt, exchange_id)
        except AttributeError as exc:
            raise ExchangeNotSupported(
                f"ccxt does not expose an exchange named '{exchange_id}'."
            ) from exc

        options = {"enableRateLimit": True}
        options.update(client_kwargs)
        self.client = exchange_cls(options)
        self.client.load_markets()

    def fetch_klines(
        self,
        symbol: str,
        interval: str,
        start_time: Any,
        end_time: Any,
        *,
        progress: bool = False,
        batch_size: int = 1000,
        params: Optional[Dict[str, Any]] = None,
    ) -> List[List[Any]]:
        """
        Synchronously capture full or incremental K-line data.
        
        parameter:
            symbol (str): trading pair.
            interval (str): cycle.
            start_time (Any): Start time.
            end_time (Any): end time.
            progress (bool): Whether to display a simple progress report.
            batch_size (int): Fetch volume per crawl.
            params (Optional[Dict]): CCXT Special extra parameters.
            
        return:
            List[List[Any]]: List of CCXT-compliant OHLCVs.
        """
        if not self.client.has.get("fetchOHLCV"):
            raise MarketAPIError("Selected exchange does not support fetchOHLCV.")

        timeframe = self._normalize_timeframe(interval)
        start_ms = parse_time(start_time)
        end_ms = parse_time(end_time)
        if end_ms <= start_ms:
            raise ValueError("end_time must be greater than start_time")

        tf_ms = timeframe_to_milliseconds(timeframe)
        estimated = estimate_total_bars(start_ms, end_ms, tf_ms)
        tracker = ProgressTracker(total=estimated, enabled=progress)

        normalized_symbol = apply_symbol_transform(
            normalize_symbol(symbol), self.kline_profile.symbol_transform
        )

        merged_params: Dict[str, Any] = {}
        merged_params.update(self.kline_profile.params)
        if params:
            merged_params.update(params)

        cursor = start_ms
        results: List[List[Any]] = []
        last_timestamp = None
        limit = self._pick_limit(batch_size)
        max_retries = 5
        backoff = 1.5
        last_error: Optional[Exception] = None

        while cursor < end_ms:
            try:
                chunk = self.client.fetch_ohlcv(
                    normalized_symbol,
                    timeframe=timeframe,
                    since=cursor,
                    limit=limit,
                    params=merged_params,
                )
                last_error = None
            except ccxt.BaseError as exc:  # type: ignore[attr-defined]
                log_error(self.kline_profile.exchange_id, symbol, interval, self.kline_profile.type, str(exc))
                last_error = exc
                max_retries -= 1
                if max_retries <= 0:
                    raise MarketAPIError(f"Exhausted retries: {exc}") from exc
                time.sleep(backoff)
                backoff *= 1.5
                continue

            if not chunk:
                cursor += tf_ms * limit
                continue

            appended = 0
            for candle in chunk:
                ts = candle[0]
                if ts is None:
                    continue
                if ts >= end_ms:
                    break
                if ts < start_ms:
                    continue
                if last_timestamp is not None and ts <= last_timestamp:
                    continue
                results.append(candle)
                last_timestamp = ts
                tracker.advance(1)
                appended += 1

            if last_timestamp is None:
                cursor += tf_ms * limit
            else:
                cursor = last_timestamp + tf_ms

            if appended == 0:
                cursor += tf_ms * limit

        return results

    def _pick_limit(self, batch_size: int) -> int:
        limit_config = (
            self.client.describe()
            .get("limits", {})
            .get("fetchOHLCV", {})
        )
        max_limit = limit_config.get("max")
        if max_limit:
            return min(batch_size, max_limit)
        return batch_size

    def _normalize_timeframe(self, interval: str) -> str:
        try:
            return normalize_timeframe(interval, self.client.timeframes)
        except ValueError as exc:
            raise TimeframeNotSupported(str(exc)) from exc


def fetch_klines(
    *,
    exchange: str,
    symbol: str,
    interval: str,
    start_time: Any,
    end_time: Any,
    progress: bool = False,
    batch_size: int = 1000,
    kline_type: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
    client_options: Optional[Dict[str, Any]] = None,
    proxy: Optional[ProxyConfig] = None,
) -> List[List[Any]]:
    """Module-level convenience wrapper."""

    options = dict(client_options or {})
    if proxy:
        options = _attach_proxy(options, proxy)

    fetcher = CCXTKlineFetcher(
        exchange,
        kline_type,
        **options,
    )
    return fetcher.fetch_klines(
        symbol=symbol,
        interval=interval,
        start_time=start_time,
        end_time=end_time,
        progress=progress,
        batch_size=batch_size,
        params=params,
    )


def _attach_proxy(options: Dict[str, Any], proxy: ProxyConfig) -> Dict[str, Any]:
    """Inject proxy settings without mutating caller-provided dict."""

    proxy_map: Dict[str, str] = {}
    existing = options.get("proxies")
    if isinstance(existing, dict):
        proxy_map.update(existing)

    if isinstance(proxy, str):
        proxy_map.update({"http": proxy, "https": proxy})
    else:
        proxy_map.update(proxy)

    merged = dict(options)
    merged["proxies"] = proxy_map
    return merged
