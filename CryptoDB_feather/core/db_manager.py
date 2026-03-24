import json
import os
from datetime import datetime, timezone, timedelta
from typing import Optional, List

import pandas as pd
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table

from CryptoDataProviders.providers.binance_api import fetch_klines as binance_fetch_klines
from CryptoDataProviders.providers.ccxt_api import fetch_klines as ccxt_fetch_klines, resolve_exchange_profile
from CryptoDataProviders.providers.ccxt_api.utils import timeframe_to_milliseconds
from CryptoDataProviders.utils.common import parse_time, log_error_to_json
from .storage import read_feather, upsert_klines, get_synced_filepath


DEFAULT_START_MS = int(datetime(2000, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
BASE_CANDLE_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume"]


def log_error(db_root_path: str, exchange: str, symbol: str, kline_type: str, interval: str, error_msg: str):
    """
    Record the error message of failed fetching and update the list of trading pairs to be retried.
    
    parameter:
        db_root_path (str): Database root directory.
        exchange (str): Exchange name.
        symbol (str): trading pair.
        kline_type (str): K Line type.
        interval (str): K line cycle.
        error_msg (str): Error message details.
    """
    error_dir = os.path.join(db_root_path, "errors")
    error_info = {
        "exchange": exchange,
        "symbol": symbol,
        "kline_type": kline_type,
        "interval": interval,
        "error_message": str(error_msg)
    }
    retry_info = {
        "exchange": exchange,
        "symbol": symbol,
        "kline_type": kline_type,
        "interval": interval
    }
    log_error_to_json(error_dir, error_info, retry_info)


def _build_candle_columns(length: int) -> list[str]:
    """
    Dynamically construct DataFrame column names based on the returned K-line data length.
    
    parameter:
        length (int): Data row length.
        
    return:
        list[str]: List of column names.
    """
    if length <= len(BASE_CANDLE_COLUMNS):
        return BASE_CANDLE_COLUMNS[:length]
    extra = [f"value_{idx}" for idx in range(length - len(BASE_CANDLE_COLUMNS))]
    return BASE_CANDLE_COLUMNS + extra


def load_local_klines(
        db_root_path: str,
        exchange: str,
        symbol: str,
        kline_type: str,
        interval: str,
) -> pd.DataFrame:
    """
    Load existing candlestick data from the local Feather library.
    
    parameter:
        db_root_path (str): Database root path.
        exchange (str): Exchange.
        symbol (str): trading pair.
        kline_type (str): K Line type.
        interval (str): interval.
        
    return:
        pd.DataFrame: Historical data loaded into.
    """
    filepath = get_synced_filepath(
        db_root_path,
        exchange,
        symbol,
        kline_type,
        interval,
    )
    return read_feather(filepath)


def save_local_klines(
        db_root_path: str,
        exchange: str,
        symbol: str,
        kline_type: str,
        interval: str,
        df: pd.DataFrame,
):
    """
    Save the captured K-line data incrementally to the local Feather library.
    
    parameter:
        db_root_path (str): Database root path.
        exchange (str): Exchange.
        symbol (str): trading pair.
        kline_type (str): K Line type.
        interval (str): interval.
        df (pd.DataFrame): Prepare the data for storage.
    """
    filepath = get_synced_filepath(
        db_root_path,
        exchange,
        symbol,
        kline_type,
        interval,
    )
    upsert_klines(filepath, df)


def run_ccxt_updater(
    db_root_path: str,
    exchange_list: list,
    symbol_list: list,
    kline_type_list: list,
    interval_list: list,
):
    """
    Use the CCXT framework to concurrently update K-line data of multiple exchanges and currencies.
    
    parameter:
        db_root_path (str): Database storage root path.
        exchange_list (list): List of exchange IDs (e.g. ['binance', 'okx'])。
        symbol_list (list): List of trading pairs.
        kline_type_list (list): KLine type (e.g. ['swap', 'spot'])。
        interval_list (list): List of time intervals.
    """
    console = Console()
    
    # Print summary table
    table = Table(title="📊 CCXT Incremental update task configuration")
    table.add_column("parameter", style="cyan")
    table.add_column("value", style="green")
    table.add_row("Exchange list", ", ".join(exchange_list))
    table.add_row("trading pair", ", ".join(symbol_list))
    table.add_row("KLine type", ", ".join(kline_type_list) if kline_type_list else "default")
    table.add_row("time interval", ", ".join(interval_list))
    console.print(table)
    console.print()

    # Calculate total tasks
    total_tasks = 0
    for exchange in exchange_list:
        profile = resolve_exchange_profile(exchange)
        candidate_types = kline_type_list or [profile.default_type]
        total_tasks += len(candidate_types) * len(symbol_list) * len(interval_list)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console
    ) as progress:
        task_progress = progress.add_task("[cyan]Handle CCXT tasks", total=total_tasks)

        for exchange in exchange_list:
            profile = resolve_exchange_profile(exchange)
            candidate_types = kline_type_list or [profile.default_type]

            for raw_type in candidate_types:
                target_type = (raw_type or profile.default_type).lower()
                if target_type not in profile.kline_types:
                    progress.console.print(f"[yellow]⚠️  Skip unsupported kline_type'{target_type}' (Exchange:{exchange})[/yellow]")
                    progress.update(task_progress, advance=len(symbol_list) * len(interval_list))
                    continue

                for symbol in symbol_list:
                    for interval in interval_list:
                        interval_norm = interval.strip().lower()
                        try:
                            interval_ms = timeframe_to_milliseconds(interval_norm)
                        except ValueError as exc:
                            progress.console.print(f"[red]❌ Skip invalid intervals'{interval}' ({symbol}): {exc}[/red]")
                            progress.update(task_progress, advance=1)
                            continue

                        existing_df = load_local_klines(
                            db_root_path,
                            exchange,
                            symbol,
                            target_type,
                            interval,
                        )

                        if not existing_df.empty and "timestamp" in existing_df.columns:
                            last_timestamp = existing_df["timestamp"].max()
                            start_ms = int(last_timestamp) if pd.notna(last_timestamp) else DEFAULT_START_MS
                        else:
                            start_ms = DEFAULT_START_MS

                        now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
                        end_ms = now_ms - (now_ms % interval_ms)
                        
                        if end_ms <= start_ms:
                            # progress.console.print(f"[blue]ℹ️  {exchange} {symbol} {interval}: Already the latest[/blue]")
                            progress.update(task_progress, advance=1)
                            continue

                        try:
                            candles = ccxt_fetch_klines(
                                exchange=exchange,
                                symbol=symbol,
                                interval=interval,
                                start_time=start_ms,
                                end_time=end_ms,
                                batch_size=1000,
                                progress=False,
                                kline_type=target_type,
                            )
                        except Exception as exc:
                            log_error(db_root_path, exchange, symbol, target_type, interval, str(exc))
                            progress.console.print(
                                f"[red]❌ Failed to obtain{exchange} {symbol} {interval} ({target_type}): {exc}[/red]"
                            )
                            progress.update(task_progress, advance=1)
                            continue

                        if not candles:
                            # progress.console.print(f"[yellow]⚠️  {exchange} {symbol} {interval}: No data returned[/yellow]")
                            progress.update(task_progress, advance=1)
                            continue

                        columns = _build_candle_columns(len(candles[0]))
                        new_df = pd.DataFrame(candles, columns=columns)
                        new_df["created_at"] = pd.Timestamp.utcnow()

                        save_local_klines(
                            db_root_path,
                            exchange,
                            symbol,
                            target_type,
                            interval,
                            new_df,
                        )
                        progress.console.print(
                            f"[green]✓ saved{len(new_df)} OK:{exchange} {symbol} {interval} ({target_type})[/green]"
                        )
                        progress.update(task_progress, advance=1)
    
    console.print("\n[bold green]✅ CCXT Update task completed![/bold green]")


def run_binance_rest_updater(
    db_root_path: str,
    exchange: str,
    symbol_list: list,
    kline_type_list: list,
    interval_list: list,
    # *,
    batch_size: int = 1000,
    limit: Optional[int] = None,
    progress: bool = False,
    proxy:dict = None
):
    """
    Use Binance REST API for incremental K-line updates. Suitable for scenarios where CCXT does not support or requires native API performance.
    
    parameter:
        db_root_path (str): Database storage root path.
        exchange (str): Exchange name (default'binance'）。
        symbol_list (list): List of trading pairs.
        kline_type_list (list): KLine type (spot, swap, mark, index).
        interval_list (list): Cycle list.
        batch_size (int): The quantity per crawl.
        limit (Optional[int]): Total quantity limit.
        progress (bool): Whether to display tqdm.
        proxy (dict): Agent configuration.
    """
    """Use the legacy REST-based market_api client to refresh Feather stores."""
    console = Console()
    
    # Print summary table
    table = Table(title="📊 Binance API Incremental update task configuration")
    table.add_column("parameter", style="cyan")
    table.add_column("value", style="green")
    table.add_row("exchange", exchange)
    table.add_row("trading pair", ", ".join(symbol_list))
    table.add_row("KLine type", ", ".join(kline_type_list))
    table.add_row("time interval", ", ".join(interval_list))
    table.add_row("batch size", str(batch_size))
    table.add_row("acting", "Enabled" if proxy else "Not enabled")
    console.print(table)
    console.print()

    supported_types = {"spot", "swap", "mark", "index"}
    total_tasks = len(symbol_list) * len(kline_type_list) * len(interval_list)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console
    ) as progress_bar:
        task_progress = progress_bar.add_task("[cyan]Handle Binance API tasks", total=total_tasks)

        for symbol in symbol_list:
            for raw_type in kline_type_list:
                kline_type = (raw_type or "").lower()
                if kline_type not in supported_types:
                    progress_bar.console.print(f"[yellow]⚠️  Skip unsupported kline_type'{raw_type}'[/yellow]")
                    progress_bar.update(task_progress, advance=len(interval_list))
                    continue

                for interval in interval_list:
                    existing_df = load_local_klines(
                        db_root_path,
                        exchange,
                        symbol,
                        kline_type,
                        interval,
                    )

                    ts_column = None
                    if not existing_df.empty:
                        if "timestamp" in existing_df.columns:
                            ts_column = "timestamp"
                        elif "open_time" in existing_df.columns:
                            ts_column = "open_time"

                    if ts_column:
                        last_timestamp = existing_df[ts_column].max()
                        start_ms = int(last_timestamp) + 1 if pd.notna(last_timestamp) else DEFAULT_START_MS
                    else:
                        start_ms = DEFAULT_START_MS

                    end_time = datetime.utcnow() - timedelta(minutes=1)
                    total_saved = 0

                    try:
                        batch_iter = binance_fetch_klines(
                            symbol=symbol,
                            interval=interval,
                            start_time=start_ms,
                            end_time=end_time,
                            batch_size=batch_size,
                            progress=progress,
                            kline_type=kline_type,
                            limit=limit,
                            proxy=proxy,
                        )
                    except Exception as exc:
                        log_error(db_root_path, exchange, symbol, kline_type, interval, str(exc))
                        progress_bar.console.print(
                            f"[red]❌ Initialization failed{symbol} {interval} ({kline_type}): {exc}[/red]"
                        )
                        progress_bar.update(task_progress, advance=1)
                        continue

                    for batch_df in batch_iter:
                        if batch_df is None or batch_df.empty:
                            continue

                        prepared_df = batch_df.copy()
                        prepared_df["timestamp"] = prepared_df["open_time"].astype("int64")
                        prepared_df["created_at"] = pd.Timestamp.utcnow()

                        save_local_klines(
                            db_root_path,
                            exchange,
                            symbol,
                            kline_type,
                            interval,
                            prepared_df,
                        )
                        total_saved += len(prepared_df)

                    if total_saved > 0:
                        progress_bar.console.print(
                            f"[green]✓ saved{total_saved} OK:{exchange} {symbol} {interval} ({kline_type})[/green]"
                        )
                    
                    progress_bar.update(task_progress, advance=1)
    
    console.print("\n[bold green]✅ Binance API Update task completed![/bold green]")