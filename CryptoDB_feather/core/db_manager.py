import os
from datetime import datetime, timezone, timedelta
from typing import Optional

import pandas as pd
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table

from CryptoDataProviders.providers.binance_api import fetch_klines as binance_fetch_klines
from CryptoDataProviders.utils.common import log_error_to_json
from .storage import read_feather, upsert_klines, get_synced_filepath


DEFAULT_START_MS = int(datetime(2000, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)


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
    Use Binance REST API for incremental K-line updates.
    
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