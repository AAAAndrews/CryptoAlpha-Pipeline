"""
Multi-threaded bulk downloader for Binance data with Feather storage
"""

import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from itertools import product
from typing import List, Optional, Dict, Any

import pandas as pd
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table

from CryptoDataProviders.providers.binance_bulk.bulk_fetcher import BinanceBulkFetcher
from CryptoDataProviders.utils.common import build_kline_filepath
from .storage import read_feather, upsert_klines, get_synced_filepath

# --- Constants ---
CONSOLE = Console()

KLINE_TYPE_TO_ASSET = {
    "spot": "spot",
    "swap": "um",
    "mark": "um",
    "index": "um",
}

# --- Internal Helpers ---

def _create_summary_table(title: str, config: Dict[str, Any]) -> Table:
    """
    Create a rich text table (Rich Table) to display the task configuration summary on the console.
    
    parameter:
        title (str): Table title.
        config (Dict): Configuration item dictionary.
        
    return:
        Table: Rich Table object.
    """
    table = Table(title=title)
    table.add_column("parameter", style="cyan")
    table.add_column("value", style="green")
    for k, v in config.items():
        table.add_row(k, str(v))
    return table

# --- Core Logic ---

def process_symbol_interval(
    db_root_path: str,
    exchange: str,
    symbol: str,
    kline_type: str,
    interval: str,
    batch_size: int = 100,
    proxy: Optional[dict] = None,
    progress: Optional[Progress] = None
):
    """
    Download logic that handles individual trading pairs and time interval combinations.
    Contains incremental logic: automatically detects the latest local timestamp and starts requesting Binance S3 buckets from that date.
    
    parameter:
        db_root_path (str): Database storage root path.
        exchange (str): Exchange name (usually'binance'）。
        symbol (str): trading pair.
        kline_type (str): KLine type (spot, swap, etc.).
        interval (str): Kline cycle.
        batch_size (int): The number of files saved in batches.
        proxy (Optional[dict]): Agent configuration.
        progress (Optional[Progress]): Rich Progress bar instance.
    """
    """Core logic for handling single trading pair-time interval combinations"""
    asset = KLINE_TYPE_TO_ASSET.get(kline_type, "spot")
    log = progress.console.print if progress else CONSOLE.print
    
    log(f"[cyan]🚀 Start processing:{symbol} {interval} ({kline_type})[/cyan]")
    
    file_path = get_synced_filepath(db_root_path, exchange, symbol, kline_type, interval)
    existing_df = read_feather(file_path)
    
    if not existing_df.empty:
        log(f"[blue]  Already{len(existing_df)} row data[/blue]")
    
    last_ts = int(existing_df["timestamp"].max()) if not existing_df.empty and "timestamp" in existing_df.columns else 0

    # Calculate start date based on last_ts to avoid redundant downloads
    start_date = None
    if last_ts > 0:
        dt = datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc)
        start_date = dt.strftime("%Y-%m-%d")
        log(f"[blue]  Breakpoint detected:{start_date}, Incremental downloads will begin from this date[/blue]")

    fetcher = BinanceBulkFetcher(
        data_type="klines", data_frequency=interval, asset=asset,
        timeperiod_per_file="daily", symbols=symbol, proxy=proxy, progress=progress,
        start_date=start_date,
    )
    
    total_saved = 0

    try:
        for batch_df in fetcher.fetch_batches(batch_size=batch_size):
            if batch_df is None or batch_df.empty:
                continue
            
            prepared_df = batch_df.copy()
            # Make sure open_time is a numeric type and filter out possible header rows (although the fetcher has already been processed, secondary insurance is done here)
            prepared_df["open_time"] = pd.to_numeric(prepared_df["open_time"], errors='coerce')
            prepared_df = prepared_df.dropna(subset=["open_time"])
            
            prepared_df["timestamp"] = prepared_df["open_time"].astype("int64")
            prepared_df["created_at"] = pd.Timestamp.utcnow()
            
            # incremental filtering
            prepared_df = prepared_df[prepared_df["timestamp"] > last_ts]
            
            if not prepared_df.empty:
                upsert_klines(file_path, prepared_df)
                total_saved += len(prepared_df)
                last_ts = int(prepared_df["timestamp"].max())
        
        status = f"[green]✓ saved{total_saved} OK[/green]" if total_saved > 0 else "[yellow]⚠️ No new data[/yellow]"
        log(f"{status} -> {symbol} {interval} ({kline_type})")
            
    except Exception as exc:
        log(f"[red]❌ {symbol} {interval} ({kline_type}) fail:{str(exc)[:100]}[/red]")

# --- Public API ---

def run_bulk_updater(
    db_root_path: str,
    exchange: str,
    symbol_list: List[str],
    kline_type_list: List[str],
    interval_list: List[str],
    batch_size: int = 100,
    max_workers: int = 5,
    proxy: Optional[dict] = None,
    single_threaded: bool = False
):
    """
    The main entrance to launch the Binance S3 historical data batch download task.
    Supports concurrent downloads (multi-threaded mode) or single-threaded mode.
    
    parameter:
        db_root_path (str): Database root path.
        exchange (str): Exchange ('binance'）。
        symbol_list (List[str]): List of trading pairs.
        kline_type_list (List[str]): KList of line types.
        interval_list (List[str]): List of time intervals.
        batch_size (int): Number of files processed per batch.
        max_workers (int): Number of parallel threads.
        proxy (Optional[dict]): acting.
        single_threaded (bool): True to force single-threaded sequential execution.
    """
    """Unified batch update entrance"""
    config = {
        "exchange": exchange,
        "Number of trading pairs": len(symbol_list),
        "KLine type": ", ".join(kline_type_list),
        "time interval": ", ".join(interval_list),
        "model": "single thread" if single_threaded else f"multithreading ({max_workers})",
        "acting": "Enabled" if proxy else "Not enabled"
    }
    CONSOLE.print(_create_summary_table("📊 Batch download task configuration", config))
    
    # Use itertools.product to generate task combinations
    tasks = list(product(symbol_list, kline_type_list, interval_list))
    CONSOLE.print(f"[bold cyan]📋 total{len(tasks)} tasks[/bold cyan]\n")
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=CONSOLE
    ) as progress:
        task_id = progress.add_task("[cyan]processing tasks", total=len(tasks))
        
        if single_threaded:
            for symbol, kt, interval in tasks:
                process_symbol_interval(db_root_path, exchange, symbol, kt, interval, batch_size, proxy, progress)
                progress.update(task_id, advance=1)
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(process_symbol_interval, db_root_path, exchange, s, kt, i, batch_size, proxy, progress): (s, kt, i)
                    for s, kt, i in tasks
                }
                for future in as_completed(futures):
                    progress.update(task_id, advance=1)
                    
    CONSOLE.print("\n[bold green]✅ All tasks completed![/bold green]")
