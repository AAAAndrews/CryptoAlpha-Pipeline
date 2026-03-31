import os
from datetime import datetime, timezone, timedelta
from typing import Optional

import pandas as pd
import requests.exceptions
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table

from CryptoDataProviders.providers.binance_api import fetch_klines as binance_fetch_klines
from CryptoDataProviders.utils.common import log_error_to_json
from CryptoDataProviders.utils.retry import retry_with_backoff
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


def _fetch_and_save_pair(
    db_root_path: str,
    exchange: str,
    symbol: str,
    kline_type: str,
    interval: str,
    batch_size: int,
    limit: Optional[int],
    progress: bool,
    proxy: dict,
) -> int:
    """
    对单个交易对执行完整的拉取-保存流程，带重试错误恢复。
    Full fetch-and-save cycle for a single trading pair, with retry error recovery.

    参数 / Parameters:
        db_root_path: 数据库根路径 / Database root path.
        exchange: 交易所名称 / Exchange name.
        symbol: 交易对 / Trading pair symbol.
        kline_type: K线类型 / K-line type (spot/swap/mark/index).
        interval: 时间周期 / Time interval.
        batch_size: 每批拉取数量 / Rows per batch.
        limit: 总数量限制 / Total row limit.
        progress: 是否显示进度 / Show tqdm progress.
        proxy: 代理配置 / Proxy configuration.

    返回 / Returns:
        int: 成功保存的记录数 / Number of rows saved.
    """
    # 加载本地已有数据，确定增量起点 / Load local data to find incremental start point
    existing_df = load_local_klines(
        db_root_path, exchange, symbol, kline_type, interval,
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

    # 拉取数据（底层 market_api 已有重试） / Fetch data (market_api already retries internally)
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

    # 逐批保存 / Save batch by batch
    total_saved = 0
    for batch_df in batch_iter:
        if batch_df is None or batch_df.empty:
            continue

        prepared_df = batch_df.copy()
        prepared_df["timestamp"] = prepared_df["open_time"].astype("int64")
        prepared_df["created_at"] = pd.Timestamp.utcnow()

        save_local_klines(
            db_root_path, exchange, symbol, kline_type, interval, prepared_df,
        )
        total_saved += len(prepared_df)

    return total_saved


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

    # 为单个交易对的处理函数添加重试装饰器 / Add retry decorator for per-pair processing
    retriable_fetch_and_save = retry_with_backoff(
        max_retries=2,
        base_delay=5.0,
        max_delay=30.0,
        exponential_base=5.0,
        retryable_exceptions=(requests.exceptions.RequestException, OSError),
    )(_fetch_and_save_pair)

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
                    try:
                        total_saved = retriable_fetch_and_save(
                            db_root_path, exchange, symbol, kline_type, interval,
                            batch_size, limit, progress, proxy,
                        )
                    except Exception as exc:
                        # 重试全部耗尽后，记录错误并跳过该交易对 / After all retries exhausted, log and skip
                        log_error(db_root_path, exchange, symbol, kline_type, interval, str(exc))
                        progress_bar.console.print(
                            f"[red]❌ Failed{symbol} {interval} ({kline_type}): {exc}[/red]"
                        )
                        progress_bar.update(task_progress, advance=1)
                        continue

                    if total_saved > 0:
                        progress_bar.console.print(
                            f"[green]✓ saved{total_saved} OK:{exchange} {symbol} {interval} ({kline_type})[/green]"
                        )

                    progress_bar.update(task_progress, advance=1)

    console.print("\n[bold green]✅ Binance API Update task completed![/bold green]")