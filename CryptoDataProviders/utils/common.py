import os
import json
from datetime import datetime, timezone
from typing import Optional, Union, Any, Dict, List
import pandas as pd

# --- Time Utilities ---

TimeLike = Union[str, int, float, datetime]

_TIME_FORMATS: tuple[str, ...] = (
    "%Y%m%d %H%M%S",
    "%Y%m%d%H%M%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d",
    "%Y%m%d",
)

def parse_time(value: TimeLike) -> int:
    """
    Convert time input in multiple formats to UTC millisecond timestamps.
    
    parameter:
        value (TimeLike): Input time, supporting datetime objects, integers/floats (seconds or millisecond stamps), and strings in various formats.
        
    return:
        int: UTC Millisecond timestamp.
        
    abnormal:
        ValueError: Thrown when the string format is not recognized.
        TypeError: Thrown when the input type is not supported.
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)

    if isinstance(value, (int, float)):
        ivalue = int(value)
        if ivalue > 10**12:  # already ms
            return ivalue
        if ivalue > 10**9:  # seconds with some headroom
            return ivalue * 1000
        return ivalue * 1000

    if isinstance(value, str):
        cleaned = " ".join(value.strip().split())
        for fmt in _TIME_FORMATS:
            try:
                dt = datetime.strptime(cleaned, fmt)
                dt = dt.replace(tzinfo=timezone.utc)
                return int(dt.timestamp() * 1000)
            except ValueError:
                continue
        raise ValueError(
            f"Unsupported time format: '{value}'. Expected like '20250101 010101' or ISO 8601."
        )

    raise TypeError(f"Unsupported time value type: {type(value)}")

# --- Path Utilities ---

def build_kline_filepath(db_root_path: str, exchange: str, symbol: str, kline_type: str, interval: str) -> str:
    """
    Build a standardized Feather file storage path based on the given exchange, currency, candlestick type and interval.
    
    parameter:
        db_root_path (str): Database root directory.
        exchange (str): Exchange name (e.g.'binance'）。
        symbol (str): Trading pair name (e.g.'BTCUSDT'）。
        kline_type (str): KLine type (e.g.'swap', 'spot'）。
        interval (str): Kline spacing (e.g.'1h', '1d'）。
        
    return:
        str: Full absolute file path (pointing to klines.feather).
    """
    return os.path.join(db_root_path, exchange, symbol, kline_type, interval, "klines.feather")

# --- Logging Utilities ---

def log_error_to_json(error_dir: str, error_info: Dict[str, Any], retry_info: Optional[Dict[str, Any]] = None):
    """
    Log error information to a JSON file and update the retry list as needed.
    
    parameter:
        error_dir (str): Directory to store error logs.
        error_info (Dict): A dictionary of error details logged to errors.json.
        retry_info (Optional[Dict]): If provided, a dictionary of task information to be retried logged to retry_pairs.json.
    """
    os.makedirs(error_dir, exist_ok=True)
    
    # Update errors.json
    error_file = os.path.join(error_dir, "errors.json")
    try:
        if os.path.exists(error_file):
            with open(error_file, 'r', encoding='utf-8') as f:
                logs = json.load(f)
        else:
            logs = []
    except (json.JSONDecodeError, IOError):
        logs = []
    
    logs.append({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        **error_info
    })
    
    with open(error_file, 'w', encoding='utf-8') as f:
        json.dump(logs, f, indent=4, ensure_ascii=False)
    
    # Update retry_pairs.json if provided
    if retry_info:
        retry_file = os.path.join(error_dir, "retry_pairs.json")
        try:
            if os.path.exists(retry_file):
                with open(retry_file, 'r', encoding='utf-8') as f:
                    retry_list = json.load(f)
            else:
                retry_list = []
        except (json.JSONDecodeError, IOError):
            retry_list = []
            
        # Check for duplicates using all items in retry_info
        exists = any(all(item.get(k) == v for k, v in retry_info.items()) for item in retry_list)
        if not exists:
            retry_list.append(retry_info)
            with open(retry_file, 'w', encoding='utf-8') as f:
                json.dump(retry_list, f, indent=4, ensure_ascii=False)

# --- Progress Tracking ---

class ProgressTracker:
    """
    A lightweight progress report class used to output task progress without relying on the heavy library.
    
    parameter:
        total (Optional[int]): Total number of tasks.
        enabled (bool): Whether to enable progress output.
        label (str): Progress bar label (such as'klines'）。
    """
    def __init__(self, total: Optional[int], enabled: bool, label: str = "klines"):
        self.total = total
        self.enabled = enabled
        self.label = label
        self._count = 0
        self._last_percentage = -1

    def advance(self, step: int = 1) -> None:
        """
        Increase progress.
        
        parameter:
            step (int): step value.
        """
        if not self.enabled:
            return
        self._count += step
        if self.total:
            pct = int((self._count / self.total) * 100)
            if pct != self._last_percentage:
                self._last_percentage = pct
                print(f"Progress: {pct}% ({self._count}/{self.total} {self.label})")
        else:
            print(f"Progress: {self._count} {self.label}")
