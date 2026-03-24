"""
Binance Bulk Fetcher - Returns DataFrame batches instead of saving files
"""

import io
import zipfile
from typing import Optional, List, Union, Iterator
from xml.etree import ElementTree

import pandas as pd
import requests
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
from rich.console import Console

from .exceptions import (
    BinanceBulkDownloaderDownloadError,
    BinanceBulkDownloaderParamsError,
)


class BinanceBulkFetcher:
    """
    Binance historical data batch capture class. Unlike traditional downloaders that save files directly, this class reads directly from an S3 bucket
    Compressed CSV, decompressed in memory and converted to a Pandas DataFrame, yielded to the caller in batches.
    """

    _BINANCE_DATA_S3_BUCKET_URL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
    _BINANCE_DATA_DOWNLOAD_BASE_URL = "https://data.binance.vision"
    _FUTURES_ASSET = ("um", "cm")
    _OPTIONS_ASSET = ("option",)
    _ASSET = ("spot",)
    _DATA_TYPE_BY_ASSET = {
        "um": {
            "daily": (
                "aggTrades", "bookDepth", "bookTicker", "indexPriceKlines",
                "klines", "liquidationSnapshot", "markPriceKlines", "metrics",
                "premiumIndexKlines", "trades",
            ),
            "monthly": (
                "aggTrades", "bookTicker", "fundingRate", "indexPriceKlines",
                "klines", "markPriceKlines", "premiumIndexKlines", "trades",
            ),
        },
        "cm": {
            "daily": (
                "aggTrades", "bookDepth", "bookTicker", "indexPriceKlines",
                "klines", "liquidationSnapshot", "markPriceKlines", "metrics",
                "premiumIndexKlines", "trades",
            ),
            "monthly": (
                "aggTrades", "bookTicker", "fundingRate", "indexPriceKlines",
                "klines", "markPriceKlines", "premiumIndexKlines", "trades",
            ),
        },
        "spot": {
            "daily": ("aggTrades", "klines", "trades"),
            "monthly": ("aggTrades", "klines", "trades"),
        },
        "option": {"daily": ("BVOLIndex", "EOHSummary")},
    }
    _DATA_FREQUENCY_REQUIRED_BY_DATA_TYPE = (
        "klines", "markPriceKlines", "indexPriceKlines", "premiumIndexKlines",
    )
    _DATA_FREQUENCY = (
        "1s", "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h",
        "6h", "8h", "12h", "1d", "3d", "1w", "1mo",
    )

    def __init__(
        self,
        data_type: str = "klines",
        data_frequency: str = "1m",
        asset: str = "um",
        timeperiod_per_file: str = "daily",
        symbols: Optional[Union[str, List[str]]] = None,
        proxy: Optional[dict] = None,
        progress: Optional[Progress] = None,
        start_date: Optional[str] = None,
    ) -> None:
        """
        Initialize BinanceBulkFetcher

        :param data_type: Type of data to download (klines, aggTrades, etc.)
        :param data_frequency: Frequency of data (1m, 1h, 1d, etc.)
        :param asset: Type of asset (um, cm, spot, option)
        :param timeperiod_per_file: Time period per file (daily, monthly)
        :param symbols: Symbol or list of symbols (e.g., "BTCUSDT" or ["BTCUSDT", "ETHUSDT"])
        :param proxy: Optional proxy settings
        :param progress: Optional rich Progress instance for shared progress tracking
        :param start_date: Optional start date in YYYY-MM-DD format to filter files
        """
        self._data_type = data_type
        self._data_frequency = data_frequency
        self._asset = asset
        self._timeperiod_per_file = timeperiod_per_file
        self._symbols = [symbols] if isinstance(symbols, str) else symbols
        self._proxy = proxy
        self._start_date = start_date
        self.marker = None
        self.is_truncated = True
        self._progress = progress
        self._console = progress.console if progress else Console()

    def _check_params(self) -> None:
        """Check params validity"""
        if self._asset not in self._ASSET + self._FUTURES_ASSET + self._OPTIONS_ASSET:
            raise BinanceBulkDownloaderParamsError(
                f"asset must be {self._ASSET + self._FUTURES_ASSET + self._OPTIONS_ASSET}."
            )

        if self._timeperiod_per_file not in ["daily", "monthly"]:
            raise BinanceBulkDownloaderParamsError(
                "timeperiod_per_file must be daily or monthly."
            )

        if self._data_frequency not in self._DATA_FREQUENCY:
            raise BinanceBulkDownloaderParamsError(
                f"data_frequency must be {self._DATA_FREQUENCY}."
            )

        if self._asset not in self._DATA_TYPE_BY_ASSET:
            raise BinanceBulkDownloaderParamsError(
                f"asset {self._asset} is not supported."
            )

        asset_data = self._DATA_TYPE_BY_ASSET.get(self._asset, {})
        if self._timeperiod_per_file not in asset_data:
            raise BinanceBulkDownloaderParamsError(
                f"timeperiod {self._timeperiod_per_file} is not supported for {self._asset}."
            )

        valid_data_types = asset_data.get(self._timeperiod_per_file, [])
        if self._data_type not in valid_data_types:
            raise BinanceBulkDownloaderParamsError(
                f"data_type must be one of {valid_data_types}."
            )

        if self._data_frequency == "1s" and self._asset != "spot":
            raise BinanceBulkDownloaderParamsError(
                f"data_frequency 1s is not supported for {self._asset}."
            )

    def _get_file_list_from_s3_bucket(self, prefix: str) -> List[str]:
        """Get file list from s3 bucket with retry logic"""
        files = []
        marker = None
        is_truncated = True
        
        self._console.print(f"[cyan]📥 Getting file list:{prefix}[/cyan]")

        while is_truncated:
            params = {"prefix": prefix, "max-keys": 1000}
            if marker:
                params["marker"] = marker

            max_retries = 3
            retry_delay = 2
            
            for attempt in range(max_retries):
                try:
                    kwargs = {'params': params, 'timeout': 30}
                    if self._proxy is not None:
                        kwargs['proxies'] = self._proxy
                    response = requests.get(
                        self._BINANCE_DATA_S3_BUCKET_URL, 
                        **kwargs
                    )
                    response.raise_for_status()
                    break
                except (requests.exceptions.ProxyError,
                        requests.exceptions.ConnectionError,
                        requests.exceptions.Timeout) as e:
                    if attempt < max_retries - 1:
                        self._console.print(f"[yellow]⚠️  Connection error (try{attempt + 1}/{max_retries}): {str(e)[:100]}[/yellow]")
                        import time
                        time.sleep(retry_delay)
                        continue
                    else:
                        self._console.print(f"[red]❌ Failed to get file list ({max_retries}attempts):{str(e)[:100]}[/red]")
                        return files
                except Exception as e:
                    self._console.print(f"[red]❌ An exception occurred while getting the file list:{str(e)[:100]}[/red]")
                    return files
            
            tree = ElementTree.fromstring(response.content)

            for content in tree.findall("{http://s3.amazonaws.com/doc/2006-03-01/}Contents"):
                key = content.find("{http://s3.amazonaws.com/doc/2006-03-01/}Key").text
                if key.endswith(".zip"):
                    if isinstance(self._symbols, list) and len(self._symbols) > 1:
                        if any(symbol.upper() in key for symbol in self._symbols):
                            files.append(key)
                            marker = key
                    else:
                        files.append(key)
                        marker = key

            is_truncated_element = tree.find("{http://s3.amazonaws.com/doc/2006-03-01/}IsTruncated")
            is_truncated = (
                is_truncated_element is not None
                and is_truncated_element.text.lower() == "true"
            )

        return files

    def _make_asset_type(self) -> str:
        """Convert asset to asset type"""
        if self._asset == "um":
            return "futures/um"
        elif self._asset == "cm":
            return "futures/cm"
        elif self._asset in self._OPTIONS_ASSET:
            return "option"
        elif self._asset in self._ASSET:
            return "spot"
        else:
            raise BinanceBulkDownloaderParamsError("asset must be futures, options or spot.")

    def _build_prefix(self) -> str:
        """Build prefix to download"""
        url_parts = ["data", self._make_asset_type(), self._timeperiod_per_file, self._data_type]

        if isinstance(self._symbols, list) and len(self._symbols) == 1:
            symbol = self._symbols[0].upper()
            url_parts.append(symbol)
            if self._data_type in ["trades", "aggTrades"]:
                url_parts.append(symbol)
        elif isinstance(self._symbols, str):
            symbol = self._symbols.upper()
            url_parts.append(symbol)
            if self._data_type in ["trades", "aggTrades"]:
                url_parts.append(symbol)

        if (
            self._data_type in self._DATA_FREQUENCY_REQUIRED_BY_DATA_TYPE
            and self._data_frequency
        ):
            if isinstance(self._symbols, (str, list)):
                url_parts.append(f"{self._data_frequency}/")

        return "/".join(url_parts)

    def _download_and_parse_zip(self, file_key: str) -> Optional[pd.DataFrame]:
        """Download zip file and parse to DataFrame with retry logic"""
        max_retries = 3
        retry_delay = 2  # seconds
        
        for attempt in range(max_retries):
            try:
                url = f"{self._BINANCE_DATA_DOWNLOAD_BASE_URL}/{file_key}"
                kwargs = {'timeout': 30}
                if self._proxy is not None:
                    kwargs['proxies'] = self._proxy
                response = requests.get(url, **kwargs)
                response.raise_for_status()

                with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                    csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
                    if not csv_files:
                        return None
                    
                    csv_file = csv_files[0]
                    with zf.open(csv_file) as f:
                        # Some files have headers, some don't. 
                        # We read the first few bytes to check if it starts with "open_time"
                        content = f.read(100).decode('utf-8')
                        f.seek(0)
                        has_header = content.startswith("open_time")
                        
                        df = pd.read_csv(f, header=0 if has_header else None)
                        
                        # Set column names based on data type
                        if self._data_type == "klines":
                            df.columns = [
                                "open_time", "open", "high", "low", "close", "volume",
                                "close_time", "quote_volume", "count", "taker_buy_volume",
                                "taker_buy_quote_volume", "ignore"
                            ][:len(df.columns)]
                        
                        return df
                        
            except (requests.exceptions.ProxyError, 
                    requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as e:
                if attempt < max_retries - 1:
                    import time
                    time.sleep(retry_delay)
                    continue
                else:
                    return None
            except Exception as e:
                return None
        
        return None

    def fetch_batches(self, batch_size: int = 100) -> Iterator[pd.DataFrame]:
        """
        Fetch data in batches and return DataFrames as generators.
        
        parameter:
            batch_size (int): Number of files (CSV) processed per batch.
            
        Yields:
            pd.DataFrame: A data frame of batch_size files merged.
        """
        self._check_params()
        
        # Get file list
        file_list = []
        if isinstance(self._symbols, list) and len(self._symbols) > 1:
            original_symbols = self._symbols
            for symbol in original_symbols:
                self._symbols = symbol
                symbol_files = self._get_file_list_from_s3_bucket(self._build_prefix())
                file_list.extend(symbol_files)
            self._symbols = original_symbols
        else:
            file_list = self._get_file_list_from_s3_bucket(self._build_prefix())

        # Filter by data frequency if needed
        if (
            self._data_type in self._DATA_FREQUENCY_REQUIRED_BY_DATA_TYPE
            and not isinstance(self._symbols, (str, list))
        ):
            file_list = [
                prefix for prefix in file_list
                if prefix.count(self._data_frequency) == 2
            ]

        if not file_list:
            self._console.print("[yellow]⚠️  No matching file found[/yellow]")
            return

        # Filter by start_date if provided
        if self._start_date and file_list:
            filtered_files = []
            for f in file_list:
                # Extract date from filename (e.g., BTCUSDT-1h-2023-10-01.zip)
                filename = f.split('/')[-1].replace('.zip', '')
                parts = filename.split('-')
                
                try:
                    if self._timeperiod_per_file == "daily":
                        # Expected format: SYMBOL-INTERVAL-YYYY-MM-DD
                        file_date = "-".join(parts[-3:])
                        compare_date = self._start_date
                    else:
                        # Expected format: SYMBOL-INTERVAL-YYYY-MM
                        file_date = "-".join(parts[-2:])
                        compare_date = self._start_date[:7]  # YYYY-MM
                    
                    if file_date >= compare_date:
                        filtered_files.append(f)
                except Exception:
                    # If filename format is unexpected, keep it to be safe
                    filtered_files.append(f)
            
            if len(filtered_files) < len(file_list):
                self._console.print(f"[blue]⏳ based on start date{self._start_date} filter, remainder{len(filtered_files)}/{len(file_list)} files[/blue]")
            file_list = filtered_files

        if not file_list:
            self._console.print(f"[yellow]⚠️  No matching files found after filtering (Start Date:{self._start_date})[/yellow]")
            return

        self._console.print(f"[green]✓ turn up{len(file_list)} files[/green]")
        
        # Process in batches
        total_files = len(file_list)
        total_batches = (total_files + batch_size - 1) // batch_size
        
        # Use shared progress if available, otherwise create a temporary one
        if self._progress:
            batch_task = self._progress.add_task(f"[cyan]📦 process batch", total=total_batches)
            
            for batch_idx in range(0, total_files, batch_size):
                batch = file_list[batch_idx:batch_idx + batch_size]
                batch_dfs = []
                batch_num = batch_idx // batch_size + 1
                
                file_task = self._progress.add_task(
                    f"[blue]  batch{batch_num}/{total_batches}: Download file", 
                    total=len(batch)
                )
                
                for file_key in batch:
                    try:
                        df = self._download_and_parse_zip(file_key)
                        if df is not None and not df.empty:
                            batch_dfs.append(df)
                        self._progress.update(file_task, advance=1)
                    except Exception as e:
                        self._progress.update(file_task, advance=1)
                        continue
                
                self._progress.remove_task(file_task)
                
                if batch_dfs:
                    combined_df = pd.concat(batch_dfs, ignore_index=True)
                    combined_df = combined_df.sort_values("open_time").reset_index(drop=True)
                    self._progress.update(batch_task, advance=1)
                    self._console.print(f"[green]✓ batch{batch_num}: deal with{len(batch_dfs)} files, get{len(combined_df)} row data[/green]")
                    yield combined_df
                else:
                    self._progress.update(batch_task, advance=1)
            
            self._progress.remove_task(batch_task)
        else:
            # Fallback to local progress if no shared progress provided
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                TextColumn("•"),
                TimeRemainingColumn(),
                console=self._console
            ) as progress:
                batch_task = progress.add_task(f"[cyan]📦 process batch", total=total_batches)
                
                for batch_idx in range(0, total_files, batch_size):
                    batch = file_list[batch_idx:batch_idx + batch_size]
                    batch_dfs = []
                    batch_num = batch_idx // batch_size + 1
                    
                    file_task = progress.add_task(
                        f"[blue]  batch{batch_num}/{total_batches}: Download file", 
                        total=len(batch)
                    )
                    
                    for file_key in batch:
                        try:
                            df = self._download_and_parse_zip(file_key)
                            if df is not None and not df.empty:
                                batch_dfs.append(df)
                            progress.update(file_task, advance=1)
                        except Exception as e:
                            progress.update(file_task, advance=1)
                            continue
                    
                    progress.remove_task(file_task)
                    
                    if batch_dfs:
                        combined_df = pd.concat(batch_dfs, ignore_index=True)
                        combined_df = combined_df.sort_values("open_time").reset_index(drop=True)
                        progress.update(batch_task, advance=1)
                        self._console.print(f"[green]✓ batch{batch_num}: deal with{len(batch_dfs)} files, get{len(combined_df)} row data[/green]")
                        yield combined_df
                    else:
                        progress.update(batch_task, advance=1)
