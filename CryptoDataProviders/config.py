"""
CryptoDataProviders Configuration file
"""

# Proxy configuration (if you need to bypass the firewall to access the exchange API)
PROXY = {
    "http": "http://127.0.0.1:7890",
    "https": "http://127.0.0.1:7890"
}

# Default request timeout (seconds)
REQUEST_TIMEOUT = 30

# Default batch size
DEFAULT_BATCH_SIZE = 1000

# Supported exchange list
SUPPORTED_EXCHANGES = [
    "binance", "binanceusdm", "binancecoinm",
    "okx", "bybit", "huobi", "gateio"
]

# Supported K-line types
SUPPORTED_KLINE_TYPES = {
    "binance": ["spot", "swap", "mark", "index"],
    "default": ["spot", "swap"]
}
