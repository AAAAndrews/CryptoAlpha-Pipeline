# Scripts Import specifications

## 馃搵 Import schema

The refactored script adopts a clear hierarchical import architecture:

```
scripts/
  鈫?  鈹溾攢鈫?CryptoDataProviders (data source interface)
  鈹斺攢鈫?CryptoDB_feather (Storage layer and configuration)
```

## 馃幆 Import specification

### Standard import format

All scripts follow the following unified import format:

```python
import sys
import os
from datetime import datetime, timezone
from copy import deepcopy

# 1. Add project root directory to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 2. Add submodule to path (supports internal relative imports)
for submodule in ["CryptoDataProviders", "CryptoDB_feather"]:
    submodule_path = os.path.join(project_root, submodule)
    if submodule_path not in sys.path:
        sys.path.insert(0, submodule_path)

# 3. Import data source interface from CryptoDataProviders
from CryptoDataProviders.utils.trading_pairs import get_trading_pairs

# 4. Import storage layer and configuration from CryptoDB_feather
from CryptoDB_feather.core.db_manager import run_binance_rest_updater
from CryptoDB_feather.config import PROXY, DEFAULT_BINANCE_PARAMS
```

## 馃摝 Module import instructions

### CryptoDataProviders锛坉ata source)

```python
# 鉁?Recommended: Use the full module path
from CryptoDataProviders.utils.trading_pairs import get_trading_pairs

# 鉂?Avoid: Use relative paths
from utils.trading_pairs import get_trading_pairs
```

**Available modules**:
- `CryptoDataProviders.utils.trading_pairs` - Get trading pair
- `CryptoDataProviders.utils.common` - general tools
- `CryptoDataProviders.providers.binance_api` - Binance API
- `CryptoDataProviders.providers.binance_bulk` - Batch download

### CryptoDB_feather锛坰torage layer)

```python
# 鉁?Recommended: Use the full module path
from CryptoDB_feather.core.db_manager import run_binance_rest_updater
from CryptoDB_feather.core.bulk_manager import run_bulk_updater
from CryptoDB_feather.config import PROXY, DB_ROOT_PATH

# 鉂?Avoid: Use relative paths
from core.db_manager import run_binance_rest_updater
from config import PROXY
```

**Available modules**:
- `CryptoDB_feather.core.db_manager` - REST API Update management
- `CryptoDB_feather.core.bulk_manager` - Batch download management
- `CryptoDB_feather.core.reader` - Data reading
- `CryptoDB_feather.core.storage` - Feather storage
- `CryptoDB_feather.config` - Global configuration

## 馃攳 Why do I need to add submodule paths?

because`CryptoDataProviders` and`CryptoDB_feather` Relative imports are used internally:

```python
# CryptoDataProviders/providers/binance_api/market_api.py
from utils.common import parse_time  # relative import

# CryptoDB_feather/core/db_manager.py
from providers.binance_api import fetch_klines  # relative import
```

Therefore these two submodules need to be added to`sys.path`锛孍nables Python to correctly resolve these internal relative imports.

## 鉁?Advantages of refactoring

### 1. Clear module boundaries
```python
# Clear distinction between data sources and storage tiers
from CryptoDataProviders.utils.trading_pairs import get_trading_pairs  # data source
from CryptoDB_feather.core.db_manager import run_binance_rest_updater # storage
```

### 2. Easy to maintain
- The import path is clear at a glance
- Know exactly which module the functionality comes from
- Avoid naming conflicts

### 3. Easy to test
```python
# Each module can be tested individually
from CryptoDataProviders.utils.trading_pairs import get_trading_pairs
from CryptoDB_feather.config import PROXY
```

### 4. IDE friendly
- Autocomplete is more accurate
- It is more convenient to jump to the definition
- Better refactoring tool support

## 馃摎 Practical application examples

### update_api.py
```python
# Clear layered import
from CryptoDataProviders.utils.trading_pairs import get_trading_pairs
from CryptoDB_feather.core.db_manager import run_binance_rest_updater
from CryptoDB_feather.config import PROXY, DEFAULT_BINANCE_PARAMS

# use
pairs = get_trading_pairs(exchange="binance", market_type="swap", proxy=PROXY)
run_binance_rest_updater(symbol_list=pairs, ...)
```

### cleanup_fake_data.py
```python
# Data source interface is used to obtain active trading pairs
from CryptoDataProviders.utils.trading_pairs import get_trading_pairs

# Storage layer configuration for accessing the database
from CryptoDB_feather.config import DB_ROOT_PATH, PROXY

# use
active_symbols = get_trading_pairs(exchange="binance", proxy=PROXY)
# Access database files: os.path.join(DB_ROOT_PATH, ...)
```

## 馃敡 Troubleshooting

### ModuleNotFoundError

**question**:`ModuleNotFoundError: No module named 'CryptoDataProviders'`

**solve**:
1. Make sure to run the script in the project root directory
2. examine`sys.path` Whether added correctly
3. Confirm that the submodule folder exists

```python
# debug code
import sys
print("sys.path:", sys.path)
print("CryptoDataProviders exists:", 
      os.path.exists("CryptoDataProviders"))
```

### Relative import failed

**Problem**: Internal module cannot be found`utils` or`providers`

**Fix**: Make sure the submodule path is added

```python
# This code must be included
for submodule in ["CryptoDataProviders", "CryptoDB_feather"]:
    submodule_path = os.path.join(project_root, submodule)
    if submodule_path not in sys.path:
        sys.path.insert(0, submodule_path)
```

## 馃摑 best practices

1. **Always use full module path**
   ```python
   # 鉁?Good
   from CryptoDataProviders.utils.trading_pairs import get_trading_pairs
   
   # 鉂?Bad
   from utils.trading_pairs import get_trading_pairs
   ```

2. **Keep import order consistent**
   ```python
   # 1. standard library
   import sys, os
   from datetime import datetime
   
   # 2. Path settings
   project_root = ...
   
   # 3. Third-party library import
   from CryptoDataProviders.xxx import xxx
   from CryptoDB_feather.xxx import xxx
   ```

3. **Use explicit aliases (if needed)**
   ```python
   from CryptoDataProviders.utils.trading_pairs import get_trading_pairs as get_pairs
   from CryptoDB_feather.core.db_manager import run_binance_rest_updater as run_api_update
   ```

## 馃幆 Summarize

The refactored import architecture has the following features:

- 鉁?**Clarity**: You can see where the functionality comes from at a glance
- 鉁?**Specification**: unified import format
- 鉁?**Maintainable**: easy to understand and modify
- 鉁?**Testable**: supports unit testing
- 鉁?**IDEFriendly**: autocomplete and jump

---

**Version**: 1.0.0
**Update time**: 2026-01-14
**Applies to **: all scripts under scripts/
