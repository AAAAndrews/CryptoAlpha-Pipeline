"""
Extract all U-margin contract trading pairs from Binance data source
"""
import requests
import xml.etree.ElementTree as ET
from typing import List, Set, Tuple
import os


def fetch_all_trading_pairs(url: str, proxies:dict = None) -> List[str]:
    """
    Pull all trading pairs from Binance S3 bucket
    
    Args:
        url: BinanceData source URL
        
    Returns:
        Trading pair list
    """
    try:
        # Send request
        response = requests.get(url, timeout=30,proxies=proxies)
        response.raise_for_status()
        
        # Parse XML
        root = ET.fromstring(response.content)
        
        # XMLnamespace
        namespace = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
        
        # Extract all trading pairs
        trading_pairs = set()
        
        # Find all CommonPrefixes elements
        for prefix in root.findall('.//s3:CommonPrefixes/s3:Prefix', namespace):
            prefix_text = prefix.text
            if prefix_text:
                # Extract trading pair name from path
                # For example: data/futures/um/daily/klines/BTCUSDT/ -> BTCUSDT
                parts = prefix_text.strip('/').split('/')
                if len(parts) > 0:
                    trading_pair = parts[-1]
                    trading_pairs.add(trading_pair)
        
        # Convert to sorted list
        return sorted(list(trading_pairs))
        
    except requests.RequestException as e:
        print(f"Request error:{e}")
        return []
    except ET.ParseError as e:
        print(f"XMLParse error:{e}")
        return []




def get_trading_pairs(
    exchange: str = "binance",
    quote_currency: str = "USDT",
    market_type: str = "swap",
    proxy: dict = None
) -> List[str]:
    """
    Get list of trading pairs
    
    Args:
        exchange: Exchange name (currently only binance is supported)
        quote_currency: Quotation currency (such as USDT)
        market_type: Market type (spot or swap)
        proxy: Agent configuration
        
    Returns:
        Trading pair list
    """
    if exchange.lower() != "binance":
        raise ValueError(f"Exchanges are not currently supported:{exchange}")
    
    if market_type == "swap":
        url = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix=data/futures/um/daily/klines/"
    elif market_type == "spot":
        url = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix=data/spot/daily/klines/"
    else:
        raise ValueError(f"Unsupported market types:{market_type}")
    
    all_pairs = fetch_all_trading_pairs(url, proxies=proxy)
    
    # Filter by denomination currency
    if quote_currency:
        filtered_pairs = [pair for pair in all_pairs if pair.endswith(quote_currency)]
        return filtered_pairs
    
    return all_pairs


def run(proxies:dict = None) -> List[str]:
    """main function"""
    return get_trading_pairs(market_type="swap", proxy=proxies)


def get_active_trading_pairs_from_api(proxy: dict = None) -> Set[str]:
    """
    从 Binance REST API 获取当前活跃交易对列表 / Get active trading pairs from Binance REST API.

    使用 /fapi/v1/exchangeInfo 端点获取所有 U 本位合约的交易对信息。
    Uses /fapi/v1/exchangeInfo endpoint to get all USDT-margined futures trading pairs.

    参数 / Parameters:
        proxy: 代理配置 / Proxy configuration dict.

    返回 / Returns:
        Set[str]: 活跃交易对集合 / Set of active trading pair symbols.

    说明 / Notes:
        仅返回状态为 "TRADING" 的交易对，过滤掉已下架、暂停等状态的交易对。
        Only returns pairs with status "TRADING", filters out delisted/suspended pairs.
    """
    try:
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        response = requests.get(url, timeout=30, proxies=proxy)
        response.raise_for_status()

        data = response.json()

        # 提取所有状态为 TRADING 的 U 本位合约交易对 / Extract all USDT-Margined pairs with TRADING status
        active_pairs = set()
        for symbol_info in data.get('symbols', []):
            if (symbol_info.get('contractType') == 'PERPETUAL' and
                symbol_info.get('status') == 'TRADING' and
                symbol_info.get('quoteAsset') == 'USDT'):
                active_pairs.add(symbol_info['symbol'])

        return active_pairs

    except requests.RequestException as e:
        print(f"获取活跃交易对失败 / Failed to fetch active trading pairs: {e}")
        return set()


def get_local_trading_pairs(db_root_path: str, exchange: str = "binance") -> Set[str]:
    """
    从本地数据库目录获取已存储的交易对列表 / Get trading pairs stored in local database.

    参数 / Parameters:
        db_root_path: 数据库根目录路径 / Database root directory path.
        exchange: 交易所名称 / Exchange name.

    返回 / Returns:
        Set[str]: 本地交易对集合 / Set of local trading pair symbols.

    说明 / Notes:
        通过扫描 db_root_path/{exchange}/ 目录下的子目录来获取交易对列表。
        Scans subdirectories under db_root_path/{exchange}/ to find trading pairs.
    """
    exchange_path = os.path.join(db_root_path, exchange)

    if not os.path.exists(exchange_path):
        return set()

    local_pairs = set()
    for item in os.listdir(exchange_path):
        item_path = os.path.join(exchange_path, item)
        if os.path.isdir(item_path):
            local_pairs.add(item)

    return local_pairs


def validate_active_trading_pairs(
    db_root_path: str,
    exchange: str = "binance",
    proxy: dict = None
) -> Tuple[Set[str], Set[str], Set[str]]:
    """
    活跃交易对校验工具：获取活跃交易对并与本地数据库交易对列表对比 / Active trading pair validator.

    参数 / Parameters:
        db_root_path: 数据库根目录路径 / Database root directory path.
        exchange: 交易所名称 / Exchange name.
        proxy: 代理配置 / Proxy configuration dict.

    返回 / Returns:
        Tuple[Set[str], Set[str], Set[str]]:
            - active_pairs: API 返回的活跃交易对集合 / Active trading pairs from API.
            - local_pairs: 本地数据库中的交易对集合 / Local trading pairs in database.
            - valid_pairs: 既活跃又存在于本地的交易对集合 / Pairs that are both active and local.

    说明 / Notes:
        此函数用于数据管道入口层，在拉取数据前过滤掉已下架的交易对。
        Used at data pipeline entry layer to filter delisted trading pairs before fetching.
    """
    # 获取 API 活跃交易对 / Get active trading pairs from API
    active_pairs = get_active_trading_pairs_from_api(proxy=proxy)

    # 获取本地交易对 / Get local trading pairs from database
    local_pairs = get_local_trading_pairs(db_root_path, exchange)

    # 计算有效交易对 / Calculate valid trading pairs
    valid_pairs = active_pairs & local_pairs

    return active_pairs, local_pairs, valid_pairs


if __name__ == "__main__":
    pairs = run()
    print(f"turn up{len(pairs)} trading pairs")
    print(pairs[:20])
