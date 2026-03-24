"""
Extract all U-margin contract trading pairs from Binance data source
"""
import requests
import xml.etree.ElementTree as ET
from typing import List, Set


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


if __name__ == "__main__":
    pairs = run()
    print(f"turn up{len(pairs)} trading pairs")
    print(pairs[:20])
