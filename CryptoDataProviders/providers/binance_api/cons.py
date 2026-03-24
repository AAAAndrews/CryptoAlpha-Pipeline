# cons.py
import socket

# Get the current hostname


Spot_REVERSE_PROXY_IP = "http://8.217.85.68:8080" #Spot goods

Derivatives_REVERSE_PROXY_IP = "http://8.217.85.68:8081" #derivatives
# def go_get_spot(url):
#     hostname = socket.gethostname()
#     print(f'The current host name is:{hostname}')

#     # Get IP address based on hostname
#     ip_address = socket.gethostbyname(hostname)
#     print(f'The IP of the current host is:{ip_address}')

#     if ip_address == "8.217.85.68":
#         Spot_REVERSE_PROXY_IP.replace(ip_address,"127.0.0.1")
    
#     return Spot_REVERSE_PROXY_IP + url


# def go_get_derivatives(url):
#     hostname = socket.gethostname()
#     print(f'The current host name is:{hostname}')

#     # Get IP address based on hostname
#     ip_address = socket.gethostbyname(hostname)
#     print(f'The IP of the current host is:{ip_address}')

#     if ip_address == "8.217.85.68":
#         Spot_REVERSE_PROXY_IP.replace(ip_address,"127.0.0.1")
#     return Derivatives_REVERSE_PROXY_IP + url


def go_get_spot(url):
    return "http://api.binance.com"+ url


def go_get_derivatives(url):
    return "http://fapi.binance.com"+ url




# api It is the derivatives transaction data interface. API is the spot transaction data interface.
# https://developers.binance.com/docs/zh-CN/derivatives/usds-margined-futures/market-data/rest-api/Kline-Candlestick-Data

test_url = go_get_spot("/api/v1/ping")
spot_klines_url = go_get_spot("/api/v1/klines")

spot_exchange_info_url = go_get_spot("/api/v1/exchangeInfo")
spot_depth_url = go_get_spot("/api/v1/depth")
spot_trades_url = go_get_spot("/api/v1/trades")
spot_agg_trades_url = go_get_spot("/api/v1/aggTrades")




ftest_url = go_get_derivatives("/fapi/v1/ping")
derivatives_swap_klines_url = go_get_derivatives("/fapi/v1/continuousKlines")
derivatives_price_index_url = go_get_derivatives("/fapi/v1/indexPriceKlines")
derivatives_mark_price_url = go_get_derivatives("/fapi/v1/markPriceKlines")


derivatives_exchange_info_url = go_get_derivatives("/fapi/v1/exchangeInfo")
derivatives_depth_url = go_get_derivatives("/fapi/v1/depth")
derivatives_trades_url = go_get_derivatives("/fapi/v1/trades")
derivatives_agg_trades_url = go_get_derivatives("/fapi/v1/aggTrades")
