from threading import Thread
from datetime import datetime
import pytz
import time
from binance_orderbook import OrderBookCollectorBinance, OrderBookManagerBinance
from coinex_orderbook_btc_eth import OrderBookCollectorCoinex, OrderBookManagerCoinex
from okx_order_book import OrderBookCollectorOKX, OrderBookManagerOKX

TELEGRAM_BOT_TOKEN = "7732239390:AAGuFI4pDUANbNxAbY9eT2FqzIawMZCoMA4"
TELEGRAM_CHAT_ID = "5904776497"

# Binance Collectors
binance_btc_collector = OrderBookCollectorBinance(
    token="BTCUSDT",
    telegram_bot_token=TELEGRAM_BOT_TOKEN,
    telegram_chat_id=TELEGRAM_CHAT_ID
)
binance_eth_collector = OrderBookCollectorBinance(
    token="ETHUSDT",
    telegram_bot_token=TELEGRAM_BOT_TOKEN,
    telegram_chat_id=TELEGRAM_CHAT_ID
)
binance_manager = OrderBookManagerBinance([binance_btc_collector, binance_eth_collector])

# CoinEx Collectors
coinex_btc_collector = OrderBookCollectorCoinex(
    token="BTCUSDT",
    telegram_bot_token=TELEGRAM_BOT_TOKEN,
    telegram_chat_id=TELEGRAM_CHAT_ID
)
coinex_eth_collector = OrderBookCollectorCoinex(
    token="ETHUSDT",
    telegram_bot_token=TELEGRAM_BOT_TOKEN,
    telegram_chat_id=TELEGRAM_CHAT_ID
)
coinex_manager = OrderBookManagerCoinex([coinex_btc_collector, coinex_eth_collector])

# OKX Collectors
okx_btc_collector = OrderBookCollectorOKX(
    token="BTC-USDT",
    telegram_bot_token=TELEGRAM_BOT_TOKEN,
    telegram_chat_id=TELEGRAM_CHAT_ID
)
okx_eth_collector = OrderBookCollectorOKX(
    token="ETH-USDT",
    telegram_bot_token=TELEGRAM_BOT_TOKEN,
    telegram_chat_id=TELEGRAM_CHAT_ID
)
okx_manager = OrderBookManagerOKX([okx_btc_collector, okx_eth_collector])

# Start all managers in threads
managers = [binance_manager, coinex_manager, okx_manager]
threads = []

for manager in managers:
    thread = Thread(target=manager.start)
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()
