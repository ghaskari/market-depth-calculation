# Combined Script for Wallex, Nobitex, and Bitpin
from threading import Thread
from wallex_order_book import OrderBookCollectorWallex, OrderBookManagerWallex
from nobitex_order_book import OrderBookCollectorNobitex, OrderBookManagerNobitex
from bitpin_orderbook import OrderBookCollectorBitpin, OrderBookManagerBitpin

TELEGRAM_BOT_TOKEN = "7732239390:AAGuFI4pDUANbNxAbY9eT2FqzIawMZCoMA4"
TELEGRAM_CHAT_ID = "5904776497"

def run_bitpin():

    btc_usdt_collector = OrderBookCollectorBitpin(
        url="https://api.bitpin.org/api/v1/mth/orderbook/BTC_USDT/",
        token="BTC_USDT",
        telegram_bot_token=TELEGRAM_BOT_TOKEN,
        telegram_chat_id=TELEGRAM_CHAT_ID
    )

    eth_usdt_collector = OrderBookCollectorBitpin(
        url="https://api.bitpin.org/api/v1/mth/orderbook/ETH_USDT/",
        token="ETH_USDT",
        telegram_bot_token=TELEGRAM_BOT_TOKEN,
        telegram_chat_id=TELEGRAM_CHAT_ID
    )

    manager = OrderBookManagerBitpin([btc_usdt_collector, eth_usdt_collector])
    manager.start()

def run_nobitex():
    btc_usdt_collector = OrderBookCollectorNobitex(
        telegram_bot_token=TELEGRAM_BOT_TOKEN,
        telegram_chat_id=TELEGRAM_CHAT_ID
    )

    manager = OrderBookManagerNobitex([btc_usdt_collector])
    manager.start()

def run_wallex():
    btc_usdt_collector = OrderBookCollectorWallex(
        telegram_bot_token=TELEGRAM_BOT_TOKEN,
        telegram_chat_id=TELEGRAM_CHAT_ID
    )

    manager = OrderBookManagerWallex([btc_usdt_collector])
    manager.start()

def main():
    run_nobitex()
    run_bitpin()
    run_wallex()

if __name__ == '__main__':
    main()
