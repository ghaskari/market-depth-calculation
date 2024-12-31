from threading import Thread
import os
from dotenv import load_dotenv
from wallex_order_book import OrderBookCollectorWallex, OrderBookManagerWallex
from nobitex_order_book import OrderBookCollectorNobitex, OrderBookManagerNobitex
from bitpin_orderbook import OrderBookCollectorBitpin, OrderBookManagerBitpin

load_dotenv()

# Fetch variables from the environment
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Ensure the variables are set
if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    raise ValueError("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is not set in the environment variables.")


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
    # Create threads for each function
    threads = [
        Thread(target=run_bitpin, name="BitpinThread"),
        Thread(target=run_nobitex, name="NobitexThread"),
        Thread(target=run_wallex, name="WallexThread")
    ]

    # Start all threads
    for thread in threads:
        thread.start()

    # Wait for all threads to finish
    for thread in threads:
        thread.join()


if __name__ == '__main__':
    main()
