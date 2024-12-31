from threading import Thread
import os
from dotenv import load_dotenv
from binance_orderbook import OrderBookCollectorBinance, OrderBookManagerBinance
from coinex_orderbook_btc_eth import OrderBookCollectorCoinex, OrderBookManagerCoinex
from okx_order_book import OrderBookCollectorOKX, OrderBookManagerOKX

# Load environment variables
load_dotenv()

# Fetch variables from the environment
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Ensure the variables are set
if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    raise ValueError("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is not set in the environment variables.")

# Define Binance Manager
def run_binance():
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
    binance_manager.start()

# Define CoinEx Manager
def run_coinex():
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
    coinex_manager.start()

# Define OKX Manager
def run_okx():
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
    okx_manager.start()

# Main function to run all managers concurrently
def main():
    threads = [
        Thread(target=run_binance, name="BinanceThread"),
        Thread(target=run_coinex, name="CoinExThread"),
        Thread(target=run_okx, name="OKXThread"),
    ]

    # Start all threads
    for thread in threads:
        thread.start()

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

if __name__ == '__main__':
    main()
