# Combined Script for Wallex, Nobitex, and Bitpin
from threading import Thread
from wallex_order_book import OrderBookCollectorWallex
from nobitex_order_book import OrderBookCollectorNobitex
from bitpin_orderbook import OrderBookCollectorBitpin

TELEGRAM_BOT_TOKEN = "7732239390:AAGuFI4pDUANbNxAbY9eT2FqzIawMZCoMA4"
TELEGRAM_CHAT_ID = "5904776497"

# Initialize Wallex collectors
wallex_collector = OrderBookCollectorWallex(
    telegram_bot_token=TELEGRAM_BOT_TOKEN,
    telegram_chat_id=TELEGRAM_CHAT_ID
)

# Initialize Nobitex collectors
nobitex_collector = OrderBookCollectorNobitex(
    telegram_bot_token=TELEGRAM_BOT_TOKEN,
    telegram_chat_id=TELEGRAM_CHAT_ID
)

# Initialize Bitpin collectors
bitpin_btc_collector = OrderBookCollectorBitpin(
    url="https://api.bitpin.org/api/v1/mth/orderbook/BTC_USDT/",
    token="BTC_USDT",
    telegram_bot_token=TELEGRAM_BOT_TOKEN,
    telegram_chat_id=TELEGRAM_CHAT_ID
)

bitpin_eth_collector = OrderBookCollectorBitpin(
    url="https://api.bitpin.org/api/v1/mth/orderbook/ETH_USDT/",
    token="ETH_USDT",
    telegram_bot_token=TELEGRAM_BOT_TOKEN,
    telegram_chat_id=TELEGRAM_CHAT_ID
)

# Combine all collectors
all_collectors = [
    wallex_collector,
    nobitex_collector,
    bitpin_btc_collector,
    bitpin_eth_collector
]

# Start all collectors in threads
def start_collectors(collectors):
    threads = []
    for collector in collectors:
        thread = Thread(target=collector.start)
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

if __name__ == "__main__":
    start_collectors(all_collectors)
