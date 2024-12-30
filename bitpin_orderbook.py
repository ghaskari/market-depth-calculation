from threading import Thread
from datetime import datetime
import pytz
import requests
import pandas as pd
from telegram import Bot
import time
import io


class OrderBookCollectorBitpin:
    def __init__(self, url, token, telegram_bot_token, telegram_chat_id, interval_seconds=15):
        self.url = url
        self.token = token
        self.telegram_bot = Bot(token=telegram_bot_token)
        self.telegram_chat_id = telegram_chat_id
        self.interval_seconds = interval_seconds
        self.data_list = []
        self.current_date = datetime.now(pytz.utc).date()

    def fetch_orderbook(self):
        try:
            response = requests.get(self.url)
            response.raise_for_status()
            data = response.json()
            return data
        except requests.RequestException as e:
            print(f"Failed to fetch data from {self.url}: {e}")
            return None

    def process_orderbook(self, data):
        max_len = max(len(data.get("asks", [])), len(data.get("bids", [])))
        asks = data.get("asks", []) + [[None, None]] * (max_len - len(data.get("asks", [])))
        bids = data.get("bids", []) + [[None, None]] * (max_len - len(data.get("bids", [])))

        best_buy = max((float(b[0]) for b in data.get("bids", []) if b[0]), default=0)
        best_sell = min((float(a[0]) for a in data.get("asks", []) if a[0]), default=0)
        spread = best_sell - best_buy

        now = datetime.now(pytz.utc)
        timestamp = now.timestamp()
        datetime_str = now.isoformat()
        date_str = now.strftime('%Y-%m-%d')

        total_ask_volume = sum((float(a[1]) for a in data.get("asks", []) if a[1]), 0)
        total_bid_volume = sum((float(b[1]) for b in data.get("bids", []) if b[1]), 0)

        all_prices = [float(a[0]) for a in data.get("asks", []) if a[0]] + [float(b[0]) for b in data.get("bids", []) if b[0]]
        reference_price = pd.Series(all_prices).median() if all_prices else 0

        iteration_data = pd.DataFrame({
            "Item": [self.token] * max_len,
            "Timestamp": [timestamp] * max_len,
            "DateTime": [datetime_str] * max_len,
            "Date": [date_str] * max_len,
            "Ask_Price": [float(a[0]) if a[0] else None for a in asks],
            "Ask_Volume": [float(a[1]) if a[1] else None for a in asks],
            "Bid_Price": [float(b[0]) if b[0] else None for b in bids],
            "Bid_Volume": [float(b[1]) if b[1] else None for b in bids],
            "Total_Ask_Volume": [total_ask_volume] * max_len,
            "Total_Bid_Volume": [total_bid_volume] * max_len,
            "Best_Bid_Price": [best_buy] * max_len,
            "Best_Ask_Price": [best_sell] * max_len,
            "Spread": [spread] * max_len,
            "Reference_Price": [reference_price] * max_len
        })

        iteration_data.drop_duplicates(subset=["Timestamp", "Item"], inplace=True)

        return iteration_data

    def send_to_telegram(self):
        try:
            if self.data_list:
                df = pd.concat(self.data_list, ignore_index=True)

                csv_buffer = io.BytesIO()
                df.to_csv(csv_buffer, index=False, encoding='utf-8')
                csv_buffer.seek(0)

                file_name = f"bitpin_order_book_{self.token}_{datetime.now(pytz.utc).strftime('%Y-%m-%d')}.csv"
                self.telegram_bot.send_document(
                    chat_id=self.telegram_chat_id,
                    document=csv_buffer,
                    filename=file_name
                )
                print(f"Data sent to Telegram for {self.token}.")

        except Exception as e:
            print(f"Failed to send data to Telegram: {e}")

    def start(self):
        while True:
            try:
                now = datetime.now(pytz.utc)

                if now.second % 15 == 0:
                    if now.date() != self.current_date:
                        print(f"New day detected: {now.date()}. Resetting data.")
                        self.current_date = now.date()
                        self.data_list = []

                    data = self.fetch_orderbook()
                    if data:
                        iteration_data = self.process_orderbook(data)
                        self.data_list.append(iteration_data)

                    if now.minute == 59 and now.second >= (60 - self.interval_seconds):
                        self.send_to_telegram()

                time.sleep(5)
            except Exception as e:
                print(f"An error occurred for {self.token}: {e}")
                time.sleep(5)


class OrderBookManagerBitpin:
    def __init__(self, collectors):
        self.collectors = collectors

    def start(self):
        threads = []
        try:
            for collector in self.collectors:
                thread = Thread(target=collector.start)
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()
        except KeyboardInterrupt:
            print("Data collection interrupted by user.")


# if __name__ == "__main__":
#     TELEGRAM_BOT_TOKEN = "7732239390:AAGuFI4pDUANbNxAbY9eT2FqzIawMZCoMA4"
#     TELEGRAM_CHAT_ID = "5904776497"
#
#     btc_usdt_collector = OrderBookCollectorBitpin(
#         url="https://api.bitpin.org/api/v1/mth/orderbook/BTC_USDT/",
#         token="BTC_USDT",
#         telegram_bot_token=TELEGRAM_BOT_TOKEN,
#         telegram_chat_id=TELEGRAM_CHAT_ID
#     )
#
#     eth_usdt_collector = OrderBookCollectorBitpin(
#         url="https://api.bitpin.org/api/v1/mth/orderbook/ETH_USDT/",
#         token="ETH_USDT",
#         telegram_bot_token=TELEGRAM_BOT_TOKEN,
#         telegram_chat_id=TELEGRAM_CHAT_ID
#     )
#
#     manager = OrderBookManagerBitpin([btc_usdt_collector, eth_usdt_collector])
#     manager.start()
