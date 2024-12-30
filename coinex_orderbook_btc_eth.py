from threading import Thread
from datetime import datetime
import pytz
import requests
import pandas as pd
from telegram import Bot
import time
import io
import concurrent.futures


class OrderBookCollectorCoinex:
    def __init__(self,token ,telegram_bot_token, telegram_chat_id, interval_seconds=15):
        self.name_exchange = "CoinEx"
        self.symbols = token
        self.telegram_bot = Bot(token=telegram_bot_token)
        self.telegram_chat_id = telegram_chat_id
        self.interval_seconds = interval_seconds
        self.data_list = []
        self.current_date = datetime.now(pytz.utc).date()

        self.proxies = {
            'http': 'socks5://127.0.0.1:2080',
            'https': 'socks5://127.0.0.1:2080',
        }

    def fetch_market_depth(self, symbol):
        url = f"https://api.coinex.com/v1/market/depth?market={symbol.lower()}&merge=0"
        try:
            response = requests.get(url, proxies=self.proxies)
            response.raise_for_status()
            return symbol, response.json()
        except requests.RequestException as e:
            print(f"Failed to fetch data for {symbol}: {e}")
            return symbol, None

    def process_order_book_data(self, symbol):
        symbol, order_book_data = self.fetch_market_depth(symbol)
        if order_book_data and "data" in order_book_data and len(order_book_data["data"]) > 0:
            asks = pd.DataFrame(order_book_data['data']['asks'], columns=["Ask_Price", "Ask_Volume"])
            bids = pd.DataFrame(order_book_data['data']['bids'], columns=["Bid_Price", "Bid_Volume"])

            asks["Ask_Price"] = pd.to_numeric(asks["Ask_Price"])
            asks["Ask_Volume"] = pd.to_numeric(asks["Ask_Volume"])
            bids["Bid_Price"] = pd.to_numeric(bids["Bid_Price"])
            bids["Bid_Volume"] = pd.to_numeric(bids["Bid_Volume"])

            last_price = float(order_book_data['data']['last'])
            timestamp = int(order_book_data['data']['time'])
            datetime_str = datetime.utcfromtimestamp(timestamp / 1000).isoformat()
            date_str = datetime_str.split("T")[0]

            total_ask_volume = asks["Ask_Volume"].sum()
            total_bid_volume = bids["Bid_Volume"].sum()

            best_bid_price = bids["Bid_Price"].max()
            best_ask_price = asks["Ask_Price"].min()
            spread = best_ask_price - best_bid_price

            iteration_data = pd.DataFrame({
                "Item": symbol,
                "Timestamp": timestamp,
                "DateTime": datetime_str,
                "Date": date_str,
                "Ask_Price": asks["Ask_Price"],
                "Ask_Volume": asks["Ask_Volume"],
                "Bid_Price": bids["Bid_Price"],
                "Bid_Volume": bids["Bid_Volume"],
                "Total_Ask_Volume": total_ask_volume,
                "Total_Bid_Volume": total_bid_volume,
                "Best_Bid_Price": best_bid_price,
                "Best_Ask_Price": best_ask_price,
                "Spread": spread,
                "Reference_Price": last_price
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

                file_name = f"coinex_order_book_{self.symbols}_{datetime.now(pytz.utc).strftime('%Y-%m-%d')}.csv"
                self.telegram_bot.send_document(
                    chat_id=self.telegram_chat_id,
                    document=csv_buffer,
                    filename=file_name
                )
                print(f"Data sent to Telegram for {self.symbols}.")

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

                    iteration_data = self.process_order_book_data(self.symbols)
                    self.data_list.append(iteration_data)

                    if now.minute == 59 and now.second >= (60 - self.interval_seconds):
                        self.send_to_telegram()

                time.sleep(5)
            except Exception as e:
                print(f"An error occurred for {self.symbols}: {e}")
                time.sleep(5)

class OrderBookManagerCoinex:
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
#     btc_usdt_collector = OrderBookCollectorCoinex(
#         token="BTCUSDT",
#         telegram_bot_token=TELEGRAM_BOT_TOKEN,
#         telegram_chat_id=TELEGRAM_CHAT_ID
#     )
#
#     eth_usdt_collector = OrderBookCollectorCoinex(
#         token="ETHUSDT",
#         telegram_bot_token=TELEGRAM_BOT_TOKEN,
#         telegram_chat_id=TELEGRAM_CHAT_ID
#     )
#
#     manager = OrderBookManagerCoinex([btc_usdt_collector, eth_usdt_collector])
#     manager.start()
