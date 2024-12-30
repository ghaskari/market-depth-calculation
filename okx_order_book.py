from threading import Thread
from datetime import datetime
import pytz
import requests
import pandas as pd
from telegram import Bot
import time
import io
import concurrent.futures

class OrderBookCollectorOKX:
    def __init__(self,token ,telegram_bot_token, telegram_chat_id, interval_seconds=15):

        self.name_exchange = "OKX"
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

    def fetch_order_book(self, symbol):
        url = f"https://www.okx.com/api/v5/market/books?instId={symbol}&sz=10"
        try:
            response = requests.get(url, proxies=self.proxies)
            response.raise_for_status()
            return symbol, response.json()
        except requests.RequestException as e:
            print(f"Failed to fetch data for {symbol}: {e}")
            return symbol, None

    def save_data(self, data, name_exchange, symbol):
        today_date = datetime.today().strftime('%Y-%m-%d')
        filename = f'order_book_data/{name_exchange.lower()}/order_book_{name_exchange.lower()}_{symbol}_{today_date}.csv'

        data.to_csv(filename, index=False)


    def process_order_book_data(self, symbol):
        symbol, order_book_data = self.fetch_order_book(symbol)
        if order_book_data and "data" in order_book_data and len(order_book_data["data"]) > 0:
            order_data = order_book_data["data"][0]

            max_len = max(len(order_data["asks"]), len(order_data["bids"]))
            asks = order_data["asks"] + [['', '']] * (max_len - len(order_data["asks"]))
            bids = order_data["bids"] + [['', '']] * (max_len - len(order_data["bids"]))

            best_buy = max(float(b[0]) for b in order_data["bids"] if b[0])
            best_sell = min(float(a[0]) for a in order_data["asks"] if a[0])

            spread = best_sell - best_buy

            timestamp = datetime.now().timestamp()
            datetime_str = datetime.now().isoformat()
            date_str = datetime_str.split("T")[0]

            total_ask_volume = sum(float(a[1]) for a in order_data["asks"] if a[1])
            total_bid_volume = sum(float(b[1]) for b in order_data["bids"] if b[1])

            iteration_data = pd.DataFrame({
                "Item": symbol,
                "Timestamp": timestamp,
                "DateTime": datetime_str,
                "Date": date_str,
                "Ask_Price": [float(a[0]) if a[0] else None for a in asks],
                "Ask_Volume": [float(a[1]) if a[1] else None for a in asks],
                "Bid_Price": [float(b[0]) if b[0] else None for b in bids],
                "Bid_Volume": [float(b[1]) if b[1] else None for b in bids],
                "Total_Ask_Volume": total_ask_volume,
                "Total_Bid_Volume": total_bid_volume,
                "Best_Bid_Price": best_buy,
                "Best_Ask_Price": best_sell,
                "Spread": spread
            })

            all_prices = [float(a[0]) for a in order_data["asks"] if a[0]] + \
                         [float(b[0]) for b in order_data["bids"] if b[0]]

            reference_price = pd.Series(all_prices).median()
            iteration_data["Reference_Price"] = reference_price

            iteration_data.drop_duplicates(subset=["Timestamp", "Item"], inplace=True)

            self.save_data(iteration_data, self.name_exchange, symbol)

            return iteration_data


    def main(self):
        while True:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = {executor.submit(self.fetch_order_book, symbol): symbol for symbol in self.symbols}

                for future in concurrent.futures.as_completed(futures):
                    symbol = futures[future]
                    try:
                        symbol, order_book_data = future.result()
                        self.process_order_book_data(symbol)
                    except Exception as e:
                        print(f"An error occurred for {symbol}: {e}")

            time.sleep(15)

    def send_to_telegram(self):
        try:
            if self.data_list:
                df = pd.concat(self.data_list, ignore_index=True)

                csv_buffer = io.BytesIO()
                df.to_csv(csv_buffer, index=False, encoding='utf-8')
                csv_buffer.seek(0)

                file_name = f"okx_order_book_{self.symbols}_{datetime.now(pytz.utc).strftime('%Y-%m-%d')}.csv"
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


class OrderBookManagerOKX:
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
#     btc_usdt_collector = OrderBookCollectorOKX(
#         token="BTC-USDT",
#         telegram_bot_token=TELEGRAM_BOT_TOKEN,
#         telegram_chat_id=TELEGRAM_CHAT_ID
#     )
#
#     eth_usdt_collector = OrderBookCollectorOKX(
#         token="ETH-USDT",
#         telegram_bot_token=TELEGRAM_BOT_TOKEN,
#         telegram_chat_id=TELEGRAM_CHAT_ID
#     )
#
#     manager = OrderBookManagerOKX([btc_usdt_collector, eth_usdt_collector])
#     manager.start()
