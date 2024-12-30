from threading import Thread
from datetime import datetime
import pytz
import requests
import pandas as pd
from telegram import Bot
import time
import io


class OrderBookCollectorNobitex:
    def __init__(self, telegram_bot_token, telegram_chat_id, interval_seconds=15):
        self.URL_ORDERBOOK_BTCUSDT_NOBITEX = 'https://api.nobitex.ir/v3/orderbook/BTCUSDT'
        self.URL_ORDERBOOK_ETHUSDT_NOBITEX = 'https://api.nobitex.ir/v3/orderbook/ETHUSDT'
        self.URL_ORDERBOOK_NOBITEX_ALL = "https://api.nobitex.ir/v3/orderbook/all"

        self.LIST_COLUMN_NAME_INTERCEPT = ['Item', 'Date', 'DateTime', 'Timestamp', 'Reference_Price']

        self.telegram_bot = Bot(token=telegram_bot_token)
        self.telegram_chat_id = telegram_chat_id

        self.interval_seconds = interval_seconds
        self.current_date = datetime.now(pytz.utc).date()
        self.data_list_spread = []
        self.data_list_depth = []


    def fetch_market_depth_url(self, url):
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            print("Failed to fetch data:", response.status_code)
            return {}

    def extract_ask_bid(self, data):
        rows = []
        last_update = []

        for key, value in data.items():
            last_update.append(value['lastUpdate'])
            for bid, ask in zip(value['bids'], value['asks']):
                row = {
                    'Item': key,
                    'Timestamp': value['lastUpdate'],
                    'Reference_Price': value['lastTradePrice'],
                    'Bid_Price': float(bid[0]),
                    'Bid_Volume': float(bid[1]),
                    'Ask_Price': float(ask[0]),
                    'Ask_Volume': float(ask[1])
                }
                rows.append(row)

        return pd.DataFrame(rows), last_update


    def dataset_preparation(self, result_df):
        result_df.fillna({'Bid_Price': 0, 'Bid_Volume': 0, 'Ask_Price': 0, 'Ask_Volume': 0, 'LastTradePrice': 0},
                         inplace=True)
        result_df['DateTime'] = pd.to_datetime(result_df['Timestamp'], unit='ms')
        result_df['FormattedDateTime'] = result_df['DateTime'].dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
        result_df['Date'] = result_df['DateTime'].dt.date
        result_df['DateTime'] = result_df['FormattedDateTime']

        return result_df

    def spread_calculation(self, result_df):
        spread_data = result_df.groupby(self.LIST_COLUMN_NAME_INTERCEPT).agg(
            {'Ask_Price': 'max', 'Bid_Price': 'min'}).rename(
                columns={'Ask_Price': 'Best_Ask_Price', 'Bid_Price': 'Best_Bid_Price'}
            ).reset_index()

        spread_data['Spread'] = (spread_data['Best_Ask_Price'] - spread_data['Best_Bid_Price'])

        return spread_data

    def calculate_depth_with_percentages(self, df, percentages=[0, 2, 5, 10]):
        combined_depth_df = pd.DataFrame(columns=self.LIST_COLUMN_NAME_INTERCEPT)
        ALTERNATE_COLUMN_NAME_INTERCEPT = self.LIST_COLUMN_NAME_INTERCEPT.copy()

        def calculate_depth(percentage):
            depth_df = df.groupby(ALTERNATE_COLUMN_NAME_INTERCEPT).agg(
                Total_Bid_Volume=('Bid_Volume', 'sum'),
                Total_Ask_Volume=('Ask_Volume', 'sum'),
            ).reset_index()

            depth_df['Percentage'] = percentage
            return depth_df

        depth_dfs = [calculate_depth(percentage) for percentage in percentages]

        for df in depth_dfs:
            combined_depth_df = pd.concat([combined_depth_df, df], ignore_index=True)

        return combined_depth_df

    def process_data(self, url):
        data = self.fetch_market_depth_url(url)
        data.pop("status", None)
        result_df, last_update = self.extract_ask_bid(data)
        item_date, last_item_str = pd.to_datetime(last_update[-1], unit='ms').date(), last_update[-1]
        result_df = self.dataset_preparation(result_df)
        spread_df = self.spread_calculation(result_df)
        depth_df_with_percentages = self.calculate_depth_with_percentages(result_df)
        return result_df, spread_df, depth_df_with_percentages, item_date, last_item_str

    def collect_data(self, url):

        result_df, spread_df, depth_df, item_date, last_item_str = self.process_data(url)

        return spread_df, depth_df

    def send_to_telegram(self):
        try:
            csv_buffer = io.BytesIO()
            spread_df = pd.concat(self.data_list_spread, ignore_index=True)
            spread_df.to_csv(csv_buffer, index=False, encoding='utf-8')
            csv_buffer.seek(0)

            file_name_spread = f"nobitex_df_spread_{datetime.now(pytz.utc).strftime('%Y-%m-%d')}.csv"
            self.telegram_bot.send_document(
                chat_id=self.telegram_chat_id,
                document=csv_buffer,
                filename=file_name_spread
            )

            csv_buffer = io.BytesIO()
            csv_buffer.seek(0)

            depth_df = pd.concat(self.data_list_depth, ignore_index=True)

            depth_df.to_csv(csv_buffer, index=False, encoding='utf-8')
            csv_buffer.seek(0)

            file_name_depth = f"nobitex_depth_all_{datetime.now(pytz.utc).strftime('%Y-%m-%d')}.csv"
            self.telegram_bot.send_document(
                chat_id=self.telegram_chat_id,
                document=csv_buffer,
                filename=file_name_depth
            )

        except Exception as e:
            print(f"Failed to send data to Telegram: {e}")

    def start(self):
        self.data_list_spread = []
        self.data_list_depth = []
        while True:
            try:
                now = datetime.now(pytz.utc)

                if now.second % 15 == 0:
                    if now.date() != self.current_date:
                        print(f"New day detected: {now.date()}. Resetting data.")
                        self.current_date = now.date()

                    df_slippage_spread_all, df_depth_all = self.collect_data(self.URL_ORDERBOOK_NOBITEX_ALL)

                    self.data_list_spread.append(df_slippage_spread_all)
                    self.data_list_depth.append(df_depth_all)

                    if now.minute == 59 and now.second >= (60 - self.interval_seconds):
                      self.send_to_telegram()

                time.sleep(5)
            except Exception as e:
                print(f"An error occurred: {e}")
                time.sleep(5)


class OrderBookManagerNobitex:
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
#     btc_usdt_collector = OrderBookCollectorNobitex(
#         telegram_bot_token=TELEGRAM_BOT_TOKEN,
#         telegram_chat_id=TELEGRAM_CHAT_ID
#     )
#
#     manager = OrderBookManagerNobitex([btc_usdt_collector])
#     manager.start()
