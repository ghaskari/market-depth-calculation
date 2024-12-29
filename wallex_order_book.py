from threading import Thread
from datetime import datetime
import pytz
import requests
import pandas as pd
from telegram import Bot
import time
import io


class OrderBookCollectorWallex:
    def __init__(self, telegram_bot_token, telegram_chat_id, interval_seconds=15):
        self.URL_ORDERBOOK_BTCUSDT_WALLEX = 'https://api.wallex.ir/v1/depth?symbol=BTCUSDT'
        self.URL_ORDERBOOK_ETHUSDT_WALLEX = 'https://api.wallex.ir/v1/depth?symbol=ETHUSDT'
        self.URL_ORDERBOOK_wallex_ALL = "https://api.wallex.ir/v2/depth/all"

        self.LIST_COLUMN_NAME_INTERCEPT = ['Item', 'Date', 'DateTime', 'Timestamp']
        self.df_slippage_spread_all = pd.DataFrame(columns=['Item',
                                                       'Date', 'DateTime', 'Timestamp',
                                                       'Reference_Price',
                                                       'Ask_Price', 'Bid_Price', 'Spread'
                                                       ])

        self.df_depth_all = pd.DataFrame(columns=['Item',
                                             'Date', 'DateTime', 'Timestamp',
                                             'Reference_Price',
                                             'Total_Bid_Volume', 'Total_Ask_Volume',
                                             'Percentage'
                                             ])

        self.output_dir = 'order_book_data/wallex/'

        self.telegram_bot = Bot(token=telegram_bot_token)
        self.telegram_chat_id = telegram_chat_id

        self.interval_seconds = interval_seconds
        self.current_date = datetime.now(pytz.utc).date()
        self.data_list_spread = []
        self.data_list_depth = []

    def save_orderbook_files(self, df, filename):

        output_dir = 'order_book_data/wallex/'

        csv_filepath = (f'{output_dir}wallex_orderbook_{filename}.csv')
        df.to_csv(csv_filepath, index=False)

    def fetch_market_depth_url(self, url):
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            return None

    def extract_ask_bid(self, data):

        all_asks = []
        all_bids = []

        for name in data['result']:
            asks = data['result'][name]['ask']
            bids = data['result'][name]['bid']

            for ask in asks:
                ask['name'] = name
                ask['type'] = 'ask'
            for bid in bids:
                bid['name'] = name
                bid['type'] = 'bid'

            all_asks.extend(asks)
            all_bids.extend(bids)

        df_asks = pd.DataFrame(all_asks)
        df_bids = pd.DataFrame(all_bids)

        df_asks.columns = ['Ask_Price', 'Ask_Quantity', 'Ask_Sum', 'Item', 'Type']
        df_bids.columns = ['Bid_Price', 'Bid_Quantity', 'Bid_Sum', 'Item', 'Type']

        df_asks[['Ask_Sum', 'Ask_Quantity', 'Ask_Price']] = df_asks[['Ask_Sum', 'Ask_Quantity', 'Ask_Price']].astype(float)
        df_bids[['Bid_Price', 'Bid_Quantity', 'Bid_Sum']] = df_bids[['Bid_Price', 'Bid_Quantity', 'Bid_Sum']].astype(float)

        df_asks.drop('Type', inplace=True, axis=1)
        df_bids.drop('Type', inplace=True, axis=1)
        df = pd.merge(df_asks, df_bids, on='Item', how='outer')

        all_prices = pd.concat([df['Ask_Price'], df['Bid_Price']])
        overall_median_price = all_prices.median()
        df['Reference_Price'] = overall_median_price

        result_df = df.copy()
        result_df.fillna({'Bid_Price': 0,
                          'Bid_Quantity': 0,
                          'Ask_Price': 0,
                          'Ask_Quantity': 0,
                          'Reference_Price': 0,
                          },
                         inplace=True)

        result_df = result_df.replace('', 0)

        now = datetime.now(pytz.utc)
        timestamp = now.timestamp()
        datetime_str = now.isoformat()
        date_str = now.strftime('%Y-%m-%d')

        result_df["Timestamp"] = timestamp
        result_df["DateTime"] = datetime_str
        result_df["Date"] = date_str

        return result_df

    def dataset_preparation(self, result_df):
        result_df.fillna({'Bid_Price': 0,
                          'Bid_Quantity': 0,
                          'Ask_Price': 0,
                          'Ask_Quantity': 0,
                          'LastTradePrice': 0},
                         inplace=True)

        result_df = result_df.replace('', 0)
        return result_df

    def spread_calculation(self, result_df):
        spread_data = result_df.groupby(self.LIST_COLUMN_NAME_INTERCEPT).agg(
            {'Ask_Price': 'max', 'Bid_Price': 'min'}).rename(
            columns={'Ask_Price': 'Best_Ask_Price', 'Bid_Price': 'Best_Bid_Price'}
        ).reset_index()

        spread_data['Spread'] = (spread_data['Best_Ask_Price'] - spread_data['Best_Bid_Price'])
        spread_data['Reference_Price'] = (spread_data['Best_Ask_Price'] + spread_data['Best_Bid_Price']) / 2

        return spread_data

    def calculate_depth_with_percentages(self, df, percentages=[0, 2, 5, 10], group_columns=['DateTime', 'Item']):

        def calculate_depth(df, percentage=[0, 2, 5, 10], group_columns=['DateTime', 'Item']):
            df_depth_group = df.groupby(group_columns).agg(
                Best_Bid_Price=('Bid_Price', 'max'),
                Best_Ask_Price=('Ask_Price', 'min')
            ).reset_index()

            df_with_bounds = pd.merge(df, df_depth_group, on=group_columns, how='outer')
            df_with_bounds['Reference_Price'] = (df_with_bounds['Best_Bid_Price'] + df_with_bounds['Best_Ask_Price']) / 2

            key_columns = ['Item',
                           'Date', 'DateTime', 'Timestamp',
                           'Best_Bid_Price', 'Best_Ask_Price', 'Reference_Price'
                           ]

            depth_df = df_with_bounds.groupby(key_columns).agg(
                Total_Bid_Volume=('Bid_Quantity', 'sum'),
                Total_Ask_Volume=('Ask_Quantity', 'sum'),
            ).reset_index()

            depth_df['Percentage'] = percentage

            return depth_df

        def combine_depth_dfs(df, percentages):
            combined_depth_df = pd.DataFrame()

            for percentage in percentages:
                depth_df = calculate_depth(df, percentage)
                combined_depth_df = pd.concat([combined_depth_df, depth_df], ignore_index=True)

            return combined_depth_df

        return combine_depth_dfs(df, percentages)


    def run_code(self, url):
        data = self.fetch_market_depth_url(url)

        if data is None:
            return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), None, None)

        data.pop("status", None)
        result_df = self.extract_ask_bid(data)

        result_df = self.dataset_preparation(result_df)
        spread_df = self.spread_calculation(result_df)

        depth_df_with_percentages = self.calculate_depth_with_percentages(
            result_df, group_columns=['DateTime', 'Item']
        )

        return result_df, spread_df, depth_df_with_percentages


    def collect_data(self, url , slippage_spread_all_df, depth_all_df, run_interval=15):

        while True:
            result_df, slippage_spread_df, depth_df  = self.run_code(url)

            spread_data = result_df.groupby(self.LIST_COLUMN_NAME_INTERCEPT).agg(
                {'Ask_Price': 'max', 'Bid_Price': 'min'}).rename(
                columns={'Ask_Price': 'Best_Ask_Price', 'Bid_Price': 'Best_Bid_Price'}
            ).reset_index()

            spread_data['Spread'] = (spread_data['Best_Ask_Price'] - spread_data['Best_Bid_Price'])
            spread_data['Reference_Price'] = (spread_data['Best_Ask_Price'] + spread_data['Best_Bid_Price']) / 2
            self.save_orderbook_files(spread_data, f"df_spread_all_{datetime.now(pytz.utc).strftime('%Y-%m-%d')}")
            slippage_spread_all_df.columns = ['Item',
                                              'Date', 'DateTime', 'Timestamp',
                                              'Best_Ask_Price', 'Best_Bid_Price',
                                              'Spread',
                                              'Reference_Price',
                                              ]

            depth_all_df = pd.concat([depth_all_df, depth_df], ignore_index=True)
            depth_all_df = depth_all_df.drop_duplicates()

            time.sleep(run_interval)

            return slippage_spread_df, depth_all_df

    def send_to_telegram(self):
        try:
            csv_buffer = io.BytesIO()
            spread_df = pd.concat(self.data_list_spread, ignore_index=True)
            spread_df.to_csv(csv_buffer, index=False, encoding='utf-8')
            csv_buffer.seek(0)

            file_name_spread = f"wallex_df_spread_{datetime.now(pytz.utc).strftime('%Y-%m-%d')}.csv"
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

            file_name_depth = f"wallex_depth_all_{datetime.now(pytz.utc).strftime('%Y-%m-%d')}.csv"
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

                    df_results, df_slippage_spread_all, df_depth_all = self.run_code(self.URL_ORDERBOOK_wallex_ALL)

                    self.data_list_spread.append(df_slippage_spread_all)
                    self.data_list_depth.append(df_depth_all)

                    if now.minute == 59 and now.second >= (60 - self.interval_seconds):
                        self.send_to_telegram()

                time.sleep(1)
            except Exception as e:
                print(f"An error occurred: {e}")
                time.sleep(1)

class OrderBookManagerWallex:
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


if __name__ == "__main__":
    TELEGRAM_BOT_TOKEN = "7732239390:AAGuFI4pDUANbNxAbY9eT2FqzIawMZCoMA4"
    TELEGRAM_CHAT_ID = "5904776497"

    btc_usdt_collector = OrderBookCollectorWallex(
        telegram_bot_token=TELEGRAM_BOT_TOKEN,
        telegram_chat_id=TELEGRAM_CHAT_ID
    )

    manager = OrderBookManagerWallex([btc_usdt_collector])
    manager.start()


# if __name__ == "__main__":
#     df_slippage_spread_all = pd.DataFrame(columns=['Item',
#                                                    'Date', 'DateTime', 'Timestamp',
#                                                    'Reference_Price',
#                                                    'Ask_Price', 'Bid_Price', 'Spread'
#                                                    ])
#
#     df_depth_all = pd.DataFrame(columns=['Item',
#                                          'Date', 'DateTime', 'Timestamp',
#                                          'Reference_Price',
#                                          'Total_Bid_Volume', 'Total_Ask_Volume',
#                                          'Percentage'
#                                          ])
#
#     df_result_all = pd.DataFrame(columns=['Ask_Price', 'Ask_Volume', 'Ask_Sum', 'Item', 'Bid_Price',
#                                           'Bid_Volume', 'Bid_Sum', 'Timestamp',
#                                           'Reference_Price', 'DateTime', 'Date',
#                                           ])
#
#     df_new_all = pd.DataFrame(columns=['Ask_Price', 'Ask_Volume', 'Ask_Sum',
#                                        'Item',
#                                        'Bid_Price', 'Bid_Volume', 'Bid_Sum',
#                                        'Timestamp',
#                                        'Reference_Price',
#                                        'DateTime', 'Date',
#                                        'Best_Ask_Price', 'Best_Bid_Price',
#                                        'Spread'
#                                        ])
#
#     collect_data(15, URL_ORDERBOOK_wallex_ALL, df_slippage_spread_all, df_depth_all,
#                  df_result_all, df_new_all)
