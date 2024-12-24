import pandas as pd
import pytz
import requests
import time
import concurrent.futures
from datetime import datetime

# URL to token mapping
url_token_mapping = {
    'BTC-USDT': 'https://www.okx.com/api/v5/market/books?instId=BTC-USDT&sz=10',
    'ETH-USDT': 'https://www.okx.com/api/v5/market/books?instId=ETH-USDT&sz=10'
}

LIST_COLUMN_NAME_INTERCEPT = ['timestamp', 'DateTime', 'Date', 'Item', 'Coins', 'CoinName', 'Currency']


def save_csv_orderbook(df, filename):
    df.to_csv(f'OrderBookDataInternational/okx/okx_orderbook_{filename}.csv', index=False)


def convert_to_tehran_time(timestamp_ms):
    tehran_timezone = pytz.timezone('Asia/Tehran')
    datetime_utc = pd.to_datetime(timestamp_ms, unit='ms').tz_localize('UTC')
    datetime_tehran = datetime_utc.tz_convert(tehran_timezone)
    return datetime_tehran


def data_separate(df, split_sign, column_name):
    df['CoinName'] = df[column_name].str.split(split_sign).str.get(0)
    df['Currency'] = df[column_name].str.split(split_sign).str.get(1)
    return df


def fetch_market_depth_url(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None


def extract_ask_bid(data):
    try:
        # Ensure the "data" field exists and contains valid data
        if not data or "data" not in data or not data["data"]:
            print("Invalid data format or empty response.")
            return pd.DataFrame(), None

        # Extract the first dataset from the response
        order_data = data["data"][0]

        # Extract price and quantity only from asks and bids
        asks = [(float(ask[0]), float(ask[1]), 'ask') for ask in order_data["asks"]]
        bids = [(float(bid[0]), float(bid[1]), 'bid') for bid in order_data["bids"]]

        # Combine into a single DataFrame
        combined_data = asks + bids
        order_book_df = pd.DataFrame(combined_data, columns=['Price', 'Quantity', 'Type'])

        # Add metadata
        timestamp = int(order_data["ts"])  # Extract timestamp from response
        order_book_df["timestamp"] = timestamp
        order_book_df["DateTime"] = datetime.utcfromtimestamp(timestamp / 1000).isoformat()
        order_book_df["Date"] = order_book_df["DateTime"].str.split("T").str.get(0)

        return order_book_df, timestamp
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return pd.DataFrame(), None


def calculate_spread_okx(df):
    if df.empty:
        return df

    buy_df = df[df['Type'] == 'bid']
    sell_df = df[df['Type'] == 'ask']
    highest_buy_price = buy_df['Price'].max()
    lowest_sell_price = sell_df['Price'].min()
    spread = lowest_sell_price - highest_buy_price

    df['BestBuy'] = highest_buy_price
    df['BestSell'] = lowest_sell_price
    df['Spread'] = spread
    return df

def calculate_depth_percent_okx(df, percentages=[0, 2, 5, 10]):
    if df.empty:
        print("The input DataFrame is empty.")
    else:
        combined_depth_df = pd.DataFrame(columns=LIST_COLUMN_NAME_INTERCEPT)
        ALTERNATE_COLUMN_NAME_INTERCEPT = LIST_COLUMN_NAME_INTERCEPT.copy()
        ALTERNATE_COLUMN_NAME_INTERCEPT.extend(['LowerBond', 'UpperBond'])

        return combined_depth_df


if __name__ == "__main__":
    df_all_okx_orderbook = pd.DataFrame()
    while True:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {executor.submit(fetch_market_depth_url, url): token for token, url in url_token_mapping.items()}
            for future in concurrent.futures.as_completed(futures):
                token = futures[future]
                data = future.result()
                if not data:
                    continue

                df, last_update = extract_ask_bid(data)

                if not df.empty:
                    df['Item'] = token
                    df['Coins'] = f"{token.split('-')[0]}_USDT"
                    df = data_separate(df, '_', 'Coins')
                    df = calculate_spread_okx(df)
                    df_all_okx_orderbook = pd.concat([df_all_okx_orderbook, df], axis=0)

                    # save_csv_orderbook(df, f'total_orderbook_okx_{token}_{df["Date"].iloc[0]}_{last_update}')

                    spread_df = df[['Item', 'Coins', 'CoinName', 'Currency', 'timestamp', 'DateTime', 'Date', 'BestBuy',
                                    'BestSell', 'Spread']].iloc[0:1, :]
                    save_csv_orderbook(spread_df,
                                       f'spread_okx_{token}_{spread_df["Date"].iloc[0]}')

                    # df_depth = calculate_depth_percent_okx(df)
                    # save_csv_orderbook(df_depth, f'depth_okx_{token}_{df_depth["Date"].iloc[0]}')

                    save_csv_orderbook(df_all_okx_orderbook,
                                       f'all_order_book_okx_{token}_{df["Date"].iloc[0]}')

        time.sleep(15)
