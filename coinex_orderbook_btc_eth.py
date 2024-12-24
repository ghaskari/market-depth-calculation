import pandas as pd
import pytz
import requests
import time
import concurrent.futures

# Dictionary of token names and URLs
url_token_mapping = {
    'BTCUSDT': 'https://api.coinex.com/v1/market/depth?market=btcusdt&merge=0',
    'ETHUSDT': 'https://api.coinex.com/v1/market/depth?market=ethusdt&merge=0'
}

LIST_COLUMN_NAME_INTERCEPT = ['timestamp', 'DateTime', 'Date', 'Item', 'Coins', 'CoinName', 'Currency']


def save_csv_orderbook(df, filename):
    df.to_csv(f'OrderBookDataInternational/coinex/coinex_orderbook_{filename}.csv', index=False)


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
        asks = data['data']['asks']
        bids = data['data']['bids']
        last = data['data']['last']
        timestamp = data['data']['time']
        asks_with_type = [(price, quantity, 'ask') for price, quantity in asks]
        bids_with_type = [(price, quantity, 'bid') for price, quantity in bids]
        combined_data = asks_with_type + bids_with_type
        order_book_df = pd.DataFrame(combined_data, columns=['price', 'quantity', 'type'])
        order_book_df['LastPrice'] = last
        order_book_df['timestamp'] = timestamp
        if not order_book_df.empty:
            last_update = order_book_df['timestamp'].iloc[0]
            order_book_df['DateTime'] = pd.to_datetime(last_update, unit='ms')
            order_book_df['DateTime'] = order_book_df['DateTime'].apply(convert_to_tehran_time)
            order_book_df['Date'] = order_book_df['DateTime'].dt.date
            return order_book_df, last_update
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def calculate_spread_coinx(df):
    df['Price'] = pd.to_numeric(df['price'])
    df['LastPrice'] = pd.to_numeric(df['LastPrice'])
    buy_df = df[df['type'] == 'bid']
    sell_df = df[df['type'] == 'ask']
    highest_buy_price = buy_df['Price'].max()
    lowest_sell_price = sell_df['Price'].min()
    spread = lowest_sell_price - highest_buy_price
    df['BestBuy'] = highest_buy_price
    df['BestSell'] = lowest_sell_price
    df['Spread'] = spread
    df['Slippage'] = ((df['LastPrice'] - df['BestBuy']) / df['BestBuy']) * 100
    return df


def calculate_depth_percent_coinx(df, percentages=[0, 2, 5, 10]):
    if df.empty:
        print("The input DataFrame is empty.")
    else:
        combined_depth_df = pd.DataFrame(columns=LIST_COLUMN_NAME_INTERCEPT)
        ALTERNATE_COLUMN_NAME_INTERCEPT = LIST_COLUMN_NAME_INTERCEPT.copy()
        ALTERNATE_COLUMN_NAME_INTERCEPT.extend(['LowerBond', 'UpperBond'])

        def calculate_depth(percentage):

            buy_df = df[df['type'] == 'bid'].copy()
            sell_df = df[df['type'] == 'ask'].copy()
            highest_buy_price = buy_df['Price'].max()
            lowest_sell_price = sell_df['Price'].min()
            buy_df.loc[:, 'BestBuy'] = highest_buy_price
            sell_df.loc[:, 'BestSell'] = lowest_sell_price
            sell_df.loc[:, 'LowerBond'] = sell_df['BestSell'] * (1 - (percentage / 100))
            buy_df.loc[:, 'UpperBond'] = buy_df['BestBuy'] * (1 + (percentage / 100))
            df_filtered_buy = buy_df[(buy_df['Price'] <= buy_df['UpperBond'])]
            df_filtered_sell = sell_df[(sell_df['Price'] >= sell_df['LowerBond'])]
            depth_df = df_filtered_buy.merge(df_filtered_sell, how='outer')
            depth_df['VolumeTotal'] = depth_df['quantity'].sum()
            depth_df['Percentage'] = percentage

            return depth_df

        depth_dfs = [calculate_depth(percentage) for percentage in percentages]
        for df in depth_dfs:
            combined_depth_df = pd.concat([combined_depth_df, df], ignore_index=True)
        return combined_depth_df


if __name__ == "__main__":
    df_all_coinex_orderbook = pd.DataFrame()
    while True:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {executor.submit(fetch_market_depth_url, url): token for token, url in url_token_mapping.items()}
            for future in concurrent.futures.as_completed(futures):
                token = futures[future]
                data = future.result()
                if not data:
                    continue
                df, last_update = extract_ask_bid(data)
                print(token)
                if not df.empty or df['timestamp'].notna().any():
                    df['Item'] = token
                    df['Coins'] = f"{token.split('USDT')[0]}_USDT"
                    data_separate(df, '_', 'Coins')
                    df = calculate_spread_coinx(df)
                    df_all_coinex_orderbook = pd.concat([df_all_coinex_orderbook, df], axis=0)

                    # save_csv_orderbook(df, f'total_orderbook_coinex_{token}_{df["Date"].iloc[0]}_{df["timestamp"].iloc[0]}')

                    spread_df = df[['Item', 'Coins', 'CoinName', 'Currency', 'timestamp', 'DateTime', 'Date', 'BestBuy',
                                    'BestSell', 'Spread']].iloc[0:1, :]
                    # save_csv_orderbook(spread_df,
                    #                    f'spread_coinex_{token}_{spread_df["Date"].iloc[0]}_{spread_df["timestamp"].iloc[0]}')

                    df_depth = calculate_depth_percent_coinx(df)
                    save_csv_orderbook(df_depth, f'depth_coinex_{token}_{df_depth["Date"].iloc[0]}')

                    save_csv_orderbook(df_all_coinex_orderbook,
                                       f'all_order_book_coinex_{token}_{df_depth["Date"].iloc[0]}')

        time.sleep(15)
