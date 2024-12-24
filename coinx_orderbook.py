import pandas as pd
import pytz
import requests
import time
import concurrent.futures

url_coinx_tokens = 'https://api.coinex.com/v1/market/depth?market=btcusdt&merge=0'
url_coinx_market = 'https://api.coinex.com/v1/market/list'

LIST_COLUMN_NAME_INTERCEPT = ['timestamp', 'DateTime',
       'Date', 'Item', 'Coins', 'CoinName', 'Currency']


def save_csv_orderbook(df, filename):
    df.to_csv(f'order_book_data/coinx/coinx_orderbook_{filename}.csv', index=False)


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
        # Extract data from the dictionary
        asks = data['data']['asks']
        bids = data['data']['bids']
        last = data['data']['last']
        timestamp = data['data']['time']

        # Add a type indicator to each entry
        asks_with_type = [(price, quantity, 'ask') for price, quantity in asks]
        bids_with_type = [(price, quantity, 'bid') for price, quantity in bids]

        # Combine the asks and bids into a single list
        combined_data = asks_with_type + bids_with_type

        # Create a single DataFrame from the combined data
        order_book_df = pd.DataFrame(combined_data, columns=['price', 'quantity', 'type'])

        # Add additional information (like 'last' and 'timestamp') to the DataFrame
        order_book_df['LastPrice'] = last
        order_book_df['timestamp'] = timestamp

        # Ensure there's at least one timestamp
        if not order_book_df.empty:
            last_update = order_book_df['timestamp'].iloc[0]
            order_book_df['DateTime'] = pd.to_datetime(last_update, unit='ms')
            order_book_df['DateTime'] = order_book_df['DateTime'].apply(convert_to_tehran_time)
            order_book_df['Date'] = order_book_df['DateTime'].dt.date

            return order_book_df, last_update

    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def calculate_spread_coinx(df):
    # Convert Price column to numeric
    df['Price'] = pd.to_numeric(df['price'])
    df['LastPrice'] = pd.to_numeric(df['LastPrice'])

    # Separate buy and sell orders
    buy_df = df[df['type'] == 'bid']
    sell_df = df[df['type'] == 'ask']

    # Find highest buy price and lowest sell price
    highest_buy_price = buy_df['Price'].max()
    lowest_sell_price = sell_df['Price'].min()

    # Calculate spread
    spread = lowest_sell_price - highest_buy_price
    df['BestBuy'] = highest_buy_price
    df['BestSell'] = lowest_sell_price

    df['Spread'] = spread
    df['Slippage'] = ((df['LastPrice'] - df['BestBuy']) / df['BestBuy']) * 100

    return spread


def calculate_depth_percent_coinx(df, percentages=[0, 2, 5, 10]):

    if df.empty:
        print("The input DataFrame is empty.")

    else:

        combined_depth_df = pd.DataFrame(columns=LIST_COLUMN_NAME_INTERCEPT)
        ALTERNATE_COLUMN_NAME_INTERCEPT = LIST_COLUMN_NAME_INTERCEPT.copy()
        ALTERNATE_COLUMN_NAME_INTERCEPT.extend(['LowerBond', 'UpperBond'])

        def calculate_depth(percentage):

            # Separate buy and sell orders
            buy_df = df[df['type'] == 'bid'].copy()
            sell_df = df[df['type'] == 'ask'].copy()

            # Find highest buy price and lowest sell price
            highest_buy_price = buy_df['Price'].max()
            lowest_sell_price = sell_df['Price'].min()

            buy_df.loc[:, 'BestBuy'] = highest_buy_price
            sell_df.loc[:, 'BestSell'] = lowest_sell_price

            # And for the calculations:
            sell_df.loc[:, 'LowerBond'] = sell_df['BestSell'] * (1 - (percentage / 100))
            buy_df.loc[:, 'UpperBond'] = buy_df['BestBuy'] * (1 + (percentage / 100))

            df_filtered_buy = buy_df[(buy_df['Price'] <= buy_df['UpperBond'])]
            df_filtered_sell = sell_df[(sell_df['Price'] >= sell_df['LowerBond'])]
            df_filtered = pd.merge(df_filtered_buy, df_filtered_sell, on=['Item', 'Coins', 'CoinName', 'Currency', 'timestamp', 'DateTime', 'Date'], suffixes=('_buy', '_sell'))
            df_filtered['quantity_sell'] = df_filtered['quantity_sell'].astype(float)
            df_filtered['quantity_buy'] = df_filtered['quantity_buy'].astype(float)

            depth_df = df_filtered.groupby(['Item', 'Coins', 'CoinName', 'Currency', 'timestamp', 'DateTime', 'Date']).agg(
                SellDepth=('quantity_sell', 'sum'),
                BuyDepth=('quantity_buy', 'sum'),
                BestSell=('BestSell_sell', 'mean'),
                BestBuy=('BestBuy_buy', 'mean'),
            ).reset_index()

            depth_df['VolumeTotal'] = depth_df['SellDepth'].fillna(0) - depth_df['BuyDepth'].fillna(0)
            depth_df['Percentage'] = percentage
            depth_df['LowerBond'] = sell_df.loc[0:1, 'LowerBond']
            depth_df['UpperBond'] = buy_df['UpperBond'].iloc[0]

            return depth_df

        depth_dfs = [calculate_depth(percentage) for percentage in percentages]

        for df in depth_dfs:
            combined_depth_df = pd.concat([combined_depth_df, df], ignore_index=True)

        return combined_depth_df


if __name__ == "__main__":
    while True:
        # Fetch market data
        data_market = fetch_market_depth_url(url_coinx_market)

        spread_total_df = pd.DataFrame()
        depth_total_df = pd.DataFrame()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []

            for token in data_market['data']:
                futures.append(executor.submit(fetch_market_depth_url, f'https://api.coinex.com/v1/market/depth?market={token}&merge=0'))

            for future in concurrent.futures.as_completed(futures):
                data = future.result()
                if not data:
                    continue

                token = data['data']['market']
                df, last_update = extract_ask_bid(data)

                print(token)

                if not df.empty or df['timestamp'].notna().any():

                    df['Item'] = token
                    df['Coins'] = df['Item'].replace({'USDT': '_USDT', 'BTC': '_BTC', 'USDC': '_USDC'}, regex=True)
                    data_separate(df, '_', 'Coins')
                    calculate_spread_coinx(df)

                    spread_total_df = pd.concat([spread_total_df, df[['Item', 'Coins', 'CoinName', 'Currency', 'timestamp',
                                                                              'DateTime', 'Date', 'BestBuy', 'BestSell',
                                                                              'Spread']].iloc[0:1, :]], ignore_index=True)

                    df_depth = calculate_depth_percent_coinx(df)
                    depth_total_df = pd.concat([depth_total_df, df_depth], ignore_index=True)

                    save_csv_orderbook(df,
                                        f'total_orderbook_{token}_{df["Date"].iloc[0]}_{df["timestamp"].iloc[0]}')
                    save_csv_orderbook(depth_total_df, f'depth_{df["Date"].iloc[0]}')
                    save_csv_orderbook(spread_total_df, f'spread_{df["Date"].iloc[0]}')

        time.sleep(15)
