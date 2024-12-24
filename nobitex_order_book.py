import pandas as pd
import pytz
import requests

URL_ORDERBOOK_BTCUSDT_NOBITEX = 'https://api.nobitex.ir/v3/orderbook/BTCUSDT'
URL_ORDERBOOK_ETHUSDT_NOBITEX = 'https://api.nobitex.ir/v3/orderbook/ETHUSDT'
URL_ORDERBOOK_NOBITEX_ALL = "https://api.nobitex.ir/v3/orderbook/all"

LIST_COLUMN_NAME_INTERCEPT = ['Item', 'Date', 'DateTime', 'Timestamp', 'Reference_Price']


def fetch_market_depth_url(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to fetch data:", response.status_code)
        return {}


def convert_to_tehran_time(timestamp_ms):
    tehran_timezone = pytz.timezone('Asia/Tehran')
    datetime_utc = pd.to_datetime(timestamp_ms, unit='ms').tz_localize('UTC')
    datetime_tehran = datetime_utc.tz_convert(tehran_timezone)
    return datetime_tehran


def data_separate(df, split_sign, column_name):
    df['CoinName'] = df[column_name].str.split(split_sign).str.get(0)
    df['Currency'] = df[column_name].str.split(split_sign).str.get(1)
    return df


def extract_ask_bid(data):
    rows = []
    last_update = []

    for key, value in data.items():
        last_update.append(value['lastUpdate'])
        for bid, ask in zip(value['bids'], value['asks']):
            row = {
                'Item': key,
                'Timestamp': value['lastUpdate'],
                'LastTradePrice': value['lastTradePrice'],
                'Bid_Price': float(bid[0]),
                'Bid_Volume': float(bid[1]),
                'Ask_Price': float(ask[0]),
                'Ask_Volume': float(ask[1])
            }
            rows.append(row)

    return pd.DataFrame(rows), last_update


def dataset_preparation(result_df):
    result_df.fillna({'Bid_Price': 0, 'Bid_Volume': 0, 'Ask_Price': 0, 'Ask_Volume': 0, 'LastTradePrice': 0},
                     inplace=True)
    result_df['Reference_Price'] = result_df['LastTradePrice'].astype(float)
    result_df['DateTime'] = pd.to_datetime(result_df['Timestamp'], unit='ms')
    result_df['DateTime'] = result_df['DateTime'].apply(convert_to_tehran_time)
    result_df['Date'] = result_df['DateTime'].dt.date
    return result_df


def spread_calculation(result_df):
    spread_data = result_df.groupby(LIST_COLUMN_NAME_INTERCEPT).agg(
        {'Ask_Price': 'max', 'Bid_Price': 'min'}).rename(
            columns={'Ask_Price': 'Best_Ask_Price', 'Bid_Price': 'Best_Bid_Price'}
        ).reset_index()

    spread_data['Spread'] = (spread_data['Best_Ask_Price'] - spread_data['Best_Bid_Price'])

    return spread_data


def calculate_depth_with_percentages(df, percentages=[0, 2, 5, 10]):
    combined_depth_df = pd.DataFrame(columns=LIST_COLUMN_NAME_INTERCEPT)
    ALTERNATE_COLUMN_NAME_INTERCEPT = LIST_COLUMN_NAME_INTERCEPT.copy()

    def calculate_depth(percentage):
        depth_df = df.groupby(ALTERNATE_COLUMN_NAME_INTERCEPT).agg(
            Bid_Depth=('Bid_Volume', 'sum'),
            Ask_Depth=('Ask_Volume', 'sum'),
        ).reset_index()

        depth_df['Percentage'] = percentage
        return depth_df

    depth_dfs = [calculate_depth(percentage) for percentage in percentages]

    for df in depth_dfs:
        combined_depth_df = pd.concat([combined_depth_df, df], ignore_index=True)

    return combined_depth_df


def process_data(url):
    data = fetch_market_depth_url(url)
    data.pop("status", None)
    result_df, last_update = extract_ask_bid(data)
    item_date, last_item_str = pd.to_datetime(last_update[-1], unit='ms').date(), last_update[-1]
    result_df = dataset_preparation(result_df)
    spread_df = spread_calculation(result_df)
    depth_df_with_percentages = calculate_depth_with_percentages(result_df)
    return result_df, spread_df, depth_df_with_percentages, item_date, last_item_str


def collect_data(url):
    df_slippage_spread_all = pd.DataFrame()
    df_depth_all = pd.DataFrame()
    df_result_all = pd.DataFrame()

    while True:
        result_df, spread_df, depth_df, item_date, last_item_str = process_data(url)
        df_result_all = pd.concat([df_result_all, result_df], ignore_index=True)

        df_slippage_spread_all = pd.concat([df_slippage_spread_all, spread_df], ignore_index=True)
        df_slippage_spread_all.to_csv(f'order_book_data/nobitex/nobitex_df_spread{item_date}.csv', index=False)

        df_depth_all = pd.concat([df_depth_all, depth_df], ignore_index=True)
        df_depth_all.to_csv(f'order_book_data/nobitex/nobitex_depth_all_{item_date}.csv', index=False)


collect_data(URL_ORDERBOOK_NOBITEX_ALL)