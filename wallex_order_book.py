import pandas as pd
import time
import datetime
import requests
import os

URL_ORDERBOOK_BTCUSDT_WALLEX = 'https://api.wallex.ir/v1/depth?symbol=BTCUSDT'
URL_ORDERBOOK_ETHUSDT_WALLEX = 'https://api.wallex.ir/v1/depth?symbol=ETHUSDT'
URL_ORDERBOOK_wallex_ALL = "https://api.wallex.ir/v2/depth/all"

LIST_COLUMN_NAME_INTERCEPT = ['Item', 'Date', 'DateTime', 'Timestamp']
output_dir = 'order_book_data/wallex/'

def save_orderbook_files(df, filename, save_csv=True, save_json=False):

    output_dir = 'order_book_data/wallex/'
    os.makedirs(output_dir, exist_ok=True)

    if save_csv:
        csv_filepath = os.path.join(output_dir, f'wallex_orderbook_{filename}.csv')
        df.to_csv(csv_filepath, index=False)

    if save_json:
        json_filepath = os.path.join(output_dir, f'wallex_orderbook_{filename}.json')
        df.to_json(json_filepath, orient='records', lines=True)


def fetch_market_depth_url(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None


def data_separate(df, split_sign, column_name):
    df['CoinName'] = df[column_name].str.split(split_sign).str.get(0)
    df['Currency'] = df[column_name].str.split(split_sign).str.get(1)
    return df


def extract_ask_bid(data):

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

    current_datetime = datetime.datetime.now()

    LastUpdate = current_datetime.timestamp()
    result_df['Timestamp'] = LastUpdate
    result_df['DateTime'] = current_datetime
    result_df['Date'] = result_df['DateTime'].dt.date

    result_df['DateTime'] = pd.to_datetime(result_df['Timestamp'], unit='ms')
    result_df['DateTime'] = result_df['DateTime'].dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]

    return result_df


def dataset_preparation(result_df):
    result_df.fillna({'Bid_Price': 0,
                      'Bid_Quantity': 0,
                      'Ask_Price': 0,
                      'Ask_Quantity': 0,
                      'LastTradePrice': 0},
                     inplace=True)

    result_df = result_df.replace('', 0)
    return result_df


def prepare_datatime_timestamp():
    current_datetime = datetime.datetime.now()
    current_date = current_datetime.date()
    LastUpdate = current_datetime.timestamp()

    return current_date, LastUpdate


def spread_calculation(result_df):
    spread_data = result_df.groupby(LIST_COLUMN_NAME_INTERCEPT).agg(
        {'Ask_Price': 'max', 'Bid_Price': 'min'}).rename(
        columns={'Ask_Price': 'Best_Ask_Price', 'Bid_Price': 'Best_Bid_Price'}
    ).reset_index()

    spread_data['Spread'] = (spread_data['Best_Ask_Price'] - spread_data['Best_Bid_Price'])
    spread_data['Reference_Price'] = (spread_data['Best_Ask_Price'] + spread_data['Best_Bid_Price']) / 2

    return spread_data


def calculate_depth_with_percentages(df, percentages=[0, 2, 5, 10], group_columns=LIST_COLUMN_NAME_INTERCEPT):

    def calculate_depth(df, percentage=[0, 2, 5, 10], group_columns=LIST_COLUMN_NAME_INTERCEPT):
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
            Bid_Depth=('Bid_Quantity', 'sum'),
            Ask_Depth=('Ask_Quantity', 'sum'),
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


def run_code(url):
    data = fetch_market_depth_url(url)

    if data is None:
        return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), None, None)

    data.pop("status", None)
    result_df = extract_ask_bid(data)

    item_date, last_item_str = prepare_datatime_timestamp()

    result_df = dataset_preparation(result_df)
    spread_df = spread_calculation(result_df)

    depth_df_with_percentages = calculate_depth_with_percentages(
        result_df, group_columns=['datetime', 'coin']
    )

    return (result_df, spread_df, depth_df_with_percentages, item_date, last_item_str)


def collect_data(run_interval, url, slippage_spread_all_df, depth_all_df, df_result_all, df_new_all):

    while True:
        result_df, slippage_spread_df, depth_df, item_date, last_item_str = run_code(url)
        df_result_all = pd.concat([df_result_all, result_df], ignore_index=True)

        spread_data = result_df.groupby(LIST_COLUMN_NAME_INTERCEPT).agg(
            {'Ask_Price': 'max', 'Bid_Price': 'min'}).rename(
            columns={'Ask_Price': 'Best_Ask_Price', 'Bid_Price': 'Best_Bid_Price'}
        ).reset_index()

        spread_data['Spread'] = (spread_data['Best_Ask_Price'] - spread_data['Best_Bid_Price'])
        spread_data['Reference_Price'] = (spread_data['Best_Ask_Price'] + spread_data['Best_Bid_Price']) / 2

        df_new = pd.merge(result_df, spread_data, how='left', on=['Item',
                                                                  'Timestamp',
                                                                  'DateTime',
                                                                  'Date',
                                                                  ]
                          )

        slippage_spread_all_df.columns = ['Item',
                                          'Date', 'DateTime', 'Timestamp',
                                          'Best_Ask_Price', 'Best_Bid_Price',
                                          'Spread',
                                          'Reference_Price',
                                          ]

        df_new_all = pd.concat([df_new_all, df_new], ignore_index=True)

        depth_all_df = pd.concat([depth_all_df, depth_df], ignore_index=True)
        depth_all_df = depth_all_df.drop_duplicates()

        save_orderbook_files(slippage_spread_df, f'df_spread_all_{item_date}')

        save_orderbook_files(depth_all_df, f'depth_all_{item_date}')

        time.sleep(run_interval)


if __name__ == "__main__":
    df_slippage_spread_all = pd.DataFrame(columns=['Item',
                                                   'Date', 'DateTime', 'Timestamp',
                                                   'Reference_Price',
                                                   'Ask_Price', 'Bid_Price', 'Spread'
                                                   ])

    df_depth_all = pd.DataFrame(columns=['Item',
                                         'Date', 'DateTime', 'Timestamp',
                                         'Reference_Price',
                                         'Bid_Depth', 'Ask_Depth',
                                         'Percentage'
                                         ])

    df_result_all = pd.DataFrame(columns=['Ask_Price', 'Ask_Volume', 'Ask_Sum', 'Item', 'Bid_Price',
                                          'Bid_Volume', 'Bid_Sum', 'Timestamp',
                                          'Reference_Price', 'DateTime', 'Date',
                                          ])

    df_new_all = pd.DataFrame(columns=['Ask_Price', 'Ask_Volume', 'Ask_Sum',
                                       'Item',
                                       'Bid_Price', 'Bid_Volume', 'Bid_Sum',
                                       'Timestamp',
                                       'Reference_Price',
                                       'DateTime', 'Date',
                                       'Best_Ask_Price', 'Best_Bid_Price',
                                       'Spread'
                                       ])

    collect_data(15, URL_ORDERBOOK_wallex_ALL, df_slippage_spread_all, df_depth_all,
                 df_result_all, df_new_all)
