import time
import requests
import pandas as pd
from datetime import datetime
import os

name_exchange = "Binance"
symbols = ["BTCUSDT", "ETHUSDT"]
proxies = {
    'http': 'socks5://127.0.0.1:2080',
    'https': 'socks5://127.0.0.1:2080',
}


def fetch_order_book(symbol):
    url = f"https://api.binance.com/api/v3/depth?limit=10&symbol={symbol}"
    try:
        response = requests.get(url, proxies=proxies)  # Use proxies here
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Failed to fetch data for {symbol}: {e}")
        return None


def save_data(data, name_exchange, symbol):
    today_date = datetime.today().strftime('%Y-%m-%d')
    filename = f'OrderBookDataInternational/order_book_data_{name_exchange.lower()}_{today_date}_{symbol}.csv'

    os.makedirs(os.path.dirname(filename), exist_ok=True)

    df = pd.DataFrame(data)
    df.to_csv(filename, mode='a', header=not os.path.exists(filename), index=False)
    print(f"Data saved to {filename}")


def gather_data_binance(symbol):
    data_list = []

    while True:
        order_book_data = fetch_order_book(symbol)
        if order_book_data:
            timestamp = int(time.time())
            datetime_timestamp = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

            max_len = max(len(order_book_data["asks"]), len(order_book_data["bids"]))
            asks = order_book_data["asks"] + [['', '']] * (max_len - len(order_book_data["asks"]))
            bids = order_book_data["bids"] + [['', '']] * (max_len - len(order_book_data["bids"]))

            best_buy = max(float(b[0]) for b in order_book_data["bids"] if b[0])
            best_sell = min(float(a[0]) for a in order_book_data["asks"] if a[0])

            spread = best_sell - best_buy

            timestamp = datetime.now().timestamp()
            datetime_str = datetime.now().isoformat()
            date_str = datetime_str.split("T")[0]

            iteration_data = pd.DataFrame({
                "Item": symbol,
                # "Currency": "USDT",
                "Timestamp": timestamp,
                "Ask_Price": [float(a[0]) if a[0] else None for a in asks],
                "Ask_Volume": [float(a[1]) if a[1] else None for a in asks],
                "Bid_Price": [float(b[0]) if b[0] else None for b in bids],
                "Bid_Volume": [float(b[1]) if b[1] else None for b in bids],
                "DateTime": datetime_str,
                "Date": date_str,
                "Best_Bid_Price": best_buy,
                "Best_Ask_Price": best_sell,
                "Spread": spread
            })

            all_prices = [float(a[0]) for a in order_book_data["asks"] if a[0]] + \
                         [float(b[0]) for b in order_book_data["bids"] if b[0]]
            reference_price = pd.Series(all_prices).median()
            iteration_data["Reference_Price"] = reference_price

            data_list.append(iteration_data)

            order_book_df = pd.concat(data_list, ignore_index=True)
            output_dir = f'order_book_data/{name_exchange.lower()}/'
            os.makedirs(output_dir, exist_ok=True)
            order_book_df.to_csv(f"{output_dir}order_book_binance_{symbol}_{date_str}.csv", index=False)
            print(order_book_df)

        time.sleep(15)


def main():
    for symbol in symbols:
        print(f"Fetching data for {symbol}")
        gather_data_binance(symbol)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Data fetching interrupted by user.")
