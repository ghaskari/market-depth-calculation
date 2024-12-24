import time
import requests
import pandas as pd
from datetime import datetime
import os
import concurrent.futures

name_exchange = "CoinEx"
symbols = ["BTCUSDT", "ETHUSDT"]
proxies = {
    'http': 'socks5://127.0.0.1:2080',
    'https': 'socks5://127.0.0.1:2080',
}


def fetch_market_depth(symbol):
    url = f"https://api.coinex.com/v1/market/depth?market={symbol.lower()}&merge=0"
    try:
        response = requests.get(url, proxies=proxies)
        response.raise_for_status()
        return symbol, response.json()
    except requests.RequestException as e:
        print(f"Failed to fetch data for {symbol}: {e}")
        return symbol, None


def save_data(data, name_exchange, symbol):
    today_date = datetime.today().strftime('%Y-%m-%d')
    filename = f'order_book_data/{name_exchange.lower()}/order_book_{name_exchange.lower()}_{symbol}_{today_date}.csv'


    existing_data = pd.read_csv(filename)
    combined_data = pd.concat([existing_data, data], ignore_index=True)

    combined_data.to_csv(filename, index=False)


def process_order_book_data(symbol, order_book_data):
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

        save_data(iteration_data, name_exchange, symbol)


def main():
    while True:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {executor.submit(fetch_market_depth, symbol): symbol for symbol in symbols}

            for future in concurrent.futures.as_completed(futures):
                symbol = futures[future]
                try:
                    symbol, order_book_data = future.result()
                    process_order_book_data(symbol, order_book_data)
                except Exception as e:
                    print(f"An error occurred for {symbol}: {e}")

        time.sleep(15)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Data fetching interrupted by user.")
