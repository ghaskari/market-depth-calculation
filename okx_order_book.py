import time
import requests
import pandas as pd
from datetime import datetime
import concurrent.futures

name_exchange = "OKX"
symbols = ["BTC-USDT", "ETH-USDT"]
proxies = {
    'http': 'socks5://127.0.0.1:2080',
    'https': 'socks5://127.0.0.1:2080',
}


def fetch_order_book(symbol):
    url = f"https://www.okx.com/api/v5/market/books?instId={symbol}&sz=10"
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

    data.to_csv(filename, mode='a', header=False, index=False)


def process_order_book_data(symbol, order_book_data):
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

        save_data(iteration_data, name_exchange, symbol)


def main():
    while True:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {executor.submit(fetch_order_book, symbol): symbol for symbol in symbols}

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
