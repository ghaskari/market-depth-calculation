import time
import requests
import pandas as pd
from datetime import datetime
import os
import asyncio
import platform

# Set WindowsSelectorEventLoopPolicy only on Windows
if platform.system() == 'Windows' and asyncio.get_event_loop_policy().__class__.__name__ != 'SelectorEventLoopPolicy':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

name_exchange = "Binance"
symbols = ["BTCUSDT", "ETHUSDT"]
order_book_data = {symbol: [] for symbol in symbols}
last_save_time = {symbol: time.time() for symbol in symbols}
save_interval = 60

async def fetch_order_book(symbol):
    url = f"https://api.binance.com/api/v3/depth?limit=10&symbol={symbol}"
    try:
        response = requests.get(url)
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

async def gather_data_binance(symbol):
    data_list = []

    global order_book_data, last_save_time
    while True:
        order_book_data = await fetch_order_book(symbol)
        print(order_book_data)
        if order_book_data:
            timestamp = int(time.time())
            datetime_timestamp = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

            max_len = max(len(order_book_data["asks"]), len(order_book_data["bids"]))
            asks = order_book_data["asks"] + [['', '']] * (max_len - len(order_book_data["asks"]))
            bids = order_book_data["bids"] + [['', '']] * (max_len - len(order_book_data["bids"]))

            best_buy = max(float(b[0]) for b in order_book_data["bids"])
            best_sell = min(float(a[0]) for a in order_book_data["asks"])

            spread = best_sell - best_buy

            timestamp = datetime.now().timestamp()
            datetime_str = datetime.now().isoformat()
            date_str = datetime_str.split("T")[0]

            iteration_data = pd.DataFrame({
                "Token": symbol,
                "Currency": "USDT",
                "Timestamp": timestamp,
                "AskPrice": [float(a[0]) if a[0] else None for a in asks],
                "AskVolume": [float(a[1]) if a[1] else None for a in asks],
                "BidPrice": [float(b[0]) if b[0] else None for b in bids],
                "BidVolume": [float(b[1]) if b[1] else None for b in bids],
                "DateTime": datetime_str,
                "Date": date_str,
                "BestBuy": best_buy,
                "BestSell": best_sell,
                "Spread": spread
            })

            all_prices = [float(a[0]) for a in order_book_data["asks"]] + [float(b[0]) for b in order_book_data["bids"]]
            reference_price = pd.Series(all_prices).median()
            iteration_data["Reference_Price"] = reference_price

            data_list.append(iteration_data)

            order_book_df = pd.concat(data_list, ignore_index=True)
            output_dir = 'OrderBookDataInternational/binance/'
            os.makedirs(output_dir, exist_ok=True)
            order_book_df.to_csv(f"{output_dir}order_book_binance_{symbol}_{date_str}.csv", index=False)
            print(order_book_df)

        await asyncio.sleep(15)

async def main():
    tasks = [
        gather_data_binance('BTCUSDT'),
        gather_data_binance('ETHUSDT')
    ]

    results = await asyncio.gather(*tasks)
    return results

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Data fetching interrupted by user.")
