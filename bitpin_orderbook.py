import requests
import pandas as pd
from datetime import datetime
import time

api_url_btc_usdt_bitpin = "https://api.bitpin.org/api/v1/mth/orderbook/BTC_USDT/"
api_url_eth_usdt_bitpin = "https://api.bitpin.org/api/v1/mth/orderbook/ETH_USDT/"


def fetch_orderbook_bitpin(url, token, interval_seconds=15):
    data_list = []
    while True:
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()

                max_len = max(len(data["asks"]), len(data["bids"]))
                asks = data["asks"] + [['', '']] * (max_len - len(data["asks"]))
                bids = data["bids"] + [['', '']] * (max_len - len(data["bids"]))

                best_buy = max(float(b[0]) for b in data["bids"] if b[0])
                best_sell = min(float(a[0]) for a in data["asks"] if a[0])
                spread = best_sell - best_buy

                timestamp = datetime.now().timestamp()
                datetime_str = datetime.now().isoformat()
                date_str = datetime_str.split("T")[0]

                total_ask_volume = sum(float(a[1]) for a in data["asks"] if a[1])
                total_bid_volume = sum(float(b[1]) for b in data["bids"] if b[1])

                iteration_data = pd.DataFrame({
                    "Item": token,
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

                all_prices = [float(a[0]) for a in data["asks"] if a[0]] + [float(b[0]) for b in data["bids"] if b[0]]
                reference_price = pd.Series(all_prices).median()
                iteration_data["Reference_Price"] = reference_price

                data_list.append(iteration_data)

                order_book_df = pd.concat(data_list, ignore_index=True)
                today_date = datetime.today().strftime('%Y-%m-%d')
                output_dir = 'order_book_data/bitpin/'
                order_book_df.to_csv(f"{output_dir}order_book_bitpin_{token}_{today_date}.csv", mode='w', index=False)

                time.sleep(interval_seconds)
            else:
                print(f"Failed to fetch data from {url}, status code: {response.status_code}")
                time.sleep(interval_seconds)
        except Exception as e:
            print(f"An error occurred for {token}: {e}")
            time.sleep(interval_seconds)


def run_collectors():
    try:
        while True:
            fetch_orderbook_bitpin(api_url_btc_usdt_bitpin, "BTC_USDT")
            fetch_orderbook_bitpin(api_url_eth_usdt_bitpin, "ETH_USDT")
    except KeyboardInterrupt:
        print("Data collection interrupted by user.")


if __name__ == "__main__":
    run_collectors()
