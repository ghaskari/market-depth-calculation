import time
import ccxt
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor


def fetch_order_book_from_timestamp(symbol, frm_ts, duration_in_seconds, interval_in_seconds, exchange, exchange_info,
                                    name_exchange):

    proxies = {
        # 'http': 'socks5://127.0.0.1:10808',
        # 'https': 'socks5://127.0.0.1:10808',
        'http': 'socks5://127.0.0.1:2080',
        'https': 'socks5://127.0.0.1:2080',
    }

    if exchange_info.lower() in ['okx', 'coinex', 'binance']:
        exchange.proxies = proxies

    current_time = time.time()
    if frm_ts > current_time:
        time_to_wait = frm_ts - current_time
        print(f"Waiting {time_to_wait} seconds until the start time.")
        time.sleep(time_to_wait)

    end_time = current_time + duration_in_seconds
    order_book_data = []
    last_save_time = time.time()

    while time.time() < end_time:
        params = {'paginate': True} if exchange_info.lower() != 'binance' else {}
        order_book = exchange.fetch_order_book(symbol,
                                               # params=params
                                               )

        timestamp = order_book['timestamp']
        if timestamp is None:
            timestamp = int(time.time() * 1000)
        timestamp_in_seconds = timestamp / 1000
        datetime_timestamp = datetime.utcfromtimestamp(timestamp_in_seconds)

        bids = order_book['bids']
        asks = order_book['asks']

        for bid in bids:
            order_book_data.append({
                'timestamp': timestamp,
                'datetime': datetime_timestamp,
                'type': 'bid',
                'price': bid[0],
                'volume': bid[1]
            })

        for ask in asks:
            order_book_data.append({
                'timestamp': timestamp,
                'datetime': datetime_timestamp,
                'type': 'ask',
                'price': ask[0],
                'volume': ask[1]
            })

        if time.time() - last_save_time >= 60:
            save_data(order_book_data, name_exchange, symbol)
            last_save_time = time.time()

        time.sleep(interval_in_seconds)

    save_data(order_book_data, name_exchange, symbol)


def save_data(data, name_exchange, symbol):
    today_date = datetime.today().strftime('%Y-%m-%d')
    df = pd.DataFrame(data)
    filename = f'OrderBookDataInternational/order_book_data_{name_exchange}_{today_date}_{symbol}.csv'

    df.to_csv(filename, mode='a', header=not pd.io.common.file_exists(filename), index=False)
    print(f"Data saved to {filename}")


def create_dataset(symbol, frm_ts, duration, interval, exchange, name_exchange):
    fetch_order_book_from_timestamp(symbol, frm_ts, duration, interval, exchange, name_exchange, name_exchange)


def run_symbols_in_parallel(symbols, frm_ts, duration, interval):
    exchanges = {
        'kucoin': ccxt.kucoin(),
        'okx': ccxt.okx(),
        'coinex': ccxt.coinex(),
        'binance': ccxt.binance(),
    }

    with ThreadPoolExecutor() as executor:
        futures = []
        for symbol, symbol_map in symbols.items():
            for name_exchange, exchange in exchanges.items():
                if name_exchange in symbol_map:
                    futures.append(
                        executor.submit(create_dataset, symbol_map[name_exchange], frm_ts, duration, interval, exchange,
                                        name_exchange)
                    )
        results = [future.result() for future in futures]
    return results


# Symbols mapping for both BTCUSDT and ETHUSDT
symbols = {
    'BTCUSDT': {
        'kucoin': 'BTC-USDT',
        'okx': 'BTC-USDT',
        'coinex': 'BTCUSDT',
        'binance': 'BTCUSDT'
    },
    'ETHUSDT': {
        'kucoin': 'ETH-USDT',
        'okx': 'ETH-USDT',
        'coinex': 'ETHUSDT',
        'binance': 'ETHUSDT'
    }
}

# Parameters
frm_ts = 1698926896
duration = 20000
interval = 15

run_symbols_in_parallel(symbols, frm_ts, duration, interval)
