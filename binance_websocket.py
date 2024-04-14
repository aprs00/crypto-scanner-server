from binance import ThreadedWebsocketManager

import time
import redis
import dotenv

dotenv.load_dotenv()

from crypto_scanner.constants import (
    test_socket_symbols,
)

r = redis.Redis(host="redis", port=6379, decode_responses=True)


def create_redis_keys(retention="900000"):
    for symbol in test_socket_symbols:
        if not r.exists(f"1s:volume:{symbol}"):
            r.execute_command(
                f"TS.CREATE 1s:volume:{symbol} LABELS value_type volume type binance_1s_data symbol {symbol} RETENTION {retention}"
            )
        if not r.exists(f"1s:price:{symbol}"):
            r.execute_command(
                f"TS.CREATE 1s:price:{symbol} LABELS value_type price type binance_1s_data symbol {symbol} RETENTION {retention}"
            )
        if not r.exists(f"1s:trades:{symbol}"):
            r.execute_command(
                f"TS.CREATE 1s:trades:{symbol} LABELS value_type trades type binance_1s_data symbol {symbol} RETENTION {retention}"
            )


def store_socket_data_to_redis(msg):
    data = msg["data"]["k"]
    symbol = data["s"]
    quote_volume = float(data["q"])
    price = float(data["c"])
    num_of_trades = data["n"]
    timestamp = data["t"]

    r.execute_command(f"ts.add 1s:volume:{symbol} {timestamp} {quote_volume}")
    r.execute_command(f"ts.add 1s:price:{symbol} {timestamp} {price}")
    r.execute_command(f"ts.add 1s:trades:{symbol} {timestamp} {num_of_trades}")


def main():
    twm = ThreadedWebsocketManager()
    twm.start()

    create_redis_keys()

    streams = [f"{symbol.lower()}@kline_1s" for symbol in test_socket_symbols]

    while True:
        try:
            twm.start_multiplex_socket(
                callback=store_socket_data_to_redis, streams=streams
            )
            twm.join()
        except Exception as e:
            print(e)
            twm.stop()
            time.sleep(8)
            main()


if __name__ == "__main__":
    main()

"""
ts.range 1s:BTCUSDT:sum - + +
TS.MRANGE - + FILTER symbol=BTCUSDT
TS.MRANGE - + FILTER aggregation_type=sum

ts.REVRANGE 1s:BTCUSDT - + AGGREGATION sum 15000

ts.madd 1s:trades:BTCUSDT 10000 10
"""
