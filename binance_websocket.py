from binance import ThreadedWebsocketManager

import redis
import dotenv

dotenv.load_dotenv()

from crypto_scanner.constants import (
    test_socket_symbols,
)

r = redis.Redis(host="redis", port=6379, decode_responses=True)


def create_redis_keys(retention):
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


def main():
    twm = ThreadedWebsocketManager()
    twm.start()

    retention = "900000"

    create_redis_keys(retention=retention)

    def handle_socket_message_test(msg):
        data = msg["data"]["k"]
        symbol = data["s"]
        quote_volume = float(data["q"])
        price = float(data["c"])
        num_of_trades = data["n"]
        timestamp = data["t"]

        r.execute_command(f"ts.add 1s:volume:{symbol} {timestamp} {quote_volume}")
        r.execute_command(f"ts.add 1s:price:{symbol} {timestamp} {price}")
        r.execute_command(f"ts.add 1s:trades:{symbol} {timestamp} {num_of_trades}")

    streams2 = [f"{symbol.lower()}@kline_1s" for symbol in test_socket_symbols]
    twm.start_multiplex_socket(callback=handle_socket_message_test, streams=streams2)

    twm.join()


if __name__ == "__main__":
    main()

"""
ts.range 1s:BTCUSDT:sum - + +
TS.MRANGE - + FILTER symbol=BTCUSDT
TS.MRANGE - + FILTER aggregation_type=sum

ts.REVRANGE 1s:BTCUSDT - + AGGREGATION sum 15000

ts.madd 1s:trades:BTCUSDT 10000 10
"""
