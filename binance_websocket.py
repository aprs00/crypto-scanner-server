from binance import ThreadedWebsocketManager
import redis

import time

from crypto_scanner.constants import tickers as symbols

r = redis.Redis(host="redis", port=6379, decode_responses=True)

aggregation_types = ["avg", "sum", "std.p", "std.s", "var.p", "var.s", "twa"]

""""
r.execute_command(f"TS.CREATE 1s_candles:BTCUSDT RETENTION {retention}")
r.execute_command(
    f"TS.CREATE 1s_candles:BTCUSDT:sum RETENTION {retention} LABELS symbol BTCUSDT aggregation_type sum"
)

r.execute_command(f"TS.CREATE 1s_candles:ETHUSDT RETENTION {retention}")
r.execute_command(
    f"TS.CREATE 1s_candles:ETHUSDT:sum RETENTION {retention} LABELS symbol ETHUSDT aggregation_type sum"
)

r.execute_command(
    f"TS.CREATERULE 1s_candles:BTCUSDT 1s_candles:BTCUSDT:sum AGGREGATION sum 15000"
)
r.execute_command(
    f"TS.CREATERULE 1s_candles:ETHUSDT 1s_candles:ETHUSDT:sum AGGREGATION sum 15000"
)
"""


def create_keys(retention, bucket_size):
    for symbol in symbols:
        if not r.exists(f"1s_candles:{symbol}"):
            r.execute_command(f"TS.CREATE 1s_candles:{symbol} RETENTION {retention}")
        for aggregation_type in aggregation_types:
            if r.exists(f"1s_candles:{symbol}:{aggregation_type}"):
                r.execute_command(
                    f"TS.CREATE 1s_candles:{symbol}:{aggregation_type} RETENTION {retention} LABELS symbol {symbol} aggregation_type {aggregation_type}"
                )
                r.execute_command(
                    f"TS.CREATERULE 1s_candles:{symbol} 1s_candles:{symbol}:{aggregation_type} AGGREGATION {aggregation_type} {bucket_size}"
                )


def main():
    twm = ThreadedWebsocketManager()
    twm.start()

    retention = "900000"
    bucket_size = "15000"

    create_keys(retention=retention, bucket_size=bucket_size)

    def handle_socket_message(msg):
        data = msg["data"]["k"]
        symbol = data["s"]
        volume = float(data["v"])
        timestamp = data["t"]

        r.execute_command(
            f"ts.add 1s_candles:{symbol} {timestamp} {volume} LABELS symbol {symbol}"
        )

        # btcusdt_data = r.execute_command(f"TS.MGET - + FILTER symbol=BTCUSDT")

        # for data in btcusdt_data:
        #     aggregation_type = data[0].split(":")[2]
        #     value = data[2][1]
        #     print(aggregation_type, value)

        # print(btcusdt_data)

    # Streams to subscribe to
    # streams = ["btcusdt@kline_1s", "ethusdt@kline_1s"]
    streams = [f"{symbol.lower()}usdt@kline_1s" for symbol in symbols]
    twm.start_multiplex_socket(callback=handle_socket_message, streams=streams)

    twm.join()


"""
ts.range 1s_candles:BTCUSDT:sum - + +
TS.MRANGE - + FILTER symbol=BTCUSDT
TS.MRANGE - + FILTER aggregation_type=sum
"""
if __name__ == "__main__":
    main()

# {
#    "stream":"btcusdt@kline_1s",
#    "data":{
#       "e":"kline",
#       "E":1695934650002,
#       "s":"BTCUSDT",
#       "k":{
#          "t":1695934649000,
#          "T":1695934649999,
#          "s":"BTCUSDT",
#          "i":"1s",
#          "f":3222854516,
#          "L":3222854516,
#          "o":"27080.36000000",
#          "c":"27080.36000000",
#          "h":"27080.36000000",
#          "l":"27080.36000000",
#          "v":"0.00384000",
#          "n":1,
#          "x":true,
#          "q":"103.98858240",
#          "V":"0.00000000",
#          "Q":"0.00000000",
#          "B":"0"
#       }
#    }
# }
