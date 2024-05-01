from binance import ThreadedWebsocketManager

import time
import redis

from crypto_scanner.constants import test_socket_symbols, redis_time_series_retention


class RedisManager:
    def __init__(self):
        self.r = redis.Redis(host="redis", port=6379, decode_responses=True)

    def initialize_keys(self, retention=redis_time_series_retention):
        for symbol in test_socket_symbols:
            if not self.r.exists(f"1s:volume:{symbol}"):
                self.r.execute_command(
                    f"TS.CREATE 1s:volume:{symbol} LABELS value_type volume type binance_1s_data symbol {symbol} RETENTION {retention}"
                )
            if not self.r.exists(f"1s:price:{symbol}"):
                self.r.execute_command(
                    f"TS.CREATE 1s:price:{symbol} LABELS value_type price type binance_1s_data symbol {symbol} RETENTION {retention}"
                )
            if not self.r.exists(f"1s:trades:{symbol}"):
                self.r.execute_command(
                    f"TS.CREATE 1s:trades:{symbol} LABELS value_type trades type binance_1s_data symbol {symbol} RETENTION {retention}"
                )

    def store_symbol_data(self, symbol, timestamp, price, quote_volume, num_of_trades):
        try:
            self.r.execute_command(
                f"TS.MADD "
                f"1s:price:{symbol} {timestamp} {price} "
                f"1s:volume:{symbol} {timestamp} {quote_volume} "
                f"1s:trades:{symbol} {timestamp} {num_of_trades}"
            )
        except Exception as e:
            self.store_error(str(e))

        # self.store_data_counter += 1
        #
        # try:
        #     pipe = self.r.ts().pipeline()
        #
        #     pipe.execute_command(
        #         f"TS.MADD "
        #         f"1s:price:{symbol} {timestamp} {price} "
        #         f"1s:volume:{symbol} {timestamp} {quote_volume} "
        #         f"1s:trades:{symbol} {timestamp} {num_of_trades}"
        #     )
        #
        #     if self.store_data_counter >= len(test_socket_symbols):
        #         print(self.store_data_counter)
        #         pipe.execute()
        #         self.store_data_counter = 0
        #
        # except Exception as e:
        #     self.store_error(str(e))

    def store_error(self, error):
        self.r.execute_command(f"LPUSH error_log {str(error)}")


class KlinesSocketManager:
    def __init__(self):
        self.twm = ThreadedWebsocketManager()
        self.r = RedisManager()
        self.stream_name = None

    def initialize(self):
        self.twm.start()

    def stop(self):
        self.twm.stop_socket(self.stream_name)

    def reconnect(self):
        self.stop()
        time.sleep(5)
        self.start()

    def start(self):
        streams = [f"{symbol.lower()}@kline_1s" for symbol in test_socket_symbols]

        try:
            self.stream_name = self.twm.start_multiplex_socket(
                callback=self.handle_message, streams=streams
            )
        except Exception as e:
            self.r.store_error(str(e))

    def handle_message(self, msg):
        if self.is_message_error(msg):
            self.r.store_error(str(msg))
            self.reconnect()

        symbol, timestamp, price, quote_volume, num_of_trades = (
            self.extract_message_data(msg)
        )

        self.r.store_symbol_data(symbol, timestamp, price, quote_volume, num_of_trades)

    def main(self):
        self.r.initialize_keys()
        self.initialize()
        self.start()
        self.twm.join()

    @staticmethod
    def is_message_error(msg):
        if "e" in msg and msg["e"] == "error":
            return True
        return False

    @staticmethod
    def extract_message_data(msg):
        data = msg["data"]["k"]
        symbol = data["s"]
        quote_volume = float(data["q"])
        price = float(data["c"])
        num_of_trades = data["n"]
        timestamp = data["t"]

        return symbol, timestamp, price, quote_volume, num_of_trades


def main():
    ksm = KlinesSocketManager()
    ksm.main()


if __name__ == "__main__":
    main()

"""
ts.range 1s:BTCUSDT:sum - + +
TS.MRANGE - + FILTER symbol=BTCUSDT
TS.MRANGE - + FILTER aggregation_type=sum

ts.REVRANGE 1s:BTCUSDT - + AGGREGATION sum 15000

ts.madd 1s:trades:BTCUSDT 10000 10
"""
