from binance import ThreadedWebsocketManager

import time
import redis

from crypto_scanner.constants import test_socket_symbols, redis_time_series_retention


class RedisManager:
    def __init__(self):
        self.r = redis.Redis(host="redis", port=6379, decode_responses=True)
        self.pipeline = self.r.pipeline()

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

    def store_symbol_data(
        self, symbol, timestamp, price, quote_volume, num_of_trades, should_store
    ):
        try:
            self.pipeline.execute_command(
                f"TS.MADD "
                f"1s:price:{symbol} {timestamp} {price} "
                f"1s:volume:{symbol} {timestamp} {quote_volume} "
                f"1s:trades:{symbol} {timestamp} {num_of_trades}"
            )

            if should_store:
                self.pipeline.execute()
                self.r.publish("test_socket_symbols_stored", "")

        except Exception as e:
            self.store_error(str(e))

    def store_error(self, error):
        self.r.execute_command(f"LPUSH error_log {str(error)}")

    def lock(self):
        self.r.set("my_lock", "False")

    def is_locked(self):
        return self.r.get("my_lock") == "True"


class KlinesSocketManager:
    def __init__(self):
        self.twm = ThreadedWebsocketManager()
        self.r = RedisManager()
        self.stream_name = None
        self.symbols_executed = set()

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

        self.symbols_executed.add(symbol)
        all_symbols_executed = len(self.symbols_executed) >= len(test_socket_symbols)
        if all_symbols_executed:
            self.symbols_executed = set()

        self.r.store_symbol_data(
            symbol, timestamp, price, quote_volume, num_of_trades, all_symbols_executed
        )

    def main(self):
        if self.r.is_locked():
            return

        self.r.lock()
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


"""
ts.range 1s:BTCUSDT:sum - + +
TS.MRANGE - + FILTER symbol=BTCUSDT
TS.MRANGE - + FILTER aggregation_type=sum

ts.REVRANGE 1s:BTCUSDT - + AGGREGATION sum 15000

ts.madd 1s:trades:BTCUSDT 10000 10
"""
