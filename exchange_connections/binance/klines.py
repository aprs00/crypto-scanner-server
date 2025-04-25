from binance import ThreadedWebsocketManager, Client
from binance.enums import ContractType, KLINE_INTERVAL_1MINUTE
import time
import redis
from datetime import datetime
from django.utils import timezone
from exchange_connections.models import Kline1m
from decimal import Decimal
import threading
from django.db import transaction


from exchange_connections.constants import (
    redis_time_series_data_types,
    BinanceContractStatus,
)
from core.constants import RedisPubMessages

time_series_retention = str(1 * 60 * 60 * 1000)  # 1h in miliseconds


def ms_to_datetime(ms_timestamp):
    try:
        timestamp_sec = int(ms_timestamp) / 1000.0
        return datetime.fromtimestamp(timestamp_sec, tz=timezone.utc)
    except (ValueError, TypeError) as e:
        return None


class RedisManager:
    def __init__(self):
        self.r = redis.Redis(host="redis")
        self.pipeline = self.r.pipeline()

    def initialize_keys(self, symbols, exchange="binance"):
        for symbol in symbols:
            for data_type in redis_time_series_data_types:
                key = f"250ms:{exchange}:{symbol}:{data_type}"

                if not self.r.exists(key):
                    self.r.execute_command(
                        f"TS.CREATE {key} LABELS value_type volume type 250ms:{exchange}:data symbol {symbol} RETENTION {time_series_retention}"
                    )

    def store_symbol_data(
        self, symbol, timestamp, price, quote_volume, num_of_trades, should_store
    ):
        try:
            self.pipeline.execute_command(
                f"TS.MADD "
                f"250ms:{symbol}:price {timestamp} {price} "
                f"250ms:{symbol}:volume {timestamp} {quote_volume} "
                f"250ms:{symbol}:trades {timestamp} {num_of_trades}"
            )

            if should_store:
                self.pipeline.execute()

        except Exception as e:
            self.store_error(str(e))

    def store_error(self, error):
        self.r.execute_command(f"LPUSH error_log {str(error)}")

    def publish_message(self, message, data=""):
        self.r.publish(message, data)


class KlinesSocketManager:
    def __init__(self):
        self.twm = ThreadedWebsocketManager()
        self.r = RedisManager()
        self.stream_name = None
        self.symbols_executed = set()
        self.message_batch = []
        self.symbols = []
        self.symbols_count = 0

    def initialize(self):
        self.twm.start()

    def stop(self):
        self.twm.stop_socket(self.stream_name)

    def reconnect(self):
        self.stop()
        self.message_batch = []
        time.sleep(5)
        self.start()

    def fetch_futures_symbols(self):
        """Fetch all futures symbols from Binance API."""
        try:
            client = Client()
            exchange_info = client.futures_exchange_info()

            self.symbols = [
                symbol["symbol"]
                for symbol in exchange_info["symbols"]
                if symbol["contractType"] == ContractType.PERPETUAL.value.upper()
                and symbol["status"] == BinanceContractStatus.TRADING.value
            ]
            self.symbols_count = len(self.symbols)

            client.close_connection()
        except Exception as e:
            self.r.store_error(f"Error fetching futures symbols: {str(e)}")

    def start(self):
        self.fetch_futures_symbols()
        try:
            self.stream_name = self.twm.start_futures_multiplex_socket(
                callback=self.handle_message,
                streams=[
                    f"{symbol.lower()}@kline_{KLINE_INTERVAL_1MINUTE}"
                    for symbol in self.symbols
                ],
            )
        except Exception as e:
            self.r.store_error(str(e))

    def handle_message(self, msg):
        """
        {
            "e": "kline",                     # event type
            "E": 1499404907056,               # event time
            "s": "ETHBTC",                    # symbol
            "k": {
                "t": 1499404860000,           # start time of this bar
                "T": 1499404919999,           # end time of this bar
                "s": "ETHBTC",                # symbol
                "i": "1m",                    # interval
                "f": 77462,                   # first trade id
                "L": 77465,                   # last trade id
                "o": "0.10278577",            # open
                "c": "0.10278645",            # close
                "h": "0.10278712",            # high
                "l": "0.10278518",            # low
                "v": "17.47929838",           # volume
                "n": 4,                       # number of trades
                "x": false,                   # whether this bar is final
                "q": "1.79662878",            # quote volume
                "V": "2.34879839",            # volume of active buy
                "Q": "0.24142166",            # quote volume of active buy
                "B": "13279784.01349473"      # can be ignored
            }
        }
        """
        if self.is_message_error(msg):
            self.r.store_error(str(msg))
            self.reconnect()
            return

        msg_data = msg["data"]
        kline_data = msg_data["k"]

        if kline_data["x"]:
            self.message_batch.append(
                Kline1m(
                    start_time=ms_to_datetime(kline_data["t"]),
                    close_time=ms_to_datetime(kline_data["T"]),
                    symbol=kline_data["s"],
                    open=Decimal(kline_data["o"]),
                    high=Decimal(kline_data["h"]),
                    low=Decimal(kline_data["l"]),
                    close=Decimal(kline_data["c"]),
                    base_volume=Decimal(kline_data["v"]),
                    quote_volume=Decimal(kline_data["q"]),
                    taker_buy_base_volume=Decimal(kline_data["V"]),
                    taker_buy_quote_volume=Decimal(kline_data["Q"]),
                    number_of_trades=Decimal(kline_data["n"]),
                    exchange="binance",
                    contract_type=ContractType.PERPETUAL.value.lower(),
                )
            )

            if len(self.message_batch) == self.symbols_count:
                thread = threading.Thread(
                    target=self._save_batch_sync, args=(list(self.message_batch),)
                )
                thread.start()

                self.message_batch = []
                self.r.publish_message(
                    RedisPubMessages.KLINE_SAVED_TO_DB.value, kline_data["t"]
                )

        # if kline_data["s"] == "BTCUSDT":
        #     self.btc_message_counter += 1

        # should_store = self.btc_message_counter == 4

        # self.r.store_symbol_data(
        #     symbol=kline_data["s"],
        #     timestamp=msg_data["E"],
        #     price=kline_data["c"],
        #     quote_volume=kline_data["q"],
        #     num_of_trades=kline_data["n"],
        #     should_store=should_store,
        # )
        # print("rece")

        # if should_store:
        #     print("STORED")
        #     self.btc_message_counter = 0

    def _save_batch_sync(self, batch):
        with transaction.atomic():
            Kline1m.objects.bulk_create(batch)

    def main(self):
        self.initialize()
        self.start()
        self.twm.join()

    @staticmethod
    def is_message_error(msg):
        return "e" in msg and msg["e"] == "error"


def main():
    ksm = KlinesSocketManager()
    ksm.main()


"""
ts.range 250ms:BTCUSDT:sum - + +
TS.MRANGE - + FILTER symbol=BTCUSDT
TS.MRANGE - + FILTER aggregation_type=sum

ts.REVRANGE 250ms:BTCUSDT - + AGGREGATION sum 15000

ts.madd 250ms:trades:BTCUSDT 10000 10
"""
