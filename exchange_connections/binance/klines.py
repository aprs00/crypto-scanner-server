from binance import ThreadedWebsocketManager, Client
from binance.enums import ContractType, KLINE_INTERVAL_1MINUTE
import time
import redis
import threading

from exchange_connections.constants import BinanceContractStatus
from core.constants import RedisPubMessages
from exchange_connections.services.klines_ingest import (
    build_model_from_ws,
    bulk_insert_klines,
)


class KlinesSocketManager:
    def __init__(self):
        self.twm = ThreadedWebsocketManager()
        self.r = redis.Redis(host="redis")
        self.stream_name = None
        self.symbols_executed = set()
        self.message_batch = []
        self.symbols = []
        self.symbols_count = 0

    def store_error(self, error):
        self.r.execute_command(f"LPUSH error_log {str(error)}")

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
            self.store_error(f"Error fetching futures symbols: {str(e)}")

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
            self.store_error(str(e))

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
            self.store_error(str(msg))
            self.reconnect()
            return

        msg_data = msg["data"]
        kline_data = msg_data["k"]

        if kline_data.get("x"):
            self.message_batch.append(kline_data)

            if len(self.message_batch) == self.symbols_count:
                batch_copy = list(self.message_batch)
                self.message_batch = []
                thread = threading.Thread(
                    target=self._save_batch_sync, args=(batch_copy,)
                )
                thread.start()

                self.r.publish(
                    RedisPubMessages.KLINE_SAVED_TO_DB.value, kline_data["t"]
                )

    def _save_batch_sync(self, batch):
        try:
            models = [
                build_model_from_ws(
                    kline_dict,
                    exchange="binance",
                    contract_type=ContractType.PERPETUAL.value.lower(),
                )
                for kline_dict in batch
            ]

            bulk_insert_klines(models, chunk_size=len(models) or 1)
        except Exception as e:
            self.store_error(f"kline_batch_save_error: {e}")

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
