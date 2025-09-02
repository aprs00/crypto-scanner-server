from enum import Enum

KLINE_FIELD_MAP = {
    "price": "close",
    "volume": "base_volume",
    "trades": "number_of_trades",
}


class BinanceContractStatus(Enum):
    PENDING_TRADING = "PENDING_TRADING"
    TRADING = "TRADING"
    PRE_DELIVERING = "PRE_DELIVERING"
    DELIVERING = "DELIVERING"
    DELIVERED = "DELIVERED"
    PRE_SETTLE = "PRE_SETTLE"
    SETTLING = "SETTLING"
    CLOSE = "CLOSE"


tickers = [
    "BTCUSDT",
    "ETHUSDT",
    "XRPUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "DOTUSDT",
    "AVAXUSDT",
    "ADAUSDT",
    "WIFUSDT",
    "SUIUSDT",
    "DOGEUSDT",
    "LTCUSDT",
    "TRXUSDT",
    "LINKUSDT",
    "BCHUSDT",
    "SHIBUSDT",
]
