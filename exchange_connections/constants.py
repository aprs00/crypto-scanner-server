from enum import Enum

from core.constants import Exchange

KLINE_FIELD_MAP = {
    "price": "close",
    "volume": "base_volume",
    "trades": "number_of_trades",
}


def get_btc_symbol(exchange: str) -> str:
    """Get the BTC symbol for a given exchange."""
    if exchange == Exchange.BINANCE:
        return "BTCUSDT"
    elif exchange == Exchange.BYBIT:
        return "BTCUSDT"
    # Hyperliquid
    return "BTC"


def get_sol_symbol(exchange: str) -> str:
    """Get the SOL symbol for a given exchange."""
    if exchange == Exchange.BINANCE:
        return "SOLUSDT"
    elif exchange == Exchange.BYBIT:
        return "SOLUSDT"
    # Hyperliquid
    return "SOL"


class BinanceContractStatus(Enum):
    PENDING_TRADING = "PENDING_TRADING"
    TRADING = "TRADING"
    PRE_DELIVERING = "PRE_DELIVERING"
    DELIVERING = "DELIVERING"
    DELIVERED = "DELIVERED"
    PRE_SETTLE = "PRE_SETTLE"
    SETTLING = "SETTLING"
    CLOSE = "CLOSE"
