from enum import Enum

invalid_params_error = {
    "error": "Invalid parameters",
    "code": "INVALID_PARAMS",
}


class RedisPubMessages(Enum):
    KLINE_SAVED_TO_DB = b"KLINE_SAVED_TO_DB"
    SYMBOL_DELISTED = b"SYMBOL_DELISTED"
    SYMBOL_ADDED = b"SYMBOL_ADDED"


tf_options = {
    "correlation": {
        "1h": 1,
        "4h": 4,
        # "12h": 12,
        # "1d": 24,
        # "3d": 72,
        # "7d": 168,
    },
    "zscore": {
        "1h": 1,
        "4h": 4,
        "12h": 12,
    },
    "average_price": {
        "1w": 7 * 24,
        "1M": 30 * 24,
        "3M": 90 * 24,
        "6M": 180 * 24,
    },
}
