from enum import Enum

invalid_params_error = {
    "error": "Invalid parameters",
    "code": "INVALID_PARAMS",
}


class RedisPubMessages(Enum):
    KLINE_SAVED_TO_DB = b"KLINE_SAVED_TO_DB"
