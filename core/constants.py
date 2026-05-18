from enum import Enum

invalid_params_error = {
    "error": "Invalid parameters",
    "code": "INVALID_PARAMS",
}


class Exchange(str, Enum):
    BINANCE = "binance"
    HYPERLIQUID = "hyperliquid"
    BYBIT = "bybit"
    OKX = "okx"

    def __str__(self):
        return self.value


EXCHANGE_CONFIG = {
    Exchange.BINANCE: {
        "active": True,
        "name": "Binance",
        "data_types": ["price", "volume", "trades"],
        "hours_options": {
            "correlation": {
                "1h": 1,
                "4h": 4,
                "12h": 12,
                "1d": 24,
                "3d": 72,
                "7d": 168,
                "14d": 336,
            },
            "correlation_pair": {"1h": 1, "4h": 4},
            "cointegration_pair": {"4h": 4, "8h": 8},
            "zscore": {"1h": 1, "4h": 4, "12h": 12},
            "average_price": {"1w": 168, "1M": 720, "3M": 2160},
        },
    },
    Exchange.HYPERLIQUID: {
        "active": True,
        "name": "Hyperliquid",
        "data_types": ["price", "volume", "trades"],
        "hours_options": {
            "correlation": {
                "1h": 1,
                "4h": 4,
                "12h": 12,
                "1d": 24,
                "3d": 72,
                "7d": 168,
                "14d": 336,
            },
            "correlation_pair": {"1h": 1, "4h": 4},
            "cointegration_pair": {"4h": 4, "8h": 8},
            "zscore": {"1h": 1, "4h": 4, "12h": 12},
            "average_price": {"1w": 168, "1M": 720},
        },
    },
    Exchange.BYBIT: {
        "active": True,
        "name": "Bybit",
        "data_types": ["price", "volume"],
        "hours_options": {
            "correlation": {
                "1h": 1,
                "4h": 4,
                "12h": 12,
                "1d": 24,
                "3d": 72,
                "7d": 168,
                "14d": 336,
            },
            "correlation_pair": {"1h": 1, "4h": 4},
            "cointegration_pair": {"4h": 4, "8h": 8},
            "zscore": {"1h": 1, "4h": 4, "12h": 12},
            "average_price": {"1w": 168, "1M": 720, "3M": 2160},
        },
    },
    Exchange.OKX: {
        "active": True,
        "name": "OKX",
        "data_types": ["price", "volume"],
        "hours_options": {
            "correlation": {
                "1h": 1,
                "4h": 4,
                "12h": 12,
                "1d": 24,
                "3d": 72,
                "7d": 168,
                "14d": 336,
            },
            "correlation_pair": {"1h": 1, "4h": 4},
            "cointegration_pair": {"4h": 4, "8h": 8},
            "zscore": {"1h": 1, "4h": 4, "12h": 12},
            "average_price": {"1w": 168, "1M": 720, "3M": 2160},
        },
    },
}

ACTIVE_EXCHANGES = {
    exchange for exchange, config in EXCHANGE_CONFIG.items() if config.get("active", True)
}

# Collect all unique timeframe hours across all exchanges
TIMEFRAME_HOURS = sorted(
    set().union(
        *[
            set(config["hours_options"]["zscore"].values())
            | set(config["hours_options"]["correlation"].values())
            for config in EXCHANGE_CONFIG.values()
        ]
    )
)
