from enum import Enum

invalid_params_error = {
    "error": "Invalid parameters",
    "code": "INVALID_PARAMS",
}


class Exchange(str, Enum):
    BINANCE = "binance"
    HYPERLIQUID = "hyperliquid"
    BYBIT = "bybit"

    def __str__(self):
        return self.value


class RedisStreamKeys:
    """Redis Stream keys for persistent message delivery."""

    @staticmethod
    def klines(exchange: Exchange) -> str:
        """Stream key for kline updates."""
        return f"klines:{exchange}:stream"

    @staticmethod
    def symbols(exchange: Exchange) -> str:
        """Stream key for symbol add/delist events."""
        return f"symbols:{exchange}:stream"

    @staticmethod
    def consumer_group(service: str, exchange: Exchange) -> str:
        """Consumer group name for a service."""
        return f"{service}:{exchange}"


# Per-exchange configuration
EXCHANGE_CONFIG = {
    Exchange.BINANCE: {
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
            "zscore": {"1h": 1, "4h": 4, "12h": 12},
            "average_price": {"1w": 168, "1M": 720, "3M": 2160},
        },
    },
    Exchange.HYPERLIQUID: {
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
            "zscore": {"1h": 1, "4h": 4, "12h": 12},
            "average_price": {"1w": 168, "1M": 720},
        },
    },
    Exchange.BYBIT: {
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
            "zscore": {"1h": 1, "4h": 4, "12h": 12},
            "average_price": {"1w": 168, "1M": 720, "3M": 2160},
        },
    },
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
