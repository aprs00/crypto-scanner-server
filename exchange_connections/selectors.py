from exchange_connections.models import Kline1m


def get_exchange_symbols(exchange="binance", contract_type="perpetual"):
    """Return a list of unique symbols from Kline1m where exchange is 'binance'."""
    return list(
        Kline1m.objects.filter(exchange=exchange, contract_type=contract_type)
        .order_by("symbol")
        .distinct("symbol")
        .values_list("symbol", flat=True)
    )


def get_latest_kline_values(exchange="binance", contract_type="perpetual"):
    return (
        Kline1m.objects.filter(exchange=exchange, contract_type=contract_type)
        .order_by("symbol", "-start_time")
        .distinct("symbol")
    )
