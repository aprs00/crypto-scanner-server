from exchange_connections.models import Kline1m


def get_exchange_symbols(exchange="binance", contract_type="perpetual"):
    """Return a list of unique symbols from Kline1m where exchange is 'binance'."""
    return list(
        Kline1m.objects.filter(
            exchange__name=exchange, contract_type__name=contract_type
        )
        .order_by("symbol__name")
        .distinct("symbol__name")
        .values_list("symbol__name", flat=True)
    )


def get_latest_kline_values(exchange="binance", contract_type="perpetual"):
    return (
        Kline1m.objects.filter(
            exchange__name=exchange, contract_type__name=contract_type
        )
        .select_related("symbol", "exchange", "contract_type")
        .order_by("symbol__name", "-start_time")
        .distinct("symbol__name")
    )
