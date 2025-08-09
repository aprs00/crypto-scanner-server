from exchange_connections.models import Kline1m, Symbol


def get_exchange_symbols(exchange="binance", contract_type="perpetual"):
    return list(
        Symbol.objects.filter(
            exchange__name=exchange, contract_type__name=contract_type
        )
        .order_by("name")
        .values_list("name", flat=True)
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
