from exchange_connections.models import Kline1m, Symbol


def get_exchange_symbols(exchange="binance", contract_type="perpetual"):
    return list(
        Symbol.objects.filter(
            exchange__name=exchange,
            contract_type__name=contract_type,
        )
        .order_by("name")
        .distinct("name")
        .values_list("name", flat=True)
    )
