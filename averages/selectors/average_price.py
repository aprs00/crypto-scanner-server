from exchange_connections.models import Kline1m, Symbol


def get_average_symbol_data(symbol, exchange, start_time_utc, group_by, contract_type):
    symbol_obj = Symbol.objects.get(
        name=symbol,
        exchange__name=exchange,
        contract_type__name=contract_type,
    )

    klines = (
        Kline1m.objects.filter(symbol=symbol_obj, start_time__gte=start_time_utc)
        .values("start_time", "open", "close")
        .order_by("start_time")
    )

    return list(klines)
