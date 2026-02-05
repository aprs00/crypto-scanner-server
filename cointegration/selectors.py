import logging
from datetime import timedelta

from django.db import models
from django.utils import timezone

from cointegration.models import CointegrationPairHistory
from exchange_connections.models import Symbol

logger = logging.getLogger(__name__)


def get_cointegration_pair_history(
    *,
    exchange: str,
    contract_type: str,
    base_symbol: str,
    comparison_symbols: list[str],
    window_minutes: int,
    hours: int,
):
    if not base_symbol:
        return [[] for _ in comparison_symbols]

    symbol_objects = Symbol.objects.filter(
        name__in=[base_symbol, *comparison_symbols],
        exchange__name=exchange,
        contract_type__name=contract_type,
    ).select_related("exchange", "contract_type")

    symbol_map = {symbol.name: symbol for symbol in symbol_objects}

    if base_symbol not in symbol_map:
        logger.warning(
            "Cointegration history base symbol not found: %s (%s/%s)",
            base_symbol,
            exchange,
            contract_type,
        )
        return [[] for _ in comparison_symbols]

    symbol1 = symbol_map[base_symbol]

    time_threshold = timezone.now() - timedelta(hours=hours)

    pair_filter = models.Q()
    for symbol2_name in comparison_symbols:
        symbol2 = symbol_map.get(symbol2_name)
        if not symbol2:
            continue
        if symbol1.id < symbol2.id:  # type: ignore[operator]
            pair_filter |= models.Q(symbol1=symbol1, symbol2=symbol2)
        else:
            pair_filter |= models.Q(symbol1=symbol2, symbol2=symbol1)

    if not pair_filter:
        return [[] for _ in comparison_symbols]

    rows = (
        CointegrationPairHistory.objects.filter(
            exchange__name=exchange,
            contract_type__name=contract_type,
            window_minutes=window_minutes,
            calculated_at__gte=time_threshold,
        )
        .filter(pair_filter)
        .order_by("calculated_at")
        .values(
            "symbol1_id",
            "symbol2_id",
            "calculated_at",
            "spread_z",
            "half_life",
            "adf_t",
            "hedge_ratio",
            "spread_std",
        )
    )

    id_to_name = {symbol.id: symbol.name for symbol in symbol_map.values()}
    results_by_pair = {symbol2_name: [] for symbol2_name in comparison_symbols}

    for row in rows:
        symbol1_id = row["symbol1_id"]
        symbol2_id = row["symbol2_id"]

        if symbol1_id == symbol1.id:  # type: ignore[operator]
            symbol2_name = id_to_name.get(symbol2_id)
        elif symbol2_id == symbol1.id:  # type: ignore[operator]
            symbol2_name = id_to_name.get(symbol1_id)
        else:
            continue

        if symbol2_name in results_by_pair:
            results_by_pair[symbol2_name].append(row)

    return [results_by_pair.get(symbol2_name, []) for symbol2_name in comparison_symbols]
