import logging
from typing import List
from django.db import models
from django.utils import timezone
from datetime import timedelta

from correlations.models import CorrelationPairHistory
from exchange_connections.models import Symbol

logger = logging.getLogger(__name__)


def get_symbol_pair_correlation_history(
    symbol1_name: str,
    symbol2_name: str,
    data_type: str,
    hours: int,
    exchange: str = "binance",
    contract_type: str = "perpetual",
) -> List[List]:
    """
    Get historical correlation values for a specific symbol pair.

    Args:
        symbol1_name: First symbol name (e.g., 'BTCUSDT')
        symbol2_name: Second symbol name (e.g., 'ETHUSDT')
        data_type: Type of data (e.g., 'close', 'volume', 'trades')
        hours: Time window in hours
        exchange: Exchange name (default: 'binance')
        contract_type: Contract type (default: 'perpetual')

    Returns:
        List of lists where each item is [correlation_value, calculated_at_iso_string]
    """
    try:
        symbols = Symbol.objects.filter(
            name__in=[symbol1_name, symbol2_name],
            exchange__name=exchange,
            contract_type__name=contract_type,
        ).select_related("exchange", "contract_type")

        symbol_map = {s.name: s for s in symbols}

        if symbol1_name not in symbol_map or symbol2_name not in symbol_map:
            logger.error(
                f"One or both symbols not found: {symbol1_name}, {symbol2_name}"
            )
            return []

        symbol1 = symbol_map[symbol1_name]
        symbol2 = symbol_map[symbol2_name]

        time_threshold = timezone.now() - timedelta(hours=hours)

        correlations = (
            CorrelationPairHistory.objects.filter(
                data_type=data_type, hours=hours, calculated_at__gte=time_threshold
            )
            .filter(
                models.Q(symbol1=symbol1, symbol2=symbol2)
                | models.Q(symbol1=symbol2, symbol2=symbol1)
            )
            .order_by("-calculated_at")
        )

        return [
            [
                corr.calculated_at.isoformat(),
                corr.correlation_value,
            ]
            for corr in correlations
        ]

    except Exception as e:
        logger.error(
            f"Error getting correlation history for {symbol1_name}-{symbol2_name}: {e}",
            exc_info=True,
        )
        return []
