import logging
from typing import List
from django.db import models
from django.utils import timezone
from datetime import timedelta

from correlations.models import CorrelationPairHistory
from exchange_connections.models import Symbol
from core.constants import Exchange

logger = logging.getLogger(__name__)


def get_symbol_pair_correlation_history(
    base_symbol: str,
    comparison_symbols: List[str],
    data_type: str,
    hours: int,
    exchange: str = Exchange.BINANCE,
    contract_type: str = "perpetual",
) -> List[List[List]]:
    """
    Efficiently get historical correlation values for one symbol vs multiple others.
    Uses a single DB query and groups results in Python.
    """
    try:
        symbol_objects = Symbol.objects.filter(
            name__in=[base_symbol, *comparison_symbols],
            exchange__name=exchange,
            contract_type__name=contract_type,
        ).select_related("exchange", "contract_type")

        symbol_map = {s.name: s for s in symbol_objects}
        if base_symbol not in symbol_map:
            logger.error(f"Symbol not found: {base_symbol}")
            return []

        symbol1 = symbol_map[base_symbol]
        time_threshold = timezone.now() - timedelta(hours=hours)

        q = models.Q()
        for sym2_name in comparison_symbols:
            sym2 = symbol_map.get(sym2_name)
            if not sym2:
                continue
            if symbol1.id < sym2.id:  # type: ignore
                q |= models.Q(symbol1=symbol1, symbol2=sym2)
            else:
                q |= models.Q(symbol1=sym2, symbol2=symbol1)

        if not q:
            logger.warning("No valid symbol2s found")
            return [[] for _ in comparison_symbols]

        correlations = (
            CorrelationPairHistory.objects.filter(
                data_type=data_type,
                hours=1,
                calculated_at__gte=time_threshold,
            )
            .filter(q)
            .order_by("-calculated_at")
            .values("symbol1_id", "symbol2_id", "calculated_at", "correlation_value")
        )

        results_by_pair = {sym2_name: [] for sym2_name in comparison_symbols}

        for corr in correlations:
            s1_id, s2_id = corr["symbol1_id"], corr["symbol2_id"]

            if s1_id == symbol1.id:  # type: ignore
                sym2 = next(
                    (name for name, s in symbol_map.items() if s.id == s2_id), None  # type: ignore
                )
            elif s2_id == symbol1.id:  # type: ignore
                sym2 = next(
                    (name for name, s in symbol_map.items() if s.id == s1_id), None  # type: ignore
                )
            else:
                continue

            if sym2 in results_by_pair:
                results_by_pair[sym2].append(
                    [
                        corr["calculated_at"].isoformat(),
                        round(corr["correlation_value"], 3),
                    ]
                )

        return [results_by_pair.get(sym2_name, []) for sym2_name in comparison_symbols]

    except Exception as e:
        logger.error(
            f"Error getting correlation history for {base_symbol} vs {comparison_symbols}: {e}",
            exc_info=True,
        )
        return []
