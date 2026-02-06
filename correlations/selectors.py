import logging
from typing import Dict, List, Optional
from django.db import models
from django.db import connection
from django.utils import timezone
from datetime import datetime, timedelta, timezone as dt_timezone
import numpy as np

from correlations.models import CorrelationPairHistory
from exchange_connections.constants import KLINE_FIELD_MAP
from exchange_connections.models import Symbol

logger = logging.getLogger(__name__)


def get_historical_correlation_matrix(
    hours: int,
    symbols: List[str],
    symbol_to_idx: Dict[str, int],
    exchange: str,
    contract_type: str,
    data_type: str,
    end_time: Optional[datetime] = None,
) -> np.ndarray:
    """Fetch [symbols, minutes] matrix for one data type with NaN gaps."""
    n = len(symbols)
    total_minutes = max(hours, 0) * 60
    matrix = np.full((n, total_minutes), np.nan, dtype=np.float64)
    if n == 0 or total_minutes == 0:
        return matrix

    value_column = KLINE_FIELD_MAP.get(data_type)
    if value_column is None:
        return matrix

    if end_time is None:
        end_time = datetime.now(dt_timezone.utc)
    elif end_time.tzinfo is None:
        end_time = end_time.replace(tzinfo=dt_timezone.utc)
    end_time = end_time.replace(second=0, microsecond=0)
    start_time = end_time - timedelta(hours=hours)
    start_time_utc = start_time.astimezone(dt_timezone.utc)
    end_time_utc = end_time.astimezone(dt_timezone.utc)

    symbol_placeholders = ",".join(["%s"] * n)
    query = f"""
        SELECT
            s.name AS symbol_name,
            k.start_time,
            k.{value_column}
        FROM cs_klines_1m k
        JOIN cs_symbols s ON k.symbol_id = s.id
        JOIN cs_exchanges e ON k.exchange_id = e.id
        JOIN cs_contract_types ct ON s.contract_type_id = ct.id
        WHERE
            e.name = %s
            AND ct.name = %s
            AND s.name IN ({symbol_placeholders})
            AND k.start_time >= %s
            AND k.start_time < %s
        ORDER BY s.name, k.start_time
    """

    with connection.cursor() as cursor:
        params = [exchange, contract_type] + symbols + [start_time_utc, end_time_utc]
        cursor.execute(query, params)
        for symbol_name, row_time, row_value in cursor.fetchall():
            idx = symbol_to_idx.get(symbol_name)
            if idx is None or row_value is None:
                continue
            minute_idx = int((row_time - start_time_utc).total_seconds() // 60)
            if 0 <= minute_idx < total_minutes:
                matrix[idx, minute_idx] = float(row_value)

    return matrix


def get_symbol_pair_correlation_history(
    base_symbol: str,
    comparison_symbols: List[str],
    data_type: str,
    hours: int,
    exchange: str,
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
