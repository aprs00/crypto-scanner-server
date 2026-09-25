import logging
from typing import Dict, List, Optional, Sequence
from django.db import models
from django.db import connection, transaction
from django.utils import timezone
from datetime import datetime, timedelta, timezone as dt_timezone
import numpy as np

from correlations.models import CorrelationPairHistory
from exchange_connections.constants import KLINE_FIELD_MAP
from exchange_connections.models import Symbol

logger = logging.getLogger(__name__)


def get_historical_correlation_matrices(
    hours: int,
    symbols: List[str],
    symbol_to_idx: Dict[str, int],
    exchange: str,
    contract_type: str,
    data_types: Sequence[str],
    end_time: Optional[datetime] = None,
) -> Dict[str, np.ndarray]:
    """Stream aligned candle matrices without a large SQL sort or result buffer."""
    n = len(symbols)
    total_minutes = max(hours, 0) * 60
    matrices = {
        data_type: np.full((n, total_minutes), np.nan, dtype=np.float64)
        for data_type in data_types
    }
    if n == 0 or total_minutes == 0 or not data_types:
        return matrices

    columns = [KLINE_FIELD_MAP[data_type] for data_type in data_types]

    if end_time is None:
        end_time = datetime.now(dt_timezone.utc)
    elif end_time.tzinfo is None:
        end_time = end_time.replace(tzinfo=dt_timezone.utc)
    end_time = end_time.replace(second=0, microsecond=0)
    start_time = end_time - timedelta(hours=hours)
    start_time_utc = start_time.astimezone(dt_timezone.utc)
    end_time_utc = end_time.astimezone(dt_timezone.utc)

    # One database snapshot keeps price and volume aligned even if a backfill
    # updates historical candles during initialization.
    with transaction.atomic():
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute(
                """
                SELECT s.id, s.name, s.exchange_id
                FROM cs_symbols s
                JOIN cs_exchanges e ON s.exchange_id = e.id
                JOIN cs_contract_types ct ON s.contract_type_id = ct.id
                WHERE e.name = %s AND ct.name = %s AND s.name = ANY(%s)
                """,
                [exchange, contract_type, symbols],
            )
            records = cursor.fetchall()
            if not records:
                return matrices

            id_to_idx = {
                record[0]: symbol_to_idx[record[1]]
                for record in records
                if record[1] in symbol_to_idx
            }
            exchange_id = records[0][2]
            value_columns = ", ".join(f"k.{column}" for column in columns)
            matrix_list = [matrices[data_type] for data_type in data_types]

            # The time-leading index and a sequential scan can each serve this
            # range without revisiting the table for every symbol. A named
            # cursor keeps each FETCH small under the connection's timeout.
            with connection.connection.cursor(name="correlation_history") as stream:
                stream.execute(
                    f"""
                    SELECT k.symbol_id, k.start_time, {value_columns}
                    FROM cs_klines_1m k
                    WHERE k.exchange_id = %s
                      AND k.start_time >= %s AND k.start_time < %s
                    """,
                    [exchange_id, start_time_utc, end_time_utc],
                )
                while rows := stream.fetchmany(10_000):
                    for symbol_id, row_time, *values in rows:
                        idx = id_to_idx.get(symbol_id)
                        if idx is None:
                            continue
                        minute_idx = int(
                            (row_time - start_time_utc).total_seconds() // 60
                        )
                        if 0 <= minute_idx < total_minutes:
                            for matrix, value in zip(matrix_list, values):
                                if value is not None:
                                    matrix[idx, minute_idx] = float(value)

    return matrices


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
