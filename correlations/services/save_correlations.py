import logging
from typing import List, Dict
from datetime import timedelta
from django.db import transaction, models
from django.utils import timezone

from correlations.models import CorrelationPairHistory
from exchange_connections.models import Symbol

logger = logging.getLogger(__name__)


def cleanup_old_correlation_data(retention_hours) -> int:
    """
    Delete correlation records older than the specified retention period.
    """
    try:
        cutoff_time = timezone.now() - timedelta(hours=retention_hours)
        deleted_count, _ = CorrelationPairHistory.objects.filter(
            calculated_at__lt=cutoff_time
        ).delete()

        if deleted_count > 0:
            logger.info(
                f"Cleaned up {deleted_count} correlation records older than {retention_hours} hours "
                f"(before {cutoff_time})"
            )

        return deleted_count

    except Exception as e:
        logger.error(f"Error cleaning up old correlation data: {e}", exc_info=True)
        return 0


def save_correlation_matrix_to_db(
    symbols: List[str],
    correlation_matrix: List[float],
    data_type: str,
    hours: int,
    exchange: str = "binance",
    contract_type: str = "perpetual",
) -> int:
    """
    Save a correlation matrix to the database.

    Args:
        symbols: List of symbol names in order
        correlation_matrix: Upper triangle correlation values in row-major order
        data_type: Type of data (e.g., 'close', 'volume', 'trades')
        hours: Time window in hours
        exchange: Exchange name (default: 'binance')
        contract_type: Contract type (default: 'perpetual')

    Returns:
        Number of correlation records saved
    """
    if not symbols or not correlation_matrix:
        logger.warning("Empty symbols or correlation_matrix provided")
        return 0

    n_symbols = len(symbols)
    expected_pairs = n_symbols * (n_symbols - 1) // 2

    if len(correlation_matrix) != expected_pairs:
        logger.error(
            f"Correlation matrix size mismatch. Expected {expected_pairs} pairs "
            f"for {n_symbols} symbols, got {len(correlation_matrix)}"
        )
        return 0

    try:
        symbol_objs = Symbol.objects.filter(
            name__in=symbols, exchange__name=exchange, contract_type__name=contract_type
        ).select_related("exchange", "contract_type")

        symbol_map: Dict[str, Symbol] = {s.name: s for s in symbol_objs}

        missing_symbols = set(symbols) - set(symbol_map.keys())
        if missing_symbols:
            logger.error(f"Symbols not found in database: {missing_symbols}")
            return 0

        correlation_records = []
        calculated_at = timezone.now()

        idx = 0
        for i in range(n_symbols):
            for j in range(i + 1, n_symbols):
                symbol1_name = symbols[i]
                symbol2_name = symbols[j]
                correlation_value = correlation_matrix[idx]

                symbol1 = symbol_map[symbol1_name]
                symbol2 = symbol_map[symbol2_name]

                if symbol1.id > symbol2.id:  # type: ignore
                    symbol1, symbol2 = symbol2, symbol1

                correlation_records.append(
                    CorrelationPairHistory(
                        symbol1=symbol1,
                        symbol2=symbol2,
                        correlation_value=correlation_value,
                        data_type=data_type,
                        hours=hours,
                        calculated_at=calculated_at,
                    )
                )

                idx += 1

        with transaction.atomic():
            CorrelationPairHistory.objects.bulk_create(
                correlation_records, ignore_conflicts=True
            )

        logger.info(
            f"Saved {len(correlation_records)} correlation records "
            f"for {data_type} ({hours}h) at {calculated_at}"
        )

        cleanup_old_correlation_data(retention_hours=13)

        return len(correlation_records)

    except Exception as e:
        logger.error(f"Error saving correlation matrix to database: {e}", exc_info=True)
        return 0
