import logging
from datetime import timedelta
from io import StringIO
from django.db import transaction, connection
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
    symbols: list[str],
    correlation_matrix: list[float],
    data_type: str,
    hours: int,
    exchange: str = "binance",
    contract_type: str = "perpetual",
) -> int:
    if not symbols or not correlation_matrix:
        return 0

    n_symbols = len(symbols)
    expected_pairs = n_symbols * (n_symbols - 1) // 2
    if len(correlation_matrix) != expected_pairs:
        logger.warning(
            f"Correlation matrix size {len(correlation_matrix)} does not match expected number of pairs {expected_pairs} for {n_symbols} symbols"
        )
        return 0

    symbol_objs = Symbol.objects.filter(
        name__in=symbols, exchange__name=exchange, contract_type__name=contract_type
    ).only("id", "name")

    symbol_map = {s.name: s.id for s in symbol_objs}  # type: ignore
    missing_symbols = set(symbols) - set(symbol_map.keys())
    if missing_symbols:
        logger.warning(
            f"Some symbols not found in DB for exchange {exchange} and contract type {contract_type}: {missing_symbols}"
        )
        return 0

    calculated_at = timezone.now()

    out_lines = []
    append = out_lines.append
    idx = 0

    for i in range(n_symbols):
        id1 = symbol_map[symbols[i]]
        for j in range(i + 1, n_symbols):
            id2 = symbol_map[symbols[j]]
            val = correlation_matrix[idx]

            if id1 > id2:
                id1, id2 = id2, id1

            append(f"{id1}\t{id2}\t{val}\t{data_type}\t{hours}\t{calculated_at}\n")
            idx += 1

    buf = StringIO("".join(out_lines))

    with transaction.atomic(), connection.cursor() as cursor:
        cursor.copy_from(
            buf,
            "cs_correlation_pair_history",
            sep="\t",
            columns=[
                "symbol1_id",
                "symbol2_id",
                "correlation_value",
                "data_type",
                "hours",
                "calculated_at",
            ],
        )

    return expected_pairs
