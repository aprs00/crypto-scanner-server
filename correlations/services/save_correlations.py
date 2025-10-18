import logging
import time
from datetime import timedelta
from io import StringIO
from django.db import transaction, connection
from django.utils import timezone
from typing import List, Dict

from correlations.models import CorrelationPairHistory
from exchange_connections.models import Symbol

logger = logging.getLogger(__name__)


def save_correlation_matrix_to_db(
    symbols: List[str],
    correlation_matrix: List[float],
    data_type: str,
    hours: int,
    exchange: str = "binance",
    contract_type: str = "perpetual",
) -> int:
    if not symbols or not correlation_matrix:
        print("Empty symbols or correlation_matrix provided")
        return 0

    n_symbols = len(symbols)
    expected_pairs = n_symbols * (n_symbols - 1) // 2

    if len(correlation_matrix) != expected_pairs:
        print(
            f"Correlation matrix size mismatch. Expected {expected_pairs} pairs "
            f"for {n_symbols} symbols, got {len(correlation_matrix)}"
        )
        return 0

    symbol_query = Symbol.objects.filter(
        name__in=symbols, exchange__name=exchange, contract_type__name=contract_type
    ).select_related("exchange", "contract_type")

    symbol_objs = symbol_query
    symbol_map: Dict[str, Symbol] = {s.name: s for s in symbol_objs}

    missing_symbols = set(symbols) - set(symbol_map.keys())
    if missing_symbols:
        print(f"Symbols not found in database: {missing_symbols}")
        return 0

    correlation_data = []
    calculated_at = timezone.now()
    calculated_at_str = calculated_at.isoformat()

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

            correlation_data.append(
                (
                    symbol1.id,  # type: ignore
                    symbol2.id,  # type: ignore
                    correlation_value,
                    data_type,
                    hours,
                    calculated_at_str,
                )
            )

            idx += 1

    if not correlation_data:
        return 0

    data_io = StringIO()
    for row in correlation_data:
        data_io.write(
            "\t".join(str(field) if field is not None else "\\N" for field in row)
            + "\n"
        )

    table_name = CorrelationPairHistory._meta.db_table
    columns = (
        "symbol1_id",
        "symbol2_id",
        "correlation_value",
        "data_type",
        "hours",
        "calculated_at",
    )
    columns_str = ", ".join(columns)

    with transaction.atomic():
        with connection.cursor() as cursor:
            with cursor.cursor.copy(  # type: ignore
                f"COPY {table_name} ({columns_str}) FROM STDIN"
            ) as copy:
                copy.write(data_io.getvalue())

    return len(correlation_data)
