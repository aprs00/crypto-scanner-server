from django.db import transaction, connection
from django.utils import timezone
from typing import List, Dict, Generator

from correlations.models import CorrelationPairHistory
from exchange_connections.models import Symbol


def _generate_copy_chunks(
    symbol_ids: List[int],
    correlation_matrix: List[float],
    data_type: str,
    hours: int,
    calculated_at_str: str,
    chunk_size: int = 10000,
) -> Generator[bytes, None, None]:
    """Generate COPY data in chunks for efficient streaming to PostgreSQL."""
    n_symbols = len(symbol_ids)
    lines: List[str] = []
    idx = 0

    for i in range(n_symbols):
        id1 = symbol_ids[i]
        for j in range(i + 1, n_symbols):
            id2 = symbol_ids[j]
            if id1 > id2:
                s1, s2 = id2, id1
            else:
                s1, s2 = id1, id2

            lines.append(
                f"{s1}\t{s2}\t{correlation_matrix[idx]}\t{data_type}\t{hours}\t{calculated_at_str}"
            )
            idx += 1

            if len(lines) >= chunk_size:
                yield ("\n".join(lines) + "\n").encode()
                lines.clear()

    if lines:
        yield ("\n".join(lines) + "\n").encode()


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

    symbol_ids = [symbol_map[s].id for s in symbols]  # type: ignore

    calculated_at = timezone.now()
    calculated_at_str = calculated_at.isoformat()

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
                for chunk in _generate_copy_chunks(
                    symbol_ids,
                    correlation_matrix,
                    data_type,
                    hours,
                    calculated_at_str,
                ):
                    copy.write(chunk)

    return expected_pairs
