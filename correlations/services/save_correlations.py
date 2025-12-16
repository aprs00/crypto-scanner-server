from django.db import transaction, connection
from django.utils import timezone
from typing import List, Dict, Optional, Tuple
from io import StringIO

import numpy as np

from correlations.models import CorrelationPairHistory
from exchange_connections.models import Symbol


def _generate_copy_data(
    symbol_ids: np.ndarray,
    correlation_matrix: np.ndarray,
    data_type: str,
    hours: int,
    calculated_at_str: str,
    precomputed_indices: Optional[Tuple[np.ndarray, np.ndarray]] = None,
) -> bytes:
    """
    Generate COPY data efficiently using numpy vectorization.

    Args:
        symbol_ids: Array of symbol IDs in order
        correlation_matrix: Flat array of correlation values (upper triangle)
        data_type: Type of correlation data (e.g., 'close', 'volume')
        hours: Time window in hours
        calculated_at_str: ISO formatted timestamp string
        precomputed_indices: Optional tuple of (i_idx, j_idx) for reuse

    Returns:
        Encoded bytes ready for PostgreSQL COPY
    """
    n = len(symbol_ids)

    # Use precomputed indices if provided, otherwise compute them
    if precomputed_indices is not None:
        i_idx, j_idx = precomputed_indices
    else:
        i_idx, j_idx = np.triu_indices(n, k=1)

    # Get symbol IDs for each pair
    s1 = symbol_ids[i_idx]
    s2 = symbol_ids[j_idx]

    # Ensure s1 < s2 for consistent ordering
    mask = s1 > s2
    s1_result = np.where(mask, s2, s1)
    s2_result = np.where(mask, s1, s2)

    # Build output efficiently using StringIO
    suffix = f"\t{data_type}\t{hours}\t{calculated_at_str}\n"
    buf = StringIO()
    write = buf.write  # Local reference for speed

    for i in range(len(s1_result)):
        write(str(s1_result[i]))
        write("\t")
        write(str(s2_result[i]))
        write("\t")
        write(str(correlation_matrix[i]))
        write(suffix)

    return buf.getvalue().encode()


def save_correlation_matrix_to_db(
    symbols: List[str],
    correlation_matrix: List[float],
    data_type: str,
    hours: int,
    exchange: str = "binance",
    contract_type: str = "perpetual",
) -> int:
    """
    Save correlation matrix to database using optimized COPY.

    This function uses several optimizations for high-throughput inserts:
    1. Disables triggers during COPY (FK validation happens in application)
    2. Uses numpy for efficient data generation
    3. Single COPY operation instead of chunked approach

    Args:
        symbols: List of symbol names in order
        correlation_matrix: Flat list of correlation values (upper triangle)
        data_type: Type of correlation data (e.g., 'close', 'volume')
        hours: Time window in hours
        exchange: Exchange name
        contract_type: Contract type

    Returns:
        Number of rows inserted
    """
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

    # Convert to numpy arrays for efficient processing
    symbol_ids = np.array([symbol_map[s].id for s in symbols], dtype=np.int64)
    corr_matrix = np.asarray(correlation_matrix, dtype=np.float64)

    calculated_at = timezone.now()
    calculated_at_str = calculated_at.isoformat()

    # Generate COPY data
    copy_data = _generate_copy_data(
        symbol_ids,
        corr_matrix,
        data_type,
        hours,
        calculated_at_str,
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
            # Disable triggers for faster inserts (FK validation in app layer)
            cursor.execute(f"ALTER TABLE {table_name} DISABLE TRIGGER ALL")

            try:
                with cursor.cursor.copy(  # type: ignore
                    f"COPY {table_name} ({columns_str}) FROM STDIN"
                ) as copy:
                    copy.write(copy_data)
            finally:
                # Always re-enable triggers
                cursor.execute(f"ALTER TABLE {table_name} ENABLE TRIGGER ALL")

    return expected_pairs
