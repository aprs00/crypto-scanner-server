from io import BytesIO

from django.db import transaction, connection
from django.utils import timezone
from typing import List, Dict

import numpy as np
import polars as pl

from correlations.models import CorrelationPairHistory
from correlations.db_utils import ensure_partition_exists
from exchange_connections.models import Symbol
from core.constants import Exchange as ExchangeEnum


def save_correlation_matrices_batch_to_db(
    symbols: List[str],
    correlation_matrices: Dict[str, List[float]],
    hours: int,
    exchange: ExchangeEnum,
    contract_type: str = "perpetual",
) -> int:
    """
    Save multiple correlation matrices to database in a single COPY operation.

    Args:
        symbols: List of symbol names in order
        correlation_matrices: Dict mapping data_type to flat correlation values
        hours: Time window in hours
        exchange: Exchange name
        contract_type: Contract type

    Returns:
        Total number of rows inserted
    """
    if not symbols or not correlation_matrices:
        return 0

    n = len(symbols)
    expected_pairs = n * (n - 1) // 2

    # Validate matrix sizes
    for data_type, matrix in correlation_matrices.items():
        if len(matrix) != expected_pairs:
            print(
                f"Matrix size mismatch for {data_type}: expected {expected_pairs}, got {len(matrix)}"
            )
            return 0

    # Fetch symbol IDs
    symbol_map = {
        s.name: s.id  # type: ignore
        for s in Symbol.objects.filter(
            name__in=symbols,
            exchange__name=exchange,
            contract_type__name=contract_type,
        ).only("id", "name")
    }

    if len(symbol_map) != n:
        missing = set(symbols) - set(symbol_map.keys())
        print(f"Symbols not found: {missing}")
        return 0

    symbol_ids = np.array([symbol_map[s] for s in symbols], dtype=np.int64)

    calculated_at = timezone.now()
    ensure_partition_exists(calculated_at)

    # Generate pair indices once
    i_idx, j_idx = np.triu_indices(n, k=1)
    s1, s2 = symbol_ids[i_idx], symbol_ids[j_idx]
    mask = s1 > s2
    s1_final = np.where(mask, s2, s1)
    s2_final = np.where(mask, s1, s2)

    # Build Polars DataFrames for each data type
    calculated_at_str = calculated_at.isoformat()
    frames = []
    for data_type, corr_matrix in correlation_matrices.items():
        df = pl.DataFrame({
            "symbol1_id": s1_final,
            "symbol2_id": s2_final,
            "correlation_value": corr_matrix,
            "data_type": [data_type] * expected_pairs,
            "hours": [hours] * expected_pairs,
            "calculated_at": [calculated_at_str] * expected_pairs,
        })
        frames.append(df)

    combined_df = pl.concat(frames)

    # Generate CSV for COPY
    buf = BytesIO()
    combined_df.write_csv(buf, include_header=False, separator="\t")
    copy_data = buf.getvalue()

    table = CorrelationPairHistory._meta.db_table
    cols = "symbol1_id, symbol2_id, correlation_value, data_type, hours, calculated_at"

    with transaction.atomic():
        with connection.cursor() as cursor:
            cursor.execute(f"ALTER TABLE {table} DISABLE TRIGGER ALL")
            try:
                with cursor.cursor.copy(f"COPY {table} ({cols}) FROM STDIN") as copy:  # type: ignore
                    copy.write(copy_data)
            finally:
                cursor.execute(f"ALTER TABLE {table} ENABLE TRIGGER ALL")

    return expected_pairs * len(correlation_matrices)
