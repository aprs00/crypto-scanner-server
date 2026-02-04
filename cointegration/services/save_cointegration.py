from io import BytesIO
from datetime import datetime
import numpy as np
import polars as pl
from django.db import transaction, connection

from cointegration.db_utils import ensure_partition_exists
from cointegration.models import CointegrationPair, CointegrationPairHistory


def _build_dataframe(
    exchange_id: int,
    contract_type_id: int,
    symbol1_ids: np.ndarray,
    symbol2_ids: np.ndarray,
    window_minutes: int,
    hedge_ratio: np.ndarray,
    intercept: np.ndarray,
    spread_mean: np.ndarray,
    spread_std: np.ndarray,
    spread_z: np.ndarray,
    half_life: np.ndarray,
    adf_t: np.ndarray,
    calculated_at: datetime,
) -> pl.DataFrame:
    calculated_at_str = calculated_at.isoformat()

    data = {
        "exchange_id": symbol1_ids * 0 + exchange_id,
        "contract_type_id": symbol1_ids * 0 + contract_type_id,
        "symbol1_id": symbol1_ids,
        "symbol2_id": symbol2_ids,
        "window_minutes": symbol1_ids * 0 + window_minutes,
        "hedge_ratio": hedge_ratio,
        "intercept": intercept,
        "spread_mean": spread_mean,
        "spread_std": spread_std,
        "spread_z": spread_z,
        "half_life": half_life,
        "adf_t": adf_t,
        "calculated_at": [calculated_at_str] * len(symbol1_ids),
    }

    df = pl.DataFrame(data)
    df = df.with_columns(
        pl.when(pl.col("half_life").is_nan())
        .then(None)
        .otherwise(pl.col("half_life"))
        .alias("half_life")
    )
    return df


def save_cointegration_results(
    exchange_id: int,
    contract_type_id: int,
    symbol1_ids: np.ndarray,
    symbol2_ids: np.ndarray,
    window_minutes: int,
    hedge_ratio: np.ndarray,
    intercept: np.ndarray,
    spread_mean: np.ndarray,
    spread_std: np.ndarray,
    spread_z: np.ndarray,
    half_life: np.ndarray,
    adf_t: np.ndarray,
    calculated_at: datetime,
) -> int:
    if symbol1_ids.size == 0:
        return 0

    ensure_partition_exists(calculated_at)

    df = _build_dataframe(
        exchange_id=exchange_id,
        contract_type_id=contract_type_id,
        symbol1_ids=symbol1_ids,
        symbol2_ids=symbol2_ids,
        window_minutes=window_minutes,
        hedge_ratio=hedge_ratio,
        intercept=intercept,
        spread_mean=spread_mean,
        spread_std=spread_std,
        spread_z=spread_z,
        half_life=half_life,
        adf_t=adf_t,
        calculated_at=calculated_at,
    )

    buf = BytesIO()
    df.write_csv(buf, include_header=False, separator="\t", null_value="\\N")
    copy_data = buf.getvalue()

    history_table = CointegrationPairHistory._meta.db_table
    latest_table = CointegrationPair._meta.db_table

    cols = (
        "exchange_id, contract_type_id, symbol1_id, symbol2_id, window_minutes, "
        "hedge_ratio, intercept, spread_mean, spread_std, spread_z, half_life, adf_t, calculated_at"
    )

    with transaction.atomic():
        with connection.cursor() as cursor:
            with cursor.cursor.copy(
                f"COPY {history_table} ({cols}) FROM STDIN"
            ) as copy:
                copy.write(copy_data)

            cursor.execute(
                """
                CREATE TEMP TABLE cointegration_pair_stage (
                    exchange_id BIGINT NOT NULL,
                    contract_type_id BIGINT NOT NULL,
                    symbol1_id BIGINT NOT NULL,
                    symbol2_id BIGINT NOT NULL,
                    window_minutes INTEGER NOT NULL,
                    hedge_ratio DOUBLE PRECISION NOT NULL,
                    intercept DOUBLE PRECISION NOT NULL,
                    spread_mean DOUBLE PRECISION NOT NULL,
                    spread_std DOUBLE PRECISION NOT NULL,
                    spread_z DOUBLE PRECISION NOT NULL,
                    half_life DOUBLE PRECISION,
                    adf_t DOUBLE PRECISION NOT NULL,
                    calculated_at TIMESTAMPTZ NOT NULL
                ) ON COMMIT DROP
                """
            )

            with cursor.cursor.copy(
                f"COPY cointegration_pair_stage ({cols}) FROM STDIN"
            ) as copy:
                copy.write(copy_data)

            cursor.execute(
                f"""
                INSERT INTO {latest_table} (
                    exchange_id,
                    contract_type_id,
                    symbol1_id,
                    symbol2_id,
                    window_minutes,
                    hedge_ratio,
                    intercept,
                    spread_mean,
                    spread_std,
                    spread_z,
                    half_life,
                    adf_t,
                    calculated_at
                )
                SELECT
                    exchange_id,
                    contract_type_id,
                    symbol1_id,
                    symbol2_id,
                    window_minutes,
                    hedge_ratio,
                    intercept,
                    spread_mean,
                    spread_std,
                    spread_z,
                    half_life,
                    adf_t,
                    calculated_at
                FROM cointegration_pair_stage
                ON CONFLICT (exchange_id, contract_type_id, symbol1_id, symbol2_id, window_minutes)
                DO UPDATE SET
                    hedge_ratio = EXCLUDED.hedge_ratio,
                    intercept = EXCLUDED.intercept,
                    spread_mean = EXCLUDED.spread_mean,
                    spread_std = EXCLUDED.spread_std,
                    spread_z = EXCLUDED.spread_z,
                    half_life = EXCLUDED.half_life,
                    adf_t = EXCLUDED.adf_t,
                    calculated_at = EXCLUDED.calculated_at
                """
            )

    return int(symbol1_ids.size)
