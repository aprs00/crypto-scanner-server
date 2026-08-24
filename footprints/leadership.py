from __future__ import annotations

import psycopg
from django.conf import settings


COLLECTOR_ADVISORY_LOCK_IDS = {
    "binance": 0x4353465042494E41,
    "bybit": 0x4353465042594249,
    "hyperliquid": 0x4353465048595045,
}


class FootprintWriterLeadership:
    """Own one exchange writer lease for the lifetime of a PostgreSQL session."""

    def __init__(self, exchange: str) -> None:
        try:
            self._lock_id = COLLECTOR_ADVISORY_LOCK_IDS[exchange]
        except KeyError as exc:
            raise ValueError(f"unsupported footprint exchange: {exchange}") from exc
        self.exchange = exchange
        self._connection = None
        self._acquired = False

    @property
    def acquired(self) -> bool:
        return self._acquired

    def try_acquire(self) -> bool:
        if self._acquired:
            self.verify()
            return True

        connection = self._ensure_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_try_advisory_lock(%s)",
                    (self._lock_id,),
                )
                row = cursor.fetchone()
        except Exception:
            self.close()
            raise

        if row == (True,):
            self._acquired = True
            return True
        if row == (False,):
            return False

        self.close()
        raise RuntimeError(
            f"{self.exchange} footprint writer lock returned an unexpected result"
        )

    def verify(self) -> None:
        connection = self._connection
        if not self._acquired or connection is None:
            raise RuntimeError(
                f"{self.exchange} footprint writer lock is not owned"
            )
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                row = cursor.fetchone()
        except Exception as exc:
            self.close()
            raise RuntimeError(
                f"{self.exchange} footprint writer lock connection failed"
            ) from exc
        if row != (1,):
            self.close()
            raise RuntimeError(
                f"{self.exchange} footprint writer lock connection failed"
            )

    def close(self) -> None:
        connection = self._connection
        self._connection = None
        self._acquired = False
        if connection is not None:
            connection.close()

    def _ensure_connection(self):
        if self._connection is None:
            configuration = settings.DATABASES["default"]
            self._connection = psycopg.connect(
                dbname=configuration["NAME"],
                user=configuration["USER"],
                password=configuration["PASSWORD"],
                host=configuration["HOST"],
                port=configuration["PORT"],
                connect_timeout=10,
                autocommit=True,
            )
        return self._connection
