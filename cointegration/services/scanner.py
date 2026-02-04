import time
from datetime import timedelta
from typing import List, Tuple

import numpy as np
from django.utils import timezone

from exchange_connections.models import Symbol
from exchange_connections.selectors import (
    get_historical_kline_data,
    get_symbol_kline_data_at_timestamp,
)
from cointegration.db_utils import try_acquire_lock, release_lock
from cointegration.services.save_cointegration import save_cointegration_results


class CointegrationScanner:
    def __init__(
        self,
        exchange: str,
        contract_type: str,
        window_minutes: int,
        cadence_minutes: int,
        batch_size: int = 5000,
    ):
        if window_minutes % 60 != 0:
            raise ValueError("window_minutes must be a multiple of 60")

        self.exchange = exchange
        self.contract_type = contract_type
        self.window_minutes = window_minutes
        self.window_len = window_minutes
        self.cadence_minutes = cadence_minutes
        self.batch_size = batch_size

        self.symbols: List[str] = []
        self.symbol_ids: np.ndarray = np.array([], dtype=np.int64)
        self.buffer: np.ndarray | None = None
        self.last_values: np.ndarray | None = None
        self.pos = 0
        self.last_timestamp = None
        self.pair_i: np.ndarray | None = None
        self.pair_j: np.ndarray | None = None

        self.exchange_id: int | None = None
        self.contract_type_id: int | None = None

    def _load_symbols(self) -> None:
        qs = (
            Symbol.objects.filter(
                exchange__name=self.exchange,
                contract_type__name=self.contract_type,
            )
            .select_related("exchange", "contract_type")
            .order_by("id")
        )

        symbols: List[str] = []
        symbol_ids: List[int] = []
        for sym in qs:
            symbols.append(sym.name)
            symbol_ids.append(sym.id)  # type: ignore[arg-type]

        if not symbols:
            raise RuntimeError(
                f"No symbols found for {self.exchange}:{self.contract_type}"
            )

        self.symbols = symbols
        self.symbol_ids = np.array(symbol_ids, dtype=np.int64)

        if qs:
            self.exchange_id = qs[0].exchange_id
            self.contract_type_id = qs[0].contract_type_id

    def _fill_missing(self, values: List[float]) -> np.ndarray | None:
        arr = np.array(values, dtype=np.float64)
        valid = np.isfinite(arr)

        if not np.any(valid):
            return None

        idx = np.where(valid, np.arange(arr.size), 0)
        np.maximum.accumulate(idx, out=idx)
        filled = arr[idx]

        first_valid = int(np.argmax(valid))
        filled[:first_valid] = arr[first_valid]

        return filled

    def _load_initial_window(self) -> None:
        hours = self.window_minutes // 60
        end_time = timezone.now().replace(second=0, microsecond=0)

        data = get_historical_kline_data(
            hours=hours,
            symbols=self.symbols,
            exchange=self.exchange,
            contract_type=self.contract_type,
            end_time=end_time,
        )

        series_list = []
        kept_symbols = []
        kept_ids = []

        for symbol, symbol_id in zip(self.symbols, self.symbol_ids):
            series = data.get(symbol, {}).get("price")
            if not series:
                continue

            filled = self._fill_missing(series)
            if filled is None:
                continue

            if np.any(filled <= 0):
                continue

            log_prices = np.log(filled)
            series_list.append(log_prices)
            kept_symbols.append(symbol)
            kept_ids.append(symbol_id)

        if not series_list:
            raise RuntimeError("No valid price series available for cointegration")

        if len(series_list) != len(self.symbols):
            print(
                f"[{self.exchange}] Dropped {len(self.symbols) - len(series_list)} symbols due to missing data"
            )

        self.symbols = kept_symbols
        self.symbol_ids = np.array(kept_ids, dtype=np.int64)

        self.buffer = np.vstack(series_list)
        self.last_values = self.buffer[:, -1].copy()
        self.pos = 0
        self.last_timestamp = end_time - timedelta(minutes=1)

        n = self.buffer.shape[0]
        self.pair_i, self.pair_j = np.triu_indices(n, k=1)

    def _update_to_latest(self) -> None:
        if self.buffer is None or self.last_values is None or self.last_timestamp is None:
            return

        target_time = timezone.now().replace(second=0, microsecond=0) - timedelta(
            minutes=1
        )

        while self.last_timestamp < target_time:
            next_ts = self.last_timestamp + timedelta(minutes=1)
            data = get_symbol_kline_data_at_timestamp(
                symbols=self.symbols,
                exchange=self.exchange,
                contract_type=self.contract_type,
                kline_timestamp_ms=int(next_ts.timestamp() * 1000),
            )

            new_vals = self.last_values.copy()
            for idx, symbol in enumerate(self.symbols):
                row = data.get(symbol)
                if not row:
                    continue
                price = row.get("price")
                if price is None or price <= 0:
                    continue
                new_vals[idx] = np.log(float(price))

            self.buffer[:, self.pos] = new_vals
            self.last_values = new_vals
            self.pos = (self.pos + 1) % self.window_len
            self.last_timestamp = next_ts

    def _get_window_matrix(self) -> np.ndarray:
        if self.buffer is None:
            raise RuntimeError("Buffer not initialized")

        if self.pos == 0:
            return self.buffer.copy()

        return np.concatenate(
            (self.buffer[:, self.pos :], self.buffer[:, : self.pos]), axis=1
        )

    def _align_next_compute(self, now):
        minute = (now.minute // self.cadence_minutes) * self.cadence_minutes
        base = now.replace(minute=minute, second=0, microsecond=0)
        if base <= now:
            base += timedelta(minutes=self.cadence_minutes)
        return base

    def _compute_and_store(self) -> None:
        if self.buffer is None or self.pair_i is None or self.pair_j is None:
            return
        if self.exchange_id is None or self.contract_type_id is None:
            return

        window = self._get_window_matrix()
        n_symbols, length = window.shape
        if length < 10 or n_symbols < 2:
            return

        mean = window.mean(axis=1)
        demeaned = window - mean[:, None]
        var = (demeaned * demeaned).mean(axis=1)
        cov = (demeaned @ demeaned.T) / float(length)

        i_idx = self.pair_i
        j_idx = self.pair_j
        n_pairs = i_idx.size

        cov_ij = cov[i_idx, j_idx]
        var_i = var[i_idx]
        mean_i = mean[i_idx]
        mean_j = mean[j_idx]

        beta = np.divide(cov_ij, var_i, out=np.zeros_like(cov_ij), where=var_i > 1e-12)
        alpha = mean_j - beta * mean_i

        spread_mean = np.zeros(n_pairs, dtype=np.float64)
        spread_std = np.zeros(n_pairs, dtype=np.float64)
        spread_z = np.zeros(n_pairs, dtype=np.float64)
        half_life = np.full(n_pairs, np.nan, dtype=np.float64)
        adf_t = np.zeros(n_pairs, dtype=np.float64)

        batch = self.batch_size
        for start in range(0, n_pairs, batch):
            end = min(start + batch, n_pairs)
            idx = slice(start, end)
            ii = i_idx[idx]
            jj = j_idx[idx]
            beta_batch = beta[idx]

            resid = demeaned[jj] - beta_batch[:, None] * demeaned[ii]

            spread_mean_batch = resid.mean(axis=1)
            spread_std_batch = resid.std(axis=1, ddof=0)
            spread_mean[idx] = spread_mean_batch
            spread_std[idx] = spread_std_batch

            last_resid = resid[:, -1]
            spread_z[idx] = np.divide(
                last_resid - spread_mean_batch,
                spread_std_batch,
                out=np.zeros_like(last_resid),
                where=spread_std_batch > 1e-12,
            )

            x = resid[:, :-1]
            y = resid[:, 1:] - resid[:, :-1]

            x_mean = x.mean(axis=1)
            y_mean = y.mean(axis=1)

            x_demean = x - x_mean[:, None]
            y_demean = y - y_mean[:, None]

            cov_xy = (x_demean * y_demean).mean(axis=1)
            var_x = (x_demean * x_demean).mean(axis=1)

            b = np.divide(cov_xy, var_x, out=np.zeros_like(cov_xy), where=var_x > 1e-12)
            a = y_mean - b * x_mean

            eps = y - (a[:, None] + b[:, None] * x)
            sse = (eps * eps).sum(axis=1)

            n = x.shape[1]
            s2 = np.divide(
                sse,
                n - 2,
                out=np.zeros_like(sse),
                where=(n - 2) > 0,
            )
            denom = var_x * n
            se_b = np.sqrt(
                np.divide(s2, denom, out=np.zeros_like(s2), where=denom > 1e-12)
            )

            adf_t[idx] = np.divide(
                b, se_b, out=np.zeros_like(b), where=se_b > 1e-12
            )

            phi = 1 + b
            with np.errstate(divide="ignore", invalid="ignore"):
                half_life[idx] = np.where(
                    (phi > 0) & (phi < 1), -np.log(2) / np.log(phi), np.nan
                )

        calculated_at = timezone.now()

        save_cointegration_results(
            exchange_id=self.exchange_id,
            contract_type_id=self.contract_type_id,
            symbol1_ids=self.symbol_ids[i_idx],
            symbol2_ids=self.symbol_ids[j_idx],
            window_minutes=self.window_minutes,
            hedge_ratio=beta,
            intercept=alpha,
            spread_mean=spread_mean,
            spread_std=spread_std,
            spread_z=spread_z,
            half_life=half_life,
            adf_t=adf_t,
            calculated_at=calculated_at,
        )

    def run(self) -> None:
        self._load_symbols()
        self._load_initial_window()

        next_compute = self._align_next_compute(timezone.now())

        while True:
            try:
                self._update_to_latest()

                now = timezone.now()
                if now >= next_compute:
                    if try_acquire_lock(
                        self.exchange, self.contract_type, self.window_minutes
                    ):
                        try:
                            self._compute_and_store()
                        finally:
                            release_lock(
                                self.exchange, self.contract_type, self.window_minutes
                            )
                    else:
                        print(
                            f"[{self.exchange}] Previous cointegration run still active. Skipping."
                        )

                    next_compute += timedelta(minutes=self.cadence_minutes)

            except Exception as exc:
                print(f"[{self.exchange}] Cointegration scan error: {exc}")

            time.sleep(1)
