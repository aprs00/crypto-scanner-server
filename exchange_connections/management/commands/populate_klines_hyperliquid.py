import requests
import time
from datetime import datetime
from typing import List

from exchange_connections.management.commands.base_populate_klines import (
    BasePopulateKlinesCommand,
)
from core.constants import Exchange

HYPERLIQUID_INFO_URL = "https://api.hyperliquid.xyz/info"
CHUNK_SIZE_MS = 86400000  # 24 hours in milliseconds


class Command(BasePopulateKlinesCommand):
    help = "Populate kline data from Hyperliquid API"

    exchange = Exchange.HYPERLIQUID
    contract_type = "perpetual"
    request_delay = 3

    def fetch_all_klines_paginated(self, symbol, start_date, end_date) -> List:
        """Fetch all klines using Hyperliquid API in 24-hour chunks."""
        all_klines = []

        current_start = self.parse_date(start_date)
        final_end = self.parse_date(end_date)

        current_start_ms = int(current_start.timestamp() * 1000)
        final_end_ms = int(final_end.timestamp() * 1000)

        while current_start_ms < final_end_ms:
            try:
                chunk_end_ms = min(current_start_ms + CHUNK_SIZE_MS, final_end_ms)

                print(
                    f"Fetching klines for {symbol}: "
                    f"{datetime.fromtimestamp(current_start_ms / 1000)} to "
                    f"{datetime.fromtimestamp(chunk_end_ms / 1000)}"
                )

                klines = self._fetch_hyperliquid_klines(
                    symbol, current_start_ms, chunk_end_ms
                )

                if not klines:
                    print(f"No klines in this range for {symbol}, advancing...")
                    current_start_ms = chunk_end_ms + 1
                    time.sleep(self.request_delay)
                    continue

                all_klines.extend(klines)

                # Advance past the last kline's close time
                last_kline_close_time_ms = klines[-1]["T"]
                current_start_ms = last_kline_close_time_ms + 1

                print(f"Fetched {len(klines)} klines. Total so far: {len(all_klines)}")

                time.sleep(self.request_delay)

                if current_start_ms >= final_end_ms:
                    break

            except Exception as e:
                print(
                    f"Error fetching klines for {symbol} at {current_start_ms}: {str(e)}"
                )
                time.sleep(1)
                break

        print(
            f"Completed fetching klines for {symbol}. Total klines: {len(all_klines)}"
        )
        return all_klines

    @staticmethod
    def _fetch_hyperliquid_klines(symbol: str, start_ms: int, end_ms: int) -> list:
        """Fetch klines from Hyperliquid candleSnapshot API."""
        response = requests.post(
            HYPERLIQUID_INFO_URL,
            headers={"Content-Type": "application/json"},
            json={
                "type": "candleSnapshot",
                "req": {
                    "coin": symbol,
                    "interval": "1m",
                    "startTime": start_ms,
                    "endTime": end_ms,
                },
            },
            timeout=30,
        )
        response.raise_for_status()
        return response.json()
