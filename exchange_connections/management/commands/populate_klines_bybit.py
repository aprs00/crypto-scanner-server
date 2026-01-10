"""
Bybit Historical Kline Population Command

Fetches historical 1-minute kline data from Bybit REST API and populates the database.
Uses cursor-based pagination with 1000 klines per request.

Usage:
    python manage.py populate_klines_bybit --ticker BTCUSDT --start-date "01 Jan 2024"
    python manage.py populate_klines_bybit --start-date "01 Jan 2024" --end-date "31 Jan 2024"
"""

import requests
import time
from datetime import datetime
from typing import List

from exchange_connections.management.commands.base_populate_klines import (
    BasePopulateKlinesCommand,
)
from core.constants import Exchange

BYBIT_KLINE_URL = "https://api.bybit.com/v5/market/kline"
MAX_KLINES_PER_REQUEST = 1000


class Command(BasePopulateKlinesCommand):
    help = "Populate kline data from Bybit API"

    exchange = Exchange.BYBIT
    contract_type = "perpetual"
    request_delay = 0.2

    def fetch_all_klines_paginated(self, symbol, start_date, end_date) -> List:
        """
        Fetch all klines using Bybit API with backward pagination.

        Bybit returns klines sorted newest-first, so we paginate backward
        through time until we reach our start_date.
        """
        all_klines = []

        start_dt = self.parse_date(start_date)
        end_dt = self.parse_date(end_date)

        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = int(end_dt.timestamp() * 1000)

        # Bybit returns newest first, so we start from end and work backward
        current_end_ms = end_ms

        while current_end_ms > start_ms:
            try:
                print(
                    f"Fetching klines for {symbol}: "
                    f"until {datetime.fromtimestamp(current_end_ms / 1000)}"
                )

                klines = self._fetch_bybit_klines(
                    symbol,
                    start_ms,
                    current_end_ms,
                    limit=MAX_KLINES_PER_REQUEST,
                )

                if not klines:
                    print(f"No more klines available for {symbol}")
                    break

                # Convert to our standard format and add to list
                for kline in klines:
                    kline_start_ms = int(kline["t"])
                    if kline_start_ms >= start_ms:
                        all_klines.append(kline)

                # Find the oldest kline in this batch to set next query end
                oldest_kline_start = min(int(k["t"]) for k in klines)

                if oldest_kline_start <= start_ms:
                    # We've reached or passed our start date
                    break

                # Move end pointer to just before the oldest kline we got
                current_end_ms = oldest_kline_start - 1

                print(f"Fetched {len(klines)} klines. Total so far: {len(all_klines)}")

                time.sleep(self.request_delay)

            except Exception as e:
                print(f"Error fetching klines for {symbol}: {str(e)}")
                time.sleep(1)
                break

        # Sort by start time (oldest first) for consistent ordering
        all_klines.sort(key=lambda k: k["t"])

        print(
            f"Completed fetching klines for {symbol}. Total klines: {len(all_klines)}"
        )
        return all_klines

    @staticmethod
    def _fetch_bybit_klines(
        symbol: str, start_ms: int, end_ms: int, limit: int = 1000
    ) -> list:
        """
        Fetch klines from Bybit API.

        Returns list of klines in our normalized dict format:
        {t, T, s, o, h, l, c, v, n, q}
        """
        response = requests.get(
            BYBIT_KLINE_URL,
            params={
                "category": "linear",
                "symbol": symbol,
                "interval": "1",  # 1 minute
                "start": start_ms,
                "end": end_ms,
                "limit": limit,
            },
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()

        if data.get("retCode") != 0:
            raise ValueError(f"Bybit API error: {data.get('retMsg')}")

        result = data.get("result", {})
        raw_list = result.get("list", [])

        klines = []
        for item in raw_list:
            if len(item) >= 7:
                start_time_ms = int(item[0])
                klines.append(
                    {
                        "t": start_time_ms,
                        "T": start_time_ms + 59999,
                        "s": symbol,
                        "o": item[1],
                        "h": item[2],
                        "l": item[3],
                        "c": item[4],
                        "v": item[5],
                        "n": 0,
                        "q": item[6],
                        "V": None,
                        "Q": None,
                    }
                )

        return klines
