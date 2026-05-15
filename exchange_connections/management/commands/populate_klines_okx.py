"""
OKX Historical Kline Population Command

Fetches historical 1-minute kline data from OKX REST API and populates the database.
Uses backward pagination via the `after` parameter (exclusive, returns older candles).

Usage:
    python manage.py populate_klines_okx --ticker BTC-USDT-SWAP --start-date "01 Jan 2024"
    python manage.py populate_klines_okx --start-date "01 Jan 2024" --end-date "31 Jan 2024"
"""

import requests
import time
from datetime import datetime
from typing import List

from exchange_connections.management.commands.base_populate_klines import (
    BasePopulateKlinesCommand,
)
from core.constants import Exchange

OKX_HISTORY_CANDLES_URL = "https://www.okx.com/api/v5/market/history-candles"
MAX_KLINES_PER_REQUEST = 100


class Command(BasePopulateKlinesCommand):
    help = "Populate kline data from OKX API"

    exchange = Exchange.OKX
    contract_type = "perpetual"
    request_delay = 0.15

    def fetch_all_klines_paginated(self, symbol, start_date, end_date) -> List:
        """Fetch all klines using OKX API with backward pagination.

        OKX `after` is exclusive and returns candles strictly OLDER than the
        provided timestamp (newest-first). We page backward by setting the next
        `after` to the oldest timestamp from the previous batch.
        """
        all_klines = []

        start_dt = self.parse_date(start_date)
        end_dt = self.parse_date(end_date)

        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = int(end_dt.timestamp() * 1000)

        current_after_ms = end_ms

        while current_after_ms > start_ms:
            try:
                print(
                    f"Fetching klines for {symbol}: "
                    f"before {datetime.fromtimestamp(current_after_ms / 1000)}"
                )

                klines = self._fetch_okx_klines(
                    symbol,
                    after_ms=current_after_ms,
                    limit=MAX_KLINES_PER_REQUEST,
                )

                if not klines:
                    print(f"No more klines available for {symbol}")
                    break

                for kline in klines:
                    if int(kline["t"]) >= start_ms:
                        all_klines.append(kline)

                oldest_kline_start = min(int(k["t"]) for k in klines)

                if oldest_kline_start <= start_ms:
                    break

                current_after_ms = oldest_kline_start

                print(f"Fetched {len(klines)} klines. Total so far: {len(all_klines)}")

                time.sleep(self.request_delay)

            except Exception as e:
                print(f"Error fetching klines for {symbol}: {str(e)}")
                time.sleep(1)
                break

        all_klines.sort(key=lambda k: k["t"])

        print(
            f"Completed fetching klines for {symbol}. Total klines: {len(all_klines)}"
        )
        return all_klines

    @staticmethod
    def _fetch_okx_klines(
        symbol: str, after_ms: int, limit: int = MAX_KLINES_PER_REQUEST
    ) -> list:
        """Fetch klines from OKX history-candles API.

        Returns list of klines in our normalized dict format:
        {t, T, s, o, h, l, c, v, n, q, V, Q}
        """
        response = requests.get(
            OKX_HISTORY_CANDLES_URL,
            params={
                "instId": symbol,
                "bar": "1m",
                "after": after_ms,
                "limit": limit,
            },
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()

        if data.get("code") != "0":
            raise ValueError(f"OKX API error: {data.get('msg')}")

        klines = []
        for item in data.get("data", []):
            if len(item) >= 8:
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
                        "q": item[7],
                        "V": None,
                        "Q": None,
                    }
                )

        return klines
