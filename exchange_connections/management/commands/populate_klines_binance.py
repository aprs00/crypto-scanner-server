import time
from datetime import datetime, timedelta
from typing import List

from binance.client import Client

from exchange_connections.management.commands.base_populate_klines import (
    BasePopulateKlinesCommand,
)

client = Client()


class Command(BasePopulateKlinesCommand):
    help = "Populate kline data from Binance API"

    exchange = "binance"
    contract_type = "perpetual"
    request_delay = 0.5

    def fetch_all_klines_paginated(self, symbol, start_date, end_date) -> List:
        """Fetch all klines using Binance API with pagination (1000 klines per request)."""
        all_klines = []

        current_start = self.parse_date(start_date)
        final_end = self.parse_date(end_date)

        while current_start < final_end:
            try:
                print(
                    f"Fetching klines from {current_start.strftime('%d %b %Y %H:%M:%S')} for {symbol}"
                )

                klines = client.futures_historical_klines(
                    symbol,
                    Client.KLINE_INTERVAL_1MINUTE,
                    current_start.strftime("%d %b %Y %H:%M:%S"),
                    limit=1000,
                )

                if not klines:
                    print(f"No more klines available for {symbol}")
                    break

                all_klines.extend(klines)

                # Advance to next batch using last kline's close time
                last_kline_close_time = datetime.fromtimestamp(klines[-1][6] / 1000)
                current_start = last_kline_close_time + timedelta(minutes=1)

                print(f"Fetched {len(klines)} klines. Total so far: {len(all_klines)}")

                time.sleep(self.request_delay)

                if current_start >= final_end:
                    break

            except Exception as e:
                print(
                    f"Error fetching klines for {symbol} at {current_start}: {str(e)}"
                )
                time.sleep(1)
                break

        print(
            f"Completed fetching klines for {symbol}. Total klines: {len(all_klines)}"
        )
        return all_klines
