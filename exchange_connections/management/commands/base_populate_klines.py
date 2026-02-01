"""Base class for populate_klines commands across different exchanges."""

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import List

from django.core.management.base import BaseCommand
from django.db import IntegrityError

from core.constants import Exchange

from exchange_connections.selectors import get_exchange_symbols
from exchange_connections.services.klines_ingest import (
    build_models_from_rest,
    bulk_insert_klines,
)


class BasePopulateKlinesCommand(BaseCommand, ABC):
    """Abstract base class for populating klines from exchange APIs."""

    # Subclasses must define these
    exchange: Exchange
    contract_type: str = "perpetual"
    request_delay: float = 0.1  # seconds between API requests

    def add_arguments(self, parser):
        parser.add_argument(
            "--ticker",
            type=str,
            help="Specific ticker to populate (defaults to all symbols)",
        )
        parser.add_argument(
            "--start-date",
            type=str,
            help='Start date in format "DD MMM YYYY" or "DD MMM YYYY HH:MM" (defaults to 1 month ago)',
        )
        parser.add_argument(
            "--end-date",
            type=str,
            help='End date in format "DD MMM YYYY" or "DD MMM YYYY HH:MM" (defaults to now)',
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=40000,
            help="Batch size for bulk insert (default: 40000)",
        )

    def handle(self, *args, **options):
        ticker = options.get("ticker")
        start_date = options.get("start_date") or self.get_1_month_ago_date()
        end_date = options.get("end_date") or datetime.now()
        batch_size = options.get("batch_size", 40000)

        self.stdout.write(f"Starting {self.exchange} kline population...")

        if ticker:
            self.stdout.write(f"Processing single ticker: {ticker}")
            self.populate_kline_1m(ticker, start_date, end_date, batch_size)
        else:
            self.stdout.write(f"Processing all symbols from {start_date} to {end_date}")
            self.populate_all_klines_1m(start_date, end_date, batch_size)

        self.stdout.write(
            self.style.SUCCESS(
                f"{self.exchange.capitalize()} kline population completed successfully!"
            )
        )

    def get_symbols(self) -> List[str]:
        """Get all symbols for this exchange from Redis."""
        return get_exchange_symbols(self.exchange, self.contract_type)

    def populate_all_klines_1m(self, start_date, end_date, batch):
        """Populate 1-minute klines for all symbols sequentially."""
        print(
            f"Starting {self.exchange} 1m kline population from {start_date} to {end_date}"
        )

        symbols = self.get_symbols()

        for symbol in symbols:
            try:
                self.populate_kline_1m(symbol, start_date, end_date, batch)
            except Exception as exc:
                print(f"{symbol} generated an exception: {exc}")

        print(f"Completed {self.exchange} 1m kline population for all symbols")

    def populate_kline_1m(self, symbol, start_date, end_date, batch):
        """Populate 1-minute klines for a specific symbol with pagination."""
        print(
            f"Fetching {self.exchange} 1m klines for {symbol} from {start_date} to {end_date}"
        )

        try:
            all_klines = self.fetch_all_klines_paginated(symbol, start_date, end_date)
            print(f"Total klines fetched: {len(all_klines)}")

            if not all_klines:
                print(f"No klines found for {symbol}")
                return

            kline_objects = build_models_from_rest(
                all_klines,
                exchange=self.exchange,
                contract_type=self.contract_type,
                symbol=symbol,
            )

            if kline_objects:
                print(f"Created {len(kline_objects)} kline objects for {symbol}")

                try:
                    attempted = bulk_insert_klines(kline_objects, chunk_size=batch)
                    print(
                        f"Inserted (attempted) {attempted} kline records for {symbol} (duplicates ignored)"
                    )
                except IntegrityError as e:
                    print(f"IntegrityError bulk inserting {symbol}: {e}")

            print(f"Completed processing {symbol}")

        except Exception as e:
            print(f"Error processing {symbol}: {str(e)}")

    @abstractmethod
    def fetch_all_klines_paginated(self, symbol, start_date, end_date) -> List:
        """Fetch all klines by making paginated API requests.

        Subclasses must implement this with exchange-specific API logic.
        """
        pass

    @staticmethod
    def get_1_month_ago_date() -> str:
        """Get the date 1 month ago from today."""
        return (datetime.now() - timedelta(days=30)).strftime("%d %b %Y")

    @staticmethod
    def parse_date(date_input) -> datetime:
        """Parse date from string or return as-is if already datetime.

        Supports formats:
        - "DD MMM YYYY" (e.g., "06 Dec 2026")
        - "DD MMM YYYY HH:MM" (e.g., "06 Dec 2026 14:00")
        """
        if isinstance(date_input, str):
            for fmt in ("%d %b %Y %H:%M", "%d %b %Y"):
                try:
                    return datetime.strptime(date_input, fmt)
                except ValueError:
                    continue
            raise ValueError(
                f"Date '{date_input}' does not match expected formats: "
                "'DD MMM YYYY' or 'DD MMM YYYY HH:MM'"
            )
        return date_input
