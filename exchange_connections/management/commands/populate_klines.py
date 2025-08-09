from binance.client import Client
from datetime import datetime, timedelta
from django.core.management.base import BaseCommand
from django.db import IntegrityError
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

from exchange_connections.selectors import get_exchange_symbols
from exchange_connections.services.klines_ingest import (
    build_models_from_rest,
    bulk_insert_klines,
)

client = Client()


class Command(BaseCommand):
    help = "Populate kline data from Binance API"

    def add_arguments(self, parser):
        parser.add_argument(
            "--ticker",
            type=str,
            help="Specific ticker to populate (defaults to all tickers)",
        )
        parser.add_argument(
            "--start-date",
            type=str,
            help='Start date in format "DD MMM YYYY" (defaults to 1 month ago)',
        )
        parser.add_argument(
            "--end-date",
            type=str,
            help='End date in format "DD MMM YYYY" (defaults to now)',
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=40000,
            help="Batch size for bulk insert (default: 40000)",
        )

    def handle(self, *args, **options):
        ticker = options.get("ticker")
        start_date = options.get("start_date") or get_1_month_ago_date()
        end_date = options.get("end_date") or datetime.now()
        batch_size = options.get("batch_size", 40000)

        self.stdout.write(f"Starting kline population...")

        if ticker:
            self.stdout.write(f"Processing single ticker: {ticker}")
            populate_kline_1m(ticker, start_date, end_date, batch_size)
        else:
            self.stdout.write(f"Processing all tickers from {start_date} to {end_date}")
            populate_all_klines_1m(start_date, end_date, batch_size)

        self.stdout.write(
            self.style.SUCCESS("Kline population completed successfully!")
        )


def populate_all_klines_1m(start_date, end_date, batch):
    """
    Populate 1-minute klines for all tickers from start_date to end_date using threading
    """
    print(f"Starting 1m kline population from {start_date} to {end_date}")

    tickers = get_exchange_symbols()

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(
                populate_kline_1m, ticker, start_date, end_date, batch
            ): ticker
            for ticker in tickers
        }
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                future.result()
            except Exception as exc:
                print(f"{ticker} generated an exception: {exc}")

    print("Completed 1m kline population for all tickers")


def populate_kline_1m(ticker, start_date, end_date, batch):
    """
    Populate 1-minute klines for a specific ticker with pagination
    """
    print(f"Fetching 1m klines for {ticker} from {start_date} to {end_date}")

    try:
        all_klines = fetch_all_klines_paginated(ticker, start_date, end_date)
        print(f"Total klines fetched: {len(all_klines)}")
        if not all_klines:
            print(f"No klines found for {ticker}")
            return
        kline_objects = build_models_from_rest(ticker, all_klines)
        if kline_objects:
            print(f"Created {len(kline_objects)} kline objects for {ticker}")
            try:
                attempted = bulk_insert_klines(kline_objects, chunk_size=batch)
                print(
                    f"Inserted (attempted) {attempted} kline records for {ticker} (duplicates ignored)"
                )
            except IntegrityError as e:
                print(f"IntegrityError bulk inserting {ticker}: {e}")
        print(f"Completed processing {ticker}")
    except Exception as e:
        print(f"Error processing {ticker}: {str(e)}")


def fetch_all_klines_paginated(ticker, start_date, end_date):
    """
    Fetch all klines by making multiple requests to handle the 1000 limit, with a max of 2400 requests per minute
    """
    all_klines = []

    if isinstance(start_date, str):
        current_start = datetime.strptime(start_date, "%d %b %Y")
    else:
        current_start = start_date

    if isinstance(end_date, str):
        final_end = datetime.strptime(end_date, "%d %b %Y")
    else:
        final_end = end_date

    while current_start < final_end:
        try:
            print(
                f"Fetching klines from {current_start.strftime('%d %b %Y %H:%M:%S')} for {ticker}"
            )

            klines = client.futures_historical_klines(
                ticker,
                Client.KLINE_INTERVAL_1MINUTE,
                current_start.strftime("%d %b %Y %H:%M:%S"),
                limit=1000,
            )

            if not klines:
                print(f"No more klines available for {ticker}")
                break

            all_klines.extend(klines)

            last_kline_close_time = datetime.fromtimestamp(klines[-1][6] / 1000)
            current_start = last_kline_close_time + timedelta(minutes=1)

            print(f"Fetched {len(klines)} klines. Total so far: {len(all_klines)}")

            time.sleep(0.1)

            if current_start >= final_end:
                break

        except Exception as e:
            print(f"Error fetching klines for {ticker} at {current_start}: {str(e)}")
            time.sleep(1)
            break

    print(f"Completed fetching klines for {ticker}. Total klines: {len(all_klines)}")
    return all_klines


def get_1_month_ago_date():
    """
    Get the date 1 month ago from today
    """
    return (datetime.now() - timedelta(days=30)).strftime("%d %b %Y")
