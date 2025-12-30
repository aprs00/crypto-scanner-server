# Crypto Scanner Server

A Django-based server for scanning and analyzing cryptocurrency market data.


# Kline Population Command

This module provides a Django management command to populate 1-minute kline data from the Binance API into your database.

## Usage

### Arguments
- `--ticker`: Specific ticker to populate (defaults to all symbols)
- `--start-date`: Start date in format `DD MMM YYYY` (defaults to 1 month ago)
- `--end-date`: End date in format `DD MMM YYYY` (defaults to now)
- `--batch-size`: Batch size for bulk insert (default: 40000)

## Example
Populate klines for all symbols from 1 month ago to now:

```sh
docker exec -it cs-exchange-connections python manage.py populate_klines
```

Populate klines for a specific ticker:

```sh
docker exec -it cs-binance-klines python manage.py populate_klines --start-date "10 Aug 2025"
```
