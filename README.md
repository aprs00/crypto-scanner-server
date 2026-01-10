# Crypto Scanner Server

A Django-based server for scanning and analyzing cryptocurrency market data.


# Populate Klines

Populate historical 1-minute kline data from exchange APIs.

```sh
# Binance
docker exec -it cs-binance-klines python manage.py populate_klines_binance --start-date "01 Jan 2026 08:00"

# Bybit
docker exec -it cs-bybit-klines python manage.py populate_klines_bybit --start-date "01 Jan 2026 08:00"

# Hyperliquid
# Only last 5000 candles available
docker exec -it cs-hyperliquid-klines python manage.py populate_klines_hyperliquid --start-date "01 Jan 2026 08:00"
```

Options: `--ticker`, `--start-date`, `--end-date`, `--batch-size`
