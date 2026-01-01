# Crypto Scanner Server

A Django-based server for scanning and analyzing cryptocurrency market data.


# Populate Klines

Populate historical 1-minute kline data from exchange APIs.

```sh
# Binance
docker exec -it cs-binance-klines python manage.py populate_klines_binance --ticker BTCUSDT --start-date "01 Dec 2025"

# Hyperliquid
# Only last 5000 candles available
docker exec -it cs-hyperliquid-klines python manage.py populate_klines_hyperliquid --ticker BTC --start-date "01 Dec 2025"
```

Options: `--ticker`, `--start-date`, `--end-date`, `--batch-size`
