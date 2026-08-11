# Crypto Scanner Server

A Django-based server for scanning and analyzing cryptocurrency market data.

See [Tick and TRev chart data](docs/footprints.md) for the execution-driven
ClickHouse pipeline. Exact projector rules live in
[Tick/TRev projector semantics](docs/footprint-algorithm-semantics.md), and
[ClickHouse operations](clickhouse/README.md) documents the local schema.
The small single-host release checklist is in
[Footprint production deployment](docs/footprints-production.md).


# Populate Klines

Populate historical 1-minute kline data from exchange APIs.

```sh
# Binance
docker exec -it cs-binance-klines python manage.py populate_klines_binance --start-date "18 Jan 2026 08:00"

# Bybit
docker exec -it cs-bybit-klines python manage.py populate_klines_bybit --start-date "18 Jan 2026 08:00"

# Hyperliquid
# Only last 5000 candles available
docker exec -it cs-hyperliquid-klines python manage.py populate_klines_hyperliquid --start-date "14 Jan 2026 08:00"

# OKX
docker exec -it cs-okx-klines python manage.py populate_klines_okx --start-date "18 Jan 2026 08:00"
```

Options: `--ticker`, `--start-date`, `--end-date`, `--batch-size`
