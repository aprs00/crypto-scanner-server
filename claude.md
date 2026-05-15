# Claude Code Notes

## Pull Request Guidelines

Never include "Test plan" sections in PR descriptions.

## Git and Server Rules

- Never commit or push changes without explicit user approval.
- Never make changes on the SSH hetzner server — use it READ-ONLY for logs and debugging.

---

## Adding a new exchange

### 1. Research the exchange API

Collect these facts before writing code:

- **WebSocket URL(s).** Some exchanges split channels across endpoints (e.g. OKX puts ticker/book on `/ws/v5/public` but candles on `/ws/v5/business`). Confirm the candle channel works on the URL you pick.
- **Heartbeat.** Plain string `"ping"`/`"pong"` (OKX) vs JSON `{"op":"ping"}` (Bybit) vs library-managed (Binance). Note the disconnect timeout — ping interval must beat it.
- **Subscribe message format and batch limit.** Stay under the limit (use a conservative number like Bybit=10, OKX=100).
- **Candle payload.** Which array index is open/high/low/close/volume/quote_volume? Is `number_of_trades` present? Which field marks a confirmed/closed candle?
- **REST: instruments endpoint.** Path + filter criteria for live USDT linear perpetuals.
- **REST: historical candles.** Endpoint, max candles per request, pagination direction (forward vs backward), and whether the boundary timestamp is inclusive or exclusive.
- **Rate limits.** Set `request_delay` in the populate command accordingly.

Pick the existing exchange that's structurally closest to mirror — Bybit V5 is closest for most CEX V5-style APIs.

### 2. Files to change

| File | Change |
|---|---|
| `core/constants.py` | Add to `Exchange` enum; add block to `EXCHANGE_CONFIG` mirroring the closest peer. |
| `exchange_connections/constants.py` | Add cases to `get_btc_symbol()` and `get_sol_symbol()` — these drive the BTC gap-detection primary symbol. |
| `exchange_connections/<name>/__init__.py` | Export the collector class. |
| `exchange_connections/<name>/klines.py` | Subclass `BaseKlineCollector`. Implement `fetch_perpetual_symbols`, `normalize_candle`, `fetch_historical_klines`, `_on_message`, `_handle_kline`, `_on_open/_on_close/_on_error`, `_setup`, `_heartbeat_loop`, `connect_websocket`, `close_websocket`, `main`. Override `get_backfill_chunk_minutes()` to match the REST per-request limit. |
| `exchange_connections/management/commands/populate_klines_<name>.py` | Subclass `BasePopulateKlinesCommand`. Implement `fetch_all_klines_paginated`. Emit dicts with keys `{t, T, s, o, h, l, c, v, n, q, V, Q}` — `WsKline.to_model` consumes this. |
| `exchange_connections/management/commands/klines.py` | Add the new enum value to `EXCHANGE_KLINES_MAP` and the `--exchange` help string. |
| `exchange_connections/base/kline_collector.py` | Extend `_map_market_cap_symbol` to convert a CoinGecko base symbol (`BTC`) into the exchange's symbol format. Without this, market-cap rankings will silently match zero symbols. |
| `exchange_connections/services/klines_ingest.py` | Touch the comment at the dict-format branch if you want it to name your exchange. |
| `cointegration/management/commands/cointegration_scan.py`, `correlations/management/commands/incremental_correlations.py`, `zscore/management/commands/incremental_zscore.py` | Add the new exchange to the `--exchange` help string. (No `choices=` to update — these accept any string.) |
| `docker-compose.yml` | Add 4 services after the previous exchange's block: `<name>-klines`, `<name>-correlations`, `<name>-zscore`, `<name>-cointegration`. Mirror the bybit block exactly, swapping the name. |
| `README.md` | Add a populate example. |

No DB migration is needed — exchange and symbol rows are created on demand via `get_or_create` in `klines_ingest.py`.

### 3. Verify before declaring done

Run a dry-run suite without DB (the venv has Django but the docker DB+Redis may be down):

```python
DJANGO_SETTINGS_MODULE=core.settings python -c "
import django; django.setup()
from exchange_connections.<name>.klines import <Name>KlineCollector
c = <Name>KlineCollector.__new__(<Name>KlineCollector)
symbols = c.fetch_perpetual_symbols()              # expect ~exchange's symbol count
candles = c.fetch_historical_klines('<BTC_SYM>', start_ms, end_ms)  # check sort, range, close_time = open_time+59999
"
```

Then a 60–90s live WebSocket test:
- Bypass `__init__` (avoid Redis/DB), inject `symbols`, stub `save_kline`, run `connect_websocket()`, hold for ~80s.
- Assert: 0 subscribe errors, ≥3 heartbeat sends, ≥3 pongs, ≥1 confirmed candle per subscribed symbol, unconfirmed candles filtered out.

Finally, end-to-end deployment verification (in docker, with DB + Redis up):

```sh
docker exec -it cs-<name>-klines python manage.py populate_klines_<name> --ticker <BTC_SYM> --start-date "01 May 2026"
docker compose up -d <name>-klines
docker logs cs-<name>-klines
```

### Pitfalls to watch

- **WS URL split** (OKX-style): symptoms are clean handshake + every subscribe returning an error code; pings still work, so it looks healthy.
- **Heartbeat type**: sending JSON when the exchange wants a raw string (or vice versa) shows up as a forced disconnect after the server-side timeout (~30s).
- **History pagination boundary**: an `after` param that is exclusive vs inclusive flips your loop termination — verify by checking the oldest timestamp in the first response matches expectations.
- **Symbol format in `_map_market_cap_symbol`**: easy to miss; only surfaces when market-cap-ranked features return empty results for the new exchange.
- **`number_of_trades` field**: if the exchange doesn't expose it, store `0` (matches Bybit/OKX).
