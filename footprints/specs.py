from __future__ import annotations

import json
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from footprints.models import FootprintMarket, InstrumentSpec
from footprints.types import RuntimeInstrument


def _json_request(url: str, payload: dict | None = None):
    body = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json", "User-Agent": "crypto-scanner-server/1.0"},
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.loads(response.read().decode())


def _binance_spec(symbol: str) -> tuple[Decimal, Decimal]:
    data = _json_request("https://fapi.binance.com/fapi/v1/exchangeInfo")
    instrument = next(item for item in data["symbols"] if item["symbol"] == symbol)
    filters = {item["filterType"]: item for item in instrument["filters"]}
    return Decimal(filters["PRICE_FILTER"]["tickSize"]), Decimal(filters["LOT_SIZE"]["stepSize"])


def _bybit_spec(symbol: str) -> tuple[Decimal, Decimal]:
    query = urllib.parse.urlencode({"category": "linear", "symbol": symbol})
    data = _json_request(f"https://api.bybit.com/v5/market/instruments-info?{query}")
    instrument = data["result"]["list"][0]
    return Decimal(instrument["priceFilter"]["tickSize"]), Decimal(instrument["lotSizeFilter"]["qtyStep"])


def _hyperliquid_spec(symbol: str) -> tuple[Decimal, Decimal]:
    meta = _json_request("https://api.hyperliquid.xyz/info", {"type": "meta"})
    instrument = next(item for item in meta["universe"] if item["name"] == symbol)
    size_decimals = int(instrument["szDecimals"])
    # Hyperliquid's five-significant-figure rule is price-dependent and also
    # explicitly permits every integer price.  The stable lossless grid is the
    # venue's maximum decimal precision, not the spacing around today's mark.
    price_quantum = Decimal(1).scaleb(size_decimals - 6)
    return price_quantum, Decimal(1).scaleb(-size_decimals)


SPEC_LOADERS = {
    "binance": _binance_spec,
    "bybit": _bybit_spec,
    "hyperliquid": _hyperliquid_spec,
}


def refresh_instrument_spec(market: FootprintMarket) -> InstrumentSpec:
    latest = market.instrument_specs.order_by("-effective_from").first()
    try:
        tick_size, quantity_step = SPEC_LOADERS[market.exchange](market.symbol)
    except Exception:
        if latest is None:
            raise
        return latest

    if latest and latest.tick_size == tick_size and latest.quantity_step == quantity_step:
        return latest

    minute = datetime.now(tz=timezone.utc).replace(second=0, microsecond=0)
    # Do not reinterpret executions already received in this minute under a
    # newly observed specification.  The collector resolves this timeline by
    # each execution's exchange timestamp.
    effective_from = minute if latest is None else minute + timedelta(minutes=1)
    spec, _ = InstrumentSpec.objects.get_or_create(
        market=market,
        effective_from=effective_from,
        defaults={"tick_size": tick_size, "quantity_step": quantity_step},
    )
    return spec


def load_runtime_instruments(exchange: str) -> dict[str, RuntimeInstrument]:
    instruments: dict[str, RuntimeInstrument] = {}
    markets = FootprintMarket.objects.filter(exchange=exchange, enabled=True)
    for market in markets:
        spec = refresh_instrument_spec(market)
        instruments[market.market_key] = RuntimeInstrument(
            market_id=market.pk,
            spec_id=spec.pk,
            symbol=market.symbol,
        )
    return instruments
