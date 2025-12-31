from __future__ import annotations
from dataclasses import dataclass
from enum import IntEnum
from decimal import Decimal
from typing import Iterable, List, Sequence, Dict
from django.db import transaction
from utils.convert import ms_to_aware_datetime

from exchange_connections.models import Kline1m, Exchange, ContractType, Symbol


_exchange_cache: Dict[str, Exchange] = {}
_contract_type_cache: Dict[str, ContractType] = {}
_symbol_cache: Dict[str, Symbol] = {}


def get_or_create_exchange(name: str) -> Exchange:
    if name not in _exchange_cache:
        exchange, _ = Exchange.objects.get_or_create(name=name)
        _exchange_cache[name] = exchange

    return _exchange_cache[name]


def get_or_create_contract_type(name: str) -> ContractType:
    if name not in _contract_type_cache:
        contract_type, _ = ContractType.objects.get_or_create(name=name)
        _contract_type_cache[name] = contract_type

    return _contract_type_cache[name]


def get_or_create_symbol(
    name: str, exchange: Exchange, contract_type: ContractType
) -> Symbol:
    cache_key = f"{name}_{exchange.pk}_{contract_type.pk}"

    if cache_key not in _symbol_cache:
        symbol, _ = Symbol.objects.get_or_create(
            name=name,
            exchange=exchange,
            contract_type=contract_type,
        )
        _symbol_cache[cache_key] = symbol

    return _symbol_cache[cache_key]


class RestKlineIndex(IntEnum):
    OPEN_TIME = 0
    OPEN = 1
    HIGH = 2
    LOW = 3
    CLOSE = 4
    BASE_VOLUME = 5
    CLOSE_TIME = 6
    QUOTE_VOLUME = 7
    NUMBER_OF_TRADES = 8
    TAKER_BUY_BASE_VOLUME = 9
    TAKER_BUY_QUOTE_VOLUME = 10
    IGNORE = 11


@dataclass(slots=True)
class RawRestKline:
    data: Sequence

    def to_model(
        self,
        symbol: str,
        exchange: str = "binance",
        contract_type: str = "perpetual",
    ) -> Kline1m:
        d = self.data

        exchange_obj = get_or_create_exchange(exchange)
        contract_type_obj = get_or_create_contract_type(contract_type)
        symbol_obj = get_or_create_symbol(symbol, exchange_obj, contract_type_obj)

        return Kline1m(
            start_time=ms_to_aware_datetime(d[RestKlineIndex.OPEN_TIME]),
            close_time=ms_to_aware_datetime(d[RestKlineIndex.CLOSE_TIME]),
            symbol=symbol_obj,
            open=d[RestKlineIndex.OPEN],
            high=d[RestKlineIndex.HIGH],
            low=d[RestKlineIndex.LOW],
            close=d[RestKlineIndex.CLOSE],
            base_volume=d[RestKlineIndex.BASE_VOLUME],
            quote_volume=d[RestKlineIndex.QUOTE_VOLUME],
            number_of_trades=d[RestKlineIndex.NUMBER_OF_TRADES],
            taker_buy_base_volume=d[RestKlineIndex.TAKER_BUY_BASE_VOLUME],
            taker_buy_quote_volume=d[RestKlineIndex.TAKER_BUY_QUOTE_VOLUME],
            exchange=exchange_obj,
        )


@dataclass(slots=True)
class WsKline:
    """Handles WebSocket candle data from NormalizedCandle.to_dict()."""

    data: dict

    def to_model(
        self,
        exchange: str = "binance",
        contract_type: str = "perpetual",
    ) -> Kline1m:
        d = self.data

        exchange_obj = get_or_create_exchange(exchange)
        contract_type_obj = get_or_create_contract_type(contract_type)
        symbol_obj = get_or_create_symbol(d["s"], exchange_obj, contract_type_obj)

        quote_volume = Decimal(d["q"]) if d.get("q") is not None else None
        taker_buy_base = Decimal(d["V"]) if d.get("V") is not None else None
        taker_buy_quote = Decimal(d["Q"]) if d.get("Q") is not None else None

        return Kline1m(
            start_time=ms_to_aware_datetime(d["t"]),
            close_time=ms_to_aware_datetime(d["T"]),
            symbol=symbol_obj,
            open=Decimal(d["o"]),
            high=Decimal(d["h"]),
            low=Decimal(d["l"]),
            close=Decimal(d["c"]),
            base_volume=Decimal(d["v"]),
            quote_volume=quote_volume,
            taker_buy_base_volume=taker_buy_base,
            taker_buy_quote_volume=taker_buy_quote,
            number_of_trades=int(d["n"]),
            exchange=exchange_obj,
        )


def build_models_from_rest(
    symbol: str, raw_klines: Iterable[Sequence]
) -> List[Kline1m]:
    return [RawRestKline(k).to_model(symbol) for k in raw_klines]


def build_model_from_ws(
    kline_dict: dict, exchange: str = "binance", contract_type: str = "perpetual"
) -> Kline1m:
    return WsKline(kline_dict).to_model(exchange=exchange, contract_type=contract_type)


def bulk_insert_klines(objs: List[Kline1m], chunk_size: int = 10000) -> int:
    if not objs:
        return 0
    inserted = 0
    with transaction.atomic():
        for i in range(0, len(objs), chunk_size):
            batch = objs[i : i + chunk_size]
            Kline1m.objects.bulk_create(batch, ignore_conflicts=True)
            inserted += len(batch)
    return inserted
