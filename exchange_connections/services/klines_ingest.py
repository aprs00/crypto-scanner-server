from __future__ import annotations
from dataclasses import dataclass
from enum import IntEnum
from decimal import Decimal
from typing import Iterable, List, Sequence
from django.db import transaction
from utils.convert import ms_to_aware_datetime

from exchange_connections.models import Kline1m


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

        return Kline1m(
            start_time=ms_to_aware_datetime(d[RestKlineIndex.OPEN_TIME]),
            close_time=ms_to_aware_datetime(d[RestKlineIndex.CLOSE_TIME]),
            symbol=symbol,
            open=d[RestKlineIndex.OPEN],
            high=d[RestKlineIndex.HIGH],
            low=d[RestKlineIndex.LOW],
            close=d[RestKlineIndex.CLOSE],
            base_volume=d[RestKlineIndex.BASE_VOLUME],
            quote_volume=d[RestKlineIndex.QUOTE_VOLUME],
            number_of_trades=d[RestKlineIndex.NUMBER_OF_TRADES],
            taker_buy_base_volume=d[RestKlineIndex.TAKER_BUY_BASE_VOLUME],
            taker_buy_quote_volume=d[RestKlineIndex.TAKER_BUY_QUOTE_VOLUME],
            exchange=exchange,
            contract_type=contract_type,
        )


@dataclass(slots=True)
class RawWsKline:
    k: dict

    def to_model(
        self,
        exchange: str = "binance",
        contract_type: str = "perpetual",
    ) -> Kline1m:
        kd = self.k

        return Kline1m(
            start_time=ms_to_aware_datetime(kd["t"]),
            close_time=ms_to_aware_datetime(kd["T"]),
            symbol=kd["s"],
            open=Decimal(kd["o"]),
            high=Decimal(kd["h"]),
            low=Decimal(kd["l"]),
            close=Decimal(kd["c"]),
            base_volume=Decimal(kd["v"]),
            quote_volume=Decimal(kd["q"]),
            taker_buy_base_volume=Decimal(kd["V"]),
            taker_buy_quote_volume=Decimal(kd["Q"]),
            number_of_trades=Decimal(kd["n"]),
            exchange=exchange,
            contract_type=contract_type,
        )


def build_models_from_rest(
    symbol: str, raw_klines: Iterable[Sequence]
) -> List[Kline1m]:
    return [RawRestKline(k).to_model(symbol) for k in raw_klines]


def build_model_from_ws(
    kline_dict: dict, exchange: str = "binance", contract_type: str = "perpetual"
) -> Kline1m:
    return RawWsKline(kline_dict).to_model(
        exchange=exchange, contract_type=contract_type
    )


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
