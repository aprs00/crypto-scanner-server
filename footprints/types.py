from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum


class TakerSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


@dataclass(frozen=True, slots=True)
class NormalizedTrade:
    """One venue execution, represented without floating-point conversion.

    ``venue_sequence`` and ``batch_index`` provide deterministic venue ordering
    for exchanges whose trade identifiers are not naturally sortable.
    """

    market_key: str
    exchange_timestamp_us: int
    trade_id: str
    price: Decimal
    quantity: Decimal
    taker_side: TakerSide
    venue_sequence: int | None = None
    batch_index: int = 0

    def __post_init__(self) -> None:
        if not self.market_key:
            raise ValueError("market_key is required")
        if self.exchange_timestamp_us <= 0:
            raise ValueError("exchange_timestamp_us must be positive")
        if not self.trade_id:
            raise ValueError("trade_id is required")
        if self.price <= 0:
            raise ValueError("price must be positive")
        if self.quantity <= 0:
            raise ValueError("quantity must be positive")
        if self.batch_index < 0:
            raise ValueError("batch_index cannot be negative")

@dataclass(frozen=True, slots=True)
class RuntimeInstrument:
    market_id: int
    spec_id: int
    symbol: str
