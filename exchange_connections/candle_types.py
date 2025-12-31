from dataclasses import dataclass
from decimal import Decimal
from typing import Optional


@dataclass(slots=True)
class NormalizedCandle:
    """Exchange-agnostic candle representation."""

    open_time_ms: int
    close_time_ms: int
    symbol: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    base_volume: Decimal
    number_of_trades: int
    quote_volume: Optional[Decimal] = None
    taker_buy_base_volume: Optional[Decimal] = None
    taker_buy_quote_volume: Optional[Decimal] = None

    def to_dict(self) -> dict:
        """Convert to dictionary for compatibility with existing code."""
        return {
            "t": self.open_time_ms,
            "T": self.close_time_ms,
            "s": self.symbol,
            "o": str(self.open),
            "h": str(self.high),
            "l": str(self.low),
            "c": str(self.close),
            "v": str(self.base_volume),
            "n": self.number_of_trades,
            "q": str(self.quote_volume) if self.quote_volume is not None else None,
            "V": (
                str(self.taker_buy_base_volume)
                if self.taker_buy_base_volume is not None
                else None
            ),
            "Q": (
                str(self.taker_buy_quote_volume)
                if self.taker_buy_quote_volume is not None
                else None
            ),
        }
