from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from footprints.types import NormalizedTrade, TakerSide


@dataclass(frozen=True, slots=True)
class StoredExecution:
    """Storage-neutral representation of one authoritative execution."""

    market_id: int
    instrument_spec_id: int
    source_epoch: int
    price_atoms: int
    quantity_atoms: int
    trade: NormalizedTrade

    def __post_init__(self) -> None:
        if self.market_id <= 0 or self.instrument_spec_id <= 0:
            raise ValueError("execution market and instrument specification IDs must be positive")
        if self.source_epoch <= 0:
            raise ValueError("execution source epoch must be positive")
        if self.trade.venue_sequence is None or self.trade.venue_sequence < 0:
            raise ValueError("execution venue sequence must be a nonnegative integer")
        if self.price_atoms <= 0 or self.quantity_atoms <= 0:
            raise ValueError("execution price and quantity atoms must be positive")

    @classmethod
    def from_trade(
        cls,
        *,
        market_id: int,
        instrument_spec_id: int,
        source_epoch: int,
        price_scale: int,
        quantity_scale: int,
        trade: NormalizedTrade,
    ) -> StoredExecution:
        """Resolve immutable decimal atoms before a batch is assigned."""

        return cls(
            market_id=market_id,
            instrument_spec_id=instrument_spec_id,
            source_epoch=source_epoch,
            price_atoms=_decimal_atoms(trade.price, price_scale),
            quantity_atoms=_decimal_atoms(trade.quantity, quantity_scale),
            trade=trade,
        )

    @property
    def identity(self) -> tuple[int, str]:
        return self.market_id, self.trade.trade_id

    @property
    def source_sequence(self) -> int:
        assert self.trade.venue_sequence is not None
        return self.trade.venue_sequence

    @property
    def immutable_payload(
        self,
    ) -> tuple[int, int, int, int, int, int, TakerSide, int]:
        return (
            self.source_epoch,
            self.source_sequence,
            self.trade.batch_index,
            self.trade.exchange_timestamp_us,
            self.price_atoms,
            self.quantity_atoms,
            self.trade.taker_side,
            self.instrument_spec_id,
        )


def _decimal_atoms(value: Decimal, scale: int) -> int:
    if not 0 <= scale <= 18:
        raise ValueError("execution decimal atom scale must be between zero and eighteen")
    scaled = value.scaleb(scale)
    integral = scaled.to_integral_value()
    if scaled != integral:
        raise ValueError(f"{value} exceeds configured execution scale {scale}")
    atoms = int(integral)
    if atoms < -(2**63) or atoms > 2**63 - 1:
        raise OverflowError(f"{value} exceeds execution int64 at scale {scale}")
    return atoms
