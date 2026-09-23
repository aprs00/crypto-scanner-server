# Tick and TRev algorithm semantics

Both projectors consume the same ordered stream of individual executions.
Prices and quantities are exact integer multiples of the versioned instrument
tick size and quantity step. Source order—not wall-clock arrival order—is the
projector order.

## Common rules

- An execution is never split and its volume is never duplicated.
- Buy/sell volume and sparse price levels are accumulated from taker side.
- An instrument specification change closes a nonempty partial bar and starts
  a visibly broken segment.
- A collector reconnection changes `source_epoch`; the projector closes a
  nonempty partial bar and marks the next bar `breakBefore=true`.
- Event timestamps may regress while source rank increases. Projector order is
  still deterministic; stored bar end time is clamped to its open time for
  database ordering.
- Checkpoints contain only the current engine state and last source cursor.

## Tick

`tradesPerBar=N` means exactly N individual executions, regardless of their
quantities. The Nth execution belongs to and closes the bar. A gap/specification
boundary closes a nonempty partial bar; no empty bar is emitted.

## TRev

TRev uses `continuationTicks` and `reversalTicks`:

1. The first execution opens the segment. A later different price establishes
   the initial direction.
2. In the current direction, equality with the continuation threshold closes.
3. In the opposite direction, equality with the reversal threshold closes and
   changes direction.
4. The boundary execution belongs to the bar ending on that boundary.
5. A price jump may cross multiple boundaries. Intermediate synthetic bars
   have zero trade count/volume; the real execution is assigned once.
6. Expansion from one execution is bounded to prevent pathological input from
   exhausting memory.

Each market, threshold pair, and `lookbackDays` value has its own continuous
series. Its initial anchor is the requested number of days before creation,
rounded down to the hour; the first available execution at or after that
anchor opens the segment. The requested window is built forward once, with no
execution-count history cap. The anchor is retained as the series advances.
Changing `lookbackDays` creates a new series, so TRev boundaries can differ
between lookbacks. A repaired generation may rebase its anchor to keep the
requested window inside retained history.

Opening an existing series restores the complete checkpoint and processes only
trades after its source cursor. Restoring and replaying must produce exactly the
same candles and active state as uninterrupted forward processing. Without a
valid checkpoint, a new generation must bootstrap from its requested anchor;
neither backward construction nor initialization from an arbitrary recent trade
is valid. Source reconciliation replaces affected generations, and a checkpoint
whose required replay window has expired cannot be resumed silently.
