# Candle completion and delivery

Feed builders expose `.candle_policy(CandlePolicy::...)` with the `candles`
feature. The policy applies at the common runtime dispatcher for all active
exchanges, before handlers, raw/identified broadcasts, counters and normalized
data observations. It does not change the exchange subscription or wire parser.

| Policy | `closed = Some(true)` | `Some(false)` | `None` |
| --- | --- | --- | --- |
| `All` (Rust default) | deliver | deliver | deliver |
| `ClosedOnly` | deliver | omit | omit |
| `ClosedOrUnknown` | deliver | omit | deliver unchanged |

```rust
use cryptofeed_rs::prelude::*;

let feed = Binance::new()
    .candles()
    .candles_interval("1m")
    .candle_policy(CandlePolicy::ClosedOnly)
    .symbol("BTC-USDT")
    .build();
```

The inspected Python `feed.py` defaults `candle_closed_only=True`; its Binance
adapter filters `k.x == false`. Rust preserves its existing all-update default
for compatibility; explicitly choose `ClosedOnly` to obtain confirmed final
bars. `ClosedOrUnknown` lets callers omit known unfinished bars while accepting
sources without completion evidence. Unknown remains `None`, never `true`.

Completion is the normalized exchange flag. Neither a receive timestamp after
`end`, a timer, a new candle, nor a reconnect proves that a prior candle is final.
Existing parsers use Binance `x`, Bybit `confirm`, OKX's confirmation column and
Gate `w` when present. Bitget's current parser emits `None`; strict `ClosedOnly`
therefore delivers no Bitget candles. Choose `All` or `ClosedOrUnknown` if that
is the desired caller workflow. This increment makes no new protocol claims.

Filtering does not deduplicate repeated final updates or synthesize missing
bars. A ready subscription can have zero delivered candles while awaiting final
flags; readiness evidence and transport receipt diagnostics remain independent
of normalized event delivery. Configuration survives connection grouping,
reconnect and managed replacement through the existing feed configuration.

The offline `candle_completion_policy` regression covers all three flags/policies,
handler counts, both event streams, configuration identity, counters, unchanged
models and receipt-after-end behavior. Existing parser/session parity tests
remain enabled. Multi-handler/error/deadline behavior and recoverable L2
consumers are still separate unfinished phase 4 work.
