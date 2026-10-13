# Recorded trades to custom state — 2026-10-11

Command: `cargo run -p cryptofeed-rs --features recording --example custom_aggregate_replay -- TRADE_RECORDING 1`.
Input was the temporary normalized OKX Spot BTC-USDT capture described in
[the sink smoke](live-smoke-sink-2026-10-11.md): 20 trades, 6,765 bytes, LimitReached.
The file is not committed. This run reads only that saved file and uses no
exchange/catalog/HTTP/WS connection. Exit 0.

| Check | Observed |
| --- | --- |
| Reader | Strict header/sequence/footer validation completed |
| State | Buy/sell counts grouped by feed ID/generation/exchange/symbol |
| Application bounds | At most 128 groups and 10,000 inputs per window |
| Windows | One second, 10 closed populated windows and 1 partial tail |
| Counts | Every state's buy+sell sum matched window.events; output total matched 20 input events |

Console: `offline custom events=20 closed=10 partial=1 per-source buy/sell counts verified`.
This is an application-defined deterministic calculation over previously captured
public input, not a new live exchange test, arbitrary callback memory bound,
rollback, lossless market completeness or external storage certification.
Offline tests separately verify every custom state/window field after replay,
subsecond boundaries, input caps and terminal initializer/reducer error/panic behavior.
