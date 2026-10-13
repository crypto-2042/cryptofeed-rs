# Recorded public trades to OHLCV — 2026-10-11

Command: `cargo run -p cryptofeed-rs --features recording --example aggregate_replay -- TRADE_RECORDING 1`.
Input was the temporary normalized OKX Spot BTC-USDT capture described in
[the sink smoke](live-smoke-sink-2026-10-11.md): 20 trades, 6,765 bytes, LimitReached.
The recording itself is not committed. This command uses only the saved file;
no exchange/catalog/HTTP/WS call is made. Exit 0.

| Check | Observed |
| --- | --- |
| Input | Strict normalized reader accepted header, sequence and footer |
| Window | One second, aligned to recorded elapsed zero |
| Output | 11 populated bars: 10 closed, 1 partial |
| Trade count | Output sum equals 20 input trades |
| Native amount volume | Output sum equals input amount sum |

Console: `offline trades=20 bars=11 closed=10 partial=1 count/volume verified`.
EOF left the last populated window partial. No empty windows were generated.
This is offline processing of previously captured public data, not a new live
exchange test or proof of complete market/trade coverage. Deterministic tests
separately compare all OHLCV bar fields between direct input and recording replay,
and cover arithmetic, clock, capacity and source-isolation failure boundaries.
