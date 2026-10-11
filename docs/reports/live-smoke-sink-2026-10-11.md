# JSONL event sink smoke — 2026-10-11

Command: `cargo run -p cryptofeed-rs --features recording --example recording_public -- NEW_PATH`.
The example uses `run_sink(RecordingWriter, ...)` over the identified event stream.
The create_new output file remains in /private/tmp and is not committed.
No authentication, custom proxy or private channel was used. Exit 0.

| Stage | Observed |
| --- | --- |
| Source | OKX public Spot BTC-USDT trades |
| JSONL sink | 20 events, 6,765 bytes, LimitReached |
| Runtime | Managed shutdown completed before offline replay |
| Replay | 20 trade callbacks; exchange/symbol and footer/count checked |

Console: `capture=20 bytes=6765 end=LimitReached; offline replay=20 verified`.
This proves one public live source-to-sink/file-to-reader workflow. It does not
certify database delivery, fsync durability, every category/product, sustained
load, exactly-once writes or full field equality against a separate live copy.
Offline tests independently assert precise decimals, source generations, original
clocks, actual bounded prefix counts, lag and uncertain-write failures.
