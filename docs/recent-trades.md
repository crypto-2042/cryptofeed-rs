# Recent public trades

With the `trade` feature, `PublicRestClient::recent_trades(&symbol, limit)` returns
one bounded public batch as `Vec<Trade>`. Current profiles cover Binance
Spot/USD-M/COIN-M, Bitget v3 Spot/contracts, Bybit v5 Spot/linear/inverse,
OKX v5 Spot/swap/futures and Gate v4 Spot/perpetual/USDT delivery.
Check REST `supported_channels()` for `Channel::Trade`; WS catalog capabilities
remain a separate contract. No credentials or private endpoints are involved.

```rust,no_run
use cryptofeed_rs::prelude::*;
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let client = PublicRestClient::load(ExchangeId::Bybit, InstrumentKind::Spot).await?;
let trades = client.recent_trades(&Symbol::spot("BTC", "USDT"), 20).await?;
for trade in trades {
    println!("{:?} {} @ {}", trade.side, trade.amount, trade.price);
}
# Ok(()) }
```

| Venue | Current public endpoint | Limit |
| --- | --- | --- |
| Binance | Spot `api/v3/trades`, UM `fapi/v1/trades`, CM `dapi/v1/trades` | 1..1000 |
| Bitget | `api/v3/market/fills`, explicit category | 1..100 |
| Bybit | `v5/market/recent-trade`, explicit category | Spot 1..60; contracts 1..1000 |
| OKX | `api/v5/market/trades` | 1..500 |
| Gate | Spot/futures/delivery `trades`, product/settlement route | 1..1000 SDK cap; Spot native maximum is 1000; derivative tables omit a numeric maximum |

Unsupported product/unknown symbol and invalid limits fail before HTTP. Rows are
validated before returning the batch: missing/invalid fields, nonpositive prices
or sizes, negative time, duplicate IDs, mismatched transmitted identities or
Bybit categories, and responses larger than the requested limit fail explicitly.
An empty source response returns an empty vector. No rows are silently discarded.
The shared transport provides explicit proxy routing, HTTP timeout/body limits,
admission and venue cooldown. Dropping the future cancels its pending request;
there is no background paging or automatic query retry.

Prices and quantities convert directly to exact Decimal. Amount units remain
native: Spot and linear base quantities, Binance COIN-M/OKX derivative/Gate
contract quantities, and Bitget COIN-FUTURES quote quantity. Bybit inverse sizes
retain the native contract quantity; no multiplier/FX conversion is inferred.
`side` is the taker/aggressor side: Binance `isBuyerMaker=true` means Sell;
Bybit/OKX use their documented taker side. Gate derivative signed `size` gives
Buy when positive and Sell when negative; returned amount is its absolute value.

IDs remain exact native strings, including integer IDs above 2^53. Bitget uses
`execId`, not `execLinkId`. These recent queries preserve the current endpoint's
trade granularity. In particular Binance `/trades` returns individual executions;
Python's Binance REST mixin and Rust's Binance `aggTrade` WS source use aggregate
executions. Their ID namespaces differ: do not deduplicate REST individual IDs
against WS aggregate IDs or claim identical event counts. A historical aggregate
API must expose that distinction explicitly.

Results sort by exact native timestamp ascending, preserving source order for
ties. Distinct IDs at the same instant survive. Sorting precedes conversion to
the existing model's f64 seconds; that model may round sub-microsecond differences.
Gate Spot `create_time_ms` counts milliseconds with fractions. Gate contracts
use seconds with fractional precision despite the same field name. Both fall
back to second-valued `create_time` only when the preferred field is absent/null.
Other sources use native millisecond fields. All rows share local `received_ts`,
which never replaces a missing trade time. No implied volatility is inferred.

This API is **recent-only**: no time-range completeness, historical pagination,
cursor, subscription, feed counter update or WS recovery anchor is promised.
Bybit v5 and Bitget v3 recent fills do not expose history cursors here. [Native history/aggregate retrieval](trade-history.md) is implemented on
Binance/OKX/Gate Spot/perpetual profiles; archived-file import is not implemented. Repeating the recent call can overlap as the source moves.

Current official references checked on 2026-10-10:
[Binance Spot](https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#recent-trades-list),
[USD-M](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data),
[COIN-M](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-coin-m-futures/api/rest-api/market-data),
[Bitget v3 fills](https://www.bitget.com/docs/catalog/market/market-data),
[Bybit v5](https://bybit-exchange.github.io/docs/v5/market/recent-trade),
[OKX v5](https://app.okx.com/docs-v5/en/#order-book-trading-market-data-get-trades),
[Gate Spot](https://www.gate.com/docs/developers/apiv4/en/spot/),
[Gate perpetual](https://www.gate.com/docs/developers/apiv4/en/futures/),
[Gate delivery](https://www.gate.com/docs/developers/apiv4/en/delivery/).

Offline checks: `cargo test -p cryptofeed-rs --lib rest::trades`.
Manual public smoke: `cargo run -p cryptofeed-rs --example recent_trades_public`.

[Ten-product public smoke evidence](reports/live-smoke-recent-trades-2026-10-10.md) records 50 recent executions, distinct from historical completeness.
