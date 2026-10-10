# Bounded public funding history

With the `funding` feature, `PublicRestClient::funding_history` retrieves actual
regular settlement rates for existing perpetual/swap profiles on the five active
exchanges. It uses catalog-native identity, explicit transport and the shared
HTTP scheduler. It does not poll, access positions, enable dated-futures funding,
trade or expand the Binance OI scope.

```rust,no_run
use cryptofeed_rs::prelude::*;

async fn example() -> Result<(), Box<dyn std::error::Error>> {
    let client = PublicRestClient::load(ExchangeId::Bybit, InstrumentKind::Perpetual).await?;
    let symbol = Symbol::perpetual("BTC", "USDT");
    let query = FundingHistoryQuery::new(1790985600000, 1791590400000)?.limits(100, 2)?;
    let first = client.funding_history(&symbol, query.clone()).await?;
    println!("{} records, {:?}", first.records.len(), first.stop);
    if let Some(cursor) = first.next {
        let encoded = serde_json::to_vec(&cursor)?;
        let restored: FundingHistoryCursor = serde_json::from_slice(&encoded)?;
        let next = client.funding_history(&symbol, query.resume(restored)).await?;
        println!("{} additional records", next.records.len());
    }
    Ok(())
}
```

The range is half-open UTC milliseconds `[start_ms, end_ms)`, validated as positive
length within the APIs' signed-integer timestamp range. The query defaults to
100 rows/page and ten pages/call. Both limits can be 1–100; `maximum_rows()` is
page_size × max_pages, never more than 10,000 scanned rows per call. Out-of-range
and duplicate rows still consume scanning budget. Oversized source pages fail
rather than bypass the budget. Dropping the future cancels the pending page;
a failed page returns an error rather than a success containing undisclosed
partial progress.

## Native paging and actual rate selection

| Exchange | Current endpoint and continuation | Rate/time normalization |
| --- | --- | --- |
| Binance USD-M/COIN-M | fapi/dapi v1 fundingRate, ascending startTime; next start is observed maximum + 1 ms; inclusive endTime is query end − 1 | fundingRate, optional markPrice, fundingTime ms |
| Bitget v3 | market/history-fund-rate, category and numeric cursor 1–100; data.resultList; no legacy pageNo | fundingRate, fundingRateTimestamp ms |
| Bybit v5 | market/funding/history, linear/inverse and endTime; next inclusive end is observed minimum − 1 ms | fundingRate, fundingRateTimestamp ms |
| OKX v5 | public/funding-rate-history and exclusive after cursor; next is observed minimum | realizedRate (actual), fundingTime ms; predicted fundingRate is not substituted |
| Gate v4 | futures settlement funding_rate and inclusive to in seconds; next excludes the observed minimum | r, t in seconds, converted exactly to integer milliseconds |

Bybit does not receive a start-only query. Bybit/OKX/Gate page backward and filter
start locally. Bitget has no requested-time parameters; newer rows may need to
be scanned before the target window. Its offset-style page cursor can shift as
the source adds/removes data. SDK continuation filters the previously delivered
time segment and detects repeated full pages/no progress; it cannot turn moving
source pages into an atomic historical database.

Records are returned in ascending settlement-time order within each batch,
deduplicated by timestamp with conflicting rates/mark prices rejected. Resumed
backward batches are older than preceding batches, so callers collecting many
batches must merge/sort rather than assume concatenation is globally ascending.
Normal-crypto funding has one supported regular settlement per timestamp;
Binance Special/non-Regular rate types are explicitly unsupported instead of
flattened into regular funding. Spot/delivery requests fail before HTTP.

Rates use exact signed Decimal conversion, with no percent conversion. Existing
Funding models carry rate, historical settlement `exchange_ts` in seconds and
page receipt time. Binance's historical mark price is retained when provided.
`next_funding_time` and `predicted_rate` remain None: no interval or next estimate
is inferred. OKX rows lacking actual realizedRate in the requested segment fail
rather than using a forecast as actual settlement. Native integer timestamp
comparisons/cursors are not derived by rounding model f64 timestamps.

## Termination, retention and cursor scope

`FundingHistory` reports records, pages, scanned_rows, stop and optional next:

- `BudgetReached`: the batch used its page budget; next resumes the fixed query.
- `RangeBoundary`: paging reached the requested edge.
- `SourceExhausted`: the API returned an empty or short page.
- `SourceLimit`: Bitget's native cursor ceiling was reached; no cursor beyond it
  is offered.

There is deliberately no complete-history boolean. None next means the current
walk stopped without a supported continuation, not proof that every historical
settlement exists in the response. Bitget documents last 90 days and OKX up to
three months; retention, pre-listing periods, source omissions and moving pages
can leave a requested interval uncovered. Do not fill missing cycles with a
fixed eight-hour schedule: funding intervals can vary.

Version-1 JSON cursors bind exchange, normalized symbol, exact native mapping,
original range, page size and typed position. Untrusted/deserialized cursors are
validated before HTTP, including version/position and scope. A new catalog that
renames the native mapping requires a fresh query. Per-call max_pages may change;
page_size may not change while resuming. Cursor opacity is API encapsulation,
not encryption/authentication; cursor data is public market-query state.

## Evidence and remaining work

Offline tests cover all five normalization/paging families, half-open Gate
second granularity, bounded batches/resume/JSON roundtrip, cursor mismatch before
fetch, actual-versus-predicted rate, native identity, malformed/conflicting/too-
large pages, repeated-page detection, exact negative JSON rates, native page
ceiling and cancellation during a later page. Matching request references are
recorded in [provenance](../sample_data/SOURCES.md#2026-10-10-funding-history).
The separate [live report](reports/live-smoke-funding-history-2026-10-10.md) verifies
first/continued batches on five public services. Run `funding_history_public`
for a seven-day, two-batch bounded observation.

Public trade/candle history and pagination, recording/replay, sinks/aggregation,
NBBO and advanced resource policies remain unfinished. This increment is not a
claim of full Python REST parity or complete exchange retention coverage.

Primary sources: [Binance USD-M](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data),
[COIN-M](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-coin-m-futures/api/rest-api/market-data),
[Bitget v3 derivatives](https://www.bitget.com/docs/catalog/market/derivatives),
[Bybit history](https://bybit-exchange.github.io/docs/v5/market/history-fund-rate),
[OKX history](https://app.okx.com/docs-v5/en/#public-data-rest-api-get-funding-rate-history),
[Gate funding history](https://www.gate.com/docs/developers/apiv4/en/futures/).
