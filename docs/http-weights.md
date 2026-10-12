# Binance public HTTP weights

Verified 2026-10-12 against the current official [Spot REST specification](https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md), [Spot general limits](https://developers.binance.info/en/docs/catalog/core-trading-spot-trading/api/rest-api/general), [USD-M market data](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data) and [COIN-M market data](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-coin-m-futures/api/rest-api/market-data). Python is not the fee-table source.

Actual SDK Binance public GETs now consume endpoint costs at admission. Directory
fetches, standalone REST/history and L2 bootstrap/resync use the same process
tracker, including the Spot public data hostname. Cached/coalesced directory
results do not create extra charges. No endpoint or normalized wire model changes.

| Operation | Spot | USD-M / COIN-M |
| --- | --- | --- |
| exchangeInfo | 20 | 1 |
| ticker/bookTicker | 2 with symbol, otherwise 4 | 2 with symbol, otherwise 5 |
| depth | limit ≤100: 5; ≤500: 25; ≤1000: 50; ≤5000: 250 | limit 5/10/20/50: 2; 100: 5; 500: 10; 1000: 20 |
| trades | 25 | 5 |
| aggTrades | 4 | 20 |
| klines | 2 | limit <100: 1; <500: 2; ≤1000: 5; ≤1500: 10 |
| fundingRate | unavailable | USD-M: one request in separate funding window; COIN-M: weight 1 |

Unspecified depth limits use the official defaults (Spot 100, futures 500);
unspecified futures klines limit is 500. Spot recent trades uses the current
weight 25. USD-M funding's documented 500 requests/5 minutes is separate from
REQUEST_WEIGHT; a general-IP weight is not inferred for that operation.

## Windows and scope

The SDK uses conservative rolling windows: Spot 6000 weight/60 seconds, a shared
USD-M/COIN-M pool of 2400 weight/60 seconds, and USD-M funding 500 requests/300
seconds. The compiled minute ceilings follow official published limits/examples;
the shared futures pool and rolling scheduling are SDK choices. Sharing futures
usage is conservative and does not assert that Binance shares both native pools.
An exchangeInfo response can lower its minute REQUEST_WEIGHT ceiling; it cannot
raise the compiled or previously observed ceiling during this process lifetime.
Other intervals and private order limits are ignored.

All requests must still satisfy HTTP capacity, process/scoped start spacing and
[native cooldowns](native-rate-limits.md). Weight, both clocks and admission are
checked together. Waiting releases both network permits. Only admitted requests
are charged; cancellation while waiting costs nothing, and a started request is
not refunded on cancellation or failure because it may have reached the server.
Unknown Binance hosts/routes, duplicate query keys and unsupported weight tiers
fail before network access rather than assuming a low cost.

```rust,no_run
use cryptofeed_rs::prelude::*;
# fn example() -> Result<(), Box<dyn std::error::Error>> {
let policy = ResourcePolicy::default().binance_weight_ceiling(100)?;
let transport = TransportConfig::direct()
    .with_resource_budget(ResourceBudget::new(policy));
# Ok(()) }
```

The optional ceiling accepts 1..=6000 and tightens each relevant window's capacity.
It is a threshold against **total process usage**, including other groups; it is
not a fresh ledger per ResourceBudget. The bucket's own lower capacity still
wins. A request costing more than its effective ceiling fails immediately, so a
ceiling below 20 cannot perform live Spot directory hydration. The policy is
immutable and clones retain it. Independent HTTP/WS group limits are unchanged.

[HTTP payload replay](http-replay.md) bypasses classification and live admission;
archived exchangeInfo limits do not affect real clients. Native quota responses
still retain their original error and JSON semantics.

## Validation and remaining scope

Eleven new offline regressions cover cost tiers, rolling expiry, stricter metadata,
composite admission, permit release, cancellation, exchange isolation, policy
bounds and replay without live weight changes. The synthetic reference
`sample_data/public-rest.weights.jsonl` records representative asserted costs;
it is not a traffic capture or a server saturation test.

[Other venue public count limits](public-http-frequency-audit.md) are now audited
against existing paced HTTP admission and verified native cooldowns; no Binance
weight table is inferred for them. Header used-weight feedback and fairness
remain open. Cross-process/machine quotas are explicitly deferred. Verified native
WS failures and opt-in proven minimum connection packing are now implemented.
These windows bound SDK admissions, not server receipt times; no live limit was
intentionally triggered.
