# Market metadata snapshots

`MarketCatalog` now exposes `market(&Symbol)` and sorted `markets()` records in
addition to symbols, exact native lookup and selection. `exchange()`, `product()`
and `supported_channels()` identify the requested catalog and public WS channels
supported by its product and current Cargo features. This is the typed counterpart
to the inspected Python `Exchange.info()` symbols/instrument/tick/channel data,
with additional reported directory constraints and contract fields.

```rust,no_run
use cryptofeed_rs::prelude::*;

async fn example() -> Result<(), Box<dyn std::error::Error>> {
    let catalog = MarketCatalog::refresh(ExchangeId::Okx, InstrumentKind::Perpetual).await?;
    let info = catalog.market(&Symbol::perpetual("BTC", "USDT"))?;
    println!("{} tick={:?} lot={:?} face={:?} {:?}",
        info.exchange_symbol, info.price_increment, info.quantity_increment,
        info.contract_value, info.contract_value_currency);
    println!("WS channels: {:?}", catalog.supported_channels());
    Ok(())
}
```

Run `cargo run -p cryptofeed-rs --example metadata_public` to observe Spot and
Perpetual catalogs for all five active exchanges. It collects catalog failures
while continuing other observations and exits unsuccessfully if any failed.
Standalone transport-aware load/refresh and feed/discovery routing remain as
specified in [transport configuration](transport.md).

## Fields and source semantics

Every `MarketInfo` contains exchange, normalized symbol (including product kind),
exact native name, explicit price/quantity increments, minimum quantity/notional,
reported decimal-place counts, native status/type, settlement currency, contract
face value/currency and contract multiplier. Optional fields are None when
absent, null, empty or inapplicable. No decimal-place count is converted into a
step and no native denomination is guessed from the normalized quote.

| Exchange | Reported metadata mapping |
| --- | --- |
| Binance | Match `filters` by `filterType`, not array position: PRICE_FILTER tick, LOT_SIZE step/minimum, NOTIONAL or MIN_NOTIONAL lower bound. Derivative precision fields are retained independently of steps; marginAsset and contractSize are retained where supplied. |
| Bitget v3 | Price/quantity decimal-place counts remain counts. Futures priceMultiplier/quantityMultiplier and minOrderQty are separate values. minOrderAmount is quote-denominated. Spot does not reuse futures-only multiplier/minimum fields. |
| Bybit v5 | priceFilter tickSize; Spot basePrecision is the reported quantity quantum and minOrderAmt the amount minimum. Deprecated Spot minOrderQty is omitted. Derivatives use qtyStep/minOrderQty/minNotionalValue, priceScale and settleCoin. |
| OKX v5 | tickSz, lotSz and minSz are separate; derivative ctVal, ctValCcy, ctMult, ctType and settleCcy remain independent reported fields. |
| Gate v4 | Spot precision/amount_precision remain decimal-place counts, with min_base_amount/min_quote_amount. Derivatives retain order_price_round, order_size_min and quanto_multiplier; settlement comes from the validated request route. No quantity step is invented. |

Only existing eligible catalog rows are returned; this does not list delisted,
preopen or otherwise filtered markets. Native statuses are source strings, not
normalized trading permissions. Supported channels are product/build capabilities,
not proof that each directory row produces data on every channel. Feed capability,
feature and symbol preflight remains in force; no capability cell was expanded.

## Precision, units and limitations

Known decimal strings and JSON numbers convert directly to Decimal without float
conversion. Scientific mantissas/exponents are checked for exact representability;
malformed types, negative constraints and inexact/out-of-range values fail the
catalog load. Zero values are preserved, including legal zero Gate multipliers
and disabled native filters; callers must not assume every reported value is a
positive divisor. Unknown extra source fields remain ignored.

Quantity steps/minima describe reported native limit-order size constraints.
OKX derivatives use contracts while Spot uses base quantity; other native order
and feed quantity units remain exchange/product specific. No trade/book quantity
is converted by this API. Contract face value and multiplier are not combined or
used to synthesize another denomination. For example, Binance contractSize is
retained but its value currency remains None because that row has no explicit
value-currency field; OKX ctValCcy is retained directly. Gate quanto_multiplier
is a native multiplier, not a guessed contract face-value currency.

This is not a complete order validator: market-order filters, applicability flags,
price bands, fees, account/risk limits and private trading permissions are outside
this metadata surface. Binance's reported limit-order notional lower bound is the
maximum when both notional filters are present; this does not assert market-order
applicability. Do not interpret missing Bitget/Gate Spot increments as zero or
derive them from displayed precision. Additional endpoint-specific trading rules
are not silently fetched or substituted from legacy Python APIs.

## Snapshot and refresh behavior

Metadata is decoded from the same product-qualified directory responses as native
symbol identity, with no new per-symbol requests. `load` uses the existing 24-hour
transport-scoped cache; `refresh` forces new responses. The object is immutable
and does not update itself or running feeds. Catalog pages are not a globally
atomic exchange transaction. Conflicting duplicate identity/metadata records fail
instead of selecting one page's values arbitrarily; previous catalog objects
remain valid independent snapshots.

Missing optional fields in minimal references remain allowed. Invalid applicable
metadata can now fail directory loading/hydration, preserving strict precision
instead of silently ignoring corrupt known values. Inapplicable derivative fields
are not decoded as Spot constraints. Explicit native feed mappings still bypass
catalog existence/hydration as caller assertions.

`DiscoveryFeed` follows symbol/native-name membership changes, not order-constraint
changes. A metadata-only refresh does not restart an otherwise unchanged healthy
subscription; callers needing updated metadata obtain a new catalog snapshot.
The current Rust WS capability list is available even in metadata-only builds,
where supported_channels can be empty. [PublicRestClient](public-rest.md) separately reports its implemented REST methods;
history/pagination remain unfinished.

## Verification and references

Offline tests verify all five Spot/Perpetual catalog helper mappings, exact native
lookup/metadata retention, feature-aware channel lists, filter order, precision
versus steps, deprecated/inapplicable fields, contract value/currency/multiplier
separation, zero preservation, JSON numeric/scientific precision, malformed data,
duplicate conflicts and no subscription restart for metadata-only updates.
Matching sanitized HTTP references are documented in
[fixture provenance](../sample_data/SOURCES.md#2026-10-10-typed-market-metadata).
The separate [live report](reports/live-smoke-metadata-2026-10-10.md) records all ten
catalog observations plus the earlier COIN-M connectivity failure.

Primary field sources: [Binance Spot](https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md),
[USD-M directory](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data),
[COIN-M directory](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-coin-m-futures/api/rest-api/market-data),
[Bitget v3 instruments](https://www.bitget.com/docs/catalog/market-market-data/market-instruments),
[Bybit instruments](https://bybit-exchange.github.io/docs/v5/market/instrument),
[OKX instruments](https://app.okx.com/docs-v5/en/#public-data-rest-api-get-instruments),
[Gate Spot](https://www.gate.com/docs/developers/apiv4/en/spot/),
[Gate futures](https://www.gate.com/docs/developers/apiv4/en/futures/).
