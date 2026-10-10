# Fixture provenance

The records in this directory are deterministic, sanitized official examples
or minimal references derived from official field tables. They are not live
traffic captures. Timestamps, prices, quantities, identifiers, connection IDs,
and symbol selections may be shortened or substituted while preserving the
documented wire shape and precision relevant to normalization.

| Fixture | Official source | Evidence represented |
| --- | --- | --- |
| `binance.http.v3` | [Binance Spot REST](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/general-endpoints), [USD-M REST](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Exchange-Information), [COIN-M REST](https://developers.binance.com/docs/derivatives/coin-margined-futures/market-data/rest-api/Exchange-Information) | Spot, USD-M, and COIN-M product identity plus REST book bootstrap. |
| `binance.ws.v3` | [Binance Spot streams](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams), [USD-M public streams](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public), [USD-M market streams](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/market), [current COIN-M streams](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-coin-m-futures/api/ws-streams) | Spot book ticker without event metadata; USD-M public/market endpoint split; non-USDT spot and dated/coin-margined identifiers; European-options ticker and trade references. |
| `bitget.http.v3` | [Bitget v3 instruments](https://www.bitget.com/api-doc/uta/public/Instruments), [Bitget v3 order book](https://www.bitget.com/api-doc/uta/public/OrderBook) | Unified v3 discovery categories and REST book shape. |
| `bitget.ws.v3` | [Bitget v3 guide](https://www.bitget.com/api-doc/uta/guide), [candlesticks](https://www.bitget.com/api-doc/uta/websocket/public/Candlesticks-Channel), [public trades](https://www.bitget.com/api-doc/uta/websocket/public/New-Trades-Channel), [depth](https://www.bitget.com/api-doc/uta/websocket/public/Order-Book-Channel), [liquidation](https://www.bitgetapp.com/api-doc/uta/websocket/public/Liquidation-Channel) | Successful control acknowledgement without `data`; text heartbeat; `topic: kline` with separate interval; batched trades; full-depth snapshot/update linkage. |
| `bitget.ws.v3.liq` | [Bitget v3 liquidation channel](https://www.bitgetapp.com/api-doc/uta/websocket/public/Liquidation-Channel) | One instType-scoped `liquidation` update (`side`/`price`/`amount`/`ts` row shape). |
| `bybit.http.v5` | [Bybit v5 instruments](https://bybit-exchange.github.io/docs/v5/market/instrument) | Spot, linear, inverse perpetual, and dated-futures identity. |
| `bybit.ws.v5` | [Bybit ticker](https://bybit-exchange.github.io/docs/v5/websocket/public/ticker), [public trades](https://bybit-exchange.github.io/docs/v5/websocket/public/trade), [order book](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook), [funding](https://bybit-exchange.github.io/docs/v5/websocket/public/funding), [all liquidation](https://bybit-exchange.github.io/docs/v5/websocket/public/all-liquidation), [tickers](https://bybit-exchange.github.io/docs/v5/market/tickers), [options instruments](https://bybit-exchange.github.io/docs/v5/market/instrument) | Spot ticker without BBO, level-1 BBO source, batched trades, replacement snapshot/delta semantics, derivative funding/liquidation references, open interest inside the derivative ticker stream, and an option ticker reference. |
| `gateio.http.v4` | [Gate spot REST](https://www.gate.com/docs/developers/apiv4/en/spot/), [perpetual futures REST](https://www.gate.com/docs/developers/apiv4/en/futures/), [delivery REST](https://www.gate.com/docs/developers/apiv4/en/delivery/) | Spot, USDT/BTC perpetual, and USDT delivery discovery; a legal zero multiplier on a non-delisted inverse contract; product-specific REST book bootstrap. |
| `gateio.ws.v4` | [Gate spot WebSocket v4](https://www.gate.com/docs/developers/apiv4/ws/), [perpetual futures WebSocket](https://www.gate.com/docs/developers/futures/), [delivery WebSocket](https://www.gate.com/docs/developers/delivery/ws/en/) | Current spot trade/ticker/depth, USDT/BTC perpetual and USDT delivery `futures.order_book_update`, derivative `futures.tickers` (funding rate, mark/index price, open interest), and `futures.book_ticker` (BBO with sizes) references. Delivery endpoint channel-prefix verified live 2026-08-07 (`futures.*` served; `delivery.*` unknown). |
| `okx.http.v5` | [OKX v5 instruments](https://app.okx.com/docs-v5/en/#public-data-rest-api-get-instruments) | Spot, swap, and dated-futures identity. |
| `okx.ws.v5` | [OKX v5 market-data WebSocket](https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-order-book-channel), [funding rate](https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-funding-rate-channel), [liquidation orders](https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-liquidation-orders-channel), [open interest](https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-open-interest-channel), [index tickers](https://www.okx.com/docs-v5/en/#market-data-ws-index-tickers) | Public/business endpoint routing, text heartbeat, batched trades, book snapshot/update sequencing, SWAP funding-rate, liquidation-order, open-interest, index-ticker, and option-ticker references. |

## 2026-10-08 public-channel additions

The appended rows are sanitized official field-table references, not live captures:

- `bitget.ws.v3`: [books1](https://www.bitget.com/docs/uta/websocket/public/Order-Book-Channel)
  and [derivative ticker](https://www.bitget.com/docs/uta/websocket/public/Tickers-Channel),
  including price/size precision and funding/OI/index/mark fields.
- `gateio.ws.v4`: [public liquidation orders](https://www.gate.com/docs/developers/futures/ws/#public-liquidates-order-api),
  signed fractional contract quantities and multi-contract rows.
- `okx.ws.v5`: [index tickers](https://app.okx.com/docs-v5/en/#public-data-websocket-index-tickers-channel),
  shared BTC-USD index mapped to configured dated futures in inline tests.

The appended Gate.io `futures.public_liquidates` subscription acknowledgement
was observed on the unauthenticated USDT perpetual WebSocket on 2026-10-08.
Connection/trace identifiers were removed; the corresponding inline parity
assertion verifies that this successful control response produces no event.
No live liquidation data was observed during the short smoke window.

## 2026-10-08 pre-push contract corrections

- Binance mark-price `P` is the estimated settlement price, verified from the
  [official USD-M field table](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/market#mark-price-stream).
  The appended payload and corrected mark-price parity assertion leave
  predicted funding rates unset.
- OKX `oiCcy` is a numeric coin quantity, verified from the
  [official open-interest field table](https://app.okx.com/docs-v5/en/#public-data-websocket-open-interest-channel).
  Its prior synthetic currency-code fixture was corrected together with the
  Decimal `coin_quantity` model and inline regression.
- Bitget dated native symbols in the appended book/liquidation examples are
  substituted field-table references to exercise configured catalog identity,
  not live instrument captures. Shape follows the official v3 depth and
  liquidation sources above.
- Gate.io public liquidation numeric price/size references include extra
  decimal precision to verify lossless JSON-number parsing before Decimal
  conversion; these are sanitized protocol examples, not live trades.

Additional pre-push regressions cover:

- Bybit snapshot/delta omission semantics from the official ticker contract
  above; the appended small snapshot and partial delta are field-table
  references, not live recordings. The runtime test verifies carried values,
  replacement snapshots, unrequested symbols, and fresh-session isolation.
- Bitget current v3 candle wire units `1H/4H/6H/12H/1D`, checked at
  [the current candlestick page](https://www.bitget.com/docs/uta/websocket/public/Candlesticks-Channel).
- OKX `1M/3M` calendar ends at the documented UTC+8 boundary. Appended leap-year
  and 31-day references preserve the official candle-array shape with substituted
  start timestamps; expectations follow the calendar rather than fixed durations.
- Binance mark-price index extraction uses the documented `i` and `E` fields
  from the appended mark-price payload, also tested with substituted native
  COIN-M perpetual and dated identifiers. Legacy standalone index references
  are not the active subscription source.

## 2026-10-09 OKX endpoint and catalog refresh

- [OKX port discontinuation announcement](https://www.okx.com/en-us/help/okx-websocket-port-8443-discontinuation-announcement)
  and [Global API changelog](https://www.okx.com/docs-v5/log_en/) are the sources
  for default WS port 443 and the recommended `openapi.okx.com` REST domain.
- `okx.ws.v5` endpoint labels use default TLS and separate the candle business
  subscription from public channels. Existing synthetic timestamps/payloads
  were retained; endpoint-label updates are not new live recordings.
- `okx.http.v5` includes a minimal regression reference derived from the public
  SPOT directory observed on 2026-10-09: a preopen row with empty base/quote,
  alongside a valid live BTC-USDT row. Its timestamp is substituted. The
  corresponding markets unit test verifies non-live filtering and rejects an
  additional synthetic malformed-live row rather than guessing its identity.

## 2026-10-10 subscription batching

The appended two Bybit spot subscribe frames are synthetic request references
from [the current connection guide](https://bybit-exchange.github.io/docs/v5/ws/connect):
ten args in the first request, one in the second. S0..S10 are substituted native
names, not listed-instrument or live-connect claims. The public parity test
asserts both request sizes and the complete ordered topic union. Connection
capacity sources and conservative SDK budgets are recorded in
[the planning guide](../docs/connection-planning.md).

The appended two Gate spot depth references use the official v4 delta shape
with substituted IDs/prices/timestamps. The runtime regression buffers IDs
101 and 102, starts one deferred REST job after the first, and preserves both.
It does not claim a newly recorded live bootstrap failure or relax bridging.

## 2026-10-10 readiness confirmation references

Appended request/response pairs use current official confirmation contracts:
[Binance Spot](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md),
[USD-M](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Live-Subscribing-Unsubscribing-to-streams),
[COIN-M](https://developers.binance.com/en/docs/products/derivatives-trading-coin-futures/websocket-market-streams/Live-Subscribing-Unsubscribing-to-streams),
[Bybit](https://bybit-exchange.github.io/docs/v5/ws/connect),
[Gate](https://www.gate.com/docs/developers/apiv4/ws/) and
[OKX](https://app.okx.com/docs-v5/en/#overview-websocket-subscribe).
These are synthetic field-table references with substituted timestamps and
sanitized connection IDs, not recordings from the manual smoke. Existing
Bitget v3 arg-based acknowledgement references remain applicable.

The Binance public parity test asserts explicit endpoint/topic/id planning;
readiness unit tests assert correlated confirmations and stale/duplicate guards.
The Bybit full-session double asserts acknowledgement before L2 initialization
using the appended snapshot shape. Readiness never treats handshake, arbitrary
market data, or a wrong request ID as complete subscription evidence. Manual
service results are recorded separately in the dated readiness report.

## 2026-10-10 catalog eligibility

Appended HTTP references exercise active/inactive statuses in inline markets
unit tests, using substituted symbols and timestamps. They are not live listings:

- Binance [Spot exchangeInfo](https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md),
  [USD-M market data](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data),
  and [COIN-M exchange information](https://developers.binance.com/docs/derivatives/coin-margined-futures/market-data/rest-api/Exchange-Information):
  `TRADING` versus inactive statuses. The COIN-M reference intentionally adds a
  contradictory legacy `status` field to test that `contractStatus` takes precedence;
  this extra field is a synthetic compatibility guard, not an official response claim.
- [Bitget v3 instruments](https://www.bitget.com/docs/catalog/market-market-data/market-instruments):
  `online`, `limit_open`, `limit_close`, `offline`, and `restrictedAPI`.
- [Bybit instruments](https://bybit-exchange.github.io/docs/v5/market/instrument):
  `Trading` versus `PendingOpen`, with cursor retention. Requests explicitly
  select Trading; mixed-response references are defensive filtering tests.
- [Gate spot pairs](https://www.gate.com/docs/developers/apiv4/en/spot/):
  `tradable` versus `untradable`; buyable/sellable remain public-data eligible.

The discovery engine uses scripted MarketCatalog snapshots to exercise additions,
removals and native aliases. These scripts are SDK policy tests, not exchange
wire fixtures. Existing OKX live/preopen and Gate derivative delisting references
remain applicable.

## 2026-10-10 sparse subscription packing

Appended per-channel configurations and send references select Trade BTC/ETH and
L2 BTC on one Spot endpoint. They are sanitized request-shape references, not live
traffic. The `sparse_subscriptions_pack_only_requested_native_topics` public parity
case asserts every selected topic/arg plus L2-only snapshot URLs on all five
exchanges. Wire shapes remain those of the official sources already listed above;
selection/capacity is SDK policy. Native confirmation IDs/timestamps retain the
separate readiness/send policies.

The appended Binance USD-M public/market URLs, Gate USDT/BTC derivative requests
and OKX public/business topic sets also back
`sparse_packing_preserves_required_product_and_endpoint_separation`. Current
sources were rechecked: [Binance Spot](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md),
[USD-M public](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public),
[USD-M market](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/market),
[Bitget v3 depth](https://www.bitget.com/docs/uta/websocket/public/Order-Book-Channel),
[Bybit connect](https://bybit-exchange.github.io/docs/v5/ws/connect),
[OKX subscriptions](https://app.okx.com/docs-v5/en/#overview-websocket-subscribe),
[Gate Spot](https://www.gate.com/docs/developers/apiv4/ws/) and
[Gate futures](https://www.gate.com/docs/developers/futures/).
No market-data parser expectation was changed. Scripted unsolicited ETH depth
messages verify absence of unrequested sync/bootstrap/cache work; they are not
claims that a correctly subscribed venue sends those unsolicited messages.

## 2026-10-10 typed market metadata

Appended Spot/Perpetual HTTP rows contain sanitized field-table metadata, with
substituted symbols/numbers/timestamps, not live constraints. The inline
`catalog_helpers_populate_verified_metadata_for_all_five_exchanges` regression
asserts price increments/precision, product identity and native names after actual
catalog helpers. `market_info::tests` covers the additional fields, applicability,
filter order, zero/scientific/exact-number conversion and malformed/conflicting
records. Existing dated-futures identity fixtures remain applicable.

Sources are the current Binance Spot/USD-M/COIN-M directory pages, Bitget v3
instruments, Bybit v5 instruments, OKX v5 instruments, and Gate v4 Spot/futures
field tables linked in [the metadata guide](../docs/market-metadata.md). Derivative
precision is never a tick/step substitute; Bitget multipliers are separate from
counts; deprecated Bybit Spot minOrderQty is not an active minimum. Contract
values/multipliers are retained without guessed currency conversions.

The extra Bitget JSON-number minimum-amount row is a synthetic numeric precision
stress reference (9007199254740993.123456789012), not a real BTC minimum. Its pure
metadata assertion reads literal JSON with arbitrary_precision before exact
Decimal conversion. Adversarial unit-only malformed/duplicate/inapplicable fields
are SDK validation probes, not claims that official endpoints emit those values.

## 2026-10-10 public REST snapshots

Appended ticker/book payloads match the new inline `rest::adapter::tests`
normalization cases for all five Spot surfaces, plus numeric-precision and Gate
derivative row-shape references. Values, IDs and timestamps are substituted;
these are not recordings of actual BTC quotes. Existing product identity
fixtures and the new request-plan tests cover futures routing independently.
Sources are the current public REST pages linked in [the guide](../docs/public-rest.md).

Gate Spot `current`/`update` are milliseconds, while derivative book fields are
seconds. The older Spot bootstrap reference's second-valued `current` was
corrected to millisecond input alongside its narrow public parity timestamp
assertion, without changing the normalized timestamp or sequence rule. A new
near-epoch product test prevents magnitude-based unit guessing in the REST API.

The long JSON-number Bitget book price/size and IDs above 2^53 are explicit
precision stress references, not real quotes. HTTP Retry-After/status/body-limit
checks use in-process HTTP-response doubles rather than inventing exchange
market-data fixture rows. Final real-service observations are recorded separately
in the dated REST report.

## 2026-10-10 funding history

Appended funding request/payload references mirror `rest::history::tests`' five
normalization families: Binance ascending millisecond bounds, Bitget v3 numeric
cursor/data.resultList/fundingRateTimestamp, Bybit end-time paging, OKX after and
actual realizedRate, and Gate second-valued t/r. These are sanitized field-table
references with substituted small epochs/rates, not actual crypto settlements
at the Unix epoch. Current primary sources are linked in
[funding history](../docs/funding-history.md).

The long negative JSON-number Binance rate is a synthetic exact-precision probe,
not a real market rate. The OKX forecast 0.009 versus actual -0.0001 deliberately
asserts actual-rate selection. SDK-only JSON cursor mutation/empty/duplicate/
conflicting/oversized/cancelled-page tests are not claims of venue behavior.
The dated live report separately records first/resume success; it does not label
budget-limited records as complete history.

## 2026-10-10 Binance/Bybit candle history

Appended candle arrays mirror `rest::candles::tests` normalization assertions,
using substituted OHLCV, counts and epochs from current official field tables
linked in [the guide](../docs/candle-history.md). The long Decimal price is a
synthetic precision probe. Neither response includes a close/finality flag.
Cursor corruption, duplicate/out-of-range pages and cancellation are unit-only
SDK probes. Captures are references, not automatically loaded test fixtures.

## 2026-10-10 remaining candle-history venues

Appended Bitget v3 seven-field, OKX nine-field confirm and Gate Spot eight-field /
perpetual object examples mirror `rest::candles::tests::new_venues_normalize_and_resume_exact_bar_windows`.
Small epochs and prices/volumes are substituted references, not real quotes.
Current primary sources are linked in [the guide](../docs/candle-history.md).
Request tests separately assert Bitget end rounding/90-day cap, Gate no-limit
second bounds and REST 1d mapping. The documented one-earlier Bitget overlap,
Gate range rounding and old seven-field Spot example are bounded regression
probes; actual first-failure/follow-up observations remain in the dated report.
