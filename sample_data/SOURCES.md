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
