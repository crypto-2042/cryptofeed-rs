# Instrument naming across supported venues

Status: reference, verified against public REST catalogs on **2026-11-06**.
This document records native symbol formats only; it does not classify or
filter instruments in the SDK and does not change any runtime semantics.

## Native crypto formats

| Venue | Spot | Perpetual / futures | Notes |
| --- | --- | --- | --- |
| Binance | `BTCUSDT` | USDT-M `BTCUSDT`; coin-M `BTCUSD_PERP`; dated `BTCUSDT_YYMMDD` | Symbols are concatenated, uppercase. |
| Bybit | `BTCUSDT` | Linear `BTCUSDT`; inverse `BTCUSD` | Options use `BTC-...` identifiers. |
| OKX | `BTC-USDT` | Swap `BTC-USDT-SWAP`; dated `BTC-USDT-YYMMDD` | Hyphen-separated, uppercase. |
| KuCoin | `BTC-USDT` | `XBTUSDTM` (legacy XBT alias) and UTA contract symbols | The catalog maps XBT to BTC; futures are product-qualified. |
| Gate | `BTC_USDT` | Perpetual `BTC_USDT`; delivery `BTC_USDT_YYMMDD` | Underscore-separated. |
| Bitget | `BTCUSDT` | USDT-M `BTCUSDT`; coin-M `BTCUSD` | Concatenated, uppercase. |
| Kraken | `BTC/USD` | (spot integration only) | Slash-separated; quote is `USD`/`USDT`. |
| Upbit | `KRW-BTC`, `BTC-ETH` | — | Quote is a fiat (`KRW`) or crypto (`BTC`) market prefix. |
| Bithumb | `KRW-BTC` | — | Same prefix convention as Upbit. |
| Coinbase | `BTC-USD` | — | Hyphen-separated; Advanced Trade product id. |
| Deribit | — | `BTC-PERPETUAL`, dated `BTC-25DEC26`, options | Perpetual is a name, not a `PERP` suffix. |
| HTX | `btcusdt` (lowercase) | USDT-M `BTC-USDT`; notification `BTC-USDT` | Spot is lowercase concatenation; derivatives hyphenated. |
| Hyperliquid | `PURR/USDC` or `@{index}` | Coin name `BTC` | Spot exposes a display name plus a numeric pair index. |

The SDK normalizes base/quote to uppercase and product-qualifies the symbol
(`BASE-QUOTE`, with spot/perpetual/futures kind). Native case is preserved for
the exchange mapping where the venue is case-sensitive (for example
Hyperliquid coins).

## Tokenized-stock products

The venues below list tokenized equities / RWA stock products as **spot pairs**;
none of them provides native US equity market data (no SIP, pre/post-market,
auctions or corporate-action feed). Naming is venue-specific:

| Venue | Pattern | Native examples (base) | Quote | Normalized base | Observed status |
| --- | --- | --- | --- | --- | --- |
| Binance | `<TICKER>B` | `NVDAB`, `AAPLB`, `TSLAB`, `COINB`, `METAB`, `MSFTB` | USDT | `NVDAB` | Trading |
| Bybit | `<TICKER>X` | `AAPLX`, `TSLAX`, `NVDAX`, `COINX`, `METAX`, `AMZNX`, `GOOGLX` | USDT | `AAPLX` | Trading |
| OKX | `X<TICKER>` | `XAAPL`, `XNVDA`, `XQQQ`, `XSPY`, `XMETA` | USDT, USDC | `XAAPL` | Live |
| KuCoin | `<TICKER>X` | `AAPLX`, `TSLAX`, `NVDAX`, `AMZNX`, `COINX`, `MSTRX`, `INTCX` | USDT | `AAPLX` | Trading |
| Gate | `<TICKER>X` (xStocks), `<TICKER>ON` (Ondo), `<TICKER>G`, `<TICKER>3L/3S` | `TSLAX`, `TSLAON`, `QQQG`, `AAPL3L`, `SPY3L` | USDT | `TSLAX`, `TSLAON`, `AAPL3L` | Tradable |
| Bitget | `r<TICKER>` | base `rAAPL`, `rTSLA`, `rNVDA`, `rQQQ`, `rSPY`, symbol `RAAPLUSDT` | USDT | `RAAPL` | Online |
| HTX | plain lowercase `<ticker>` | `aapl`, `msft`, `nvda`, `tslax`, `coinx`, `mstrx` | USDT | `AAPL`, `TSLAX` | Mostly `offline`; only `ainvda` online in the stock-like set |
| Kraken | — | none found in current `AssetPairs` | — | — | — |
| Coinbase | — | `META-USD` is Metadium, not Meta Platforms | — | — | — |
| Upbit / Bithumb | — | `META`/`META2` are Metadium/MetaDAO | — | — | — |
| Deribit / Hyperliquid | — | no stock-like spot or perp names in current catalogs | — | — | — |

Examples of the same underlying across venues: Apple is `AAPLB` (Binance),
`XAAPL` (OKX), `AAPLX` (Bybit, KuCoin, Gate) and `RAAPL` (Bitget). Tesla adds a
second Gate issuer (`TSLAON`) plus leveraged tokens (`TSLA3L`, `TSLA3S`) that
are not stock exposure.

## Ambiguity and collision rules

- There is **no cross-venue standard**: the same stock can be prefix-marked,
  suffix-marked or unmarked, and one venue can list several issuer variants.
- `X` means different things: OKX uses a **prefix** (`XAAPL`); Bybit, KuCoin and
  Gate use a **suffix** (`AAPLX`).
- Bitget bases are mixed case in the native catalog (`rAAPL`); the SDK's
  normalized symbol is uppercase (`RAAPL-USDT`).
- HTX stock tokens carry **no marker** (`aapl`, `msft`) and most are offline;
  after normalization `AAPL-USDT` is indistinguishable from an equity-style
  ticker by name alone.
- Gate `3L`/`3S` tokens are daily-rebalanced leveraged products, not shares;
  `ON` and `G` are separate issuer families from `X`.
- Crypto collisions exist: `META` (Metadium/MetaDAO on Coinbase, Upbit,
  Bithumb), `COIN`-like crypto tickers, and the `B` suffix can plausibly occur
  in crypto names. Name parsing alone cannot classify an instrument safely.
- Stock tokens are quoted in `USDT` (OKX also `USDC`), not fiat `USD`; they are
  spot tokens with issuer/venue trading hours, premium/discount and
  corporate-action handling outside the SDK's model.

## Why tokenized stocks are not unified with crypto symbols

Tokenized-stock pairs stay venue-native spot products in this SDK. They are not
mapped to an equity-style ticker (`AAPLX` → `AAPL`), merged across issuers, or
given a special asset class in 0.1. The reasons are concrete:

- **Different products, same underlying.** Gate lists xStocks (`TSLAX`), Ondo
  (`TSLAON`) and leveraged tokens (`TSLA3L`/`TSLA3S`) side by side; they have
  different issuers, prices, liquidity and payoff. A single `TSLA` symbol would
  hide which product is being watched.
- **No equity market data.** These are not NYSE/Nasdaq feeds: there is no
  consolidated tape, pre/post-market, auction or corporate-action stream.
  Treating them as equity quotes would overstate what the SDK verifies.
- **Issuer and venue semantics.** Trading hours (24/7 or venue-defined),
  premium/discount to the underlying, redemption mechanics and corporate
  actions are issuer/venue concerns that the normalized crypto model does not
  represent.
- **Inconsistent markers.** `X` is a prefix on OKX and a suffix on
  Bybit/KuCoin/Gate; Bitget uses `r<TICKER>`; HTX is unmarked and mostly
  offline. Name parsing alone cannot classify a product, and the same ticker
  can collide with a crypto asset (`META` is Metadium/MetaDAO on several
  venues).
- **Leveraged tokens are not stock exposure.** `3L`/`3S` rebalance daily and
  have path-dependent returns; merging them with shares would be incorrect.

Consequently, if a venue catalog includes a tokenized-stock pair, it behaves as
that venue's spot symbol with the venue's own hours, spreads and issuer risk.
Classifying these products, rejecting leveraged tokens, or adding an underlying
mapping would be a separate cross-module design change with explicit product
decisions, not a naming-document change.

## Method

Catalogs queried on 2026-11-06: Binance `/api/v3/exchangeInfo`, Bybit
`/v5/market/instruments-info?category=spot`, OKX `/api/v5/public/instruments`,
KuCoin `/api/v2/symbols`, Gate `/api/v4/spot/currency_pairs`, Bitget
`/api/v2/spot/public/symbols`, HTX `/v1/common/symbols`, Upbit `/v1/market/all`,
Bithumb `/public/ticker/ALL_KRW`, Coinbase `/products`, Kraken
`/0/public/AssetPairs`, Hyperliquid `POST /info {"type":"spotMeta"}`.

No SDK filtering, validation or symbol semantics were changed by this
document. Classifying tokenized equities (and rejecting leveraged tokens) would
be a separate cross-module design change.
