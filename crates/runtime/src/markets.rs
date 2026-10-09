use std::collections::{HashMap, HashSet};

use crate::exchange::ExchangeFeed;
use cryptofeed_core::{
    error::{Error, Result},
    exchange::{Channel, ExchangeId},
    symbol::{InstrumentKind, Symbol},
};
use serde_json::Value;

const BITGET_SPOT_CHANNELS: &[Channel] = &[
    Channel::L1Book,
    Channel::Ticker,
    Channel::Trade,
    Channel::L2Book,
    Channel::Candles,
];
/// Gate.io spot additionally exposes the L1 top-of-book via the
/// `book_ticker` stream (best bid/ask with sizes).
const GATEIO_SPOT_CHANNELS: &[Channel] = &[
    Channel::Ticker,
    Channel::Trade,
    Channel::L2Book,
    Channel::L1Book,
    Channel::Candles,
];
const BINANCE_SPOT_CHANNELS: &[Channel] = &[
    Channel::Ticker,
    Channel::Trade,
    Channel::L2Book,
    Channel::L1Book,
    Channel::Candles,
];
const BYBIT_SPOT_CHANNELS: &[Channel] = &[
    Channel::Ticker,
    Channel::Trade,
    Channel::L2Book,
    Channel::Candles,
    Channel::L1Book,
];
const OKX_SPOT_CHANNELS: &[Channel] = &[
    Channel::Ticker,
    Channel::Trade,
    Channel::L2Book,
    Channel::L1Book,
    Channel::Candles,
    Channel::Index,
];
/// Gate.io derivative products expose funding, open interest, index price,
/// and mark price inside the `futures.tickers` stream. Perpetuals additionally
/// expose `futures.public_liquidates`; delivery liquidation support is not
/// verified and stays rejected.
const GATEIO_DERIVATIVE_CHANNELS: &[Channel] = &[
    Channel::Ticker,
    Channel::Trade,
    Channel::L2Book,
    Channel::L1Book,
    Channel::Candles,
    Channel::OpenInterest,
    Channel::Index,
    Channel::MarkPrice,
];
const GATEIO_PERPETUAL_CHANNELS: &[Channel] = &[
    Channel::Ticker,
    Channel::Trade,
    Channel::L2Book,
    Channel::L1Book,
    Channel::Candles,
    Channel::Funding,
    Channel::OpenInterest,
    Channel::Index,
    Channel::MarkPrice,
    Channel::Liquidations,
];
const BINANCE_DERIVATIVE_CHANNELS: &[Channel] = &[
    Channel::Ticker,
    Channel::Trade,
    Channel::L2Book,
    Channel::L1Book,
    Channel::Candles,
    Channel::Funding,
    Channel::Liquidations,
    Channel::MarkPrice,
    Channel::Index,
];
const BINANCE_FUTURES_CHANNELS: &[Channel] = &[
    Channel::Ticker,
    Channel::Trade,
    Channel::L2Book,
    Channel::L1Book,
    Channel::Candles,
    Channel::Liquidations,
    Channel::MarkPrice,
    Channel::Index,
];
const BYBIT_DERIVATIVE_CHANNELS: &[Channel] = &[
    Channel::Ticker,
    Channel::Trade,
    Channel::L2Book,
    Channel::L1Book,
    Channel::Candles,
    Channel::Funding,
    Channel::Liquidations,
    Channel::MarkPrice,
    Channel::OpenInterest,
    Channel::Index,
];
const BYBIT_FUTURES_CHANNELS: &[Channel] = &[
    Channel::Ticker,
    Channel::Trade,
    Channel::L2Book,
    Channel::L1Book,
    Channel::Candles,
    Channel::Liquidations,
    Channel::MarkPrice,
    Channel::OpenInterest,
    Channel::Index,
];
const OKX_SWAP_CHANNELS: &[Channel] = &[
    Channel::Index,
    Channel::Ticker,
    Channel::Trade,
    Channel::L2Book,
    Channel::L1Book,
    Channel::Candles,
    Channel::Funding,
    Channel::Liquidations,
    Channel::MarkPrice,
    Channel::OpenInterest,
];
const OKX_FUTURES_CHANNELS: &[Channel] = &[
    Channel::Index,
    Channel::Ticker,
    Channel::Trade,
    Channel::L2Book,
    Channel::L1Book,
    Channel::Candles,
    Channel::Liquidations,
    Channel::MarkPrice,
    Channel::OpenInterest,
];
const BITGET_DERIVATIVE_CHANNELS: &[Channel] = &[
    Channel::L1Book,
    Channel::Funding,
    Channel::OpenInterest,
    Channel::Index,
    Channel::MarkPrice,
    Channel::Ticker,
    Channel::Trade,
    Channel::L2Book,
    Channel::Candles,
    Channel::Liquidations,
];
const BITGET_FUTURES_CHANNELS: &[Channel] = &[
    Channel::L1Book,
    Channel::OpenInterest,
    Channel::Index,
    Channel::MarkPrice,
    Channel::Ticker,
    Channel::Trade,
    Channel::L2Book,
    Channel::Candles,
    Channel::Liquidations,
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Capability {
    pub exchange: ExchangeId,
    pub product: InstrumentKind,
    pub channels: &'static [Channel],
}

const CAPABILITIES: &[Capability] = &[
    Capability {
        exchange: ExchangeId::Binance,
        product: InstrumentKind::Spot,
        channels: BINANCE_SPOT_CHANNELS,
    },
    Capability {
        exchange: ExchangeId::Binance,
        product: InstrumentKind::Perpetual,
        channels: BINANCE_DERIVATIVE_CHANNELS,
    },
    Capability {
        exchange: ExchangeId::Binance,
        product: InstrumentKind::Futures,
        channels: BINANCE_FUTURES_CHANNELS,
    },
    Capability {
        exchange: ExchangeId::Bitget,
        product: InstrumentKind::Spot,
        channels: BITGET_SPOT_CHANNELS,
    },
    Capability {
        exchange: ExchangeId::Bitget,
        product: InstrumentKind::Perpetual,
        channels: BITGET_DERIVATIVE_CHANNELS,
    },
    Capability {
        exchange: ExchangeId::Bitget,
        product: InstrumentKind::Futures,
        channels: BITGET_FUTURES_CHANNELS,
    },
    Capability {
        exchange: ExchangeId::Bybit,
        product: InstrumentKind::Spot,
        channels: BYBIT_SPOT_CHANNELS,
    },
    Capability {
        exchange: ExchangeId::Bybit,
        product: InstrumentKind::Perpetual,
        channels: BYBIT_DERIVATIVE_CHANNELS,
    },
    Capability {
        exchange: ExchangeId::Bybit,
        product: InstrumentKind::Futures,
        channels: BYBIT_FUTURES_CHANNELS,
    },
    Capability {
        exchange: ExchangeId::Gateio,
        product: InstrumentKind::Spot,
        channels: GATEIO_SPOT_CHANNELS,
    },
    Capability {
        exchange: ExchangeId::Gateio,
        product: InstrumentKind::Perpetual,
        channels: GATEIO_PERPETUAL_CHANNELS,
    },
    Capability {
        exchange: ExchangeId::Gateio,
        product: InstrumentKind::Futures,
        channels: GATEIO_DERIVATIVE_CHANNELS,
    },
    Capability {
        exchange: ExchangeId::Okx,
        product: InstrumentKind::Spot,
        channels: OKX_SPOT_CHANNELS,
    },
    Capability {
        exchange: ExchangeId::Okx,
        product: InstrumentKind::Perpetual,
        channels: OKX_SWAP_CHANNELS,
    },
    Capability {
        exchange: ExchangeId::Okx,
        product: InstrumentKind::Futures,
        channels: OKX_FUTURES_CHANNELS,
    },
];

pub fn capability_matrix() -> &'static [Capability] {
    CAPABILITIES
}

pub fn validate_feed(feed: &ExchangeFeed) -> Result<InstrumentKind> {
    if feed.channels.is_empty() {
        return Err(Error::InvalidConfiguration(
            "a feed must contain at least one channel".to_owned(),
        ));
    }

    let product = feed.product_kind()?;

    let capability = CAPABILITIES
        .iter()
        .find(|capability| capability.exchange == feed.exchange && capability.product == product)
        .ok_or_else(|| {
            Error::UnsupportedCapability(format!("{:?}/{:?}", feed.exchange, product))
        })?;
    for channel in &feed.channels {
        if !capability.channels.contains(channel) || !channel_feature_enabled(*channel) {
            return Err(Error::UnsupportedCapability(format!(
                "{:?}/{:?}/{:?}",
                feed.exchange, product, channel
            )));
        }
    }

    validate_candle_interval(feed)?;
    validate_l2_book_depth(feed, product)?;
    validate_l2_book_interval(feed, product)?;

    Ok(product)
}

fn validate_candle_interval(feed: &ExchangeFeed) -> Result<()> {
    if !feed.channels.contains(&Channel::Candles) {
        return Ok(());
    }
    let supported = match feed.exchange {
        ExchangeId::Binance => {
            crate::exchange::binance::adapter::candle_interval_wire(&feed.candle_interval).is_some()
        }
        ExchangeId::Bybit => crate::exchange::bybit::adapter::BybitAdapter::candle_interval_wire(
            &feed.candle_interval,
        )
        .is_some(),
        ExchangeId::Okx => {
            crate::exchange::okx::adapter::OkxAdapter::candle_interval_wire(&feed.candle_interval)
                .is_some()
        }
        ExchangeId::Bitget => {
            crate::exchange::bitget::adapter::BitgetAdapter::candle_interval_wire(
                &feed.candle_interval,
            )
            .is_some()
        }
        ExchangeId::Gateio => {
            crate::exchange::gateio::adapter::GateioAdapter::candle_interval_wire(
                &feed.candle_interval,
            )
            .is_some()
        }
        _ => false,
    };
    if supported {
        Ok(())
    } else {
        Err(Error::UnsupportedCapability(format!(
            "{:?} candle interval {:?}",
            feed.exchange, feed.candle_interval
        )))
    }
}

fn validate_l2_book_depth(feed: &ExchangeFeed, product: InstrumentKind) -> Result<()> {
    let Some(level) = feed.l2_book_depth else {
        return Ok(());
    };
    if !feed.channels.contains(&Channel::L2Book) {
        return Err(Error::InvalidConfiguration(
            "l2_book_depth requires the l2_book channel".to_owned(),
        ));
    }
    let supported = match feed.exchange {
        ExchangeId::Binance => matches!(level, 5 | 10 | 20),
        ExchangeId::Bybit => {
            crate::exchange::bybit::adapter::BybitAdapter::l2_book_depth_supported(
                level,
                if product == InstrumentKind::Option {
                    crate::exchange::bybit::adapter::BybitProduct::Option
                } else {
                    crate::exchange::bybit::adapter::BybitProduct::Linear
                },
            )
        }
        ExchangeId::Okx => {
            crate::exchange::okx::adapter::OkxAdapter::l2_book_depth_supported(level)
        }
        ExchangeId::Bitget => {
            crate::exchange::bitget::adapter::BitgetAdapter::l2_book_depth_supported(level)
        }
        ExchangeId::Gateio => {
            crate::exchange::gateio::adapter::GateioAdapter::l2_book_depth_supported(level)
        }
        _ => false,
    };
    if supported {
        Ok(())
    } else {
        Err(Error::UnsupportedCapability(format!(
            "{:?} l2 depth level {level}",
            feed.exchange
        )))
    }
}

fn validate_l2_book_interval(feed: &ExchangeFeed, product: InstrumentKind) -> Result<()> {
    let Some(interval) = feed.l2_book_interval.as_deref() else {
        return Ok(());
    };
    if !feed.channels.contains(&Channel::L2Book) {
        return Err(Error::InvalidConfiguration(
            "l2_book_interval requires the l2_book channel".to_owned(),
        ));
    }
    if feed.exchange != ExchangeId::Binance {
        return Err(Error::UnsupportedCapability(format!(
            "{:?} l2 book interval {interval}",
            feed.exchange
        )));
    }
    let supported = match product {
        InstrumentKind::Spot => matches!(interval, "100ms" | "1000ms"),
        InstrumentKind::Perpetual | InstrumentKind::Futures => {
            matches!(interval, "100ms" | "250ms" | "500ms")
        }
        _ => false,
    };
    if supported {
        Ok(())
    } else {
        Err(Error::UnsupportedCapability(format!(
            "binance {product:?} l2 book interval {interval}"
        )))
    }
}

#[allow(clippy::match_like_matches_macro)] // per-arm `cfg!` cannot use `matches!`
fn channel_feature_enabled(channel: Channel) -> bool {
    match channel {
        Channel::Candles => cfg!(feature = "candles"),
        Channel::Funding => cfg!(feature = "funding"),
        Channel::Liquidations => cfg!(feature = "liquidations"),
        Channel::Ticker => cfg!(feature = "ticker"),
        Channel::Trade => cfg!(feature = "trade"),
        Channel::L2Book => cfg!(feature = "orderbook"),
        Channel::OpenInterest => cfg!(feature = "openinterest"),
        Channel::Index => cfg!(feature = "index"),
        Channel::L1Book => cfg!(feature = "orderbook"),
        Channel::MarkPrice => cfg!(feature = "markprice"),
        _ => false,
    }
}

#[derive(Debug, Default)]
pub struct SymbolRegistry {
    normalized_to_exchange: HashMap<Symbol, String>,
    exchange_to_normalized: HashMap<(String, InstrumentKind), Symbol>,
    exchange_products: HashMap<String, HashSet<InstrumentKind>>,
}

impl SymbolRegistry {
    pub(crate) fn into_symbols(self) -> Vec<Symbol> {
        let mut symbols: Vec<_> = self.normalized_to_exchange.into_keys().collect();
        symbols.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        symbols
    }

    pub fn insert(&mut self, symbol: Symbol, exchange_symbol: &str) -> Result<()> {
        let exchange_symbol = exchange_symbol.to_ascii_uppercase();
        if exchange_symbol.is_empty() {
            return Err(Error::UnsupportedSymbol(symbol.as_str().to_owned()));
        }
        if matches!(
            self.normalized_to_exchange.get(&symbol),
            Some(existing) if existing != &exchange_symbol
        ) {
            return Err(Error::AmbiguousSymbol(symbol.as_str().to_owned()));
        }

        let reverse_key = (exchange_symbol.clone(), symbol.kind());
        if matches!(
            self.exchange_to_normalized.get(&reverse_key),
            Some(existing) if existing != &symbol
        ) {
            return Err(Error::AmbiguousSymbol(format!(
                "{exchange_symbol} ({:?})",
                symbol.kind()
            )));
        }

        self.normalized_to_exchange
            .insert(symbol.clone(), exchange_symbol.clone());
        self.exchange_to_normalized
            .insert(reverse_key, symbol.clone());
        self.exchange_products
            .entry(exchange_symbol)
            .or_default()
            .insert(symbol.kind());
        Ok(())
    }

    pub fn to_exchange(&self, symbol: &Symbol) -> Result<&str> {
        self.normalized_to_exchange
            .get(symbol)
            .map(String::as_str)
            .ok_or_else(|| Error::UnsupportedSymbol(symbol.as_str().to_owned()))
    }

    pub fn to_normalized(&self, exchange_symbol: &str, product: InstrumentKind) -> Result<&Symbol> {
        let exchange_symbol = exchange_symbol.to_ascii_uppercase();
        self.exchange_to_normalized
            .get(&(exchange_symbol.clone(), product))
            .ok_or_else(|| Error::UnsupportedSymbol(format!("{exchange_symbol} ({product:?})")))
    }

    pub fn to_normalized_unqualified(&self, exchange_symbol: &str) -> Result<&Symbol> {
        let exchange_symbol = exchange_symbol.to_ascii_uppercase();
        let products = self
            .exchange_products
            .get(&exchange_symbol)
            .ok_or_else(|| Error::UnsupportedSymbol(exchange_symbol.clone()))?;
        if products.len() != 1 {
            return Err(Error::AmbiguousSymbol(exchange_symbol));
        }
        self.to_normalized(
            &exchange_symbol,
            *products.iter().next().expect("one product"),
        )
    }
}

pub async fn resolve_feed_symbols(feed: &ExchangeFeed) -> Result<Vec<String>> {
    let product = validate_feed(feed)?;

    if !feed.exchange_symbols.is_empty() {
        if feed.exchange_symbols.len() != feed.symbols.len() {
            return Err(Error::InvalidConfiguration(
                "normalized and exchange symbol counts must match".to_owned(),
            ));
        }
        let mut registry = SymbolRegistry::default();
        for (symbol, exchange_symbol) in feed.symbols.iter().zip(&feed.exchange_symbols) {
            registry.insert(symbol.clone(), exchange_symbol)?;
        }
        return feed
            .symbols
            .iter()
            .map(|symbol| registry.to_exchange(symbol).map(ToOwned::to_owned))
            .collect();
    }

    let registry = fetch_symbol_registry(feed.exchange, product).await?;
    feed.symbols
        .iter()
        .map(|symbol| registry.to_exchange(symbol).map(ToOwned::to_owned))
        .collect()
}

pub(crate) async fn fetch_symbol_registry(
    exchange: ExchangeId,
    product: InstrumentKind,
) -> Result<SymbolRegistry> {
    fetch_symbol_registry_with_refresh(exchange, product, false).await
}

pub(crate) async fn fetch_symbol_registry_with_refresh(
    exchange: ExchangeId,
    product: InstrumentKind,
    refresh: bool,
) -> Result<SymbolRegistry> {
    let mut registry = SymbolRegistry::default();
    match exchange {
        ExchangeId::Binance => {
            if product == InstrumentKind::Spot {
                let payload =
                    fetch_json("https://api.binance.com/api/v3/exchangeInfo", refresh).await?;
                for item in array_at(&payload, &["symbols"])? {
                    add_spot_market(
                        &mut registry,
                        str_at(item, &["symbol"]),
                        str_at(item, &["baseAsset"]),
                        str_at(item, &["quoteAsset"]),
                    )?;
                }
            } else if product == InstrumentKind::Option {
                let payload =
                    fetch_json("https://eapi.binance.com/eapi/v1/exchangeInfo", refresh).await?;
                for item in array_at(&payload, &["optionSymbols"])? {
                    add_binance_option_market(&mut registry, item)?;
                }
            } else {
                for url in [
                    "https://fapi.binance.com/fapi/v1/exchangeInfo",
                    "https://dapi.binance.com/dapi/v1/exchangeInfo",
                ] {
                    let payload = fetch_json(url, refresh).await?;
                    add_binance_markets(&mut registry, &payload, product)?;
                }
            }
        }
        ExchangeId::Bitget => {
            let categories: &[&str] = if product == InstrumentKind::Spot {
                &["SPOT"]
            } else {
                &["USDT-FUTURES", "USDC-FUTURES", "COIN-FUTURES"]
            };
            for category in categories {
                let url =
                    format!("https://api.bitget.com/api/v3/market/instruments?category={category}");
                let payload = fetch_json(&url, refresh).await?;
                if product == InstrumentKind::Spot {
                    for item in array_at(&payload, &["data"])? {
                        add_spot_market(
                            &mut registry,
                            str_at(item, &["symbol"]),
                            str_at(item, &["baseCoin"]),
                            str_at(item, &["quoteCoin"]),
                        )?;
                    }
                } else {
                    add_bitget_markets(&mut registry, &payload, product)?;
                }
            }
        }
        ExchangeId::Bybit => {
            if product == InstrumentKind::Spot {
                let payload = fetch_json(
                    "https://api.bybit.com/v5/market/instruments-info?category=spot",
                    refresh,
                )
                .await?;
                for item in array_at(&payload, &["result", "list"])? {
                    add_spot_market(
                        &mut registry,
                        str_at(item, &["symbol"]),
                        str_at(item, &["baseCoin"]),
                        str_at(item, &["quoteCoin"]),
                    )?;
                }
            } else if product == InstrumentKind::Option {
                // Verified live 2026-08-07: `tickers?category=option`
                // requires a `baseCoin` parameter (PARAMS_ERROR otherwise);
                // the full option catalog pages from
                // `instruments-info?category=option` without a base coin.
                fetch_bybit_option_category(&mut registry, refresh).await?;
            } else {
                for category in ["linear", "inverse"] {
                    fetch_bybit_category(&mut registry, category, product, refresh).await?;
                }
            }
        }
        ExchangeId::Gateio => match product {
            InstrumentKind::Spot => {
                let payload =
                    fetch_json("https://api.gateio.ws/api/v4/spot/currency_pairs", refresh).await?;
                for item in gateio_contracts(&payload)? {
                    add_spot_market(
                        &mut registry,
                        str_at(item, &["id"]),
                        str_at(item, &["base"]),
                        str_at(item, &["quote"]),
                    )?;
                }
            }
            InstrumentKind::Perpetual => {
                for settle in ["usdt", "btc"] {
                    let url = format!("https://api.gateio.ws/api/v4/futures/{settle}/contracts");
                    let payload = fetch_json(&url, refresh).await?;
                    add_gateio_perpetual_markets(&mut registry, &payload, settle)?;
                }
            }
            InstrumentKind::Futures => {
                let payload = fetch_json(
                    "https://api.gateio.ws/api/v4/delivery/usdt/contracts",
                    refresh,
                )
                .await?;
                add_gateio_delivery_markets(&mut registry, &payload)?;
            }
            InstrumentKind::Unknown | InstrumentKind::Option | InstrumentKind::Margin => {
                return Err(Error::UnsupportedCapability(format!(
                    "{:?}/{:?} symbol discovery",
                    exchange, product
                )));
            }
            _ => {
                return Err(Error::UnsupportedCapability(format!(
                    "{:?}/{:?} symbol discovery",
                    exchange, product
                )));
            }
        },
        ExchangeId::Okx => {
            let inst_type = match product {
                InstrumentKind::Spot => "SPOT",
                InstrumentKind::Perpetual => "SWAP",
                InstrumentKind::Futures => "FUTURES",
                InstrumentKind::Option => "OPTION",
                InstrumentKind::Margin | InstrumentKind::Unknown => {
                    return Err(Error::UnsupportedCapability(format!(
                        "{:?}/{:?} symbol discovery; margin instruments share the spot instId form and require explicit exchange_symbol pairs",
                        exchange, product
                    )));
                }
                _ => {
                    return Err(Error::UnsupportedCapability(format!(
                        "{:?}/{:?} symbol discovery",
                        exchange, product
                    )));
                }
            };
            let url =
                format!("https://openapi.okx.com/api/v5/public/instruments?instType={inst_type}");
            let payload = fetch_json(&url, refresh).await?;
            if product == InstrumentKind::Spot {
                add_okx_spot_markets(&mut registry, &payload)?;
            } else {
                add_okx_markets(&mut registry, &payload, product)?;
            }
        }
        // Any exchange without a verified catalog (including the not-yet-
        // live Coinbase/Kraken builders) fails symbol discovery explicitly.
        _ => {
            return Err(Error::UnsupportedCapability(format!(
                "{:?}/{:?} symbol discovery",
                exchange, product
            )));
        }
    }
    Ok(registry)
}

fn add_binance_option_market(registry: &mut SymbolRegistry, item: &Value) -> Result<()> {
    // Native form: {BASE}-{YYMMDD}-{STRIKE}-{C|P}. The current eapi
    // `optionSymbols` response carries `underlying` (e.g. `BTCUSDT`) and
    // `quoteAsset` instead of a separate `baseAsset` (verified 2026-08-06);
    // the base is derived by stripping the quote suffix from the underlying.
    let exchange_symbol = required_str(item, "symbol")?;
    let parts: Vec<_> = exchange_symbol.split('-').collect();
    let [base_part, expiry, strike, option_type] = parts.as_slice() else {
        return Err(Error::MalformedData(format!(
            "invalid Binance option symbol {exchange_symbol}"
        )));
    };
    if !matches!(*option_type, "C" | "P") {
        return Err(Error::MalformedData(format!(
            "invalid Binance option type in {exchange_symbol}"
        )));
    }
    let Some(quote) = str_at(item, &["quoteAsset"]) else {
        // Rows without a quote currency cannot be normalized faithfully;
        // skip them instead of failing the whole catalog.
        return Ok(());
    };
    let underlying = str_at(item, &["underlying"]).unwrap_or(base_part);
    let base = underlying
        .strip_suffix(quote)
        .filter(|base| !base.is_empty())
        .unwrap_or(underlying);
    if *base_part != base {
        return Err(Error::MalformedData(format!(
            "Binance option {exchange_symbol} disagrees with underlying {underlying}"
        )));
    }
    registry.insert(
        Symbol::option(base, quote, expiry, strike, option_type),
        exchange_symbol,
    )
}

fn add_binance_markets(
    registry: &mut SymbolRegistry,
    payload: &Value,
    product: InstrumentKind,
) -> Result<()> {
    for item in array_at(payload, &["symbols"])? {
        let contract_type = required_str(item, "contractType")?;
        let item_product = if contract_type == "PERPETUAL" {
            InstrumentKind::Perpetual
        } else {
            InstrumentKind::Futures
        };
        if item_product != product {
            continue;
        }
        let exchange_symbol = required_str(item, "symbol")?;
        let base = required_str(item, "baseAsset")?;
        let quote = required_str(item, "quoteAsset")?;
        let symbol = match product {
            InstrumentKind::Perpetual => Symbol::perpetual(base, quote),
            InstrumentKind::Futures => dated_symbol(
                base,
                quote,
                exchange_symbol,
                str_at(item, &["deliveryDate"]),
                "Binance",
            )?,
            _ => continue,
        };
        registry.insert(symbol, exchange_symbol)?;
    }
    Ok(())
}

fn add_bitget_markets(
    registry: &mut SymbolRegistry,
    payload: &Value,
    product: InstrumentKind,
) -> Result<()> {
    for item in array_at(payload, &["data"])? {
        let contract_type = required_str(item, "type")?;
        let item_product = match contract_type {
            "perpetual" => InstrumentKind::Perpetual,
            "delivery" => InstrumentKind::Futures,
            other => {
                return Err(Error::MalformedData(format!(
                    "unknown Bitget futures type {other}"
                )));
            }
        };
        if item_product != product {
            continue;
        }
        let exchange_symbol = required_str(item, "symbol")?;
        let base = required_str(item, "baseCoin")?;
        let quote = required_str(item, "quoteCoin")?;
        let symbol = match product {
            InstrumentKind::Perpetual => Symbol::perpetual(base, quote),
            InstrumentKind::Futures => {
                let expiry = expiry_from_millis(required_str(item, "deliveryTime")?)?;
                Symbol::futures(base, quote, &expiry)
            }
            _ => continue,
        };
        registry.insert(symbol, exchange_symbol)?;
    }
    Ok(())
}

async fn fetch_bybit_category(
    registry: &mut SymbolRegistry,
    category: &str,
    product: InstrumentKind,
    refresh: bool,
) -> Result<()> {
    let mut cursor: Option<String> = None;
    let mut seen = HashSet::new();
    loop {
        let mut url = url::Url::parse("https://api.bybit.com/v5/market/instruments-info")
            .map_err(|error| Error::Transport(error.to_string()))?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("category", category);
            query.append_pair("limit", "1000");
            if let Some(cursor) = &cursor {
                query.append_pair("cursor", cursor);
            }
        }
        let payload = fetch_json(url.as_str(), refresh).await?;
        cursor = add_bybit_page(registry, &payload, product)?;
        let Some(next) = cursor.as_deref() else {
            return Ok(());
        };
        record_bybit_cursor(&mut seen, next)?;
    }
}

async fn fetch_bybit_option_category(registry: &mut SymbolRegistry, refresh: bool) -> Result<()> {
    let mut cursor: Option<String> = None;
    let mut seen = HashSet::new();
    loop {
        let mut url = url::Url::parse("https://api.bybit.com/v5/market/instruments-info")
            .map_err(|error| Error::Transport(error.to_string()))?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("category", "option");
            query.append_pair("limit", "1000");
            if let Some(cursor) = &cursor {
                query.append_pair("cursor", cursor);
            }
        }
        let payload = fetch_json(url.as_str(), refresh).await?;
        cursor = add_bybit_option_page(registry, &payload)?;
        let Some(next) = cursor.as_deref() else {
            return Ok(());
        };
        record_bybit_cursor(&mut seen, next)?;
    }
}

fn add_bybit_option_page(registry: &mut SymbolRegistry, payload: &Value) -> Result<Option<String>> {
    for item in array_at(payload, &["result", "list"])? {
        let exchange_symbol = required_str(item, "symbol")?;
        // The instruments-info contract carries `optionsType`
        // (`Call`/`Put`); cross-check it against the documented
        // `{C|P}` suffix so a catalog contradiction is explicit.
        if let Some(options_type) = str_at(item, &["optionsType"]) {
            let suffix = exchange_symbol
                .strip_suffix("-USDT")
                .or_else(|| exchange_symbol.strip_suffix("-USDC"))
                .and_then(|stripped| stripped.rsplit('-').next())
                .ok_or_else(|| {
                    Error::MalformedData(format!("invalid Bybit option symbol {exchange_symbol}"))
                })?;
            let expected = if matches!(suffix, "C" | "P") {
                suffix
            } else {
                ""
            };
            let expected = match expected {
                "C" => "Call",
                "P" => "Put",
                _ => {
                    return Err(Error::MalformedData(format!(
                        "invalid Bybit option type in {exchange_symbol}"
                    )));
                }
            };
            if options_type != expected {
                return Err(Error::MalformedData(format!(
                    "Bybit option {exchange_symbol} disagrees with optionsType {options_type}"
                )));
            }
        }
        add_bybit_option_market(registry, exchange_symbol)?;
    }

    let cursor = str_at(payload, &["result", "nextPageCursor"])
        .ok_or_else(|| {
            Error::MalformedData("Bybit options page is missing nextPageCursor".to_owned())
        })?
        .trim();
    Ok((!cursor.is_empty()).then(|| cursor.to_owned()))
}

fn add_bybit_option_market(registry: &mut SymbolRegistry, exchange_symbol: &str) -> Result<()> {
    // Native form: {BASE}-{EXPIRY}-{STRIKE}-{C|P}, with a documented -USDT
    // suffix for USDT-settled options; otherwise USDC settlement.
    let settle = if exchange_symbol.ends_with("-USDT") {
        "USDT"
    } else {
        "USDC"
    };
    let stripped = exchange_symbol
        .strip_suffix("-USDT")
        .unwrap_or(exchange_symbol);
    let parts: Vec<_> = stripped.split('-').collect();
    let [base, expiry, strike, option_type] = parts.as_slice() else {
        return Err(Error::MalformedData(format!(
            "invalid Bybit option symbol {exchange_symbol}"
        )));
    };
    if !matches!(*option_type, "C" | "P") {
        return Err(Error::MalformedData(format!(
            "invalid Bybit option type in {exchange_symbol}"
        )));
    }
    registry.insert(
        Symbol::option(base, settle, expiry, strike, option_type),
        exchange_symbol,
    )
}

fn add_bybit_page(
    registry: &mut SymbolRegistry,
    payload: &Value,
    product: InstrumentKind,
) -> Result<Option<String>> {
    for item in array_at(payload, &["result", "list"])? {
        let contract_type = required_str(item, "contractType")?;
        let item_product = if contract_type.ends_with("Perpetual") {
            InstrumentKind::Perpetual
        } else if contract_type.ends_with("Futures") {
            InstrumentKind::Futures
        } else {
            return Err(Error::MalformedData(format!(
                "unknown Bybit contract type {contract_type}"
            )));
        };
        if item_product != product {
            continue;
        }
        let exchange_symbol = required_str(item, "symbol")?;
        let base = required_str(item, "baseCoin")?;
        let quote = required_str(item, "quoteCoin")?;
        let symbol = match product {
            InstrumentKind::Perpetual => Symbol::perpetual(base, quote),
            InstrumentKind::Futures => {
                let expiry = expiry_from_millis(required_str(item, "deliveryTime")?)?;
                Symbol::futures(base, quote, &expiry)
            }
            _ => continue,
        };
        registry.insert(symbol, exchange_symbol)?;
    }

    let cursor = str_at(payload, &["result", "nextPageCursor"])
        .ok_or_else(|| {
            Error::MalformedData("Bybit instruments page is missing nextPageCursor".to_owned())
        })?
        .trim();
    Ok((!cursor.is_empty()).then(|| cursor.to_owned()))
}

fn record_bybit_cursor(seen: &mut HashSet<String>, cursor: &str) -> Result<()> {
    if !seen.insert(cursor.to_owned()) {
        return Err(Error::Protocol(format!(
            "Bybit repeated instruments cursor {cursor}"
        )));
    }
    Ok(())
}

fn add_okx_spot_markets(registry: &mut SymbolRegistry, payload: &Value) -> Result<()> {
    for item in array_at(payload, &["data"])? {
        if str_at(item, &["state"]).is_some_and(|state| state != "live") {
            continue;
        }
        add_spot_market(
            registry,
            str_at(item, &["instId"]),
            str_at(item, &["baseCcy"]),
            str_at(item, &["quoteCcy"]),
        )?;
    }
    Ok(())
}

fn add_okx_markets(
    registry: &mut SymbolRegistry,
    payload: &Value,
    product: InstrumentKind,
) -> Result<()> {
    for item in array_at(payload, &["data"])? {
        if str_at(item, &["state"]).is_some_and(|state| state != "live") {
            continue;
        }
        let inst_type = required_str(item, "instType")?;
        let expected_type = match product {
            InstrumentKind::Perpetual => "SWAP",
            InstrumentKind::Futures => "FUTURES",
            InstrumentKind::Option => "OPTION",
            _ => continue,
        };
        if inst_type != expected_type {
            continue;
        }
        if product == InstrumentKind::Futures
            && str_at(item, &["ruleType"]).is_some_and(|rule| rule != "normal")
        {
            continue;
        }

        let exchange_symbol = required_str(item, "instId")?;
        let mut parts = exchange_symbol.split('-');
        let base = parts
            .next()
            .filter(|part| !part.is_empty())
            .ok_or_else(|| {
                Error::MalformedData(format!("invalid OKX instrument {exchange_symbol}"))
            })?;
        let quote = parts
            .next()
            .filter(|part| !part.is_empty())
            .ok_or_else(|| {
                Error::MalformedData(format!("invalid OKX instrument {exchange_symbol}"))
            })?;
        let symbol = match product {
            InstrumentKind::Perpetual => Symbol::perpetual(base, quote),
            InstrumentKind::Futures => dated_symbol(
                base,
                quote,
                exchange_symbol,
                str_at(item, &["expTime"]),
                "OKX",
            )?,
            InstrumentKind::Option => {
                // OKX option instIds are {BASE}-{QUOTE}-{EXPIRY}-{STRIKE}-{C|P}.
                let parts: Vec<_> = exchange_symbol.split('-').collect();
                let [base, quote, expiry, strike, option_type] = parts.as_slice() else {
                    return Err(Error::MalformedData(format!(
                        "invalid OKX option instrument {exchange_symbol}"
                    )));
                };
                if !matches!(*option_type, "C" | "P") {
                    return Err(Error::MalformedData(format!(
                        "invalid OKX option type in {exchange_symbol}"
                    )));
                }
                Symbol::option(base, quote, expiry, strike, option_type)
            }
            _ => continue,
        };
        registry.insert(symbol, exchange_symbol)?;
    }
    Ok(())
}

fn add_gateio_perpetual_markets(
    registry: &mut SymbolRegistry,
    payload: &Value,
    settle: &str,
) -> Result<()> {
    let (expected_type, expected_quote) = match settle {
        "usdt" => ("direct", "USDT"),
        "btc" => ("inverse", "USD"),
        other => {
            return Err(Error::InvalidConfiguration(format!(
                "unsupported Gateio futures settlement {other}"
            )));
        }
    };

    for item in gateio_contracts(payload)? {
        if item
            .get("in_delisting")
            .and_then(Value::as_bool)
            .is_some_and(|value| value)
        {
            continue;
        }
        let exchange_symbol = required_str(item, "name")?;
        let contract_type = required_str(item, "type")?;
        let (base, quote) = gateio_pair(exchange_symbol)?;
        if contract_type != expected_type || quote != expected_quote {
            return Err(Error::MalformedData(format!(
                "Gateio {settle} contract {exchange_symbol} has type {contract_type} and quote {quote}"
            )));
        }
        registry.insert(Symbol::perpetual(base, quote), exchange_symbol)?;
    }
    Ok(())
}

fn add_gateio_delivery_markets(registry: &mut SymbolRegistry, payload: &Value) -> Result<()> {
    for item in gateio_contracts(payload)? {
        if item
            .get("in_delisting")
            .and_then(Value::as_bool)
            .is_some_and(|value| value)
        {
            continue;
        }
        let exchange_symbol = required_str(item, "name")?;
        let underlying = required_str(item, "underlying")?;
        if required_str(item, "type")? != "direct" {
            return Err(Error::MalformedData(format!(
                "Gateio USDT delivery contract {exchange_symbol} is not direct"
            )));
        }

        let (base, quote) = gateio_pair(underlying)?;
        if quote != "USDT" {
            return Err(Error::MalformedData(format!(
                "Gateio USDT delivery contract {exchange_symbol} has quote {quote}"
            )));
        }
        let prefix = exchange_symbol
            .strip_suffix(expiry_from_native(exchange_symbol)?)
            .and_then(|value| value.strip_suffix('_'));
        if prefix != Some(underlying) {
            return Err(Error::MalformedData(format!(
                "Gateio delivery contract {exchange_symbol} does not match underlying {underlying}"
            )));
        }
        let expiry = expiry_from_native(exchange_symbol)?;
        let expire_time = required_i64(item, "expire_time")?;
        let timestamp_expiry = expiry_from_seconds(expire_time)?;
        if !expiry.ends_with(&timestamp_expiry) {
            return Err(Error::MalformedData(format!(
                "Gateio delivery contract {exchange_symbol} disagrees with expire_time {expire_time}"
            )));
        }
        registry.insert(Symbol::futures(base, quote, expiry), exchange_symbol)?;
    }
    Ok(())
}

fn gateio_contracts(payload: &Value) -> Result<&[Value]> {
    payload.as_array().map(Vec::as_slice).ok_or_else(|| {
        Error::MalformedData("Gateio instrument response is not an array".to_owned())
    })
}

fn gateio_pair(value: &str) -> Result<(&str, &str)> {
    let mut parts = value.split('_');
    let base = parts.next().filter(|part| !part.is_empty());
    let quote = parts.next().filter(|part| !part.is_empty());
    if let (Some(base), Some(quote), None) = (base, quote, parts.next()) {
        return Ok((base, quote));
    }
    Err(Error::MalformedData(format!(
        "invalid Gateio contract pair {value}"
    )))
}

fn required_i64(value: &Value, field: &str) -> Result<i64> {
    let field_value = value
        .get(field)
        .ok_or_else(|| Error::MalformedData(format!("instrument is missing {field}")))?;
    field_value
        .as_i64()
        .or_else(|| field_value.as_str()?.parse().ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| Error::MalformedData(format!("instrument has invalid {field}")))
}

fn expiry_from_native(exchange_symbol: &str) -> Result<&str> {
    let expiry = exchange_symbol
        .rsplit(['-', '_'])
        .next()
        .filter(|value| matches!(value.len(), 6 | 8) && value.chars().all(|ch| ch.is_ascii_digit()))
        .ok_or_else(|| {
            Error::MalformedData(format!("instrument {exchange_symbol} has no dated suffix"))
        })?;
    Ok(expiry)
}

fn dated_symbol(
    base: &str,
    quote: &str,
    exchange_symbol: &str,
    delivery_millis: Option<&str>,
    exchange: &str,
) -> Result<Symbol> {
    let expiry = match expiry_from_native(exchange_symbol) {
        Ok(expiry) => expiry.to_owned(),
        Err(_) => expiry_from_millis(delivery_millis.ok_or_else(|| {
            Error::MalformedData(format!(
                "{exchange} delivery instrument {exchange_symbol} has no expiry"
            ))
        })?)?,
    };
    Ok(Symbol::futures(base, quote, &expiry))
}

fn expiry_from_millis(value: &str) -> Result<String> {
    let millis = value
        .parse::<i64>()
        .map_err(|_| Error::MalformedData(format!("invalid delivery timestamp {value}")))?;
    if millis <= 0 {
        return Err(Error::MalformedData(format!(
            "invalid delivery timestamp {value}"
        )));
    }
    let days = millis.div_euclid(86_400_000);
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 }.div_euclid(146_097);
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    Ok(format!("{:02}{month:02}{day:02}", year.rem_euclid(100)))
}

fn expiry_from_seconds(seconds: i64) -> Result<String> {
    let millis = seconds
        .checked_mul(1_000)
        .ok_or_else(|| Error::MalformedData(format!("invalid delivery timestamp {seconds}")))?;
    expiry_from_millis(&millis.to_string())
}

fn required_str<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    str_at(value, &[field])
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::MalformedData(format!("instrument is missing {field}")))
}

fn add_spot_market(
    registry: &mut SymbolRegistry,
    exchange_symbol: Option<&str>,
    base: Option<&str>,
    quote: Option<&str>,
) -> Result<()> {
    let (Some(exchange_symbol), Some(base), Some(quote)) = (exchange_symbol, base, quote) else {
        return Err(Error::MalformedData(
            "instrument is missing its native symbol, base, or quote".to_owned(),
        ));
    };
    if exchange_symbol.is_empty() || base.is_empty() || quote.is_empty() {
        return Err(Error::MalformedData(
            "instrument has an empty native symbol, base, or quote".to_owned(),
        ));
    }
    registry.insert(Symbol::spot(base, quote), exchange_symbol)
}

async fn fetch_json(url: &str, refresh: bool) -> Result<Value> {
    fetch_catalog_with(url, refresh, || fetch_catalog_json(url)).await
}

// Each waiter retains the gate; weak entries avoid retaining completed requests.
type CatalogRequest =
    tokio::sync::Mutex<Option<(std::time::Instant, std::result::Result<Value, String>)>>;
type CatalogRequests = std::sync::Mutex<HashMap<String, std::sync::Weak<CatalogRequest>>>;
static CATALOG_REQUESTS: std::sync::OnceLock<CatalogRequests> = std::sync::OnceLock::new();

async fn fetch_catalog_with<F, Fut>(url: &str, refresh: bool, fetch: F) -> Result<Value>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<Value>>,
{
    let requested_at = std::time::Instant::now();
    if !refresh {
        if let Some(value) = read_catalog_cache(url) {
            tracing::debug!(url, "serving exchange catalog from cache (24h TTL)");
            return Ok(value);
        }
    }
    let gate = {
        let mut requests = CATALOG_REQUESTS
            .get_or_init(|| std::sync::Mutex::new(HashMap::new()))
            .lock()
            .expect("catalog requests lock");
        requests.retain(|_, gate| gate.strong_count() > 0);
        if let Some(gate) = requests.get(url).and_then(std::sync::Weak::upgrade) {
            gate
        } else {
            let gate = std::sync::Arc::new(tokio::sync::Mutex::new(None));
            requests.insert(url.to_owned(), std::sync::Arc::downgrade(&gate));
            gate
        }
    };
    let mut completed = gate.lock().await;
    // Share only work completed since this call began. In particular, a new
    // refresh never reuses an earlier success or a failed request indefinitely.
    if let Some((at, result)) = completed.as_ref() {
        if *at >= requested_at {
            return result.clone().map_err(Error::Transport);
        }
    }
    if !refresh {
        if let Some(value) = read_catalog_cache(url) {
            return Ok(value);
        }
    }
    let result = fetch().await.map_err(|error| error.to_string());
    if let Ok(value) = &result {
        write_catalog_cache(url, value);
    }
    *completed = Some((std::time::Instant::now(), result.clone()));
    result.map_err(Error::Transport)
}

fn catalog_http_client() -> Result<&'static reqwest::Client> {
    static CLIENT: std::sync::OnceLock<std::result::Result<reqwest::Client, String>> =
        std::sync::OnceLock::new();
    CLIENT
        .get_or_init(|| {
            reqwest::Client::builder()
                .build()
                .map_err(|error| error.to_string())
        })
        .as_ref()
        .map_err(|error| Error::Transport(error.clone()))
}

async fn fetch_catalog_json(url: &str) -> Result<Value> {
    let attempt = async |attempt: u8| -> Result<Value> {
        let response = catalog_http_client()?
            .get(url)
            .timeout(std::time::Duration::from_secs(45))
            .header(reqwest::header::USER_AGENT, "cryptofeed-rs/0.1")
            .send()
            .await
            .map_err(|e| Error::Transport(format!("{url}: {e} (attempt {attempt})")))?;
        let mut response = response
            .error_for_status()
            .map_err(|e| Error::Transport(format!("{url}: {e} (attempt {attempt})")))?;
        if response
            .content_length()
            .is_some_and(|length| length > MAX_CATALOG_RESPONSE_BYTES as u64)
        {
            return Err(Error::MalformedData(format!(
                "{url}: catalog exceeds {MAX_CATALOG_RESPONSE_BYTES} bytes (attempt {attempt})"
            )));
        }

        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|e| Error::Transport(format!("{url}: {e} (attempt {attempt})")))?
        {
            if body.len().saturating_add(chunk.len()) > MAX_CATALOG_RESPONSE_BYTES {
                return Err(Error::MalformedData(format!(
                    "{url}: catalog exceeds {MAX_CATALOG_RESPONSE_BYTES} bytes (attempt {attempt})"
                )));
            }
            body.extend_from_slice(&chunk);
        }
        let value = serde_json::from_slice::<Value>(&body)
            .map_err(|e| Error::MalformedData(format!("{url}: {e} (attempt {attempt})")))?;
        validate_catalog_envelope(url, &value)?;
        Ok(value)
    };

    match attempt(1).await {
        Ok(value) => Ok(value),
        Err(first) => match attempt(2).await {
            Ok(value) => Ok(value),
            Err(second) => Err(Error::Transport(format!(
                "catalog fetch failed twice for {url}: first: {first}; second: {second}"
            ))),
        },
    }
}

const MAX_CATALOG_RESPONSE_BYTES: usize = 32 * 1024 * 1024;
const CATALOG_CACHE_TTL_SECS: u64 = 24 * 60 * 60;
type CatalogCache = std::sync::Mutex<HashMap<String, (std::time::SystemTime, Value)>>;
static CATALOG_CACHE: std::sync::OnceLock<CatalogCache> = std::sync::OnceLock::new();

fn catalog_cache() -> &'static CatalogCache {
    CATALOG_CACHE.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

fn read_catalog_cache(url: &str) -> Option<Value> {
    let mut cache = catalog_cache().lock().ok()?;
    let (stored_at, value) = cache.get(url).cloned()?;
    let age = std::time::SystemTime::now()
        .duration_since(stored_at)
        .ok()?
        .as_secs();
    if age > CATALOG_CACHE_TTL_SECS {
        cache.remove(url);
        return None;
    }
    Some(value)
}

fn write_catalog_cache(url: &str, value: &Value) {
    if let Ok(mut cache) = catalog_cache().lock() {
        cache.insert(
            url.to_owned(),
            (std::time::SystemTime::now(), value.clone()),
        );
    }
}

fn validate_catalog_envelope(url: &str, value: &Value) -> Result<()> {
    if let Some(code) = value.get("retCode").and_then(Value::as_i64) {
        if code != 0 {
            return Err(Error::MalformedData(format!(
                "{url}: exchange returned retCode {code}"
            )));
        }
    }
    if let Some(code) = value.get("code").and_then(Value::as_str) {
        if !matches!(code, "0" | "00000") {
            return Err(Error::MalformedData(format!(
                "{url}: exchange returned code {code}"
            )));
        }
    }
    if value.get("error").is_some_and(|error| !error.is_null()) {
        return Err(Error::MalformedData(format!(
            "{url}: exchange returned an error envelope"
        )));
    }
    Ok(())
}

fn array_at<'a>(value: &'a Value, path: &[&str]) -> Result<&'a [Value]> {
    let mut cursor = value;
    for key in path {
        cursor = cursor.get(*key).ok_or_else(|| {
            Error::MalformedData(format!("instrument response is missing {}", path.join(".")))
        })?;
    }
    cursor
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| Error::MalformedData(format!("{} is not an array", path.join("."))))
}

fn str_at<'a>(value: &'a Value, path: &[&str]) -> Option<&'a str> {
    let mut cursor = value;
    for key in path {
        cursor = cursor.get(*key)?;
    }
    cursor.as_str()
}

#[cfg(test)]
mod tests {
    use super::{
        SymbolRegistry, add_binance_markets, add_binance_option_market, add_bitget_markets,
        add_bybit_option_market, add_bybit_option_page, add_bybit_page,
        add_gateio_delivery_markets, add_gateio_perpetual_markets, add_okx_markets,
        capability_matrix, record_bybit_cursor, resolve_feed_symbols, validate_catalog_envelope,
        validate_feed,
    };
    use crate::exchange::binance::Binance;
    use crate::exchange::{ExchangeFeedBuilder, gateio::Gateio};
    use cryptofeed_core::{
        error::Error,
        exchange::ExchangeId,
        symbol::{InstrumentKind, Symbol},
    };
    use serde_json::json;

    #[tokio::test]
    async fn catalog_refresh_bypasses_cache_and_failed_refresh_preserves_it() {
        let url = "test://refresh-preserves-cache";
        super::write_catalog_cache(url, &json!({"version": 1}));
        let value =
            super::fetch_catalog_with(url, false, || async { panic!("cache hit must not fetch") })
                .await
                .unwrap();
        assert_eq!(value["version"], 1);
        let value = super::fetch_catalog_with(url, true, || async { Ok(json!({"version": 2})) })
            .await
            .unwrap();
        assert_eq!(value["version"], 2);
        assert!(
            super::fetch_catalog_with(url, true, || async {
                Err(Error::Transport("unavailable".into()))
            })
            .await
            .is_err()
        );
        assert_eq!(super::read_catalog_cache(url).unwrap()["version"], 2);
    }

    #[tokio::test]
    async fn overlapping_catalog_requests_share_success_or_failure() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        for (url, fail, refresh) in [
            ("test://cold-success", false, false),
            ("test://refresh-success", false, true),
            ("test://cold-failure", true, false),
        ] {
            let calls = Arc::new(AtomicUsize::new(0));
            let requests = (0..8).map(|_| {
                let calls = calls.clone();
                super::fetch_catalog_with(url, refresh, move || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    tokio::task::yield_now().await;
                    if fail {
                        Err(Error::Transport("unavailable".into()))
                    } else {
                        Ok(json!({"version": 1}))
                    }
                })
            });
            let results = futures::future::join_all(requests).await;
            assert_eq!(calls.load(Ordering::SeqCst), 1, "{url}");
            assert!(results.iter().all(|result| result.is_err() == fail));
            if fail {
                assert!(super::read_catalog_cache(url).is_none());
                assert!(
                    super::fetch_catalog_with(url, false, || async { Ok(json!({"version": 2})) })
                        .await
                        .is_ok()
                );
            }
        }
    }

    #[tokio::test]
    async fn different_catalog_urls_fetch_independently() {
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        let requests = ["test://independent-a", "test://independent-b"]
            .into_iter()
            .map(|url| {
                let barrier = barrier.clone();
                super::fetch_catalog_with(url, false, move || async move {
                    barrier.wait().await;
                    Ok(json!({"url": url}))
                })
            });
        let values = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            futures::future::join_all(requests),
        )
        .await
        .unwrap();
        assert!(values.iter().all(|value| value.is_ok()));
    }

    #[tokio::test]
    async fn cancelling_catalog_leader_does_not_block_later_requests() {
        let url = "test://cancelled-leader";
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(super::fetch_catalog_with(url, false, || async move {
            started_tx.send(()).unwrap();
            std::future::pending::<cryptofeed_core::error::Result<serde_json::Value>>().await
        }));
        started_rx.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            super::fetch_catalog_with(url, false, || async { Ok(json!({"version": 1})) }),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result["version"], 1);
    }

    #[test]
    fn okx_spot_catalog_skips_preopen_identity_without_guessing() {
        let mut registry = super::SymbolRegistry::default();
        super::add_okx_spot_markets(&mut registry, &json!({"data": [
            {"instId":"XBB-USDT","instType":"SPOT","state":"preopen","baseCcy":"","quoteCcy":""},
            {"instId":"BTC-USDT","instType":"SPOT","state":"live","baseCcy":"BTC","quoteCcy":"USDT"}
        ]})).unwrap();
        assert_eq!(registry.into_symbols(), [Symbol::spot("BTC", "USDT")]);
        assert!(
            super::add_okx_spot_markets(
                &mut super::SymbolRegistry::default(),
                &json!({"data":[
                    {"instId":"BTC-USDT","state":"live","baseCcy":"","quoteCcy":"USDT"}
                ]})
            )
            .is_err()
        );
    }

    #[test]
    fn empty_spot_catalog_identity_returns_an_error() {
        let mut registry = super::SymbolRegistry::default();
        for (native, base, quote) in [
            ("BTC-USDT", "", "USDT"),
            ("BTC-USDT", "BTC", ""),
            ("", "BTC", "USDT"),
        ] {
            assert!(matches!(
                super::add_spot_market(&mut registry, Some(native), Some(base), Some(quote)),
                Err(Error::MalformedData(_))
            ));
        }
    }

    #[test]
    fn catalog_error_envelopes_are_rejected_before_caching() {
        assert!(validate_catalog_envelope("bybit", &json!({"retCode": 10001})).is_err());
        assert!(validate_catalog_envelope("bitget", &json!({"code": "42900"})).is_err());
        assert!(validate_catalog_envelope("gate", &json!({"error": {"message": "busy"}})).is_err());
        assert!(validate_catalog_envelope("okx", &json!({"code": "0"})).is_ok());
    }
    use std::collections::HashSet;

    #[test]
    fn capability_matrix_is_non_empty_and_channel_consistent() {
        let matrix = capability_matrix();
        assert!(!matrix.is_empty());
        for capability in matrix {
            assert!(!capability.channels.is_empty());
            assert!(matches!(
                capability.product,
                InstrumentKind::Spot | InstrumentKind::Perpetual | InstrumentKind::Futures
            ));
            for channel in capability.channels {
                assert!(
                    super::channel_feature_enabled(*channel),
                    "{:?}/{:?} channel {:?} lacks a feature gate",
                    capability.exchange,
                    capability.product,
                    channel
                );
            }
        }
        // Every active exchange appears in the matrix at least once.
        for exchange in [
            ExchangeId::Binance,
            ExchangeId::Bitget,
            ExchangeId::Bybit,
            ExchangeId::Okx,
            ExchangeId::Gateio,
        ] {
            assert!(
                matrix.iter().any(|c| c.exchange == exchange),
                "{exchange:?} missing from capability matrix"
            );
        }
    }

    #[test]
    fn registry_resolves_both_directions_with_product_identity() {
        let mut registry = SymbolRegistry::default();
        let symbol = Symbol::perpetual("BTC", "USD");
        registry.insert(symbol.clone(), "BTCUSD").unwrap();

        assert_eq!(registry.to_exchange(&symbol).unwrap(), "BTCUSD");
        assert_eq!(
            registry
                .to_normalized("BTCUSD", InstrumentKind::Perpetual)
                .unwrap(),
            &symbol
        );
    }

    #[test]
    fn registry_rejects_ambiguous_normalized_mapping() {
        let mut registry = SymbolRegistry::default();
        let symbol = Symbol::spot("BTC", "USD");
        registry.insert(symbol.clone(), "BTCUSD").unwrap();

        assert!(matches!(
            registry.insert(symbol, "XBTUSD"),
            Err(Error::AmbiguousSymbol(_))
        ));
    }

    #[test]
    fn native_symbol_requires_product_when_shared() {
        let mut registry = SymbolRegistry::default();
        registry
            .insert(Symbol::spot("BTC", "USD"), "BTCUSD")
            .unwrap();
        registry
            .insert(Symbol::perpetual("BTC", "USD"), "BTCUSD")
            .unwrap();

        assert!(matches!(
            registry.to_normalized_unqualified("BTCUSD"),
            Err(Error::AmbiguousSymbol(_))
        ));
    }

    #[test]
    fn preflight_rejects_unknown_and_mixed_product_symbols() {
        let unknown = Binance::new().ticker().symbol("BTCUSD").build();
        assert!(matches!(
            validate_feed(&unknown),
            Err(Error::UnsupportedSymbol(_))
        ));

        let mixed = Binance::new()
            .ticker()
            .instrument(Symbol::spot("BTC", "USDT"))
            .instrument(Symbol::perpetual("ETH", "USDT"))
            .build();
        assert!(matches!(
            validate_feed(&mixed),
            Err(Error::InvalidConfiguration(_))
        ));
    }

    #[test]
    fn preflight_rejects_unsupported_product_channel_combination() {
        let feed = Binance::new().funding().symbol("BTC-USDT").build();
        assert!(matches!(
            validate_feed(&feed),
            Err(Error::UnsupportedCapability(_))
        ));
    }

    #[test]
    fn preflight_allows_verified_derivative_cells_only() {
        let binance = Binance::new()
            .funding()
            .liquidations()
            .instrument(Symbol::perpetual("BTC", "USDT"))
            .build();
        assert_eq!(validate_feed(&binance).unwrap(), InstrumentKind::Perpetual);

        for exchange in [ExchangeId::Bitget, ExchangeId::Okx] {
            let feed = ExchangeFeedBuilder::new(exchange)
                .ticker()
                .trade()
                .l2_book()
                .candles()
                .instrument(Symbol::futures("BTC", "USD", "240628"))
                .build();
            assert_eq!(validate_feed(&feed).unwrap(), InstrumentKind::Futures);

            let unsupported_funding = ExchangeFeedBuilder::new(exchange)
                .funding()
                .instrument(Symbol::futures("BTC", "USD", "240628"))
                .build();
            assert!(matches!(
                validate_feed(&unsupported_funding),
                Err(Error::UnsupportedCapability(_))
            ));
        }

        let bybit_funding = ExchangeFeedBuilder::new(ExchangeId::Bybit)
            .funding()
            .liquidations()
            .instrument(Symbol::perpetual("BTC", "USDT"))
            .build();
        assert_eq!(
            validate_feed(&bybit_funding).unwrap(),
            InstrumentKind::Perpetual
        );

        let bybit_spot_funding = ExchangeFeedBuilder::new(ExchangeId::Bybit)
            .funding()
            .instrument(Symbol::spot("BTC", "USDT"))
            .build();
        assert!(matches!(
            validate_feed(&bybit_spot_funding),
            Err(Error::UnsupportedCapability(_))
        ));

        let okx_swap_funding = ExchangeFeedBuilder::new(ExchangeId::Okx)
            .funding()
            .liquidations()
            .instrument(Symbol::perpetual("BTC", "USDT"))
            .build();
        assert_eq!(
            validate_feed(&okx_swap_funding).unwrap(),
            InstrumentKind::Perpetual
        );

        let okx_futures_liquidations = ExchangeFeedBuilder::new(ExchangeId::Okx)
            .liquidations()
            .instrument(Symbol::futures("BTC", "USD", "260925"))
            .build();
        assert_eq!(
            validate_feed(&okx_futures_liquidations).unwrap(),
            InstrumentKind::Futures
        );

        let okx_futures_funding = ExchangeFeedBuilder::new(ExchangeId::Okx)
            .funding()
            .instrument(Symbol::futures("BTC", "USD", "260925"))
            .build();
        assert!(matches!(
            validate_feed(&okx_futures_funding),
            Err(Error::UnsupportedCapability(_))
        ));

        let bitget_liquidations = ExchangeFeedBuilder::new(ExchangeId::Bitget)
            .liquidations()
            .instrument(Symbol::perpetual("BTC", "USDT"))
            .build();
        assert_eq!(
            validate_feed(&bitget_liquidations).unwrap(),
            InstrumentKind::Perpetual
        );

        let bitget_spot_liquidations = ExchangeFeedBuilder::new(ExchangeId::Bitget)
            .liquidations()
            .instrument(Symbol::spot("BTC", "USDT"))
            .build();
        assert!(matches!(
            validate_feed(&bitget_spot_liquidations),
            Err(Error::UnsupportedCapability(_))
        ));

        // Index follows the documented convention: OKX spot row exposes it,
        // Binance derivative rows expose the per-symbol index price stream,
        // and unsupported rows reject it explicitly.
        let okx_index = ExchangeFeedBuilder::new(ExchangeId::Okx)
            .index()
            .instrument(Symbol::spot("BTC", "USD"))
            .exchange_symbol("BTC-USD")
            .build();
        assert_eq!(validate_feed(&okx_index).unwrap(), InstrumentKind::Spot);

        let binance_index = ExchangeFeedBuilder::new(ExchangeId::Binance)
            .index()
            .instrument(Symbol::spot("BTC", "USD"))
            .build();
        assert!(matches!(
            validate_feed(&binance_index),
            Err(Error::UnsupportedCapability(_))
        ));

        let binance_derivative_index = ExchangeFeedBuilder::new(ExchangeId::Binance)
            .index()
            .instrument(Symbol::perpetual("BTC", "USDT"))
            .build();
        assert_eq!(
            validate_feed(&binance_derivative_index).unwrap(),
            InstrumentKind::Perpetual
        );

        let binance_derivative_open_interest = ExchangeFeedBuilder::new(ExchangeId::Binance)
            .open_interest()
            .instrument(Symbol::perpetual("BTC", "USDT"))
            .build();
        assert!(matches!(
            validate_feed(&binance_derivative_open_interest),
            Err(Error::UnsupportedCapability(_))
        ));

        let bybit_l1 = ExchangeFeedBuilder::new(ExchangeId::Bybit)
            .l1_book()
            .instrument(Symbol::perpetual("BTC", "USDT"))
            .build();
        assert_eq!(validate_feed(&bybit_l1).unwrap(), InstrumentKind::Perpetual);

        let okx_l1 = ExchangeFeedBuilder::new(ExchangeId::Okx)
            .l1_book()
            .instrument(Symbol::spot("BTC", "USDT"))
            .build();
        assert_eq!(validate_feed(&okx_l1).unwrap(), InstrumentKind::Spot);

        let binance_l1 = ExchangeFeedBuilder::new(ExchangeId::Binance)
            .l1_book()
            .instrument(Symbol::spot("BTC", "USDT"))
            .build();
        assert_eq!(validate_feed(&binance_l1).unwrap(), InstrumentKind::Spot);

        // The 0.1 release scope is spot, perpetuals, and dated futures.
        // Option parsers remain covered by fixtures, but every option product
        // is rejected by capability preflight.
        let binance_option = ExchangeFeedBuilder::new(ExchangeId::Binance)
            .ticker()
            .instrument(Symbol::option("BTC", "USDT", "250627", "100000", "C"))
            .exchange_symbol("BTC-250627-100000-C")
            .build();
        assert!(matches!(
            validate_feed(&binance_option),
            Err(Error::UnsupportedCapability(_))
        ));

        let bybit_option = ExchangeFeedBuilder::new(ExchangeId::Bybit)
            .ticker()
            .l2_book()
            .instrument(Symbol::option("BTC", "USDC", "30DEC22", "18000", "C"))
            .exchange_symbol("BTC-30DEC22-18000-C")
            .build();
        assert!(matches!(
            validate_feed(&bybit_option),
            Err(Error::UnsupportedCapability(_))
        ));

        let bybit_option_funding = ExchangeFeedBuilder::new(ExchangeId::Bybit)
            .funding()
            .instrument(Symbol::option("BTC", "USDC", "30DEC22", "18000", "C"))
            .build();
        assert!(matches!(
            validate_feed(&bybit_option_funding),
            Err(Error::UnsupportedCapability(_))
        ));

        let okx_option = ExchangeFeedBuilder::new(ExchangeId::Okx)
            .ticker()
            .trade()
            .instrument(Symbol::option("BTC", "USD", "250627", "100000", "C"))
            .build();
        assert!(matches!(
            validate_feed(&okx_option),
            Err(Error::UnsupportedCapability(_))
        ));

        let okx_option_candles = ExchangeFeedBuilder::new(ExchangeId::Okx)
            .candles()
            .instrument(Symbol::option("BTC", "USD", "250627", "100000", "C"))
            .build();
        assert!(matches!(
            validate_feed(&okx_option_candles),
            Err(Error::UnsupportedCapability(_))
        ));

        // Bybit kline covers spot/linear/inverse only (verified 2026-08-06);
        // option candles stay rejected.
        let bybit_option_candles = ExchangeFeedBuilder::new(ExchangeId::Bybit)
            .candles()
            .instrument(Symbol::option("BTC", "USDC", "30DEC22", "18000", "C"))
            .exchange_symbol("BTC-30DEC22-18000-C")
            .build();
        assert!(matches!(
            validate_feed(&bybit_option_candles),
            Err(Error::UnsupportedCapability(_))
        ));

        // Mark price rides the funding/mark-price streams on Binance, Bybit,
        // and OKX derivatives.
        let binance_mark = ExchangeFeedBuilder::new(ExchangeId::Binance)
            .mark_price()
            .instrument(Symbol::perpetual("BTC", "USDT"))
            .build();
        assert_eq!(
            validate_feed(&binance_mark).unwrap(),
            InstrumentKind::Perpetual
        );

        let bybit_mark = ExchangeFeedBuilder::new(ExchangeId::Bybit)
            .mark_price()
            .instrument(Symbol::perpetual("BTC", "USDT"))
            .build();
        assert_eq!(
            validate_feed(&bybit_mark).unwrap(),
            InstrumentKind::Perpetual
        );

        let okx_mark = ExchangeFeedBuilder::new(ExchangeId::Okx)
            .mark_price()
            .instrument(Symbol::perpetual("BTC", "USDT"))
            .build();
        assert_eq!(validate_feed(&okx_mark).unwrap(), InstrumentKind::Perpetual);

        // MARGIN protocol helpers remain as future work, but the 0.1 public
        // capability matrix rejects the product before connecting.
        let okx_margin = ExchangeFeedBuilder::new(ExchangeId::Okx)
            .candles()
            .liquidations()
            .mark_price()
            .instrument(Symbol::margin("BTC", "USDT"))
            .exchange_symbol("BTC-USDT")
            .build();
        assert!(matches!(
            validate_feed(&okx_margin),
            Err(Error::UnsupportedCapability(_))
        ));

        let okx_margin_ticker = ExchangeFeedBuilder::new(ExchangeId::Okx)
            .ticker()
            .instrument(Symbol::margin("BTC", "USDT"))
            .exchange_symbol("BTC-USDT")
            .build();
        assert!(matches!(
            validate_feed(&okx_margin_ticker),
            Err(Error::UnsupportedCapability(_))
        ));

        // Margin is OKX-only: other exchanges reject the product.
        let binance_margin = ExchangeFeedBuilder::new(ExchangeId::Binance)
            .candles()
            .instrument(Symbol::margin("BTC", "USDT"))
            .exchange_symbol("BTCUSDT")
            .build();
        assert!(matches!(
            validate_feed(&binance_margin),
            Err(Error::UnsupportedCapability(_))
        ));

        let gate_perpetual = Gateio::new()
            .ticker()
            .trade()
            .l2_book()
            .candles()
            .instrument(Symbol::perpetual("BTC", "USDT"))
            .build();
        assert_eq!(
            validate_feed(&gate_perpetual).unwrap(),
            InstrumentKind::Perpetual
        );

        let gate_futures = Gateio::new()
            .ticker()
            .trade()
            .l2_book()
            .candles()
            .instrument(Symbol::futures("BTC", "USDT", "20200814"))
            .build();
        assert_eq!(
            validate_feed(&gate_futures).unwrap(),
            InstrumentKind::Futures
        );

        // Gate.io derivative funding/open interest/index ride the
        // `futures.tickers` stream; public liquidations use a separate
        // perpetual-only `futures.public_liquidates` subscription.
        let gate_funding = Gateio::new()
            .funding()
            .instrument(Symbol::perpetual("BTC", "USDT"))
            .build();
        assert_eq!(
            validate_feed(&gate_funding).unwrap(),
            InstrumentKind::Perpetual
        );

        let gate_oi_index = Gateio::new()
            .open_interest()
            .index()
            .instrument(Symbol::futures("BTC", "USDT", "20260925"))
            .build();
        assert_eq!(
            validate_feed(&gate_oi_index).unwrap(),
            InstrumentKind::Futures
        );

        let gate_mark = Gateio::new()
            .mark_price()
            .instrument(Symbol::perpetual("BTC", "USDT"))
            .build();
        assert_eq!(
            validate_feed(&gate_mark).unwrap(),
            InstrumentKind::Perpetual
        );

        let gate_l1 = Gateio::new()
            .l1_book()
            .instrument(Symbol::spot("BTC", "USDT"))
            .build();
        assert_eq!(validate_feed(&gate_l1).unwrap(), InstrumentKind::Spot);

        let gate_spot_funding = Gateio::new()
            .funding()
            .instrument(Symbol::spot("BTC", "USDT"))
            .build();
        assert!(matches!(
            validate_feed(&gate_spot_funding),
            Err(Error::UnsupportedCapability(_))
        ));

        let gate_liquidations = Gateio::new()
            .liquidations()
            .instrument(Symbol::perpetual("BTC", "USDT"))
            .build();
        assert_eq!(
            validate_feed(&gate_liquidations).unwrap(),
            InstrumentKind::Perpetual
        );

        let gate_spot_mark = Gateio::new()
            .mark_price()
            .instrument(Symbol::spot("BTC", "USDT"))
            .build();
        assert!(matches!(
            validate_feed(&gate_spot_mark),
            Err(Error::UnsupportedCapability(_))
        ));
    }

    #[test]
    fn preflight_validates_candle_interval_per_exchange() {
        let binance_hourly = ExchangeFeedBuilder::new(ExchangeId::Binance)
            .candles()
            .candles_interval("1h")
            .instrument(Symbol::perpetual("BTC", "USDT"))
            .build();
        assert_eq!(
            validate_feed(&binance_hourly).unwrap(),
            InstrumentKind::Perpetual
        );

        // Bybit has no 8-hour interval in the official kline set.
        let bybit_eight_hours = ExchangeFeedBuilder::new(ExchangeId::Bybit)
            .candles()
            .candles_interval("8h")
            .instrument(Symbol::perpetual("BTC", "USDT"))
            .build();
        assert!(matches!(
            validate_feed(&bybit_eight_hours),
            Err(Error::UnsupportedCapability(_))
        ));

        // Bitget v3 has no 2-hour interval.
        let bitget_two_hours = ExchangeFeedBuilder::new(ExchangeId::Bitget)
            .candles()
            .candles_interval("2h")
            .instrument(Symbol::perpetual("BTC", "USDT"))
            .build();
        assert!(matches!(
            validate_feed(&bitget_two_hours),
            Err(Error::UnsupportedCapability(_))
        ));

        // Gate.io maps the normalized vocabulary to its own wire form.
        let gate_daily = Gateio::new()
            .candles()
            .candles_interval("1d")
            .instrument(Symbol::spot("BTC", "USDT"))
            .build();
        assert_eq!(validate_feed(&gate_daily).unwrap(), InstrumentKind::Spot);

        let gate_10s = Gateio::new()
            .candles()
            .candles_interval("10s")
            .instrument(Symbol::perpetual("BTC", "USDT"))
            .build();
        assert_eq!(validate_feed(&gate_10s).unwrap(), InstrumentKind::Perpetual);
    }

    #[test]
    fn preflight_validates_l2_depth_level_per_exchange() {
        let bybit_200 = ExchangeFeedBuilder::new(ExchangeId::Bybit)
            .l2_book()
            .l2_book_depth(200)
            .instrument(Symbol::perpetual("BTC", "USDT"))
            .build();
        assert_eq!(
            validate_feed(&bybit_200).unwrap(),
            InstrumentKind::Perpetual
        );

        // 1000 is linear-only; options expose only 25 and 100.
        let bybit_option_1000 = ExchangeFeedBuilder::new(ExchangeId::Bybit)
            .l2_book()
            .l2_book_depth(1000)
            .instrument(Symbol::option("BTC", "USDC", "30DEC22", "18000", "C"))
            .exchange_symbol("BTC-30DEC22-18000-C")
            .build();
        assert!(matches!(
            validate_feed(&bybit_option_1000),
            Err(Error::UnsupportedCapability(_))
        ));

        let okx_5 = ExchangeFeedBuilder::new(ExchangeId::Okx)
            .l2_book()
            .l2_book_depth(5)
            .instrument(Symbol::perpetual("BTC", "USDT"))
            .build();
        assert_eq!(validate_feed(&okx_5).unwrap(), InstrumentKind::Perpetual);

        // OKX 50/400 tick-by-tick channels (`books50-l2-tbt`/`books-l2-tbt`)
        // are VIP4+-gated (error 64003 otherwise) and not wired into book
        // sync; they must fail explicitly at preflight instead of silently
        // dropping every push (PARITY.md keeps them unsupported).
        for level in [50u16, 400] {
            let okx_tbt = ExchangeFeedBuilder::new(ExchangeId::Okx)
                .l2_book()
                .l2_book_depth(level)
                .instrument(Symbol::perpetual("BTC", "USDT"))
                .build();
            assert!(matches!(
                validate_feed(&okx_tbt),
                Err(Error::UnsupportedCapability(_))
            ));
        }

        let binance_10 = ExchangeFeedBuilder::new(ExchangeId::Binance)
            .l2_book()
            .l2_book_depth(10)
            .instrument(Symbol::perpetual("BTC", "USDT"))
            .build();
        assert_eq!(
            validate_feed(&binance_10).unwrap(),
            InstrumentKind::Perpetual
        );

        // Gate.io WebSocket depth takes no level parameter.
        let gate_depth = Gateio::new()
            .l2_book()
            .l2_book_depth(100)
            .instrument(Symbol::spot("BTC", "USDT"))
            .build();
        assert!(matches!(
            validate_feed(&gate_depth),
            Err(Error::UnsupportedCapability(_))
        ));

        // A depth level without the L2 channel is invalid configuration.
        let depth_without_channel = ExchangeFeedBuilder::new(ExchangeId::Bybit)
            .ticker()
            .l2_book_depth(200)
            .instrument(Symbol::perpetual("BTC", "USDT"))
            .build();
        assert!(matches!(
            validate_feed(&depth_without_channel),
            Err(Error::InvalidConfiguration(_))
        ));
    }

    #[test]
    fn preflight_validates_l2_book_interval_per_exchange() {
        let binance_spot_1000 = ExchangeFeedBuilder::new(ExchangeId::Binance)
            .l2_book()
            .l2_book_interval("1000ms")
            .instrument(Symbol::spot("BTC", "USDT"))
            .build();
        assert_eq!(
            validate_feed(&binance_spot_1000).unwrap(),
            InstrumentKind::Spot
        );

        let binance_futures_250 = ExchangeFeedBuilder::new(ExchangeId::Binance)
            .l2_book()
            .l2_book_depth(20)
            .l2_book_interval("250ms")
            .instrument(Symbol::perpetual("BTC", "USDT"))
            .build();
        assert_eq!(
            validate_feed(&binance_futures_250).unwrap(),
            InstrumentKind::Perpetual
        );

        // Spot supports only 100ms/1000ms; 250ms is USD-M/COIN-M-only.
        let binance_spot_250 = ExchangeFeedBuilder::new(ExchangeId::Binance)
            .l2_book()
            .l2_book_interval("250ms")
            .instrument(Symbol::spot("BTC", "USDT"))
            .build();
        assert!(matches!(
            validate_feed(&binance_spot_250),
            Err(Error::UnsupportedCapability(_))
        ));

        // USD-M/COIN-M reject the spot-only 1000ms interval.
        let binance_futures_1000 = ExchangeFeedBuilder::new(ExchangeId::Binance)
            .l2_book()
            .l2_book_interval("1000ms")
            .instrument(Symbol::perpetual("BTC", "USDT"))
            .build();
        assert!(matches!(
            validate_feed(&binance_futures_1000),
            Err(Error::UnsupportedCapability(_))
        ));

        // The interval is Binance-only; other exchanges reject it.
        let bybit_interval = ExchangeFeedBuilder::new(ExchangeId::Bybit)
            .l2_book()
            .l2_book_interval("250ms")
            .instrument(Symbol::perpetual("BTC", "USDT"))
            .build();
        assert!(matches!(
            validate_feed(&bybit_interval),
            Err(Error::UnsupportedCapability(_))
        ));

        // An interval without the L2 channel is invalid configuration.
        let interval_without_channel = ExchangeFeedBuilder::new(ExchangeId::Binance)
            .ticker()
            .l2_book_interval("1000ms")
            .instrument(Symbol::spot("BTC", "USDT"))
            .build();
        assert!(matches!(
            validate_feed(&interval_without_channel),
            Err(Error::InvalidConfiguration(_))
        ));
    }

    #[test]
    fn gateio_catalog_maps_usdt_and_btc_perpetual_contracts() {
        let usdt_payload = json!([
            {"name":"BTC_USDT","type":"direct","quanto_multiplier":"0.0001","status":"trading"}
        ]);
        let btc_payload = json!([
            {"name":"BTC_USD","type":"inverse","quanto_multiplier":"1","status":"trading"}
        ]);
        let mut registry = SymbolRegistry::default();

        add_gateio_perpetual_markets(&mut registry, &usdt_payload, "usdt").unwrap();
        add_gateio_perpetual_markets(&mut registry, &btc_payload, "btc").unwrap();

        assert_eq!(
            registry
                .to_exchange(&Symbol::perpetual("BTC", "USDT"))
                .unwrap(),
            "BTC_USDT"
        );
        assert_eq!(
            registry
                .to_exchange(&Symbol::perpetual("BTC", "USD"))
                .unwrap(),
            "BTC_USD"
        );
    }

    #[test]
    fn gateio_catalog_does_not_use_zero_multiplier_as_identity_gate() {
        let payload = json!([
            {
                "name":"BTC_USD",
                "type":"inverse",
                "quanto_multiplier":"0",
                "in_delisting":false
            }
        ]);
        let mut registry = SymbolRegistry::default();

        add_gateio_perpetual_markets(&mut registry, &payload, "btc").unwrap();

        assert_eq!(
            registry
                .to_exchange(&Symbol::perpetual("BTC", "USD"))
                .unwrap(),
            "BTC_USD"
        );
    }

    #[test]
    fn gateio_catalog_maps_usdt_delivery_underlying_and_native_expiry() {
        let payload = json!([
            {
                "name":"BTC_USDT_20200814",
                "underlying":"BTC_USDT",
                "type":"direct",
                "quanto_multiplier":"0.0001",
                "expire_time":1597363200,
                "status":"trading"
            }
        ]);
        let mut registry = SymbolRegistry::default();

        add_gateio_delivery_markets(&mut registry, &payload).unwrap();

        assert_eq!(
            registry
                .to_exchange(&Symbol::futures("BTC", "USDT", "20200814"))
                .unwrap(),
            "BTC_USDT_20200814"
        );
    }

    #[test]
    fn gateio_catalog_rejects_contracts_that_cannot_be_mapped_faithfully() {
        let wrong_settle = json!([
            {"name":"BTC_USD","type":"direct","quanto_multiplier":"0.0001"}
        ]);
        let inconsistent_delivery = json!([
            {
                "name":"ETH_USDT_20200814",
                "underlying":"BTC_USDT",
                "type":"direct",
                "quanto_multiplier":"0.0001",
                "expire_time":1597363200
            }
        ]);
        let mut registry = SymbolRegistry::default();

        assert!(matches!(
            add_gateio_perpetual_markets(&mut registry, &wrong_settle, "usdt"),
            Err(Error::MalformedData(_))
        ));
        assert!(matches!(
            add_gateio_delivery_markets(&mut registry, &inconsistent_delivery),
            Err(Error::MalformedData(_))
        ));
    }

    #[test]
    fn binance_catalog_preserves_perpetual_and_delivery_identity() {
        let payload = json!({"symbols": [
            {"symbol":"BTCUSD_PERP","baseAsset":"BTC","quoteAsset":"USD","contractType":"PERPETUAL"},
            {"symbol":"BTCUSD_240628","baseAsset":"BTC","quoteAsset":"USD","contractType":"CURRENT_QUARTER"}
        ]});
        let mut registry = SymbolRegistry::default();
        add_binance_markets(&mut registry, &payload, InstrumentKind::Perpetual).unwrap();
        add_binance_markets(&mut registry, &payload, InstrumentKind::Futures).unwrap();

        assert_eq!(
            registry
                .to_exchange(&Symbol::perpetual("BTC", "USD"))
                .unwrap(),
            "BTCUSD_PERP"
        );
        assert_eq!(
            registry
                .to_exchange(&Symbol::futures("BTC", "USD", "240628"))
                .unwrap(),
            "BTCUSD_240628"
        );
    }

    #[test]
    fn bitget_catalog_uses_delivery_time_for_futures_expiry() {
        let payload = json!({"data": [
            {"symbol":"BTCUSDT","baseCoin":"BTC","quoteCoin":"USDT","type":"perpetual"},
            {"symbol":"BTCUSDT_240628","baseCoin":"BTC","quoteCoin":"USDT","type":"delivery","deliveryTime":"1719532800000"}
        ]});
        let mut registry = SymbolRegistry::default();
        add_bitget_markets(&mut registry, &payload, InstrumentKind::Perpetual).unwrap();
        add_bitget_markets(&mut registry, &payload, InstrumentKind::Futures).unwrap();

        assert_eq!(
            registry
                .to_exchange(&Symbol::futures("BTC", "USDT", "240628"))
                .unwrap(),
            "BTCUSDT_240628"
        );
    }

    #[test]
    fn bybit_catalog_returns_cursor_and_preserves_delivery_date() {
        let payload = json!({"result": {
            "list": [
                {"symbol":"BTCUSDT","baseCoin":"BTC","quoteCoin":"USDT","contractType":"LinearPerpetual","deliveryTime":"0"},
                {"symbol":"BTCUSDH24","baseCoin":"BTC","quoteCoin":"USD","contractType":"InverseFutures","deliveryTime":"1719532800000"}
            ],
            "nextPageCursor":"next-page"
        }});
        let mut registry = SymbolRegistry::default();
        let cursor = add_bybit_page(&mut registry, &payload, InstrumentKind::Futures).unwrap();

        assert_eq!(cursor.as_deref(), Some("next-page"));
        assert_eq!(
            registry
                .to_exchange(&Symbol::futures("BTC", "USD", "240628"))
                .unwrap(),
            "BTCUSDH24"
        );
    }

    #[test]
    fn bybit_pagination_rejects_a_repeated_cursor() {
        let mut seen = HashSet::new();
        record_bybit_cursor(&mut seen, "next-page").unwrap();
        assert!(matches!(
            record_bybit_cursor(&mut seen, "next-page"),
            Err(Error::Protocol(_))
        ));
    }

    #[test]
    fn bybit_derivative_page_requires_pagination_metadata() {
        let payload = json!({"result": {"list": []}});
        let mut registry = SymbolRegistry::default();

        assert!(matches!(
            add_bybit_page(&mut registry, &payload, InstrumentKind::Perpetual),
            Err(Error::MalformedData(_))
        ));
    }

    #[test]
    fn okx_catalog_maps_swap_and_normal_expiry_futures() {
        let payload = json!({"data": [
            {"instId":"BTC-USDT-SWAP","instType":"SWAP","state":"live","ruleType":"normal"},
            {"instId":"BTC-USD-240628","instType":"FUTURES","state":"live","ruleType":"normal","expTime":"1719532800000"},
            {"instId":"NEW-USDT-XPERP","instType":"FUTURES","state":"live","ruleType":"xperp","expTime":""}
        ]});
        let mut registry = SymbolRegistry::default();
        add_okx_markets(&mut registry, &payload, InstrumentKind::Perpetual).unwrap();
        add_okx_markets(&mut registry, &payload, InstrumentKind::Futures).unwrap();

        assert_eq!(
            registry
                .to_exchange(&Symbol::perpetual("BTC", "USDT"))
                .unwrap(),
            "BTC-USDT-SWAP"
        );
        assert_eq!(
            registry
                .to_exchange(&Symbol::futures("BTC", "USD", "240628"))
                .unwrap(),
            "BTC-USD-240628"
        );
        assert!(matches!(
            registry.to_exchange(&Symbol::futures("NEW", "USDT", "XPERP")),
            Err(Error::UnsupportedSymbol(_))
        ));
    }

    #[test]
    fn bybit_option_catalog_resolves_settlement_from_suffix() {
        let payload = json!({"result": {"list": [
            {"symbol": "BTC-30JUN26-65000-C"},
            {"symbol": "BTC-27MAR26-70000-P-USDT"}
        ]}});
        let mut registry = SymbolRegistry::default();
        for item in payload["result"]["list"].as_array().unwrap() {
            add_bybit_option_market(&mut registry, item.get("symbol").unwrap().as_str().unwrap())
                .unwrap();
        }

        assert_eq!(
            registry
                .to_exchange(&Symbol::option("BTC", "USDC", "30JUN26", "65000", "C"))
                .unwrap(),
            "BTC-30JUN26-65000-C"
        );
        assert_eq!(
            registry
                .to_exchange(&Symbol::option("BTC", "USDT", "27MAR26", "70000", "P"))
                .unwrap(),
            "BTC-27MAR26-70000-P-USDT"
        );
    }

    #[test]
    fn bybit_option_page_pages_and_cross_checks_options_type() {
        let first_page = json!({"result": {
            "list": [
                {"symbol": "BTC-30OCT26-58000-C-USDT", "optionsType": "Call"},
                {"symbol": "BTC-30OCT26-46000-P-USDT", "optionsType": "Put"}
            ],
            "nextPageCursor": "0%2C500"
        }});
        let last_page = json!({"result": {
            "list": [{"symbol": "ETH-25JUN27-5500-P-USDT", "optionsType": "Put"}],
            "nextPageCursor": ""
        }});
        let mut registry = SymbolRegistry::default();

        let cursor = add_bybit_option_page(&mut registry, &first_page).unwrap();
        assert_eq!(cursor.as_deref(), Some("0%2C500"));
        let cursor = add_bybit_option_page(&mut registry, &last_page).unwrap();
        assert!(cursor.is_none());

        assert_eq!(
            registry
                .to_exchange(&Symbol::option("BTC", "USDT", "30OCT26", "58000", "C"))
                .unwrap(),
            "BTC-30OCT26-58000-C-USDT"
        );
        assert_eq!(
            registry
                .to_exchange(&Symbol::option("ETH", "USDT", "25JUN27", "5500", "P"))
                .unwrap(),
            "ETH-25JUN27-5500-P-USDT"
        );
    }

    #[test]
    fn bybit_option_page_rejects_options_type_contradiction() {
        let payload = json!({"result": {
            "list": [{"symbol": "BTC-30OCT26-58000-C-USDT", "optionsType": "Put"}],
            "nextPageCursor": ""
        }});
        let mut registry = SymbolRegistry::default();
        assert!(matches!(
            add_bybit_option_page(&mut registry, &payload),
            Err(Error::MalformedData(_))
        ));
    }

    #[test]
    fn okx_option_catalog_parses_native_inst_id() {
        let payload = json!({"data": [
            {"instId": "BTC-USD-250627-100000-C", "instType": "OPTION", "state": "live"}
        ]});
        let mut registry = SymbolRegistry::default();
        add_okx_markets(&mut registry, &payload, InstrumentKind::Option).unwrap();

        assert_eq!(
            registry
                .to_exchange(&Symbol::option("BTC", "USD", "250627", "100000", "C"))
                .unwrap(),
            "BTC-USD-250627-100000-C"
        );
    }

    #[test]
    fn binance_option_catalog_parses_native_inst_id() {
        // Current eapi `optionSymbols` rows carry `underlying` +
        // `quoteAsset` (no `baseAsset`); the base is derived from the
        // underlying by stripping the quote suffix.
        let payload = json!({"optionSymbols": [
            {"symbol": "BTC-250627-100000-C", "underlying": "BTCUSDT", "quoteAsset": "USDT"},
            {"symbol": "BTC-250627-100000-P", "underlying": "BTCUSDT", "quoteAsset": "USDT"}
        ]});
        let mut registry = SymbolRegistry::default();
        for item in payload["optionSymbols"].as_array().unwrap() {
            add_binance_option_market(&mut registry, item).unwrap();
        }

        assert_eq!(
            registry
                .to_exchange(&Symbol::option("BTC", "USDT", "250627", "100000", "C"))
                .unwrap(),
            "BTC-250627-100000-C"
        );
    }

    #[tokio::test]
    async fn explicit_bidirectional_mapping_resolves_without_discovery() {
        let feed = Binance::new()
            .ticker()
            .symbol("BTC-USDT")
            .exchange_symbol("BTCUSDT")
            .build();

        assert_eq!(
            resolve_feed_symbols(&feed).await.unwrap(),
            vec!["BTCUSDT".to_owned()]
        );
    }

    #[tokio::test]
    async fn explicit_mapping_requires_one_native_symbol_per_instrument() {
        let feed = Binance::new()
            .ticker()
            .symbol("BTC-USDT")
            .symbol("ETH-USDT")
            .exchange_symbol("BTCUSDT")
            .build();

        assert!(matches!(
            resolve_feed_symbols(&feed).await,
            Err(Error::InvalidConfiguration(_))
        ));
    }
}
