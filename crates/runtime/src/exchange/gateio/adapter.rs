use super::parser;
use crate::exchange::ExchangeFeed;
use cryptofeed_core::{
    error::{Error, Result},
    exchange::Channel,
    symbol::{InstrumentKind, Symbol},
};
use serde_json::Value;
use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(feature = "candles")]
use cryptofeed_candles::Candle;
#[cfg(feature = "funding")]
use cryptofeed_funding::Funding;
#[cfg(feature = "index")]
use cryptofeed_index::IndexPrice;
#[cfg(feature = "liquidations")]
use cryptofeed_liquidations::Liquidation;
#[cfg(feature = "markprice")]
use cryptofeed_markprice::MarkPrice;
#[cfg(feature = "openinterest")]
use cryptofeed_openinterest::OpenInterest;
#[cfg(feature = "orderbook")]
use cryptofeed_orderbook::{L1Book, L2Book};
#[cfg(feature = "ticker")]
use cryptofeed_ticker::Ticker;
#[cfg(feature = "trade")]
use cryptofeed_trade::Trade;

pub struct GateioAdapter;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GateioProduct {
    Spot,
    UsdtPerpetual,
    BtcPerpetual,
    UsdtDelivery,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateioInstrument {
    pub symbol: Symbol,
    pub exchange_symbol: String,
    pub product: GateioProduct,
}

impl GateioInstrument {
    pub fn new(symbol: Symbol, exchange_symbol: &str, product: GateioProduct) -> Self {
        Self {
            symbol,
            exchange_symbol: exchange_symbol.to_ascii_uppercase(),
            product,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateioConnectionPlan {
    pub product: GateioProduct,
    pub websocket_url: String,
    pub subscription_messages: Vec<String>,
    /// REST URLs for L2-subscribed instruments only, in their instrument order.
    pub snapshot_urls: Vec<String>,
    pub instruments: Vec<GateioInstrument>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GateioAdapterMetadata {
    pub product: GateioProduct,
    pub websocket_url: &'static str,
    pub snapshot_endpoint: &'static str,
    pub supported_channels: &'static [Channel],
}

const PUBLIC_CHANNELS: &[Channel] = &[
    Channel::Ticker,
    Channel::Trade,
    Channel::L2Book,
    Channel::L1Book,
    Channel::Candles,
];

/// Derivative products additionally expose funding, open interest, index
/// price, and mark price embedded in the `futures.tickers` stream (there is
/// no standalone channel for any of them; verified against official v4
/// futures docs and the live delivery endpoint 2026-08-07).
const DERIVATIVE_CHANNELS: &[Channel] = &[
    Channel::Ticker,
    Channel::Trade,
    Channel::L2Book,
    Channel::L1Book,
    Channel::Candles,
    Channel::OpenInterest,
    Channel::Index,
    Channel::MarkPrice,
];

const PERPETUAL_CHANNELS: &[Channel] = &[
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

pub enum GateioEvent {
    #[cfg(feature = "liquidations")]
    Liquidation(Liquidation),
    #[cfg(feature = "candles")]
    Candle(Candle),
    #[cfg(feature = "funding")]
    Funding(Funding),
    #[cfg(feature = "index")]
    IndexPrice(IndexPrice),
    #[cfg(feature = "orderbook")]
    L1Book(L1Book),
    #[cfg(feature = "orderbook")]
    L2Book(L2Book),
    #[cfg(feature = "markprice")]
    MarkPrice(MarkPrice),
    #[cfg(feature = "openinterest")]
    OpenInterest(OpenInterest),
    #[cfg(feature = "ticker")]
    Ticker(Ticker),
    #[cfg(feature = "trade")]
    Trade(Trade),
}

impl GateioAdapter {
    pub fn websocket_url() -> &'static str {
        "wss://api.gateio.ws/ws/v4/"
    }

    pub fn subscription_url(feed: &ExchangeFeed) -> String {
        Self::instruments(feed)
            .ok()
            .and_then(|instruments| instruments.first().map(|item| item.product))
            .map(Self::websocket_url_for)
            .unwrap_or_else(Self::websocket_url)
            .to_owned()
    }

    pub fn subscription_messages(feed: &ExchangeFeed) -> Vec<String> {
        let Ok(instruments) = Self::instruments(feed) else {
            return Vec::new();
        };
        Self::subscription_messages_for_instruments(
            &instruments,
            &feed.channels,
            &feed.candle_interval,
            feed,
        )
    }

    pub fn connection_plans(feed: &ExchangeFeed) -> Result<Vec<GateioConnectionPlan>> {
        let instruments = Self::instruments(feed)?;
        let mut plans = Vec::new();
        for product in [
            GateioProduct::Spot,
            GateioProduct::UsdtPerpetual,
            GateioProduct::BtcPerpetual,
            GateioProduct::UsdtDelivery,
        ] {
            let product_instruments: Vec<_> = instruments
                .iter()
                .filter(|instrument| instrument.product == product)
                .cloned()
                .collect();
            if product_instruments.is_empty() {
                continue;
            }
            Self::validate_channels(product, &feed.channels)?;
            let subscription_messages = Self::subscription_messages_for_instruments(
                &product_instruments,
                &feed.channels,
                &feed.candle_interval,
                feed,
            );
            let snapshot_urls = if feed.channels.contains(&Channel::L2Book) {
                product_instruments
                    .iter()
                    .filter(|instrument| feed.subscribes(Channel::L2Book, &instrument.symbol))
                    .map(|instrument| Self::snapshot_url(instrument, 100))
                    .collect()
            } else {
                Vec::new()
            };
            plans.push(GateioConnectionPlan {
                product,
                websocket_url: Self::websocket_url_for(product).to_owned(),
                subscription_messages,
                snapshot_urls,
                instruments: product_instruments,
            });
        }
        Ok(plans)
    }

    pub fn metadata(product: GateioProduct) -> GateioAdapterMetadata {
        let (websocket_url, snapshot_endpoint) = match product {
            GateioProduct::Spot => (
                "wss://api.gateio.ws/ws/v4/",
                "https://api.gateio.ws/api/v4/spot/order_book",
            ),
            GateioProduct::UsdtPerpetual => (
                "wss://fx-ws.gateio.ws/v4/ws/usdt",
                "https://api.gateio.ws/api/v4/futures/usdt/order_book",
            ),
            GateioProduct::BtcPerpetual => (
                "wss://fx-ws.gateio.ws/v4/ws/btc",
                "https://api.gateio.ws/api/v4/futures/btc/order_book",
            ),
            GateioProduct::UsdtDelivery => (
                "wss://fx-ws.gateio.ws/v4/ws/delivery/usdt",
                "https://api.gateio.ws/api/v4/delivery/usdt/order_book",
            ),
        };
        GateioAdapterMetadata {
            product,
            websocket_url,
            snapshot_endpoint,
            supported_channels: if product == GateioProduct::Spot {
                PUBLIC_CHANNELS
            } else if product == GateioProduct::UsdtDelivery {
                DERIVATIVE_CHANNELS
            } else {
                PERPETUAL_CHANNELS
            },
        }
    }

    pub fn validate_channels(product: GateioProduct, channels: &[Channel]) -> Result<()> {
        let metadata = Self::metadata(product);
        for channel in channels {
            if !metadata.supported_channels.contains(channel) {
                return Err(Error::UnsupportedChannel(channel_name(*channel).to_owned()));
            }
        }
        Ok(())
    }

    pub fn websocket_url_for(product: GateioProduct) -> &'static str {
        Self::metadata(product).websocket_url
    }

    pub fn snapshot_url(instrument: &GateioInstrument, limit: u16) -> String {
        let metadata = Self::metadata(instrument.product);
        let parameter = if instrument.product == GateioProduct::Spot {
            "currency_pair"
        } else {
            "contract"
        };
        format!(
            "{}?{}={}&limit={limit}&with_id=true",
            metadata.snapshot_endpoint, parameter, instrument.exchange_symbol
        )
    }

    pub fn instruments(feed: &ExchangeFeed) -> Result<Vec<GateioInstrument>> {
        if !feed.exchange_symbols.is_empty() && feed.exchange_symbols.len() != feed.symbols.len() {
            return Err(Error::InvalidConfiguration(
                "gateio normalized and exchange symbol counts differ".to_owned(),
            ));
        }
        feed.symbols
            .iter()
            .enumerate()
            .map(|(index, symbol)| {
                let product = product_from_symbol(symbol)?;
                let exchange_symbol = feed
                    .exchange_symbols
                    .get(index)
                    .cloned()
                    .unwrap_or_else(|| native_symbol(symbol));
                Ok(GateioInstrument::new(
                    symbol.clone(),
                    &exchange_symbol,
                    product,
                ))
            })
            .collect()
    }

    /// Normalized candle interval to the Gate.io v4 wire form (spot and
    /// futures candlesticks interval payload element). Official intervals:
    /// `10s` `1m` `5m` `15m` `30m` `1h` `4h` `8h` `24h` `7d` `30d` (official
    /// docs were unavailable for re-verification on 2026-08-06; the set
    /// follows the long-stable v4 candlesticks contract).
    pub fn candle_interval_wire(interval: &str) -> Option<&'static str> {
        match interval {
            "10s" => Some("10s"),
            "1m" => Some("1m"),
            "5m" => Some("5m"),
            "15m" => Some("15m"),
            "30m" => Some("30m"),
            "1h" => Some("1h"),
            "4h" => Some("4h"),
            "8h" => Some("8h"),
            "1d" => Some("24h"),
            "1w" => Some("7d"),
            "1M" => Some("30d"),
            _ => None,
        }
    }

    /// Gate.io v4 WebSocket depth channels do not take a depth parameter
    /// (`order_book_update` is full-depth); any explicit L2 depth level is
    /// unsupported.
    pub fn l2_book_depth_supported(_level: u16) -> bool {
        false
    }

    fn subscription_messages_for_instruments(
        instruments: &[GateioInstrument],
        channels: &[Channel],
        candle_interval: &str,
        feed: &ExchangeFeed,
    ) -> Vec<String> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap_or(0);
        let mut messages = Vec::new();
        // Funding, open interest, and index all ride the same derivative
        // `futures.tickers` stream, so a (channel, pair) key deduplicates
        // the subscription.
        let mut seen = HashSet::new();
        for instrument in instruments {
            let pair = &instrument.exchange_symbol;
            let derivative = instrument.product != GateioProduct::Spot;
            let prefix = if derivative { "futures" } else { "spot" };
            for channel in channels {
                if !feed.subscribes(*channel, &instrument.symbol) {
                    continue;
                }
                let (channel, payload) = match channel {
                    Channel::Candles => (
                        format!("{prefix}.candlesticks"),
                        serde_json::json!([
                            Self::candle_interval_wire(candle_interval).unwrap_or("1m"),
                            pair
                        ]),
                    ),
                    Channel::Ticker => (format!("{prefix}.book_ticker"), serde_json::json!([pair])),
                    Channel::L1Book => (format!("{prefix}.book_ticker"), serde_json::json!([pair])),
                    Channel::Trade => (format!("{prefix}.trades"), serde_json::json!([pair])),
                    Channel::L2Book if derivative => (
                        format!("{prefix}.order_book_update"),
                        serde_json::json!([pair, "100ms", "100"]),
                    ),
                    Channel::L2Book => (
                        format!("{prefix}.order_book_update"),
                        serde_json::json!([pair, "100ms"]),
                    ),
                    // Funding, open interest, index price, and mark price are
                    // embedded in the derivative tickers stream; there is no
                    // standalone channel (spot has none of the four).
                    Channel::Funding
                    | Channel::OpenInterest
                    | Channel::Index
                    | Channel::MarkPrice
                        if derivative =>
                    {
                        (format!("{prefix}.tickers"), serde_json::json!([pair]))
                    }
                    Channel::Liquidations
                        if matches!(
                            instrument.product,
                            GateioProduct::UsdtPerpetual | GateioProduct::BtcPerpetual
                        ) =>
                    {
                        (
                            "futures.public_liquidates".to_owned(),
                            serde_json::json!([pair]),
                        )
                    }
                    Channel::Funding
                    | Channel::Liquidations
                    | Channel::MarkPrice
                    | Channel::OpenInterest
                    | Channel::Index => continue,
                    // Future channels are not subscribed.
                    _ => continue,
                };
                if !seen.insert(format!("{channel}:{pair}")) {
                    continue;
                }
                messages.push(
                    serde_json::json!({
                        "time": timestamp,
                        "channel": channel,
                        "event": "subscribe",
                        "payload": payload,
                    })
                    .to_string(),
                );
            }
        }
        messages
    }

    pub fn parse_message(message: &Value, received_ts: f64) -> Option<GateioEvent> {
        Self::parse_messages(message, received_ts)
            .into_iter()
            .next()
    }

    pub fn parse_messages(message: &Value, received_ts: f64) -> Vec<GateioEvent> {
        Self::parse_messages_inner(message, received_ts, None)
    }

    pub fn parse_messages_for_instrument(
        message: &Value,
        received_ts: f64,
        instrument: &GateioInstrument,
    ) -> Vec<GateioEvent> {
        Self::parse_messages_inner(message, received_ts, Some(instrument))
    }

    pub fn instrument_for_message<'a>(
        message: &Value,
        instruments: &'a [GateioInstrument],
    ) -> Option<&'a GateioInstrument> {
        let result = match message.get("result")? {
            Value::Array(rows) => rows.first()?,
            result @ Value::Object(_) => result,
            _ => return None,
        };
        let native = result
            .get("s")
            .or_else(|| result.get("contract"))
            .or_else(|| result.get("currency_pair"))
            .and_then(Value::as_str)
            .or_else(|| {
                result
                    .get("n")
                    .and_then(Value::as_str)
                    .and_then(|name| name.split_once('_').map(|(_, symbol)| symbol))
            })?;
        instruments
            .iter()
            .find(|instrument| instrument.exchange_symbol.eq_ignore_ascii_case(native))
    }

    fn parse_messages_inner(
        message: &Value,
        received_ts: f64,
        instrument: Option<&GateioInstrument>,
    ) -> Vec<GateioEvent> {
        if message.get("event").and_then(|v| v.as_str()) == Some("subscribe") {
            return Vec::new();
        }
        let Some(channel) = message.get("channel").and_then(Value::as_str) else {
            return Vec::new();
        };
        match channel {
            #[cfg(feature = "liquidations")]
            "futures.public_liquidates" => {
                parser::parse_liquidations_for_instrument(message, received_ts, instrument)
                    .into_iter()
                    .map(GateioEvent::Liquidation)
                    .collect()
            }
            #[cfg(feature = "candles")]
            "spot.candlesticks" | "futures.candlesticks" => {
                let events = match instrument {
                    Some(instrument) => {
                        parser::parse_candles_for_instrument(message, received_ts, instrument)
                    }
                    None => parser::parse_candles(message, received_ts),
                };
                events.into_iter().map(GateioEvent::Candle).collect()
            }
            #[cfg(feature = "orderbook")]
            "spot.order_book_update" | "futures.order_book_update" => {
                let events = match instrument {
                    Some(instrument) => {
                        parser::parse_l2_books_for_instrument(message, received_ts, instrument)
                    }
                    None => parser::parse_l2_books(message, received_ts),
                };
                events.into_iter().map(GateioEvent::L2Book).collect()
            }
            #[cfg(any(feature = "ticker", feature = "orderbook"))]
            "spot.book_ticker" | "futures.book_ticker" => {
                let mut events = Vec::new();
                #[cfg(feature = "ticker")]
                {
                    events.extend(
                        match instrument {
                            Some(instrument) => parser::parse_tickers_for_instrument(
                                message,
                                received_ts,
                                instrument,
                            ),
                            None => parser::parse_tickers(message, received_ts),
                        }
                        .into_iter()
                        .map(GateioEvent::Ticker),
                    );
                }
                // The book_ticker stream doubles as the L1 top-of-book
                // channel (it carries best bid/ask with sizes).
                #[cfg(feature = "orderbook")]
                {
                    events.extend(
                        match instrument {
                            Some(instrument) => parser::parse_l1_books_for_instrument(
                                message,
                                received_ts,
                                instrument,
                            ),
                            None => parser::parse_l1_books(message, received_ts),
                        }
                        .into_iter()
                        .map(GateioEvent::L1Book),
                    );
                }
                events
            }
            #[cfg(feature = "trade")]
            "spot.trades" | "futures.trades" => {
                let events = match instrument {
                    Some(instrument) => {
                        parser::parse_trades_for_instrument(message, received_ts, instrument)
                    }
                    None => parser::parse_trades(message, received_ts),
                };
                events.into_iter().map(GateioEvent::Trade).collect()
            }
            // The derivative tickers stream is the single source for funding,
            // open interest, and index price (see DERIVATIVE_CHANNELS).
            "futures.tickers" => {
                let mut events = Vec::new();
                #[cfg(feature = "funding")]
                {
                    events.extend(
                        match instrument {
                            Some(instrument) => parser::parse_fundings_for_instrument(
                                message,
                                received_ts,
                                instrument,
                            ),
                            None => parser::parse_fundings(message, received_ts),
                        }
                        .into_iter()
                        .map(GateioEvent::Funding),
                    );
                }
                #[cfg(feature = "openinterest")]
                {
                    events.extend(
                        match instrument {
                            Some(instrument) => parser::parse_open_interests_for_instrument(
                                message,
                                received_ts,
                                instrument,
                            ),
                            None => parser::parse_open_interests(message, received_ts),
                        }
                        .into_iter()
                        .map(GateioEvent::OpenInterest),
                    );
                }
                #[cfg(feature = "index")]
                {
                    events.extend(
                        match instrument {
                            Some(instrument) => parser::parse_index_prices_for_instrument(
                                message,
                                received_ts,
                                instrument,
                            ),
                            None => parser::parse_index_prices(message, received_ts),
                        }
                        .into_iter()
                        .map(GateioEvent::IndexPrice),
                    );
                }
                #[cfg(feature = "markprice")]
                {
                    events.extend(
                        match instrument {
                            Some(instrument) => parser::parse_mark_prices_for_instrument(
                                message,
                                received_ts,
                                instrument,
                            ),
                            None => parser::parse_mark_prices(message, received_ts),
                        }
                        .into_iter()
                        .map(GateioEvent::MarkPrice),
                    );
                }
                events
            }
            _ => Vec::new(),
        }
    }
}

pub(crate) fn product_from_symbol(symbol: &Symbol) -> Result<GateioProduct> {
    let quote = symbol.as_str().split('-').nth(1).unwrap_or_default();
    match (symbol.kind(), quote) {
        (InstrumentKind::Spot, _) => Ok(GateioProduct::Spot),
        (InstrumentKind::Perpetual, "USDT") => Ok(GateioProduct::UsdtPerpetual),
        (InstrumentKind::Perpetual, _) => Ok(GateioProduct::BtcPerpetual),
        (InstrumentKind::Futures, "USDT") => Ok(GateioProduct::UsdtDelivery),
        _ => Err(Error::UnsupportedSymbol(symbol.as_str().to_owned())),
    }
}

fn native_symbol(symbol: &Symbol) -> String {
    match symbol.kind() {
        InstrumentKind::Spot | InstrumentKind::Perpetual => symbol
            .as_str()
            .split('-')
            .take(2)
            .collect::<Vec<_>>()
            .join("_"),
        InstrumentKind::Option
        | InstrumentKind::Futures
        | InstrumentKind::Margin
        | InstrumentKind::Unknown => symbol.as_str().replace('-', "_"),
        // Future instrument kinds keep the raw normalized form.
        _ => symbol.as_str().replace('-', "_"),
    }
}

fn channel_name(channel: Channel) -> &'static str {
    match channel {
        Channel::Candles => "candles",
        Channel::Funding => "funding",
        Channel::Liquidations => "liquidations",
        Channel::Ticker => "ticker",
        Channel::Trade => "trade",
        Channel::L2Book => "l2_book",
        Channel::OpenInterest => "open_interest",
        Channel::Index => "index",
        Channel::L1Book => "l1_book",
        Channel::MarkPrice => "mark_price",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::{GateioAdapter, GateioProduct};
    use crate::exchange::gateio::Gateio;
    use serde_json::Value;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn candle_interval_reaches_subscription_payload() {
        let feed = Gateio::new()
            .candles()
            .candles_interval("1d")
            .symbol("BTC-USDT")
            .build();
        let messages = GateioAdapter::subscription_messages(&feed);
        let payload: Value = serde_json::from_str(&messages[0]).expect("subscribe payload");
        let serialized = payload.to_string();
        assert!(serialized.contains("spot.candlesticks"));
        assert!(serialized.contains("\"24h\""));
    }

    #[test]
    fn candle_interval_wire_maps_official_gateio_intervals() {
        assert_eq!(GateioAdapter::candle_interval_wire("10s"), Some("10s"));
        assert_eq!(GateioAdapter::candle_interval_wire("1m"), Some("1m"));
        assert_eq!(GateioAdapter::candle_interval_wire("1h"), Some("1h"));
        assert_eq!(GateioAdapter::candle_interval_wire("1d"), Some("24h"));
        assert_eq!(GateioAdapter::candle_interval_wire("1w"), Some("7d"));
        assert_eq!(GateioAdapter::candle_interval_wire("1M"), Some("30d"));
        assert_eq!(GateioAdapter::candle_interval_wire("3m"), None);
    }

    #[test]
    fn builds_gateio_v4_public_websocket_url() {
        let feed = Gateio::new().ticker().symbol("BTC-USDT").build();
        assert_eq!(
            GateioAdapter::subscription_url(&feed),
            "wss://api.gateio.ws/ws/v4/"
        );
    }

    #[test]
    fn builds_gateio_v4_subscription_messages() {
        let feed = Gateio::new()
            .ticker()
            .trade()
            .l2_book()
            .candles()
            .symbol("BTC-USDT")
            .build();
        let payloads = GateioAdapter::subscription_messages(&feed);
        let joined = payloads.join("\n");

        assert!(joined.contains("spot.book_ticker"));
        assert!(joined.contains("spot.trades"));
        assert!(joined.contains("spot.order_book_update"));
        assert!(joined.contains("spot.candlesticks"));
        assert!(joined.contains("BTC_USDT"));
    }

    #[test]
    fn uses_current_timestamp_in_subscription() {
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let feed = Gateio::new().trade().symbol("BTC-USDT").build();
        let payload: Value =
            serde_json::from_str(&GateioAdapter::subscription_messages(&feed)[0]).unwrap();
        let after = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let timestamp = payload["time"].as_u64().unwrap();
        assert!((before..=after).contains(&timestamp));
    }

    #[test]
    fn builds_usdt_futures_connection_and_channels() {
        let feed = Gateio::new()
            .trade()
            .l2_book()
            .symbol("BTC-USDT-PERP")
            .exchange_symbol("BTC_USDT")
            .build();

        assert_eq!(
            GateioAdapter::subscription_url(&feed),
            "wss://fx-ws.gateio.ws/v4/ws/usdt"
        );
        let joined = GateioAdapter::subscription_messages(&feed).join("\n");
        assert!(joined.contains("futures.trades"));
        assert!(joined.contains("futures.order_book_update"));
        assert!(joined.contains("\"100ms\",\"100\""));
    }

    #[test]
    fn builds_delivery_connection_for_dated_future() {
        let feed = Gateio::new()
            .trade()
            .symbol("BTC-USDT-20260925")
            .exchange_symbol("BTC_USDT_20260925")
            .build();

        assert_eq!(
            GateioAdapter::subscription_url(&feed),
            "wss://fx-ws.gateio.ws/v4/ws/delivery/usdt"
        );
    }

    #[test]
    fn plans_spot_perpetual_and_delivery_connections() {
        let spot = Gateio::new()
            .ticker()
            .l2_book()
            .symbol("BTC-USDT")
            .exchange_symbol("BTC_USDT")
            .build();
        let perpetual = Gateio::new()
            .trade()
            .l2_book()
            .symbol("BTC-USDT-PERP")
            .exchange_symbol("BTC_USDT")
            .build();
        let delivery = Gateio::new()
            .candles()
            .l2_book()
            .symbol("BTC-USDT-20260925")
            .exchange_symbol("BTC_USDT_20260925")
            .build();

        let spot_plan = GateioAdapter::connection_plans(&spot).unwrap().remove(0);
        assert_eq!(spot_plan.product, GateioProduct::Spot);
        assert!(spot_plan.websocket_url.ends_with("/ws/v4/"));
        assert!(spot_plan.snapshot_urls[0].contains("/spot/order_book?"));
        assert!(spot_plan.snapshot_urls[0].contains("with_id=true"));

        let perpetual_plan = GateioAdapter::connection_plans(&perpetual)
            .unwrap()
            .remove(0);
        assert_eq!(perpetual_plan.product, GateioProduct::UsdtPerpetual);
        assert!(perpetual_plan.websocket_url.ends_with("/ws/usdt"));
        assert!(perpetual_plan.snapshot_urls[0].contains("/futures/usdt/order_book?"));

        let delivery_plan = GateioAdapter::connection_plans(&delivery)
            .unwrap()
            .remove(0);
        assert_eq!(delivery_plan.product, GateioProduct::UsdtDelivery);
        assert!(delivery_plan.websocket_url.ends_with("/ws/delivery/usdt"));
        assert!(delivery_plan.snapshot_urls[0].contains("/delivery/usdt/order_book?"));
    }

    #[test]
    fn rejects_gateio_spot_extended_channels() {
        let spot_funding = Gateio::new()
            .funding()
            .symbol("BTC-USDT")
            .exchange_symbol("BTC_USDT")
            .build();

        assert!(GateioAdapter::connection_plans(&spot_funding).is_err());

        let spot_oi = Gateio::new()
            .open_interest()
            .symbol("BTC-USDT")
            .exchange_symbol("BTC_USDT")
            .build();

        assert!(GateioAdapter::connection_plans(&spot_oi).is_err());
    }

    #[test]
    fn derivative_extended_channels_ride_futures_tickers_once() {
        let feed = Gateio::new()
            .funding()
            .open_interest()
            .index()
            .mark_price()
            .symbol("BTC-USDT-PERP")
            .exchange_symbol("BTC_USDT")
            .build();

        let plans = GateioAdapter::connection_plans(&feed).unwrap();
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].product, GateioProduct::UsdtPerpetual);
        let messages = plans[0].subscription_messages.join("\n");
        // Four channels collapse onto a single `futures.tickers`
        // subscription.
        assert_eq!(messages.matches("futures.tickers").count(), 1);
        assert!(messages.contains("futures.tickers"));
        assert!(!messages.contains("book_ticker"));
    }

    #[test]
    fn l1_book_rides_book_ticker_once() {
        let feed = Gateio::new()
            .ticker()
            .l1_book()
            .symbol("BTC-USDT")
            .exchange_symbol("BTC_USDT")
            .build();

        let plans = GateioAdapter::connection_plans(&feed).unwrap();
        assert_eq!(plans[0].product, GateioProduct::Spot);
        let messages = plans[0].subscription_messages.join("\n");
        assert_eq!(messages.matches("spot.book_ticker").count(), 1);
    }

    #[test]
    fn delivery_extended_channels_are_planned() {
        let feed = Gateio::new()
            .open_interest()
            .index()
            .symbol("BTC-USDT-20260925")
            .exchange_symbol("BTC_USDT_20260925")
            .build();

        let plans = GateioAdapter::connection_plans(&feed).unwrap();
        assert_eq!(plans[0].product, GateioProduct::UsdtDelivery);
        assert!(
            plans[0]
                .subscription_messages
                .join("\n")
                .contains("futures.tickers")
        );
    }
}
