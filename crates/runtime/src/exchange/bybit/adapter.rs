use super::parser;
use crate::exchange::ExchangeFeed;
use cryptofeed_core::{
    error::{Error, Result},
    exchange::Channel,
    symbol::{InstrumentKind, Symbol},
};
use serde_json::Value;

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

pub struct BybitAdapter;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BybitProduct {
    Spot,
    Linear,
    Inverse,
    Option,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BybitControl {
    Pong,
    Subscribed,
}

pub enum BybitEvent {
    #[cfg(feature = "candles")]
    Candle(Candle),
    #[cfg(feature = "funding")]
    Funding(Funding),
    #[cfg(feature = "index")]
    IndexPrice(IndexPrice),
    #[cfg(feature = "liquidations")]
    Liquidation(Liquidation),
    #[cfg(feature = "markprice")]
    MarkPrice(MarkPrice),
    #[cfg(feature = "openinterest")]
    OpenInterest(OpenInterest),
    #[cfg(feature = "orderbook")]
    L1Book(L1Book),
    #[cfg(feature = "orderbook")]
    L2Book(L2Book),
    #[cfg(feature = "ticker")]
    Ticker(Ticker),
    #[cfg(feature = "trade")]
    Trade(Trade),
}

impl BybitAdapter {
    pub const HEARTBEAT_INTERVAL_SECS: u64 = 20;
    pub fn websocket_url() -> &'static str {
        "wss://stream.bybit.com/v5/public/spot"
    }

    pub fn websocket_url_for_product(product: BybitProduct) -> &'static str {
        match product {
            BybitProduct::Spot => "wss://stream.bybit.com/v5/public/spot",
            BybitProduct::Linear => "wss://stream.bybit.com/v5/public/linear",
            BybitProduct::Inverse => "wss://stream.bybit.com/v5/public/inverse",
            BybitProduct::Option => "wss://stream.bybit.com/v5/public/option",
        }
    }

    pub fn product_for_symbol(symbol: &Symbol) -> BybitProduct {
        let normalized = symbol.as_str();
        match symbol.kind() {
            InstrumentKind::Spot | InstrumentKind::Margin => BybitProduct::Spot,
            InstrumentKind::Option => BybitProduct::Option,
            InstrumentKind::Perpetual | InstrumentKind::Futures => derivative_product(normalized),
            InstrumentKind::Unknown => {
                if normalized.ends_with("-PERP") || looks_like_dated_future(normalized) {
                    derivative_product(normalized)
                } else {
                    BybitProduct::Spot
                }
            }
            // Future instrument kinds default to spot, like the Unknown
            // fallback.
            _ => BybitProduct::Spot,
        }
    }

    pub fn subscription_url(feed: &ExchangeFeed) -> String {
        let product = feed
            .symbols
            .first()
            .map(Self::product_for_symbol)
            .unwrap_or(BybitProduct::Spot);
        Self::websocket_url_for_product(product).to_owned()
    }

    pub fn subscription_urls(feed: &ExchangeFeed) -> Vec<String> {
        let mut products = feed
            .symbols
            .iter()
            .map(Self::product_for_symbol)
            .collect::<Vec<_>>();
        if products.is_empty() {
            products.push(BybitProduct::Spot);
        }
        products.sort_by_key(|product| match product {
            BybitProduct::Spot => 0,
            BybitProduct::Linear => 1,
            BybitProduct::Inverse => 2,
            BybitProduct::Option => 3,
        });
        products.dedup();
        products
            .into_iter()
            .map(|product| Self::websocket_url_for_product(product).to_owned())
            .collect()
    }

    pub fn heartbeat_message() -> &'static str {
        r#"{"op":"ping"}"#
    }

    /// Normalized candle interval to the Bybit wire form (`kline.{wire}`).
    /// Official intervals: `1` `3` `5` `15` `30` `60` `120` `240` `360`
    /// `720` (min), `D` (day), `W` (week), `M` (month); verified against the
    /// official v5 kline documentation on 2026-08-06.
    pub fn candle_interval_wire(interval: &str) -> Option<&'static str> {
        match interval {
            "1m" => Some("1"),
            "3m" => Some("3"),
            "5m" => Some("5"),
            "15m" => Some("15"),
            "30m" => Some("30"),
            "1h" => Some("60"),
            "2h" => Some("120"),
            "4h" => Some("240"),
            "6h" => Some("360"),
            "12h" => Some("720"),
            "1d" => Some("D"),
            "1w" => Some("W"),
            "1M" => Some("M"),
            _ => None,
        }
    }

    /// Whether the L2 depth level is officially supported for the product.
    /// Spot/linear/inverse expose `orderbook.{1,50,200,1000}` (level 1 is the
    /// L1 channel family); options expose `orderbook.{25,100}`; verified
    /// against the official v5 orderbook documentation on 2026-08-06.
    pub fn l2_book_depth_supported(level: u16, product: BybitProduct) -> bool {
        if product == BybitProduct::Option {
            matches!(level, 25 | 100)
        } else {
            matches!(level, 50 | 200 | 1000)
        }
    }

    pub fn parse_control_message(message: &Value) -> Option<Result<BybitControl>> {
        let op = message.get("op")?.as_str()?;
        if !matches!(op, "ping" | "pong" | "subscribe") {
            return None;
        }
        if message.get("success").and_then(Value::as_bool) == Some(false) {
            let detail = message
                .get("ret_msg")
                .and_then(Value::as_str)
                .unwrap_or("Bybit request rejected");
            return Some(Err(Error::Parse(format!("bybit {op} error: {detail}"))));
        }
        if matches!(op, "ping" | "pong") {
            Some(Ok(BybitControl::Pong))
        } else {
            Some(Ok(BybitControl::Subscribed))
        }
    }

    pub fn subscription_message(feed: &ExchangeFeed) -> String {
        let symbols: Vec<String> = if feed.exchange_symbols.is_empty() {
            feed.symbols
                .iter()
                .map(|symbol| fallback_exchange_symbol(symbol.as_str()))
                .collect()
        } else {
            feed.exchange_symbols.clone()
        };
        let args: Vec<String> = symbols
            .iter()
            .enumerate()
            .flat_map(|(index, exchange_symbol)| {
                let product = feed
                    .symbols
                    .get(index)
                    .map(Self::product_for_symbol)
                    .unwrap_or(BybitProduct::Spot);
                feed.channels.iter().map(move |channel| match channel {
                    Channel::Candles => format!(
                        "kline.{}.{exchange_symbol}",
                        Self::candle_interval_wire(&feed.candle_interval).unwrap_or("1")
                    ),
                    Channel::Ticker if product == BybitProduct::Spot => {
                        format!("orderbook.1.{exchange_symbol}")
                    }
                    Channel::Ticker => format!("tickers.{exchange_symbol}"),
                    // Option trades are a base-coin stream
                    // (`publicTrade.BTC`), not per-symbol.
                    Channel::Trade if product == BybitProduct::Option => {
                        format!(
                            "publicTrade.{}",
                            feed.symbols
                                .get(index)
                                .map(|symbol| symbol.as_str().split('-').next().unwrap_or(""))
                                .unwrap_or("")
                        )
                    }
                    Channel::Trade => format!("publicTrade.{exchange_symbol}"),
                    Channel::L2Book => format!(
                        "orderbook.{}.{exchange_symbol}",
                        // Options expose only `orderbook.{25,100}`; other
                        // products default to level 50 (verified 2026-08-06).
                        feed.l2_book_depth
                            .unwrap_or(if product == BybitProduct::Option {
                                25
                            } else {
                                50
                            })
                    ),
                    // Bybit no longer serves `funding.{symbol}` (verified
                    // live 2026-08-06): funding rides the derivative
                    // `tickers.{symbol}` stream alongside OI/index/mark price.
                    Channel::Funding => format!("tickers.{exchange_symbol}"),
                    Channel::Liquidations => format!("allLiquidation.{exchange_symbol}"),
                    // Bybit carries open interest inside the derivative
                    // tickers stream; there is no standalone channel.
                    Channel::OpenInterest => format!("tickers.{exchange_symbol}"),
                    Channel::Index => format!("tickers.{exchange_symbol}"),
                    Channel::MarkPrice => format!("tickers.{exchange_symbol}"),
                    Channel::L1Book => format!("orderbook.1.{exchange_symbol}"),
                    _ => String::new(),
                })
            })
            .collect();

        let mut seen = std::collections::HashSet::new();
        let args: Vec<String> = args
            .into_iter()
            .filter(|arg| seen.insert(arg.clone()))
            .collect();

        serde_json::json!({
            "op": "subscribe",
            "args": args,
        })
        .to_string()
    }

    pub fn parse_message(message: &Value, received_ts: f64) -> Option<BybitEvent> {
        Self::parse_messages(message, received_ts)
            .into_iter()
            .next()
    }

    pub fn parse_messages(message: &Value, received_ts: f64) -> Vec<BybitEvent> {
        Self::parse_messages_with_symbol(message, received_ts, None, None)
    }

    pub fn parse_messages_for_feed(
        feed: &ExchangeFeed,
        message: &Value,
        received_ts: f64,
    ) -> Vec<BybitEvent> {
        let symbol = normalized_symbol(feed, message);
        Self::parse_messages_with_symbol(message, received_ts, symbol, Some(feed))
    }

    #[cfg(feature = "orderbook")]
    pub fn parse_l2_book_update_for_feed(
        feed: &ExchangeFeed,
        message: &Value,
        received_ts: f64,
    ) -> Option<super::book_sync::BybitDepthUpdate> {
        match normalized_symbol(feed, message) {
            Some(symbol) => parser::parse_l2_book_update_for_symbol(message, received_ts, symbol),
            None => parser::parse_l2_book_update(message, received_ts),
        }
    }

    fn parse_messages_with_symbol(
        message: &Value,
        received_ts: f64,
        symbol: Option<&Symbol>,
        feed: Option<&ExchangeFeed>,
    ) -> Vec<BybitEvent> {
        let Some(topic) = message.get("topic").and_then(Value::as_str) else {
            return Vec::new();
        };

        if topic.starts_with("tickers.") {
            let mut events = Vec::new();
            #[cfg(feature = "ticker")]
            {
                events.extend(
                    match symbol {
                        Some(symbol) => {
                            parser::parse_tickers_for_symbol(message, received_ts, symbol)
                        }
                        None => parser::parse_tickers(message, received_ts),
                    }
                    .into_iter()
                    .map(BybitEvent::Ticker),
                );
            }
            #[cfg(feature = "openinterest")]
            {
                events.extend(
                    match symbol {
                        Some(symbol) => {
                            parser::parse_open_interests_for_symbol(message, received_ts, symbol)
                        }
                        None => parser::parse_open_interests(message, received_ts),
                    }
                    .into_iter()
                    .map(BybitEvent::OpenInterest),
                );
            }
            #[cfg(feature = "index")]
            {
                events.extend(
                    match symbol {
                        Some(symbol) => {
                            parser::parse_index_prices_for_symbol(message, received_ts, symbol)
                        }
                        None => parser::parse_index_prices(message, received_ts),
                    }
                    .into_iter()
                    .map(BybitEvent::IndexPrice),
                );
            }
            #[cfg(feature = "markprice")]
            {
                events.extend(
                    match symbol {
                        Some(symbol) => {
                            parser::parse_mark_prices_for_symbol(message, received_ts, symbol)
                        }
                        None => parser::parse_mark_prices(message, received_ts),
                    }
                    .into_iter()
                    .map(BybitEvent::MarkPrice),
                );
            }
            #[cfg(feature = "funding")]
            {
                events.extend(
                    match symbol {
                        Some(symbol) => {
                            parser::parse_fundings_for_symbol(message, received_ts, symbol)
                        }
                        None => parser::parse_fundings(message, received_ts),
                    }
                    .into_iter()
                    .map(BybitEvent::Funding),
                );
            }
            return events;
        }
        if topic.starts_with("orderbook.1.") {
            let mut events = Vec::new();
            #[cfg(feature = "ticker")]
            {
                events.extend(
                    match symbol {
                        Some(symbol) => {
                            parser::parse_bbo_ticker_for_symbol(message, received_ts, symbol)
                        }
                        None => parser::parse_bbo_ticker(message, received_ts),
                    }
                    .map(BybitEvent::Ticker),
                );
            }
            #[cfg(feature = "orderbook")]
            {
                events.extend(
                    match symbol {
                        Some(symbol) => {
                            parser::parse_l1_book_for_symbol(message, received_ts, Some(symbol))
                        }
                        None => parser::parse_l1_book(message, received_ts),
                    }
                    .map(BybitEvent::L1Book),
                );
            }
            return events;
        }
        if topic.starts_with("publicTrade.") {
            #[cfg(feature = "trade")]
            {
                // `publicTrade.{base}` is a base-coin stream for options: the
                // full option symbol appears per row in `data.s`, and a batch
                // can span several series of the base coin. Resolve each row
                // against the feed; per-symbol streams keep the topic symbol.
                if feed.is_some_and(|feed| {
                    feed.symbols
                        .iter()
                        .any(|symbol| symbol.kind() == InstrumentKind::Option)
                }) {
                    return parser::parse_trades_for_rows(message, received_ts, |native| {
                        feed.and_then(|feed| normalized_symbol_for_native(feed, native))
                            .cloned()
                    })
                    .into_iter()
                    .map(BybitEvent::Trade)
                    .collect();
                }
                return match symbol {
                    Some(symbol) => parser::parse_trades_for_symbol(message, received_ts, symbol),
                    None => parser::parse_trades(message, received_ts),
                }
                .into_iter()
                .map(BybitEvent::Trade)
                .collect();
            }
        }
        if topic.starts_with("orderbook.") {
            #[cfg(feature = "orderbook")]
            {
                return match symbol {
                    Some(symbol) => parser::parse_l2_book_for_symbol(message, received_ts, symbol),
                    None => parser::parse_l2_book(message, received_ts),
                }
                .map(BybitEvent::L2Book)
                .into_iter()
                .collect();
            }
        }
        if topic.starts_with("kline.") {
            #[cfg(feature = "candles")]
            {
                return match symbol {
                    Some(symbol) => parser::parse_candles_for_symbol(message, received_ts, symbol),
                    None => parser::parse_candles(message, received_ts),
                }
                .into_iter()
                .map(BybitEvent::Candle)
                .collect();
            }
        }
        if topic.starts_with("funding.") {
            #[cfg(feature = "funding")]
            {
                return match symbol {
                    Some(symbol) => parser::parse_fundings_for_symbol(message, received_ts, symbol),
                    None => parser::parse_fundings(message, received_ts),
                }
                .into_iter()
                .map(BybitEvent::Funding)
                .collect();
            }
        }
        if topic.starts_with("allLiquidation.") {
            #[cfg(feature = "liquidations")]
            {
                return match symbol {
                    Some(symbol) => {
                        parser::parse_liquidations_for_symbol(message, received_ts, symbol)
                    }
                    None => parser::parse_liquidations(message, received_ts),
                }
                .into_iter()
                .map(BybitEvent::Liquidation)
                .collect();
            }
        }

        Vec::new()
    }
}

fn looks_like_dated_future(symbol: &str) -> bool {
    symbol
        .rsplit('-')
        .next()
        .is_some_and(|suffix| suffix.len() >= 6 && suffix.chars().any(|ch| ch.is_ascii_digit()))
}

fn derivative_product(symbol: &str) -> BybitProduct {
    match symbol.split('-').nth(1).unwrap_or_default() {
        "USDT" | "USDC" => BybitProduct::Linear,
        _ => BybitProduct::Inverse,
    }
}

fn fallback_exchange_symbol(symbol: &str) -> String {
    symbol
        .strip_suffix("-PERP")
        .unwrap_or(symbol)
        .replace('-', "")
}

/// Resolve a native stream symbol (per row or per topic) against the feed's
/// configured instruments.
fn normalized_symbol_for_native<'a>(feed: &'a ExchangeFeed, native: &str) -> Option<&'a Symbol> {
    feed.symbols.iter().enumerate().find_map(|(index, symbol)| {
        let exchange_symbol = feed
            .exchange_symbols
            .get(index)
            .map(String::as_str)
            .unwrap_or_else(|| symbol.as_str());
        let fallback = fallback_exchange_symbol(symbol.as_str());
        (exchange_symbol.eq_ignore_ascii_case(native) || fallback.eq_ignore_ascii_case(native))
            .then_some(symbol)
    })
}

fn normalized_symbol<'a>(feed: &'a ExchangeFeed, message: &Value) -> Option<&'a Symbol> {
    let topic = message.get("topic").and_then(Value::as_str);
    // `publicTrade.{base}` is a base-coin stream for options: the full option
    // symbol appears per row in `data.s`, not in the topic.
    let native = if topic.is_some_and(|topic| topic.starts_with("publicTrade.")) {
        // Trade payloads are arrays; the instrument is per row.
        message
            .get("data")
            .and_then(Value::as_array)
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("s"))
            .and_then(Value::as_str)
            .or_else(|| topic.and_then(|topic| topic.rsplit('.').next()))
    } else {
        topic
            .and_then(|topic| topic.rsplit('.').next())
            .or_else(|| {
                message
                    .get("data")
                    .and_then(|data| data.get("symbol").or_else(|| data.get("s")))
                    .and_then(Value::as_str)
            })
    }?;

    normalized_symbol_for_native(feed, native)
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "trade")]
    use super::BybitEvent;
    use super::{BybitAdapter, BybitControl, BybitProduct};
    use crate::exchange::bybit::Bybit;

    #[test]
    fn websocket_url_for_product_matches_official_endpoints() {
        assert_eq!(
            BybitAdapter::websocket_url_for_product(BybitProduct::Spot),
            "wss://stream.bybit.com/v5/public/spot"
        );
        assert_eq!(
            BybitAdapter::websocket_url_for_product(BybitProduct::Linear),
            "wss://stream.bybit.com/v5/public/linear"
        );
        assert_eq!(
            BybitAdapter::websocket_url_for_product(BybitProduct::Inverse),
            "wss://stream.bybit.com/v5/public/inverse"
        );
        assert_eq!(
            BybitAdapter::websocket_url_for_product(BybitProduct::Option),
            "wss://stream.bybit.com/v5/public/option"
        );
    }

    #[test]
    fn candle_interval_and_l2_depth_reach_subscription_message() {
        let feed = Bybit::new()
            .candles()
            .candles_interval("1h")
            .l2_book()
            .l2_book_depth(200)
            .symbol("BTC-USDT")
            .build();
        let payload = BybitAdapter::subscription_message(&feed);
        assert!(payload.contains("kline.60.BTCUSDT"));
        assert!(payload.contains("orderbook.200.BTCUSDT"));
    }

    #[test]
    fn candle_interval_wire_maps_official_bybit_intervals() {
        assert_eq!(BybitAdapter::candle_interval_wire("1m"), Some("1"));
        assert_eq!(BybitAdapter::candle_interval_wire("1h"), Some("60"));
        assert_eq!(BybitAdapter::candle_interval_wire("12h"), Some("720"));
        assert_eq!(BybitAdapter::candle_interval_wire("1d"), Some("D"));
        assert_eq!(BybitAdapter::candle_interval_wire("1w"), Some("W"));
        assert_eq!(BybitAdapter::candle_interval_wire("1M"), Some("M"));
        assert_eq!(BybitAdapter::candle_interval_wire("8h"), None);
    }

    #[test]
    fn builds_bybit_v5_public_websocket_url() {
        let feed = Bybit::new().ticker().symbol("BTC-USDT").build();
        assert_eq!(
            BybitAdapter::subscription_url(&feed),
            "wss://stream.bybit.com/v5/public/spot"
        );
    }

    #[test]
    fn routes_bybit_products_to_distinct_v5_urls() {
        let linear = Bybit::new().ticker().symbol("BTC-USDT-PERP").build();
        let inverse = Bybit::new().ticker().symbol("BTC-USD-PERP").build();

        assert_eq!(
            BybitAdapter::subscription_url(&linear),
            "wss://stream.bybit.com/v5/public/linear"
        );
        assert_eq!(
            BybitAdapter::subscription_url(&inverse),
            "wss://stream.bybit.com/v5/public/inverse"
        );

        let mixed = Bybit::new()
            .ticker()
            .symbol("BTC-USDT")
            .symbol("BTC-USDT-PERP")
            .symbol("BTC-USD-PERP")
            .build();
        assert_eq!(BybitAdapter::subscription_urls(&mixed).len(), 3);
    }

    #[cfg(feature = "trade")]
    #[test]
    fn feed_context_preserves_bybit_inverse_product_identity() {
        let feed = Bybit::new()
            .trade()
            .symbol("BTC-USD-PERP")
            .exchange_symbol("BTCUSD")
            .build();
        let message = serde_json::json!({
            "topic": "publicTrade.BTCUSD",
            "data": [{"T": 1672304486865i64, "s": "BTCUSD", "S": "Buy", "v": "1", "p": "65000", "i": "a"}]
        });

        let events = BybitAdapter::parse_messages_for_feed(&feed, &message, 1672304487.0);
        match &events[0] {
            BybitEvent::Trade(trade) => assert_eq!(trade.symbol.as_str(), "BTC-USD-PERP"),
            _ => panic!("expected trade"),
        }
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn feed_context_preserves_bybit_linear_book_identity() {
        let feed = Bybit::new()
            .l2_book()
            .symbol("BTC-USDT-PERP")
            .exchange_symbol("BTCUSDT")
            .build();
        let message = serde_json::json!({
            "topic": "orderbook.50.BTCUSDT",
            "type": "snapshot",
            "ts": 1672304484978i64,
            "data": {
                "s": "BTCUSDT",
                "b": [["1", "2"]],
                "a": [["3", "4"]],
                "u": 100,
                "seq": 200
            }
        });

        let update =
            BybitAdapter::parse_l2_book_update_for_feed(&feed, &message, 1.0).expect("book update");
        match update.book {
            cryptofeed_orderbook::L2Book::Snapshot(snapshot) => {
                assert_eq!(snapshot.symbol.as_str(), "BTC-USDT-PERP")
            }
            _ => panic!("expected snapshot"),
        }
    }

    #[test]
    fn exposes_heartbeat_and_subscription_control_messages() {
        assert_eq!(BybitAdapter::heartbeat_message(), r#"{"op":"ping"}"#);
        assert!(matches!(
            BybitAdapter::parse_control_message(&serde_json::json!({
                "success": true,
                "ret_msg": "pong",
                "op": "ping"
            })),
            Some(Ok(BybitControl::Pong))
        ));
        assert!(
            BybitAdapter::parse_control_message(&serde_json::json!({
                "success": false,
                "ret_msg": "invalid topic",
                "op": "subscribe"
            }))
            .expect("control")
            .is_err()
        );
    }

    #[test]
    fn builds_bybit_v5_subscription_message() {
        let feed = Bybit::new()
            .ticker()
            .trade()
            .l2_book()
            .candles()
            .symbol("BTC-USDT")
            .build();
        let payload = BybitAdapter::subscription_message(&feed);

        assert!(payload.contains("orderbook.1.BTCUSDT"));
        assert!(payload.contains("publicTrade.BTCUSDT"));
        assert!(payload.contains("orderbook.50.BTCUSDT"));
        assert!(payload.contains("kline.1.BTCUSDT"));
    }

    #[test]
    fn option_l2_book_defaults_to_level_25() {
        let feed = Bybit::new()
            .l2_book()
            .instrument(cryptofeed_core::symbol::Symbol::option(
                "BTC", "USDC", "30DEC22", "18000", "C",
            ))
            .exchange_symbol("BTC-30DEC22-18000-C")
            .build();
        let payload = BybitAdapter::subscription_message(&feed);
        assert!(payload.contains("orderbook.25.BTC-30DEC22-18000-C"));
        assert!(!payload.contains("orderbook.50."));
    }

    #[test]
    fn routes_options_to_public_option_endpoint() {
        let feed = Bybit::new()
            .ticker()
            .instrument(cryptofeed_core::symbol::Symbol::option(
                "BTC", "USDC", "30DEC22", "18000", "C",
            ))
            .exchange_symbol("BTC-30DEC22-18000-C")
            .build();

        assert_eq!(
            BybitAdapter::subscription_url(&feed),
            "wss://stream.bybit.com/v5/public/option"
        );
        assert!(BybitAdapter::subscription_message(&feed).contains("tickers.BTC-30DEC22-18000-C"));
    }

    #[test]
    fn keeps_derivative_ticker_on_tickers_topic() {
        let feed = Bybit::new()
            .ticker()
            .symbol("BTC-USDT-PERP")
            .exchange_symbol("BTCUSDT")
            .build();

        assert!(BybitAdapter::subscription_message(&feed).contains("tickers.BTCUSDT"));
    }
}
