//! Upbit public WebSocket routing and subscription framing.
//!
//! One public Korean quotation endpoint serves every implemented channel. A
//! subscription request is a single JSON array containing a ticket object, one
//! data-type object per channel and a format object; all configured symbols of a
//! channel share one object. Upbit documents no subscription acknowledgement:
//! a rejected request is answered with a documented error envelope and otherwise
//! data starts streaming. Liveness is the documented text `PING` /
//! `{"status":"UP"}` exchange.

use super::parser;
use crate::exchange::ExchangeFeed;
use cryptofeed_core::{
    error::{Error, Result},
    exchange::Channel,
    symbol::Symbol,
};
use serde_json::Value;
use std::time::Duration;

#[cfg(feature = "candles")]
use cryptofeed_candles::Candle;
#[cfg(feature = "orderbook")]
use cryptofeed_orderbook::L2Book;
#[cfg(feature = "trade")]
use cryptofeed_trade::Trade;

pub struct UpbitAdapter;

pub enum UpbitEvent {
    #[cfg(feature = "candles")]
    Candle(Candle),
    #[cfg(feature = "trade")]
    Trade(Trade),
}

/// Documented public Korean quotation endpoint.
pub const WEBSOCKET_URL: &str = "wss://api.upbit.com/websocket/v1";
/// Requester identifier sent in the ticket object.
pub const TICKET: &str = "cryptofeed-rs";

impl UpbitAdapter {
    pub fn websocket_url() -> &'static str {
        WEBSOCKET_URL
    }

    pub fn subscription_url(_feed: &ExchangeFeed) -> String {
        Self::websocket_url().to_owned()
    }

    /// Application liveness: a text `PING` is answered by periodic
    /// `{"status":"UP"}` frames.
    pub fn heartbeat_message() -> &'static str {
        "PING"
    }

    /// Normalized candle interval to the documented `candle.<unit>` type.
    pub fn candle_type(interval: &str) -> Option<&'static str> {
        Some(match interval {
            "1s" => "candle.1s",
            "1m" => "candle.1m",
            "3m" => "candle.3m",
            "5m" => "candle.5m",
            "10m" => "candle.10m",
            "15m" => "candle.15m",
            "30m" => "candle.30m",
            "1h" => "candle.60m",
            "4h" => "candle.240m",
            _ => return None,
        })
    }

    /// Documented order-book unit counts. `None` keeps the documented default
    /// of 30 units, which is requested as a plain market code.
    pub fn l2_book_depth_wire(depth: Option<u16>) -> Option<u16> {
        match depth {
            None => Some(parser::DEFAULT_ORDERBOOK_UNITS),
            Some(level) if Self::l2_book_depth_supported(level) => Some(level),
            Some(_) => None,
        }
    }

    pub fn l2_book_depth_supported(level: u16) -> bool {
        matches!(level, 1 | 5 | 15 | 30)
    }

    /// Documented `{code}.{count}` order-book form. The default count is sent as
    /// a plain code so the request matches the documented default exactly.
    pub fn orderbook_code(native: &str, units: u16) -> String {
        if units == parser::DEFAULT_ORDERBOOK_UNITS {
            native.to_owned()
        } else {
            format!("{native}.{units}")
        }
    }

    fn native_symbols(feed: &ExchangeFeed) -> Result<Vec<String>> {
        if feed.exchange_symbols.is_empty() {
            return Ok(feed.symbols.iter().map(fallback_exchange_symbol).collect());
        }
        if feed.exchange_symbols.len() != feed.symbols.len() {
            return Err(Error::InvalidConfiguration(
                "normalized and exchange symbol counts must match".to_owned(),
            ));
        }
        Ok(feed.exchange_symbols.clone())
    }

    /// One documented subscription message for every configured channel.
    pub fn subscription_messages(feed: &ExchangeFeed) -> Result<Vec<String>> {
        let symbols = Self::native_symbols(feed)?;
        let units = Self::l2_book_depth_wire(feed.l2_book_depth).ok_or_else(|| {
            Error::UnsupportedCapability(format!("Upbit l2 depth level {:?}", feed.l2_book_depth))
        })?;
        let mut message = vec![serde_json::json!({"ticket": TICKET})];
        let mut seen = std::collections::HashSet::new();
        for channel in &feed.channels {
            let (data_type, codes) = match channel {
                Channel::Trade => ("trade", symbols.clone()),
                Channel::L2Book => (
                    "orderbook",
                    symbols
                        .iter()
                        .map(|native| Self::orderbook_code(native, units))
                        .collect(),
                ),
                Channel::Candles => (
                    Self::candle_type(&feed.candle_interval).ok_or_else(|| {
                        Error::UnsupportedCapability(format!(
                            "Upbit candle interval {:?}",
                            feed.candle_interval
                        ))
                    })?,
                    symbols.clone(),
                ),
                // Unreachable after capability preflight; do not invent a
                // native type for ticker, L1 or derivative channels.
                _ => continue,
            };
            if !seen.insert(data_type) {
                continue;
            }
            message.push(serde_json::json!({"type": data_type, "codes": codes}));
        }
        message.push(serde_json::json!({"format": "DEFAULT"}));
        Ok(vec![Value::Array(message).to_string()])
    }

    /// Documented error envelope (`{"error": {"name": ..., "message": ...}}`).
    pub fn error(message: &Value) -> Option<Error> {
        let name = message.get("error")?.get("name")?.as_str()?;
        // Both documented throttling names are transient; every other name is a
        // permanent request/configuration problem.
        if matches!(name, "Too Many Requests" | "I'm a teapot") {
            return Some(Error::RateLimited {
                code: name.to_owned(),
                retry_after: Duration::from_secs(60),
            });
        }
        Some(Error::Subscription(format!(
            "upbit websocket error: {name}"
        )))
    }

    /// Documented heartbeat acknowledgement, filtered before parsing.
    pub fn is_heartbeat_status(message: &Value) -> bool {
        message.get("status").and_then(Value::as_str) == Some("UP")
    }

    /// Resolves the configured normalized symbol for a native market code. The
    /// documented order-book unit suffix is stripped first.
    pub fn symbol_for_native<'a>(feed: &'a ExchangeFeed, native: &str) -> Option<&'a Symbol> {
        let native = parser::market_code(native);
        feed.symbols.iter().enumerate().find_map(|(index, symbol)| {
            let exchange_symbol = feed
                .exchange_symbols
                .get(index)
                .map(String::as_str)
                .unwrap_or_else(|| symbol.as_str());
            (exchange_symbol.eq_ignore_ascii_case(native)
                || fallback_exchange_symbol(symbol).eq_ignore_ascii_case(native))
            .then_some(symbol)
        })
    }

    #[cfg(feature = "orderbook")]
    pub fn parse_orderbook_for_feed(
        feed: &ExchangeFeed,
        message: &Value,
        received_ts: f64,
    ) -> Option<L2Book> {
        let native = parser::native_code(message)?;
        let symbol = Self::symbol_for_native(feed, native)?.clone();
        parser::parse_book_for_symbol(message, received_ts, &symbol)
    }

    pub fn parse_messages_for_feed(
        feed: &ExchangeFeed,
        message: &Value,
        received_ts: f64,
    ) -> Vec<UpbitEvent> {
        let Some(native) = parser::native_code(message) else {
            return Vec::new();
        };
        let Some(symbol) = Self::symbol_for_native(feed, native).cloned() else {
            return Vec::new();
        };
        Self::parse_messages_with_symbol(message, received_ts, &symbol)
    }

    pub fn parse_messages(message: &Value, received_ts: f64) -> Vec<UpbitEvent> {
        let Some(native) = parser::native_code(message) else {
            return Vec::new();
        };
        let Some(symbol) = parser::normalize_symbol(parser::market_code(native)) else {
            return Vec::new();
        };
        Self::parse_messages_with_symbol(message, received_ts, &symbol)
    }

    fn parse_messages_with_symbol(
        message: &Value,
        received_ts: f64,
        symbol: &Symbol,
    ) -> Vec<UpbitEvent> {
        let Some(data_type) = parser::message_type(message) else {
            return Vec::new();
        };
        match data_type {
            #[cfg(feature = "trade")]
            "trade" => parser::parse_trade_for_symbol(message, received_ts, symbol)
                .map(UpbitEvent::Trade)
                .into_iter()
                .collect(),
            // Order-book frames are routed through the snapshot book-sync layer
            // by the runtime, never through this event list.
            _ => {
                #[cfg(feature = "candles")]
                if data_type.starts_with("candle.") {
                    return parser::parse_candle_for_symbol(message, received_ts, symbol)
                        .map(UpbitEvent::Candle)
                        .into_iter()
                        .collect();
                }
                Vec::new()
            }
        }
    }
}

fn fallback_exchange_symbol(symbol: &Symbol) -> String {
    // Normalized form is `BASE-QUOTE`; the native Upbit code is `QUOTE-BASE`.
    symbol.as_str().split_once('-').map_or_else(
        || symbol.as_str().to_owned(),
        |(base, quote)| format!("{quote}-{base}"),
    )
}

#[cfg(test)]
mod tests {
    use super::UpbitAdapter;
    use crate::exchange::upbit::Upbit;
    use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
    use serde_json::Value;

    fn parsed(frames: &[String]) -> Value {
        assert_eq!(frames.len(), 1, "one documented request array");
        serde_json::from_str(&frames[0]).expect("request array")
    }

    #[test]
    fn routes_every_channel_to_the_korean_quotation_endpoint() {
        assert_eq!(
            UpbitAdapter::websocket_url(),
            "wss://api.upbit.com/websocket/v1"
        );
        let feed = Upbit::new().trade().symbol("BTC-KRW").build();
        assert_eq!(
            UpbitAdapter::subscription_url(&feed),
            "wss://api.upbit.com/websocket/v1"
        );
    }

    #[test]
    fn builds_the_documented_ticket_type_format_array() {
        let feed = Upbit::new()
            .trade()
            .l2_book()
            .candles()
            .candles_interval("1m")
            .symbol("BTC-KRW")
            .exchange_symbol("KRW-BTC")
            .symbol("ETH-BTC")
            .exchange_symbol("BTC-ETH")
            .build();
        let frames = UpbitAdapter::subscription_messages(&feed).expect("frames");
        let message = parsed(&frames);
        let entries = message.as_array().expect("array");
        assert_eq!(entries[0]["ticket"], "cryptofeed-rs");
        assert_eq!(entries[1]["type"], "trade");
        assert_eq!(
            entries[1]["codes"],
            serde_json::json!(["KRW-BTC", "BTC-ETH"])
        );
        assert_eq!(entries[2]["type"], "orderbook");
        assert_eq!(entries[3]["type"], "candle.1m");
        assert_eq!(entries[4]["format"], "DEFAULT");
    }

    #[test]
    fn documented_unit_counts_reach_the_codes_with_the_default_omitted() {
        for (depth, expected) in [
            (None, "KRW-BTC"),
            (Some(30), "KRW-BTC"),
            (Some(1), "KRW-BTC.1"),
            (Some(5), "KRW-BTC.5"),
            (Some(15), "KRW-BTC.15"),
        ] {
            let feed = {
                let feed = Upbit::new()
                    .l2_book()
                    .symbol("BTC-KRW")
                    .exchange_symbol("KRW-BTC");
                match depth {
                    Some(level) => feed.l2_book_depth(level),
                    None => feed,
                }
                .build()
            };
            let message = parsed(&UpbitAdapter::subscription_messages(&feed).expect("frames"));
            assert_eq!(message[1]["codes"][0], expected);
        }
        assert_eq!(UpbitAdapter::l2_book_depth_wire(None), Some(30));
        assert_eq!(UpbitAdapter::l2_book_depth_wire(Some(50)), None);
        assert!(!UpbitAdapter::l2_book_depth_supported(10));
    }

    #[test]
    fn candle_interval_to_type_follows_the_documented_units() {
        for (interval, data_type) in [
            ("1s", "candle.1s"),
            ("1m", "candle.1m"),
            ("3m", "candle.3m"),
            ("5m", "candle.5m"),
            ("10m", "candle.10m"),
            ("15m", "candle.15m"),
            ("30m", "candle.30m"),
            ("1h", "candle.60m"),
            ("4h", "candle.240m"),
        ] {
            assert_eq!(UpbitAdapter::candle_type(interval), Some(data_type));
        }
        assert_eq!(UpbitAdapter::candle_type("2h"), None);
        assert_eq!(UpbitAdapter::candle_type("1d"), None);
    }

    #[test]
    fn documented_error_envelopes_are_terminal_or_rate_limited() {
        let fatal = UpbitAdapter::error(&serde_json::json!({
            "error": {"name": "WRONG_FORMAT", "message": "Format is invalid."}
        }))
        .expect("error");
        assert!(matches!(
            fatal,
            cryptofeed_core::error::Error::Subscription(_)
        ));
        let limited = UpbitAdapter::error(&serde_json::json!({
            "error": {"name": "Too Many Requests", "message": "slow down"}
        }))
        .expect("error");
        assert!(matches!(
            limited,
            cryptofeed_core::error::Error::RateLimited { .. }
        ));
        assert!(UpbitAdapter::error(&serde_json::json!({"type": "trade"})).is_none());
        assert!(UpbitAdapter::is_heartbeat_status(
            &serde_json::json!({"status": "UP"})
        ));
    }

    #[test]
    fn heartbeat_is_the_documented_text_ping() {
        assert_eq!(UpbitAdapter::heartbeat_message(), "PING");
    }

    #[cfg(feature = "trade")]
    #[test]
    fn feed_context_resolves_legacy_market_alias_and_filters_unconfigured_codes() {
        let feed = Upbit::new()
            .trade()
            .symbol("BTC-KRW")
            .exchange_symbol("KRW-BTC")
            .build();
        let events = UpbitAdapter::parse_messages_for_feed(
            &feed,
            &serde_json::json!({
                "type": "trade", "market": "KRW-BTC", "trade_price": 1.0,
                "trade_volume": 2.0, "ask_bid": "BID", "trade_timestamp": 1_700_000_000_000_u64
            }),
            1.0,
        );
        assert_eq!(events.len(), 1);
        // The parser tests cover the field mapping; here the feed mapping is
        // what matters: the configured code resolves to the normalized pair.
        assert_eq!(
            UpbitAdapter::symbol_for_native(&feed, "KRW-BTC").map(Symbol::as_str),
            Some("BTC-KRW")
        );
        assert!(
            UpbitAdapter::parse_messages_for_feed(
                &feed,
                &serde_json::json!({
                    "type": "trade", "code": "KRW-ETH", "trade_price": 1.0,
                    "trade_volume": 2.0, "ask_bid": "BID"
                }),
                1.0,
            )
            .is_empty()
        );
        assert_eq!(
            UpbitAdapter::symbol_for_native(&feed, "KRW-BTC.5").map(Symbol::as_str),
            Some("BTC-KRW")
        );
        assert_eq!(ExchangeId::Upbit, feed.exchange);
    }
}
