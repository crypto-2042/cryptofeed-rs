//! Bithumb public WebSocket routing and subscription framing.
//!
//! One public v1 endpoint serves the implemented channels. A subscription
//! request is a single JSON array containing a ticket object, one data-type
//! object per channel and a format object; all configured symbols of a channel
//! share one object. Bithumb documents no subscription acknowledgement: a
//! rejected request is answered with a documented error envelope and otherwise
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

#[cfg(feature = "orderbook")]
use cryptofeed_orderbook::L2Book;
#[cfg(feature = "trade")]
use cryptofeed_trade::Trade;

pub struct BithumbAdapter;

pub enum BithumbEvent {
    #[cfg(feature = "trade")]
    Trade(Trade),
}

/// Documented public WebSocket v1 endpoint.
pub const WEBSOCKET_URL: &str = "wss://ws-api.bithumb.com/websocket/v1";
/// Requester identifier sent in the ticket object.
pub const TICKET: &str = "cryptofeed-rs";

impl BithumbAdapter {
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
        if feed.l2_book_depth.is_some() {
            // The documented `level` field aggregates price steps; it is not a
            // unit count, so the venue exposes no depth parameter to honor.
            return Err(Error::UnsupportedCapability(format!(
                "Bithumb l2 depth level {:?}",
                feed.l2_book_depth
            )));
        }
        let symbols = Self::native_symbols(feed)?;
        let mut message = vec![serde_json::json!({"ticket": TICKET})];
        let mut seen = std::collections::HashSet::new();
        for channel in &feed.channels {
            let data_type = match channel {
                Channel::Trade => "trade",
                Channel::L2Book => "orderbook",
                // Unreachable after capability preflight; no public WS candle
                // stream and no ticker capability exist for this venue.
                _ => continue,
            };
            if !seen.insert(data_type) {
                continue;
            }
            message.push(serde_json::json!({"type": data_type, "codes": symbols}));
        }
        message.push(serde_json::json!({"format": "DEFAULT"}));
        Ok(vec![Value::Array(message).to_string()])
    }

    /// Documented error envelope (`{"error": {"name": ..., "message": ...}}`).
    /// Every documented name is a permanent request/configuration problem; the
    /// venue documents connection-level 429 for request pacing, not a WS
    /// message error name.
    pub fn error(message: &Value) -> Option<Error> {
        let name = message.get("error")?.get("name")?.as_str()?;
        Some(Error::Subscription(format!(
            "bithumb websocket error: {name}"
        )))
    }

    /// Documented heartbeat acknowledgement, filtered before parsing.
    pub fn is_heartbeat_status(message: &Value) -> bool {
        message.get("status").and_then(Value::as_str) == Some("UP")
    }

    /// Resolves the configured normalized symbol for a native market code.
    pub fn symbol_for_native<'a>(feed: &'a ExchangeFeed, native: &str) -> Option<&'a Symbol> {
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
    ) -> Vec<BithumbEvent> {
        let Some(native) = parser::native_code(message) else {
            return Vec::new();
        };
        let Some(symbol) = Self::symbol_for_native(feed, native).cloned() else {
            return Vec::new();
        };
        Self::parse_messages_with_symbol(message, received_ts, &symbol)
    }

    pub fn parse_messages(message: &Value, received_ts: f64) -> Vec<BithumbEvent> {
        let Some(native) = parser::native_code(message) else {
            return Vec::new();
        };
        let Some(symbol) = parser::normalize_symbol(native) else {
            return Vec::new();
        };
        Self::parse_messages_with_symbol(message, received_ts, &symbol)
    }

    fn parse_messages_with_symbol(
        message: &Value,
        received_ts: f64,
        symbol: &Symbol,
    ) -> Vec<BithumbEvent> {
        let Some(data_type) = parser::message_type(message) else {
            return Vec::new();
        };
        match data_type {
            #[cfg(feature = "trade")]
            "trade" => parser::parse_trade_for_symbol(message, received_ts, symbol)
                .map(BithumbEvent::Trade)
                .into_iter()
                .collect(),
            // Order-book frames are routed through the snapshot book-sync layer
            // by the runtime, never through this event list.
            _ => Vec::new(),
        }
    }
}

fn fallback_exchange_symbol(symbol: &Symbol) -> String {
    // Normalized form is `BASE-QUOTE`; the native Bithumb code is `QUOTE-BASE`.
    symbol.as_str().split_once('-').map_or_else(
        || symbol.as_str().to_owned(),
        |(base, quote)| format!("{quote}-{base}"),
    )
}

#[cfg(test)]
mod tests {
    use super::BithumbAdapter;
    use crate::exchange::bithumb::Bithumb;
    use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
    use serde_json::Value;

    fn parsed(frames: &[String]) -> Value {
        assert_eq!(frames.len(), 1, "one documented request array");
        serde_json::from_str(&frames[0]).expect("request array")
    }

    #[test]
    fn routes_every_channel_to_the_public_v1_endpoint() {
        assert_eq!(
            BithumbAdapter::websocket_url(),
            "wss://ws-api.bithumb.com/websocket/v1"
        );
        let feed = Bithumb::new().trade().symbol("BTC-KRW").build();
        assert_eq!(
            BithumbAdapter::subscription_url(&feed),
            "wss://ws-api.bithumb.com/websocket/v1"
        );
    }

    #[test]
    fn builds_the_documented_ticket_type_format_array() {
        let feed = Bithumb::new()
            .trade()
            .l2_book()
            .symbol("BTC-KRW")
            .exchange_symbol("KRW-BTC")
            .symbol("ETH-BTC")
            .exchange_symbol("BTC-ETH")
            .build();
        let message = parsed(&BithumbAdapter::subscription_messages(&feed).expect("frames"));
        let entries = message.as_array().expect("array");
        assert_eq!(entries[0]["ticket"], "cryptofeed-rs");
        assert_eq!(entries[1]["type"], "trade");
        assert_eq!(
            entries[1]["codes"],
            serde_json::json!(["KRW-BTC", "BTC-ETH"])
        );
        assert_eq!(entries[2]["type"], "orderbook");
        assert_eq!(entries[3]["format"], "DEFAULT");
    }

    #[test]
    fn depth_is_rejected_instead_of_being_silently_ignored() {
        let feed = Bithumb::new()
            .l2_book()
            .l2_book_depth(5)
            .symbol("BTC-KRW")
            .exchange_symbol("KRW-BTC")
            .build();
        assert!(matches!(
            BithumbAdapter::subscription_messages(&feed),
            Err(cryptofeed_core::error::Error::UnsupportedCapability(_))
        ));
    }

    #[test]
    fn documented_error_envelopes_are_terminal() {
        let error = BithumbAdapter::error(&serde_json::json!({
            "error": {"name": "NO_CODES", "message": "codes field is missing."}
        }))
        .expect("error");
        assert!(matches!(
            error,
            cryptofeed_core::error::Error::Subscription(_)
        ));
        assert!(BithumbAdapter::error(&serde_json::json!({"type": "trade"})).is_none());
        assert!(BithumbAdapter::is_heartbeat_status(
            &serde_json::json!({"status": "UP"})
        ));
    }

    #[test]
    fn heartbeat_is_the_documented_text_ping() {
        assert_eq!(BithumbAdapter::heartbeat_message(), "PING");
    }

    #[cfg(feature = "trade")]
    #[test]
    fn feed_context_filters_unconfigured_codes() {
        let feed = Bithumb::new()
            .trade()
            .symbol("BTC-KRW")
            .exchange_symbol("KRW-BTC")
            .build();
        let events = BithumbAdapter::parse_messages_for_feed(
            &feed,
            &serde_json::json!({
                "type": "trade", "code": "KRW-BTC", "trade_price": 1.0,
                "trade_volume": 2.0, "ask_bid": "BID", "trade_timestamp": 1_700_000_000_000_u64
            }),
            1.0,
        );
        assert_eq!(events.len(), 1);
        // The parser tests cover the field mapping; here the feed mapping is
        // what matters: the configured code resolves to the normalized pair.
        assert_eq!(
            BithumbAdapter::symbol_for_native(&feed, "KRW-BTC").map(Symbol::as_str),
            Some("BTC-KRW")
        );
        assert!(
            BithumbAdapter::parse_messages_for_feed(
                &feed,
                &serde_json::json!({
                    "type": "trade", "code": "KRW-ETH", "trade_price": 1.0,
                    "trade_volume": 2.0, "ask_bid": "BID"
                }),
                1.0,
            )
            .is_empty()
        );
        assert_eq!(ExchangeId::Bithumb, feed.exchange);
    }
}
