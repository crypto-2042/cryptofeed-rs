//! Kraken Spot WebSocket v2 routing and subscription framing.
//!
//! One public endpoint serves every spot channel. Subscriptions are
//! `method`-based JSON objects and may carry several symbols per frame; each
//! symbol is acknowledged separately and echoes the client `req_id`. Liveness
//! is `{"method":"ping"}` / `{"method":"pong"}`.

use super::parser;
use crate::exchange::ExchangeFeed;
use cryptofeed_core::{
    error::{Error, Result},
    exchange::Channel,
    symbol::Symbol,
};
use serde_json::Value;

#[cfg(feature = "candles")]
use cryptofeed_candles::Candle;
#[cfg(feature = "ticker")]
use cryptofeed_ticker::Ticker;
#[cfg(feature = "trade")]
use cryptofeed_trade::Trade;

pub struct KrakenAdapter;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KrakenControl {
    Pong,
    /// Server-originated `heartbeat` channel frame.
    Heartbeat,
    /// Server-originated `status` channel frame.
    Status,
    Subscribed,
}

pub enum KrakenEvent {
    #[cfg(feature = "candles")]
    Candle(Candle),
    #[cfg(feature = "ticker")]
    Ticker(Ticker),
    #[cfg(feature = "trade")]
    Trade(Trade),
}

impl KrakenAdapter {
    /// Public spot WebSocket v2 endpoint.
    pub fn websocket_url() -> &'static str {
        "wss://ws.kraken.com/v2"
    }

    pub fn subscription_url(_feed: &ExchangeFeed) -> String {
        Self::websocket_url().to_owned()
    }

    pub fn heartbeat_message() -> &'static str {
        r#"{"method":"ping"}"#
    }

    /// Normalized candle interval to the documented Kraken OHLC interval in
    /// minutes: `1`, `5`, `15`, `30`, `60`, `240`, `1440`, `10080`, `21600`.
    /// The normalized vocabulary cannot express `21600` (15 days), so it stays
    /// unsupported instead of being relabeled.
    pub fn candle_interval_wire(interval: &str) -> Option<u16> {
        match interval {
            "1m" => Some(1),
            "5m" => Some(5),
            "15m" => Some(15),
            "30m" => Some(30),
            "1h" => Some(60),
            "4h" => Some(240),
            "1d" => Some(1440),
            "1w" => Some(10080),
            _ => None,
        }
    }

    pub fn l2_book_depth_supported(level: u16) -> bool {
        matches!(level, 10 | 25 | 100 | 500 | 1000)
    }

    /// Documented `book` depth; the venue default is 10.
    pub fn l2_book_depth_wire(depth: Option<u16>) -> Option<u16> {
        match depth {
            None => Some(10),
            Some(level) if Self::l2_book_depth_supported(level) => Some(level),
            Some(_) => None,
        }
    }

    pub fn parse_control_message(message: &Value) -> Option<Result<KrakenControl>> {
        if message.get("method").and_then(Value::as_str) == Some("pong") {
            return Some(Ok(KrakenControl::Pong));
        }
        match message.get("channel").and_then(Value::as_str) {
            Some("heartbeat") => return Some(Ok(KrakenControl::Heartbeat)),
            Some("status") => return Some(Ok(KrakenControl::Status)),
            _ => {}
        }
        let method = message.get("method").and_then(Value::as_str)?;
        if !matches!(method, "subscribe" | "unsubscribe") {
            return None;
        }
        let success = message.get("success")?.as_bool()?;
        if success {
            Some(Ok(KrakenControl::Subscribed))
        } else {
            let detail = message
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("subscription rejected");
            // A rejected subscription is a permanent configuration problem;
            // `Subscription` errors fail fast instead of burning the retries.
            Some(Err(Error::Subscription(format!(
                "kraken websocket error: {detail}"
            ))))
        }
    }

    fn wire_symbols(feed: &ExchangeFeed) -> Result<Vec<String>> {
        if feed.exchange_symbols.is_empty() {
            return Ok(feed
                .symbols
                .iter()
                .map(|symbol| symbol.as_str().replace('-', "/"))
                .collect());
        }
        if feed.exchange_symbols.len() != feed.symbols.len() {
            return Err(Error::InvalidConfiguration(
                "normalized and exchange symbol counts must match".to_owned(),
            ));
        }
        Ok(feed.exchange_symbols.clone())
    }

    /// One subscription frame per (channel, channel parameter), carrying every
    /// configured symbol of the feed.
    pub fn subscription_messages(feed: &ExchangeFeed) -> Result<Vec<String>> {
        let symbols = Self::wire_symbols(feed)?;
        let mut frames: Vec<Value> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for channel in &feed.channels {
            let (wire_channel, parameter) = match channel {
                // `bbo` triggers updates on best-bid/ask changes; the ticker
                // payload already carries both sides with sizes.
                Channel::Ticker => (
                    "ticker",
                    Some(("event_trigger", Value::String("bbo".to_owned()))),
                ),
                Channel::Trade => ("trade", None),
                Channel::L2Book => {
                    let depth = Self::l2_book_depth_wire(feed.l2_book_depth).ok_or_else(|| {
                        Error::UnsupportedCapability(format!(
                            "Kraken l2 depth level {:?}",
                            feed.l2_book_depth
                        ))
                    })?;
                    ("book", Some(("depth", Value::from(depth))))
                }
                Channel::Candles => {
                    let minutes =
                        Self::candle_interval_wire(&feed.candle_interval).ok_or_else(|| {
                            Error::UnsupportedCapability(format!(
                                "Kraken candle interval {:?}",
                                feed.candle_interval
                            ))
                        })?;
                    ("ohlc", Some(("interval", Value::from(minutes))))
                }
                // Unreachable after capability preflight.
                _ => continue,
            };
            let key = (
                wire_channel,
                parameter
                    .as_ref()
                    .map(|(field, value)| (*field, value.to_string())),
            );
            if !seen.insert(key) {
                continue;
            }
            let mut params = serde_json::json!({
                "channel": wire_channel,
                "symbol": symbols,
            });
            if let Some((field, value)) = parameter {
                params[field] = value;
            }
            if wire_channel == "book" {
                params["snapshot"] = Value::Bool(true);
            }
            frames.push(serde_json::json!({
                "method": "subscribe",
                "params": params,
                "req_id": frames.len() + 1,
            }));
        }
        Ok(frames.into_iter().map(|frame| frame.to_string()).collect())
    }

    /// In-session resnapshot for one order-book symbol: unsubscribe and
    /// subscribe the same depth so the next push is a replacement snapshot.
    pub fn book_resubscribe_messages(feed: &ExchangeFeed, symbol: &Symbol) -> Result<Vec<String>> {
        let wire_symbol = feed.exchange_symbol_for(symbol)?.to_owned();
        let depth = Self::l2_book_depth_wire(feed.l2_book_depth).ok_or_else(|| {
            Error::UnsupportedCapability(format!("Kraken l2 depth level {:?}", feed.l2_book_depth))
        })?;
        let params = |snapshot: bool| {
            let mut params = serde_json::json!({
                "channel": "book",
                "symbol": [wire_symbol],
                "depth": depth,
            });
            if snapshot {
                params["snapshot"] = Value::Bool(true);
            }
            params
        };
        Ok(vec![
            serde_json::json!({
                "method": "unsubscribe",
                "params": params(false),
                "req_id": 1,
            })
            .to_string(),
            serde_json::json!({
                "method": "subscribe",
                "params": params(true),
                "req_id": 2,
            })
            .to_string(),
        ])
    }

    /// Resolves the configured normalized symbol for a native payload pair.
    pub fn symbol_for_native<'a>(feed: &'a ExchangeFeed, native: &str) -> Option<&'a Symbol> {
        feed.symbols.iter().enumerate().find_map(|(index, symbol)| {
            let exchange_symbol = feed
                .exchange_symbols
                .get(index)
                .map(String::as_str)
                .unwrap_or_else(|| symbol.as_str());
            (exchange_symbol.eq_ignore_ascii_case(native)
                || symbol
                    .as_str()
                    .replace('-', "/")
                    .eq_ignore_ascii_case(native))
            .then_some(symbol)
        })
    }

    pub fn parse_messages_for_feed(
        feed: &ExchangeFeed,
        message: &Value,
        received_ts: f64,
    ) -> Vec<KrakenEvent> {
        Self::parse_rows(message, received_ts, |native| {
            Self::symbol_for_native(feed, native).cloned()
        })
    }

    pub fn parse_messages(message: &Value, received_ts: f64) -> Vec<KrakenEvent> {
        Self::parse_rows(message, received_ts, |native| {
            parser::normalize_symbol(native)
        })
    }

    fn parse_rows(
        message: &Value,
        received_ts: f64,
        resolve: impl Fn(&str) -> Option<Symbol>,
    ) -> Vec<KrakenEvent> {
        let Some(channel) = parser::channel(message) else {
            return Vec::new();
        };
        let mut events = Vec::new();
        for row in parser::data_rows(message) {
            let Some(native) = parser::row_symbol(row) else {
                continue;
            };
            let Some(symbol) = resolve(native) else {
                continue;
            };
            match channel {
                #[cfg(feature = "ticker")]
                "ticker" => {
                    if let Some(ticker) = parser::parse_ticker_for_symbol(row, received_ts, &symbol)
                    {
                        events.push(KrakenEvent::Ticker(ticker));
                    }
                }
                #[cfg(feature = "trade")]
                "trade" => {
                    if let Some(trade) = parser::parse_trade_for_symbol(row, received_ts, &symbol) {
                        events.push(KrakenEvent::Trade(trade));
                    }
                }
                #[cfg(feature = "candles")]
                "ohlc" => {
                    if let Some(candle) = parser::parse_candle_for_symbol(row, received_ts, &symbol)
                    {
                        events.push(KrakenEvent::Candle(candle));
                    }
                }
                _ => {}
            }
        }
        events
    }
}

#[cfg(test)]
mod tests {
    use super::{KrakenAdapter, KrakenControl};
    use crate::exchange::kraken::Kraken;
    use cryptofeed_core::symbol::Symbol;
    use serde_json::Value;

    fn frame(frames: &[String], channel: &str) -> Value {
        frames
            .iter()
            .map(|frame| serde_json::from_str::<Value>(frame).expect("frame json"))
            .find(|frame| frame["params"]["channel"] == channel)
            .unwrap_or_else(|| panic!("missing {channel} frame: {frames:?}"))
    }

    #[test]
    fn builds_kraken_v2_subscription_frames() {
        let feed = Kraken::new()
            .ticker()
            .trade()
            .l2_book()
            .candles()
            .candles_interval("5m")
            .symbol("BTC-USD")
            .exchange_symbol("BTC/USD")
            .symbol("ETH-USDT")
            .exchange_symbol("ETH/USDT")
            .build();
        let frames = KrakenAdapter::subscription_messages(&feed).expect("frames");
        assert_eq!(frames.len(), 4);
        let ticker = frame(&frames, "ticker");
        assert_eq!(ticker["method"], "subscribe");
        assert_eq!(
            ticker["params"]["symbol"],
            serde_json::json!(["BTC/USD", "ETH/USDT"])
        );
        assert_eq!(ticker["params"]["event_trigger"], "bbo");
        assert_eq!(ticker["req_id"], 1);
        assert_eq!(frame(&frames, "trade")["params"]["symbol"][0], "BTC/USD");
        assert_eq!(frame(&frames, "book")["params"]["depth"], 10);
        assert_eq!(frame(&frames, "book")["params"]["snapshot"], true);
        assert_eq!(frame(&frames, "ohlc")["params"]["interval"], 5);
    }

    #[test]
    fn routes_every_spot_channel_to_the_single_v2_endpoint() {
        assert_eq!(KrakenAdapter::websocket_url(), "wss://ws.kraken.com/v2");
        let feed = Kraken::new().ticker().symbol("BTC-USD").build();
        assert_eq!(
            KrakenAdapter::subscription_url(&feed),
            "wss://ws.kraken.com/v2"
        );
    }

    #[test]
    fn validates_documented_depths_and_intervals() {
        for depth in [10u16, 25, 100, 500, 1000] {
            assert!(KrakenAdapter::l2_book_depth_supported(depth));
            assert_eq!(KrakenAdapter::l2_book_depth_wire(Some(depth)), Some(depth));
        }
        assert!(!KrakenAdapter::l2_book_depth_supported(50));
        assert_eq!(KrakenAdapter::l2_book_depth_wire(Some(50)), None);
        assert_eq!(KrakenAdapter::l2_book_depth_wire(None), Some(10));

        assert_eq!(KrakenAdapter::candle_interval_wire("1m"), Some(1));
        assert_eq!(KrakenAdapter::candle_interval_wire("4h"), Some(240));
        assert_eq!(KrakenAdapter::candle_interval_wire("1w"), Some(10080));
        assert_eq!(KrakenAdapter::candle_interval_wire("6h"), None);
        assert_eq!(KrakenAdapter::candle_interval_wire("1M"), None);
    }

    #[test]
    fn parses_method_based_control_frames() {
        assert_eq!(
            KrakenAdapter::parse_control_message(&serde_json::json!({
                "method": "pong", "time_in": "2026-11-03T07:00:17.747666Z"
            }))
            .expect("pong")
            .expect("ok"),
            KrakenControl::Pong
        );
        assert_eq!(
            KrakenAdapter::parse_control_message(&serde_json::json!({"channel": "heartbeat"}))
                .expect("heartbeat")
                .expect("ok"),
            KrakenControl::Heartbeat
        );
        assert_eq!(
            KrakenAdapter::parse_control_message(&serde_json::json!({
                "channel": "status", "type": "update",
                "data": [{"system": "online"}]
            }))
            .expect("status")
            .expect("ok"),
            KrakenControl::Status
        );
        assert_eq!(
            KrakenAdapter::parse_control_message(&serde_json::json!({
                "method": "subscribe", "req_id": 1, "success": true,
                "result": {"channel": "ticker", "symbol": "BTC/USD"}
            }))
            .expect("ack")
            .expect("ok"),
            KrakenControl::Subscribed
        );
        assert!(
            KrakenAdapter::parse_control_message(&serde_json::json!({
                "error": "Currency pair not supported XBT/USD",
                "method": "subscribe", "success": false, "symbol": "XBT/USD"
            }))
            .expect("ack")
            .is_err()
        );
        assert!(KrakenAdapter::parse_control_message(&serde_json::json!({})).is_none());
    }

    #[test]
    fn resubscribe_pair_requests_a_replacement_snapshot() {
        let feed = Kraken::new()
            .l2_book()
            .symbol("BTC-USD")
            .exchange_symbol("BTC/USD")
            .build();
        let frames = KrakenAdapter::book_resubscribe_messages(&feed, &Symbol::spot("BTC", "USD"))
            .expect("frames");
        assert_eq!(frames.len(), 2);
        let unsubscribe: Value = serde_json::from_str(&frames[0]).expect("json");
        let subscribe: Value = serde_json::from_str(&frames[1]).expect("json");
        assert_eq!(unsubscribe["method"], "unsubscribe");
        assert_eq!(subscribe["method"], "subscribe");
        assert_eq!(
            subscribe["params"]["symbol"],
            serde_json::json!(["BTC/USD"])
        );
        assert_eq!(subscribe["params"]["depth"], 10);
        assert_eq!(subscribe["params"]["snapshot"], true);
    }

    #[cfg(feature = "ticker")]
    #[test]
    fn feed_context_filters_unconfigured_symbols_and_preserves_identity() {
        let feed = Kraken::new()
            .ticker()
            .symbol("BTC-USD")
            .exchange_symbol("BTC/USD")
            .build();
        let message = serde_json::json!({
            "channel": "ticker", "type": "snapshot",
            "data": [
                {"symbol": "BTC/USD", "bid": 76497.5, "ask": 76497.6,
                 "timestamp": "2026-11-03T07:01:22.016355Z"},
                {"symbol": "ETH/USD", "bid": 2243.84, "ask": 2243.85,
                 "timestamp": "2026-11-03T07:01:22.016355Z"}
            ]
        });
        let events = KrakenAdapter::parse_messages_for_feed(&feed, &message, 1.0);
        assert_eq!(events.len(), 1);
        match &events[0] {
            super::KrakenEvent::Ticker(ticker) => assert_eq!(ticker.symbol.as_str(), "BTC-USD"),
            _ => panic!("expected ticker"),
        }
    }
}
