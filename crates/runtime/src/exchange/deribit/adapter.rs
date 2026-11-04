//! Deribit JSON-RPC v2 public routing and subscription framing.
//!
//! One public JSON-RPC endpoint serves every channel. Subscriptions are a single
//! `public/subscribe` call carrying the documented channel names; the reply is a
//! JSON-RPC result array and errors are JSON-RPC error objects. Heartbeats are
//! enabled with `public/set_heartbeat` and kept alive by periodic `public/test`
//! calls. Normalized scope is perpetuals and dated futures; options are never
//! subscribed.

use cryptofeed_core::{
    error::{Error, Result},
    exchange::Channel,
    symbol::{InstrumentKind, Symbol},
};
use serde_json::Value;

use super::parser;
use crate::exchange::ExchangeFeed;

#[cfg(feature = "candles")]
use cryptofeed_candles::Candle;
#[cfg(feature = "funding")]
use cryptofeed_funding::Funding;
#[cfg(feature = "index")]
use cryptofeed_index::IndexPrice;
#[cfg(feature = "markprice")]
use cryptofeed_markprice::MarkPrice;
#[cfg(feature = "openinterest")]
use cryptofeed_openinterest::OpenInterest;
#[cfg(feature = "ticker")]
use cryptofeed_ticker::Ticker;
#[cfg(feature = "trade")]
use cryptofeed_trade::Trade;

pub struct DeribitAdapter;

/// Public `book`/`trades`/`ticker` interval. `raw` is documented as
/// authenticated-only, so the public feed always uses the finest public
/// aggregation (`100ms`).
pub const PUBLIC_INTERVAL: &str = "100ms";

/// `public/set_heartbeat` interval in seconds (documented range 10..=300).
pub const HEARTBEAT_INTERVAL_SECS: u64 = 30;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeribitControl {
    /// JSON-RPC success response for a subscribe/unsubscribe/heartbeat call.
    Subscribed,
    /// Documented idle `heartbeat` notification.
    Heartbeat,
    /// Documented `test_request`; the caller must answer `public/test`.
    TestRequest,
}

pub enum DeribitEvent {
    #[cfg(feature = "candles")]
    Candle(Candle),
    #[cfg(feature = "funding")]
    Funding(Funding),
    #[cfg(feature = "index")]
    IndexPrice(IndexPrice),
    #[cfg(feature = "markprice")]
    MarkPrice(MarkPrice),
    #[cfg(feature = "openinterest")]
    OpenInterest(OpenInterest),
    #[cfg(feature = "ticker")]
    Ticker(Ticker),
    #[cfg(feature = "trade")]
    Trade(Trade),
}

impl DeribitAdapter {
    /// Public production JSON-RPC WebSocket.
    pub fn websocket_url() -> &'static str {
        "wss://www.deribit.com/ws/api/v2"
    }

    pub fn subscription_url(_feed: &ExchangeFeed) -> String {
        Self::websocket_url().to_owned()
    }

    /// JSON-RPC `public/test` liveness call. Any client text counts as activity
    /// for the documented heartbeat; the JSON-RPC `{"result":"ok"}` reply is
    /// filtered before market parsing.
    pub fn heartbeat_message() -> &'static str {
        r#"{"jsonrpc":"2.0","id":"cf-heartbeat","method":"public/test"}"#
    }

    /// Normalized candle interval -> documented `chart.trades` resolution.
    pub fn candle_interval_wire(interval: &str) -> Option<&'static str> {
        match interval {
            "1m" => Some("1"),
            "3m" => Some("3"),
            "5m" => Some("5"),
            "10m" => Some("10"),
            "15m" => Some("15"),
            "30m" => Some("30"),
            "1h" => Some("60"),
            "2h" => Some("120"),
            "3h" => Some("180"),
            "6h" => Some("360"),
            "12h" => Some("720"),
            "1d" => Some("1D"),
            _ => None,
        }
    }

    /// The full `book.{instrument}.{interval}` channel has no depth parameter;
    /// a requested depth is rejected at preflight rather than silently using a
    /// different grouped channel.
    pub fn l2_book_depth_supported(_level: u16) -> bool {
        false
    }

    /// Channels a normalized product may subscribe; used by the capability
    /// matrix tests and as the adapter's product gate.
    pub fn channels_for_product(product: InstrumentKind) -> &'static [Channel] {
        match product {
            InstrumentKind::Perpetual => &[
                Channel::Ticker,
                Channel::Trade,
                Channel::L2Book,
                Channel::Candles,
                Channel::Funding,
                Channel::MarkPrice,
                Channel::Index,
                Channel::OpenInterest,
            ],
            InstrumentKind::Futures => &[
                Channel::Ticker,
                Channel::Trade,
                Channel::L2Book,
                Channel::Candles,
                Channel::MarkPrice,
                Channel::Index,
                Channel::OpenInterest,
            ],
            _ => &[],
        }
    }

    fn wire_symbols(feed: &ExchangeFeed) -> Result<Vec<String>> {
        if feed.exchange_symbols.is_empty() {
            return Err(Error::InvalidConfiguration(
                "Deribit feeds require resolved native instrument names".to_owned(),
            ));
        }
        if feed.exchange_symbols.len() != feed.symbols.len() {
            return Err(Error::InvalidConfiguration(
                "normalized and exchange symbol counts must match".to_owned(),
            ));
        }
        Ok(feed.exchange_symbols.clone())
    }

    /// One `public/subscribe` call with every documented channel, preceded by
    /// the heartbeat setup call.
    pub fn subscription_messages(feed: &ExchangeFeed) -> Result<Vec<String>> {
        let symbols = Self::wire_symbols(feed)?;
        let mut channels: Vec<String> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for channel in &feed.channels {
            for symbol in &symbols {
                let wire = match channel {
                    Channel::Ticker
                    | Channel::MarkPrice
                    | Channel::Index
                    | Channel::OpenInterest
                    | Channel::Funding => format!("ticker.{symbol}.{PUBLIC_INTERVAL}"),
                    Channel::Trade => format!("trades.{symbol}.{PUBLIC_INTERVAL}"),
                    Channel::L2Book => format!("book.{symbol}.{PUBLIC_INTERVAL}"),
                    Channel::Candles => {
                        let resolution = Self::candle_interval_wire(&feed.candle_interval)
                            .ok_or_else(|| {
                                Error::UnsupportedCapability(format!(
                                    "Deribit candle interval {:?}",
                                    feed.candle_interval
                                ))
                            })?;
                        format!("chart.trades.{symbol}.{resolution}")
                    }
                    // Unreachable after capability preflight.
                    _ => continue,
                };
                if seen.insert(wire.clone()) {
                    channels.push(wire);
                }
            }
        }
        let mut frames = Vec::with_capacity(2);
        frames.push(
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "public/set_heartbeat",
                "params": {"interval": HEARTBEAT_INTERVAL_SECS},
            })
            .to_string(),
        );
        frames.push(
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "public/subscribe",
                "params": {"channels": channels},
            })
            .to_string(),
        );
        Ok(frames)
    }

    /// Replacement-snapshot request for one book channel: unsubscribe then
    /// subscribe the same documented full-book channel.
    pub fn book_resubscribe_messages(feed: &ExchangeFeed, symbol: &Symbol) -> Result<Vec<String>> {
        let native = feed.exchange_symbol_for(symbol)?.to_owned();
        let channel = format!("book.{native}.{PUBLIC_INTERVAL}");
        Ok(vec![
            serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": "public/unsubscribe",
                "params": {"channels": [channel]},
            })
            .to_string(),
            serde_json::json!({
                "jsonrpc": "2.0", "id": 2, "method": "public/subscribe",
                "params": {"channels": [channel]},
            })
            .to_string(),
        ])
    }

    /// Classifies JSON-RPC control frames and documented heartbeat
    /// notifications before market parsing.
    pub fn parse_control_message(message: &Value) -> Option<Result<DeribitControl>> {
        if let Some(error) = message.get("error") {
            let detail = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("json-rpc error");
            let code = error.get("code").and_then(Value::as_i64);
            return Some(Err(Error::Subscription(format!(
                "deribit json-rpc error code={code:?}: {detail}"
            ))));
        }
        match parser::heartbeat_type(message) {
            Some("heartbeat") => return Some(Ok(DeribitControl::Heartbeat)),
            Some("test_request") => return Some(Ok(DeribitControl::TestRequest)),
            _ => {}
        }
        if message.get("id").is_some() && message.get("result").is_some() {
            return Some(Ok(DeribitControl::Subscribed));
        }
        None
    }

    /// Resolves a configured normalized symbol for a native instrument name.
    pub fn symbol_for_native<'a>(feed: &'a ExchangeFeed, native: &str) -> Option<&'a Symbol> {
        feed.symbols.iter().enumerate().find_map(|(index, symbol)| {
            let exchange_symbol = feed
                .exchange_symbols
                .get(index)
                .map(String::as_str)
                .unwrap_or_else(|| symbol.as_str());
            exchange_symbol
                .eq_ignore_ascii_case(native)
                .then_some(symbol)
        })
    }

    pub fn parse_messages_for_feed(
        feed: &ExchangeFeed,
        message: &Value,
        received_ts: f64,
    ) -> Vec<DeribitEvent> {
        let Some(channel) = parser::channel(message) else {
            return Vec::new();
        };
        let Some(data) = parser::data(message) else {
            return Vec::new();
        };
        // Never emit events for an instrument this feed did not configure.
        let Some(native) = parser::channel_instrument(channel) else {
            return Vec::new();
        };
        let Some(symbol) = Self::symbol_for_native(feed, native) else {
            return Vec::new();
        };
        let mut events = Vec::new();
        match parser::channel_family(channel) {
            "ticker" => {
                #[cfg(feature = "ticker")]
                if let Some(ticker) = parser::parse_ticker_for_symbol(data, received_ts, symbol) {
                    events.push(DeribitEvent::Ticker(ticker));
                }
                #[cfg(feature = "markprice")]
                if let Some(mark) = parser::parse_mark_price_for_symbol(data, received_ts, symbol) {
                    events.push(DeribitEvent::MarkPrice(mark));
                }
                #[cfg(feature = "index")]
                if let Some(index) = parser::parse_index_price_for_symbol(data, received_ts, symbol)
                {
                    events.push(DeribitEvent::IndexPrice(index));
                }
                #[cfg(feature = "openinterest")]
                if let Some(open_interest) =
                    parser::parse_open_interest_for_symbol(data, received_ts, symbol)
                {
                    events.push(DeribitEvent::OpenInterest(open_interest));
                }
                #[cfg(feature = "funding")]
                if data.get("current_funding").is_some() {
                    if let Some(funding) =
                        parser::parse_funding_for_symbol(data, received_ts, symbol)
                    {
                        events.push(DeribitEvent::Funding(funding));
                    }
                }
            }
            "trades" => {
                #[cfg(feature = "trade")]
                {
                    let rows: Vec<&Value> = match data {
                        Value::Array(rows) => rows.iter().collect(),
                        other => vec![other],
                    };
                    for row in rows {
                        if let Some(trade) =
                            parser::parse_trade_for_symbol(row, received_ts, symbol)
                        {
                            events.push(DeribitEvent::Trade(trade));
                        }
                    }
                }
            }
            "chart" => {
                #[cfg(feature = "candles")]
                if let Some(candle) = parser::parse_candle_for_symbol(
                    data,
                    &feed.candle_interval,
                    received_ts,
                    symbol,
                ) {
                    events.push(DeribitEvent::Candle(candle));
                }
            }
            _ => {}
        }
        events
    }
}

#[cfg(test)]
mod tests {
    use super::{DeribitAdapter, DeribitControl};
    use crate::exchange::deribit::Deribit;
    use serde_json::Value;

    fn frames(feed: &crate::exchange::ExchangeFeed) -> Vec<Value> {
        DeribitAdapter::subscription_messages(feed)
            .expect("frames")
            .iter()
            .map(|frame| serde_json::from_str(frame).expect("frame json"))
            .collect()
    }

    #[test]
    fn builds_heartbeat_setup_and_one_json_rpc_subscribe_call() {
        let feed = Deribit::new()
            .ticker()
            .trade()
            .l2_book()
            .candles()
            .candles_interval("5m")
            .funding()
            .mark_price()
            .index()
            .open_interest()
            .symbol("BTC-USD-PERP")
            .exchange_symbol("BTC-PERPETUAL")
            .build();
        let frames = frames(&feed);
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0]["method"], "public/set_heartbeat");
        assert_eq!(frames[0]["params"]["interval"], 30);
        assert_eq!(frames[1]["method"], "public/subscribe");
        let channels = frames[1]["params"]["channels"]
            .as_array()
            .expect("channels")
            .iter()
            .map(|channel| channel.as_str().expect("channel").to_owned())
            .collect::<Vec<_>>();
        assert!(channels.contains(&"ticker.BTC-PERPETUAL.100ms".to_owned()));
        assert!(channels.contains(&"trades.BTC-PERPETUAL.100ms".to_owned()));
        assert!(channels.contains(&"book.BTC-PERPETUAL.100ms".to_owned()));
        assert!(channels.contains(&"chart.trades.BTC-PERPETUAL.5".to_owned()));
        // The five ticker-derived categories share one native channel.
        assert_eq!(
            channels
                .iter()
                .filter(|channel| channel.starts_with("ticker."))
                .count(),
            1
        );
    }

    #[test]
    fn dated_futures_omit_funding_and_require_a_resolved_catalog_symbol() {
        let feed = Deribit::new()
            .ticker()
            .symbol("BTC-USD-261225")
            .exchange_symbol("BTC-25DEC26")
            .build();
        let frames = frames(&feed);
        assert!(
            !frames[1]["params"]["channels"][0]
                .as_str()
                .unwrap()
                .contains("funding")
        );
        let unresolved = Deribit::new().ticker().symbol("BTC-USD-PERP").build();
        assert!(DeribitAdapter::subscription_messages(&unresolved).is_err());
    }

    #[test]
    fn classifies_json_rpc_results_errors_and_heartbeat_notifications() {
        assert_eq!(
            DeribitAdapter::parse_control_message(&serde_json::json!({
                "jsonrpc":"2.0","id":1,"result":["ticker.BTC-PERPETUAL.100ms"]
            }))
            .expect("result")
            .expect("ok"),
            DeribitControl::Subscribed
        );
        assert_eq!(
            DeribitAdapter::parse_control_message(&serde_json::json!({
                "jsonrpc":"2.0","method":"subscription","params":{"type":"heartbeat"}
            }))
            .expect("heartbeat")
            .expect("ok"),
            DeribitControl::Heartbeat
        );
        assert_eq!(
            DeribitAdapter::parse_control_message(&serde_json::json!({
                "jsonrpc":"2.0","method":"subscription","params":{"type":"test_request"}
            }))
            .expect("test")
            .expect("ok"),
            DeribitControl::TestRequest
        );
        let error = DeribitAdapter::parse_control_message(&serde_json::json!({
            "jsonrpc":"2.0","id":2,"error":{"code":-32602,"message":"invalid params"}
        }))
        .expect("error");
        assert!(error.is_err());
        assert!(
            DeribitAdapter::parse_control_message(&serde_json::json!({
                "jsonrpc":"2.0","method":"subscription","params":{
                    "channel":"ticker.BTC-PERPETUAL.100ms","data":{}
                }
            }))
            .is_none()
        );
    }

    #[test]
    fn resnapshot_pair_unsubscribes_and_resubscribes_the_full_book_channel() {
        let feed = Deribit::new()
            .l2_book()
            .symbol("BTC-USD-PERP")
            .exchange_symbol("BTC-PERPETUAL")
            .build();
        let symbol = cryptofeed_core::symbol::Symbol::perpetual("BTC", "USD");
        let messages = DeribitAdapter::book_resubscribe_messages(&feed, &symbol).expect("messages");
        let first: Value = serde_json::from_str(&messages[0]).expect("json");
        let second: Value = serde_json::from_str(&messages[1]).expect("json");
        assert_eq!(first["method"], "public/unsubscribe");
        assert_eq!(second["method"], "public/subscribe");
        assert_eq!(second["params"]["channels"][0], "book.BTC-PERPETUAL.100ms");
    }

    #[test]
    fn documented_resolutions_map_to_normalized_intervals() {
        assert_eq!(DeribitAdapter::candle_interval_wire("1m"), Some("1"));
        assert_eq!(DeribitAdapter::candle_interval_wire("1h"), Some("60"));
        assert_eq!(DeribitAdapter::candle_interval_wire("12h"), Some("720"));
        assert_eq!(DeribitAdapter::candle_interval_wire("1d"), Some("1D"));
        assert_eq!(DeribitAdapter::candle_interval_wire("1w"), None);
        assert_eq!(DeribitAdapter::candle_interval_wire("4h"), None);
    }
}
