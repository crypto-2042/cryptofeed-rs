//! Coinbase Advanced Trade public WebSocket routing and subscription framing.
//!
//! One public endpoint serves every market-data channel. Each subscription
//! frame carries exactly one channel and its product ids; `heartbeats` is
//! product-less. Acknowledgements arrive as a cumulative `subscriptions` map
//! and errors as `{"type":"error","message":"..."}`; the adapter treats every
//! error frame as a permanent subscription failure.

use cryptofeed_core::{
    error::{Error, Result},
    exchange::Channel,
    symbol::Symbol,
};
use serde_json::Value;

use super::parser;
use crate::exchange::ExchangeFeed;

#[cfg(feature = "candles")]
use cryptofeed_candles::Candle;
#[cfg(feature = "orderbook")]
use cryptofeed_orderbook::L1Book;
#[cfg(feature = "ticker")]
use cryptofeed_ticker::Ticker;
#[cfg(feature = "trade")]
use cryptofeed_trade::Trade;

pub struct CoinbaseAdapter;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoinbaseControl {
    /// `heartbeats` channel push; liveness only.
    Heartbeat,
    /// Cumulative `subscriptions` acknowledgement.
    Subscribed,
}

pub enum CoinbaseEvent {
    #[cfg(feature = "candles")]
    Candle(Candle),
    #[cfg(feature = "orderbook")]
    L1Book(L1Book),
    #[cfg(feature = "ticker")]
    Ticker(Ticker),
    #[cfg(feature = "trade")]
    Trade(Trade),
}

/// Intra-frame `market_trades` order: exact exchange time ascending, then
/// numeric trade id, then symbol. Only compares trades from one received frame.
#[cfg(feature = "trade")]
fn trade_order(left: &Trade, right: &Trade) -> std::cmp::Ordering {
    left.exchange_ts
        .total_cmp(&right.exchange_ts)
        .then_with(|| trade_id_order(left.id.as_deref(), right.id.as_deref()))
        .then_with(|| left.symbol.as_str().cmp(right.symbol.as_str()))
}

#[cfg(feature = "trade")]
fn trade_id_order(left: Option<&str>, right: Option<&str>) -> std::cmp::Ordering {
    match (
        left.and_then(|id| id.parse::<u128>().ok()),
        right.and_then(|id| id.parse::<u128>().ok()),
    ) {
        (Some(left), Some(right)) => left.cmp(&right),
        _ => left.unwrap_or("").cmp(right.unwrap_or("")),
    }
}

impl CoinbaseAdapter {
    /// Public Advanced Trade market-data WebSocket (no JWT).
    pub fn websocket_url() -> &'static str {
        "wss://advanced-trade-ws.coinbase.com"
    }

    pub fn subscription_url(_feed: &ExchangeFeed) -> String {
        Self::websocket_url().to_owned()
    }

    /// The public `candles` channel only transmits five-minute buckets. The
    /// normalized interval vocabulary is unchanged: `5m` is the only accepted
    /// value and `1m`/everything else fails preflight instead of being
    /// silently substituted.
    pub fn candle_interval_wire(interval: &str) -> Option<&'static str> {
        match interval {
            "5m" => Some("5m"),
            _ => None,
        }
    }

    /// The documented `level2` channel has no depth parameter; it always sends
    /// the full book. Any requested depth level is rejected at preflight.
    pub fn l2_book_depth_supported(_level: u16) -> bool {
        false
    }

    fn wire_symbols(feed: &ExchangeFeed) -> Result<Vec<String>> {
        if feed.exchange_symbols.is_empty() {
            return Ok(feed
                .symbols
                .iter()
                .map(|symbol| symbol.as_str().to_owned())
                .collect());
        }
        if feed.exchange_symbols.len() != feed.symbols.len() {
            return Err(Error::InvalidConfiguration(
                "normalized and exchange symbol counts must match".to_owned(),
            ));
        }
        Ok(feed.exchange_symbols.clone())
    }

    /// One subscription frame per channel plus the product-less `heartbeats`
    /// frame. Coinbase accepts only one channel per message.
    pub fn subscription_messages(feed: &ExchangeFeed) -> Result<Vec<String>> {
        let symbols = Self::wire_symbols(feed)?;
        let mut frames = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for channel in &feed.channels {
            let wire = match channel {
                Channel::Ticker => "ticker",
                Channel::L1Book => "ticker",
                Channel::Trade => "market_trades",
                Channel::L2Book => "level2",
                Channel::Candles => {
                    // Preflight rejects unsupported intervals; this is the
                    // defensive adapter-side check.
                    Self::candle_interval_wire(&feed.candle_interval).ok_or_else(|| {
                        Error::UnsupportedCapability(format!(
                            "Coinbase candle interval {:?}",
                            feed.candle_interval
                        ))
                    })?;
                    "candles"
                }
                // Unreachable after capability preflight.
                _ => continue,
            };
            if !seen.insert(wire) {
                continue;
            }
            frames.push(
                serde_json::json!({
                    "type": "subscribe",
                    "channel": wire,
                    "product_ids": symbols,
                })
                .to_string(),
            );
        }
        frames.push(
            serde_json::json!({
                "type": "subscribe",
                "channel": "heartbeats",
            })
            .to_string(),
        );
        Ok(frames)
    }

    /// Classifies control frames before market parsing.
    pub fn parse_control_message(message: &Value) -> Option<Result<CoinbaseControl>> {
        if message.get("type").and_then(Value::as_str) == Some("error") {
            let detail = message
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("subscription rejected");
            // Public-channel errors are free-form and permanent: the venue
            // will not fix an unknown channel/product by retrying.
            return Some(Err(Error::Subscription(format!(
                "coinbase websocket error: {detail}"
            ))));
        }
        match parser::channel(message) {
            Some("heartbeats") => Some(Ok(CoinbaseControl::Heartbeat)),
            Some("subscriptions") => Some(Ok(CoinbaseControl::Subscribed)),
            _ => None,
        }
    }

    /// Resolves the configured normalized symbol for a native product id.
    pub fn symbol_for_native<'a>(feed: &'a ExchangeFeed, native: &str) -> Option<&'a Symbol> {
        feed.symbols.iter().enumerate().find_map(|(index, symbol)| {
            let exchange_symbol = feed
                .exchange_symbols
                .get(index)
                .map(String::as_str)
                .unwrap_or_else(|| symbol.as_str());
            (exchange_symbol.eq_ignore_ascii_case(native)
                || symbol.as_str().eq_ignore_ascii_case(native))
            .then_some(symbol)
        })
    }

    pub fn parse_messages_for_feed(
        feed: &ExchangeFeed,
        message: &Value,
        received_ts: f64,
    ) -> Vec<CoinbaseEvent> {
        let exchange_ts = parser::message_timestamp(message, received_ts);
        let Some(channel) = parser::channel(message) else {
            return Vec::new();
        };
        // Coinbase documents ~250 ms aggregation but does not promise that a
        // `market_trades` batch is time-ordered; live frames contain reversed
        // pairs. Normalize only the fully received frame: emit every trade
        // sorted by exact exchange time, with numeric trade id (then symbol)
        // as the deterministic tie-break for equal timestamps. Nothing is
        // sorted across frames, so genuinely late trades keep their arrival
        // order and nothing is dropped.
        if channel == "market_trades" {
            #[cfg(feature = "trade")]
            {
                let mut trades = Vec::new();
                for event in parser::event_rows(message) {
                    for row in event
                        .get("trades")
                        .and_then(Value::as_array)
                        .map(Vec::as_slice)
                        .unwrap_or_default()
                    {
                        let Some(native) = row.get("product_id").and_then(Value::as_str) else {
                            continue;
                        };
                        let Some(symbol) = Self::symbol_for_native(feed, native) else {
                            continue;
                        };
                        if let Some(trade) =
                            parser::parse_trade_for_symbol(row, received_ts, symbol)
                        {
                            trades.push(trade);
                        }
                    }
                }
                trades.sort_by(trade_order);
                return trades.into_iter().map(CoinbaseEvent::Trade).collect();
            }
            #[cfg(not(feature = "trade"))]
            return Vec::new();
        }
        let mut events = Vec::new();
        for event in parser::event_rows(message) {
            match channel {
                "ticker" => {
                    for row in event
                        .get("tickers")
                        .and_then(Value::as_array)
                        .map(Vec::as_slice)
                        .unwrap_or_default()
                    {
                        let Some(native) = row.get("product_id").and_then(Value::as_str) else {
                            continue;
                        };
                        let Some(symbol) = Self::symbol_for_native(feed, native) else {
                            continue;
                        };
                        #[cfg(feature = "ticker")]
                        if let Some(ticker) =
                            parser::parse_ticker_for_symbol(row, received_ts, symbol)
                        {
                            events.push(CoinbaseEvent::Ticker(ticker));
                        }
                        #[cfg(feature = "orderbook")]
                        if let Some(book) =
                            parser::parse_l1_book_for_symbol(row, exchange_ts, received_ts, symbol)
                        {
                            events.push(CoinbaseEvent::L1Book(book));
                        }
                    }
                }
                "candles" => {
                    let interval = &feed.candle_interval;
                    if Self::candle_interval_wire(interval).is_none() {
                        continue;
                    }
                    for row in event
                        .get("candles")
                        .and_then(Value::as_array)
                        .map(Vec::as_slice)
                        .unwrap_or_default()
                    {
                        let Some(native) = row.get("product_id").and_then(Value::as_str) else {
                            continue;
                        };
                        let Some(symbol) = Self::symbol_for_native(feed, native) else {
                            continue;
                        };
                        #[cfg(feature = "candles")]
                        if let Some(candle) = parser::parse_candle_for_symbol(
                            row,
                            interval,
                            exchange_ts,
                            received_ts,
                            symbol,
                        ) {
                            events.push(CoinbaseEvent::Candle(candle));
                        }
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
    use super::{CoinbaseAdapter, CoinbaseControl};
    use crate::exchange::coinbase::Coinbase;
    use serde_json::Value;

    fn frame(frames: &[String], channel: &str) -> Value {
        frames
            .iter()
            .map(|frame| serde_json::from_str::<Value>(frame).expect("frame json"))
            .find(|frame| frame["channel"] == channel)
            .unwrap_or_else(|| panic!("missing {channel} frame: {frames:?}"))
    }

    #[test]
    fn builds_one_channel_per_frame_plus_heartbeats() {
        let feed = Coinbase::new()
            .ticker()
            .trade()
            .l2_book()
            .candles()
            .candles_interval("5m")
            .symbol("BTC-USD")
            .symbol("ETH-USD")
            .build();
        let frames = CoinbaseAdapter::subscription_messages(&feed).expect("frames");
        assert_eq!(frames.len(), 5);
        for (channel, expected) in [
            ("ticker", 2usize),
            ("market_trades", 2),
            ("level2", 2),
            ("candles", 2),
        ] {
            let frame = frame(&frames, channel);
            assert_eq!(frame["type"], "subscribe");
            assert_eq!(
                frame["product_ids"].as_array().map(Vec::len),
                Some(expected)
            );
        }
        let heartbeats = frame(&frames, "heartbeats");
        assert!(heartbeats.get("product_ids").is_none());
    }

    #[test]
    fn merges_ticker_and_l1_book_into_one_ticker_subscription() {
        let feed = Coinbase::new().ticker().l1_book().symbol("BTC-USD").build();
        let frames = CoinbaseAdapter::subscription_messages(&feed).expect("frames");
        assert_eq!(frames.len(), 2);
        assert_eq!(frame(&frames, "ticker")["product_ids"][0], "BTC-USD");
    }

    #[test]
    fn routes_every_channel_to_the_single_public_endpoint() {
        assert_eq!(
            CoinbaseAdapter::websocket_url(),
            "wss://advanced-trade-ws.coinbase.com"
        );
        let feed = Coinbase::new().ticker().symbol("BTC-USD").build();
        assert_eq!(
            CoinbaseAdapter::subscription_url(&feed),
            "wss://advanced-trade-ws.coinbase.com"
        );
    }

    #[test]
    fn validates_five_minute_only_candles() {
        assert_eq!(CoinbaseAdapter::candle_interval_wire("5m"), Some("5m"));
        assert_eq!(CoinbaseAdapter::candle_interval_wire("1m"), None);
        assert_eq!(CoinbaseAdapter::candle_interval_wire("15m"), None);
        assert!(!CoinbaseAdapter::l2_book_depth_supported(10));
        assert!(!CoinbaseAdapter::l2_book_depth_supported(50));
    }

    #[cfg(feature = "trade")]
    fn trade_frame(event_type: &str, trades: Value, sequence: u64) -> Value {
        serde_json::json!({
            "channel": "market_trades",
            "timestamp": "2026-11-06T07:08:17.247217733Z",
            "sequence_num": sequence,
            "events": [{"type": event_type, "trades": trades}],
        })
    }

    #[cfg(feature = "trade")]
    fn trades(events: &[super::CoinbaseEvent]) -> Vec<(String, f64, Option<String>)> {
        events
            .iter()
            .filter_map(|event| match event {
                super::CoinbaseEvent::Trade(trade) => Some((
                    trade.symbol.as_str().to_owned(),
                    trade.exchange_ts,
                    trade.id.clone(),
                )),
                _ => None,
            })
            .collect()
    }

    #[cfg(feature = "trade")]
    fn feed() -> crate::exchange::ExchangeFeed {
        Coinbase::new()
            .trade()
            .symbol("BTC-USD")
            .symbol("ETH-USD")
            .build()
    }

    #[cfg(feature = "trade")]
    #[test]
    fn reversed_update_batch_is_emitted_in_time_order() {
        let message = trade_frame(
            "update",
            serde_json::json!([
                {"product_id":"BTC-USD","trade_id":"1116181183","price":"75353.38",
                 "size":"0.00094449","time":"2026-11-06T07:08:17.218455Z","side":"SELL"},
                {"product_id":"BTC-USD","trade_id":"1116181182","price":"75352.4",
                 "size":"0.00000007","time":"2026-11-06T07:08:17.185577Z","side":"BUY"}
            ]),
            16,
        );
        let rows = CoinbaseAdapter::parse_messages_for_feed(&feed(), &message, 1.0);
        let rows = trades(&rows);
        assert_eq!(rows.len(), 2);
        assert!(rows[0].1 < rows[1].1);
        assert_eq!(rows[0].2.as_deref(), Some("1116181182"));
        assert_eq!(rows[1].2.as_deref(), Some("1116181183"));
    }

    #[cfg(feature = "trade")]
    #[test]
    fn reversed_snapshot_batch_is_emitted_in_time_order() {
        let message = trade_frame(
            "snapshot",
            serde_json::json!([
                {"product_id":"BTC-USD","trade_id":"10","price":"2","size":"1",
                 "time":"2026-11-06T07:08:18.000000Z","side":"SELL"},
                {"product_id":"BTC-USD","trade_id":"9","price":"1","size":"1",
                 "time":"2026-11-06T07:08:17.000000Z","side":"BUY"}
            ]),
            17,
        );
        let rows = CoinbaseAdapter::parse_messages_for_feed(&feed(), &message, 1.0);
        assert_eq!(rows.len(), 2);
        let rows = trades(&rows);
        assert!(rows[0].1 < rows[1].1);
        assert_eq!(rows[0].2.as_deref(), Some("9"));
    }

    #[cfg(feature = "trade")]
    #[test]
    fn same_microsecond_and_exact_time_ties_use_numeric_trade_id() {
        // Different microseconds inside the same millisecond.
        let message = trade_frame(
            "update",
            serde_json::json!([
                {"product_id":"BTC-USD","trade_id":"1116181250","price":"75349.37","size":"1",
                 "time":"2026-11-06T07:08:35.856382Z","side":"SELL"},
                {"product_id":"BTC-USD","trade_id":"1116181249","price":"75347.48","size":"1",
                 "time":"2026-11-06T07:08:35.856021Z","side":"BUY"}
            ]),
            117,
        );
        let rows = trades(&CoinbaseAdapter::parse_messages_for_feed(
            &feed(),
            &message,
            1.0,
        ));
        assert!(rows[0].1 < rows[1].1);
        assert_eq!(rows[0].2.as_deref(), Some("1116181249"));

        // Exactly equal timestamps: numeric trade id, not lexical or input order.
        let tied = trade_frame(
            "update",
            serde_json::json!([
                {"product_id":"BTC-USD","trade_id":"99","price":"2","size":"1",
                 "time":"2026-11-06T07:08:36.000000Z","side":"SELL"},
                {"product_id":"BTC-USD","trade_id":"100","price":"1","size":"1",
                 "time":"2026-11-06T07:08:36.000000Z","side":"BUY"}
            ]),
            118,
        );
        let rows = trades(&CoinbaseAdapter::parse_messages_for_feed(
            &feed(),
            &tied,
            1.0,
        ));
        assert_eq!(rows[0].2.as_deref(), Some("99"));
        assert_eq!(rows[1].2.as_deref(), Some("100"));
    }

    #[cfg(feature = "trade")]
    #[test]
    fn mixed_symbols_are_sorted_by_time_within_one_frame_only() {
        let message = trade_frame(
            "update",
            serde_json::json!([
                {"product_id":"BTC-USD","trade_id":"3","price":"3","size":"1",
                 "time":"2026-11-06T07:08:19.000000Z","side":"SELL"},
                {"product_id":"ETH-USD","trade_id":"1","price":"1","size":"1",
                 "time":"2026-11-06T07:08:17.000000Z","side":"BUY"},
                {"product_id":"BTC-USD","trade_id":"2","price":"2","size":"1",
                 "time":"2026-11-06T07:08:18.000000Z","side":"BUY"}
            ]),
            20,
        );
        let rows = trades(&CoinbaseAdapter::parse_messages_for_feed(
            &feed(),
            &message,
            1.0,
        ));
        let times: Vec<f64> = rows.iter().map(|row| row.1).collect();
        assert!(times.windows(2).all(|pair| pair[0] <= pair[1]));
        assert_eq!(rows[0].0, "ETH-USD");

        // A later frame carrying an older trade is not reordered across frames.
        let older = trade_frame(
            "update",
            serde_json::json!([
                {"product_id":"BTC-USD","trade_id":"4","price":"4","size":"1",
                 "time":"2026-11-06T07:08:16.000000Z","side":"SELL"}
            ]),
            21,
        );
        let late = trades(&CoinbaseAdapter::parse_messages_for_feed(
            &feed(),
            &older,
            1.0,
        ));
        assert_eq!(late.len(), 1);
        assert!(late[0].1 < times[0]);
    }

    #[test]
    fn classifies_subscription_acks_heartbeats_and_errors() {
        assert_eq!(
            CoinbaseAdapter::parse_control_message(&serde_json::json!({
                "channel": "subscriptions", "timestamp": "2026-11-04T01:46:46Z",
                "sequence_num": 1, "events": [{"subscriptions": {"ticker": ["BTC-USD"]}}]
            }))
            .expect("ack")
            .expect("ok"),
            CoinbaseControl::Subscribed
        );
        assert_eq!(
            CoinbaseAdapter::parse_control_message(&serde_json::json!({
                "channel": "heartbeats", "events": [{"heartbeat_counter": 1}]
            }))
            .expect("heartbeat")
            .expect("ok"),
            CoinbaseControl::Heartbeat
        );
        let error = CoinbaseAdapter::parse_control_message(&serde_json::json!({
            "type": "error", "message": "subscribe or unsubscribe required"
        }))
        .expect("error");
        assert!(error.is_err());
        assert!(
            CoinbaseAdapter::parse_control_message(&serde_json::json!({
                "channel": "ticker", "events": []
            }))
            .is_none()
        );
    }

    #[cfg(all(feature = "ticker", feature = "orderbook"))]
    #[test]
    fn feed_context_filters_unconfigured_products_and_normalizes() {
        let feed = Coinbase::new().ticker().symbol("BTC-USD").build();
        let message = serde_json::json!({
            "channel": "ticker", "timestamp": "2026-11-04T01:46:46Z", "sequence_num": 0,
            "events": [{"type": "snapshot", "tickers": [
                {"type": "ticker", "product_id": "BTC-USD", "price": "1",
                 "best_bid": "76778.5", "best_ask": "76778.51",
                 "best_bid_quantity": "0.5", "best_ask_quantity": "0.25"},
                {"type": "ticker", "product_id": "ETH-USD", "price": "1",
                 "best_bid": "1", "best_ask": "2",
                 "best_bid_quantity": "1", "best_ask_quantity": "1"}
            ]}]
        });
        let events = CoinbaseAdapter::parse_messages_for_feed(&feed, &message, 1.0);
        // The documented ticker payload carries BBO sizes as well, so the
        // configured feed receives a Ticker and an L1Book.
        assert_eq!(events.len(), 2);
        let mut saw_ticker = false;
        let mut saw_l1 = false;
        for event in &events {
            match event {
                super::CoinbaseEvent::Ticker(ticker) => {
                    saw_ticker = true;
                    assert_eq!(ticker.symbol.as_str(), "BTC-USD");
                    assert_eq!(ticker.bid.to_string(), "76778.5");
                }
                super::CoinbaseEvent::L1Book(book) => {
                    saw_l1 = true;
                    assert_eq!(book.bid.amount.to_string(), "0.5");
                }
                _ => panic!("unexpected event"),
            }
        }
        assert!(saw_ticker && saw_l1);
    }
}
