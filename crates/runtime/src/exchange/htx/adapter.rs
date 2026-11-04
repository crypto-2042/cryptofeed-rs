//! HTX (Huobi) public WebSocket routing and subscription framing.
//!
//! Three hosts are used, selected by product and channel:
//!
//! - spot market: `wss://api.huobi.pro/ws`
//! - USDT-M market: `wss://api.hbdm.com/linear-swap-ws`
//! - USDT-M funding/liquidation notifications:
//!   `wss://api.hbdm.com/linear-swap-notification`
//!
//! Market topics use `{"sub":topic,"id":..}` and are acknowledged with an
//! `id`/`subbed` echo; notification topics use
//! `{"op":"sub","topic":..,"cid":..}` and are acknowledged with `err-code: 0`.
//! Both hosts GZIP-compress every frame and use server-initiated integer
//! `{"ping":..}` (market) / `{"op":"ping","ts":..}` (notification) heartbeats.
//! Market subscriptions are lower-case for spot (`btcusdt`) and upper-case
//! contract codes for USDT-M (`BTC-USDT`).

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
#[cfg(feature = "liquidations")]
use cryptofeed_liquidations::Liquidation;
#[cfg(feature = "orderbook")]
use cryptofeed_orderbook::L1Book;
#[cfg(feature = "ticker")]
use cryptofeed_ticker::Ticker;
#[cfg(feature = "trade")]
use cryptofeed_trade::Trade;

pub struct HtxAdapter;

pub const SPOT_WS_URL: &str = "wss://api.huobi.pro/ws";
pub const SWAP_MARKET_WS_URL: &str = "wss://api.hbdm.com/linear-swap-ws";
pub const SWAP_NOTIFICATION_WS_URL: &str = "wss://api.hbdm.com/linear-swap-notification";

/// Documented unmerged incremental depth size for USDT-M (`20` or `150`).
pub const SWAP_DEPTH_SIZE: u16 = 20;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HtxProduct {
    Spot,
    Perpetual,
}

#[derive(Clone, Debug, PartialEq)]
pub enum HtxControl {
    /// `id`/`subbed` + `status:ok` (market) or `op:sub` + `err-code:0`.
    Ack,
    /// Server heartbeat whose value must be echoed in a `pong` frame.
    Ping(Value),
}

pub enum HtxEvent {
    #[cfg(feature = "candles")]
    Candle(Candle),
    #[cfg(feature = "funding")]
    Funding(Funding),
    #[cfg(feature = "liquidations")]
    Liquidation(Liquidation),
    #[cfg(feature = "orderbook")]
    L1Book(L1Book),
    #[cfg(feature = "ticker")]
    Ticker(Ticker),
    #[cfg(feature = "trade")]
    Trade(Trade),
}

impl HtxAdapter {
    pub fn product_for_kind(kind: InstrumentKind) -> Option<HtxProduct> {
        match kind {
            InstrumentKind::Spot => Some(HtxProduct::Spot),
            InstrumentKind::Perpetual => Some(HtxProduct::Perpetual),
            _ => None,
        }
    }

    pub fn product_for_feed(feed: &ExchangeFeed) -> Result<HtxProduct> {
        let kind = feed.product_kind()?;
        Self::product_for_kind(kind)
            .ok_or_else(|| Error::UnsupportedCapability(format!("HTX {:?} product", kind)))
    }

    pub fn is_notification_url(url: &str) -> bool {
        url == SWAP_NOTIFICATION_WS_URL
    }

    /// Market connection first; a second notification connection only when a
    /// perpetual feed subscribes funding/liquidations.
    pub fn subscription_urls(feed: &ExchangeFeed) -> Vec<String> {
        let product = Self::product_for_feed(feed).unwrap_or(HtxProduct::Spot);
        let mut urls = vec![match product {
            HtxProduct::Spot => SPOT_WS_URL.to_owned(),
            HtxProduct::Perpetual => SWAP_MARKET_WS_URL.to_owned(),
        }];
        if product == HtxProduct::Perpetual
            && feed
                .channels
                .iter()
                .any(|channel| matches!(channel, Channel::Funding | Channel::Liquidations))
        {
            urls.push(SWAP_NOTIFICATION_WS_URL.to_owned());
        }
        urls
    }

    /// Legacy single-URL helper used by `planned_url`; the first connection is
    /// always the market connection.
    pub fn subscription_url(feed: &ExchangeFeed) -> String {
        Self::subscription_urls(feed)
            .into_iter()
            .next()
            .unwrap_or_else(|| SPOT_WS_URL.to_owned())
    }

    /// Projects a feed onto one planned connection's channels/symbols.
    pub fn feed_for_url(feed: &ExchangeFeed, url: &str) -> ExchangeFeed {
        let mut planned = feed.clone();
        let notification = Self::is_notification_url(url);
        planned.channels.retain(|channel| {
            let notification_channel = matches!(channel, Channel::Funding | Channel::Liquidations);
            notification_channel == notification
        });
        planned.prune_connection_scope();
        planned
    }

    /// Normalized candle interval -> documented wire period.
    pub fn candle_interval_wire(interval: &str, product: InstrumentKind) -> Option<&'static str> {
        match (interval, product) {
            ("1m", _) => Some("1min"),
            ("5m", _) => Some("5min"),
            ("15m", _) => Some("15min"),
            ("30m", _) => Some("30min"),
            ("1h", _) => Some("60min"),
            ("4h", _) => Some("4hour"),
            ("1d", _) => Some("1day"),
            ("1w", InstrumentKind::Spot) => Some("1week"),
            ("1M", _) => Some("1mon"),
            _ => None,
        }
    }

    /// Spot depth is a full snapshot (`step0`); USDT-M uses the fixed
    /// `size_20.high_freq` incremental topic. A requested depth has no
    /// documented per-feed parameter and is rejected at preflight.
    pub fn l2_book_depth_supported(_level: u16) -> bool {
        false
    }

    fn native_symbol(feed: &ExchangeFeed, symbol: &Symbol, product: HtxProduct) -> Result<String> {
        let native = feed.exchange_symbol_for(symbol)?.to_owned();
        Ok(match product {
            HtxProduct::Spot => native.to_ascii_lowercase(),
            HtxProduct::Perpetual => native.to_ascii_uppercase(),
        })
    }

    fn market_topic(
        feed: &ExchangeFeed,
        channel: Channel,
        symbol: &Symbol,
        product: HtxProduct,
    ) -> Result<String> {
        let native = Self::native_symbol(feed, symbol, product)?;
        Ok(match (channel, product) {
            (Channel::Ticker, HtxProduct::Spot) => format!("market.{native}.ticker"),
            (Channel::Ticker, HtxProduct::Perpetual) => format!("market.{native}.detail"),
            (Channel::L1Book, _) => format!("market.{native}.bbo"),
            (Channel::Trade, _) => format!("market.{native}.trade.detail"),
            (Channel::L2Book, HtxProduct::Spot) => format!("market.{native}.depth.step0"),
            (Channel::L2Book, HtxProduct::Perpetual) => {
                format!("market.{native}.depth.size_{SWAP_DEPTH_SIZE}.high_freq")
            }
            (Channel::Candles, _) => {
                let period =
                    Self::candle_interval_wire(&feed.candle_interval, feed.product_kind()?)
                        .ok_or_else(|| {
                            Error::UnsupportedCapability(format!(
                                "HTX candle interval {:?}",
                                feed.candle_interval
                            ))
                        })?;
                format!("market.{native}.kline.{period}")
            }
            _ => {
                return Err(Error::UnsupportedCapability(format!(
                    "HTX {product:?} channel {channel:?}"
                )));
            }
        })
    }

    /// Subscription frames for one planned connection (market or notification).
    pub fn subscription_messages(feed: &ExchangeFeed) -> Result<Vec<String>> {
        let product = Self::product_for_feed(feed)?;
        let notification = feed
            .channels
            .iter()
            .all(|channel| matches!(channel, Channel::Funding | Channel::Liquidations))
            && !feed.channels.is_empty();
        let mut frames = Vec::new();
        let mut seen = std::collections::HashSet::new();
        if notification && product == HtxProduct::Perpetual {
            for channel in &feed.channels {
                for symbol in &feed.symbols {
                    let native = Self::native_symbol(feed, symbol, product)?;
                    let topic = match channel {
                        Channel::Funding => format!("public.{native}.funding_rate"),
                        Channel::Liquidations => {
                            format!("public.{native}.liquidation_orders")
                        }
                        _ => continue,
                    };
                    if !seen.insert(topic.clone()) {
                        continue;
                    }
                    frames.push(
                        serde_json::json!({
                            "op": "sub",
                            "topic": topic,
                            "cid": format!("cf-{}", frames.len() + 1),
                        })
                        .to_string(),
                    );
                }
            }
            return Ok(frames);
        }

        for channel in &feed.channels {
            for symbol in &feed.symbols {
                if !feed.subscribes(*channel, symbol) {
                    continue;
                }
                let topic = Self::market_topic(feed, *channel, symbol, product)?;
                if !seen.insert(topic.clone()) {
                    continue;
                }
                let mut frame = serde_json::json!({
                    "sub": topic,
                    "id": format!("cf-{}", frames.len() + 1),
                });
                if *channel == Channel::L2Book && product == HtxProduct::Perpetual {
                    frame["data_type"] = Value::String("incremental".to_owned());
                }
                frames.push(frame.to_string());
            }
        }
        Ok(frames)
    }

    /// In-session resubscription pair for a USDT-M incremental depth version
    /// gap; the next push is the documented replacement snapshot.
    pub fn book_resubscribe_messages(feed: &ExchangeFeed, symbol: &Symbol) -> Result<Vec<String>> {
        let product = Self::product_for_feed(feed)?;
        if product != HtxProduct::Perpetual {
            return Err(Error::UnsupportedCapability(
                "HTX spot depth is snapshot-only".to_owned(),
            ));
        }
        let topic = Self::market_topic(feed, Channel::L2Book, symbol, product)?;
        Ok(vec![
            serde_json::json!({"unsub": topic, "id": "cf-resub-1"}).to_string(),
            serde_json::json!({
                "sub": topic, "data_type": "incremental", "id": "cf-resub-2"
            })
            .to_string(),
        ])
    }

    /// Classifies acks, errors and documented heartbeat pings before parsing.
    pub fn parse_control_message(message: &Value) -> Option<Result<HtxControl>> {
        if let Some(ping) = message.get("ping") {
            return Some(Ok(HtxControl::Ping(ping.clone())));
        }
        if message.get("op").and_then(Value::as_str) == Some("ping") {
            return Some(Ok(HtxControl::Ping(
                message.get("ts").cloned().unwrap_or(Value::Null),
            )));
        }
        // Market ack/error: {"id":..,"status":"ok","subbed":..} /
        // {"id":..,"status":"error","err-code":..,"err-msg":..}.
        if let Some(status) = message.get("status").and_then(Value::as_str) {
            if status == "ok" {
                return Some(Ok(HtxControl::Ack));
            }
            let detail = message
                .get("err-msg")
                .and_then(Value::as_str)
                .unwrap_or("subscription rejected");
            return Some(Err(Error::Subscription(format!(
                "htx websocket error: {detail}"
            ))));
        }
        // Notification ack: {"op":"sub","cid":..,"topic":..,"err-code":0}.
        if message.get("op").and_then(Value::as_str) == Some("sub") {
            let code = message.get("err-code").and_then(Value::as_i64).unwrap_or(0);
            if code == 0 {
                return Some(Ok(HtxControl::Ack));
            }
            let detail = message
                .get("err-msg")
                .and_then(Value::as_str)
                .unwrap_or("subscription rejected");
            return Some(Err(Error::Subscription(format!(
                "htx notification error {code}: {detail}"
            ))));
        }
        None
    }

    /// Documented heartbeat reply for a server ping frame.
    pub fn pong_message(message: &Value) -> Option<String> {
        if let Some(ping) = message.get("ping") {
            return Some(serde_json::json!({"pong": ping}).to_string());
        }
        if message.get("op").and_then(Value::as_str) == Some("ping") {
            return Some(
                serde_json::json!({
                    "op": "pong",
                    "ts": message.get("ts").cloned().unwrap_or(Value::Null),
                })
                .to_string(),
            );
        }
        None
    }

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
    ) -> Vec<HtxEvent> {
        let product = match Self::product_for_feed(feed) {
            Ok(product) => product,
            Err(_) => return Vec::new(),
        };
        let mut events = Vec::new();
        if parser::op(message) == Some("notify") {
            let Some(topic) = parser::topic(message) else {
                return Vec::new();
            };
            let Some(native) = parser::notification_symbol(topic) else {
                return Vec::new();
            };
            let Some(symbol) = Self::symbol_for_native(feed, native) else {
                return Vec::new();
            };
            let exchange_ts = message
                .get("ts")
                .and_then(parser::parse_millis)
                .unwrap_or(received_ts);
            let rows = message
                .get("data")
                .and_then(Value::as_array)
                .map(Vec::as_slice)
                .unwrap_or_default();
            for row in rows {
                match parser::notification_kind(topic) {
                    #[cfg(feature = "funding")]
                    Some("funding_rate") => {
                        if let Some(funding) =
                            parser::parse_funding(row, exchange_ts, received_ts, symbol)
                        {
                            events.push(HtxEvent::Funding(funding));
                        }
                    }
                    #[cfg(feature = "liquidations")]
                    Some("liquidation_orders") => {
                        if let Some(liquidation) =
                            parser::parse_liquidation(row, received_ts, symbol)
                        {
                            events.push(HtxEvent::Liquidation(liquidation));
                        }
                    }
                    _ => {}
                }
            }
            return events;
        }

        let Some(channel) = parser::channel(message) else {
            return Vec::new();
        };
        let Some((native, kind, extra)) = parser::topic_parts(channel) else {
            return Vec::new();
        };
        let Some(symbol) = Self::symbol_for_native(feed, native) else {
            return Vec::new();
        };
        let Some(tick) = message.get("tick") else {
            return Vec::new();
        };
        match (product, kind) {
            (HtxProduct::Spot, "ticker") => {
                #[cfg(feature = "ticker")]
                if let Some(ticker) = parser::parse_ticker(tick, received_ts, symbol) {
                    events.push(HtxEvent::Ticker(ticker));
                }
            }
            (HtxProduct::Perpetual, "detail") => {
                #[cfg(feature = "ticker")]
                if let Some(ticker) = parser::parse_swap_detail_ticker(tick, received_ts, symbol) {
                    events.push(HtxEvent::Ticker(ticker));
                }
            }
            (_, "bbo") => {
                #[cfg(feature = "orderbook")]
                {
                    let book = match product {
                        HtxProduct::Spot => parser::parse_spot_bbo(tick, received_ts, symbol),
                        HtxProduct::Perpetual => parser::parse_swap_bbo(tick, received_ts, symbol),
                    };
                    if let Some(book) = book {
                        events.push(HtxEvent::L1Book(book));
                    }
                }
            }
            (_, "trade") => {
                #[cfg(feature = "trade")]
                for row in tick
                    .get("data")
                    .and_then(Value::as_array)
                    .map(Vec::as_slice)
                    .unwrap_or_default()
                {
                    let trade = match product {
                        HtxProduct::Spot => parser::parse_spot_trade(row, received_ts, symbol),
                        HtxProduct::Perpetual => parser::parse_swap_trade(row, received_ts, symbol),
                    };
                    if let Some(trade) = trade {
                        events.push(HtxEvent::Trade(trade));
                    }
                }
            }
            (_, "kline") =>
            {
                #[cfg(feature = "candles")]
                if let Some(period) = extra {
                    let candle = match product {
                        HtxProduct::Spot => {
                            parser::parse_spot_candle(tick, period, received_ts, symbol)
                        }
                        HtxProduct::Perpetual => {
                            parser::parse_swap_candle(tick, period, received_ts, symbol)
                        }
                    };
                    if let Some(candle) = candle {
                        events.push(HtxEvent::Candle(candle));
                    }
                }
            }
            _ => {}
        }
        events
    }

    /// Decompresses one GZIP WebSocket binary frame. Every HTX market and
    /// notification push is compressed; plain-text control frames are handled
    /// by the connection layer before this is called.
    pub fn decode_gzip_frame(bytes: &[u8]) -> Result<String> {
        use std::io::Read;
        let mut decoder = flate2::read::GzDecoder::new(bytes);
        let mut text = String::new();
        decoder
            .read_to_string(&mut text)
            .map_err(|error| Error::Parse(format!("HTX gzip frame: {error}")))?;
        Ok(text)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        HtxAdapter, HtxControl, HtxProduct, SPOT_WS_URL, SWAP_MARKET_WS_URL,
        SWAP_NOTIFICATION_WS_URL,
    };
    use crate::exchange::htx::Htx;
    use serde_json::Value;

    #[test]
    fn spot_market_topics_use_lowercase_symbols_and_sub_id_framing() {
        let feed = Htx::new()
            .ticker()
            .trade()
            .l2_book()
            .l1_book()
            .candles()
            .candles_interval("5m")
            .symbol("BTC-USDT")
            .exchange_symbol("BTCUSDT")
            .build();
        assert_eq!(
            HtxAdapter::subscription_urls(&feed),
            vec![SPOT_WS_URL.to_owned()]
        );
        let frames = HtxAdapter::subscription_messages(&feed).expect("frames");
        assert_eq!(frames.len(), 5);
        let topics: Vec<String> = frames
            .iter()
            .map(|frame| {
                let value: Value = serde_json::from_str(frame).expect("json");
                value["sub"].as_str().expect("sub").to_owned()
            })
            .collect();
        assert!(topics.contains(&"market.btcusdt.ticker".to_owned()));
        assert!(topics.contains(&"market.btcusdt.trade.detail".to_owned()));
        assert!(topics.contains(&"market.btcusdt.depth.step0".to_owned()));
        assert!(topics.contains(&"market.btcusdt.bbo".to_owned()));
        assert!(topics.contains(&"market.btcusdt.kline.5min".to_owned()));
        assert_eq!(
            HtxAdapter::candle_interval_wire("1w", cryptofeed_core::symbol::InstrumentKind::Spot),
            Some("1week")
        );
        assert_eq!(
            HtxAdapter::candle_interval_wire(
                "1w",
                cryptofeed_core::symbol::InstrumentKind::Perpetual
            ),
            None
        );
    }

    #[test]
    fn swap_routes_market_and_notification_connections_separately() {
        let feed = Htx::new()
            .ticker()
            .trade()
            .l2_book()
            .funding()
            .liquidations()
            .symbol("BTC-USDT-PERP")
            .exchange_symbol("BTC-USDT")
            .build();
        assert_eq!(
            HtxAdapter::subscription_urls(&feed),
            vec![
                SWAP_MARKET_WS_URL.to_owned(),
                SWAP_NOTIFICATION_WS_URL.to_owned()
            ]
        );
        let market = HtxAdapter::feed_for_url(&feed, SWAP_MARKET_WS_URL);
        let market_frames = HtxAdapter::subscription_messages(&market).expect("market");
        let topics: Vec<String> = market_frames
            .iter()
            .map(|frame| {
                serde_json::from_str::<Value>(frame).expect("json")["sub"]
                    .as_str()
                    .expect("sub")
                    .to_owned()
            })
            .collect();
        assert!(topics.contains(&"market.BTC-USDT.detail".to_owned()));
        assert!(topics.contains(&"market.BTC-USDT.depth.size_20.high_freq".to_owned()));
        assert!(
            market_frames
                .iter()
                .any(|frame| frame.contains("\"incremental\""))
        );

        let notification = HtxAdapter::feed_for_url(&feed, SWAP_NOTIFICATION_WS_URL);
        let notification_frames =
            HtxAdapter::subscription_messages(&notification).expect("notification");
        assert_eq!(notification_frames.len(), 2);
        let first: Value = serde_json::from_str(&notification_frames[0]).expect("json");
        assert_eq!(first["op"], "sub");
        assert_eq!(first["topic"], "public.BTC-USDT.funding_rate");
        assert_eq!(
            HtxAdapter::book_resubscribe_messages(
                &notification,
                &cryptofeed_core::symbol::Symbol::perpetual("BTC", "USDT")
            )
            .expect("resub")
            .len(),
            2
        );
    }

    #[test]
    fn classifies_market_and_notification_acks_errors_and_heartbeats() {
        assert_eq!(
            HtxAdapter::parse_control_message(&serde_json::json!({
                "id":"cf-1","status":"ok","subbed":"market.btcusdt.ticker","ts":1
            }))
            .expect("ack")
            .expect("ok"),
            HtxControl::Ack
        );
        assert!(
            HtxAdapter::parse_control_message(&serde_json::json!({
                "id":"cf-1","status":"error","err-code":"bad-request","err-msg":"invalid topic"
            }))
            .expect("error")
            .is_err()
        );
        assert_eq!(
            HtxAdapter::parse_control_message(&serde_json::json!({"ping": 1793757093533_i64}))
                .expect("ping")
                .expect("ok"),
            HtxControl::Ping(serde_json::json!(1793757093533_i64))
        );
        assert_eq!(
            HtxAdapter::pong_message(&serde_json::json!({"ping": 12})),
            Some(r#"{"pong":12}"#.to_owned())
        );
        assert_eq!(
            HtxAdapter::parse_control_message(&serde_json::json!({
                "op":"sub","cid":"cf-1","topic":"public.BTC-USDT.funding_rate","ts":1,"err-code":0
            }))
            .expect("ack")
            .expect("ok"),
            HtxControl::Ack
        );
        assert_eq!(
            HtxAdapter::pong_message(&serde_json::json!({"op":"ping","ts":"1"})),
            Some(r#"{"op":"pong","ts":"1"}"#.to_owned())
        );
        assert!(
            HtxAdapter::parse_control_message(&serde_json::json!({"ch":"market.btcusdt.ticker"}))
                .is_none()
        );
    }

    #[test]
    fn decodes_documented_gzip_frames() {
        use std::io::Write;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder
            .write_all(br#"{"id":"cf-1","status":"ok","subbed":"market.btcusdt.ticker"}"#)
            .unwrap();
        let bytes = encoder.finish().unwrap();
        let text = HtxAdapter::decode_gzip_frame(&bytes).expect("gzip");
        assert!(text.contains("market.btcusdt.ticker"));
        assert!(HtxAdapter::decode_gzip_frame(b"not gzip").is_err());
    }

    #[test]
    fn products_map_only_spot_and_perpetual() {
        use cryptofeed_core::symbol::InstrumentKind;
        assert_eq!(
            HtxAdapter::product_for_kind(InstrumentKind::Spot),
            Some(HtxProduct::Spot)
        );
        assert_eq!(
            HtxAdapter::product_for_kind(InstrumentKind::Perpetual),
            Some(HtxProduct::Perpetual)
        );
        assert_eq!(HtxAdapter::product_for_kind(InstrumentKind::Futures), None);
        assert_eq!(HtxAdapter::product_for_kind(InstrumentKind::Option), None);
    }
}
