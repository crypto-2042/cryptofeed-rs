//! KuCoin UTA WebSocket V2 routing and subscription framing.
//!
//! Public push hosts are product-specific (`x-push-spot` / `x-push-futures`),
//! so one logical feed always uses one host: capability preflight rejects mixed
//! product kinds. Subscription frames batch the configured symbols per
//! channel; `id` correlates the `{"id":..., "result":...}` acknowledgement.
//! Liveness is the documented `{"op":"ping"}` / `{"op":"pong"}` exchange.

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
#[cfg(feature = "markprice")]
use cryptofeed_markprice::MarkPrice;
#[cfg(feature = "openinterest")]
use cryptofeed_openinterest::OpenInterest;
#[cfg(feature = "orderbook")]
use cryptofeed_orderbook::L2Book;
#[cfg(feature = "ticker")]
use cryptofeed_ticker::Ticker;
#[cfg(feature = "trade")]
use cryptofeed_trade::Trade;

pub struct KucoinAdapter;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KucoinProduct {
    Spot,
    Futures,
}

impl KucoinProduct {
    pub fn trade_type(self) -> &'static str {
        match self {
            KucoinProduct::Spot => "SPOT",
            KucoinProduct::Futures => "FUTURES",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KucoinControl {
    Welcome,
    Subscribed,
    Pong,
}

pub enum KucoinEvent {
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
    #[cfg(feature = "orderbook")]
    L2Book(L2Book),
    #[cfg(feature = "ticker")]
    Ticker(Ticker),
    #[cfg(feature = "trade")]
    Trade(Trade),
}

/// Documented `obu` depth parameter used when no explicit partial depth is
/// requested: a snapshot followed by `O`/`C`-sequenced deltas every 10 ms.
pub const L2_DEPTH_INCREMENT: &str = "increment@10ms";

impl KucoinAdapter {
    /// Public push host for spot.
    pub fn websocket_url() -> &'static str {
        "wss://x-push-spot.kucoin.com"
    }

    /// Public push host for futures.
    pub fn futures_websocket_url() -> &'static str {
        "wss://x-push-futures.kucoin.com"
    }

    pub fn websocket_url_for_product(product: KucoinProduct) -> &'static str {
        match product {
            KucoinProduct::Spot => Self::websocket_url(),
            KucoinProduct::Futures => Self::futures_websocket_url(),
        }
    }

    pub fn product_for_symbol(symbol: &Symbol) -> KucoinProduct {
        match symbol.kind() {
            InstrumentKind::Perpetual | InstrumentKind::Futures => KucoinProduct::Futures,
            _ => KucoinProduct::Spot,
        }
    }

    pub fn subscription_url(feed: &ExchangeFeed) -> String {
        let product = feed
            .symbols
            .first()
            .map(Self::product_for_symbol)
            .unwrap_or(KucoinProduct::Spot);
        Self::websocket_url_for_product(product).to_owned()
    }

    /// Application heartbeat: the public push host replies with
    /// `{"id":"cf-heartbeat","op":"pong"}`.
    pub fn heartbeat_message() -> &'static str {
        r#"{"id":"cf-heartbeat","op":"ping"}"#
    }

    /// Normalized candle interval to the documented KuCoin kline label.
    /// `6hour` is documented as unavailable for futures contracts, so it is
    /// rejected for `FUTURES` during preflight.
    pub fn candle_interval_wire(interval: &str, product: KucoinProduct) -> Option<&'static str> {
        let wire = match interval {
            "1m" => "1min",
            "3m" => "3min",
            "5m" => "5min",
            "15m" => "15min",
            "30m" => "30min",
            "1h" => "1hour",
            "2h" => "2hour",
            "4h" => "4hour",
            "6h" => "6hour",
            "8h" => "8hour",
            "12h" => "12hour",
            "1d" => "1day",
            "1w" => "1week",
            "1M" => "1month",
            _ => return None,
        };
        if wire == "6hour" && product == KucoinProduct::Futures {
            return None;
        }
        Some(wire)
    }

    /// Documented `obu` depth parameter. `None` requests the incremental
    /// `increment@10ms` stream (snapshot + sequenced deltas); `1`, `5` and
    /// `50` are snapshot-only partial books.
    pub fn l2_depth_wire(depth: Option<u16>) -> Option<&'static str> {
        match depth {
            None => Some(L2_DEPTH_INCREMENT),
            Some(1) => Some("1"),
            Some(5) => Some("5"),
            Some(50) => Some("50"),
            Some(_) => None,
        }
    }

    pub fn l2_book_depth_supported(level: u16) -> bool {
        matches!(level, 1 | 5 | 50)
    }

    /// Snapshot-only depths replace the local book on every push and cannot
    /// carry incremental sequence state.
    pub fn l2_book_snapshot_only(depth: Option<u16>) -> bool {
        depth.is_some()
    }

    pub fn parse_control_message(message: &Value) -> Option<Result<KucoinControl>> {
        if message.get("message").and_then(Value::as_str) == Some("welcome") {
            return Some(Ok(KucoinControl::Welcome));
        }
        if message.get("op").and_then(Value::as_str) == Some("pong") {
            return Some(Ok(KucoinControl::Pong));
        }
        let result = message.get("result")?.as_bool()?;
        if result {
            Some(Ok(KucoinControl::Subscribed))
        } else {
            let detail = message
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("subscription rejected");
            // A rejected subscription is a permanent configuration problem;
            // `Subscription` errors fail fast instead of burning the retries.
            Some(Err(Error::Subscription(format!(
                "kucoin websocket error: {detail}"
            ))))
        }
    }

    fn trade_type_for_feed(feed: &ExchangeFeed) -> Result<KucoinProduct> {
        match feed.product_kind()? {
            InstrumentKind::Spot => Ok(KucoinProduct::Spot),
            InstrumentKind::Perpetual | InstrumentKind::Futures => Ok(KucoinProduct::Futures),
            product => Err(Error::UnsupportedCapability(format!("Kucoin/{product:?}"))),
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

    /// One subscription frame per (channel, channel parameter), carrying all
    /// configured symbols of the feed. `mark-price` is shared by mark price,
    /// index price and open interest, so it is emitted exactly once.
    pub fn subscription_messages(feed: &ExchangeFeed) -> Result<Vec<String>> {
        let product = Self::trade_type_for_feed(feed)?;
        let native_symbols = Self::native_symbols(feed)?;
        let mut frames: Vec<Value> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for channel in &feed.channels {
            let (wire_channel, parameter) = match channel {
                Channel::Ticker => ("ticker", None),
                Channel::Trade => ("trade", None),
                Channel::L2Book => (
                    "obu",
                    Some((
                        "depth",
                        Self::l2_depth_wire(feed.l2_book_depth).ok_or_else(|| {
                            Error::UnsupportedCapability(format!(
                                "Kucoin l2 depth level {:?}",
                                feed.l2_book_depth
                            ))
                        })?,
                    )),
                ),
                Channel::Candles => (
                    "kline",
                    Some((
                        "interval",
                        Self::candle_interval_wire(&feed.candle_interval, product).ok_or_else(
                            || {
                                Error::UnsupportedCapability(format!(
                                    "Kucoin candle interval {:?}",
                                    feed.candle_interval
                                ))
                            },
                        )?,
                    )),
                ),
                Channel::Funding => ("funding-fee", None),
                Channel::MarkPrice | Channel::Index | Channel::OpenInterest => ("mark-price", None),
                // Unreachable after capability preflight; do not invent a
                // native channel for L1Book or liquidations.
                _ => continue,
            };
            let key = (wire_channel, parameter);
            if !seen.insert(key) {
                continue;
            }
            // The current UTA V2 push service rejects the batched `symbols`
            // array for `trade` and `mark-price` (verified live 2026-11-04,
            // `symbols are not supported for topic ...`), while `ticker`,
            // `obu`, `kline` and `funding-fee` accept it. Singled-symbol
            // channels therefore emit one frame per instrument.
            let singular = matches!(wire_channel, "trade" | "mark-price");
            let symbols: Vec<String> = if singular {
                native_symbols.clone()
            } else {
                vec![String::new()]
            };
            for symbol in symbols {
                let mut frame = serde_json::json!({
                    "id": format!("cf-{}", frames.len()),
                    "action": "SUBSCRIBE",
                    "channel": wire_channel,
                    "tradeType": product.trade_type(),
                });
                if singular {
                    frame["symbol"] = Value::String(symbol);
                } else {
                    frame["symbols"] = serde_json::json!(native_symbols);
                }
                if let Some((field, value)) = parameter {
                    frame[field] = Value::String(value.to_owned());
                }
                frames.push(frame);
            }
        }
        Ok(frames.into_iter().map(|frame| frame.to_string()).collect())
    }

    /// In-session resnapshot for one order-book symbol: unsubscribe and
    /// subscribe the same `obu` parameter so the next push is a documented
    /// snapshot. Used only by the bounded book-recovery path.
    pub fn book_resubscribe_messages(feed: &ExchangeFeed, symbol: &Symbol) -> Result<Vec<String>> {
        let product = Self::trade_type_for_feed(feed)?;
        let native = feed.exchange_symbol_for(symbol)?.to_owned();
        let depth = Self::l2_depth_wire(feed.l2_book_depth).ok_or_else(|| {
            Error::UnsupportedCapability(format!("Kucoin l2 depth level {:?}", feed.l2_book_depth))
        })?;
        let frame = |action: &str, suffix: &str| {
            serde_json::json!({
                "id": format!("cf-{action}-{suffix}"),
                "action": action,
                "channel": "obu",
                "tradeType": product.trade_type(),
                "symbols": [native],
                "depth": depth,
            })
            .to_string()
        };
        Ok(vec![frame("UNSUBSCRIBE", "1"), frame("SUBSCRIBE", "2")])
    }

    /// Resolves the configured normalized symbol for a native payload symbol.
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
    ) -> Vec<KucoinEvent> {
        let Some(native) = parser::native_symbol(message) else {
            return Vec::new();
        };
        let Some(symbol) = Self::symbol_for_native(feed, native).cloned() else {
            return Vec::new();
        };
        Self::parse_messages_with_symbol(message, received_ts, &symbol)
    }

    pub fn parse_message(message: &Value, received_ts: f64) -> Option<KucoinEvent> {
        Self::parse_messages(message, received_ts)
            .into_iter()
            .next()
    }

    pub fn parse_messages(message: &Value, received_ts: f64) -> Vec<KucoinEvent> {
        let Some(topic) = message.get("T").and_then(Value::as_str) else {
            return Vec::new();
        };
        let Some(native) = parser::native_symbol(message) else {
            return Vec::new();
        };
        let Some(product) = topic_product(topic) else {
            return Vec::new();
        };
        let symbol = parser::normalize_symbol(native, product);
        Self::parse_messages_with_symbol(message, received_ts, &symbol)
    }

    fn parse_messages_with_symbol(
        message: &Value,
        received_ts: f64,
        symbol: &Symbol,
    ) -> Vec<KucoinEvent> {
        let Some(topic) = message.get("T").and_then(Value::as_str) else {
            return Vec::new();
        };
        let base = topic.split('.').next().unwrap_or(topic);
        match base {
            #[cfg(feature = "ticker")]
            "ticker" => parser::parse_ticker_for_symbol(message, received_ts, symbol)
                .map(KucoinEvent::Ticker)
                .into_iter()
                .collect(),
            #[cfg(feature = "trade")]
            "trade" => parser::parse_trade_for_symbol(message, received_ts, symbol)
                .map(KucoinEvent::Trade)
                .into_iter()
                .collect(),
            #[cfg(feature = "orderbook")]
            "obu" => parser::parse_book_for_symbol(message, received_ts, symbol)
                .map(|book| KucoinEvent::L2Book(book.book))
                .into_iter()
                .collect(),
            #[cfg(feature = "candles")]
            "kline" => parser::parse_candle_for_symbol(message, received_ts, symbol)
                .map(KucoinEvent::Candle)
                .into_iter()
                .collect(),
            #[cfg(feature = "funding")]
            "funding-fee" => parser::parse_funding_for_symbol(message, received_ts, symbol)
                .map(KucoinEvent::Funding)
                .into_iter()
                .collect(),
            // One `mark-price` payload normalizes to three independent
            // categories under the same subscription.
            "mark-price" => {
                #[allow(unused_mut)] // one payload, up to three feature-gated events
                let mut events = Vec::new();
                #[cfg(feature = "markprice")]
                {
                    events.extend(
                        parser::parse_mark_price_for_symbol(message, received_ts, symbol)
                            .map(KucoinEvent::MarkPrice),
                    );
                }
                #[cfg(feature = "index")]
                {
                    events.extend(
                        parser::parse_index_price_for_symbol(message, received_ts, symbol)
                            .map(KucoinEvent::IndexPrice),
                    );
                }
                #[cfg(feature = "openinterest")]
                {
                    events.extend(
                        parser::parse_open_interest_for_symbol(message, received_ts, symbol)
                            .map(KucoinEvent::OpenInterest),
                    );
                }
                events
            }
            _ => Vec::new(),
        }
    }
}

fn topic_product(topic: &str) -> Option<parser::TradeType> {
    if topic.ends_with(".FUTURES") {
        Some(parser::TradeType::Futures)
    } else if topic.ends_with(".SPOT") {
        Some(parser::TradeType::Spot)
    } else if matches!(topic.split('.').next(), Some("mark-price" | "funding-fee")) {
        Some(parser::TradeType::Futures)
    } else {
        None
    }
}

fn fallback_exchange_symbol(symbol: &Symbol) -> String {
    match symbol.kind() {
        InstrumentKind::Spot => symbol.as_str().to_owned(),
        _ => {
            let contract = symbol
                .as_str()
                .strip_suffix("-PERP")
                .unwrap_or(symbol.as_str())
                .replace('-', "");
            format!("{contract}M")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{KucoinAdapter, KucoinControl, KucoinProduct};
    use crate::exchange::kucoin::Kucoin;
    use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
    use serde_json::Value;

    fn frame(frames: &[String], channel: &str) -> Value {
        frames
            .iter()
            .map(|frame| serde_json::from_str::<Value>(frame).expect("frame json"))
            .find(|frame| frame["channel"] == channel)
            .unwrap_or_else(|| panic!("missing {channel} frame: {frames:?}"))
    }

    #[test]
    fn routes_spot_and_futures_to_documented_push_hosts() {
        assert_eq!(
            KucoinAdapter::websocket_url(),
            "wss://x-push-spot.kucoin.com"
        );
        assert_eq!(
            KucoinAdapter::futures_websocket_url(),
            "wss://x-push-futures.kucoin.com"
        );
        let spot = Kucoin::new().ticker().symbol("BTC-USDT").build();
        assert_eq!(
            KucoinAdapter::subscription_url(&spot),
            "wss://x-push-spot.kucoin.com"
        );
        let perpetual = Kucoin::new().ticker().symbol("BTC-USDT-PERP").build();
        assert_eq!(
            KucoinAdapter::subscription_url(&perpetual),
            "wss://x-push-futures.kucoin.com"
        );
    }

    #[test]
    fn builds_spot_subscription_frames_with_symbol_arrays() {
        let feed = Kucoin::new()
            .ticker()
            .trade()
            .l2_book()
            .candles()
            .candles_interval("1m")
            .symbol("BTC-USDT")
            .exchange_symbol("BTC-USDT")
            .symbol("ETH-USDT")
            .exchange_symbol("ETH-USDT")
            .build();
        let frames = KucoinAdapter::subscription_messages(&feed).expect("frames");
        // `trade` is one frame per instrument; ticker/obu/kline keep the
        // documented `symbols` array.
        assert_eq!(frames.len(), 5);

        let ticker = frame(&frames, "ticker");
        assert_eq!(ticker["action"], "SUBSCRIBE");
        assert_eq!(ticker["tradeType"], "SPOT");
        assert_eq!(
            ticker["symbols"],
            serde_json::json!(["BTC-USDT", "ETH-USDT"])
        );
        let trades: Vec<serde_json::Value> = frames
            .iter()
            .map(|frame| serde_json::from_str::<serde_json::Value>(frame).expect("json"))
            .filter(|frame| frame["channel"] == "trade")
            .collect();
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0]["symbol"], "BTC-USDT");
        assert_eq!(trades[1]["symbol"], "ETH-USDT");
        assert!(trades.iter().all(|frame| frame.get("symbols").is_none()));
        let obu = frame(&frames, "obu");
        assert_eq!(obu["depth"], "increment@10ms");
        let kline = frame(&frames, "kline");
        assert_eq!(kline["interval"], "1min");
    }

    #[test]
    fn futures_mark_price_index_and_open_interest_share_one_frame() {
        let feed = Kucoin::new()
            .mark_price()
            .index()
            .open_interest()
            .funding()
            .symbol("BTC-USDT-PERP")
            .exchange_symbol("XBTUSDTM")
            .build();
        let frames = KucoinAdapter::subscription_messages(&feed).expect("frames");
        assert_eq!(frames.len(), 2, "{frames:?}");
        let mark = frame(&frames, "mark-price");
        assert_eq!(mark["tradeType"], "FUTURES");
        // `mark-price` rejects the batched array on the live service.
        assert_eq!(mark["symbol"], "XBTUSDTM");
        assert!(mark.get("symbols").is_none());
        assert_eq!(frame(&frames, "funding-fee")["symbols"][0], "XBTUSDTM");
    }

    #[test]
    fn snapshot_only_depths_reach_the_obu_frame() {
        for (level, wire) in [(1u16, "1"), (5, "5"), (50, "50")] {
            let feed = Kucoin::new()
                .l2_book()
                .l2_book_depth(level)
                .symbol("BTC-USDT")
                .exchange_symbol("BTC-USDT")
                .build();
            let frames = KucoinAdapter::subscription_messages(&feed).expect("frames");
            assert_eq!(frame(&frames, "obu")["depth"], wire);
            assert!(KucoinAdapter::l2_book_snapshot_only(Some(level)));
        }
        assert!(!KucoinAdapter::l2_book_snapshot_only(None));
        assert_eq!(KucoinAdapter::l2_depth_wire(None), Some("increment@10ms"));
        assert_eq!(KucoinAdapter::l2_depth_wire(Some(10)), None);
    }

    #[test]
    fn candle_intervals_follow_documented_labels_and_reject_six_hours_on_futures() {
        assert_eq!(
            KucoinAdapter::candle_interval_wire("1m", KucoinProduct::Spot),
            Some("1min")
        );
        assert_eq!(
            KucoinAdapter::candle_interval_wire("1M", KucoinProduct::Spot),
            Some("1month")
        );
        assert_eq!(
            KucoinAdapter::candle_interval_wire("6h", KucoinProduct::Spot),
            Some("6hour")
        );
        assert_eq!(
            KucoinAdapter::candle_interval_wire("6h", KucoinProduct::Futures),
            None
        );
        assert_eq!(
            KucoinAdapter::candle_interval_wire("7m", KucoinProduct::Spot),
            None
        );
    }

    #[test]
    fn parses_acknowledgement_and_welcome_controls() {
        assert_eq!(
            KucoinAdapter::parse_control_message(&serde_json::json!({
                "sessionId": "81f56400", "message": "welcome", "pingInterval": 18000
            }))
            .expect("welcome")
            .expect("ok"),
            KucoinControl::Welcome
        );
        assert_eq!(
            KucoinAdapter::parse_control_message(&serde_json::json!({"id": "1", "result": true}))
                .expect("ack")
                .expect("ok"),
            KucoinControl::Subscribed
        );
        assert!(
            KucoinAdapter::parse_control_message(&serde_json::json!({
                "id": "2", "result": false, "message": "invalid request data"
            }))
            .expect("ack")
            .is_err()
        );
        assert_eq!(
            KucoinAdapter::parse_control_message(&serde_json::json!({
                "id": "cf-heartbeat", "op": "pong", "timestamp": 1
            }))
            .expect("pong")
            .expect("ok"),
            KucoinControl::Pong
        );
        assert!(KucoinAdapter::parse_control_message(&serde_json::json!({})).is_none());
    }

    #[test]
    fn resubscribe_pair_uses_the_documented_obu_parameter() {
        let feed = Kucoin::new()
            .l2_book()
            .symbol("BTC-USDT-PERP")
            .exchange_symbol("XBTUSDTM")
            .build();
        let frames =
            KucoinAdapter::book_resubscribe_messages(&feed, &Symbol::perpetual("BTC", "USDT"))
                .expect("frames");
        assert_eq!(frames.len(), 2);
        let unsubscribe: Value = serde_json::from_str(&frames[0]).expect("json");
        let subscribe: Value = serde_json::from_str(&frames[1]).expect("json");
        assert_eq!(unsubscribe["action"], "UNSUBSCRIBE");
        assert_eq!(subscribe["action"], "SUBSCRIBE");
        assert_eq!(subscribe["symbols"], serde_json::json!(["XBTUSDTM"]));
        assert_eq!(subscribe["depth"], "increment@10ms");
    }

    #[cfg(feature = "markprice")]
    #[test]
    fn feed_context_splits_mark_price_into_three_events_and_filters_unknown_symbols() {
        let feed = Kucoin::new()
            .mark_price()
            .index()
            .open_interest()
            .symbol("BTC-USDT-PERP")
            .exchange_symbol("XBTUSDTM")
            .build();
        let message = serde_json::json!({
            "T": "mark-price", "P": 1731899129000_u64,
            "d": {"s": "XBTUSDTM", "mp": "90445", "ip": "90445", "oi": "23445", "ts": 1731899128999_u64}
        });
        let events = KucoinAdapter::parse_messages_for_feed(&feed, &message, 1.0);
        let mut mark = 0;
        let mut index = 0;
        let mut open_interest = 0;
        for event in &events {
            match event {
                super::KucoinEvent::MarkPrice(value) => {
                    assert_eq!(value.symbol.as_str(), "BTC-USDT-PERP");
                    mark += 1;
                }
                #[cfg(feature = "index")]
                super::KucoinEvent::IndexPrice(_) => index += 1,
                #[cfg(feature = "openinterest")]
                super::KucoinEvent::OpenInterest(_) => open_interest += 1,
                _ => panic!("unexpected event"),
            }
        }
        assert_eq!(mark, 1);
        assert_eq!(index, 1);
        assert_eq!(open_interest, 1);

        let unknown = serde_json::json!({
            "T": "mark-price", "d": {"s": "ETHUSDTM", "mp": "1", "ip": "1", "oi": "1"}
        });
        assert!(
            KucoinAdapter::parse_messages_for_feed(&feed, &unknown, 1.0).is_empty(),
            "unconfigured symbols must not be normalized"
        );
        assert_eq!(ExchangeId::Kucoin, feed.exchange);
    }
}
