//! Hyperliquid public WebSocket routing and subscription framing.
//!
//! One unauthenticated host serves every public channel:
//! `wss://api.hyperliquid.xyz/ws`. Subscriptions are
//! `{"method":"subscribe","subscription":{"type":..,"coin":..}}` and each
//! request is acknowledged by a `subscriptionResponse` frame; request errors
//! arrive on the `error` channel and are permanent configuration failures.
//! Liveness is the documented client `{"method":"ping"}` request answered by
//! `{"channel":"pong"}` (verified live 2026-11-04).
//!
//! The subscription `coin` is the perpetual name from `meta.universe[].name`
//! or the spot `spotMeta.universe[].name` value (`PURR/USDC` or `@N`). Coin
//! names are case-sensitive on the wire (`kPEPE` subscribes while `KPEPE`
//! closes the connection), so the catalog preserves the exact native spelling
//! and this adapter never re-cases it.

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
#[cfg(feature = "orderbook")]
use cryptofeed_orderbook::L1Book;
#[cfg(feature = "ticker")]
use cryptofeed_ticker::Ticker;
#[cfg(feature = "trade")]
use cryptofeed_trade::Trade;

pub struct HyperliquidAdapter;

pub const WS_URL: &str = "wss://api.hyperliquid.xyz/ws";

/// Documented maximum WebSocket subscriptions per connection.
pub const MAX_SUBSCRIPTIONS: usize = 1000;

/// Documented `l2Book` depth: 20 levels by default, 5 with `fast:true`.
pub const DEFAULT_BOOK_DEPTH: u16 = 20;
pub const FAST_BOOK_DEPTH: u16 = 5;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HyperliquidProduct {
    Spot,
    Perpetual,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HyperliquidControl {
    /// `subscriptionResponse` acknowledgement for one subscribe/unsubscribe.
    Ack,
    /// Documented heartbeat reply to the client `{"method":"ping"}` request.
    Pong,
}

pub enum HyperliquidEvent {
    #[cfg(feature = "candles")]
    Candle(Candle),
    #[cfg(feature = "funding")]
    Funding(Funding),
    #[cfg(feature = "index")]
    IndexPrice(IndexPrice),
    #[cfg(feature = "orderbook")]
    L1Book(L1Book),
    #[cfg(feature = "markprice")]
    MarkPrice(MarkPrice),
    #[cfg(feature = "openinterest")]
    OpenInterest(OpenInterest),
    #[cfg(feature = "ticker")]
    Ticker(Ticker),
    #[cfg(feature = "trade")]
    Trade(Trade),
}

impl HyperliquidAdapter {
    pub fn product_for_kind(kind: InstrumentKind) -> Option<HyperliquidProduct> {
        match kind {
            InstrumentKind::Spot => Some(HyperliquidProduct::Spot),
            InstrumentKind::Perpetual => Some(HyperliquidProduct::Perpetual),
            _ => None,
        }
    }

    pub fn product_for_feed(feed: &ExchangeFeed) -> Result<HyperliquidProduct> {
        let kind = feed.product_kind()?;
        Self::product_for_kind(kind)
            .ok_or_else(|| Error::UnsupportedCapability(format!("Hyperliquid {kind:?} product")))
    }

    pub fn websocket_url() -> &'static str {
        WS_URL
    }

    /// Legacy single-URL helper used by `planned_url`.
    pub fn subscription_url(_feed: &ExchangeFeed) -> String {
        WS_URL.to_owned()
    }

    /// Documented client liveness request; the server answers with
    /// `{"channel":"pong"}`.
    pub fn heartbeat_message() -> &'static str {
        r#"{"method":"ping"}"#
    }

    /// Requested L2 depth is documented as exactly 20 levels (default) or 5
    /// levels (`fast:true`); every other request fails preflight.
    pub fn l2_book_depth_supported(level: u16) -> bool {
        matches!(level, DEFAULT_BOOK_DEPTH | FAST_BOOK_DEPTH)
    }

    /// Normalized candle interval -> documented wire interval.
    pub fn candle_interval_wire(interval: &str) -> Option<&'static str> {
        parser::interval_wire(interval)
    }

    /// The documented `fast` flag is implied by a requested depth of 5.
    fn fast_book(feed: &ExchangeFeed) -> bool {
        feed.l2_book_depth == Some(FAST_BOOK_DEPTH)
    }

    /// Exact native `coin` for a normalized symbol. Coin spelling is
    /// case-sensitive, so the catalog value is used verbatim.
    pub fn coin_for_symbol(feed: &ExchangeFeed, symbol: &Symbol) -> Result<String> {
        feed.exchange_symbol_for(symbol).map(ToOwned::to_owned)
    }

    /// One documented subscription object for a channel/coin pair.
    fn channel_subscription(
        feed: &ExchangeFeed,
        channel: Channel,
        symbol: &Symbol,
    ) -> Result<Value> {
        let coin = Self::coin_for_symbol(feed, symbol)?;
        let subscription = match channel {
            Channel::Candles => {
                let interval =
                    Self::candle_interval_wire(&feed.candle_interval).ok_or_else(|| {
                        Error::UnsupportedCapability(format!(
                            "Hyperliquid candle interval {:?}",
                            feed.candle_interval
                        ))
                    })?;
                serde_json::json!({"type": "candle", "coin": coin, "interval": interval})
            }
            Channel::Ticker | Channel::L1Book => serde_json::json!({"type": "bbo", "coin": coin}),
            Channel::L2Book => {
                let mut subscription = serde_json::json!({"type": "l2Book", "coin": coin});
                if Self::fast_book(feed) {
                    subscription["fast"] = Value::Bool(true);
                }
                subscription
            }
            Channel::Trade => serde_json::json!({"type": "trades", "coin": coin}),
            // Spot `MarkPrice` rides the same `activeAssetCtx` subscription
            // type (the spot response channel is `activeSpotAssetCtx`).
            Channel::MarkPrice => serde_json::json!({"type": "activeAssetCtx", "coin": coin}),
            Channel::Funding | Channel::Index | Channel::OpenInterest => {
                if Self::product_for_feed(feed)? != HyperliquidProduct::Perpetual {
                    return Err(Error::UnsupportedCapability(format!(
                        "Hyperliquid spot channel {channel:?}"
                    )));
                }
                serde_json::json!({"type": "activeAssetCtx", "coin": coin})
            }
            _ => {
                return Err(Error::UnsupportedCapability(format!(
                    "Hyperliquid channel {channel:?}"
                )));
            }
        };
        Ok(subscription)
    }

    /// Subscription frames for the single planned connection.
    pub fn subscription_messages(feed: &ExchangeFeed) -> Result<Vec<String>> {
        Self::messages(feed, "subscribe")
    }

    /// Documented per-coin unsubscribe frames; the runtime's recovery policy is
    /// reconnect + fresh snapshot, so these are exposed for explicit callers
    /// and tests rather than used during a normal session.
    pub fn unsubscription_messages(feed: &ExchangeFeed) -> Result<Vec<String>> {
        Self::messages(feed, "unsubscribe")
    }

    fn messages(feed: &ExchangeFeed, method: &str) -> Result<Vec<String>> {
        let mut frames = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for channel in &feed.channels {
            for symbol in &feed.symbols {
                if !feed.subscribes(*channel, symbol) {
                    continue;
                }
                let subscription = Self::channel_subscription(feed, *channel, symbol)?;
                let frame = serde_json::json!({
                    "method": method,
                    "subscription": subscription,
                })
                .to_string();
                if seen.insert(frame.clone()) {
                    frames.push(frame);
                }
            }
        }
        Ok(frames)
    }

    /// Classifies acknowledgements, request errors and the documented
    /// heartbeat reply before payload parsing.
    pub fn parse_control_message(message: &Value) -> Option<Result<HyperliquidControl>> {
        match parser::channel(message)? {
            "subscriptionResponse" => Some(Ok(HyperliquidControl::Ack)),
            // Request parse/validation failures are permanent configuration
            // problems. The server text is not retained as it echoes the
            // rejected request.
            "error" => Some(Err(Error::Subscription(
                "hyperliquid websocket error".to_owned(),
            ))),
            "pong" => Some(Ok(HyperliquidControl::Pong)),
            _ => None,
        }
    }

    /// Correlates a `subscriptionResponse` (or the original request) with the
    /// pending subscription key used by readiness.
    pub fn subscription_key(subscription: &Value) -> Option<String> {
        let kind = subscription.get("type").and_then(Value::as_str)?;
        let coin = subscription.get("coin").and_then(Value::as_str)?;
        let interval = subscription
            .get("interval")
            .and_then(Value::as_str)
            .unwrap_or("");
        Some(format!("hl:{kind}:{coin}:{interval}"))
    }

    /// Readiness key for a `subscriptionResponse` frame.
    pub fn acknowledgement_key(message: &Value) -> Option<String> {
        Self::subscription_key(message.pointer("/data/subscription")?)
    }

    pub fn symbol_for_native<'a>(feed: &'a ExchangeFeed, coin: &str) -> Option<&'a Symbol> {
        feed.symbols.iter().enumerate().find_map(|(index, symbol)| {
            let exchange_symbol = feed
                .exchange_symbols
                .get(index)
                .map(String::as_str)
                .unwrap_or_else(|| symbol.as_str());
            exchange_symbol.eq_ignore_ascii_case(coin).then_some(symbol)
        })
    }

    /// Parses one non-book market frame into normalized events. Documented
    /// null `bbo` sides produce no quote; malformed frames for a configured
    /// channel fail closed.
    pub fn parse_messages_for_feed(
        feed: &ExchangeFeed,
        message: &Value,
        received_ts: f64,
    ) -> Result<Vec<HyperliquidEvent>> {
        let Some(channel) = parser::channel(message) else {
            return Ok(Vec::new());
        };
        match channel {
            // Whole-book snapshots are applied by the runtime's book-sync path.
            "l2Book" => Ok(Vec::new()),
            "trades" => Self::parse_trades(feed, message, received_ts),
            "bbo" => Self::parse_bbo(feed, message, received_ts),
            "candle" => Self::parse_candles(feed, message, received_ts),
            "activeAssetCtx" | "activeSpotAssetCtx" => {
                Self::parse_active_asset_ctx(feed, channel, message, received_ts)
            }
            _ => Ok(Vec::new()),
        }
    }

    fn payload(message: &Value) -> Result<&Value> {
        parser::data(message)
            .ok_or_else(|| Error::Parse("Hyperliquid data frame has no payload".to_owned()))
    }

    fn symbol_for_payload<'a>(feed: &'a ExchangeFeed, data: &Value) -> Result<Option<&'a Symbol>> {
        let coin = parser::data_coin(data)
            .ok_or_else(|| Error::Parse("Hyperliquid data frame has no coin".to_owned()))?;
        Ok(Self::symbol_for_native(feed, coin))
    }

    fn parse_trades(
        feed: &ExchangeFeed,
        message: &Value,
        received_ts: f64,
    ) -> Result<Vec<HyperliquidEvent>> {
        let data = Self::payload(message)?;
        let rows = data
            .as_array()
            .ok_or_else(|| Error::Parse("Hyperliquid trades payload is not an array".to_owned()))?;
        let mut events = Vec::new();
        for row in rows {
            let Some(symbol) = Self::symbol_for_payload(feed, row)? else {
                continue;
            };
            if !feed.subscribes(Channel::Trade, symbol) {
                continue;
            }
            #[cfg(feature = "trade")]
            {
                let trade = parser::parse_trade(row, received_ts, symbol).ok_or_else(|| {
                    Error::Parse("failed to parse Hyperliquid trade row".to_owned())
                })?;
                events.push(HyperliquidEvent::Trade(trade));
            }
        }
        Ok(events)
    }

    fn parse_bbo(
        feed: &ExchangeFeed,
        message: &Value,
        received_ts: f64,
    ) -> Result<Vec<HyperliquidEvent>> {
        let data = Self::payload(message)?;
        let Some(symbol) = Self::symbol_for_payload(feed, data)? else {
            return Ok(Vec::new());
        };
        let mut events = Vec::new();
        #[cfg(any(feature = "ticker", feature = "orderbook"))]
        {
            let quote = parser::parse_bbo(data, received_ts).ok_or_else(|| {
                Error::Parse("failed to parse Hyperliquid bbo payload".to_owned())
            })?;
            // A documented null side must not fabricate a one-sided quote.
            if let (Some(bid), Some(ask)) = (quote.bid, quote.ask) {
                #[cfg(feature = "ticker")]
                if feed.subscribes(Channel::Ticker, symbol) {
                    events.push(HyperliquidEvent::Ticker(Ticker {
                        exchange: cryptofeed_core::exchange::ExchangeId::Hyperliquid,
                        symbol: symbol.clone(),
                        bid: bid.0,
                        ask: ask.0,
                        exchange_ts: quote.exchange_ts,
                        received_ts: quote.received_ts,
                        implied_volatility: None,
                    }));
                }
                #[cfg(feature = "orderbook")]
                if feed.subscribes(Channel::L1Book, symbol) {
                    events.push(HyperliquidEvent::L1Book(L1Book {
                        exchange: cryptofeed_core::exchange::ExchangeId::Hyperliquid,
                        symbol: symbol.clone(),
                        bid: cryptofeed_orderbook::PriceLevel {
                            price: bid.0,
                            amount: bid.1,
                        },
                        ask: cryptofeed_orderbook::PriceLevel {
                            price: ask.0,
                            amount: ask.1,
                        },
                        exchange_ts: quote.exchange_ts,
                        received_ts: quote.received_ts,
                    }));
                }
            }
        }
        #[cfg(not(any(feature = "ticker", feature = "orderbook")))]
        let _ = symbol;
        Ok(events)
    }

    fn parse_candles(
        feed: &ExchangeFeed,
        message: &Value,
        received_ts: f64,
    ) -> Result<Vec<HyperliquidEvent>> {
        let data = Self::payload(message)?;
        let mut events = Vec::new();
        #[cfg(feature = "candles")]
        {
            let interval = Self::candle_interval_wire(&feed.candle_interval).ok_or_else(|| {
                Error::UnsupportedCapability(format!(
                    "Hyperliquid candle interval {:?}",
                    feed.candle_interval
                ))
            })?;
            // The documented data format is `Candle[]`; the live shape verified
            // 2026-11-04 is a single object. Both are accepted. Candle payloads
            // carry the coin as `s`, not `coin`.
            let rows = match data {
                Value::Array(rows) => rows.as_slice(),
                other => std::slice::from_ref(other),
            };
            for row in rows {
                let coin = row
                    .get("s")
                    .and_then(Value::as_str)
                    .ok_or_else(|| Error::Parse("Hyperliquid candle has no coin".to_owned()))?;
                let Some(symbol) = Self::symbol_for_native(feed, coin) else {
                    continue;
                };
                if !feed.subscribes(Channel::Candles, symbol) {
                    continue;
                }
                let candle = parser::parse_candle(row, interval, received_ts, symbol)
                    .ok_or_else(|| Error::Parse("failed to parse Hyperliquid candle".to_owned()))?;
                events.push(HyperliquidEvent::Candle(candle));
            }
        }
        #[cfg(not(feature = "candles"))]
        {
            let _ = data;
        }
        Ok(events)
    }

    fn parse_active_asset_ctx(
        feed: &ExchangeFeed,
        channel: &str,
        message: &Value,
        received_ts: f64,
    ) -> Result<Vec<HyperliquidEvent>> {
        let data = Self::payload(message)?;
        let Some(symbol) = Self::symbol_for_payload(feed, data)? else {
            return Ok(Vec::new());
        };
        let mut events = Vec::new();
        #[cfg(any(
            feature = "funding",
            feature = "openinterest",
            feature = "markprice",
            feature = "index"
        ))]
        {
            let product = Self::product_for_feed(feed)?;
            let context_product = match (channel, product) {
                ("activeAssetCtx", HyperliquidProduct::Perpetual) => {
                    parser::AssetCtxProduct::Perpetual
                }
                // Spot is documented as `WsActiveSpotAssetCtx`; the live
                // channel name is `activeSpotAssetCtx`. Both names carry the
                // spot context.
                ("activeAssetCtx" | "activeSpotAssetCtx", HyperliquidProduct::Spot) => {
                    parser::AssetCtxProduct::Spot
                }
                _ => {
                    return Err(Error::Parse(
                        "Hyperliquid asset-context channel does not match the product".to_owned(),
                    ));
                }
            };
            let context =
                parser::parse_asset_ctx(data, context_product, received_ts).ok_or_else(|| {
                    Error::Parse("failed to parse Hyperliquid activeAssetCtx".to_owned())
                })?;
            #[cfg(feature = "funding")]
            if feed.subscribes(Channel::Funding, symbol) {
                if let Some(rate) = context.funding {
                    events.push(HyperliquidEvent::Funding(Funding {
                        exchange: cryptofeed_core::exchange::ExchangeId::Hyperliquid,
                        symbol: symbol.clone(),
                        mark_price: context.mark_px,
                        rate: Some(rate),
                        // The payload carries no settlement time or predicted
                        // rate; neither is invented.
                        next_funding_time: None,
                        predicted_rate: None,
                        exchange_ts: context.received_ts,
                        received_ts: context.received_ts,
                    }));
                }
            }
            #[cfg(feature = "openinterest")]
            if feed.subscribes(Channel::OpenInterest, symbol) {
                if let Some(open_interest) = context.open_interest {
                    events.push(HyperliquidEvent::OpenInterest(OpenInterest {
                        exchange: cryptofeed_core::exchange::ExchangeId::Hyperliquid,
                        symbol: symbol.clone(),
                        open_interest,
                        coin_quantity: None,
                        value_usd: None,
                        exchange_ts: context.received_ts,
                        received_ts: context.received_ts,
                    }));
                }
            }
            #[cfg(feature = "markprice")]
            if feed.subscribes(Channel::MarkPrice, symbol) {
                if let Some(price) = context.mark_px {
                    events.push(HyperliquidEvent::MarkPrice(MarkPrice {
                        exchange: cryptofeed_core::exchange::ExchangeId::Hyperliquid,
                        symbol: symbol.clone(),
                        price,
                        next_funding_time: None,
                        predicted_rate: None,
                        exchange_ts: context.received_ts,
                        received_ts: context.received_ts,
                    }));
                }
            }
            #[cfg(feature = "index")]
            if feed.subscribes(Channel::Index, symbol) {
                if let Some(price) = context.oracle_px {
                    events.push(HyperliquidEvent::IndexPrice(IndexPrice {
                        exchange: cryptofeed_core::exchange::ExchangeId::Hyperliquid,
                        symbol: symbol.clone(),
                        price,
                        open_24h: None,
                        high_24h: None,
                        low_24h: None,
                        exchange_ts: context.received_ts,
                        received_ts: context.received_ts,
                    }));
                }
            }
        }
        #[cfg(not(any(
            feature = "funding",
            feature = "openinterest",
            feature = "markprice",
            feature = "index"
        )))]
        {
            let _ = (channel, symbol);
        }
        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        FAST_BOOK_DEPTH, HyperliquidAdapter, HyperliquidControl, HyperliquidProduct, WS_URL,
    };
    use crate::exchange::hyperliquid::Hyperliquid;
    use serde_json::Value;

    #[test]
    fn perpetual_subscriptions_use_documented_framing() {
        let feed = Hyperliquid::new()
            .ticker()
            .l1_book()
            .trade()
            .l2_book()
            .candles()
            .candles_interval("5m")
            .funding()
            .open_interest()
            .mark_price()
            .index()
            .symbol("BTC-USD-PERP")
            .exchange_symbol("BTC")
            .build();
        assert_eq!(HyperliquidAdapter::subscription_url(&feed), WS_URL);
        let frames = HyperliquidAdapter::subscription_messages(&feed).expect("frames");
        let subscriptions: Vec<Value> = frames
            .iter()
            .map(|frame| {
                let value: Value = serde_json::from_str(frame).expect("json");
                assert_eq!(value["method"], "subscribe");
                value["subscription"].clone()
            })
            .collect();
        assert!(subscriptions.contains(&serde_json::json!({"type":"bbo","coin":"BTC"})));
        assert!(subscriptions.contains(&serde_json::json!({"type":"trades","coin":"BTC"})));
        assert!(subscriptions.contains(&serde_json::json!({"type":"l2Book","coin":"BTC"})));
        assert!(
            subscriptions
                .contains(&serde_json::json!({"type":"candle","coin":"BTC","interval":"5m"}))
        );
        // One shared activeAssetCtx subscription feeds all four split channels.
        assert_eq!(
            subscriptions
                .iter()
                .filter(|value| value["type"] == "activeAssetCtx")
                .count(),
            1
        );
        let unsubscribe =
            HyperliquidAdapter::unsubscription_messages(&feed).expect("unsubscribe frames");
        assert_eq!(unsubscribe.len(), frames.len());
        assert!(
            unsubscribe
                .iter()
                .all(|frame| frame.contains("\"method\":\"unsubscribe\""))
        );
    }

    #[test]
    fn fast_depth_is_the_documented_five_level_flag() {
        let feed = Hyperliquid::new()
            .l2_book()
            .l2_book_depth(FAST_BOOK_DEPTH)
            .symbol("BTC-USD-PERP")
            .exchange_symbol("BTC")
            .build();
        let frames = HyperliquidAdapter::subscription_messages(&feed).expect("frames");
        let value: Value = serde_json::from_str(&frames[0]).expect("json");
        assert_eq!(value["subscription"]["fast"], true);
        assert_eq!(value["subscription"]["coin"], "BTC");
        assert!(HyperliquidAdapter::l2_book_depth_supported(20));
        assert!(HyperliquidAdapter::l2_book_depth_supported(5));
        assert!(!HyperliquidAdapter::l2_book_depth_supported(10));
        assert!(!HyperliquidAdapter::l2_book_depth_supported(50));
    }

    #[test]
    fn spot_subscriptions_use_the_native_coin_identity() {
        let feed = Hyperliquid::new()
            .ticker()
            .trade()
            .l2_book()
            .candles()
            .symbol("HYPE-USDC")
            .exchange_symbol("@107")
            .build();
        let frames = HyperliquidAdapter::subscription_messages(&feed).expect("frames");
        let subscriptions: Vec<Value> = frames
            .iter()
            .map(|frame| {
                serde_json::from_str::<Value>(frame).expect("json")["subscription"].clone()
            })
            .collect();
        assert!(subscriptions.contains(&serde_json::json!({"type":"bbo","coin":"@107"})));
        assert!(subscriptions.contains(&serde_json::json!({"type":"trades","coin":"@107"})));
        assert!(subscriptions.contains(&serde_json::json!({"type":"l2Book","coin":"@107"})));
        // Spot has no derivative context channels; preflight rejects them, and
        // the adapter never generates an activeAssetCtx subscription for spot.
        let spot_funding = Hyperliquid::new()
            .funding()
            .symbol("HYPE-USDC")
            .exchange_symbol("@107")
            .build();
        assert!(HyperliquidAdapter::subscription_messages(&spot_funding).is_err());
    }

    #[test]
    fn coin_spelling_is_preserved_for_case_sensitive_perpetuals() {
        let feed = Hyperliquid::new()
            .ticker()
            .symbol("KPEPE-USD-PERP")
            .exchange_symbol("kPEPE")
            .build();
        assert_eq!(
            HyperliquidAdapter::coin_for_symbol(&feed, &feed.symbols[0]).unwrap(),
            "kPEPE"
        );
        let frames = HyperliquidAdapter::subscription_messages(&feed).expect("frames");
        assert!(frames[0].contains("\"coin\":\"kPEPE\""));
    }

    #[test]
    fn classifies_acks_errors_and_the_documented_pong() {
        assert_eq!(
            HyperliquidAdapter::parse_control_message(&serde_json::json!({
                "channel":"subscriptionResponse",
                "data":{"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}
            }))
            .expect("ack")
            .expect("ok"),
            HyperliquidControl::Ack
        );
        assert!(
            HyperliquidAdapter::parse_control_message(&serde_json::json!({
                "channel":"error",
                "data":"Error parsing JSON into valid websocket request"
            }))
            .expect("error")
            .is_err()
        );
        assert_eq!(
            HyperliquidAdapter::parse_control_message(&serde_json::json!({"channel":"pong"}))
                .expect("pong")
                .expect("ok"),
            HyperliquidControl::Pong
        );
        assert!(
            HyperliquidAdapter::parse_control_message(&serde_json::json!({
                "channel":"trades","data":[]
            }))
            .is_none()
        );
        assert_eq!(
            HyperliquidAdapter::acknowledgement_key(&serde_json::json!({
                "channel":"subscriptionResponse",
                "data":{"method":"subscribe","subscription":{
                    "type":"l2Book","coin":"BTC","nSigFigs":null,"mantissa":null,"fast":false}}
            }))
            .as_deref(),
            Some("hl:l2Book:BTC:")
        );
        assert_eq!(
            HyperliquidAdapter::subscription_key(
                &serde_json::json!({"type":"candle","coin":"BTC","interval":"1m"})
            )
            .as_deref(),
            Some("hl:candle:BTC:1m")
        );
    }

    #[test]
    fn products_map_only_spot_and_perpetual() {
        use cryptofeed_core::symbol::InstrumentKind;
        assert_eq!(
            HyperliquidAdapter::product_for_kind(InstrumentKind::Spot),
            Some(HyperliquidProduct::Spot)
        );
        assert_eq!(
            HyperliquidAdapter::product_for_kind(InstrumentKind::Perpetual),
            Some(HyperliquidProduct::Perpetual)
        );
        assert_eq!(
            HyperliquidAdapter::product_for_kind(InstrumentKind::Futures),
            None
        );
        assert_eq!(
            HyperliquidAdapter::product_for_kind(InstrumentKind::Option),
            None
        );
    }

    #[cfg(feature = "trade")]
    #[test]
    fn parses_trade_arrays_and_fails_closed_on_malformed_rows() {
        let feed = Hyperliquid::new()
            .trade()
            .symbol("BTC-USD-PERP")
            .exchange_symbol("BTC")
            .build();
        let message = serde_json::json!({
            "channel":"trades",
            "data":[
                {"coin":"BTC","side":"A","px":"78468.0","sz":"0.1",
                 "time":1_793_782_868_033_i64,"hash":"0x00","tid":1_i64,"users":["a","b"]},
                {"coin":"BTC","side":"B","px":"78469.0","sz":"0.2",
                 "time":1_793_782_868_100_i64,"hash":"0x01","tid":2_i64,"users":["a","b"]}
            ]
        });
        let events =
            HyperliquidAdapter::parse_messages_for_feed(&feed, &message, 2.0).expect("events");
        assert_eq!(events.len(), 2);
        let malformed = serde_json::json!({
            "channel":"trades",
            "data":[{"coin":"BTC","side":"X","px":"1","sz":"1","time":1,"tid":1}]
        });
        assert!(HyperliquidAdapter::parse_messages_for_feed(&feed, &malformed, 2.0).is_err());
    }

    #[cfg(all(feature = "ticker", feature = "orderbook"))]
    #[test]
    fn bbo_emits_ticker_and_l1_only_when_both_sides_are_present() {
        let feed = Hyperliquid::new()
            .ticker()
            .l1_book()
            .symbol("PURR-USDC")
            .exchange_symbol("PURR/USDC")
            .build();
        let both = serde_json::json!({
            "channel":"bbo",
            "data":{"coin":"PURR/USDC","time":1_793_782_877_039_i64,
                "bbo":[{"px":"0.11525","sz":"1202.0","n":1},
                       {"px":"0.11552","sz":"728.0","n":1}]}
        });
        let events =
            HyperliquidAdapter::parse_messages_for_feed(&feed, &both, 2.0).expect("bbo events");
        assert_eq!(events.len(), 2);
        assert!(
            events
                .iter()
                .any(|event| matches!(event, super::HyperliquidEvent::Ticker(_)))
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event, super::HyperliquidEvent::L1Book(_)))
        );
        // A documented null side yields no quote at all, not a fabricated one.
        for null_side in [0, 1] {
            let mut bbo = vec![
                serde_json::json!({"px":"1","sz":"2","n":1}),
                serde_json::json!({"px":"3","sz":"4","n":1}),
            ];
            bbo[null_side] = Value::Null;
            let message = serde_json::json!({
                "channel":"bbo",
                "data":{"coin":"PURR/USDC","time":1_i64,"bbo":bbo}
            });
            let events = HyperliquidAdapter::parse_messages_for_feed(&feed, &message, 2.0)
                .expect("one-sided bbo");
            assert!(events.is_empty());
        }
    }

    #[cfg(feature = "candles")]
    #[test]
    fn parses_live_candle_object_and_documented_array_shapes() {
        let feed = Hyperliquid::new()
            .candles()
            .candles_interval("1m")
            .symbol("BTC-USD-PERP")
            .exchange_symbol("BTC")
            .build();
        let row = serde_json::json!({
            "t":1_793_782_860_000_i64,"T":1_793_782_919_999_i64,"s":"BTC","i":"1m",
            "o":"78469.0","c":"78451.0","h":"78469.0","l":"78451.0","v":"11.58778","n":86
        });
        let object = serde_json::json!({"channel":"candle","data":row.clone()});
        assert_eq!(
            HyperliquidAdapter::parse_messages_for_feed(&feed, &object, 2.0)
                .expect("object candle")
                .len(),
            1
        );
        let array = serde_json::json!({"channel":"candle","data":[row]});
        assert_eq!(
            HyperliquidAdapter::parse_messages_for_feed(&feed, &array, 2.0)
                .expect("array candle")
                .len(),
            1
        );
        let wrong_interval = serde_json::json!({"channel":"candle","data":{
            "t":1_i64,"s":"BTC","i":"5m","o":"1","c":"1","h":"1","l":"1","v":"1","n":1}});
        assert!(HyperliquidAdapter::parse_messages_for_feed(&feed, &wrong_interval, 2.0).is_err());
    }

    #[cfg(all(
        feature = "funding",
        feature = "openinterest",
        feature = "markprice",
        feature = "index"
    ))]
    #[test]
    fn splits_perpetual_asset_context_and_spot_mark_price() {
        let perp = Hyperliquid::new()
            .funding()
            .open_interest()
            .mark_price()
            .index()
            .symbol("BTC-USD-PERP")
            .exchange_symbol("BTC")
            .build();
        let message = serde_json::json!({
            "channel":"activeAssetCtx",
            "data":{"coin":"BTC","ctx":{
                "funding":"0.0000125","openInterest":"39370.73328",
                "oraclePx":"78490.4","markPx":"78464.8"}}
        });
        let events =
            HyperliquidAdapter::parse_messages_for_feed(&perp, &message, 2.0).expect("ctx");
        assert_eq!(events.len(), 4);
        assert!(
            events
                .iter()
                .any(|event| matches!(event, super::HyperliquidEvent::Funding(_)))
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event, super::HyperliquidEvent::OpenInterest(_)))
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event, super::HyperliquidEvent::MarkPrice(_)))
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event, super::HyperliquidEvent::IndexPrice(_)))
        );

        let spot = Hyperliquid::new()
            .mark_price()
            .symbol("PURR-USDC")
            .exchange_symbol("PURR/USDC")
            .build();
        let message = serde_json::json!({
            "channel":"activeSpotAssetCtx",
            "data":{"coin":"PURR/USDC","ctx":{"markPx":"0.11552","midPx":"0.115385"}}
        });
        let events =
            HyperliquidAdapter::parse_messages_for_feed(&spot, &message, 2.0).expect("spot ctx");
        assert_eq!(events.len(), 1);
        assert!(
            events
                .iter()
                .any(|event| matches!(event, super::HyperliquidEvent::MarkPrice(_)))
        );
        // `activeAssetCtx` is documented for spot as well as `activeSpotAssetCtx`.
        assert!(
            HyperliquidAdapter::parse_messages_for_feed(
                &spot,
                &serde_json::json!({
                    "channel":"activeAssetCtx","data":{"coin":"PURR/USDC","ctx":{"markPx":"1"}}
                }),
                2.0
            )
            .is_ok()
        );
        // The spot context channel is not accepted for a perpetual feed.
        assert!(
            HyperliquidAdapter::parse_messages_for_feed(
                &perp,
                &serde_json::json!({
                    "channel":"activeSpotAssetCtx","data":{"coin":"BTC","ctx":{"markPx":"1"}}
                }),
                2.0
            )
            .is_err()
        );
        assert!(
            HyperliquidAdapter::parse_messages_for_feed(
                &perp,
                &serde_json::json!({
                    "channel":"activeSpotAssetCtx","data":{"coin":"BTC","ctx":{"markPx":"1"}}
                }),
                2.0
            )
            .is_err()
        );
    }

    #[test]
    fn native_symbol_lookup_is_case_insensitive_but_preserves_the_catalog_spelling() {
        let feed = Hyperliquid::new()
            .ticker()
            .symbol("KPEPE-USD-PERP")
            .exchange_symbol("kPEPE")
            .build();
        assert!(HyperliquidAdapter::symbol_for_native(&feed, "kPEPE").is_some());
        assert!(HyperliquidAdapter::symbol_for_native(&feed, "KPEPE").is_some());
        assert!(HyperliquidAdapter::symbol_for_native(&feed, "OTHER").is_none());
    }
}
