use super::parser;
use crate::exchange::ExchangeFeed;
use cryptofeed_core::{
    error::{Error, Result},
    exchange::Channel,
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

pub struct OkxAdapter;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OkxProduct {
    Spot,
    Swap,
    Futures,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OkxControl {
    Pong,
    Subscribed,
}

pub enum OkxEvent {
    #[cfg(feature = "candles")]
    Candle(Candle),
    #[cfg(feature = "funding")]
    Funding(Funding),
    #[cfg(feature = "liquidations")]
    Liquidation(Liquidation),
    #[cfg(feature = "markprice")]
    MarkPrice(MarkPrice),
    #[cfg(feature = "openinterest")]
    OpenInterest(OpenInterest),
    #[cfg(feature = "index")]
    IndexPrice(IndexPrice),
    #[cfg(feature = "orderbook")]
    L1Book(L1Book),
    #[cfg(feature = "orderbook")]
    L2Book(L2Book),
    #[cfg(feature = "ticker")]
    Ticker(Ticker),
    #[cfg(feature = "trade")]
    Trade(Trade),
}

impl OkxAdapter {
    pub const IDLE_TIMEOUT_SECS: u64 = 30;
    pub fn websocket_url() -> &'static str {
        "wss://ws.okx.com/ws/v5/public"
    }

    pub fn subscription_url(feed: &ExchangeFeed) -> String {
        if !feed.channels.is_empty()
            && feed
                .channels
                .iter()
                .all(|channel| matches!(channel, Channel::Candles))
        {
            "wss://ws.okx.com/ws/v5/business".to_owned()
        } else {
            Self::websocket_url().to_owned()
        }
    }

    pub fn subscription_urls(feed: &ExchangeFeed) -> Vec<String> {
        let has_candles = feed.channels.contains(&Channel::Candles);
        let has_public = feed
            .channels
            .iter()
            .any(|channel| !matches!(channel, Channel::Candles));
        let mut urls = Vec::with_capacity(2);
        if has_public || !has_candles {
            urls.push(Self::websocket_url().to_owned());
        }
        if has_candles {
            urls.push("wss://ws.okx.com/ws/v5/business".to_owned());
        }
        urls
    }

    pub fn product_for_exchange_symbol(symbol: &str) -> OkxProduct {
        if symbol.ends_with("-SWAP") {
            OkxProduct::Swap
        } else if symbol.split('-').count() >= 3 {
            OkxProduct::Futures
        } else {
            OkxProduct::Spot
        }
    }

    pub fn heartbeat_message() -> &'static str {
        "ping"
    }

    /// Normalized candle interval to the OKX wire form (`candle{wire}`).
    /// Official bar values (verified 2026-08-06): `1s` `1m` `3m` `5m`
    /// `15m` `30m` `1H` `2H` `4H` `6H` `12H` `1D` `2D` `3D` `5D` `1W`
    /// `1M` `3M` plus `utc` variants; the normalized vocabulary covers the
    /// cross-exchange subset.
    pub fn candle_interval_wire(interval: &str) -> Option<&'static str> {
        match interval {
            "1m" => Some("1m"),
            "3m" => Some("3m"),
            "5m" => Some("5m"),
            "15m" => Some("15m"),
            "30m" => Some("30m"),
            "1h" => Some("1H"),
            "2h" => Some("2H"),
            "4h" => Some("4H"),
            "6h" => Some("6H"),
            "12h" => Some("12H"),
            "1d" => Some("1D"),
            "1w" => Some("1W"),
            "1M" => Some("1M"),
            "3M" => Some("3M"),
            _ => None,
        }
    }

    /// L2 channel name for the requested depth level. `None` keeps the full
    /// `books` channel. The official partial book is `books5` (5); the
    /// tick-by-tick `books50-l2-tbt` (50) and `books-l2-tbt` (400) channels
    /// are VIP4+-gated (error 64003 otherwise, verified 2026-08-06) and are
    /// NOT wired into book sync — subscribing would silently drop every push,
    /// so they stay rejected at preflight until a live capture exists to
    /// wire the snapshot-replacement path.
    pub fn l2_book_channel(depth: Option<u16>) -> &'static str {
        match depth {
            Some(5) => "books5",
            Some(50) => "books50-l2-tbt",
            Some(400) => "books-l2-tbt",
            _ => "books",
        }
    }

    pub fn l2_book_depth_supported(level: u16) -> bool {
        level == 5
    }

    pub fn parse_control_text(text: &str) -> Option<Result<OkxControl>> {
        (text == "pong").then_some(Ok(OkxControl::Pong))
    }

    pub fn parse_control_message(message: &Value) -> Option<Result<OkxControl>> {
        let event = message.get("event")?.as_str()?;
        match event {
            "subscribe" => Some(Ok(OkxControl::Subscribed)),
            "error" => {
                let code = message
                    .get("code")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown");
                let detail = message
                    .get("msg")
                    .and_then(Value::as_str)
                    .unwrap_or("request rejected");
                Some(Err(Error::Parse(format!(
                    "okx websocket error {code}: {detail}"
                ))))
            }
            _ => None,
        }
    }

    pub fn subscription_message(feed: &ExchangeFeed) -> String {
        let symbols: Vec<String> = if feed.exchange_symbols.is_empty() {
            feed.symbols
                .iter()
                .map(|symbol| symbol.as_str().to_owned())
                .collect()
        } else {
            feed.exchange_symbols.clone()
        };
        let mut args: Vec<Value> = Vec::new();
        let mut inst_type_subscribed = false;
        for (index, inst_id) in symbols.into_iter().enumerate() {
            for channel in &feed.channels {
                if !feed
                    .symbols
                    .get(index)
                    .is_some_and(|symbol| feed.subscribes(*channel, symbol))
                {
                    continue;
                }
                let channel = match channel {
                    Channel::Candles => format!(
                        "candle{}",
                        Self::candle_interval_wire(&feed.candle_interval).unwrap_or("1m")
                    ),
                    Channel::Ticker => "tickers".to_owned(),
                    Channel::Trade => "trades".to_owned(),
                    Channel::L2Book => Self::l2_book_channel(feed.l2_book_depth).to_owned(),
                    Channel::Funding => "funding-rate".to_owned(),
                    // `liquidation-orders` is scoped by `instType`, not by
                    // `instId` (verified live 2026-08-06; instId arguments
                    // are rejected with 60018), so it is subscribed once per
                    // feed product.
                    Channel::Liquidations => {
                        if inst_type_subscribed {
                            continue;
                        }
                        inst_type_subscribed = true;
                        args.push(serde_json::json!({
                            "channel": "liquidation-orders",
                            "instType": Self::inst_type_for_feed(feed),
                        }));
                        continue;
                    }
                    Channel::MarkPrice => "mark-price".to_owned(),
                    Channel::OpenInterest => "open-interest".to_owned(),
                    Channel::Index => "index-tickers".to_owned(),
                    Channel::L1Book => "bbo-tbt".to_owned(),
                    // Future channels are not subscribed.
                    _ => String::new(),
                };
                let subscription_id = if channel == "index-tickers" {
                    index_symbol(&inst_id)
                } else {
                    inst_id.clone()
                };
                let arg = serde_json::json!({ "channel": channel, "instId": subscription_id });
                if !args.contains(&arg) {
                    args.push(arg);
                }
            }
        }

        serde_json::json!({
            "op": "subscribe",
            "args": args,
        })
        .to_string()
    }

    /// `instType` for the instType-scoped `liquidation-orders` subscription.
    /// Uses the feed product kind because MARGIN and SPOT instIds share the
    /// same native form (`BTC-USDT`).
    pub fn inst_type_for_feed(feed: &ExchangeFeed) -> &'static str {
        match feed
            .symbols
            .first()
            .map(|symbol| symbol.kind())
            .unwrap_or(cryptofeed_core::symbol::InstrumentKind::Spot)
        {
            cryptofeed_core::symbol::InstrumentKind::Margin => "MARGIN",
            cryptofeed_core::symbol::InstrumentKind::Option => "OPTION",
            cryptofeed_core::symbol::InstrumentKind::Perpetual => "SWAP",
            cryptofeed_core::symbol::InstrumentKind::Futures => "FUTURES",
            cryptofeed_core::symbol::InstrumentKind::Spot
            | cryptofeed_core::symbol::InstrumentKind::Unknown => "SPOT",
            _ => "SPOT",
        }
    }

    pub fn parse_message(message: &Value, received_ts: f64) -> Option<OkxEvent> {
        Self::parse_messages(message, received_ts)
            .into_iter()
            .next()
    }

    /// Parses like [`Self::parse_messages`] and then rebinds every event
    /// symbol to the feed's configured instrument (matched on the normalized
    /// symbol value). This is required for OKX MARGIN instruments, whose
    /// payload instIds share the spot form (`BTC-USDT`): the payload parser
    /// would otherwise produce a `Spot`-kind symbol. Index pushes instead
    /// fan out from the native index ID to all matching configured contracts;
    /// other events retain the ordinary symbol rebind.
    pub fn parse_messages_for_feed(
        feed: &ExchangeFeed,
        message: &Value,
        received_ts: f64,
    ) -> Vec<OkxEvent> {
        Self::parse_messages(message, received_ts)
            .into_iter()
            .flat_map(|event| {
                #[cfg(feature = "index")]
                if let OkxEvent::IndexPrice(index) = &event {
                    return feed
                        .symbols
                        .iter()
                        .enumerate()
                        .filter_map(|(i, symbol)| {
                            let native = feed
                                .exchange_symbols
                                .get(i)
                                .map(String::as_str)
                                .unwrap_or(symbol.as_str());
                            if index_symbol(native) != index.symbol.as_str() {
                                return None;
                            }
                            let mut mapped = index.clone();
                            mapped.symbol = symbol.clone();
                            Some(OkxEvent::IndexPrice(mapped))
                        })
                        .collect::<Vec<_>>();
                }
                vec![rebind_symbol(feed, event)]
            })
            .collect()
    }

    pub fn parse_messages(message: &Value, received_ts: f64) -> Vec<OkxEvent> {
        let Some(channel) = message
            .get("arg")
            .and_then(|arg| arg.get("channel"))
            .and_then(Value::as_str)
        else {
            return Vec::new();
        };
        match channel {
            #[cfg(feature = "ticker")]
            "tickers" => parser::parse_tickers(message, received_ts)
                .into_iter()
                .map(OkxEvent::Ticker)
                .collect(),
            #[cfg(feature = "trade")]
            "trades" => parser::parse_trades(message, received_ts)
                .into_iter()
                .map(OkxEvent::Trade)
                .collect(),
            #[cfg(feature = "orderbook")]
            "books" | "books5" => parser::parse_l2_books(message, received_ts)
                .into_iter()
                .map(OkxEvent::L2Book)
                .collect(),
            #[cfg(feature = "orderbook")]
            "bbo-tbt" => parser::parse_l1_books(message, received_ts)
                .into_iter()
                .map(OkxEvent::L1Book)
                .collect(),
            #[cfg(feature = "candles")]
            channel if channel.starts_with("candle") => parser::parse_candles(message, received_ts)
                .into_iter()
                .map(OkxEvent::Candle)
                .collect(),
            #[cfg(feature = "funding")]
            "funding-rate" => parser::parse_fundings(message, received_ts)
                .into_iter()
                .map(OkxEvent::Funding)
                .collect(),
            #[cfg(feature = "liquidations")]
            "liquidation-orders" => parser::parse_liquidations(message, received_ts)
                .into_iter()
                .map(OkxEvent::Liquidation)
                .collect(),
            #[cfg(feature = "openinterest")]
            "open-interest" => parser::parse_open_interests(message, received_ts)
                .into_iter()
                .map(OkxEvent::OpenInterest)
                .collect(),
            #[cfg(feature = "markprice")]
            "mark-price" => parser::parse_mark_prices(message, received_ts)
                .into_iter()
                .map(OkxEvent::MarkPrice)
                .collect(),
            #[cfg(feature = "index")]
            "index-tickers" => parser::parse_index_prices(message, received_ts)
                .into_iter()
                .map(OkxEvent::IndexPrice)
                .collect(),
            _ => Vec::new(),
        }
    }
}

// OKX indexes are identified by base/quote, not the SWAP or expiry suffix.
fn index_symbol(native: &str) -> String {
    native.split('-').take(2).collect::<Vec<_>>().join("-")
}

fn rebind_symbol(feed: &ExchangeFeed, event: OkxEvent) -> OkxEvent {
    let rebind = |symbol: &cryptofeed_core::symbol::Symbol| -> cryptofeed_core::symbol::Symbol {
        feed.symbols
            .iter()
            .find(|candidate| candidate.as_str().eq_ignore_ascii_case(symbol.as_str()))
            .cloned()
            .unwrap_or_else(|| symbol.clone())
    };
    match event {
        #[cfg(feature = "candles")]
        OkxEvent::Candle(mut candle) => {
            candle.symbol = rebind(&candle.symbol);
            OkxEvent::Candle(candle)
        }
        #[cfg(feature = "funding")]
        OkxEvent::Funding(mut funding) => {
            funding.symbol = rebind(&funding.symbol);
            OkxEvent::Funding(funding)
        }
        #[cfg(feature = "liquidations")]
        OkxEvent::Liquidation(mut liquidation) => {
            liquidation.symbol = rebind(&liquidation.symbol);
            OkxEvent::Liquidation(liquidation)
        }
        #[cfg(feature = "markprice")]
        OkxEvent::MarkPrice(mut mark_price) => {
            mark_price.symbol = rebind(&mark_price.symbol);
            OkxEvent::MarkPrice(mark_price)
        }
        #[cfg(feature = "openinterest")]
        OkxEvent::OpenInterest(mut open_interest) => {
            open_interest.symbol = rebind(&open_interest.symbol);
            OkxEvent::OpenInterest(open_interest)
        }
        #[cfg(feature = "index")]
        OkxEvent::IndexPrice(mut index_price) => {
            index_price.symbol = rebind(&index_price.symbol);
            OkxEvent::IndexPrice(index_price)
        }
        #[cfg(feature = "orderbook")]
        OkxEvent::L1Book(mut book) => {
            book.symbol = rebind(&book.symbol);
            OkxEvent::L1Book(book)
        }
        #[cfg(feature = "orderbook")]
        OkxEvent::L2Book(book) => OkxEvent::L2Book(match book {
            cryptofeed_orderbook::L2Book::Snapshot(mut snapshot) => {
                snapshot.symbol = rebind(&snapshot.symbol);
                cryptofeed_orderbook::L2Book::Snapshot(snapshot)
            }
            cryptofeed_orderbook::L2Book::Delta(mut delta) => {
                delta.symbol = rebind(&delta.symbol);
                cryptofeed_orderbook::L2Book::Delta(delta)
            }
        }),
        #[cfg(feature = "ticker")]
        OkxEvent::Ticker(mut ticker) => {
            ticker.symbol = rebind(&ticker.symbol);
            OkxEvent::Ticker(ticker)
        }
        #[cfg(feature = "trade")]
        OkxEvent::Trade(mut trade) => {
            trade.symbol = rebind(&trade.symbol);
            OkxEvent::Trade(trade)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{OkxAdapter, OkxControl};
    use crate::exchange::okx::Okx;

    #[test]
    fn candle_interval_and_l2_depth_reach_subscription_message() {
        let feed = Okx::new()
            .candles()
            .candles_interval("1h")
            .l2_book()
            .l2_book_depth(5)
            .symbol("BTC-USDT")
            .build();
        let payload = OkxAdapter::subscription_message(&feed);
        assert!(payload.contains("candle1H"));
        assert!(payload.contains("books5"));
        assert!(!payload.contains("candle1m"));
    }

    #[test]
    fn vip_tbt_depth_levels_are_rejected_at_preflight() {
        // The official `books50-l2-tbt`/`books-l2-tbt` channels exist but are
        // not wired into book sync; subscribing would silently drop every
        // push, so depth 50/400 must fail explicitly (PARITY.md keeps them
        // unsupported until a live capture allows wiring).
        for level in [50u16, 400] {
            assert!(
                !OkxAdapter::l2_book_depth_supported(level),
                "depth {level} must stay unsupported"
            );
            let feed = Okx::new()
                .l2_book()
                .l2_book_depth(level)
                .symbol("BTC-USDT-PERP")
                .build();
            assert!(matches!(
                crate::markets::validate_feed(&feed),
                Err(cryptofeed_core::error::Error::UnsupportedCapability(_))
            ));
        }
        assert!(OkxAdapter::l2_book_depth_supported(5));
    }

    #[test]
    fn candle_interval_wire_maps_official_okx_bars() {
        assert_eq!(OkxAdapter::candle_interval_wire("1m"), Some("1m"));
        assert_eq!(OkxAdapter::candle_interval_wire("1h"), Some("1H"));
        assert_eq!(OkxAdapter::candle_interval_wire("1d"), Some("1D"));
        assert_eq!(OkxAdapter::candle_interval_wire("1w"), Some("1W"));
        assert_eq!(OkxAdapter::candle_interval_wire("3M"), Some("3M"));
        assert_eq!(OkxAdapter::candle_interval_wire("8h"), None);
    }

    #[test]
    fn l2_book_channel_maps_official_partial_books() {
        assert_eq!(OkxAdapter::l2_book_channel(None), "books");
        assert_eq!(OkxAdapter::l2_book_channel(Some(5)), "books5");
        assert_eq!(OkxAdapter::l2_book_channel(Some(50)), "books50-l2-tbt");
        assert_eq!(OkxAdapter::l2_book_channel(Some(400)), "books-l2-tbt");
        assert_eq!(OkxAdapter::l2_book_channel(Some(100)), "books");
    }

    #[test]
    fn inst_type_for_feed_follows_the_feed_product_kind() {
        let spot = Okx::new().ticker().symbol("BTC-USDT").build();
        assert_eq!(OkxAdapter::inst_type_for_feed(&spot), "SPOT");
        let swap = Okx::new().ticker().symbol("BTC-USDT-PERP").build();
        assert_eq!(OkxAdapter::inst_type_for_feed(&swap), "SWAP");
        let futures = Okx::new().ticker().symbol("BTC-USD-240628").build();
        assert_eq!(OkxAdapter::inst_type_for_feed(&futures), "FUTURES");
        let margin = Okx::new()
            .liquidations()
            .instrument(cryptofeed_core::symbol::Symbol::margin("BTC", "USDT"))
            .exchange_symbol("BTC-USDT")
            .build();
        assert_eq!(OkxAdapter::inst_type_for_feed(&margin), "MARGIN");
        let option = Okx::new()
            .ticker()
            .instrument(cryptofeed_core::symbol::Symbol::option(
                "BTC", "USD", "250627", "100000", "C",
            ))
            .exchange_symbol("BTC-USD-250627-100000-C")
            .build();
        assert_eq!(OkxAdapter::inst_type_for_feed(&option), "OPTION");
        assert_ne!(OkxAdapter::inst_type_for_feed(&spot), "SWAP");
    }

    #[test]
    fn builds_okx_v5_public_websocket_url() {
        let feed = Okx::new().ticker().symbol("BTC-USDT").build();
        assert_eq!(
            OkxAdapter::subscription_url(&feed),
            "wss://ws.okx.com/ws/v5/public"
        );
    }

    #[test]
    fn routes_okx_candles_to_business_websocket() {
        let feed = Okx::new().candles().symbol("BTC-USDT-SWAP").build();
        assert_eq!(
            OkxAdapter::subscription_url(&feed),
            "wss://ws.okx.com/ws/v5/business"
        );
        assert_eq!(
            OkxAdapter::product_for_exchange_symbol("BTC-USDT-SWAP"),
            super::OkxProduct::Swap
        );
        assert_eq!(
            OkxAdapter::product_for_exchange_symbol("BTC-USDT-260925"),
            super::OkxProduct::Futures
        );
    }

    #[test]
    fn builds_okx_v5_subscription_message() {
        let feed = Okx::new()
            .ticker()
            .trade()
            .l2_book()
            .candles()
            .symbol("BTC-USDT")
            .build();
        let payload = OkxAdapter::subscription_message(&feed);
        assert!(payload.contains("\"channel\":\"tickers\""));
        assert!(payload.contains("\"channel\":\"trades\""));
        assert!(payload.contains("\"channel\":\"books\""));
        assert!(payload.contains("\"channel\":\"candle1m\""));
        assert!(payload.contains("\"instId\":\"BTC-USDT\""));
    }

    #[test]
    fn exposes_okx_text_heartbeat_and_control_messages() {
        assert_eq!(OkxAdapter::heartbeat_message(), "ping");
        assert!(matches!(
            OkxAdapter::parse_control_text("pong"),
            Some(Ok(OkxControl::Pong))
        ));
        assert!(
            OkxAdapter::parse_control_message(&serde_json::json!({
                "event": "error",
                "code": "60012",
                "msg": "Invalid request"
            }))
            .expect("control")
            .is_err()
        );
    }
}
