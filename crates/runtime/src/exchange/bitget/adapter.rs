use super::parser;
use crate::exchange::ExchangeFeed;
use cryptofeed_core::{
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

pub struct BitgetAdapter;

pub enum BitgetEvent {
    #[cfg(feature = "funding")]
    Funding(Funding),
    #[cfg(feature = "index")]
    IndexPrice(IndexPrice),
    #[cfg(feature = "markprice")]
    MarkPrice(MarkPrice),
    #[cfg(feature = "openinterest")]
    OpenInterest(OpenInterest),
    #[cfg(feature = "orderbook")]
    L1Book(L1Book),
    #[cfg(feature = "candles")]
    Candle(Candle),
    #[cfg(feature = "liquidations")]
    Liquidation(Liquidation),
    #[cfg(feature = "orderbook")]
    L2Book(L2Book),
    #[cfg(feature = "ticker")]
    Ticker(Ticker),
    #[cfg(feature = "trade")]
    Trade(Trade),
}

impl BitgetAdapter {
    pub fn websocket_url() -> &'static str {
        "wss://ws.bitget.com/v3/ws/public"
    }

    pub fn subscription_url(_feed: &ExchangeFeed) -> String {
        Self::websocket_url().to_owned()
    }

    /// Normalized intervals mapped to the current official v3 vocabulary.
    /// Longer periods are rejected until officially documented and verified.
    pub fn candle_interval_wire(interval: &str) -> Option<&str> {
        match interval {
            "1m" | "3m" | "5m" | "15m" | "30m" => Some(interval),
            "1h" => Some("1H"),
            "4h" => Some("4H"),
            "6h" => Some("6H"),
            "12h" => Some("12H"),
            "1d" => Some("1D"),
            _ => None,
        }
    }

    /// Whether the L2 depth level is officially supported: `books1`,
    /// `books5`, `books50` partial books on the v3 `books` channel family.
    pub fn l2_book_depth_supported(level: u16) -> bool {
        matches!(level, 1 | 5 | 50)
    }

    pub fn subscription_message(feed: &ExchangeFeed) -> String {
        let symbols: Vec<String> = if feed.exchange_symbols.is_empty() {
            feed.symbols
                .iter()
                .map(|symbol| normalized_native_symbol(symbol, ""))
                .collect()
        } else {
            feed.exchange_symbols.clone()
        };
        let args: Vec<Value> = symbols
            .iter()
            .enumerate()
            .flat_map(|(index, exchange_symbol)| {
                let inst_type = feed
                    .symbols
                    .get(index)
                    .map(bitget_instrument_type)
                    .unwrap_or("spot");
                feed.channels.iter().filter_map(move |channel| {
                    if !feed
                        .symbols
                        .get(index)
                        .is_some_and(|symbol| feed.subscribes(*channel, symbol))
                    {
                        return None;
                    }
                    let topic = match channel {
                        Channel::Candles => "kline",
                        Channel::Ticker => "ticker",
                        Channel::Trade => "publicTrade",
                        Channel::L2Book => match feed.l2_book_depth {
                            Some(1) => "books1",
                            Some(5) => "books5",
                            Some(50) => "books50",
                            _ => "books",
                        },
                        Channel::Funding
                        | Channel::MarkPrice
                        | Channel::OpenInterest
                        | Channel::Index => "ticker",
                        Channel::L1Book => "books1",
                        Channel::Liquidations => "liquidation",
                        // Future channels are not subscribed.
                        _ => return None,
                    };

                    let mut arg = serde_json::json!({
                        "instType": inst_type,
                        "topic": topic,
                    });
                    // The liquidation stream is instType-scoped, not
                    // symbol-scoped.
                    if !matches!(channel, Channel::Liquidations) {
                        arg["symbol"] = Value::String(exchange_symbol.clone());
                    }
                    if matches!(channel, Channel::Candles) {
                        arg["interval"] = Value::String(
                            Self::candle_interval_wire(&feed.candle_interval)
                                .unwrap_or("1m")
                                .to_owned(),
                        );
                    }
                    Some(arg)
                })
            })
            .collect();
        // The instType-scoped liquidation arg is emitted once per symbol by
        // the loop above; dedupe identical args so a multi-symbol feed sends
        // a single `liquidation` subscription.
        let mut seen = std::collections::HashSet::new();
        let args: Vec<Value> = args
            .into_iter()
            .filter(|arg| seen.insert(arg.to_string()))
            .collect();

        serde_json::json!({
            "op": "subscribe",
            "args": args,
        })
        .to_string()
    }

    pub fn parse_messages_for_feed(
        feed: &ExchangeFeed,
        message: &Value,
        received_ts: f64,
    ) -> Vec<BitgetEvent> {
        let inst_type = message
            .get("arg")
            .and_then(|arg| arg.get("instType"))
            .and_then(Value::as_str);
        if inst_type.is_some_and(|kind| {
            !feed
                .symbols
                .iter()
                .any(|symbol| bitget_instrument_type(symbol).eq_ignore_ascii_case(kind))
        }) {
            return Vec::new();
        }
        let native = message
            .get("arg")
            .and_then(|arg| arg.get("symbol").or_else(|| arg.get("instId")))
            .and_then(Value::as_str);
        let find_symbol = |native: &str| {
            feed.symbols.iter().enumerate().find_map(|(i, symbol)| {
                let exchange_symbol = feed
                    .exchange_symbols
                    .get(i)
                    .cloned()
                    .unwrap_or_else(|| normalized_native_symbol(symbol, ""));
                (exchange_symbol.eq_ignore_ascii_case(native)
                    && inst_type.is_none_or(|kind| {
                        bitget_instrument_type(symbol).eq_ignore_ascii_case(kind)
                    }))
                .then_some(symbol)
            })
        };
        #[cfg(feature = "liquidations")]
        if message
            .get("arg")
            .and_then(|arg| arg.get("topic"))
            .and_then(Value::as_str)
            == Some("liquidation")
        {
            return message
                .get("data")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .flat_map(|row| {
                    let mut single = message.clone();
                    single["data"] = serde_json::json!([row]);
                    let symbol = row
                        .get("symbol")
                        .and_then(Value::as_str)
                        .and_then(find_symbol);
                    if symbol.is_none() {
                        return Vec::new();
                    }
                    Self::parse_messages(&single, received_ts)
                        .into_iter()
                        .map(move |mut event| {
                            if let (Some(symbol), BitgetEvent::Liquidation(value)) =
                                (symbol, &mut event)
                            {
                                value.symbol = symbol.clone();
                            }
                            event
                        })
                        .collect::<Vec<_>>()
                })
                .collect();
        }
        let symbol = native.and_then(find_symbol);
        if native.is_some() && symbol.is_none() {
            return Vec::new();
        }
        Self::parse_messages(message, received_ts)
            .into_iter()
            .map(|mut event| {
                if let Some(symbol) = symbol {
                    match &mut event {
                        #[cfg(feature = "candles")]
                        BitgetEvent::Candle(candle) => candle.symbol = symbol.clone(),
                        #[cfg(feature = "funding")]
                        BitgetEvent::Funding(funding) => funding.symbol = symbol.clone(),
                        #[cfg(feature = "index")]
                        BitgetEvent::IndexPrice(index) => index.symbol = symbol.clone(),
                        #[cfg(feature = "markprice")]
                        BitgetEvent::MarkPrice(mark) => mark.symbol = symbol.clone(),
                        #[cfg(feature = "openinterest")]
                        BitgetEvent::OpenInterest(oi) => oi.symbol = symbol.clone(),
                        #[cfg(feature = "orderbook")]
                        BitgetEvent::L1Book(book) => book.symbol = symbol.clone(),
                        #[cfg(feature = "ticker")]
                        BitgetEvent::Ticker(ticker) => ticker.symbol = symbol.clone(),
                        #[cfg(feature = "trade")]
                        BitgetEvent::Trade(trade) => trade.symbol = symbol.clone(),
                        #[cfg(feature = "liquidations")]
                        BitgetEvent::Liquidation(liquidation) => {
                            liquidation.symbol = symbol.clone()
                        }
                        #[cfg(feature = "orderbook")]
                        BitgetEvent::L2Book(book) => match book {
                            L2Book::Snapshot(book) => book.symbol = symbol.clone(),
                            L2Book::Delta(book) => book.symbol = symbol.clone(),
                        },
                        #[cfg(not(any(
                            feature = "ticker",
                            feature = "trade",
                            feature = "orderbook",
                            feature = "candles",
                            feature = "funding",
                            feature = "index",
                            feature = "markprice",
                            feature = "openinterest",
                            feature = "liquidations"
                        )))]
                        _ => {}
                    }
                }
                event
            })
            .collect()
    }

    pub fn parse_message(message: &Value, received_ts: f64) -> Option<BitgetEvent> {
        Self::parse_messages(message, received_ts)
            .into_iter()
            .next()
    }

    pub fn parse_messages(message: &Value, received_ts: f64) -> Vec<BitgetEvent> {
        let Some(arg) = message.get("arg") else {
            return Vec::new();
        };
        let Some(topic) = arg
            .get("topic")
            .or_else(|| arg.get("channel"))
            .and_then(Value::as_str)
        else {
            return Vec::new();
        };

        match topic {
            #[cfg(feature = "candles")]
            "kline" | "candle1m" => parser::parse_candles(message, received_ts)
                .into_iter()
                .map(BitgetEvent::Candle)
                .collect(),
            #[cfg(feature = "trade")]
            "trade" | "publicTrade" => parser::parse_trades(message, received_ts)
                .into_iter()
                .map(BitgetEvent::Trade)
                .collect(),
            #[cfg(any(
                feature = "ticker",
                feature = "funding",
                feature = "openinterest",
                feature = "index",
                feature = "markprice"
            ))]
            "ticker" => {
                let mut events = Vec::new();
                #[cfg(feature = "ticker")]
                events.extend(
                    parser::parse_tickers(message, received_ts)
                        .into_iter()
                        .map(BitgetEvent::Ticker),
                );
                #[cfg(feature = "funding")]
                events.extend(
                    parser::parse_fundings(message, received_ts)
                        .into_iter()
                        .map(BitgetEvent::Funding),
                );
                #[cfg(feature = "openinterest")]
                events.extend(
                    parser::parse_open_interests(message, received_ts)
                        .into_iter()
                        .map(BitgetEvent::OpenInterest),
                );
                #[cfg(feature = "index")]
                events.extend(
                    parser::parse_index_prices(message, received_ts)
                        .into_iter()
                        .map(BitgetEvent::IndexPrice),
                );
                #[cfg(feature = "markprice")]
                events.extend(
                    parser::parse_mark_prices(message, received_ts)
                        .into_iter()
                        .map(BitgetEvent::MarkPrice),
                );
                events
            }
            #[cfg(feature = "orderbook")]
            "books" | "books1" | "books5" | "books50" => {
                let mut events: Vec<_> = parser::parse_l2_books(message, received_ts)
                    .into_iter()
                    .map(BitgetEvent::L2Book)
                    .collect();
                if topic == "books1" {
                    events.extend(
                        parser::parse_l1_books(message, received_ts)
                            .into_iter()
                            .map(BitgetEvent::L1Book),
                    );
                }
                events
            }
            #[cfg(feature = "liquidations")]
            "liquidation" => parser::parse_liquidations(message, received_ts)
                .into_iter()
                .map(BitgetEvent::Liquidation)
                .collect(),
            _ => Vec::new(),
        }
    }
}

fn bitget_instrument_type(symbol: &Symbol) -> &'static str {
    if symbol.kind() == InstrumentKind::Spot {
        return "spot";
    }
    if symbol.as_str().contains("-USDT-") {
        "usdt-futures"
    } else if symbol.as_str().contains("-USDC-") {
        "usdc-futures"
    } else {
        "coin-futures"
    }
}

/// Native Bitget symbol for a normalized symbol: spot and perpetual drop the
/// kind suffix (`BTC-USDT-PERP` -> `BTCUSDT`); dated futures keep the expiry
/// segment (`BTC-USDT-240628` -> `BTCUSDT240628`).
fn normalized_native_symbol(symbol: &Symbol, separator: &str) -> String {
    let parts: Vec<_> = symbol.as_str().split('-').collect();
    if symbol.kind() == InstrumentKind::Futures && parts.len() >= 3 {
        format!("{}{}{}", parts[0], parts[1], parts[2])
    } else {
        parts
            .into_iter()
            .take(2)
            .collect::<Vec<_>>()
            .join(separator)
    }
}

#[cfg(test)]
mod tests {
    use super::BitgetAdapter;
    use crate::exchange::bitget::Bitget;
    use cryptofeed_core::symbol::Symbol;
    use serde_json::Value;

    #[test]
    fn candle_interval_and_l2_depth_reach_subscription_message() {
        let feed = Bitget::new()
            .candles()
            .candles_interval("1h")
            .l2_book()
            .l2_book_depth(5)
            .symbol("BTC-USDT")
            .build();
        let payload = BitgetAdapter::subscription_message(&feed);
        let message: Value = serde_json::from_str(&payload).expect("subscribe payload");
        let args = message["args"].as_array().expect("args");
        let kline = args
            .iter()
            .find(|arg| arg["topic"] == "kline")
            .expect("kline arg");
        assert_eq!(kline["interval"], "1H");
        assert!(args.iter().any(|arg| arg["topic"] == "books5"));
    }

    #[test]
    fn builds_bitget_v3_public_websocket_url() {
        let feed = Bitget::new().ticker().symbol("BTC-USDT").build();
        let url = BitgetAdapter::subscription_url(&feed);
        assert_eq!(url, "wss://ws.bitget.com/v3/ws/public");
    }

    #[test]
    fn builds_bitget_v3_subscription_message() {
        let feed = Bitget::new()
            .ticker()
            .trade()
            .l2_book()
            .symbol("BTC-USDT")
            .build();

        let payload = BitgetAdapter::subscription_message(&feed);
        assert!(payload.contains("\"instType\":\"spot\""));
        assert!(payload.contains("\"topic\":\"ticker\""));
        assert!(payload.contains("\"topic\":\"publicTrade\""));
        assert!(payload.contains("\"topic\":\"books\""));
        assert!(payload.contains("\"symbol\":\"BTCUSDT\""));
    }

    #[test]
    fn dated_futures_keep_expiry_in_native_symbol() {
        // Without explicit `exchange_symbol(...)` pairs the native symbol is
        // derived from the normalized form; a dated future must keep its
        // expiry (`BTCUSDT240628`), not truncate to `BTCUSDT`.
        let feed = Bitget::new()
            .ticker()
            .instrument(Symbol::futures("BTC", "USDT", "240628"))
            .build();

        let payload = BitgetAdapter::subscription_message(&feed);
        assert!(payload.contains("\"symbol\":\"BTCUSDT240628\""));
        assert!(payload.contains("\"instType\":\"usdt-futures\""));
        assert!(!payload.contains("BTCUSDT\""));
    }

    #[test]
    fn builds_v3_kline_subscription_with_separate_interval() {
        let feed = Bitget::new().candles().symbol("BTC-USDT").build();
        let payload: Value =
            serde_json::from_str(&BitgetAdapter::subscription_message(&feed)).unwrap();
        let arg = &payload["args"][0];

        assert_eq!(arg["topic"], "kline");
        assert_eq!(arg["interval"], "1m");
    }

    #[test]
    fn derives_v3_product_type_from_normalized_instrument() {
        let feed = Bitget::new()
            .ticker()
            .symbol("BTC-USDT-PERP")
            .exchange_symbol("BTCUSDT")
            .build();
        let payload: Value =
            serde_json::from_str(&BitgetAdapter::subscription_message(&feed)).unwrap();

        assert_eq!(payload["args"][0]["instType"], "usdt-futures");
    }
}
