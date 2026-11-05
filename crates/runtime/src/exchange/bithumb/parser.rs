//! Bithumb public WebSocket payload parsing.
//!
//! Wire facts come from the official API 2.0 documentation recorded in
//! `docs/bithumb-protocol.md`. Every parser takes the raw frame and the local
//! receive clock and returns normalized models only. Bithumb public v1 pushes
//! complete order-book snapshots with at most 15 units; no candle stream is
//! documented, so no candle parser exists.

use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
use rust_decimal::Decimal;
use serde_json::Value;

#[cfg(feature = "trade")]
use cryptofeed_core::model::Side;
#[cfg(feature = "orderbook")]
use cryptofeed_orderbook::{L2Book, L2BookSnapshot, PriceLevel};
#[cfg(feature = "trade")]
use cryptofeed_trade::Trade;

/// Documented maximum order-book units per side in one public push.
pub const MAX_ORDERBOOK_UNITS: u16 = 15;

/// Maps a native Bithumb market code (`{QUOTE}-{BASE}`) to the normalized model.
///
/// The quote prefix is preserved exactly: `KRW-BTC` becomes `BTC-KRW`, and no
/// fiat quote is rewritten or guessed from the base asset name. Malformed codes
/// return `None` so dispatch can reject them against the configured catalog.
pub fn normalize_symbol(raw: &str) -> Option<Symbol> {
    let (quote, base) = raw.split_once('-')?;
    if quote.is_empty() || base.is_empty() || base.contains('-') {
        return None;
    }
    Some(Symbol::spot(base, quote))
}

pub(crate) fn parse_decimal(value: &Value) -> Option<Decimal> {
    match value {
        Value::String(text) => decimal_from_text(text),
        Value::Number(number) => decimal_from_text(&number.to_string()),
        _ => None,
    }
}

/// Accepts both plain decimals and the scientific notation that JSON numbers
/// use for very small prices (e.g. `5.213462e-06`); `from_str_exact` alone
/// rejects the exponent form.
fn decimal_from_text(text: &str) -> Option<Decimal> {
    Decimal::from_str_exact(text)
        .or_else(|_| Decimal::from_scientific(text))
        .ok()
}

/// Milliseconds since the epoch (integer or numeric string) to seconds.
fn parse_millis(value: &Value) -> Option<f64> {
    match value {
        Value::String(text) => text.parse::<f64>().ok().map(|value| value / 1_000.0),
        Value::Number(number) => number.as_f64().map(|value| value / 1_000.0),
        _ => None,
    }
}

/// Data type of a market frame (`trade`, `orderbook`).
pub fn message_type(message: &Value) -> Option<&str> {
    message.get("type").and_then(Value::as_str)
}

/// Native market code of a frame.
pub fn native_code(message: &Value) -> Option<&str> {
    message.get("code").and_then(Value::as_str)
}

/// The documented `SNAPSHOT`/`REALTIME` stream marker.
pub fn stream_type(message: &Value) -> Option<&str> {
    message.get("stream_type").and_then(Value::as_str)
}

#[cfg(feature = "trade")]
pub fn parse_trade_for_symbol(message: &Value, received_ts: f64, symbol: &Symbol) -> Option<Trade> {
    let side = match message.get("ask_bid")?.as_str()? {
        "ASK" => Side::Sell,
        "BID" => Side::Buy,
        _ => return None,
    };
    Some(Trade {
        exchange: ExchangeId::Bithumb,
        symbol: symbol.clone(),
        side,
        amount: parse_decimal(message.get("trade_volume")?)?,
        price: parse_decimal(message.get("trade_price")?)?,
        // The trade execution clock is the event time; the frame publish clock
        // is only a fallback. `trade_date`/`trade_time` are KST display strings.
        exchange_ts: parse_millis(message.get("trade_timestamp").unwrap_or(&Value::Null))
            .or_else(|| parse_millis(message.get("timestamp").unwrap_or(&Value::Null)))
            .unwrap_or(received_ts),
        received_ts,
        id: message.get("sequential_id").map(|value| {
            value
                .as_str()
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| value.to_string())
        }),
        implied_volatility: None,
    })
}

#[cfg(feature = "trade")]
pub fn parse_trade(message: &Value, received_ts: f64) -> Option<Trade> {
    let symbol = normalize_symbol(native_code(message)?)?;
    parse_trade_for_symbol(message, received_ts, &symbol)
}

#[cfg(feature = "orderbook")]
fn parse_level(value: &Value, price: &str, amount: &str) -> Option<PriceLevel> {
    Some(PriceLevel {
        price: parse_decimal(value.get(price)?)?,
        amount: parse_decimal(value.get(amount)?)?,
    })
}

/// Parses one `orderbook` push. Every push is a complete snapshot of the
/// documented paired units: unit `i` pairs the `i`-th best ask with the `i`-th
/// best bid, so each side is taken from the same array entries.
#[cfg(feature = "orderbook")]
pub fn parse_book_for_symbol(message: &Value, received_ts: f64, symbol: &Symbol) -> Option<L2Book> {
    if message_type(message)? != "orderbook" {
        return None;
    }
    let units = message.get("orderbook_units")?.as_array()?;
    let mut bids = Vec::with_capacity(units.len());
    let mut asks = Vec::with_capacity(units.len());
    for unit in units {
        bids.push(parse_level(unit, "bid_price", "bid_size")?);
        asks.push(parse_level(unit, "ask_price", "ask_size")?);
    }
    Some(L2Book::Snapshot(L2BookSnapshot {
        exchange: ExchangeId::Bithumb,
        symbol: symbol.clone(),
        bids,
        asks,
        exchange_ts: parse_millis(message.get("timestamp").unwrap_or(&Value::Null))
            .unwrap_or(received_ts),
        received_ts,
    }))
}

#[cfg(feature = "orderbook")]
pub fn parse_book(message: &Value, received_ts: f64) -> Option<L2Book> {
    let symbol = normalize_symbol(native_code(message)?)?;
    parse_book_for_symbol(message, received_ts, &symbol)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn normalizes_quote_prefix_without_guessing() {
        assert_eq!(
            normalize_symbol("KRW-BTC"),
            Some(Symbol::spot("BTC", "KRW"))
        );
        assert_eq!(
            normalize_symbol("BTC-ETH"),
            Some(Symbol::spot("ETH", "BTC"))
        );
        assert_eq!(normalize_symbol("BTC"), None);
        assert_eq!(normalize_symbol("KRW-"), None);
        assert_eq!(normalize_symbol("-BTC"), None);
    }

    #[test]
    fn reads_type_code_and_stream_type() {
        let message = json!({"type": "trade", "code": "KRW-BTC", "stream_type": "SNAPSHOT"});
        assert_eq!(message_type(&message), Some("trade"));
        assert_eq!(native_code(&message), Some("KRW-BTC"));
        assert_eq!(stream_type(&message), Some("SNAPSHOT"));
        assert_eq!(native_code(&json!({"type": "trade"})), None);
    }

    #[cfg(feature = "trade")]
    #[test]
    fn parses_documented_trade_frame() {
        let message = json!({
            "type": "trade",
            "code": "KRW-BTC",
            "trade_price": 48970000.0,
            "trade_volume": 1.4825,
            "ask_bid": "BID",
            "prev_closing_price": 48450000.0,
            "change": "RISE",
            "change_price": 520000.0,
            "trade_date": "2024-09-10",
            "trade_time": "09:58:54",
            "trade_timestamp": 1725929934373_u64,
            "sequential_id": 17259299343730000_u64,
            "timestamp": 1725929934483_u64,
            "stream_type": "REALTIME"
        });
        let trade = parse_trade(&message, 1.0).expect("trade");
        assert_eq!(trade.exchange, ExchangeId::Bithumb);
        assert_eq!(trade.symbol.as_str(), "BTC-KRW");
        assert_eq!(trade.side, Side::Buy);
        assert_eq!(trade.price.to_string(), "48970000.0");
        assert_eq!(trade.amount.to_string(), "1.4825");
        assert_eq!(trade.id.as_deref(), Some("17259299343730000"));
        assert!((trade.exchange_ts - 1_725_929_934.373).abs() < 1e-6);
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn parses_paired_orderbook_units_as_a_snapshot() {
        let message = json!({
            "type": "orderbook",
            "code": "KRW-BTC",
            "total_ask_size": 450.3526,
            "total_bid_size": 63.3006,
            "orderbook_units": [
                {"ask_price": 47880000.0, "bid_price": 47830000.0, "ask_size": 4.3478, "bid_size": 5.6370},
                {"ask_price": 48970000.0, "bid_price": 47790000.0, "ask_size": 2.3642, "bid_size": 0.9705}
            ],
            "timestamp": 1725929934483_u64,
            "stream_type": "REALTIME"
        });
        let book = parse_book(&message, 1.0).expect("book");
        let L2Book::Snapshot(snapshot) = &book else {
            panic!("bithumb orderbook is snapshot-only");
        };
        assert_eq!(snapshot.symbol.as_str(), "BTC-KRW");
        assert_eq!(snapshot.bids.len(), 2);
        assert_eq!(snapshot.asks.len(), 2);
        assert_eq!(snapshot.bids[0].price.to_string(), "47830000.0");
        assert_eq!(snapshot.asks[1].price.to_string(), "48970000.0");
        assert!((snapshot.exchange_ts - 1_725_929_934.483).abs() < 1e-6);
    }
}
