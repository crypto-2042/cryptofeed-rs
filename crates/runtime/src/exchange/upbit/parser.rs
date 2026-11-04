//! Upbit public WebSocket payload parsing.
//!
//! Wire facts come from the official channel documentation recorded in
//! `docs/upbit-protocol.md`. Every parser takes the raw frame and the local
//! receive clock and returns normalized models only. Upbit pushes complete
//! order-book snapshots, so the book-sync layer has no sequence handling.
//!
//! Native product identity is resolved by the adapter from the configured
//! catalog mapping (`code` -> normalized symbol) before these helpers run. The
//! standalone `parse_*` helpers derive a best-effort symbol from the documented
//! `{QUOTE}-{BASE}` code for fixtures and single-frame tests.

use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
use rust_decimal::Decimal;
use serde_json::Value;

#[cfg(feature = "candles")]
use cryptofeed_candles::Candle;
#[cfg(feature = "trade")]
use cryptofeed_core::model::Side;
#[cfg(feature = "orderbook")]
use cryptofeed_orderbook::{L2Book, L2BookSnapshot, PriceLevel};
#[cfg(feature = "trade")]
use cryptofeed_trade::Trade;

/// Upbit order-book unit count when the caller configures no depth. The venue
/// documents `30` as the default unit count for a plain `codes` entry.
pub const DEFAULT_ORDERBOOK_UNITS: u16 = 30;

/// Maps a native Upbit market code (`{QUOTE}-{BASE}`) to the normalized model.
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

/// Strips the documented orderbook unit suffix (`KRW-BTC.5`) from a code.
pub fn market_code(raw: &str) -> &str {
    match raw.split_once('.') {
        Some((code, suffix))
            if !code.is_empty()
                && !suffix.is_empty()
                && suffix.chars().all(|ch| ch.is_ascii_digit()) =>
        {
            code
        }
        _ => raw,
    }
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

/// Days since 1970-01-01 for a proleptic Gregorian date (Howard Hinnant's
/// `days_from_civil`).
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month = i64::from(month);
    let day = i64::from(day);
    let day_of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// Parses the documented candle timestamp `yyyy-MM-dd'T'HH:mm:ss` (UTC, no zone
/// suffix) into Unix seconds.
pub fn parse_candle_time(text: &str) -> Option<f64> {
    let (date, clock) = text.split_once('T')?;
    if clock.len() != 8 {
        return None;
    }
    let mut date_parts = date.split('-');
    let year: i64 = date_parts.next()?.parse().ok()?;
    let month: u32 = date_parts.next()?.parse().ok()?;
    let day: u32 = date_parts.next()?.parse().ok()?;
    if date_parts.next().is_some() || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let mut clock_parts = clock.split(':');
    let hour: u32 = clock_parts.next()?.parse().ok()?;
    let minute: u32 = clock_parts.next()?.parse().ok()?;
    let second: u32 = clock_parts.next()?.parse().ok()?;
    if clock_parts.next().is_some() || hour > 23 || minute > 59 || second > 60 {
        return None;
    }
    let days = days_from_civil(year, month, day);
    Some(
        days as f64 * 86_400.0
            + f64::from(hour) * 3_600.0
            + f64::from(minute) * 60.0
            + f64::from(second),
    )
}

/// Normalized interval for a documented `candle.<unit>` type.
pub fn normalize_candle_interval(unit: &str) -> Option<&'static str> {
    Some(match unit {
        "1s" => "1s",
        "1m" => "1m",
        "3m" => "3m",
        "5m" => "5m",
        "10m" => "10m",
        "15m" => "15m",
        "30m" => "30m",
        "60m" => "1h",
        "240m" => "4h",
        _ => return None,
    })
}

/// Bar length in seconds for a normalized Upbit interval.
pub fn interval_seconds(interval: &str) -> Option<f64> {
    Some(match interval {
        "1s" => 1.0,
        "1m" => 60.0,
        "3m" => 180.0,
        "5m" => 300.0,
        "10m" => 600.0,
        "15m" => 900.0,
        "30m" => 1_800.0,
        "1h" => 3_600.0,
        "4h" => 14_400.0,
        _ => return None,
    })
}

/// Data type of a market frame (`trade`, `orderbook`, `candle.1m`).
pub fn message_type(message: &Value) -> Option<&str> {
    message.get("type").and_then(Value::as_str)
}

/// Native market code of a frame. The current documentation field is `code`;
/// older guide examples use `market`, which is accepted as a documented legacy
/// alias of the same value.
pub fn native_code(message: &Value) -> Option<&str> {
    message
        .get("code")
        .and_then(Value::as_str)
        .or_else(|| message.get("market").and_then(Value::as_str))
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
        exchange: ExchangeId::Upbit,
        symbol: symbol.clone(),
        side,
        amount: parse_decimal(message.get("trade_volume")?)?,
        price: parse_decimal(message.get("trade_price")?)?,
        // The trade execution clock is the event time; the frame publish clock
        // is only a fallback.
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

#[cfg(feature = "candles")]
pub fn parse_candle_for_symbol(
    message: &Value,
    received_ts: f64,
    symbol: &Symbol,
) -> Option<Candle> {
    let unit = message_type(message)?.strip_prefix("candle.")?;
    let interval = normalize_candle_interval(unit)?;
    let start = parse_candle_time(message.get("candle_date_time_utc")?.as_str()?)?;
    let end = start + interval_seconds(interval)?;
    Some(Candle {
        exchange: ExchangeId::Upbit,
        symbol: symbol.clone(),
        start,
        end,
        interval: interval.to_owned(),
        // Upbit does not transmit a trade count for candle frames.
        trades: None,
        open: parse_decimal(message.get("opening_price")?)?,
        close: parse_decimal(message.get("trade_price")?)?,
        high: parse_decimal(message.get("high_price")?)?,
        low: parse_decimal(message.get("low_price")?)?,
        volume: parse_decimal(message.get("candle_acc_trade_volume")?)?,
        // Neither `stream_type` (SNAPSHOT/REALTIME delivery) nor repeated
        // pushes of the same bar state completion; the venue never says.
        closed: None,
        exchange_ts: parse_millis(message.get("timestamp").unwrap_or(&Value::Null))
            .unwrap_or(received_ts),
        received_ts,
    })
}

#[cfg(feature = "candles")]
pub fn parse_candle(message: &Value, received_ts: f64) -> Option<Candle> {
    let symbol = normalize_symbol(native_code(message)?)?;
    parse_candle_for_symbol(message, received_ts, &symbol)
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
        exchange: ExchangeId::Upbit,
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
        assert_eq!(
            normalize_symbol("USDT-BTC"),
            Some(Symbol::spot("BTC", "USDT"))
        );
        assert_eq!(normalize_symbol("BTC"), None);
        assert_eq!(normalize_symbol("KRW-"), None);
        assert_eq!(normalize_symbol("-BTC"), None);
        assert_eq!(market_code("KRW-BTC.5"), "KRW-BTC");
        assert_eq!(market_code("KRW-BTC"), "KRW-BTC");
    }

    #[test]
    fn parses_documented_candle_timestamps() {
        assert_eq!(
            parse_candle_time("2025-01-02T04:41:51"),
            Some(1_735_792_911.0)
        );
        assert_eq!(parse_candle_time("2025-13-02T04:41:51"), None);
        assert_eq!(parse_candle_time("2025-01-02 04:41:51"), None);
        assert_eq!(parse_candle_time("2025-01-02T04:41:51Z"), None);
    }

    #[test]
    fn maps_only_documented_candle_units() {
        for (unit, interval) in [
            ("1s", "1s"),
            ("1m", "1m"),
            ("3m", "3m"),
            ("5m", "5m"),
            ("10m", "10m"),
            ("15m", "15m"),
            ("30m", "30m"),
            ("60m", "1h"),
            ("240m", "4h"),
        ] {
            assert_eq!(normalize_candle_interval(unit), Some(interval));
        }
        assert_eq!(normalize_candle_interval("2m"), None);
        assert_eq!(normalize_candle_interval("1h"), None);
        assert_eq!(normalize_candle_interval("1d"), None);
    }

    #[test]
    fn reads_code_type_and_legacy_market_alias() {
        let message = json!({"type": "trade", "code": "KRW-BTC", "stream_type": "REALTIME"});
        assert_eq!(message_type(&message), Some("trade"));
        assert_eq!(native_code(&message), Some("KRW-BTC"));
        assert_eq!(stream_type(&message), Some("REALTIME"));
        assert_eq!(
            native_code(&json!({"type": "ticker", "market": "KRW-BTC"})),
            Some("KRW-BTC")
        );
    }

    #[cfg(feature = "trade")]
    #[test]
    fn parses_documented_trade_frame() {
        let message = json!({
            "type": "trade",
            "code": "KRW-BTC",
            "timestamp": 1696585056910_u64,
            "trade_date": "2023-10-06",
            "trade_time": "09:37:36",
            "trade_timestamp": 1696585056846_u64,
            "trade_price": 37625000.0,
            "trade_volume": 0.00008428,
            "ask_bid": "ASK",
            "prev_closing_price": 37296000.0,
            "change": "RISE",
            "change_price": 329000.0,
            "sequential_id": 1696585056846000_u64,
            "best_ask_price": 37626000.0,
            "best_ask_size": 0.04414411,
            "best_bid_price": 37624000.0,
            "best_bid_size": 0.01202163,
            "stream_type": "REALTIME"
        });
        let trade = parse_trade(&message, 1.0).expect("trade");
        assert_eq!(trade.exchange, ExchangeId::Upbit);
        assert_eq!(trade.symbol.as_str(), "BTC-KRW");
        assert_eq!(trade.side, Side::Sell);
        assert_eq!(trade.price.to_string(), "37625000.0");
        assert_eq!(trade.amount.to_string(), "0.00008428");
        assert_eq!(trade.id.as_deref(), Some("1696585056846000"));
        assert!((trade.exchange_ts - 1_696_585_056.846).abs() < 1e-6);
    }

    #[cfg(feature = "candles")]
    #[test]
    fn parses_documented_candle_frame_without_invented_completion() {
        let message = json!({
            "type": "candle.1s",
            "code": "KRW-BTC",
            "candle_date_time_utc": "2025-01-02T04:41:51",
            "opening_price": 130155000.0,
            "high_price": 130155000.0,
            "low_price": 130155000.0,
            "trade_price": 130155000.0,
            "candle_acc_trade_volume": 0.00003473,
            "candle_acc_trade_price": 4.52028315,
            "timestamp": 1735792911165_u64,
            "stream_type": "REALTIME"
        });
        let candle = parse_candle(&message, 1.0).expect("candle");
        assert_eq!(candle.exchange, ExchangeId::Upbit);
        assert_eq!(candle.symbol.as_str(), "BTC-KRW");
        assert_eq!(candle.interval, "1s");
        assert_eq!(candle.start, 1_735_792_911.0);
        assert_eq!(candle.end, 1_735_792_912.0);
        assert_eq!(candle.closed, None);
        assert_eq!(candle.trades, None);
        assert_eq!(candle.close.to_string(), "130155000.0");
        assert!((candle.exchange_ts - 1_735_792_911.165).abs() < 1e-6);
    }

    #[cfg(feature = "candles")]
    #[test]
    fn hour_candles_map_to_normalized_hours() {
        let message = json!({
            "type": "candle.60m",
            "code": "KRW-BTC",
            "candle_date_time_utc": "2025-01-02T04:00:00",
            "opening_price": 1.0, "high_price": 2.0, "low_price": 0.5,
            "trade_price": 1.5, "candle_acc_trade_volume": 10.0,
            "timestamp": 1735790400000_u64
        });
        let candle = parse_candle(&message, 1.0).expect("candle");
        assert_eq!(candle.interval, "1h");
        assert_eq!(candle.end - candle.start, 3_600.0);
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn parses_paired_orderbook_units_as_a_snapshot() {
        let message = json!({
            "type": "orderbook",
            "code": "KRW-BTC",
            "timestamp": 1746602359173_u64,
            "total_ask_size": 0.68780013,
            "total_bid_size": 0.78754733,
            "orderbook_units": [
                {"ask_price": 125056000.0, "bid_price": 124743000.0, "ask_size": 0.17, "bid_size": 0.17},
                {"ask_price": 125207000.0, "bid_price": 124332000.0, "ask_size": 0.09, "bid_size": 0.09}
            ],
            "stream_type": "REALTIME"
        });
        let book = parse_book(&message, 1.0).expect("book");
        let L2Book::Snapshot(snapshot) = &book else {
            panic!("upbit orderbook is snapshot-only");
        };
        assert_eq!(snapshot.symbol.as_str(), "BTC-KRW");
        assert_eq!(snapshot.bids.len(), 2);
        assert_eq!(snapshot.asks.len(), 2);
        assert_eq!(snapshot.bids[0].price.to_string(), "124743000.0");
        assert_eq!(snapshot.asks[0].price.to_string(), "125056000.0");
        assert!((snapshot.exchange_ts - 1_746_602_359.173).abs() < 1e-6);
    }
}
