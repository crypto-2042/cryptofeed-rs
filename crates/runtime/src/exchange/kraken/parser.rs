//! Kraken Spot WebSocket v2 payload parsing.
//!
//! Wire facts come from the official channel documentation and the live shape
//! recorded in `docs/kraken-protocol.md`; timestamps are ISO-8601 and are
//! converted to Unix seconds. Price/quantity JSON numbers are parsed from their
//! transmitted literal through `serde_json`'s arbitrary precision, preserving
//! the scale needed by the book checksum.

use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
use rust_decimal::Decimal;
use serde_json::Value;

#[cfg(feature = "candles")]
use cryptofeed_candles::Candle;
#[cfg(feature = "trade")]
use cryptofeed_core::model::Side;
#[cfg(feature = "orderbook")]
use cryptofeed_orderbook::PriceLevel;
#[cfg(feature = "ticker")]
use cryptofeed_ticker::Ticker;
#[cfg(feature = "trade")]
use cryptofeed_trade::Trade;

/// Legacy Kraken asset codes that the WebSocket v2 symbol namespace spells
/// differently from the REST `AssetPairs` `wsname`. Verified live on
/// 2026-11-03: `XBT/USD` and `XDG/USD` are rejected as unsupported while
/// `BTC/USD` and `DOGE/USD` subscribe. The alias is applied to both sides so
/// `AAVE/XBT` becomes `AAVE/BTC`.
pub fn wire_asset(asset: &str) -> &str {
    match asset {
        "XBT" => "BTC",
        "XDG" => "DOGE",
        other => other,
    }
}

/// Normalized wire form (`BTC/USD`) -> normalized symbol (`BTC-USD`).
pub fn normalize_ws_symbol(raw: &str) -> String {
    raw.replace('/', "-")
}

/// Parses a WebSocket v2 pair symbol (`BTC/USD`) into the normalized model.
/// Unknown shapes return `None`; the adapter only accepts configured pairs.
pub fn normalize_symbol(raw: &str) -> Option<Symbol> {
    let (base, quote) = raw.split_once('/')?;
    if base.is_empty() || quote.is_empty() || quote.contains('/') {
        return None;
    }
    Some(Symbol::spot(wire_asset(base), wire_asset(quote)))
}

fn parse_decimal(value: &Value) -> Option<Decimal> {
    match value {
        Value::String(text) => Decimal::from_str_exact(text).ok(),
        // `arbitrary_precision` keeps the transmitted literal; `to_string`
        // therefore preserves trailing zeros and the scale used by the
        // exchange checksum.
        Value::Number(number) => Decimal::from_str_exact(&number.to_string()).ok(),
        _ => None,
    }
}

fn parse_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.parse().ok(),
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

/// Parses the documented RFC 3339 timestamp (`2026-11-03T07:02:36.699826Z`)
/// into Unix seconds. Only the UTC `Z` form is accepted.
pub fn parse_iso8601_seconds(text: &str) -> Option<f64> {
    let (date, rest) = text.split_once('T')?;
    let rest = rest.strip_suffix('Z')?;
    let mut date_parts = date.split('-');
    let year: i64 = date_parts.next()?.parse().ok()?;
    let month: u32 = date_parts.next()?.parse().ok()?;
    let day: u32 = date_parts.next()?.parse().ok()?;
    if date_parts.next().is_some() || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let (clock, fraction) = match rest.split_once('.') {
        Some((clock, fraction)) => (clock, Some(fraction)),
        None => (rest, None),
    };
    let mut clock_parts = clock.split(':');
    let hour: u32 = clock_parts.next()?.parse().ok()?;
    let minute: u32 = clock_parts.next()?.parse().ok()?;
    let second: u32 = clock_parts.next()?.parse().ok()?;
    if clock_parts.next().is_some() || hour > 23 || minute > 59 || second > 60 {
        return None;
    }
    let fraction = match fraction {
        Some(digits) => {
            if digits.is_empty()
                || digits.len() > 9
                || !digits.chars().all(|ch| ch.is_ascii_digit())
            {
                return None;
            }
            let value: f64 = digits.parse().ok()?;
            value / 10f64.powi(digits.len() as i32)
        }
        None => 0.0,
    };
    let days = days_from_civil(year, month, day);
    Some(
        days as f64 * 86_400.0
            + f64::from(hour) * 3_600.0
            + f64::from(minute) * 60.0
            + f64::from(second)
            + fraction,
    )
}

/// Normalized interval for a documented Kraken OHLC interval in minutes.
pub fn normalize_interval_minutes(minutes: u64) -> Option<&'static str> {
    Some(match minutes {
        1 => "1m",
        5 => "5m",
        15 => "15m",
        30 => "30m",
        60 => "1h",
        240 => "4h",
        1440 => "1d",
        10080 => "1w",
        _ => return None,
    })
}

/// Channel name of a market push (`ticker`, `trade`, `ohlc`, `book`).
pub fn channel(message: &Value) -> Option<&str> {
    message.get("channel").and_then(Value::as_str)
}

/// `snapshot`/`update` push type.
pub fn message_type(message: &Value) -> Option<&str> {
    message.get("type").and_then(Value::as_str)
}

/// Payload rows of a market push.
pub fn data_rows(message: &Value) -> &[Value] {
    message
        .get("data")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
}

/// Native pair of one payload row (`BTC/USD`).
pub fn row_symbol(row: &Value) -> Option<&str> {
    row.get("symbol").and_then(Value::as_str)
}

fn row_ts(row: &Value, received_ts: f64) -> f64 {
    row.get("timestamp")
        .and_then(Value::as_str)
        .and_then(parse_iso8601_seconds)
        .unwrap_or(received_ts)
}

#[cfg(feature = "ticker")]
pub fn parse_ticker_for_symbol(row: &Value, received_ts: f64, symbol: &Symbol) -> Option<Ticker> {
    Some(Ticker {
        exchange: ExchangeId::Kraken,
        symbol: symbol.clone(),
        bid: parse_decimal(row.get("bid")?)?,
        ask: parse_decimal(row.get("ask")?)?,
        exchange_ts: row_ts(row, received_ts),
        received_ts,
        implied_volatility: None,
    })
}

#[cfg(feature = "ticker")]
pub fn parse_tickers(message: &Value, received_ts: f64) -> Vec<Ticker> {
    data_rows(message)
        .iter()
        .filter_map(|row| {
            let symbol = normalize_symbol(row_symbol(row)?)?;
            parse_ticker_for_symbol(row, received_ts, &symbol)
        })
        .collect()
}

#[cfg(feature = "trade")]
pub fn parse_trade_for_symbol(row: &Value, received_ts: f64, symbol: &Symbol) -> Option<Trade> {
    Some(Trade {
        exchange: ExchangeId::Kraken,
        symbol: symbol.clone(),
        side: match row.get("side")?.as_str()? {
            "sell" => Side::Sell,
            _ => Side::Buy,
        },
        amount: parse_decimal(row.get("qty")?)?,
        price: parse_decimal(row.get("price")?)?,
        exchange_ts: row_ts(row, received_ts),
        received_ts,
        id: row.get("trade_id").map(|value| {
            value
                .as_str()
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| value.to_string())
        }),
        implied_volatility: None,
    })
}

#[cfg(feature = "trade")]
pub fn parse_trades(message: &Value, received_ts: f64) -> Vec<Trade> {
    data_rows(message)
        .iter()
        .filter_map(|row| {
            let symbol = normalize_symbol(row_symbol(row)?)?;
            parse_trade_for_symbol(row, received_ts, &symbol)
        })
        .collect()
}

#[cfg(feature = "candles")]
pub fn parse_candle_for_symbol(row: &Value, received_ts: f64, symbol: &Symbol) -> Option<Candle> {
    let minutes = parse_u64(row.get("interval")?)?;
    let interval = normalize_interval_minutes(minutes)?;
    let start = row
        .get("interval_begin")
        .and_then(Value::as_str)
        .and_then(parse_iso8601_seconds)?;
    let end = start + minutes as f64 * 60.0;
    Some(Candle {
        exchange: ExchangeId::Kraken,
        symbol: symbol.clone(),
        start,
        end,
        interval: interval.to_owned(),
        trades: row.get("trades").and_then(parse_u64),
        open: parse_decimal(row.get("open")?)?,
        close: parse_decimal(row.get("close")?)?,
        high: parse_decimal(row.get("high")?)?,
        low: parse_decimal(row.get("low")?)?,
        volume: parse_decimal(row.get("volume")?)?,
        // Kraken does not mark OHLC bars final; elapsed time is never used to
        // infer completion.
        closed: None,
        exchange_ts: end,
        received_ts,
    })
}

#[cfg(feature = "candles")]
pub fn parse_candles(message: &Value, received_ts: f64) -> Vec<Candle> {
    data_rows(message)
        .iter()
        .filter_map(|row| {
            let symbol = normalize_symbol(row_symbol(row)?)?;
            parse_candle_for_symbol(row, received_ts, &symbol)
        })
        .collect()
}

/// One `book` push for one symbol. The CRC32 checksum covers the top ten
/// levels of the resulting book and is verified by the book-sync layer.
#[cfg(feature = "orderbook")]
#[derive(Clone, Debug, PartialEq)]
pub struct BookUpdate {
    pub symbol: Symbol,
    pub snapshot: bool,
    pub checksum: u32,
    pub exchange_ts: f64,
    pub received_ts: f64,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
}

#[cfg(feature = "orderbook")]
fn parse_levels(value: Option<&Value>) -> Vec<PriceLevel> {
    value
        .and_then(Value::as_array)
        .map(|levels| {
            levels
                .iter()
                .filter_map(|level| {
                    Some(PriceLevel {
                        price: parse_decimal(level.get("price")?)?,
                        amount: parse_decimal(level.get("qty")?)?,
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(feature = "orderbook")]
pub fn parse_book_row(
    row: &Value,
    snapshot: bool,
    received_ts: f64,
    symbol: &Symbol,
) -> Option<BookUpdate> {
    let checksum = parse_u64(row.get("checksum")?)?;
    Some(BookUpdate {
        symbol: symbol.clone(),
        snapshot,
        checksum: checksum as u32,
        exchange_ts: row_ts(row, received_ts),
        received_ts,
        bids: parse_levels(row.get("bids")),
        asks: parse_levels(row.get("asks")),
    })
}

#[cfg(test)]
mod tests {
    use super::{
        channel, message_type, normalize_interval_minutes, normalize_symbol, normalize_ws_symbol,
        parse_iso8601_seconds, parse_u64, wire_asset,
    };
    use cryptofeed_core::symbol::Symbol;

    #[test]
    fn normalizes_kraken_slash_symbol() {
        assert_eq!(normalize_ws_symbol("BTC/USD"), "BTC-USD");
        assert_eq!(
            normalize_symbol("BTC/USD"),
            Some(Symbol::spot("BTC", "USD"))
        );
        assert_eq!(
            normalize_symbol("XBT/USDT"),
            Some(Symbol::spot("BTC", "USDT"))
        );
        assert_eq!(
            normalize_symbol("AAVE/XBT"),
            Some(Symbol::spot("AAVE", "BTC"))
        );
        assert_eq!(
            normalize_symbol("XDG/USD"),
            Some(Symbol::spot("DOGE", "USD"))
        );
        assert_eq!(normalize_symbol("BTCUSD"), None);
        assert_eq!(wire_asset("XBT"), "BTC");
        assert_eq!(wire_asset("XDG"), "DOGE");
    }

    #[test]
    fn parses_documented_rfc3339_timestamps() {
        // 2026-11-03T07:02:36.699826Z
        let seconds = parse_iso8601_seconds("2026-11-03T07:02:36.699826Z").expect("timestamp");
        assert!((seconds - 1_793_689_356.699_826).abs() < 1e-6);
        assert_eq!(
            parse_iso8601_seconds("2022-12-25T09:30:59.123456Z"),
            Some(1_671_960_659.123_456)
        );
        assert_eq!(
            parse_iso8601_seconds("2022-12-25T09:30:59Z"),
            Some(1_671_960_659.0)
        );
        for invalid in [
            "2022-12-25 09:30:59Z",
            "2022-12-25T09:30:59+00:00",
            "2022-13-25T09:30:59Z",
            "2022-12-25T25:30:59Z",
            "2022-12-25T09:30:59.Z",
        ] {
            assert_eq!(parse_iso8601_seconds(invalid), None, "{invalid}");
        }
    }

    #[test]
    fn maps_documented_ohlc_intervals() {
        for (minutes, interval) in [
            (1, "1m"),
            (5, "5m"),
            (15, "15m"),
            (30, "30m"),
            (60, "1h"),
            (240, "4h"),
            (1440, "1d"),
            (10080, "1w"),
        ] {
            assert_eq!(normalize_interval_minutes(minutes), Some(interval));
        }
        assert_eq!(normalize_interval_minutes(21600), None);
        assert_eq!(normalize_interval_minutes(2), None);
    }

    #[test]
    fn reads_channel_type_and_rows() {
        let message = serde_json::json!({
            "channel": "trade", "type": "update",
            "data": [{"symbol": "BTC/USD", "side": "buy", "price": 76449.6,
                      "qty": 0.00043059, "ord_type": "limit",
                      "trade_id": 114005505, "timestamp": "2026-11-03T07:02:37.171033Z"}]
        });
        assert_eq!(channel(&message), Some("trade"));
        assert_eq!(message_type(&message), Some("update"));
        assert_eq!(super::data_rows(&message).len(), 1);
        assert_eq!(
            super::row_symbol(&super::data_rows(&message)[0]),
            Some("BTC/USD")
        );
        assert_eq!(parse_u64(&serde_json::json!(42)), Some(42));
        assert_eq!(parse_u64(&serde_json::json!("42")), Some(42));
        assert_eq!(parse_u64(&serde_json::json!(-1)), None);
    }

    #[cfg(feature = "ticker")]
    #[test]
    fn parses_ticker_rows_from_the_documented_shape() {
        let message = serde_json::json!({
            "channel": "ticker", "type": "snapshot",
            "data": [{
                "symbol": "BTC/USD", "bid": 76497.5, "bid_qty": 0.00065022,
                "ask": 76497.6, "ask_qty": 0.83895981, "last": 76497.6,
                "volume": 2770.68084624, "vwap": 77090.9, "low": 76254.4,
                "high": 78269.8, "change": -1587.9, "change_pct": -2.03,
                "trades": 140869, "timestamp": "2026-11-03T07:01:22.016355Z"
            }]
        });
        let tickers = super::parse_tickers(&message, 1.0);
        assert_eq!(tickers.len(), 1);
        assert_eq!(tickers[0].symbol.as_str(), "BTC-USD");
        assert_eq!(tickers[0].bid.to_string(), "76497.5");
        assert_eq!(tickers[0].ask.to_string(), "76497.6");
        assert!((tickers[0].exchange_ts - 1_793_689_282.016_355).abs() < 1e-6);
    }

    #[cfg(feature = "trade")]
    #[test]
    fn parses_trade_rows_and_keeps_native_id() {
        let message = serde_json::json!({
            "channel": "trade", "type": "update",
            "data": [{"symbol": "BTC/USD", "side": "sell", "price": 76449.6,
                      "qty": 0.00043059, "ord_type": "limit",
                      "trade_id": 114005505, "timestamp": "2026-11-03T07:02:37.171033Z"}]
        });
        let trades = super::parse_trades(&message, 1.0);
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].symbol.as_str(), "BTC-USD");
        assert_eq!(trades[0].side, cryptofeed_core::model::Side::Sell);
        assert_eq!(trades[0].id.as_deref(), Some("114005505"));
    }

    #[cfg(feature = "candles")]
    #[test]
    fn parses_ohlc_rows_with_interval_begin() {
        let message = serde_json::json!({
            "channel": "ohlc", "type": "snapshot",
            "timestamp": "2026-11-03T07:01:24.187297694Z",
            "data": [{"symbol": "BTC/USD", "open": 76381.0, "high": 76399.4,
                      "low": 76376.7, "close": 76399.4, "trades": 70,
                      "volume": 0.63763428, "vwap": 76384.9,
                      "interval_begin": "2026-11-03T06:52:00.000000000Z",
                      "interval": 1,
                      "timestamp": "2026-11-03T06:53:00.000000Z"}]
        });
        let candles = super::parse_candles(&message, 1.0);
        assert_eq!(candles.len(), 1);
        assert_eq!(candles[0].interval, "1m");
        assert_eq!(candles[0].trades, Some(70));
        assert_eq!(candles[0].start, 1_793_688_720.0);
        assert_eq!(candles[0].end, 1_793_688_780.0);
        assert_eq!(candles[0].closed, None);
        assert_eq!(candles[0].close.to_string(), "76399.4");
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn parses_book_rows_with_object_levels_and_checksum() {
        let row = serde_json::json!({
            "symbol": "BTC/USD",
            "bids": [{"price": 76497.5, "qty": 0.00065022}],
            "asks": [{"price": 76497.6, "qty": 0.94670723}],
            "checksum": 783415404,
            "timestamp": "2026-11-03T07:01:24.552449Z"
        });
        let update = super::parse_book_row(&row, true, 1.0, &Symbol::spot("BTC", "USD"))
            .expect("book update");
        assert!(update.snapshot);
        assert_eq!(update.checksum, 783_415_404);
        assert_eq!(update.bids[0].price.to_string(), "76497.5");
        assert_eq!(update.asks[0].amount.to_string(), "0.94670723");
    }
}
