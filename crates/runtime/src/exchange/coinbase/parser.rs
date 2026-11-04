//! Coinbase Advanced Trade public WebSocket payload parsing.
//!
//! Wire facts come from the official AsyncAPI contract and the documentation
//! recorded in `docs/coinbase-protocol.md`. Prices and sizes are transmitted as
//! JSON strings; `serde_json`'s arbitrary-precision feature plus
//! `Decimal::from_str_exact` preserves every transmitted decimal. Timestamps are
//! RFC 3339 and are converted to Unix seconds; the receive timestamp is kept
//! separate.

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

/// Envelope channel of a push (`ticker`, `l2_data`, `market_trades`,
/// `candles`, `heartbeats`, `subscriptions`).
pub fn channel(message: &Value) -> Option<&str> {
    message.get("channel").and_then(Value::as_str)
}

/// Event rows of a push; each entry carries a `snapshot`/`update` type and the
/// channel-specific array.
pub fn event_rows(message: &Value) -> &[Value] {
    message
        .get("events")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
}

/// Documented event type (`snapshot`/`update`).
pub fn event_type(row: &Value) -> Option<&str> {
    row.get("type").and_then(Value::as_str)
}

/// Native product id of one event row (`BTC-USD`).
pub fn event_product(row: &Value) -> Option<&str> {
    row.get("product_id").and_then(Value::as_str)
}

fn parse_decimal(value: &Value) -> Option<Decimal> {
    match value {
        Value::String(text) => Decimal::from_str_exact(text).ok(),
        // `arbitrary_precision` preserves the transmitted literal.
        Value::Number(number) => Decimal::from_str_exact(&number.to_string()).ok(),
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

/// Parses a documented RFC 3339 UTC timestamp (`2026-11-04T01:46:34.715564011Z`)
/// into Unix seconds. Only the `Z` UTC form is accepted, matching what the
/// public feed sends.
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

fn envelope_ts(message: &Value, received_ts: f64) -> f64 {
    message
        .get("timestamp")
        .and_then(Value::as_str)
        .and_then(parse_iso8601_seconds)
        .unwrap_or(received_ts)
}

/// Normalized normalized symbol -> native product form. Coinbase products are
/// already `BASE-QUOTE`; unknown shapes return `None`.
pub fn normalize_symbol(raw: &str) -> Option<Symbol> {
    let (base, quote) = raw.split_once('-')?;
    if base.is_empty() || quote.is_empty() || quote.contains('-') {
        return None;
    }
    Some(Symbol::spot(base, quote))
}

#[cfg(feature = "ticker")]
pub fn parse_ticker_for_symbol(row: &Value, received_ts: f64, symbol: &Symbol) -> Option<Ticker> {
    Some(Ticker {
        exchange: ExchangeId::Coinbase,
        symbol: symbol.clone(),
        bid: parse_decimal(row.get("best_bid")?)?,
        ask: parse_decimal(row.get("best_ask")?)?,
        // The ticker row carries no own timestamp; the envelope timestamp is
        // documented as the server send time.
        exchange_ts: received_ts,
        received_ts,
        implied_volatility: None,
    })
}

#[cfg(feature = "orderbook")]
pub fn parse_l1_book_for_symbol(
    row: &Value,
    exchange_ts: f64,
    received_ts: f64,
    symbol: &Symbol,
) -> Option<cryptofeed_orderbook::L1Book> {
    Some(cryptofeed_orderbook::L1Book {
        exchange: ExchangeId::Coinbase,
        symbol: symbol.clone(),
        bid: PriceLevel {
            price: parse_decimal(row.get("best_bid")?)?,
            amount: parse_decimal(row.get("best_bid_quantity")?)?,
        },
        ask: PriceLevel {
            price: parse_decimal(row.get("best_ask")?)?,
            amount: parse_decimal(row.get("best_ask_quantity")?)?,
        },
        exchange_ts,
        received_ts,
    })
}

#[cfg(feature = "trade")]
pub fn parse_trade_for_symbol(row: &Value, received_ts: f64, symbol: &Symbol) -> Option<Trade> {
    // Coinbase documents `side` as the maker side; the normalized model uses
    // the aggressor side, so the maker side is inverted.
    let side = match row.get("side")?.as_str()? {
        "BUY" => Side::Sell,
        "SELL" => Side::Buy,
        _ => return None,
    };
    Some(Trade {
        exchange: ExchangeId::Coinbase,
        symbol: symbol.clone(),
        side,
        amount: parse_decimal(row.get("size")?)?,
        price: parse_decimal(row.get("price")?)?,
        exchange_ts: row
            .get("time")
            .and_then(Value::as_str)
            .and_then(parse_iso8601_seconds)
            .unwrap_or(received_ts),
        received_ts,
        id: row
            .get("trade_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        implied_volatility: None,
    })
}

#[cfg(feature = "candles")]
pub fn parse_candle_for_symbol(
    row: &Value,
    interval: &str,
    exchange_ts: f64,
    received_ts: f64,
    symbol: &Symbol,
) -> Option<Candle> {
    let start = row.get("start")?.as_str()?.parse::<f64>().ok()?;
    let end = start + interval_seconds(interval)?;
    Some(Candle {
        exchange: ExchangeId::Coinbase,
        symbol: symbol.clone(),
        start,
        end,
        interval: interval.to_owned(),
        trades: None,
        open: parse_decimal(row.get("open")?)?,
        close: parse_decimal(row.get("close")?)?,
        high: parse_decimal(row.get("high")?)?,
        low: parse_decimal(row.get("low")?)?,
        volume: parse_decimal(row.get("volume")?)?,
        // The public channel does not transmit a closed flag.
        closed: None,
        exchange_ts,
        received_ts,
    })
}

#[cfg(feature = "candles")]
pub fn interval_seconds(interval: &str) -> Option<f64> {
    match interval {
        "5m" => Some(300.0),
        _ => None,
    }
}

/// One parsed `l2_data` event: a documented snapshot or an absolute-quantity
/// update.
#[cfg(feature = "orderbook")]
#[derive(Clone, Debug, PartialEq)]
pub struct BookUpdate {
    pub snapshot: bool,
    pub symbol: Symbol,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub exchange_ts: f64,
    pub received_ts: f64,
}

/// Parses the `updates` array of one `l2_data` event. `new_quantity` is the
/// updated absolute size at `price_level`; `0` removes the level. Unknown side
/// names, missing fields and non-exact decimals fail closed (`None`).
#[cfg(feature = "orderbook")]
pub fn parse_book_event(
    event: &Value,
    exchange_ts: f64,
    received_ts: f64,
    symbol: &Symbol,
) -> Option<BookUpdate> {
    let snapshot = match event_type(event)? {
        "snapshot" => true,
        "update" => false,
        _ => return None,
    };
    let mut bids = Vec::new();
    let mut asks = Vec::new();
    let updates = event.get("updates")?.as_array()?;
    if updates.is_empty() {
        return None;
    }
    for update in updates {
        let amount = parse_decimal(update.get("new_quantity")?)?;
        let level = PriceLevel {
            price: parse_decimal(update.get("price_level")?)?,
            amount,
        };
        match update.get("side")?.as_str()? {
            "bid" => bids.push(level),
            "offer" => asks.push(level),
            _ => return None,
        }
    }
    Some(BookUpdate {
        snapshot,
        symbol: symbol.clone(),
        bids,
        asks,
        exchange_ts,
        received_ts,
    })
}

/// Envelope timestamp accessor for callers that need the documented server
/// send time.
pub fn message_timestamp(message: &Value, received_ts: f64) -> f64 {
    envelope_ts(message, received_ts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[cfg(all(feature = "ticker", feature = "orderbook"))]
    #[test]
    fn parses_documented_ticker_bbo_and_sizes() {
        let row = json!({
            "type": "ticker", "product_id": "BTC-USD", "price": "76778.5",
            "volume_24_h": "7291.09260843", "low_24_h": "76238.54",
            "high_24_h": "77981.38", "price_percent_chg_24_h": "-0.34",
            "best_bid": "76778.5", "best_ask": "76778.51",
            "best_bid_quantity": "0.00520961", "best_ask_quantity": "0.00147487"
        });
        let symbol = Symbol::spot("BTC", "USD");
        let ticker = parse_ticker_for_symbol(&row, 1.0, &symbol).expect("ticker");
        assert_eq!(ticker.exchange, ExchangeId::Coinbase);
        assert_eq!(ticker.bid, Decimal::from_str_exact("76778.5").unwrap());
        assert_eq!(ticker.ask, Decimal::from_str_exact("76778.51").unwrap());
        let l1 = parse_l1_book_for_symbol(&row, 2.0, 3.0, &symbol).expect("l1");
        assert_eq!(
            l1.bid.amount,
            Decimal::from_str_exact("0.00520961").unwrap()
        );
        assert_eq!(
            l1.ask.amount,
            Decimal::from_str_exact("0.00147487").unwrap()
        );
        assert_eq!(l1.exchange_ts, 2.0);
    }

    #[cfg(feature = "trade")]
    #[test]
    fn inverts_documented_maker_side_to_aggressor_side() {
        let symbol = Symbol::spot("BTC", "USD");
        let maker_buy = json!({"trade_id":"1","product_id":"BTC-USD","price":"76778.5",
            "size":"0.01","side":"BUY","time":"2026-11-04T01:46:46.265419Z"});
        let trade = parse_trade_for_symbol(&maker_buy, 1.0, &symbol).expect("trade");
        assert_eq!(trade.side, Side::Sell);
        assert_eq!(trade.id.as_deref(), Some("1"));
        assert!((trade.exchange_ts - 1_793_756_806.265_419).abs() < 0.000_001);
        let maker_sell = json!({"trade_id":"2","product_id":"BTC-USD","price":"1",
            "size":"1","side":"SELL","time":"2026-11-04T01:46:46.265419Z"});
        assert_eq!(
            parse_trade_for_symbol(&maker_sell, 1.0, &symbol)
                .expect("trade")
                .side,
            Side::Buy
        );
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn parses_documented_level2_snapshot_and_absolute_update() {
        let symbol = Symbol::spot("BTC", "USD");
        let snapshot = json!({"type":"snapshot","product_id":"BTC-USD","updates":[
            {"side":"bid","event_time":"2026-11-04T01:47:08.029879Z",
             "price_level":"76760.06","new_quantity":"0.00522916"},
            {"side":"offer","event_time":"2026-11-04T01:47:08.029879Z",
             "price_level":"76760.07","new_quantity":"0.02"}
        ]});
        let update = parse_book_event(&snapshot, 1.0, 2.0, &symbol).expect("snapshot");
        assert!(update.snapshot);
        assert_eq!(update.bids.len(), 1);
        assert_eq!(update.asks.len(), 1);
        assert_eq!(
            update.bids[0].amount,
            Decimal::from_str_exact("0.00522916").unwrap()
        );

        let delta = json!({"type":"update","product_id":"BTC-USD","updates":[
            {"side":"bid","event_time":"2026-11-04T01:47:08.078018Z",
             "price_level":"76760.06","new_quantity":"0"}
        ]});
        let update = parse_book_event(&delta, 1.0, 2.0, &symbol).expect("update");
        assert!(!update.snapshot);
        assert_eq!(update.bids[0].amount, Decimal::ZERO);

        let malformed = json!({"type":"snapshot","product_id":"BTC-USD","updates":[
            {"side":"middle","price_level":"1","new_quantity":"1"}]});
        assert!(parse_book_event(&malformed, 1.0, 2.0, &symbol).is_none());
        let missing = json!({"type":"update","product_id":"BTC-USD","updates":[
            {"side":"bid","price_level":"1"}]});
        assert!(parse_book_event(&missing, 1.0, 2.0, &symbol).is_none());
    }

    #[cfg(feature = "candles")]
    #[test]
    fn parses_five_minute_candle_without_inventing_completion() {
        let symbol = Symbol::spot("BTC", "USD");
        let row = json!({"start":"1793724900","high":"77654.62","low":"77559.6",
            "open":"77576.65","close":"77623.15","volume":"20.02210661",
            "product_id":"BTC-USD"});
        let candle = parse_candle_for_symbol(&row, "5m", 10.0, 11.0, &symbol).expect("candle");
        assert_eq!(candle.interval, "5m");
        assert_eq!(candle.start, 1_793_724_900.0);
        assert_eq!(candle.end, 1_793_725_200.0);
        assert_eq!(candle.closed, None);
        assert_eq!(
            candle.volume,
            Decimal::from_str_exact("20.02210661").unwrap()
        );
        assert!(parse_candle_for_symbol(&row, "1m", 10.0, 11.0, &symbol).is_none());
    }

    #[test]
    fn normalizes_only_documented_product_shape() {
        assert_eq!(
            normalize_symbol("BTC-USD"),
            Some(Symbol::spot("BTC", "USD"))
        );
        assert_eq!(
            normalize_symbol("BTC-USDC"),
            Some(Symbol::spot("BTC", "USDC"))
        );
        assert!(normalize_symbol("BTCUSD").is_none());
        assert!(normalize_symbol("BTC-").is_none());
        assert!(normalize_symbol("BTC-USD-C").is_none());
    }

    #[test]
    fn parses_rfc3339_utc_timestamps_only() {
        assert_eq!(parse_iso8601_seconds("1970-01-01T00:00:00Z"), Some(0.0));
        assert_eq!(
            parse_iso8601_seconds("2026-11-04T01:46:46.265419Z"),
            Some(1_793_756_806.265_419)
        );
        assert!(parse_iso8601_seconds("2026-11-04T01:46:46+00:00").is_none());
        assert!(parse_iso8601_seconds("not-a-time").is_none());
    }

    #[test]
    fn parses_documented_envelope_shapes() {
        let message = json!({"channel":"l2_data","timestamp":"2026-11-04T01:47:08Z",
            "sequence_num":2,"events":[{"type":"snapshot","product_id":"BTC-USD",
            "updates":[{"side":"bid","price_level":"1","new_quantity":"1"}]}]});
        assert_eq!(channel(&message), Some("l2_data"));
        assert_eq!(event_rows(&message).len(), 1);
        assert_eq!(event_type(&event_rows(&message)[0]), Some("snapshot"));
        assert_eq!(event_product(&event_rows(&message)[0]), Some("BTC-USD"));
        assert_eq!(message_timestamp(&message, 0.0), 1_793_756_828.0);
    }
}
