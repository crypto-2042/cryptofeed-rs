//! Deribit JSON-RPC v2 public notification parsing.
//!
//! Wire facts come from the official Deribit documentation recorded in
//! `docs/deribit-protocol.md`. Notifications are
//! `{"jsonrpc":"2.0","method":"subscription","params":{"channel":..,"data":..}}`;
//! prices/amounts are JSON numbers, parsed through their transmitted literal
//! with `serde_json`'s arbitrary-precision feature.

use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
use rust_decimal::Decimal;
use serde_json::Value;

#[cfg(feature = "candles")]
use cryptofeed_candles::Candle;
#[cfg(feature = "trade")]
use cryptofeed_core::model::Side;
#[cfg(feature = "funding")]
use cryptofeed_funding::Funding;
#[cfg(feature = "index")]
use cryptofeed_index::IndexPrice;
#[cfg(feature = "markprice")]
use cryptofeed_markprice::MarkPrice;
#[cfg(feature = "openinterest")]
use cryptofeed_openinterest::OpenInterest;
#[cfg(feature = "orderbook")]
use cryptofeed_orderbook::PriceLevel;
#[cfg(feature = "ticker")]
use cryptofeed_ticker::Ticker;
#[cfg(feature = "trade")]
use cryptofeed_trade::Trade;

fn parse_decimal(value: &Value) -> Option<Decimal> {
    match value {
        Value::String(text) => Decimal::from_str_exact(text).ok(),
        Value::Number(number) => Decimal::from_str_exact(&number.to_string()).ok(),
        _ => None,
    }
}

/// Milliseconds-since-epoch number -> Unix seconds.
pub fn parse_millis(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64().map(|millis| millis / 1000.0),
        Value::String(text) => text.parse::<f64>().ok().map(|millis| millis / 1000.0),
        _ => None,
    }
}

/// JSON-RPC `method` of a message (`subscription` for notifications).
pub fn method(message: &Value) -> Option<&str> {
    message.get("method").and_then(Value::as_str)
}

/// Notification channel, e.g. `book.BTC-PERPETUAL.100ms`.
pub fn channel(message: &Value) -> Option<&str> {
    message.pointer("/params/channel").and_then(Value::as_str)
}

/// Notification payload (`params.data`).
pub fn data(message: &Value) -> Option<&Value> {
    message.pointer("/params/data")
}

/// Heartbeat notifications carry `params.type` instead of a channel.
pub fn heartbeat_type(message: &Value) -> Option<&str> {
    message.pointer("/params/type").and_then(Value::as_str)
}

/// Channel family prefix before the first `.` (`ticker`, `trades`, `book`,
/// `chart`).
pub fn channel_family(channel: &str) -> &str {
    channel.split('.').next().unwrap_or(channel)
}

/// Instrument name embedded in a channel (`book.BTC-PERPETUAL.100ms` ->
/// `BTC-PERPETUAL`). `chart.trades.BTC-PERPETUAL.1` has two prefixes.
pub fn channel_instrument(channel: &str) -> Option<&str> {
    let mut parts = channel.split('.');
    let family = parts.next()?;
    if family == "chart" {
        // chart.trades.<instrument>.<resolution>
        parts.next()?;
    }
    parts.next()
}

/// Normalized candle interval -> seconds (Deribit `chart.trades` resolutions).
pub fn interval_seconds(interval: &str) -> Option<f64> {
    Some(match interval {
        "1m" => 60.0,
        "3m" => 180.0,
        "5m" => 300.0,
        "10m" => 600.0,
        "15m" => 900.0,
        "30m" => 1_800.0,
        "1h" => 3_600.0,
        "2h" => 7_200.0,
        "3h" => 10_800.0,
        "6h" => 21_600.0,
        "12h" => 43_200.0,
        "1d" => 86_400.0,
        _ => return None,
    })
}

#[cfg(feature = "ticker")]
pub fn parse_ticker_for_symbol(data: &Value, received_ts: f64, symbol: &Symbol) -> Option<Ticker> {
    Some(Ticker {
        exchange: ExchangeId::Deribit,
        symbol: symbol.clone(),
        bid: parse_decimal(data.get("best_bid_price")?)?,
        ask: parse_decimal(data.get("best_ask_price")?)?,
        exchange_ts: parse_millis(data.get("timestamp")?).unwrap_or(received_ts),
        received_ts,
        implied_volatility: None,
    })
}

#[cfg(feature = "markprice")]
pub fn parse_mark_price_for_symbol(
    data: &Value,
    received_ts: f64,
    symbol: &Symbol,
) -> Option<MarkPrice> {
    Some(MarkPrice {
        exchange: ExchangeId::Deribit,
        symbol: symbol.clone(),
        price: parse_decimal(data.get("mark_price")?)?,
        next_funding_time: None,
        predicted_rate: None,
        exchange_ts: parse_millis(data.get("timestamp")?).unwrap_or(received_ts),
        received_ts,
    })
}

#[cfg(feature = "index")]
pub fn parse_index_price_for_symbol(
    data: &Value,
    received_ts: f64,
    symbol: &Symbol,
) -> Option<IndexPrice> {
    Some(IndexPrice {
        exchange: ExchangeId::Deribit,
        symbol: symbol.clone(),
        price: parse_decimal(data.get("index_price")?)?,
        open_24h: None,
        high_24h: None,
        low_24h: None,
        exchange_ts: parse_millis(data.get("timestamp")?).unwrap_or(received_ts),
        received_ts,
    })
}

#[cfg(feature = "openinterest")]
pub fn parse_open_interest_for_symbol(
    data: &Value,
    received_ts: f64,
    symbol: &Symbol,
) -> Option<OpenInterest> {
    Some(OpenInterest {
        exchange: ExchangeId::Deribit,
        symbol: symbol.clone(),
        // Native unit for perpetuals/futures is documented USD notional; the
        // parser never invents a base-coin conversion.
        open_interest: parse_decimal(data.get("open_interest")?)?,
        coin_quantity: None,
        value_usd: None,
        exchange_ts: parse_millis(data.get("timestamp")?).unwrap_or(received_ts),
        received_ts,
    })
}

#[cfg(feature = "funding")]
pub fn parse_funding_for_symbol(
    data: &Value,
    received_ts: f64,
    symbol: &Symbol,
) -> Option<Funding> {
    Some(Funding {
        exchange: ExchangeId::Deribit,
        symbol: symbol.clone(),
        mark_price: data.get("mark_price").and_then(parse_decimal),
        rate: data.get("current_funding").and_then(parse_decimal),
        // The ticker channel transmits no next-funding timestamp.
        next_funding_time: None,
        predicted_rate: None,
        exchange_ts: parse_millis(data.get("timestamp")?).unwrap_or(received_ts),
        received_ts,
    })
}

#[cfg(feature = "trade")]
pub fn parse_trade_for_symbol(data: &Value, received_ts: f64, symbol: &Symbol) -> Option<Trade> {
    Some(Trade {
        exchange: ExchangeId::Deribit,
        symbol: symbol.clone(),
        side: match data.get("direction")?.as_str()? {
            "sell" => Side::Sell,
            "buy" => Side::Buy,
            _ => return None,
        },
        // Perpetual/futures amounts are documented in USD units; the native
        // amount is preserved.
        amount: parse_decimal(data.get("amount")?)?,
        price: parse_decimal(data.get("price")?)?,
        exchange_ts: parse_millis(data.get("timestamp")?).unwrap_or(received_ts),
        received_ts,
        id: data
            .get("trade_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        implied_volatility: None,
    })
}

#[cfg(feature = "candles")]
pub fn parse_candle_for_symbol(
    data: &Value,
    interval: &str,
    received_ts: f64,
    symbol: &Symbol,
) -> Option<Candle> {
    let start = parse_millis(data.get("tick")?)?;
    let seconds = interval_seconds(interval)?;
    Some(Candle {
        exchange: ExchangeId::Deribit,
        symbol: symbol.clone(),
        start,
        end: start + seconds,
        interval: interval.to_owned(),
        trades: None,
        open: parse_decimal(data.get("open")?)?,
        close: parse_decimal(data.get("close")?)?,
        high: parse_decimal(data.get("high")?)?,
        low: parse_decimal(data.get("low")?)?,
        volume: parse_decimal(data.get("volume")?)?,
        // `chart.trades` transmits no closed flag; completion is not inferred.
        closed: None,
        exchange_ts: start,
        received_ts,
    })
}

/// One parsed `book` notification.
#[cfg(feature = "orderbook")]
#[derive(Clone, Debug, PartialEq)]
pub struct BookUpdate {
    pub snapshot: bool,
    pub symbol: Symbol,
    pub change_id: u64,
    pub prev_change_id: Option<u64>,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub exchange_ts: f64,
    pub received_ts: f64,
}

#[cfg(feature = "orderbook")]
fn parse_levels(
    rows: Option<&Value>,
    exchange_ts: f64,
    received_ts: f64,
) -> Option<Vec<PriceLevel>> {
    let rows = rows?.as_array()?;
    let mut levels = Vec::with_capacity(rows.len());
    for row in rows {
        let parts = row.as_array()?;
        if parts.len() != 3 {
            return None;
        }
        let amount = match parts[0].as_str()? {
            "new" | "change" => parse_decimal(&parts[2])?,
            "delete" => Decimal::ZERO,
            _ => return None,
        };
        levels.push(PriceLevel {
            price: parse_decimal(&parts[1])?,
            amount,
        });
    }
    let _ = (exchange_ts, received_ts);
    Some(levels)
}

#[cfg(feature = "orderbook")]
pub fn parse_book_for_symbol(
    data: &Value,
    received_ts: f64,
    symbol: &Symbol,
) -> Option<BookUpdate> {
    let snapshot = match data.get("type")?.as_str()? {
        "snapshot" => true,
        "change" => false,
        _ => return None,
    };
    let change_id = data.get("change_id")?.as_u64()?;
    let prev_change_id = match data.get("prev_change_id") {
        None | Some(Value::Null) => None,
        Some(value) => Some(value.as_u64()?),
    };
    let exchange_ts = parse_millis(data.get("timestamp")?).unwrap_or(received_ts);
    Some(BookUpdate {
        snapshot,
        symbol: symbol.clone(),
        change_id,
        prev_change_id,
        bids: parse_levels(data.get("bids"), exchange_ts, received_ts)?,
        asks: parse_levels(data.get("asks"), exchange_ts, received_ts)?,
        exchange_ts,
        received_ts,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn perp() -> Symbol {
        Symbol::perpetual("BTC", "USD")
    }

    #[cfg(all(
        feature = "ticker",
        feature = "markprice",
        feature = "index",
        feature = "openinterest",
        feature = "funding"
    ))]
    #[test]
    fn parses_documented_ticker_split_into_normalized_categories() {
        let data = json!({
            "timestamp": 1793757174238_i64, "state": "open",
            "stats": {"high": 77999.5, "low": 76265.5, "price_change": -0.3546,
                      "volume": 23909.30, "volume_usd": 1841519840.0},
            "index_price": 76833.47, "instrument_name": "BTC-PERPETUAL",
            "last_price": 76863.5, "settlement_price": 76712.08,
            "open_interest": 657029240, "mark_price": 76862.37,
            "current_funding": 1.2614e-4, "funding_8h": 7.185e-5,
            "best_ask_price": 76864.0, "best_bid_price": 76863.5,
            "best_ask_amount": 44630.0, "best_bid_amount": 17530.0
        });
        let ticker = parse_ticker_for_symbol(&data, 1.0, &perp()).expect("ticker");
        assert_eq!(ticker.bid, Decimal::from_str_exact("76863.5").unwrap());
        assert_eq!(ticker.ask, Decimal::from_str_exact("76864").unwrap());
        assert!((ticker.exchange_ts - 1_793_757_174.238).abs() < 0.000_001);
        let mark = parse_mark_price_for_symbol(&data, 1.0, &perp()).expect("mark");
        assert_eq!(mark.price, Decimal::from_str_exact("76862.37").unwrap());
        let index = parse_index_price_for_symbol(&data, 1.0, &perp()).expect("index");
        assert_eq!(index.price, Decimal::from_str_exact("76833.47").unwrap());
        let oi = parse_open_interest_for_symbol(&data, 1.0, &perp()).expect("oi");
        assert_eq!(oi.open_interest, Decimal::from(657_029_240));
        assert_eq!(oi.coin_quantity, None);
        let funding = parse_funding_for_symbol(&data, 1.0, &perp()).expect("funding");
        assert_eq!(
            funding.rate,
            Some(Decimal::from_str_exact("0.00012614").unwrap())
        );
        assert_eq!(funding.next_funding_time, None);
        assert_eq!(
            funding.mark_price,
            Some(Decimal::from_str_exact("76862.37").unwrap())
        );
    }

    #[cfg(feature = "trade")]
    #[test]
    fn parses_documented_trade_array_and_direction() {
        let trade = json!({
            "timestamp": 1793757174445_i64, "price": 76863.5, "direction": "sell",
            "index_price": 76833.47, "instrument_name": "BTC-PERPETUAL",
            "trade_seq": 313325404, "amount": 2220.0, "mark_price": 76862.37,
            "tick_direction": 1, "trade_id": "460199050", "contracts": 222.0
        });
        let trade = parse_trade_for_symbol(&trade, 1.0, &perp()).expect("trade");
        assert_eq!(trade.side, Side::Sell);
        assert_eq!(trade.amount, Decimal::from(2220));
        assert_eq!(trade.id.as_deref(), Some("460199050"));
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn parses_documented_book_snapshot_and_change_chain() {
        let snapshot = json!({
            "timestamp": 1793757173936_i64, "type": "snapshot",
            "change_id": 181199669586_u64, "instrument_name": "BTC-PERPETUAL",
            "bids": [["new", 76863.5, 17530.0], ["new", 76862.5, 1.2e3]],
            "asks": [["new", 76864.0, 44630.0]]
        });
        let update = parse_book_for_symbol(&snapshot, 1.0, &perp()).expect("snapshot");
        assert!(update.snapshot);
        assert_eq!(update.change_id, 181_199_669_586);
        assert_eq!(update.prev_change_id, None);
        assert_eq!(update.bids.len(), 2);
        assert_eq!(update.bids[0].amount, Decimal::from(17_530));

        let change = json!({
            "timestamp": 1793757173999_i64, "type": "change",
            "change_id": 181199669587_u64, "prev_change_id": 181199669586_u64,
            "instrument_name": "BTC-PERPETUAL",
            "bids": [["delete", 76863.5, 0], ["change", 76862.5, 900]],
            "asks": [["new", 76864.5, 10]]
        });
        let update = parse_book_for_symbol(&change, 1.0, &perp()).expect("change");
        assert!(!update.snapshot);
        assert_eq!(update.prev_change_id, Some(181_199_669_586));
        assert_eq!(update.bids[0].amount, Decimal::ZERO);
        assert_eq!(update.bids[1].amount, Decimal::from(900));

        let malformed = json!({"timestamp": 1, "type": "change", "change_id": 2,
            "prev_change_id": 1, "bids": [["move", 1, 2]], "asks": []});
        assert!(parse_book_for_symbol(&malformed, 1.0, &perp()).is_none());
    }

    #[cfg(feature = "candles")]
    #[test]
    fn parses_documented_chart_candle_without_completion_flag() {
        let data = json!({"close": 76863.5, "high": 76883.0, "low": 76830.0,
            "open": 76883.0, "tick": 1793757120000_i64, "cost": 1972220.0,
            "volume": 25.66235459});
        let candle = parse_candle_for_symbol(&data, "1m", 1.0, &perp()).expect("candle");
        assert_eq!(candle.start, 1_793_757_120.0);
        assert_eq!(candle.end, 1_793_757_180.0);
        assert_eq!(candle.interval, "1m");
        assert_eq!(candle.closed, None);
        assert_eq!(
            candle.volume,
            Decimal::from_str_exact("25.66235459").unwrap()
        );
    }

    #[test]
    fn channel_helpers_split_documented_addresses() {
        let message = json!({"jsonrpc": "2.0", "method": "subscription",
            "params": {"channel": "book.BTC-PERPETUAL.100ms",
                       "data": {"type": "snapshot"}}});
        assert_eq!(method(&message), Some("subscription"));
        assert_eq!(channel(&message), Some("book.BTC-PERPETUAL.100ms"));
        assert_eq!(channel_family("book.BTC-PERPETUAL.100ms"), "book");
        assert_eq!(
            channel_instrument("book.BTC-PERPETUAL.100ms"),
            Some("BTC-PERPETUAL")
        );
        assert_eq!(
            channel_instrument("chart.trades.BTC-PERPETUAL.1"),
            Some("BTC-PERPETUAL")
        );
        assert_eq!(data(&message), Some(&json!({"type": "snapshot"})));
        assert_eq!(
            heartbeat_type(&json!({"params": {"type": "heartbeat"}})),
            Some("heartbeat")
        );
    }
}
