//! KuCoin UTA WebSocket V2 payload parsing.
//!
//! Wire facts come from the official channel documentation recorded in
//! `docs/kucoin-protocol.md`. Every parser takes the raw frame and the local
//! receive clock, and returns normalized models only; sequence handling for
//! order books lives in the book-sync layer.
//!
//! Product identity is resolved by the adapter from the configured catalog
//! mapping (`data.s` -> normalized symbol) before these helpers run. The
//! standalone `parse_*` helpers derive a best-effort symbol from the native
//! form for fixtures and single-frame tests.

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
use cryptofeed_orderbook::{L2Book, L2BookDelta, L2BookSnapshot, PriceLevel};
#[cfg(feature = "ticker")]
use cryptofeed_ticker::Ticker;
#[cfg(feature = "trade")]
use cryptofeed_trade::Trade;

/// One order-book push. `first_seq`/`last_seq` are the documented `O`/`C`
/// values and are absent on snapshot-only pushes.
#[cfg(feature = "orderbook")]
#[derive(Clone, Debug, PartialEq)]
pub struct BookMessage {
    pub symbol: Symbol,
    pub snapshot: bool,
    pub first_seq: Option<u64>,
    pub last_seq: Option<u64>,
    pub book: L2Book,
}

/// Product family carried by a frame: the `T` topic suffix or the documented
/// `tradeType` field.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TradeType {
    Spot,
    Futures,
}

impl TradeType {
    fn from_message(message: &Value) -> Option<Self> {
        let topic = message.get("T").and_then(Value::as_str)?;
        if topic.ends_with(".FUTURES") {
            Some(TradeType::Futures)
        } else if topic.ends_with(".SPOT") {
            Some(TradeType::Spot)
        } else {
            None
        }
    }
}

/// Maps a native KuCoin symbol to the normalized model.
///
/// Spot symbols are `BASE-QUOTE`. Futures contracts add a trailing `M`
/// (`XBTUSDTM`); the legacy alias `XBT` becomes `BTC`. Dated contracts and
/// inverse contracts are resolved by the adapter from the catalog mapping;
/// unknown shapes fall back to a spot symbol with the raw value as the base so
/// dispatch can still reject them against the configured catalog instead of
/// panicking.
pub fn normalize_symbol(raw: &str, trade_type: TradeType) -> Symbol {
    match trade_type {
        TradeType::Spot => {
            let (base, quote) = raw
                .split_once('-')
                .filter(|(base, quote)| !base.is_empty() && !quote.is_empty())
                .map_or((raw, "UNKNOWN"), |(base, quote)| (base, quote));
            Symbol::spot(alias_base(base), quote)
        }
        TradeType::Futures => {
            let contract = raw.strip_suffix('M').unwrap_or(raw);
            let (base, quote) = ["USDT", "USDC", "USD"]
                .into_iter()
                .find_map(|quote| {
                    contract
                        .strip_suffix(quote)
                        .filter(|base| !base.is_empty())
                        .map(|base| (base, quote))
                })
                .unwrap_or((contract, "UNKNOWN"));
            Symbol::perpetual(alias_base(base), quote)
        }
    }
}

fn alias_base(base: &str) -> &str {
    if base.eq_ignore_ascii_case("XBT") {
        "BTC"
    } else {
        base
    }
}

pub(crate) fn parse_decimal(value: &Value) -> Option<Decimal> {
    match value {
        Value::String(text) => Decimal::from_str_exact(text).ok(),
        Value::Number(number) => Decimal::from_str_exact(&number.to_string()).ok(),
        _ => None,
    }
}

/// Milliseconds since the epoch from an integer or numeric string.
fn parse_millis(value: &Value) -> Option<f64> {
    match value {
        Value::String(text) => text.parse::<f64>().ok().map(|v| v / 1_000.0),
        Value::Number(number) => number.as_f64().map(|v| v / 1_000.0),
        _ => None,
    }
}

/// Nanoseconds since the epoch (the `M`/`P` publish clocks) to seconds.
fn parse_nanos(value: &Value) -> Option<f64> {
    match value {
        Value::String(text) => text.parse::<f64>().ok().map(|v| v / 1_000_000_000.0),
        Value::Number(number) => number.as_f64().map(|v| v / 1_000_000_000.0),
        _ => None,
    }
}

fn payload(message: &Value) -> Option<&Value> {
    message.get("d")
}

/// Native symbol of the payload (`data.s`).
pub fn native_symbol(message: &Value) -> Option<&str> {
    payload(message)?.get("s").and_then(Value::as_str)
}

/// Whether a frame carries the documented publish clock `P`.
pub fn is_market_frame(message: &Value) -> bool {
    message.get("T").and_then(Value::as_str).is_some()
}

fn raw_symbol(data: &Value) -> Option<&str> {
    data.get("s").and_then(Value::as_str)
}

fn message_symbol(message: &Value) -> Option<Symbol> {
    let trade_type = TradeType::from_message(message)?;
    Some(normalize_symbol(raw_symbol(payload(message)?)?, trade_type))
}

fn exchange_ts(message: &Value, data: &Value, received_ts: f64) -> f64 {
    parse_nanos(data.get("M").unwrap_or(&Value::Null))
        .or_else(|| message.get("P").and_then(parse_nanos))
        .unwrap_or(received_ts)
}

#[cfg(feature = "ticker")]
pub fn parse_ticker_for_symbol(
    message: &Value,
    received_ts: f64,
    symbol: &Symbol,
) -> Option<Ticker> {
    let data = payload(message)?;
    Some(Ticker {
        exchange: ExchangeId::Kucoin,
        symbol: symbol.clone(),
        bid: parse_decimal(data.get("b")?)?,
        ask: parse_decimal(data.get("a")?)?,
        exchange_ts: exchange_ts(message, data, received_ts),
        received_ts,
        implied_volatility: None,
    })
}

#[cfg(feature = "ticker")]
pub fn parse_ticker(message: &Value, received_ts: f64) -> Option<Ticker> {
    let symbol = message_symbol(message)?;
    parse_ticker_for_symbol(message, received_ts, &symbol)
}

#[cfg(feature = "trade")]
pub fn parse_trade_for_symbol(message: &Value, received_ts: f64, symbol: &Symbol) -> Option<Trade> {
    let data = payload(message)?;
    Some(Trade {
        exchange: ExchangeId::Kucoin,
        symbol: symbol.clone(),
        side: match data.get("S")?.as_str()? {
            "sell" | "SELL" => Side::Sell,
            _ => Side::Buy,
        },
        amount: parse_decimal(data.get("q")?)?,
        price: parse_decimal(data.get("p")?)?,
        exchange_ts: exchange_ts(message, data, received_ts),
        received_ts,
        id: data
            .get("ti")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        implied_volatility: None,
    })
}

#[cfg(feature = "trade")]
pub fn parse_trade(message: &Value, received_ts: f64) -> Option<Trade> {
    let symbol = message_symbol(message)?;
    parse_trade_for_symbol(message, received_ts, &symbol)
}

/// Maps a documented KuCoin kline label to the normalized interval.
pub fn normalize_interval(wire: &str) -> Option<&'static str> {
    Some(match wire {
        "1min" => "1m",
        "3min" => "3m",
        "5min" => "5m",
        "15min" => "15m",
        "30min" => "30m",
        "1hour" => "1h",
        "2hour" => "2h",
        "4hour" => "4h",
        "6hour" => "6h",
        "8hour" => "8h",
        "12hour" => "12h",
        "1day" => "1d",
        "1week" => "1w",
        "1month" => "1M",
        _ => return None,
    })
}

#[cfg(feature = "candles")]
pub fn parse_candle_for_symbol(
    message: &Value,
    received_ts: f64,
    symbol: &Symbol,
) -> Option<Candle> {
    let data = payload(message)?;
    let interval = normalize_interval(data.get("i")?.as_str()?)?;
    Some(Candle {
        exchange: ExchangeId::Kucoin,
        symbol: symbol.clone(),
        start: data.get("O")?.as_f64()?,
        end: data.get("C")?.as_f64()?,
        interval: interval.to_owned(),
        trades: None,
        open: parse_decimal(data.get("o")?)?,
        close: parse_decimal(data.get("c")?)?,
        high: parse_decimal(data.get("h")?)?,
        low: parse_decimal(data.get("l")?)?,
        volume: parse_decimal(data.get("v")?)?,
        closed: data.get("S").and_then(Value::as_bool),
        exchange_ts: data.get("C")?.as_f64()?,
        received_ts,
    })
}

#[cfg(feature = "candles")]
pub fn parse_candle(message: &Value, received_ts: f64) -> Option<Candle> {
    let symbol = message_symbol(message)?;
    parse_candle_for_symbol(message, received_ts, &symbol)
}

/// `mark-price` payload timestamp (`ts`, milliseconds); falls back to `P`.
fn mark_ts(message: &Value, data: &Value, received_ts: f64) -> f64 {
    parse_millis(data.get("ts").unwrap_or(&Value::Null))
        .or_else(|| message.get("P").and_then(parse_nanos))
        .unwrap_or(received_ts)
}

#[cfg(feature = "markprice")]
pub fn parse_mark_price_for_symbol(
    message: &Value,
    received_ts: f64,
    symbol: &Symbol,
) -> Option<MarkPrice> {
    let data = payload(message)?;
    Some(MarkPrice {
        exchange: ExchangeId::Kucoin,
        symbol: symbol.clone(),
        price: parse_decimal(data.get("mp")?)?,
        next_funding_time: None,
        predicted_rate: None,
        exchange_ts: mark_ts(message, data, received_ts),
        received_ts,
    })
}

#[cfg(feature = "markprice")]
pub fn parse_mark_price(message: &Value, received_ts: f64) -> Option<MarkPrice> {
    let symbol = normalize_symbol(native_symbol(message)?, TradeType::Futures);
    parse_mark_price_for_symbol(message, received_ts, &symbol)
}

#[cfg(feature = "index")]
pub fn parse_index_price_for_symbol(
    message: &Value,
    received_ts: f64,
    symbol: &Symbol,
) -> Option<IndexPrice> {
    let data = payload(message)?;
    Some(IndexPrice {
        exchange: ExchangeId::Kucoin,
        symbol: symbol.clone(),
        price: parse_decimal(data.get("ip")?)?,
        open_24h: None,
        high_24h: None,
        low_24h: None,
        exchange_ts: mark_ts(message, data, received_ts),
        received_ts,
    })
}

#[cfg(feature = "index")]
pub fn parse_index_price(message: &Value, received_ts: f64) -> Option<IndexPrice> {
    let symbol = normalize_symbol(native_symbol(message)?, TradeType::Futures);
    parse_index_price_for_symbol(message, received_ts, &symbol)
}

#[cfg(feature = "openinterest")]
pub fn parse_open_interest_for_symbol(
    message: &Value,
    received_ts: f64,
    symbol: &Symbol,
) -> Option<OpenInterest> {
    let data = payload(message)?;
    Some(OpenInterest {
        exchange: ExchangeId::Kucoin,
        symbol: symbol.clone(),
        open_interest: parse_decimal(data.get("oi")?)?,
        coin_quantity: None,
        value_usd: None,
        exchange_ts: mark_ts(message, data, received_ts),
        received_ts,
    })
}

#[cfg(feature = "openinterest")]
pub fn parse_open_interest(message: &Value, received_ts: f64) -> Option<OpenInterest> {
    let symbol = normalize_symbol(native_symbol(message)?, TradeType::Futures);
    parse_open_interest_for_symbol(message, received_ts, &symbol)
}

/// `funding-fee` payload. `ft` is the settlement time of the current rate and
/// becomes `next_funding_time`; `nt` is one period further and is deliberately
/// not substituted (the same rule as OKX `fundingTime`/`nextFundingTime`).
#[cfg(feature = "funding")]
pub fn parse_funding_for_symbol(
    message: &Value,
    received_ts: f64,
    symbol: &Symbol,
) -> Option<Funding> {
    let data = payload(message)?;
    Some(Funding {
        exchange: ExchangeId::Kucoin,
        symbol: symbol.clone(),
        mark_price: None,
        rate: data.get("fr").and_then(parse_decimal),
        next_funding_time: data.get("ft").and_then(parse_millis),
        predicted_rate: None,
        // The payload carries no observation time; the local receive clock is
        // the only defensible event time.
        exchange_ts: received_ts,
        received_ts,
    })
}

#[cfg(feature = "funding")]
pub fn parse_funding(message: &Value, received_ts: f64) -> Option<Funding> {
    let symbol = normalize_symbol(native_symbol(message)?, TradeType::Futures);
    parse_funding_for_symbol(message, received_ts, &symbol)
}

#[cfg(feature = "orderbook")]
fn parse_levels(value: Option<&Value>) -> Vec<PriceLevel> {
    value
        .and_then(Value::as_array)
        .map(|levels| {
            levels
                .iter()
                .filter_map(|level| {
                    let pair = level.as_array()?;
                    Some(PriceLevel {
                        price: parse_decimal(pair.first()?)?,
                        amount: parse_decimal(pair.get(1)?)?,
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Parses one `obu` push. `t: "snapshot"` is a full replacement for the
/// documented depth modes; `t: "delta"` carries absolute sizes per level and
/// the `O`/`C` sequence range.
#[cfg(feature = "orderbook")]
pub fn parse_book_for_symbol(
    message: &Value,
    received_ts: f64,
    symbol: &Symbol,
) -> Option<BookMessage> {
    let data = payload(message)?;
    let snapshot = match message.get("t").and_then(Value::as_str)? {
        "snapshot" => true,
        "delta" => false,
        _ => return None,
    };
    let exchange_ts = exchange_ts(message, data, received_ts);
    let bids = parse_levels(data.get("b"));
    let asks = parse_levels(data.get("a"));
    let book = if snapshot {
        L2Book::Snapshot(L2BookSnapshot {
            exchange: ExchangeId::Kucoin,
            symbol: symbol.clone(),
            bids,
            asks,
            exchange_ts,
            received_ts,
        })
    } else {
        L2Book::Delta(L2BookDelta {
            exchange: ExchangeId::Kucoin,
            symbol: symbol.clone(),
            bids,
            asks,
            exchange_ts,
            received_ts,
        })
    };
    Some(BookMessage {
        symbol: symbol.clone(),
        snapshot,
        first_seq: data.get("O").and_then(Value::as_u64),
        last_seq: data.get("C").and_then(Value::as_u64),
        book,
    })
}

#[cfg(feature = "orderbook")]
pub fn parse_book(message: &Value, received_ts: f64) -> Option<BookMessage> {
    let trade_type = TradeType::from_message(message)?;
    let symbol = normalize_symbol(raw_symbol(payload(message)?)?, trade_type);
    parse_book_for_symbol(message, received_ts, &symbol)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn normalizes_spot_and_futures_symbols_with_alias() {
        assert_eq!(
            normalize_symbol("BTC-USDT", TradeType::Spot).as_str(),
            "BTC-USDT"
        );
        assert_eq!(
            normalize_symbol("XBTUSDTM", TradeType::Futures).as_str(),
            "BTC-USDT-PERP"
        );
    }

    #[test]
    fn exposes_the_native_payload_symbol() {
        let message = json!({"T": "ticker.SPOT", "d": {"s": "BTC-USDT"}});
        assert_eq!(native_symbol(&message), Some("BTC-USDT"));
        assert!(is_market_frame(&message));
        assert!(!is_market_frame(&json!({"id": "1", "result": true})));
    }

    #[cfg(feature = "ticker")]
    #[test]
    fn parses_spot_ticker_from_documented_frame() {
        let message = json!({
            "T": "ticker.SPOT",
            "P": 1768206966101166007_u64,
            "d": {
                "A": "0.97675941", "B": "0.02052839", "E": 25958853459_u64,
                "M": 1768206966096000000_u64, "S": "BUY",
                "a": "90968.2", "b": "90968.1", "l": "90968.2",
                "q": "0.00109929", "s": "BTC-USDT"
            }
        });
        let ticker = parse_ticker(&message, 1768206966.2).expect("ticker");
        assert_eq!(ticker.exchange, ExchangeId::Kucoin);
        assert_eq!(ticker.symbol.as_str(), "BTC-USDT");
        assert_eq!(ticker.bid.to_string(), "90968.1");
        assert_eq!(ticker.ask.to_string(), "90968.2");
        assert!((ticker.exchange_ts - 1768206966.096).abs() < 1e-6);
    }

    #[cfg(feature = "trade")]
    #[test]
    fn parses_futures_trade_and_keeps_native_id() {
        let message = json!({
            "T": "trade.FUTURES",
            "P": 1768802538491160904_u64,
            "d": {
                "E": 20631804219899904_u64, "M": 1768802538480000000_u64,
                "S": "sell", "p": "92525.6", "q": "0.00008036",
                "s": "XBTUSDTM", "ti": "20631804219899904"
            }
        });
        let trade = parse_trade(&message, 1768802538.5).expect("trade");
        assert_eq!(trade.symbol.as_str(), "BTC-USDT-PERP");
        assert_eq!(trade.side, Side::Sell);
        assert_eq!(trade.price.to_string(), "92525.6");
        assert_eq!(trade.id.as_deref(), Some("20631804219899904"));
    }

    #[cfg(feature = "candles")]
    #[test]
    fn parses_candle_and_marks_final_flag() {
        let message = json!({
            "T": "kline.SPOT",
            "P": 1776090720846219590_u64,
            "d": {
                "o": "71725.1", "c": "71728.1", "l": "71725.1", "h": "71728.1",
                "S": false, "v": "0.01504768", "i": "1min",
                "a": "1079.297822768", "C": 1776090780, "s": "BTC-USDT",
                "O": 1776090720
            }
        });
        let candle = parse_candle(&message, 1776090721.0).expect("candle");
        assert_eq!(candle.symbol.as_str(), "BTC-USDT");
        assert_eq!(candle.interval, "1m");
        assert_eq!(candle.start, 1776090720.0);
        assert_eq!(candle.end, 1776090780.0);
        assert_eq!(candle.closed, Some(false));
        assert_eq!(candle.volume.to_string(), "0.01504768");
    }

    #[cfg(feature = "markprice")]
    #[test]
    fn splits_mark_price_payload_into_mark_index_and_open_interest() {
        let message = json!({
            "T": "mark-price",
            "P": 1731899129000_u64,
            "d": {"s": "XBTUSDTM", "mp": "90445", "ip": "90445", "oi": "23445", "ts": 1731899128999_u64}
        });
        let mark = parse_mark_price(&message, 1731899129.5).expect("mark");
        #[cfg(feature = "index")]
        let index = parse_index_price(&message, 1731899129.5).expect("index");
        #[cfg(feature = "openinterest")]
        let oi = parse_open_interest(&message, 1731899129.5).expect("open interest");
        assert_eq!(mark.price.to_string(), "90445");
        #[cfg(feature = "index")]
        assert_eq!(index.price.to_string(), "90445");
        #[cfg(feature = "openinterest")]
        assert_eq!(oi.open_interest.to_string(), "23445");
        assert!((mark.exchange_ts - 1731899128.999).abs() < 1e-6);
    }

    #[cfg(feature = "funding")]
    #[test]
    fn funding_uses_settlement_time_not_the_following_period() {
        let message = json!({
            "T": "funding-fee",
            "P": 1786809901214267234_u64,
            "d": {"s": "XBTUSDTM", "fr": "0.000072", "ft": 1786809600000_u64,
                  "lfr": "0.000069", "nt": 1786838400000_u64, "gl": 28800000_u64,
                  "fc": "0.003", "ff": "-0.003"}
        });
        let funding = parse_funding(&message, 1786809901.2).expect("funding");
        assert_eq!(funding.symbol.as_str(), "BTC-USDT-PERP");
        assert_eq!(funding.rate.unwrap().to_string(), "0.000072");
        assert_eq!(funding.next_funding_time, Some(1786809600.0));
        assert_eq!(funding.predicted_rate, None);
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn distinguishes_book_snapshot_from_delta_and_keeps_sequence() {
        let snapshot = json!({
            "T": "obu.SPOT", "dp": "1", "t": "snapshot", "P": 1768217874990007701_u64,
            "d": {"C": 25984468414_u64, "M": 1768217874986000000_u64,
                  "O": 25984468414_u64,
                  "a": [["90701.2", "0.5771583"]],
                  "b": [["90701.1", "0.13918404"]], "s": "BTC-USDT"}
        });
        let book = parse_book(&snapshot, 1768217874.99).expect("snapshot");
        assert!(book.snapshot);
        assert_eq!(book.first_seq, Some(25984468414));
        assert_eq!(book.last_seq, Some(25984468414));
        assert!(matches!(book.book, L2Book::Snapshot(_)));

        let delta = json!({
            "T": "obu.SPOT", "dp": "increment", "t": "delta", "P": 1768217909684719896_u64,
            "d": {"C": 25984544840_u64, "M": 1768217909683000000_u64,
                  "O": 25984544839_u64, "a": [],
                  "b": [["1", "12996.24994153"]], "s": "BTC-USDT"}
        });
        let book = parse_book(&delta, 1768217909.69).expect("delta");
        assert!(!book.snapshot);
        assert_eq!(book.first_seq, Some(25984544839));
        assert_eq!(book.last_seq, Some(25984544840));
        assert!(matches!(book.book, L2Book::Delta(_)));
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn feed_resolved_dated_contract_keeps_catalog_identity() {
        let message = json!({
            "T": "obu.FUTURES", "dp": "increment@10ms", "t": "snapshot",
            "P": 1793689190011225806_u64,
            "d": {"O": 1749017370449_u64, "C": 1749017370449_u64,
                  "M": 1793689190009000000_u64, "a": [], "b": [], "s": "XBTMZ26"}
        });
        let symbol = Symbol::futures("BTC", "USD", "261225");
        let book = parse_book_for_symbol(&message, 1.0, &symbol).expect("book");
        assert_eq!(book.symbol.as_str(), "BTC-USD-261225");
        assert_eq!(book.last_seq, Some(1749017370449));
    }

    #[test]
    fn maps_documented_kline_intervals_only() {
        assert_eq!(normalize_interval("1min"), Some("1m"));
        assert_eq!(normalize_interval("1month"), Some("1M"));
        assert_eq!(normalize_interval("6hour"), Some("6h"));
        assert_eq!(normalize_interval("7min"), None);
    }
}
