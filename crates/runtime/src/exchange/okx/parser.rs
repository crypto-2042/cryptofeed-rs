#[cfg(feature = "orderbook")]
use super::book_sync::{OkxBookAction, OkxDepthUpdate};
#[cfg(feature = "candles")]
use cryptofeed_candles::Candle;
#[cfg(any(feature = "trade", feature = "liquidations"))]
use cryptofeed_core::model::Side;
use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
#[cfg(feature = "funding")]
use cryptofeed_funding::Funding;
#[cfg(feature = "index")]
use cryptofeed_index::IndexPrice;
#[cfg(feature = "liquidations")]
use cryptofeed_liquidations::{Liquidation, LiquidationStatus};
#[cfg(feature = "markprice")]
use cryptofeed_markprice::MarkPrice;
#[cfg(feature = "openinterest")]
use cryptofeed_openinterest::OpenInterest;
#[cfg(feature = "orderbook")]
use cryptofeed_orderbook::{L1Book, L2Book, L2BookDelta, L2BookSnapshot, PriceLevel};
#[cfg(feature = "ticker")]
use cryptofeed_ticker::Ticker;
#[cfg(feature = "trade")]
use cryptofeed_trade::Trade;
use rust_decimal::Decimal;
use serde_json::Value;

#[cfg(feature = "ticker")]
pub fn parse_ticker(message: &Value, received_ts: f64) -> Option<Ticker> {
    parse_tickers(message, received_ts).into_iter().next()
}

#[cfg(feature = "ticker")]
pub fn parse_tickers(message: &Value, received_ts: f64) -> Vec<Ticker> {
    let Some(raw_symbol) = message
        .get("arg")
        .and_then(|arg| arg.get("instId"))
        .and_then(Value::as_str)
    else {
        return Vec::new();
    };
    let symbol = parse_symbol(raw_symbol);
    let Some(rows) = message.get("data").and_then(Value::as_array) else {
        return Vec::new();
    };
    rows.iter()
        .filter_map(|row| parse_ticker_row(row, received_ts, &symbol))
        .collect()
}

#[cfg(feature = "ticker")]
fn parse_ticker_row(first: &Value, received_ts: f64, symbol: &Symbol) -> Option<Ticker> {
    Some(Ticker {
        exchange: ExchangeId::Okx,
        symbol: symbol.clone(),
        bid: parse_decimal(first.get("bidPx")?)?,
        ask: parse_decimal(first.get("askPx")?)?,
        exchange_ts: first
            .get("ts")
            .and_then(parse_millis)
            .unwrap_or(received_ts),
        received_ts,
        implied_volatility: None,
    })
}

#[cfg(feature = "trade")]
pub fn parse_trade(message: &Value, received_ts: f64) -> Option<Trade> {
    parse_trades(message, received_ts).into_iter().next()
}

#[cfg(feature = "trade")]
pub fn parse_trades(message: &Value, received_ts: f64) -> Vec<Trade> {
    let Some(raw_symbol) = message
        .get("arg")
        .and_then(|arg| arg.get("instId"))
        .and_then(Value::as_str)
    else {
        return Vec::new();
    };
    let symbol = parse_symbol(raw_symbol);
    let Some(rows) = message.get("data").and_then(Value::as_array) else {
        return Vec::new();
    };
    rows.iter()
        .filter_map(|row| parse_trade_row(row, received_ts, &symbol))
        .collect()
}

#[cfg(feature = "trade")]
fn parse_trade_row(first: &Value, received_ts: f64, symbol: &Symbol) -> Option<Trade> {
    Some(Trade {
        exchange: ExchangeId::Okx,
        symbol: symbol.clone(),
        side: match first.get("side")?.as_str()? {
            "sell" => Side::Sell,
            _ => Side::Buy,
        },
        amount: parse_decimal(first.get("sz")?)?,
        price: parse_decimal(first.get("px")?)?,
        exchange_ts: first.get("ts").and_then(parse_millis)?,
        received_ts,
        id: first
            .get("tradeId")
            .and_then(|v| v.as_str())
            .map(ToOwned::to_owned),
        implied_volatility: None,
    })
}

#[cfg(feature = "orderbook")]
pub fn parse_l1_book(message: &Value, received_ts: f64) -> Option<L1Book> {
    parse_l1_books(message, received_ts).into_iter().next()
}

#[cfg(feature = "orderbook")]
pub fn parse_l1_books(message: &Value, received_ts: f64) -> Vec<L1Book> {
    let Some(raw_symbol) = message
        .get("arg")
        .and_then(|arg| arg.get("instId"))
        .and_then(Value::as_str)
    else {
        return Vec::new();
    };
    let symbol = parse_symbol(raw_symbol);
    let Some(rows) = message.get("data").and_then(Value::as_array) else {
        return Vec::new();
    };
    rows.iter()
        .filter_map(|row| {
            let bids = parse_levels(row.get("bids")?)?;
            let asks = parse_levels(row.get("asks")?)?;
            Some(L1Book {
                exchange: ExchangeId::Okx,
                symbol: symbol.clone(),
                bid: bids.first()?.clone(),
                ask: asks.first()?.clone(),
                exchange_ts: row.get("ts").and_then(parse_millis).unwrap_or(received_ts),
                received_ts,
            })
        })
        .collect()
}

#[cfg(feature = "orderbook")]
pub fn parse_l2_book(message: &Value, received_ts: f64) -> Option<L2Book> {
    parse_l2_books(message, received_ts).into_iter().next()
}

#[cfg(feature = "orderbook")]
pub fn parse_l2_books(message: &Value, received_ts: f64) -> Vec<L2Book> {
    let Some(raw_symbol) = message
        .get("arg")
        .and_then(|arg| arg.get("instId"))
        .and_then(Value::as_str)
    else {
        return Vec::new();
    };
    let symbol = parse_symbol(raw_symbol);
    let snapshot = is_snapshot_message(message);
    let Some(rows) = message.get("data").and_then(Value::as_array) else {
        return Vec::new();
    };
    rows.iter()
        .filter_map(|row| parse_l2_book_row(row, received_ts, &symbol, snapshot))
        .collect()
}

#[cfg(feature = "orderbook")]
fn parse_l2_book_row(
    first: &Value,
    received_ts: f64,
    symbol: &Symbol,
    snapshot: bool,
) -> Option<L2Book> {
    let bids = parse_levels(first.get("bids")?)?;
    let asks = parse_levels(first.get("asks")?)?;
    let exchange_ts = first
        .get("ts")
        .and_then(parse_millis)
        .unwrap_or(received_ts);
    Some(if snapshot {
        L2Book::Snapshot(L2BookSnapshot {
            exchange: ExchangeId::Okx,
            symbol: symbol.clone(),
            bids,
            asks,
            exchange_ts,
            received_ts,
        })
    } else {
        L2Book::Delta(L2BookDelta {
            exchange: ExchangeId::Okx,
            symbol: symbol.clone(),
            bids,
            asks,
            exchange_ts,
            received_ts,
        })
    })
}

#[cfg(feature = "orderbook")]
pub fn parse_l2_book_update(message: &Value, received_ts: f64) -> Option<OkxDepthUpdate> {
    parse_l2_book_updates(message, received_ts)
        .into_iter()
        .next()
}

#[cfg(feature = "orderbook")]
pub fn parse_l2_book_updates(message: &Value, received_ts: f64) -> Vec<OkxDepthUpdate> {
    let Some(raw_symbol) = message
        .get("arg")
        .and_then(|arg| arg.get("instId"))
        .and_then(Value::as_str)
    else {
        return Vec::new();
    };
    let symbol = parse_symbol(raw_symbol);
    let action = if is_snapshot_message(message) {
        OkxBookAction::Snapshot
    } else {
        OkxBookAction::Update
    };
    let snapshot = matches!(action, OkxBookAction::Snapshot);
    // The checksum is only comparable to the local full book on the `books`
    // channel. `books5`/`bbo-tbt` checksums cover only the transmitted top
    // levels, so they are not carried for validation.
    let full_book = message
        .get("arg")
        .and_then(|arg| arg.get("channel"))
        .and_then(Value::as_str)
        == Some("books");
    let Some(rows) = message.get("data").and_then(Value::as_array) else {
        return Vec::new();
    };
    rows.iter()
        .filter_map(|row| {
            Some(OkxDepthUpdate {
                action: action.clone(),
                seq_id: parse_i64(row.get("seqId")?)?,
                prev_seq_id: row
                    .get("prevSeqId")
                    .and_then(parse_i64)
                    .or(snapshot.then_some(-1))?,
                checksum: if full_book {
                    row.get("checksum")
                        .and_then(parse_i64)
                        .filter(|value| *value >= 0)
                        .map(|value| value as u32)
                } else {
                    None
                },
                book: parse_l2_book_row(row, received_ts, &symbol, snapshot)?,
            })
        })
        .collect()
}

#[cfg(feature = "candles")]
pub fn parse_candle(message: &Value, received_ts: f64) -> Option<Candle> {
    parse_candles(message, received_ts).into_iter().next()
}

#[cfg(feature = "candles")]
pub fn parse_candles(message: &Value, received_ts: f64) -> Vec<Candle> {
    let Some(arg) = message.get("arg") else {
        return Vec::new();
    };
    let Some(raw_symbol) = arg.get("instId").and_then(Value::as_str) else {
        return Vec::new();
    };
    let symbol = parse_symbol(raw_symbol);
    let interval = arg
        .get("channel")
        .and_then(Value::as_str)
        .and_then(|channel| channel.strip_prefix("candle"))
        .unwrap_or("1m");
    let Some(rows) = message.get("data").and_then(Value::as_array) else {
        return Vec::new();
    };
    rows.iter()
        .filter_map(|row| parse_candle_row(row, received_ts, &symbol, interval))
        .collect()
}

/// Normalized candle interval vocabulary (Binance-style lowercase) mapped
/// back from the OKX wire form (`candle1H` carries `1H`). Must mirror
/// `OkxAdapter::candle_interval_wire` exactly.
#[cfg(feature = "candles")]
fn normalized_candle_interval(wire: &str) -> String {
    match wire {
        "1H" => "1h".to_owned(),
        "2H" => "2h".to_owned(),
        "4H" => "4h".to_owned(),
        "6H" => "6h".to_owned(),
        "12H" => "12h".to_owned(),
        "1D" => "1d".to_owned(),
        "1W" => "1w".to_owned(),
        other => other.to_owned(),
    }
}

/// Duration in seconds of an OKX wire candle interval, mirroring
/// `OkxAdapter::candle_interval_wire`.
#[cfg(feature = "candles")]
fn candle_duration_seconds(interval: &str) -> f64 {
    match interval {
        "3m" => 180.0,
        "5m" => 300.0,
        "15m" => 900.0,
        "30m" => 1800.0,
        "1H" => 3600.0,
        "2H" => 7200.0,
        "4H" => 14400.0,
        "6H" => 21600.0,
        "12H" => 43200.0,
        "1D" => 86400.0,
        "1W" => 604800.0,
        "1M" => 2592000.0,
        "3M" => 7776000.0,
        _ => 60.0,
    }
}

#[cfg(feature = "candles")]
fn parse_candle_row(
    value: &Value,
    received_ts: f64,
    symbol: &Symbol,
    interval: &str,
) -> Option<Candle> {
    let row = value.as_array()?;
    let start = parse_millis(row.first()?)?;
    let end = if matches!(interval, "1M" | "3M") {
        // Official unsuffixed monthly bars open in UTC+8. Calendar arithmetic
        // preserves that boundary through variable month lengths and leap years.
        let utc = chrono::DateTime::from_timestamp_millis((start * 1000.0).round() as i64)?;
        let offset = chrono::FixedOffset::east_opt(8 * 3600).expect("valid UTC+8 offset");
        utc.with_timezone(&offset)
            .checked_add_months(chrono::Months::new(if interval == "1M" { 1 } else { 3 }))?
            .timestamp_millis() as f64
            / 1000.0
    } else {
        start + candle_duration_seconds(interval)
    };

    Some(Candle {
        exchange: ExchangeId::Okx,
        symbol: symbol.clone(),
        start,
        end,
        interval: normalized_candle_interval(interval),
        trades: None,
        open: parse_decimal(row.get(1)?)?,
        high: parse_decimal(row.get(2)?)?,
        low: parse_decimal(row.get(3)?)?,
        close: parse_decimal(row.get(4)?)?,
        volume: parse_decimal(row.get(5)?)?,
        closed: row.get(8).and_then(|v| v.as_str()).map(|v| v == "1"),
        exchange_ts: start,
        received_ts,
    })
}

#[cfg(feature = "funding")]
pub fn parse_funding(message: &Value, received_ts: f64) -> Option<Funding> {
    parse_fundings(message, received_ts).into_iter().next()
}

#[cfg(feature = "funding")]
pub fn parse_fundings(message: &Value, received_ts: f64) -> Vec<Funding> {
    let Some(rows) = message.get("data").and_then(Value::as_array) else {
        return Vec::new();
    };
    rows.iter()
        .filter_map(|row| {
            Some(Funding {
                exchange: ExchangeId::Okx,
                symbol: parse_symbol(row.get("instId")?.as_str()?),
                // OKX funding-rate rows do not carry a mark price.
                mark_price: None,
                rate: Some(parse_decimal(row.get("fundingRate")?)?),
                next_funding_time: row.get("nextFundingTime").and_then(parse_millis),
                // `nextFundingRate` is absent or empty until the next period.
                predicted_rate: row.get("nextFundingRate").and_then(parse_decimal),
                exchange_ts: row.get("fundingTime").and_then(parse_millis)?,
                received_ts,
            })
        })
        .collect()
}

#[cfg(feature = "openinterest")]
pub fn parse_open_interest(message: &Value, received_ts: f64) -> Option<OpenInterest> {
    parse_open_interests(message, received_ts)
        .into_iter()
        .next()
}

#[cfg(feature = "openinterest")]
pub fn parse_open_interests(message: &Value, received_ts: f64) -> Vec<OpenInterest> {
    let Some(rows) = message.get("data").and_then(Value::as_array) else {
        return Vec::new();
    };
    rows.iter()
        .filter_map(|row| {
            Some(OpenInterest {
                exchange: ExchangeId::Okx,
                symbol: parse_symbol(row.get("instId")?.as_str()?),
                open_interest: parse_decimal(row.get("oi")?)?,
                coin_quantity: row.get("oiCcy").and_then(parse_decimal),
                value_usd: row.get("oiUsd").and_then(parse_decimal),
                exchange_ts: row.get("ts").and_then(parse_millis)?,
                received_ts,
            })
        })
        .collect()
}

#[cfg(feature = "index")]
pub fn parse_index_price(message: &Value, received_ts: f64) -> Option<IndexPrice> {
    parse_index_prices(message, received_ts).into_iter().next()
}

#[cfg(feature = "index")]
pub fn parse_index_prices(message: &Value, received_ts: f64) -> Vec<IndexPrice> {
    let Some(rows) = message.get("data").and_then(Value::as_array) else {
        return Vec::new();
    };
    rows.iter()
        .filter_map(|row| {
            Some(IndexPrice {
                exchange: ExchangeId::Okx,
                symbol: parse_symbol(row.get("instId")?.as_str()?),
                price: parse_decimal(row.get("idxPx")?)?,
                open_24h: row.get("open24h").and_then(parse_decimal),
                high_24h: row.get("high24h").and_then(parse_decimal),
                low_24h: row.get("low24h").and_then(parse_decimal),
                exchange_ts: row.get("ts").and_then(parse_millis)?,
                received_ts,
            })
        })
        .collect()
}

#[cfg(feature = "markprice")]
pub fn parse_mark_price(message: &Value, received_ts: f64) -> Option<MarkPrice> {
    parse_mark_prices(message, received_ts).into_iter().next()
}

#[cfg(feature = "markprice")]
pub fn parse_mark_prices(message: &Value, received_ts: f64) -> Vec<MarkPrice> {
    let Some(rows) = message.get("data").and_then(Value::as_array) else {
        return Vec::new();
    };
    rows.iter()
        .filter_map(|row| {
            Some(MarkPrice {
                exchange: ExchangeId::Okx,
                symbol: parse_symbol(row.get("instId")?.as_str()?),
                price: parse_decimal(row.get("markPx")?)?,
                next_funding_time: None,
                predicted_rate: None,
                exchange_ts: row.get("ts").and_then(parse_millis)?,
                received_ts,
            })
        })
        .collect()
}

#[cfg(feature = "liquidations")]
pub fn parse_liquidation(message: &Value, received_ts: f64) -> Option<Liquidation> {
    parse_liquidations(message, received_ts).into_iter().next()
}

#[cfg(feature = "liquidations")]
pub fn parse_liquidations(message: &Value, received_ts: f64) -> Vec<Liquidation> {
    let Some(rows) = message.get("data").and_then(Value::as_array) else {
        return Vec::new();
    };
    rows.iter()
        .filter_map(|row| {
            Some(Liquidation {
                exchange: ExchangeId::Okx,
                symbol: parse_symbol(row.get("instId")?.as_str()?),
                side: if row.get("side")?.as_str()? == "sell" {
                    Side::Sell
                } else {
                    Side::Buy
                },
                quantity: parse_decimal(row.get("sz")?)?,
                price: parse_decimal(row.get("px")?)?,
                id: row
                    .get("ordId")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                // OKX liquidation-orders rows do not carry a fill status.
                status: LiquidationStatus::Filled,
                exchange_ts: row.get("ts").and_then(parse_millis)?,
                received_ts,
            })
        })
        .collect()
}

fn parse_symbol(raw: &str) -> Symbol {
    // Empty segments (e.g. `BTC-` or `-USDT`) must fall through to
    // `from_input`, whose guards produce an `Unknown` symbol instead of
    // reaching the `Symbol` constructors with empty components.
    let parts: Vec<_> = raw.split('-').collect();
    match parts.as_slice() {
        [base, quote] if !base.is_empty() && !quote.is_empty() => Symbol::spot(base, quote),
        [base, quote, "SWAP"] if !base.is_empty() && !quote.is_empty() => {
            Symbol::perpetual(base, quote)
        }
        [base, quote, expiry] if !base.is_empty() && !quote.is_empty() && !expiry.is_empty() => {
            Symbol::futures(base, quote, expiry)
        }
        _ => Symbol::from_input(raw),
    }
}

fn is_snapshot_message(message: &Value) -> bool {
    message.get("action").and_then(Value::as_str) == Some("snapshot")
        || matches!(
            message
                .get("arg")
                .and_then(|arg| arg.get("channel"))
                .and_then(Value::as_str),
            Some("books5" | "bbo-tbt")
        )
}

fn parse_decimal(value: &Value) -> Option<Decimal> {
    Decimal::from_str_exact(value.as_str()?).ok()
}

#[cfg(feature = "orderbook")]
fn parse_levels(value: &Value) -> Option<Vec<PriceLevel>> {
    value
        .as_array()?
        .iter()
        .map(|level| {
            let pair = level.as_array()?;
            Some(PriceLevel {
                price: parse_decimal(pair.first()?)?,
                amount: parse_decimal(pair.get(1)?)?,
            })
        })
        .collect()
}

fn parse_millis(value: &Value) -> Option<f64> {
    value
        .as_str()
        .and_then(|v| v.parse::<f64>().ok())
        .or_else(|| value.as_f64())
        .or_else(|| value.as_i64().map(|v| v as f64))
        .map(|v| v / 1000.0)
}

fn parse_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|raw| raw.parse().ok()))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    #[cfg(feature = "candles")]
    use super::parse_candle;
    #[cfg(feature = "orderbook")]
    use super::parse_l2_book;
    #[cfg(feature = "orderbook")]
    use super::parse_l2_book_update;
    #[cfg(feature = "ticker")]
    use super::parse_ticker;
    #[cfg(feature = "trade")]
    use super::parse_trade;
    #[cfg(feature = "orderbook")]
    use cryptofeed_orderbook::L2Book;

    #[cfg(feature = "ticker")]
    #[test]
    fn parses_okx_ticker_message() {
        let message = json!({
            "arg": {"channel": "tickers", "instId": "BTC-USDT"},
            "data": [{ "bidPx": "64999.10", "askPx": "65000.20", "ts": "1710000000456" }]
        });
        let ticker = parse_ticker(&message, 1710000001.5).expect("ticker");
        assert_eq!(ticker.symbol.as_str(), "BTC-USDT");
    }

    #[cfg(feature = "ticker")]
    #[test]
    fn parses_every_okx_ticker_in_a_batch() {
        let message = json!({
            "arg": {"channel": "tickers", "instId": "BTC-USDT-SWAP"},
            "data": [
                {"bidPx": "1", "askPx": "2", "ts": "1710000000456"},
                {"bidPx": "3", "askPx": "4", "ts": "1710000000457"}
            ]
        });
        let tickers = super::parse_tickers(&message, 1710000001.5);
        assert_eq!(tickers.len(), 2);
        assert_eq!(tickers[0].symbol.as_str(), "BTC-USDT-PERP");
    }

    #[cfg(feature = "trade")]
    #[test]
    fn parses_okx_trade_message() {
        let message = json!({
            "arg": {"channel": "trades", "instId": "BTC-USDT"},
            "data": [{ "tradeId": "1", "px": "65000.50", "sz": "0.0100", "side": "buy", "ts": "1710000000123" }]
        });
        let trade = parse_trade(&message, 1710000001.5).expect("trade");
        assert_eq!(trade.symbol.as_str(), "BTC-USDT");
    }

    #[cfg(feature = "trade")]
    #[test]
    fn parses_every_okx_trade_and_preserves_swap_identity() {
        let message = json!({
            "arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"},
            "data": [
                {"tradeId": "1", "px": "1", "sz": "2", "side": "buy", "ts": "1710000000123"},
                {"tradeId": "2", "px": "2", "sz": "3", "side": "sell", "ts": "1710000000124"}
            ]
        });
        let trades = super::parse_trades(&message, 1710000001.5);
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].symbol.as_str(), "BTC-USDT-PERP");
        assert_eq!(trades[1].id.as_deref(), Some("2"));
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn parses_okx_l2_book_message() {
        let message = json!({
            "arg": {"channel": "books", "instId": "BTC-USDT"},
            "action": "snapshot",
            "data": [{
                "bids": [["64999.10", "1.25", "0", "1"]],
                "asks": [["65000.20", "0.75", "0", "1"]],
                "ts": "1710000000456"
            }]
        });
        let book = parse_l2_book(&message, 1710000001.5).expect("book");
        match book {
            L2Book::Snapshot(snapshot) => assert_eq!(snapshot.symbol.as_str(), "BTC-USDT"),
            L2Book::Delta(_) => panic!("expected snapshot event model"),
        }
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn parses_okx_l2_book_update_ids() {
        let message = json!({
            "arg": {"channel": "books", "instId": "BTC-USDT"},
            "action": "snapshot",
            "data": [{
                "bids": [["64999.10", "1.25", "0", "1"]],
                "asks": [["65000.20", "0.75", "0", "1"]],
                "ts": "1710000000456",
                "seqId": 100i64,
                "prevSeqId": -1i64
            }]
        });

        let update = parse_l2_book_update(&message, 1710000001.5).expect("update");
        assert_eq!(update.seq_id, 100);
        assert_eq!(update.prev_seq_id, -1);
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn parses_every_okx_book_entry_in_a_batch() {
        let message = json!({
            "arg": {"channel": "books", "instId": "BTC-USDT"},
            "action": "update",
            "data": [
                {"bids": [["1", "2", "0", "1"]], "asks": [], "ts": "1710000000456", "seqId": 101, "prevSeqId": 100},
                {"bids": [], "asks": [["3", "4", "0", "1"]], "ts": "1710000000457", "seqId": 102, "prevSeqId": 101}
            ]
        });
        let updates = super::parse_l2_book_updates(&message, 1710000001.5);
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[1].seq_id, 102);
        assert_eq!(updates[1].prev_seq_id, 101);
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn parses_full_book_checksum_only_on_books_channel() {
        let books = json!({
            "arg": {"channel": "books", "instId": "BTC-USDT"},
            "action": "snapshot",
            "data": [{
                "bids": [["64999.10", "1.25", "0", "1"]],
                "asks": [["65000.20", "0.75", "0", "1"]],
                "ts": "1710000000456",
                "seqId": 100i64,
                "prevSeqId": -1i64,
                "checksum": 4132486225i64
            }]
        });
        let update = parse_l2_book_update(&books, 1710000001.5).expect("books update");
        assert_eq!(update.checksum, Some(4132486225));

        let books5 = json!({
            "arg": {"channel": "books5", "instId": "BTC-USDT-SWAP"},
            "data": [{
                "bids": [["1", "2", "0", "1"]],
                "asks": [["3", "4", "0", "1"]],
                "ts": "1710000000456",
                "seqId": 101,
                "checksum": 4132486225i64
            }]
        });
        let update = parse_l2_book_update(&books5, 1710000001.5).expect("books5 update");
        assert_eq!(update.checksum, None);
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn parses_snapshot_only_channel_without_prev_sequence() {
        let message = json!({
            "arg": {"channel": "books5", "instId": "BTC-USDT-SWAP"},
            "data": [{
                "bids": [["1", "2", "0", "1"]],
                "asks": [["3", "4", "0", "1"]],
                "ts": "1710000000456",
                "seqId": 101
            }]
        });
        let update = parse_l2_book_update(&message, 1710000001.5).expect("snapshot");
        assert!(matches!(update.action, super::OkxBookAction::Snapshot));
        assert_eq!(update.prev_seq_id, -1);
        assert!(matches!(update.book, L2Book::Snapshot(_)));
    }

    #[cfg(feature = "candles")]
    #[test]
    fn parses_okx_candle_message() {
        let message = json!({
            "arg": {"channel": "candle1m", "instId": "BTC-USDT"},
            "data": [["1710000000000", "65000.00", "65100.00", "64900.00", "65050.00", "12.50", "0", "0", "1"]]
        });
        let candle = parse_candle(&message, 1710000061.0).expect("candle");
        assert_eq!(candle.symbol.as_str(), "BTC-USDT");
    }

    #[cfg(feature = "candles")]
    #[test]
    fn parses_every_okx_candle_and_preserves_futures_identity() {
        let message = json!({
            "arg": {"channel": "candle1m", "instId": "BTC-USDT-260925"},
            "data": [
                ["1710000000000", "1", "3", "0.5", "2", "12", "0", "0", "1"],
                ["1710000060000", "2", "4", "1", "3", "13", "0", "0", "0"]
            ]
        });
        let candles = super::parse_candles(&message, 1710000061.0);
        assert_eq!(candles.len(), 2);
        assert_eq!(candles[0].symbol.as_str(), "BTC-USDT-260925");
    }
}
