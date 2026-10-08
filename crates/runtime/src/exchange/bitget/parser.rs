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

#[cfg(feature = "trade")]
pub fn parse_trade(message: &Value, received_ts: f64) -> Option<Trade> {
    parse_trades(message, received_ts).into_iter().next()
}

#[cfg(feature = "trade")]
pub fn parse_trades(message: &Value, received_ts: f64) -> Vec<Trade> {
    let Some(arg) = message.get("arg") else {
        return Vec::new();
    };
    let Some(raw_symbol) = arg
        .get("symbol")
        .or_else(|| arg.get("instId"))
        .and_then(Value::as_str)
    else {
        return Vec::new();
    };
    let symbol = parse_symbol(raw_symbol, arg.get("instType").and_then(Value::as_str));
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
        exchange: ExchangeId::Bitget,
        symbol: symbol.clone(),
        side: match first.get("S").or_else(|| first.get(3))?.as_str()? {
            "sell" => Side::Sell,
            _ => Side::Buy,
        },
        amount: parse_decimal(first.get("v").or_else(|| first.get(2))?)?,
        price: parse_decimal(first.get("p").or_else(|| first.get(1))?)?,
        exchange_ts: parse_millis(first.get("T").or_else(|| first.get(0))?)?,
        received_ts,
        id: first
            .get("i")
            .and_then(|v| v.as_str())
            .map(ToOwned::to_owned),
        implied_volatility: None,
    })
}

#[cfg(feature = "ticker")]
pub fn parse_ticker(message: &Value, received_ts: f64) -> Option<Ticker> {
    parse_tickers(message, received_ts).into_iter().next()
}

#[cfg(feature = "ticker")]
pub fn parse_tickers(message: &Value, received_ts: f64) -> Vec<Ticker> {
    let Some(arg) = message.get("arg") else {
        return Vec::new();
    };
    let Some(raw_symbol) = arg
        .get("symbol")
        .or_else(|| arg.get("instId"))
        .and_then(Value::as_str)
    else {
        return Vec::new();
    };
    let symbol = parse_symbol(raw_symbol, arg.get("instType").and_then(Value::as_str));
    let Some(rows) = message.get("data").and_then(Value::as_array) else {
        return Vec::new();
    };
    rows.iter()
        .filter_map(|first| {
            Some(Ticker {
                exchange: ExchangeId::Bitget,
                symbol: symbol.clone(),
                bid: parse_decimal(first.get("bid1Price").or_else(|| first.get("bidPr"))?)?,
                ask: parse_decimal(first.get("ask1Price").or_else(|| first.get("askPr"))?)?,
                exchange_ts: message
                    .get("ts")
                    .or_else(|| first.get("ts"))
                    .and_then(parse_millis)
                    .unwrap_or(received_ts),
                received_ts,
                implied_volatility: None,
            })
        })
        .collect()
}

#[cfg(feature = "orderbook")]
pub fn parse_l1_books(message: &Value, received_ts: f64) -> Vec<L1Book> {
    parse_l2_books(message, received_ts)
        .into_iter()
        .filter_map(|book| {
            Some(L1Book {
                exchange: ExchangeId::Bitget,
                symbol: book.symbol().clone(),
                bid: book.bids().first()?.clone(),
                ask: book.asks().first()?.clone(),
                exchange_ts: match &book {
                    L2Book::Snapshot(snapshot) => snapshot.exchange_ts,
                    L2Book::Delta(delta) => delta.exchange_ts,
                },
                received_ts,
            })
        })
        .collect()
}

#[cfg(any(
    feature = "funding",
    feature = "index",
    feature = "markprice",
    feature = "openinterest"
))]
fn derivative_ticker_rows(message: &Value, received_ts: f64) -> Vec<(&Value, Symbol, f64)> {
    let Some(arg) = message.get("arg") else {
        return Vec::new();
    };
    let inst_type = arg.get("instType").and_then(Value::as_str);
    if !matches!(
        inst_type,
        Some("usdt-futures" | "usdc-futures" | "coin-futures")
    ) {
        return Vec::new();
    }
    let Some(native) = arg.get("symbol").and_then(Value::as_str) else {
        return Vec::new();
    };
    let symbol = parse_symbol(native, inst_type);
    let ts = message
        .get("ts")
        .and_then(parse_millis)
        .unwrap_or(received_ts);
    message
        .get("data")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|row| (row, symbol.clone(), ts))
        .collect()
}

#[cfg(feature = "funding")]
pub fn parse_fundings(message: &Value, received_ts: f64) -> Vec<Funding> {
    derivative_ticker_rows(message, received_ts)
        .into_iter()
        .filter_map(|(row, symbol, exchange_ts)| {
            let rate = parse_decimal(row.get("fundingRate")?)?;
            Some(Funding {
                exchange: ExchangeId::Bitget,
                symbol,
                mark_price: row.get("markPrice").and_then(parse_decimal),
                rate: Some(rate),
                next_funding_time: row
                    .get("nextFundingTime")
                    .and_then(parse_millis)
                    .filter(|ts| *ts > 0.0),
                predicted_rate: None,
                exchange_ts,
                received_ts,
            })
        })
        .collect()
}

#[cfg(feature = "openinterest")]
pub fn parse_open_interests(message: &Value, received_ts: f64) -> Vec<OpenInterest> {
    derivative_ticker_rows(message, received_ts)
        .into_iter()
        .filter_map(|(row, symbol, exchange_ts)| {
            Some(OpenInterest {
                exchange: ExchangeId::Bitget,
                symbol,
                open_interest: parse_decimal(row.get("openInterest")?)?,
                coin_quantity: None,
                value_usd: None,
                exchange_ts,
                received_ts,
            })
        })
        .collect()
}

#[cfg(feature = "index")]
pub fn parse_index_prices(message: &Value, received_ts: f64) -> Vec<IndexPrice> {
    derivative_ticker_rows(message, received_ts)
        .into_iter()
        .filter_map(|(row, symbol, exchange_ts)| {
            Some(IndexPrice {
                exchange: ExchangeId::Bitget,
                symbol,
                price: parse_decimal(row.get("indexPrice")?)?,
                open_24h: None,
                high_24h: None,
                low_24h: None,
                exchange_ts,
                received_ts,
            })
        })
        .collect()
}

#[cfg(feature = "markprice")]
pub fn parse_mark_prices(message: &Value, received_ts: f64) -> Vec<MarkPrice> {
    derivative_ticker_rows(message, received_ts)
        .into_iter()
        .filter_map(|(row, symbol, exchange_ts)| {
            Some(MarkPrice {
                exchange: ExchangeId::Bitget,
                symbol,
                price: parse_decimal(row.get("markPrice")?)?,
                next_funding_time: row
                    .get("nextFundingTime")
                    .and_then(parse_millis)
                    .filter(|ts| *ts > 0.0),
                predicted_rate: None,
                exchange_ts,
                received_ts,
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
    let Some(raw_symbol) = arg
        .get("symbol")
        .or_else(|| arg.get("instId"))
        .and_then(Value::as_str)
    else {
        return Vec::new();
    };
    let symbol = parse_symbol(raw_symbol, arg.get("instType").and_then(Value::as_str));
    let interval = arg.get("interval").and_then(Value::as_str).unwrap_or("1m");
    let Some(duration) = interval_seconds(interval) else {
        return Vec::new();
    };
    let Some(rows) = message.get("data").and_then(Value::as_array) else {
        return Vec::new();
    };
    rows.iter()
        .filter_map(|row| parse_candle_row(row, message, received_ts, &symbol, interval, duration))
        .collect()
}

#[cfg(feature = "candles")]
fn parse_candle_row(
    row: &Value,
    message: &Value,
    received_ts: f64,
    symbol: &Symbol,
    interval: &str,
    duration: f64,
) -> Option<Candle> {
    let start = row
        .get("start")
        .or_else(|| row.get(0))
        .and_then(parse_millis)?;
    Some(Candle {
        exchange: ExchangeId::Bitget,
        symbol: symbol.clone(),
        start,
        end: start + duration,
        interval: match interval {
            "1H" | "4H" | "6H" | "12H" | "1D" => interval.to_ascii_lowercase(),
            _ => interval.to_owned(),
        },
        trades: None,
        open: parse_decimal(row.get("open").or_else(|| row.get(1))?)?,
        high: parse_decimal(row.get("high").or_else(|| row.get(2))?)?,
        low: parse_decimal(row.get("low").or_else(|| row.get(3))?)?,
        close: parse_decimal(row.get("close").or_else(|| row.get(4))?)?,
        volume: parse_decimal(row.get("volume").or_else(|| row.get(5))?)?,
        closed: None,
        exchange_ts: message.get("ts").and_then(parse_millis).unwrap_or(start),
        received_ts,
    })
}

#[cfg(feature = "orderbook")]
pub fn parse_l2_book(message: &Value, received_ts: f64) -> Option<L2Book> {
    parse_l2_books(message, received_ts).into_iter().next()
}

#[cfg(feature = "orderbook")]
pub fn parse_l2_books(message: &Value, received_ts: f64) -> Vec<L2Book> {
    let Some(arg) = message.get("arg") else {
        return Vec::new();
    };
    let Some(raw_symbol) = arg
        .get("symbol")
        .or_else(|| arg.get("instId"))
        .and_then(Value::as_str)
    else {
        return Vec::new();
    };
    let symbol = parse_symbol(raw_symbol, arg.get("instType").and_then(Value::as_str));
    let snapshot = message.get("action").and_then(Value::as_str) == Some("snapshot");
    let Some(rows) = message.get("data").and_then(Value::as_array) else {
        return Vec::new();
    };
    rows.iter()
        .filter_map(|row| parse_book_row(row, received_ts, &symbol, snapshot))
        .collect()
}

#[cfg(feature = "orderbook")]
fn parse_book_row(
    row: &Value,
    received_ts: f64,
    symbol: &Symbol,
    snapshot: bool,
) -> Option<L2Book> {
    let bids = parse_levels(row.get("b").or_else(|| row.get("bids"))?)?;
    let asks = parse_levels(row.get("a").or_else(|| row.get("asks"))?)?;
    let exchange_ts = row.get("ts").and_then(parse_millis).unwrap_or(received_ts);
    if snapshot {
        Some(L2Book::Snapshot(L2BookSnapshot {
            exchange: ExchangeId::Bitget,
            symbol: symbol.clone(),
            bids,
            asks,
            exchange_ts,
            received_ts,
        }))
    } else {
        Some(L2Book::Delta(L2BookDelta {
            exchange: ExchangeId::Bitget,
            symbol: symbol.clone(),
            bids,
            asks,
            exchange_ts,
            received_ts,
        }))
    }
}

#[cfg(feature = "liquidations")]
pub fn parse_liquidation(message: &Value, received_ts: f64) -> Option<Liquidation> {
    parse_liquidations(message, received_ts).into_iter().next()
}

#[cfg(feature = "liquidations")]
pub fn parse_liquidations(message: &Value, received_ts: f64) -> Vec<Liquidation> {
    let inst_type = message
        .get("arg")
        .and_then(|arg| arg.get("instType"))
        .and_then(Value::as_str);
    let Some(rows) = message.get("data").and_then(Value::as_array) else {
        return Vec::new();
    };
    rows.iter()
        .filter_map(|row| {
            let price = parse_decimal(row.get("price")?)?;
            let quote_quantity = parse_decimal(row.get("amount")?)?;
            if price.is_zero() {
                return None;
            }
            Some(Liquidation {
                // The platform-wide liquidation stream carries the native
                // symbol per row; the instType scope resolves product identity.
                exchange: ExchangeId::Bitget,
                symbol: parse_symbol(row.get("symbol")?.as_str()?, inst_type),
                // Bitget reports the liquidated position side. Normalize it
                // to the liquidation order/aggressor side exposed by the SDK.
                side: match row.get("side")?.as_str()? {
                    "buy" => Side::Sell,
                    "sell" => Side::Buy,
                    _ => return None,
                },
                // The v3 stream reports `amount` in quote currency; normalize
                // to base quantity so the public model has one unit.
                quantity: quote_quantity / price,
                price,
                // v3 liquidation rows carry no order id or fill status.
                id: None,
                status: LiquidationStatus::Filled,
                exchange_ts: row.get("ts").and_then(parse_millis).unwrap_or(received_ts),
                received_ts,
            })
        })
        .collect()
}

fn parse_symbol(raw: &str, inst_type: Option<&str>) -> Symbol {
    // `strip_suffix` can yield an empty base for a raw symbol that IS a
    // quote currency (e.g. `USDT`); empty components must not reach the
    // `Symbol` constructors.
    let (base, quote) = ["USDT", "USDC", "USD", "BTC", "ETH", "EUR"]
        .into_iter()
        .find_map(|quote| {
            raw.strip_suffix(quote)
                .filter(|base| !base.is_empty())
                .map(|base| (base, quote))
        })
        .unwrap_or((raw, "UNKNOWN"));
    if inst_type.is_some_and(|kind| kind != "spot") {
        Symbol::perpetual(base, quote)
    } else {
        Symbol::spot(base, quote)
    }
}

/// Current v3 hourly/daily wire units are uppercase. Lowercase units and
/// longer periods remain raw-parser migration references, not runtime capability.
#[cfg(feature = "candles")]
fn interval_seconds(interval: &str) -> Option<f64> {
    let (value, unit) = interval.split_at(interval.len().checked_sub(1)?);
    let value = value.parse::<f64>().ok()?;
    match unit {
        "m" => Some(value * 60.0),
        "h" | "H" => Some(value * 3600.0),
        "d" | "D" => Some(value * 86400.0),
        "w" => Some(value * 604800.0),
        "M" => Some(value * 2592000.0),
        _ => None,
    }
}

fn parse_decimal(value: &Value) -> Option<Decimal> {
    Decimal::from_str_exact(value.as_str()?).ok()
}

#[cfg(feature = "orderbook")]
fn parse_levels(value: &Value) -> Option<Vec<PriceLevel>> {
    let levels = value.as_array()?;
    levels
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    #[cfg(feature = "candles")]
    use super::parse_candle;
    #[cfg(feature = "orderbook")]
    use super::parse_l2_book;
    #[cfg(feature = "ticker")]
    use super::parse_ticker;
    #[cfg(feature = "trade")]
    use super::{parse_trade, parse_trades};

    #[cfg(feature = "trade")]
    #[test]
    fn parses_bitget_trade_message() {
        let message = json!({
            "arg": {"instType": "spot", "topic": "publicTrade", "symbol": "BTCUSDT"},
            "data": [{
                "T": "1710000000123",
                "p": "65000.50",
                "v": "0.0100",
                "S": "buy",
                "i": "123456"
            }]
        });

        let trade = parse_trade(&message, 1710000001.5).expect("trade");
        assert_eq!(trade.symbol.as_str(), "BTC-USDT");
    }

    #[cfg(feature = "trade")]
    #[test]
    fn parses_every_trade_in_v3_batch() {
        let message = json!({
            "arg": {"instType": "spot", "topic": "publicTrade", "symbol": "BTCUSDT"},
            "data": [
                {"T": "1710000000123", "p": "65000.50", "v": "0.0100", "S": "buy", "i": "1"},
                {"T": "1710000000456", "p": "65001.50", "v": "0.0200", "S": "sell", "i": "2"}
            ]
        });

        let trades = parse_trades(&message, 1710000001.5);
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].id.as_deref(), Some("1"));
        assert_eq!(trades[1].id.as_deref(), Some("2"));
    }

    #[cfg(feature = "ticker")]
    #[test]
    fn parses_bitget_ticker_message() {
        let message = json!({
            "arg": {"instType": "spot", "topic": "ticker", "symbol": "BTCUSDT"},
            "data": [{ "bid1Price": "64999.10", "ask1Price": "65000.20" }],
            "ts": "1710000000456"
        });

        let ticker = parse_ticker(&message, 1710000001.5).expect("ticker");
        assert_eq!(ticker.symbol.as_str(), "BTC-USDT");
    }

    #[cfg(feature = "candles")]
    #[test]
    fn parses_bitget_candle_message() {
        let message = json!({
            "arg": {"instType": "spot", "topic": "kline", "symbol": "BTCUSDT", "interval": "1m"},
            "data": [{
                "start": "1710000000000",
                "open": "65000.00",
                "high": "65100.00",
                "low": "64900.00",
                "close": "65050.00",
                "volume": "12.50",
                "turnover": "812500.00"
            }]
        });

        let candle = parse_candle(&message, 1710000061.0).expect("candle");
        assert_eq!(candle.symbol.as_str(), "BTC-USDT");
        assert_eq!(candle.interval, "1m");
        assert_eq!(candle.start, 1710000000.0);
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn parses_bitget_l2_book_message() {
        let message = json!({
            "arg": {"instType": "spot", "topic": "books", "symbol": "BTCUSDT"},
            "data": [{
                "b": [["64999.10", "1.25"]],
                "a": [["65000.20", "0.75"]],
                "ts": "1710000000456"
            }]
        });

        let mut message = message;
        message["action"] = json!("snapshot");
        let book = parse_l2_book(&message, 1710000001.5).expect("book");
        match book {
            cryptofeed_orderbook::L2Book::Snapshot(snapshot) => {
                assert_eq!(snapshot.symbol.as_str(), "BTC-USDT");
                assert_eq!(snapshot.bids.len(), 1);
                assert_eq!(snapshot.asks.len(), 1);
            }
            cryptofeed_orderbook::L2Book::Delta(_) => panic!("expected snapshot"),
        }
    }
}
