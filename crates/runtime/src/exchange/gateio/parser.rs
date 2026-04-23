#[cfg(feature = "candles")]
use cryptofeed_candles::Candle;
use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
#[cfg(feature = "orderbook")]
use cryptofeed_orderbook::{L2Book, L2BookDelta, PriceLevel};
#[cfg(feature = "ticker")]
use cryptofeed_ticker::Ticker;
#[cfg(feature = "trade")]
use cryptofeed_trade::{Trade, model::Side};
use rust_decimal::Decimal;
use serde_json::Value;

#[cfg(feature = "ticker")]
pub fn parse_ticker(message: &Value, received_ts: f64) -> Option<Ticker> {
    let result = message.get("result")?;
    Some(Ticker {
        exchange: ExchangeId::Gateio,
        symbol: parse_symbol(result.get("s")?.as_str()?),
        bid: parse_decimal(result.get("b")?)?,
        ask: parse_decimal(result.get("a")?)?,
        exchange_ts: result
            .get("t")
            .and_then(parse_seconds)
            .unwrap_or(received_ts),
        received_ts,
    })
}

#[cfg(feature = "trade")]
pub fn parse_trade(message: &Value, received_ts: f64) -> Option<Trade> {
    let first = message.get("result")?.as_array()?.first()?;
    Some(Trade {
        exchange: ExchangeId::Gateio,
        symbol: parse_symbol(first.get("currency_pair")?.as_str()?),
        side: match first.get("side")?.as_str()? {
            "sell" => Side::Sell,
            _ => Side::Buy,
        },
        amount: parse_decimal(first.get("amount")?)?,
        price: parse_decimal(first.get("price")?)?,
        exchange_ts: first
            .get("create_time_ms")
            .and_then(parse_millis)
            .or_else(|| first.get("create_time").and_then(parse_seconds))?,
        received_ts,
        id: first
            .get("id")
            .and_then(|v| v.as_str())
            .map(ToOwned::to_owned),
    })
}

#[cfg(feature = "orderbook")]
pub fn parse_l2_book(message: &Value, received_ts: f64) -> Option<L2Book> {
    let result = message.get("result")?;
    Some(L2Book::Delta(L2BookDelta {
        exchange: ExchangeId::Gateio,
        symbol: parse_symbol(result.get("s")?.as_str()?),
        bids: parse_levels(result.get("b")?)?,
        asks: parse_levels(result.get("a")?)?,
        exchange_ts: result
            .get("t")
            .and_then(parse_seconds)
            .unwrap_or(received_ts),
        received_ts,
    }))
}

#[cfg(feature = "candles")]
pub fn parse_candle(message: &Value, received_ts: f64) -> Option<Candle> {
    let result = message.get("result")?;
    let name = result.get("n")?.as_str()?;
    let symbol = name.split_once('_')?.1;
    let start = result.get("t").and_then(parse_seconds)?;

    Some(Candle {
        exchange: ExchangeId::Gateio,
        symbol: parse_symbol(symbol),
        start,
        end: start + 60.0,
        interval: "1m".to_owned(),
        trades: None,
        open: parse_decimal(result.get("o")?)?,
        close: parse_decimal(result.get("c")?)?,
        high: parse_decimal(result.get("h")?)?,
        low: parse_decimal(result.get("l")?)?,
        volume: parse_decimal(result.get("v")?)?,
        closed: result.get("w").and_then(|v| v.as_bool()),
        exchange_ts: start,
        received_ts,
    })
}

fn parse_symbol(raw: &str) -> Symbol {
    let normalized = raw.replace('_', "-");
    let parts: Vec<_> = normalized.split('-').collect();
    Symbol::spot(parts[0], parts[1])
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

fn parse_seconds(value: &Value) -> Option<f64> {
    value
        .as_str()
        .and_then(|v| v.parse::<f64>().ok())
        .or_else(|| value.as_f64())
        .or_else(|| value.as_i64().map(|v| v as f64))
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
    use super::parse_trade;
    #[cfg(feature = "orderbook")]
    use cryptofeed_orderbook::L2Book;

    #[cfg(feature = "ticker")]
    #[test]
    fn parses_gateio_ticker_message() {
        let message = json!({
            "channel": "spot.book_ticker",
            "event": "update",
            "result": {"s": "BTC_USDT", "b": "64999.10", "a": "65000.20", "t": 1710000000}
        });
        let ticker = parse_ticker(&message, 1710000001.5).expect("ticker");
        assert_eq!(ticker.symbol.as_str(), "BTC-USDT");
    }

    #[cfg(feature = "trade")]
    #[test]
    fn parses_gateio_trade_message() {
        let message = json!({
            "channel": "spot.trades",
            "event": "update",
            "result": [{
                "id": "1",
                "currency_pair": "BTC_USDT",
                "price": "65000.50",
                "amount": "0.0100",
                "side": "buy",
                "create_time_ms": "1710000000123"
            }]
        });
        let trade = parse_trade(&message, 1710000001.5).expect("trade");
        assert_eq!(trade.symbol.as_str(), "BTC-USDT");
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn parses_gateio_l2_book_message() {
        let message = json!({
            "channel": "spot.order_book_update",
            "event": "update",
            "result": {
                "s": "BTC_USDT",
                "b": [["64999.10", "1.25"]],
                "a": [["65000.20", "0.75"]],
                "t": 1710000000
            }
        });
        let book = parse_l2_book(&message, 1710000001.5).expect("book");
        match book {
            L2Book::Delta(delta) => assert_eq!(delta.symbol.as_str(), "BTC-USDT"),
            L2Book::Snapshot(_) => panic!("expected delta event model"),
        }
    }

    #[cfg(feature = "candles")]
    #[test]
    fn parses_gateio_candle_message() {
        let message = json!({
            "channel": "spot.candlesticks",
            "event": "update",
            "result": {
                "t": "1710000000",
                "v": "12.50",
                "c": "65050.00",
                "h": "65100.00",
                "l": "64900.00",
                "o": "65000.00",
                "n": "1m_BTC_USDT",
                "w": true
            }
        });
        let candle = parse_candle(&message, 1710000061.0).expect("candle");
        assert_eq!(candle.symbol.as_str(), "BTC-USDT");
    }
}
