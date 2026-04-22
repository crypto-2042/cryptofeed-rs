#[cfg(feature = "orderbook")]
use super::book_sync::{BybitBookAction, BybitDepthUpdate};
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
    let data = message.get("data")?;
    Some(Ticker {
        exchange: ExchangeId::Bybit,
        symbol: parse_symbol(data.get("symbol")?.as_str()?),
        bid: parse_decimal(data.get("bid1Price")?)?,
        ask: parse_decimal(data.get("ask1Price")?)?,
        exchange_ts: message
            .get("ts")
            .and_then(parse_millis)
            .unwrap_or(received_ts),
        received_ts,
    })
}

#[cfg(feature = "trade")]
pub fn parse_trade(message: &Value, received_ts: f64) -> Option<Trade> {
    let first = message.get("data")?.as_array()?.first()?;
    Some(Trade {
        exchange: ExchangeId::Bybit,
        symbol: parse_symbol(first.get("s")?.as_str()?),
        side: match first.get("S")?.as_str()? {
            "Sell" => Side::Sell,
            _ => Side::Buy,
        },
        amount: parse_decimal(first.get("v")?)?,
        price: parse_decimal(first.get("p")?)?,
        exchange_ts: first.get("T").and_then(parse_millis)?,
        received_ts,
        id: first
            .get("i")
            .and_then(|v| v.as_str())
            .map(ToOwned::to_owned),
    })
}

#[cfg(feature = "orderbook")]
pub fn parse_l2_book(message: &Value, received_ts: f64) -> Option<L2Book> {
    let data = message.get("data")?;
    Some(L2Book::Delta(L2BookDelta {
        exchange: ExchangeId::Bybit,
        symbol: parse_symbol(data.get("s")?.as_str()?),
        bids: parse_levels(data.get("b")?)?,
        asks: parse_levels(data.get("a")?)?,
        exchange_ts: message
            .get("ts")
            .and_then(parse_millis)
            .unwrap_or(received_ts),
        received_ts,
    }))
}

#[cfg(feature = "orderbook")]
pub fn parse_l2_book_update(message: &Value, received_ts: f64) -> Option<BybitDepthUpdate> {
    let data = message.get("data")?;
    Some(BybitDepthUpdate {
        action: match message.get("type")?.as_str()? {
            "snapshot" => BybitBookAction::Snapshot,
            _ => BybitBookAction::Delta,
        },
        update_id: data.get("u")?.as_u64()?,
        seq: data.get("seq").and_then(|v| v.as_u64()),
        book: parse_l2_book(message, received_ts)?,
    })
}

#[cfg(feature = "candles")]
pub fn parse_candle(message: &Value, received_ts: f64) -> Option<Candle> {
    let first = message.get("data")?.as_array()?.first()?;
    let topic = message.get("topic")?.as_str()?;
    let symbol = topic.rsplit('.').next()?;

    Some(Candle {
        exchange: ExchangeId::Bybit,
        symbol: parse_symbol(symbol),
        start: first.get("start").and_then(parse_millis)?,
        end: first.get("end").and_then(parse_millis)?,
        interval: first.get("interval")?.as_str()?.to_owned(),
        trades: None,
        open: parse_decimal(first.get("open")?)?,
        close: parse_decimal(first.get("close")?)?,
        high: parse_decimal(first.get("high")?)?,
        low: parse_decimal(first.get("low")?)?,
        volume: parse_decimal(first.get("volume")?)?,
        closed: first.get("confirm").and_then(|v| v.as_bool()),
        exchange_ts: message
            .get("ts")
            .and_then(parse_millis)
            .unwrap_or(received_ts),
        received_ts,
    })
}

fn parse_symbol(raw: &str) -> Symbol {
    let normalized = raw.replace("USDT", "-USDT");
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
        .as_f64()
        .or_else(|| value.as_i64().map(|v| v as f64))
        .or_else(|| value.as_str().and_then(|s| s.parse::<f64>().ok()))
        .map(|v| v / 1000.0)
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
    fn parses_bybit_ticker_message() {
        let message = json!({
            "topic": "tickers.BTCUSDT",
            "type": "snapshot",
            "ts": 1672304486868i64,
            "data": {
                "symbol": "BTCUSDT",
                "bid1Price": "16578.50",
                "ask1Price": "16579.00"
            }
        });

        let ticker = parse_ticker(&message, 1672304487.0).expect("ticker");
        assert_eq!(ticker.symbol.as_str(), "BTC-USDT");
    }

    #[cfg(feature = "trade")]
    #[test]
    fn parses_bybit_trade_message() {
        let message = json!({
            "topic": "publicTrade.BTCUSDT",
            "type": "snapshot",
            "ts": 1672304486868i64,
            "data": [{
                "T": 1672304486865i64,
                "s": "BTCUSDT",
                "S": "Buy",
                "v": "0.001",
                "p": "16578.50",
                "i": "20f43950"
            }]
        });

        let trade = parse_trade(&message, 1672304487.0).expect("trade");
        assert_eq!(trade.symbol.as_str(), "BTC-USDT");
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn parses_bybit_l2_book_message() {
        let message = json!({
            "topic": "orderbook.50.BTCUSDT",
            "type": "snapshot",
            "ts": 1672304484978i64,
            "data": {
                "s": "BTCUSDT",
                "b": [["16493.50", "0.006"]],
                "a": [["16493.60", "0.100"]]
            }
        });

        let book = parse_l2_book(&message, 1672304485.0).expect("book");
        match book {
            L2Book::Delta(delta) => assert_eq!(delta.symbol.as_str(), "BTC-USDT"),
            L2Book::Snapshot(_) => panic!("expected delta event model"),
        }
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn parses_bybit_l2_book_update_ids() {
        let message = json!({
            "topic": "orderbook.50.BTCUSDT",
            "type": "snapshot",
            "ts": 1672304484978i64,
            "data": {
                "s": "BTCUSDT",
                "b": [["16493.50", "0.006"]],
                "a": [["16493.60", "0.100"]],
                "u": 18521288u64,
                "seq": 7961638724u64
            }
        });

        let update = parse_l2_book_update(&message, 1672304485.0).expect("update");
        assert_eq!(update.update_id, 18521288);
        assert_eq!(update.seq, Some(7961638724));
    }

    #[cfg(feature = "candles")]
    #[test]
    fn parses_bybit_candle_message() {
        let message = json!({
            "topic": "kline.1.BTCUSDT",
            "type": "snapshot",
            "ts": 1672324988882i64,
            "data": [{
                "start": 1672324800000i64,
                "end": 1672324859999i64,
                "interval": "1",
                "open": "16649.5",
                "close": "16677",
                "high": "16677",
                "low": "16608",
                "volume": "2.081",
                "confirm": false
            }]
        });

        let candle = parse_candle(&message, 1672324989.0).expect("candle");
        assert_eq!(candle.symbol.as_str(), "BTC-USDT");
        assert_eq!(candle.interval, "1");
    }
}
