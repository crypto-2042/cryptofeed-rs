#[cfg(feature = "orderbook")]
use super::book_sync::{OkxBookAction, OkxDepthUpdate};
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
    let arg = message.get("arg")?;
    let first = message.get("data")?.as_array()?.first()?;
    Some(Ticker {
        exchange: ExchangeId::Okx,
        symbol: parse_symbol(arg.get("instId")?.as_str()?),
        bid: parse_decimal(first.get("bidPx")?)?,
        ask: parse_decimal(first.get("askPx")?)?,
        exchange_ts: first
            .get("ts")
            .and_then(parse_millis)
            .unwrap_or(received_ts),
        received_ts,
    })
}

#[cfg(feature = "trade")]
pub fn parse_trade(message: &Value, received_ts: f64) -> Option<Trade> {
    let arg = message.get("arg")?;
    let first = message.get("data")?.as_array()?.first()?;
    Some(Trade {
        exchange: ExchangeId::Okx,
        symbol: parse_symbol(arg.get("instId")?.as_str()?),
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
    })
}

#[cfg(feature = "orderbook")]
pub fn parse_l2_book(message: &Value, received_ts: f64) -> Option<L2Book> {
    let arg = message.get("arg")?;
    let first = message.get("data")?.as_array()?.first()?;
    Some(L2Book::Delta(L2BookDelta {
        exchange: ExchangeId::Okx,
        symbol: parse_symbol(arg.get("instId")?.as_str()?),
        bids: parse_levels(first.get("bids")?)?,
        asks: parse_levels(first.get("asks")?)?,
        exchange_ts: first
            .get("ts")
            .and_then(parse_millis)
            .unwrap_or(received_ts),
        received_ts,
    }))
}

#[cfg(feature = "orderbook")]
pub fn parse_l2_book_update(message: &Value, received_ts: f64) -> Option<OkxDepthUpdate> {
    let first = message.get("data")?.as_array()?.first()?;
    Some(OkxDepthUpdate {
        action: match message.get("action").and_then(|v| v.as_str()) {
            Some("snapshot") => OkxBookAction::Snapshot,
            _ => OkxBookAction::Update,
        },
        seq_id: first.get("seqId")?.as_i64()?,
        prev_seq_id: first.get("prevSeqId")?.as_i64()?,
        book: parse_l2_book(message, received_ts)?,
    })
}

#[cfg(feature = "candles")]
pub fn parse_candle(message: &Value, received_ts: f64) -> Option<Candle> {
    let arg = message.get("arg")?;
    let row = message.get("data")?.as_array()?.first()?.as_array()?;
    let start = parse_millis(row.first()?)?;

    Some(Candle {
        exchange: ExchangeId::Okx,
        symbol: parse_symbol(arg.get("instId")?.as_str()?),
        start,
        end: start + 60.0,
        interval: "1m".to_owned(),
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

fn parse_symbol(raw: &str) -> Symbol {
    let parts: Vec<_> = raw.split('-').collect();
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

    #[cfg(feature = "orderbook")]
    #[test]
    fn parses_okx_l2_book_message() {
        let message = json!({
            "arg": {"channel": "books", "instId": "BTC-USDT"},
            "data": [{
                "bids": [["64999.10", "1.25", "0", "1"]],
                "asks": [["65000.20", "0.75", "0", "1"]],
                "ts": "1710000000456"
            }]
        });
        let book = parse_l2_book(&message, 1710000001.5).expect("book");
        match book {
            L2Book::Delta(delta) => assert_eq!(delta.symbol.as_str(), "BTC-USDT"),
            L2Book::Snapshot(_) => panic!("expected delta event model"),
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
}
