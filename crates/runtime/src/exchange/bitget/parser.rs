#[cfg(feature = "orderbook")]
use cryptofeed_orderbook::{L2Book, L2BookDelta, PriceLevel};
#[cfg(feature = "ticker")]
use cryptofeed_ticker::Ticker;
#[cfg(feature = "trade")]
use cryptofeed_trade::{model::Side, Trade};
use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
use rust_decimal::Decimal;
use serde_json::Value;

#[cfg(feature = "trade")]
pub fn parse_trade(message: &Value, received_ts: f64) -> Option<Trade> {
    let arg = message.get("arg")?;
    let symbol = parse_symbol(arg.get("symbol").or_else(|| arg.get("instId"))?.as_str()?);
    let first = message.get("data")?.as_array()?.first()?;

    Some(Trade {
        exchange: ExchangeId::Bitget,
        symbol,
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
    })
}

#[cfg(feature = "ticker")]
pub fn parse_ticker(message: &Value, received_ts: f64) -> Option<Ticker> {
    let arg = message.get("arg")?;
    let symbol = parse_symbol(arg.get("symbol").or_else(|| arg.get("instId"))?.as_str()?);
    let first = message.get("data")?.as_array()?.first()?;

    Some(Ticker {
        exchange: ExchangeId::Bitget,
        symbol,
        bid: parse_decimal(first.get("bid1Price").or_else(|| first.get("bidPr"))?)?,
        ask: parse_decimal(first.get("ask1Price").or_else(|| first.get("askPr"))?)?,
        exchange_ts: message
            .get("ts")
            .or_else(|| first.get("ts"))
            .and_then(parse_millis)
            .unwrap_or(received_ts),
        received_ts,
    })
}

#[cfg(feature = "orderbook")]
pub fn parse_l2_book(message: &Value, received_ts: f64) -> Option<L2Book> {
    let arg = message.get("arg")?;
    let symbol = parse_symbol(arg.get("symbol").or_else(|| arg.get("instId"))?.as_str()?);
    let first = message.get("data")?.as_array()?.first()?;

    Some(L2Book::Delta(L2BookDelta {
        exchange: ExchangeId::Bitget,
        symbol,
        bids: parse_levels(first.get("b").or_else(|| first.get("bids"))?)?,
        asks: parse_levels(first.get("a").or_else(|| first.get("asks"))?)?,
        exchange_ts: first
            .get("ts")
            .and_then(parse_millis)
            .unwrap_or(received_ts),
        received_ts,
    }))
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
    value.as_str()
        .and_then(|v| v.parse::<f64>().ok())
        .or_else(|| value.as_f64())
        .or_else(|| value.as_i64().map(|v| v as f64))
        .map(|v| v / 1000.0)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{parse_l2_book, parse_ticker, parse_trade};

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

        let book = parse_l2_book(&message, 1710000001.5).expect("book");
        match book {
            cryptofeed_orderbook::L2Book::Delta(delta) => {
                assert_eq!(delta.symbol.as_str(), "BTC-USDT");
                assert_eq!(delta.bids.len(), 1);
                assert_eq!(delta.asks.len(), 1);
            }
            cryptofeed_orderbook::L2Book::Snapshot(_) => panic!("expected delta"),
        }
    }
}
