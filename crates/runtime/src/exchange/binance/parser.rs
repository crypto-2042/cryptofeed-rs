#[cfg(feature = "orderbook")]
use super::book_sync::BinanceDepthDelta;
#[cfg(feature = "orderbook")]
use cryptofeed_orderbook::{L2Book, L2BookDelta, PriceLevel};
#[cfg(feature = "ticker")]
use cryptofeed_ticker::Ticker;
#[cfg(feature = "trade")]
use cryptofeed_trade::{model::Side, Trade};
use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
use rust_decimal::Decimal;
use serde_json::Value;

pub fn parse_trade_symbol(raw: &str) -> String {
    raw.replace("USDT", "-USDT")
}

#[cfg(feature = "trade")]
pub fn parse_trade(message: &Value, received_ts: f64) -> Option<Trade> {
    Some(Trade {
        exchange: ExchangeId::Binance,
        symbol: parse_symbol(message.get("s")?.as_str()?),
        side: if message.get("m")?.as_bool()? {
            Side::Sell
        } else {
            Side::Buy
        },
        amount: parse_decimal(message.get("q")?)?,
        price: parse_decimal(message.get("p")?)?,
        exchange_ts: parse_millis(message.get("T")?)?,
        received_ts,
        id: Some(message.get("a")?.to_string()),
    })
}

#[cfg(feature = "ticker")]
pub fn parse_ticker(message: &Value, received_ts: f64) -> Option<Ticker> {
    Some(Ticker {
        exchange: ExchangeId::Binance,
        symbol: parse_symbol(message.get("s")?.as_str()?),
        bid: parse_decimal(message.get("b")?)?,
        ask: parse_decimal(message.get("a")?)?,
        exchange_ts: message
            .get("E")
            .and_then(parse_millis)
            .unwrap_or(received_ts),
        received_ts,
    })
}

#[cfg(feature = "orderbook")]
pub fn parse_l2_book(message: &Value, received_ts: f64) -> Option<L2Book> {
    Some(L2Book::Delta(L2BookDelta {
        exchange: ExchangeId::Binance,
        symbol: parse_symbol(message.get("s")?.as_str()?),
        bids: parse_levels(message.get("b")?)?,
        asks: parse_levels(message.get("a")?)?,
        exchange_ts: message
            .get("E")
            .and_then(parse_millis)
            .unwrap_or(received_ts),
        received_ts,
    }))
}

#[cfg(feature = "orderbook")]
pub fn parse_l2_book_snapshot(
    message: &Value,
    symbol: &str,
    received_ts: f64,
) -> Option<(u64, cryptofeed_orderbook::L2BookSnapshot)> {
    Some((
        message.get("lastUpdateId")?.as_u64()?,
        cryptofeed_orderbook::L2BookSnapshot {
            exchange: ExchangeId::Binance,
            symbol: parse_symbol(symbol),
            bids: parse_levels(message.get("bids")?)?,
            asks: parse_levels(message.get("asks")?)?,
            exchange_ts: received_ts,
            received_ts,
        },
    ))
}

#[cfg(feature = "orderbook")]
pub fn parse_l2_book_update(message: &Value, received_ts: f64) -> Option<BinanceDepthDelta> {
    Some(BinanceDepthDelta {
        first_update_id: message.get("U")?.as_u64()?,
        last_update_id: message.get("u")?.as_u64()?,
        book: L2BookDelta {
            exchange: ExchangeId::Binance,
            symbol: parse_symbol(message.get("s")?.as_str()?),
            bids: parse_levels(message.get("b")?)?,
            asks: parse_levels(message.get("a")?)?,
            exchange_ts: message
                .get("E")
                .and_then(parse_millis)
                .unwrap_or(received_ts),
            received_ts,
        },
    })
}

fn parse_symbol(raw: &str) -> Symbol {
    let normalized = parse_trade_symbol(raw);
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
    value.as_f64().or_else(|| value.as_i64().map(|v| v as f64)).map(|v| v / 1000.0)
}

#[cfg(test)]
mod tests {
    use super::parse_trade_symbol;
    #[cfg(feature = "ticker")]
    use super::parse_ticker;
    #[cfg(feature = "trade")]
    use super::parse_trade;
    #[cfg(feature = "orderbook")]
    use super::parse_l2_book;
    #[cfg(feature = "orderbook")]
    use super::parse_l2_book_snapshot;
    #[cfg(feature = "orderbook")]
    use super::parse_l2_book_update;
    use rust_decimal::Decimal;
    use serde_json::json;

    #[test]
    fn parses_binance_trade_symbol() {
        assert_eq!(parse_trade_symbol("BTCUSDT"), "BTC-USDT");
    }

    #[cfg(feature = "trade")]
    #[test]
    fn parses_binance_trade_message() {
        let message = json!({
            "s": "BTCUSDT",
            "a": 12345,
            "p": "65000.50",
            "q": "0.01000000",
            "T": 1710000000123u64,
            "m": false
        });

        let trade = parse_trade(&message, 1710000001.5).expect("trade");

        assert_eq!(trade.symbol.as_str(), "BTC-USDT");
        assert_eq!(trade.price, Decimal::from_str_exact("65000.50").unwrap());
        assert_eq!(trade.amount, Decimal::from_str_exact("0.01000000").unwrap());
    }

    #[cfg(feature = "ticker")]
    #[test]
    fn parses_binance_ticker_message() {
        let message = json!({
            "s": "BTCUSDT",
            "b": "64999.10",
            "a": "65000.20",
            "E": 1710000000456u64
        });

        let ticker = parse_ticker(&message, 1710000001.5).expect("ticker");

        assert_eq!(ticker.symbol.as_str(), "BTC-USDT");
        assert_eq!(ticker.bid, Decimal::from_str_exact("64999.10").unwrap());
        assert_eq!(ticker.ask, Decimal::from_str_exact("65000.20").unwrap());
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn parses_binance_l2_book_message() {
        let message = json!({
            "e": "depthUpdate",
            "E": 1710000000456u64,
            "s": "BTCUSDT",
            "b": [["64999.10", "1.25"]],
            "a": [["65000.20", "0.75"]]
        });

        let book = parse_l2_book(&message, 1710000001.5).expect("book");

        match book {
            cryptofeed_orderbook::L2Book::Delta(delta) => {
                assert_eq!(delta.symbol.as_str(), "BTC-USDT");
                assert_eq!(delta.bids.len(), 1);
                assert_eq!(delta.asks.len(), 1);
                assert_eq!(delta.bids[0].price, Decimal::from_str_exact("64999.10").unwrap());
                assert_eq!(delta.asks[0].amount, Decimal::from_str_exact("0.75").unwrap());
            }
            cryptofeed_orderbook::L2Book::Snapshot(_) => panic!("expected delta"),
        }
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn parses_binance_l2_book_update_ids() {
        let message = json!({
            "e": "depthUpdate",
            "E": 1710000000456u64,
            "s": "BTCUSDT",
            "U": 100,
            "u": 101,
            "b": [["64999.10", "1.25"]],
            "a": [["65000.20", "0.75"]]
        });

        let update = parse_l2_book_update(&message, 1710000001.5).expect("update");
        assert_eq!(update.first_update_id, 100);
        assert_eq!(update.last_update_id, 101);
        assert_eq!(update.book.symbol.as_str(), "BTC-USDT");
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn parses_binance_l2_book_snapshot_response() {
        let message = json!({
            "lastUpdateId": 101u64,
            "bids": [["64999.10", "1.25"]],
            "asks": [["65000.20", "0.75"]]
        });

        let (last_update_id, snapshot) =
            parse_l2_book_snapshot(&message, "BTCUSDT", 1710000001.5).expect("snapshot");

        assert_eq!(last_update_id, 101);
        assert_eq!(snapshot.symbol.as_str(), "BTC-USDT");
        assert_eq!(snapshot.bids.len(), 1);
        assert_eq!(snapshot.asks.len(), 1);
    }
}
