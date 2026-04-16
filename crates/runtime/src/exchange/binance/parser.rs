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

fn parse_symbol(raw: &str) -> Symbol {
    let normalized = parse_trade_symbol(raw);
    let parts: Vec<_> = normalized.split('-').collect();
    Symbol::spot(parts[0], parts[1])
}

fn parse_decimal(value: &Value) -> Option<Decimal> {
    Decimal::from_str_exact(value.as_str()?).ok()
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
}
