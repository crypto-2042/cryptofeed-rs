use crate::exchange::ExchangeFeed;
use super::parser;
use cryptofeed_core::exchange::Channel;
use serde_json::Value;

#[cfg(feature = "ticker")]
use cryptofeed_ticker::Ticker;
#[cfg(feature = "trade")]
use cryptofeed_trade::Trade;

pub struct BinanceAdapter;

pub enum BinanceEvent {
    #[cfg(feature = "ticker")]
    Ticker(Ticker),
    #[cfg(feature = "trade")]
    Trade(Trade),
}

impl BinanceAdapter {
    pub fn websocket_url() -> &'static str {
        "wss://stream.binance.com:9443/stream?streams="
    }

    pub fn streams(feed: &ExchangeFeed) -> Vec<String> {
        let mut streams = Vec::new();

        for symbol in &feed.symbols {
            let exchange_symbol = symbol.as_str().replace('-', "").to_ascii_lowercase();

            for channel in &feed.channels {
                let stream = match channel {
                    Channel::Ticker => format!("{exchange_symbol}@bookTicker"),
                    Channel::Trade => format!("{exchange_symbol}@aggTrade"),
                    Channel::L2Book => format!("{exchange_symbol}@depth@100ms"),
                };
                streams.push(stream);
            }
        }

        streams
    }

    pub fn subscription_url(feed: &ExchangeFeed) -> String {
        format!("{}{}", Self::websocket_url(), Self::streams(feed).join("/"))
    }

    pub fn parse_message(message: &Value, received_ts: f64) -> Option<BinanceEvent> {
        let event = message.get("e")?.as_str()?;

        match event {
            #[cfg(feature = "trade")]
            "aggTrade" => parser::parse_trade(message, received_ts).map(BinanceEvent::Trade),
            #[cfg(feature = "ticker")]
            "bookTicker" => parser::parse_ticker(message, received_ts).map(BinanceEvent::Ticker),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BinanceAdapter, BinanceEvent};
    use crate::exchange::binance::Binance;

    #[test]
    fn builds_binance_stream_names() {
        let feed = Binance::new().ticker().trade().symbol("BTC-USDT").build();
        let streams = BinanceAdapter::streams(&feed);

        assert_eq!(
            streams,
            vec![
                "btcusdt@bookTicker".to_owned(),
                "btcusdt@aggTrade".to_owned(),
            ]
        );
    }

    #[test]
    fn builds_binance_subscription_url() {
        let feed = Binance::new().ticker().trade().symbol("BTC-USDT").build();

        assert_eq!(
            BinanceAdapter::subscription_url(&feed),
            "wss://stream.binance.com:9443/stream?streams=btcusdt@bookTicker/btcusdt@aggTrade"
        );
    }

    #[cfg(feature = "trade")]
    #[test]
    fn parses_trade_event_message() {
        let message = serde_json::json!({
            "e": "aggTrade",
            "s": "BTCUSDT",
            "a": 12345,
            "p": "65000.50",
            "q": "0.01000000",
            "T": 1710000000123u64,
            "m": false
        });

        let event = BinanceAdapter::parse_message(&message, 1710000001.5);
        assert!(matches!(event, Some(BinanceEvent::Trade(_))));
    }

    #[cfg(feature = "ticker")]
    #[test]
    fn parses_ticker_event_message() {
        let message = serde_json::json!({
            "e": "bookTicker",
            "s": "BTCUSDT",
            "b": "64999.10",
            "a": "65000.20",
            "E": 1710000000456u64
        });

        let event = BinanceAdapter::parse_message(&message, 1710000001.5);
        assert!(matches!(event, Some(BinanceEvent::Ticker(_))));
    }
}
