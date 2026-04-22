use super::parser;
use crate::exchange::ExchangeFeed;
use cryptofeed_core::exchange::Channel;
use serde_json::Value;

#[cfg(feature = "candles")]
use cryptofeed_candles::Candle;
#[cfg(feature = "orderbook")]
use cryptofeed_orderbook::L2Book;
#[cfg(feature = "ticker")]
use cryptofeed_ticker::Ticker;
#[cfg(feature = "trade")]
use cryptofeed_trade::Trade;

pub struct BinanceAdapter;

pub enum BinanceEvent {
    #[cfg(feature = "candles")]
    Candle(Candle),
    #[cfg(feature = "orderbook")]
    L2Book(L2Book),
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
                    Channel::Candles => format!("{exchange_symbol}@kline_1m"),
                };
                streams.push(stream);
            }
        }

        streams
    }

    pub fn subscription_url(feed: &ExchangeFeed) -> String {
        format!("{}{}", Self::websocket_url(), Self::streams(feed).join("/"))
    }

    pub fn unwrap_combined_message(message: &Value) -> Option<&Value> {
        message.get("data").or(Some(message))
    }

    pub fn parse_message(message: &Value, received_ts: f64) -> Option<BinanceEvent> {
        let payload = Self::unwrap_combined_message(message)?;
        let event = payload.get("e")?.as_str()?;

        match event {
            #[cfg(feature = "candles")]
            "kline" => parser::parse_candle(payload, received_ts).map(BinanceEvent::Candle),
            #[cfg(feature = "orderbook")]
            "depthUpdate" => parser::parse_l2_book(payload, received_ts).map(BinanceEvent::L2Book),
            #[cfg(feature = "trade")]
            "aggTrade" => parser::parse_trade(payload, received_ts).map(BinanceEvent::Trade),
            #[cfg(feature = "ticker")]
            "bookTicker" => parser::parse_ticker(payload, received_ts).map(BinanceEvent::Ticker),
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

    #[cfg(feature = "candles")]
    #[test]
    fn builds_binance_candle_stream_name() {
        let feed = Binance::new().candles().symbol("BTC-USDT").build();
        let streams = BinanceAdapter::streams(&feed);
        assert_eq!(streams, vec!["btcusdt@kline_1m".to_owned()]);
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

    #[cfg(feature = "trade")]
    #[test]
    fn parses_trade_event_from_combined_stream_wrapper() {
        let message = serde_json::json!({
            "stream": "btcusdt@aggTrade",
            "data": {
                "e": "aggTrade",
                "s": "BTCUSDT",
                "a": 12345,
                "p": "65000.50",
                "q": "0.01000000",
                "T": 1710000000123u64,
                "m": false
            }
        });

        let event = BinanceAdapter::parse_message(&message, 1710000001.5);
        assert!(matches!(event, Some(BinanceEvent::Trade(_))));
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn parses_l2_book_event_message() {
        let message = serde_json::json!({
            "e": "depthUpdate",
            "s": "BTCUSDT",
            "E": 1710000000456u64,
            "b": [["64999.10", "1.25"]],
            "a": [["65000.20", "0.75"]]
        });

        let event = BinanceAdapter::parse_message(&message, 1710000001.5);
        assert!(matches!(event, Some(BinanceEvent::L2Book(_))));
    }
}
