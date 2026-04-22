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

pub struct BybitAdapter;

pub enum BybitEvent {
    #[cfg(feature = "candles")]
    Candle(Candle),
    #[cfg(feature = "orderbook")]
    L2Book(L2Book),
    #[cfg(feature = "ticker")]
    Ticker(Ticker),
    #[cfg(feature = "trade")]
    Trade(Trade),
}

impl BybitAdapter {
    pub fn websocket_url() -> &'static str {
        "wss://stream.bybit.com/v5/public/spot"
    }

    pub fn subscription_url(_feed: &ExchangeFeed) -> String {
        Self::websocket_url().to_owned()
    }

    pub fn subscription_message(feed: &ExchangeFeed) -> String {
        let args: Vec<String> = feed
            .symbols
            .iter()
            .flat_map(|symbol| {
                let exchange_symbol = symbol.as_str().replace('-', "");
                feed.channels.iter().filter_map(move |channel| {
                    let topic = match channel {
                        Channel::Candles => format!("kline.1.{exchange_symbol}"),
                        Channel::Ticker => format!("tickers.{exchange_symbol}"),
                        Channel::Trade => format!("publicTrade.{exchange_symbol}"),
                        Channel::L2Book => format!("orderbook.50.{exchange_symbol}"),
                        Channel::Funding | Channel::Liquidations => return None,
                    };
                    Some(topic)
                })
            })
            .collect();

        serde_json::json!({
            "op": "subscribe",
            "args": args,
        })
        .to_string()
    }

    pub fn parse_message(message: &Value, received_ts: f64) -> Option<BybitEvent> {
        let topic = message.get("topic")?.as_str()?;

        if topic.starts_with("tickers.") {
            #[cfg(feature = "ticker")]
            {
                return parser::parse_ticker(message, received_ts).map(BybitEvent::Ticker);
            }
        }
        if topic.starts_with("publicTrade.") {
            #[cfg(feature = "trade")]
            {
                return parser::parse_trade(message, received_ts).map(BybitEvent::Trade);
            }
        }
        if topic.starts_with("orderbook.") {
            #[cfg(feature = "orderbook")]
            {
                return parser::parse_l2_book(message, received_ts).map(BybitEvent::L2Book);
            }
        }
        if topic.starts_with("kline.") {
            #[cfg(feature = "candles")]
            {
                return parser::parse_candle(message, received_ts).map(BybitEvent::Candle);
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::BybitAdapter;
    use crate::exchange::bybit::Bybit;

    #[test]
    fn builds_bybit_v5_public_websocket_url() {
        let feed = Bybit::new().ticker().symbol("BTC-USDT").build();
        assert_eq!(
            BybitAdapter::subscription_url(&feed),
            "wss://stream.bybit.com/v5/public/spot"
        );
    }

    #[test]
    fn builds_bybit_v5_subscription_message() {
        let feed = Bybit::new()
            .ticker()
            .trade()
            .l2_book()
            .candles()
            .symbol("BTC-USDT")
            .build();
        let payload = BybitAdapter::subscription_message(&feed);

        assert!(payload.contains("tickers.BTCUSDT"));
        assert!(payload.contains("publicTrade.BTCUSDT"));
        assert!(payload.contains("orderbook.50.BTCUSDT"));
        assert!(payload.contains("kline.1.BTCUSDT"));
    }
}
