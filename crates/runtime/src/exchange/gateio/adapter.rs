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

pub struct GateioAdapter;

pub enum GateioEvent {
    #[cfg(feature = "candles")]
    Candle(Candle),
    #[cfg(feature = "orderbook")]
    L2Book(L2Book),
    #[cfg(feature = "ticker")]
    Ticker(Ticker),
    #[cfg(feature = "trade")]
    Trade(Trade),
}

impl GateioAdapter {
    pub fn websocket_url() -> &'static str {
        "wss://api.gateio.ws/ws/v4/"
    }

    pub fn subscription_url(_feed: &ExchangeFeed) -> String {
        Self::websocket_url().to_owned()
    }

    pub fn subscription_messages(feed: &ExchangeFeed) -> Vec<String> {
        let mut messages = Vec::new();
        for symbol in &feed.symbols {
            let pair = symbol.as_str().replace('-', "_");
            for channel in &feed.channels {
                let (channel, payload) = match channel {
                    Channel::Candles => ("spot.candlesticks", serde_json::json!(["1m", pair])),
                    Channel::Ticker => ("spot.book_ticker", serde_json::json!([pair])),
                    Channel::Trade => ("spot.trades", serde_json::json!([pair])),
                    Channel::L2Book => {
                        ("spot.order_book_update", serde_json::json!([pair, "100ms"]))
                    }
                    Channel::Funding | Channel::Liquidations => continue,
                };
                messages.push(
                    serde_json::json!({
                        "time": 0,
                        "channel": channel,
                        "event": "subscribe",
                        "payload": payload,
                    })
                    .to_string(),
                );
            }
        }
        messages
    }

    pub fn parse_message(message: &Value, received_ts: f64) -> Option<GateioEvent> {
        if message.get("event").and_then(|v| v.as_str()) == Some("subscribe") {
            return None;
        }
        match message.get("channel")?.as_str()? {
            #[cfg(feature = "candles")]
            "spot.candlesticks" => {
                parser::parse_candle(message, received_ts).map(GateioEvent::Candle)
            }
            #[cfg(feature = "orderbook")]
            "spot.order_book_update" => {
                parser::parse_l2_book(message, received_ts).map(GateioEvent::L2Book)
            }
            #[cfg(feature = "ticker")]
            "spot.book_ticker" => {
                parser::parse_ticker(message, received_ts).map(GateioEvent::Ticker)
            }
            #[cfg(feature = "trade")]
            "spot.trades" => parser::parse_trade(message, received_ts).map(GateioEvent::Trade),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::GateioAdapter;
    use crate::exchange::gateio::Gateio;

    #[test]
    fn builds_gateio_v4_public_websocket_url() {
        let feed = Gateio::new().ticker().symbol("BTC-USDT").build();
        assert_eq!(
            GateioAdapter::subscription_url(&feed),
            "wss://api.gateio.ws/ws/v4/"
        );
    }

    #[test]
    fn builds_gateio_v4_subscription_messages() {
        let feed = Gateio::new()
            .ticker()
            .trade()
            .l2_book()
            .candles()
            .symbol("BTC-USDT")
            .build();
        let payloads = GateioAdapter::subscription_messages(&feed);
        let joined = payloads.join("\n");

        assert!(joined.contains("spot.book_ticker"));
        assert!(joined.contains("spot.trades"));
        assert!(joined.contains("spot.order_book_update"));
        assert!(joined.contains("spot.candlesticks"));
        assert!(joined.contains("BTC_USDT"));
    }
}
