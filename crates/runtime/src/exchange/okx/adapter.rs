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

pub struct OkxAdapter;

pub enum OkxEvent {
    #[cfg(feature = "candles")]
    Candle(Candle),
    #[cfg(feature = "orderbook")]
    L2Book(L2Book),
    #[cfg(feature = "ticker")]
    Ticker(Ticker),
    #[cfg(feature = "trade")]
    Trade(Trade),
}

impl OkxAdapter {
    pub fn websocket_url() -> &'static str {
        "wss://ws.okx.com:8443/ws/v5/public"
    }

    pub fn subscription_url(_feed: &ExchangeFeed) -> String {
        Self::websocket_url().to_owned()
    }

    pub fn subscription_message(feed: &ExchangeFeed) -> String {
        let args: Vec<Value> = feed
            .symbols
            .iter()
            .flat_map(|symbol| {
                let inst_id = symbol.as_str().to_owned();
                feed.channels.iter().filter_map(move |channel| {
                    let channel = match channel {
                        Channel::Candles => "candle1m",
                        Channel::Ticker => "tickers",
                        Channel::Trade => "trades",
                        Channel::L2Book => "books",
                        Channel::Funding | Channel::Liquidations => return None,
                    };
                    Some(serde_json::json!({
                        "channel": channel,
                        "instId": inst_id,
                    }))
                })
            })
            .collect();

        serde_json::json!({
            "op": "subscribe",
            "args": args,
        })
        .to_string()
    }

    pub fn parse_message(message: &Value, received_ts: f64) -> Option<OkxEvent> {
        let channel = message.get("arg")?.get("channel")?.as_str()?;
        match channel {
            #[cfg(feature = "ticker")]
            "tickers" => parser::parse_ticker(message, received_ts).map(OkxEvent::Ticker),
            #[cfg(feature = "trade")]
            "trades" => parser::parse_trade(message, received_ts).map(OkxEvent::Trade),
            #[cfg(feature = "orderbook")]
            "books" | "books5" | "bbo-tbt" => {
                parser::parse_l2_book(message, received_ts).map(OkxEvent::L2Book)
            }
            #[cfg(feature = "candles")]
            "candle1m" => parser::parse_candle(message, received_ts).map(OkxEvent::Candle),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::OkxAdapter;
    use crate::exchange::okx::Okx;

    #[test]
    fn builds_okx_v5_public_websocket_url() {
        let feed = Okx::new().ticker().symbol("BTC-USDT").build();
        assert_eq!(
            OkxAdapter::subscription_url(&feed),
            "wss://ws.okx.com:8443/ws/v5/public"
        );
    }

    #[test]
    fn builds_okx_v5_subscription_message() {
        let feed = Okx::new()
            .ticker()
            .trade()
            .l2_book()
            .candles()
            .symbol("BTC-USDT")
            .build();
        let payload = OkxAdapter::subscription_message(&feed);
        assert!(payload.contains("\"channel\":\"tickers\""));
        assert!(payload.contains("\"channel\":\"trades\""));
        assert!(payload.contains("\"channel\":\"books\""));
        assert!(payload.contains("\"channel\":\"candle1m\""));
        assert!(payload.contains("\"instId\":\"BTC-USDT\""));
    }
}
