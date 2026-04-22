use super::parser;
use crate::exchange::ExchangeFeed;
use cryptofeed_core::exchange::Channel;
use serde_json::Value;

#[cfg(feature = "orderbook")]
use cryptofeed_orderbook::L2Book;
#[cfg(feature = "ticker")]
use cryptofeed_ticker::Ticker;
#[cfg(feature = "trade")]
use cryptofeed_trade::Trade;

pub struct BitgetAdapter;

pub enum BitgetEvent {
    #[cfg(feature = "orderbook")]
    L2Book(L2Book),
    #[cfg(feature = "ticker")]
    Ticker(Ticker),
    #[cfg(feature = "trade")]
    Trade(Trade),
}

impl BitgetAdapter {
    pub fn websocket_url() -> &'static str {
        "wss://ws.bitget.com/v3/ws/public"
    }

    pub fn subscription_url(_feed: &ExchangeFeed) -> String {
        Self::websocket_url().to_owned()
    }

    pub fn subscription_message(feed: &ExchangeFeed) -> String {
        let args: Vec<Value> = feed
            .symbols
            .iter()
            .flat_map(|symbol| {
                let exchange_symbol = symbol.as_str().replace('-', "");
                feed.channels.iter().map(move |channel| {
                    let topic = match channel {
                        Channel::Ticker => "ticker",
                        Channel::Trade => "publicTrade",
                        Channel::L2Book => "books",
                    };

                    serde_json::json!({
                        "instType": "spot",
                        "topic": topic,
                        "symbol": exchange_symbol,
                    })
                })
            })
            .collect();

        serde_json::json!({
            "op": "subscribe",
            "args": args,
        })
        .to_string()
    }

    pub fn parse_message(message: &Value, received_ts: f64) -> Option<BitgetEvent> {
        let arg = message.get("arg")?;
        let topic = arg.get("topic").or_else(|| arg.get("channel"))?.as_str()?;

        match topic {
            #[cfg(feature = "trade")]
            "trade" => parser::parse_trade(message, received_ts).map(BitgetEvent::Trade),
            #[cfg(feature = "trade")]
            "publicTrade" => parser::parse_trade(message, received_ts).map(BitgetEvent::Trade),
            #[cfg(feature = "ticker")]
            "ticker" => parser::parse_ticker(message, received_ts).map(BitgetEvent::Ticker),
            #[cfg(feature = "orderbook")]
            "books" | "books1" | "books5" | "books15" => {
                parser::parse_l2_book(message, received_ts).map(BitgetEvent::L2Book)
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BitgetAdapter;
    use crate::exchange::bitget::Bitget;

    #[test]
    fn builds_bitget_v3_public_websocket_url() {
        let feed = Bitget::new().ticker().symbol("BTC-USDT").build();
        let url = BitgetAdapter::subscription_url(&feed);
        assert_eq!(url, "wss://ws.bitget.com/v3/ws/public");
    }

    #[test]
    fn builds_bitget_v3_subscription_message() {
        let feed = Bitget::new()
            .ticker()
            .trade()
            .l2_book()
            .symbol("BTC-USDT")
            .build();

        let payload = BitgetAdapter::subscription_message(&feed);
        assert!(payload.contains("\"instType\":\"spot\""));
        assert!(payload.contains("\"topic\":\"ticker\""));
        assert!(payload.contains("\"topic\":\"publicTrade\""));
        assert!(payload.contains("\"topic\":\"books\""));
        assert!(payload.contains("\"symbol\":\"BTCUSDT\""));
    }
}
