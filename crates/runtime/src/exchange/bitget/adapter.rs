use crate::exchange::ExchangeFeed;
use super::parser;
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

    pub fn parse_message(message: &Value, received_ts: f64) -> Option<BitgetEvent> {
        let arg = message.get("arg")?;
        let channel = arg.get("channel")?.as_str()?;

        match channel {
            #[cfg(feature = "trade")]
            "trade" => parser::parse_trade(message, received_ts).map(BitgetEvent::Trade),
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
}
