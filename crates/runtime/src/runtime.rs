pub mod connection;
pub mod router;
pub mod supervisor;

use crate::exchange::{binance::adapter::BinanceAdapter, ExchangeFeed};
use crate::feed::FeedHandler;
use cryptofeed_core::{
    error::Result,
    exchange::ExchangeId,
};

pub async fn run(handler: FeedHandler) -> Result<()> {
    let _planned = planned_connection_urls(&handler);
    let _router = router::Router::default();
    Ok(())
}

fn planned_connection_urls(handler: &FeedHandler) -> Vec<String> {
    handler.feeds().iter().map(planned_url).collect()
}

fn planned_url(feed: &ExchangeFeed) -> String {
    match feed.exchange {
        ExchangeId::Binance => BinanceAdapter::subscription_url(feed),
        ExchangeId::Coinbase => String::new(),
        ExchangeId::Kraken => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::planned_connection_urls;
    use crate::{exchange::binance::Binance, FeedHandler};

    #[test]
    fn plans_binance_connection_urls() {
        let mut handler = FeedHandler::new();
        handler.add_feed(Binance::new().ticker().trade().symbol("BTC-USDT").build());

        assert_eq!(
            planned_connection_urls(&handler),
            vec![
                "wss://stream.binance.com:9443/stream?streams=btcusdt@bookTicker/btcusdt@aggTrade"
                    .to_owned()
            ]
        );
    }
}
