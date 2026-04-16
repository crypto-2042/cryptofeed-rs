pub mod connection;
pub mod router;
pub mod supervisor;

use crate::exchange::{
    binance::adapter::{BinanceAdapter, BinanceEvent},
    ExchangeFeed,
};
use crate::feed::FeedHandler;
use cryptofeed_core::{
    error::{Error, Result},
    exchange::ExchangeId,
};
use serde_json::Value;
use url::Url;

pub async fn run(handler: FeedHandler) -> Result<()> {
    let _planned = planned_connection_urls(&handler);
    let _router = router::Router::default();
    for feed in handler.feeds() {
        match feed.exchange {
            ExchangeId::Binance => consume_binance_feed(feed).await?,
            ExchangeId::Coinbase | ExchangeId::Kraken => {}
        }
    }
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

async fn consume_binance_feed(feed: &ExchangeFeed) -> Result<()> {
    let url = Url::parse(&planned_url(feed)).map_err(|e| Error::Transport(e.to_string()))?;
    let connection = connection::WsConnection::new(url);
    let mut stream = connection.connect().await?;

    while let Some(text) = connection::next_text_message(&mut stream).await? {
        process_binance_text_message(feed, &text, current_timestamp()).await?;
    }

    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
async fn dispatch_binance_event(feed: &ExchangeFeed, event: BinanceEvent) {
    match event {
        #[cfg(feature = "ticker")]
        BinanceEvent::Ticker(ticker) => {
            if let Some(handler) = &feed.ticker_handler {
                handler.on_ticker(ticker).await;
            }
        }
        #[cfg(feature = "trade")]
        BinanceEvent::Trade(trade) => {
            if let Some(handler) = &feed.trade_handler {
                handler.on_trade(trade).await;
            }
        }
    }
}

#[cfg_attr(not(test), allow(dead_code))]
async fn process_binance_text_message(feed: &ExchangeFeed, text: &str, received_ts: f64) -> Result<()> {
    let message: Value =
        serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
    if let Some(event) = BinanceAdapter::parse_message(&message, received_ts) {
        dispatch_binance_event(feed, event).await;
    }
    Ok(())
}

fn current_timestamp() -> f64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::{planned_connection_urls, process_binance_text_message};
    use crate::{
        exchange::binance::{
            adapter::{BinanceAdapter, BinanceEvent},
            Binance,
        },
        FeedHandler,
    };
    use async_trait::async_trait;
    #[cfg(feature = "ticker")]
    use cryptofeed_ticker::{Ticker, TickerHandler};
    #[cfg(feature = "trade")]
    use cryptofeed_trade::{Trade, TradeHandler};

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

    #[cfg(feature = "ticker")]
    struct TestTickerHandler {
        seen: Arc<Mutex<usize>>,
    }

    #[cfg(feature = "ticker")]
    #[async_trait]
    impl TickerHandler for TestTickerHandler {
        async fn on_ticker(&self, _ticker: Ticker) {
            *self.seen.lock().expect("lock") += 1;
        }
    }

    #[cfg(feature = "trade")]
    struct TestTradeHandler {
        seen: Arc<Mutex<usize>>,
    }

    #[cfg(feature = "trade")]
    #[async_trait]
    impl TradeHandler for TestTradeHandler {
        async fn on_trade(&self, _trade: Trade) {
            *self.seen.lock().expect("lock") += 1;
        }
    }

    #[cfg(all(feature = "ticker", feature = "trade"))]
    #[tokio::test]
    async fn dispatches_binance_trade_and_ticker_events() {
        let ticker_seen = Arc::new(Mutex::new(0));
        let trade_seen = Arc::new(Mutex::new(0));
        let feed = Binance::new()
            .ticker()
            .trade()
            .ticker_handler(Arc::new(TestTickerHandler {
                seen: ticker_seen.clone(),
            }))
            .trade_handler(Arc::new(TestTradeHandler {
                seen: trade_seen.clone(),
            }))
            .symbol("BTC-USDT")
            .build();

        let trade_message = serde_json::json!({
            "e": "aggTrade",
            "s": "BTCUSDT",
            "a": 12345,
            "p": "65000.50",
            "q": "0.01000000",
            "T": 1710000000123u64,
            "m": false
        });
        let ticker_message = serde_json::json!({
            "e": "bookTicker",
            "s": "BTCUSDT",
            "b": "64999.10",
            "a": "65000.20",
            "E": 1710000000456u64
        });

        let trade_event =
            BinanceAdapter::parse_message(&trade_message, 1710000001.5).expect("trade event");
        let ticker_event =
            BinanceAdapter::parse_message(&ticker_message, 1710000001.5).expect("ticker event");

        assert!(matches!(trade_event, BinanceEvent::Trade(_)));
        assert!(matches!(ticker_event, BinanceEvent::Ticker(_)));

        super::dispatch_binance_event(&feed, trade_event).await;
        super::dispatch_binance_event(&feed, ticker_event).await;

        assert_eq!(*trade_seen.lock().expect("lock"), 1);
        assert_eq!(*ticker_seen.lock().expect("lock"), 1);
    }

    #[cfg(all(feature = "ticker", feature = "trade"))]
    #[tokio::test]
    async fn processes_combined_binance_trade_message() {
        let trade_seen = Arc::new(Mutex::new(0));
        let feed = Binance::new()
            .trade()
            .trade_handler(Arc::new(TestTradeHandler {
                seen: trade_seen.clone(),
            }))
            .symbol("BTC-USDT")
            .build();

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

        process_binance_text_message(&feed, &message.to_string(), 1710000001.5)
            .await
            .expect("process message");

        assert_eq!(*trade_seen.lock().expect("lock"), 1);
    }
}
