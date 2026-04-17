pub mod connection;
pub mod router;
pub mod supervisor;

use crate::exchange::{
    binance::adapter::{BinanceAdapter, BinanceEvent},
    bitget::adapter::{BitgetAdapter, BitgetEvent},
    ExchangeFeed,
};
use crate::feed::FeedHandler;
use cryptofeed_core::{
    error::{Error, Result},
    exchange::ExchangeId,
};
use serde_json::Value;
use std::future::Future;
use tokio::sync::watch;
use tokio::task::JoinSet;
use url::Url;

pub async fn run(handler: FeedHandler) -> Result<()> {
    let _planned = planned_connection_urls(&handler);
    let _router = router::Router::default();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = shutdown_tx.send(true);
    });
    run_feeds_until_shutdown(handler.into_feeds(), shutdown_rx, |feed, shutdown| async move {
        match feed.exchange {
            ExchangeId::Binance => consume_binance_feed(feed, shutdown).await,
            ExchangeId::Bitget => consume_bitget_feed(feed, shutdown).await,
            ExchangeId::Coinbase | ExchangeId::Kraken => Ok(()),
        }
    })
    .await?;
    Ok(())
}

fn planned_connection_urls(handler: &FeedHandler) -> Vec<String> {
    handler.feeds().iter().map(planned_url).collect()
}

fn planned_url(feed: &ExchangeFeed) -> String {
    match feed.exchange {
        ExchangeId::Binance => BinanceAdapter::subscription_url(feed),
        ExchangeId::Bitget => BitgetAdapter::subscription_url(feed),
        ExchangeId::Coinbase => String::new(),
        ExchangeId::Kraken => String::new(),
    }
}

async fn run_feeds_until_shutdown<F, Fut>(
    feeds: Vec<ExchangeFeed>,
    mut shutdown: watch::Receiver<bool>,
    consume: F,
) -> Result<()>
where
    F: Fn(ExchangeFeed, watch::Receiver<bool>) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    let mut tasks = JoinSet::new();

    for feed in feeds {
        let consume = consume.clone();
        tasks.spawn(consume(feed, shutdown.clone()));
    }

    while !tasks.is_empty() {
        tokio::select! {
            changed = shutdown.changed() => {
                match changed {
                    Ok(()) if *shutdown.borrow() => {
                        while let Some(result) = tasks.join_next().await {
                            result.map_err(|e| Error::Transport(e.to_string()))??;
                        }
                        return Ok(());
                    }
                    Ok(()) => {}
                    Err(_) => {
                        while let Some(result) = tasks.join_next().await {
                            result.map_err(|e| Error::Transport(e.to_string()))??;
                        }
                        return Ok(());
                    }
                }
            }
            result = tasks.join_next() => {
                if let Some(result) = result {
                    result.map_err(|e| Error::Transport(e.to_string()))??;
                }
            }
        }
    }

    Ok(())
}

async fn consume_binance_feed(feed: ExchangeFeed, shutdown: watch::Receiver<bool>) -> Result<()> {
    supervisor::retry_with_backoff(3, supervisor::Backoff::new(1, 8), move || {
        let feed = feed.clone();
        let shutdown = shutdown.clone();
        async move { consume_binance_session(feed, shutdown).await }
    })
    .await
}

async fn consume_bitget_feed(feed: ExchangeFeed, shutdown: watch::Receiver<bool>) -> Result<()> {
    supervisor::retry_with_backoff(3, supervisor::Backoff::new(1, 8), move || {
        let feed = feed.clone();
        let shutdown = shutdown.clone();
        async move { consume_bitget_session(feed, shutdown).await }
    })
    .await
}

async fn consume_binance_session(feed: ExchangeFeed, mut shutdown: watch::Receiver<bool>) -> Result<()> {
    let url = Url::parse(&planned_url(&feed)).map_err(|e| Error::Transport(e.to_string()))?;
    let connection = connection::WsConnection::new(url);
    let mut stream = connection.connect().await?;

    while let Some(text) = connection::next_text_message_or_shutdown(&mut stream, &mut shutdown).await? {
        process_binance_text_message(&feed, &text, current_timestamp()).await?;
    }

    Ok(())
}

async fn consume_bitget_session(feed: ExchangeFeed, mut shutdown: watch::Receiver<bool>) -> Result<()> {
    let url = Url::parse(&planned_url(&feed)).map_err(|e| Error::Transport(e.to_string()))?;
    let connection = connection::WsConnection::new(url);
    let mut stream = connection.connect().await?;
    let subscribe = BitgetAdapter::subscription_message(&feed);
    connection::send_text(&mut stream, &subscribe).await?;

    while let Some(text) = connection::next_text_message_or_shutdown(&mut stream, &mut shutdown).await? {
        process_bitget_text_message(&feed, &text, current_timestamp()).await?;
    }

    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
async fn dispatch_binance_event(feed: &ExchangeFeed, event: BinanceEvent) {
    match event {
        #[cfg(feature = "orderbook")]
        BinanceEvent::L2Book(book) => {
            if let Some(handler) = &feed.orderbook_handler {
                handler.on_l2_book(book).await;
            }
        }
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
async fn dispatch_bitget_event(feed: &ExchangeFeed, event: BitgetEvent) {
    match event {
        #[cfg(feature = "orderbook")]
        BitgetEvent::L2Book(book) => {
            if let Some(handler) = &feed.orderbook_handler {
                handler.on_l2_book(book).await;
            }
        }
        #[cfg(feature = "ticker")]
        BitgetEvent::Ticker(ticker) => {
            if let Some(handler) = &feed.ticker_handler {
                handler.on_ticker(ticker).await;
            }
        }
        #[cfg(feature = "trade")]
        BitgetEvent::Trade(trade) => {
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

#[cfg_attr(not(test), allow(dead_code))]
async fn process_bitget_text_message(feed: &ExchangeFeed, text: &str, received_ts: f64) -> Result<()> {
    let message: Value =
        serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
    if let Some(event) = BitgetAdapter::parse_message(&message, received_ts) {
        dispatch_bitget_event(feed, event).await;
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
    use std::time::Duration;

    use super::{planned_connection_urls, process_binance_text_message, process_bitget_text_message};
    use crate::{
        exchange::binance::{
            adapter::{BinanceAdapter, BinanceEvent},
            Binance,
        },
        exchange::bitget::{
            adapter::{BitgetAdapter, BitgetEvent},
            Bitget,
        },
        FeedHandler,
    };
    use async_trait::async_trait;
    #[cfg(feature = "ticker")]
    use cryptofeed_ticker::{Ticker, TickerHandler};
    #[cfg(feature = "orderbook")]
    use cryptofeed_orderbook::{L2Book, OrderBookHandler};
    #[cfg(feature = "trade")]
    use cryptofeed_trade::{Trade, TradeHandler};
    use tokio::sync::watch;
    use tokio::time::Instant;

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

    #[cfg(feature = "orderbook")]
    struct TestOrderBookHandler {
        seen: Arc<Mutex<usize>>,
    }

    #[cfg(feature = "orderbook")]
    #[async_trait]
    impl OrderBookHandler for TestOrderBookHandler {
        async fn on_l2_book(&self, _book: L2Book) {
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

    #[cfg(feature = "orderbook")]
    #[tokio::test]
    async fn dispatches_binance_l2_book_events() {
        let seen = Arc::new(Mutex::new(0));
        let feed = Binance::new()
            .l2_book()
            .orderbook_handler(Arc::new(TestOrderBookHandler { seen: seen.clone() }))
            .symbol("BTC-USDT")
            .build();

        let message = serde_json::json!({
            "e": "depthUpdate",
            "s": "BTCUSDT",
            "E": 1710000000456u64,
            "b": [["64999.10", "1.25"]],
            "a": [["65000.20", "0.75"]]
        });

        let event = BinanceAdapter::parse_message(&message, 1710000001.5).expect("book event");
        assert!(matches!(event, BinanceEvent::L2Book(_)));

        super::dispatch_binance_event(&feed, event).await;
        assert_eq!(*seen.lock().expect("lock"), 1);
    }

    #[tokio::test]
    async fn runs_multiple_feeds_concurrently() {
        let feeds = vec![
            Binance::new().ticker().symbol("BTC-USDT").build(),
            Binance::new().trade().symbol("ETH-USDT").build(),
        ];

        let started = Instant::now();
        let (_tx, rx) = watch::channel(false);
        super::run_feeds_until_shutdown(feeds, rx, |_, _| async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(())
        })
        .await
        .expect("concurrent run");

        assert!(started.elapsed() < Duration::from_millis(90));
    }

    #[tokio::test]
    async fn stops_feeds_on_shutdown_signal() {
        let feeds = vec![
            Binance::new().ticker().symbol("BTC-USDT").build(),
            Bitget::new().trade().symbol("BTC-USDT").build(),
        ];
        let (tx, rx) = watch::channel(false);
        let started = Arc::new(Mutex::new(0usize));
        let stopped = Arc::new(Mutex::new(0usize));

        let started_clone = started.clone();
        let stopped_clone = stopped.clone();
        let run = tokio::spawn(async move {
            super::run_feeds_until_shutdown(feeds, rx, move |_, mut shutdown| {
                let started = started_clone.clone();
                let stopped = stopped_clone.clone();
                async move {
                    *started.lock().expect("lock") += 1;
                    loop {
                        match shutdown.changed().await {
                            Ok(()) if *shutdown.borrow() => break,
                            Ok(()) => {}
                            Err(_) => break,
                        }
                    }
                    *stopped.lock().expect("lock") += 1;
                    Ok(())
                }
            })
            .await
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        tx.send(true).expect("shutdown signal");
        run.await.expect("join").expect("shutdown run");

        assert_eq!(*started.lock().expect("lock"), 2);
        assert_eq!(*stopped.lock().expect("lock"), 2);
    }

    #[cfg(all(feature = "ticker", feature = "trade", feature = "orderbook"))]
    #[tokio::test]
    async fn processes_bitget_trade_message() {
        let trade_seen = Arc::new(Mutex::new(0));
        let feed = Bitget::new()
            .trade()
            .trade_handler(Arc::new(TestTradeHandler {
                seen: trade_seen.clone(),
            }))
            .symbol("BTC-USDT")
            .build();

        let message = serde_json::json!({
            "arg": {"channel": "trade", "instId": "BTCUSDT"},
            "data": [[ "1710000000123", "65000.50", "0.0100", "buy" ]]
        });

        process_bitget_text_message(&feed, &message.to_string(), 1710000001.5)
            .await
            .expect("process bitget trade");

        assert_eq!(*trade_seen.lock().expect("lock"), 1);
    }

    #[cfg(all(feature = "ticker", feature = "trade", feature = "orderbook"))]
    #[tokio::test]
    async fn ignores_bitget_subscribe_ack_message() {
        let trade_seen = Arc::new(Mutex::new(0));
        let feed = Bitget::new()
            .trade()
            .trade_handler(Arc::new(TestTradeHandler {
                seen: trade_seen.clone(),
            }))
            .symbol("BTC-USDT")
            .build();

        let message = serde_json::json!({
            "event": "subscribe",
            "arg": { "instType": "spot", "topic": "publicTrade", "symbol": "BTCUSDT" }
        });

        process_bitget_text_message(&feed, &message.to_string(), 1710000001.5)
            .await
            .expect("process subscribe ack");

        assert_eq!(*trade_seen.lock().expect("lock"), 0);
    }

    #[cfg(all(feature = "ticker", feature = "trade", feature = "orderbook"))]
    #[tokio::test]
    async fn dispatches_bitget_l2_book_events() {
        let seen = Arc::new(Mutex::new(0));
        let feed = Bitget::new()
            .l2_book()
            .orderbook_handler(Arc::new(TestOrderBookHandler { seen: seen.clone() }))
            .symbol("BTC-USDT")
            .build();

        let message = serde_json::json!({
            "arg": {"channel": "books", "instId": "BTCUSDT"},
            "data": [{
                "bids": [["64999.10", "1.25"]],
                "asks": [["65000.20", "0.75"]],
                "ts": "1710000000456"
            }]
        });

        let event = BitgetAdapter::parse_message(&message, 1710000001.5).expect("bitget book");
        assert!(matches!(event, BitgetEvent::L2Book(_)));

        super::dispatch_bitget_event(&feed, event).await;
        assert_eq!(*seen.lock().expect("lock"), 1);
    }
}
