pub mod connection;
pub mod router;
pub mod supervisor;

use crate::exchange::{
    ExchangeFeed,
    binance::{
        adapter::{BinanceAdapter, BinanceEvent},
        book_sync::{BinanceBookSync, BinanceDepthDelta},
        parser as binance_parser,
    },
    bitget::{
        adapter::{BitgetAdapter, BitgetEvent},
        book_sync::{BitgetBookAction, BitgetBookSync, BitgetDepthUpdate},
        parser as bitget_parser,
    },
    bybit::adapter::BybitAdapter,
    bybit::adapter::BybitEvent,
    bybit::book_sync::BybitBookSync,
    bybit::parser as bybit_parser,
    okx::{
        adapter::{OkxAdapter, OkxEvent},
        book_sync::OkxBookSync,
        parser as okx_parser,
    },
};
use crate::feed::FeedHandler;
use cryptofeed_core::{
    error::{Error, Result},
    exchange::ExchangeId,
};
#[cfg(feature = "orderbook")]
use cryptofeed_orderbook::L2Book;
use serde_json::Value;
use std::future::Future;
#[cfg(feature = "orderbook")]
use tokio::sync::oneshot;
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
    run_feeds_until_shutdown(
        handler.into_feeds(),
        shutdown_rx,
        |feed, shutdown| async move {
            match feed.exchange {
                ExchangeId::Binance => consume_binance_feed(feed, shutdown).await,
                ExchangeId::Bitget => consume_bitget_feed(feed, shutdown).await,
                ExchangeId::Bybit => consume_bybit_feed(feed, shutdown).await,
                ExchangeId::Okx => consume_okx_feed(feed, shutdown).await,
                ExchangeId::Coinbase | ExchangeId::Kraken => Ok(()),
            }
        },
    )
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
        ExchangeId::Bybit => BybitAdapter::subscription_url(feed),
        ExchangeId::Coinbase => String::new(),
        ExchangeId::Kraken => String::new(),
        ExchangeId::Okx => OkxAdapter::subscription_url(feed),
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

async fn consume_bybit_feed(feed: ExchangeFeed, shutdown: watch::Receiver<bool>) -> Result<()> {
    supervisor::retry_with_backoff(3, supervisor::Backoff::new(1, 8), move || {
        let feed = feed.clone();
        let shutdown = shutdown.clone();
        async move { consume_bybit_session(feed, shutdown).await }
    })
    .await
}

async fn consume_okx_feed(feed: ExchangeFeed, shutdown: watch::Receiver<bool>) -> Result<()> {
    supervisor::retry_with_backoff(3, supervisor::Backoff::new(1, 8), move || {
        let feed = feed.clone();
        let shutdown = shutdown.clone();
        async move { consume_okx_session(feed, shutdown).await }
    })
    .await
}

async fn consume_binance_session(
    feed: ExchangeFeed,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let url = Url::parse(&planned_url(&feed)).map_err(|e| Error::Transport(e.to_string()))?;
    let connection = connection::WsConnection::new(url);
    let mut stream = connection.connect().await?;
    #[cfg(feature = "orderbook")]
    let mut snapshot_receivers = spawn_binance_snapshot_fetches(&feed);
    #[cfg(feature = "orderbook")]
    let mut pending_deltas: std::collections::HashMap<String, Vec<BinanceDepthDelta>> =
        std::collections::HashMap::new();

    while let Some(text) =
        connection::next_text_message_or_shutdown(&mut stream, &mut shutdown).await?
    {
        #[cfg(feature = "orderbook")]
        poll_binance_snapshot_bootstraps(&feed, &mut snapshot_receivers, &mut pending_deltas)
            .await?;
        if !process_binance_orderbook_message(
            &feed,
            &text,
            current_timestamp(),
            &mut snapshot_receivers,
            &mut pending_deltas,
        )
        .await?
        {
            process_binance_text_message(&feed, &text, current_timestamp()).await?;
        }
    }

    Ok(())
}

async fn consume_bitget_session(
    feed: ExchangeFeed,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let url = Url::parse(&planned_url(&feed)).map_err(|e| Error::Transport(e.to_string()))?;
    let connection = connection::WsConnection::new(url);
    let mut stream = connection.connect().await?;
    let subscribe = BitgetAdapter::subscription_message(&feed);
    connection::send_text(&mut stream, &subscribe).await?;

    while let Some(text) =
        connection::next_text_message_or_shutdown(&mut stream, &mut shutdown).await?
    {
        process_bitget_text_message(&feed, &text, current_timestamp()).await?;
    }

    Ok(())
}

async fn consume_bybit_session(
    feed: ExchangeFeed,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let url = Url::parse(&planned_url(&feed)).map_err(|e| Error::Transport(e.to_string()))?;
    let connection = connection::WsConnection::new(url);
    let mut stream = connection.connect().await?;
    let subscribe = BybitAdapter::subscription_message(&feed);
    connection::send_text(&mut stream, &subscribe).await?;

    while let Some(text) =
        connection::next_text_message_or_shutdown(&mut stream, &mut shutdown).await?
    {
        process_bybit_text_message(&feed, &text, current_timestamp()).await?;
    }

    Ok(())
}

async fn consume_okx_session(
    feed: ExchangeFeed,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let url = Url::parse(&planned_url(&feed)).map_err(|e| Error::Transport(e.to_string()))?;
    let connection = connection::WsConnection::new(url);
    let mut stream = connection.connect().await?;
    let subscribe = OkxAdapter::subscription_message(&feed);
    connection::send_text(&mut stream, &subscribe).await?;

    while let Some(text) =
        connection::next_text_message_or_shutdown(&mut stream, &mut shutdown).await?
    {
        process_okx_text_message(&feed, &text, current_timestamp()).await?;
    }

    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
async fn dispatch_binance_event(feed: &ExchangeFeed, event: BinanceEvent) {
    match event {
        #[cfg(feature = "candles")]
        BinanceEvent::Candle(candle) => {
            if let Some(handler) = &feed.candle_handler {
                handler.on_candle(candle).await;
            }
        }
        #[cfg(feature = "funding")]
        BinanceEvent::Funding(funding) => {
            if let Some(handler) = &feed.funding_handler {
                handler.on_funding(funding).await;
            }
        }
        #[cfg(feature = "liquidations")]
        BinanceEvent::Liquidation(liquidation) => {
            if let Some(handler) = &feed.liquidation_handler {
                handler.on_liquidation(liquidation).await;
            }
        }
        #[cfg(feature = "orderbook")]
        BinanceEvent::L2Book(book) => {
            apply_orderbook_state(feed, &book);
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
        #[cfg(feature = "candles")]
        BitgetEvent::Candle(candle) => {
            if let Some(handler) = &feed.candle_handler {
                handler.on_candle(candle).await;
            }
        }
        #[cfg(feature = "orderbook")]
        BitgetEvent::L2Book(book) => {
            dispatch_bitget_l2_book(feed, book).await;
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

#[cfg(feature = "orderbook")]
async fn dispatch_bitget_l2_book(feed: &ExchangeFeed, book: L2Book) {
    apply_orderbook_state(feed, &book);
    if let Some(handler) = &feed.orderbook_handler {
        handler.on_l2_book(book).await;
    }
}

#[cfg_attr(not(test), allow(dead_code))]
async fn dispatch_bybit_event(feed: &ExchangeFeed, event: BybitEvent) {
    match event {
        #[cfg(feature = "candles")]
        BybitEvent::Candle(candle) => {
            if let Some(handler) = &feed.candle_handler {
                handler.on_candle(candle).await;
            }
        }
        #[cfg(feature = "orderbook")]
        BybitEvent::L2Book(book) => {
            dispatch_bybit_l2_book(feed, book).await;
        }
        #[cfg(feature = "ticker")]
        BybitEvent::Ticker(ticker) => {
            if let Some(handler) = &feed.ticker_handler {
                handler.on_ticker(ticker).await;
            }
        }
        #[cfg(feature = "trade")]
        BybitEvent::Trade(trade) => {
            if let Some(handler) = &feed.trade_handler {
                handler.on_trade(trade).await;
            }
        }
    }
}

#[cfg(feature = "orderbook")]
async fn dispatch_bybit_l2_book(feed: &ExchangeFeed, book: L2Book) {
    apply_orderbook_state(feed, &book);
    if let Some(handler) = &feed.orderbook_handler {
        handler.on_l2_book(book).await;
    }
}

#[cfg_attr(not(test), allow(dead_code))]
async fn dispatch_okx_event(feed: &ExchangeFeed, event: OkxEvent) {
    match event {
        #[cfg(feature = "candles")]
        OkxEvent::Candle(candle) => {
            if let Some(handler) = &feed.candle_handler {
                handler.on_candle(candle).await;
            }
        }
        #[cfg(feature = "orderbook")]
        OkxEvent::L2Book(book) => {
            dispatch_okx_l2_book(feed, book).await;
        }
        #[cfg(feature = "ticker")]
        OkxEvent::Ticker(ticker) => {
            if let Some(handler) = &feed.ticker_handler {
                handler.on_ticker(ticker).await;
            }
        }
        #[cfg(feature = "trade")]
        OkxEvent::Trade(trade) => {
            if let Some(handler) = &feed.trade_handler {
                handler.on_trade(trade).await;
            }
        }
    }
}

#[cfg(feature = "orderbook")]
async fn dispatch_okx_l2_book(feed: &ExchangeFeed, book: L2Book) {
    apply_orderbook_state(feed, &book);
    if let Some(handler) = &feed.orderbook_handler {
        handler.on_l2_book(book).await;
    }
}

#[cfg_attr(not(test), allow(dead_code))]
async fn process_binance_text_message(
    feed: &ExchangeFeed,
    text: &str,
    received_ts: f64,
) -> Result<()> {
    let message: Value = serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
    if let Some(event) = BinanceAdapter::parse_message(&message, received_ts) {
        dispatch_binance_event(feed, event).await;
    }
    Ok(())
}

#[cfg(feature = "orderbook")]
fn apply_orderbook_state(feed: &ExchangeFeed, book: &cryptofeed_orderbook::L2Book) {
    use cryptofeed_orderbook::L2Book;

    let symbol = match book {
        L2Book::Snapshot(snapshot) => snapshot.symbol.as_str().to_owned(),
        L2Book::Delta(delta) => delta.symbol.as_str().to_owned(),
    };

    let mut states = feed.orderbook_states.lock().expect("orderbook state lock");
    let state = states.entry(symbol.clone()).or_insert_with(|| {
        cryptofeed_orderbook::L2BookState::new(match book {
            L2Book::Snapshot(snapshot) => snapshot.symbol.clone(),
            L2Book::Delta(delta) => delta.symbol.clone(),
        })
    });
    state.apply(book.clone());
}

#[cfg(feature = "orderbook")]
async fn dispatch_binance_l2_book(feed: &ExchangeFeed, book: L2Book) {
    apply_orderbook_state(feed, &book);
    if let Some(handler) = &feed.orderbook_handler {
        handler.on_l2_book(book).await;
    }
}

#[cfg(feature = "orderbook")]
fn spawn_binance_snapshot_fetches(
    feed: &ExchangeFeed,
) -> std::collections::HashMap<
    String,
    oneshot::Receiver<Result<(u64, cryptofeed_orderbook::L2BookSnapshot)>>,
> {
    let mut receivers = std::collections::HashMap::new();

    for symbol in &feed.symbols {
        let symbol_key = symbol.as_str().to_owned();
        let rx = spawn_binance_snapshot_fetch(symbol.clone());
        receivers.insert(symbol_key, rx);
    }

    receivers
}

#[cfg(feature = "orderbook")]
fn spawn_binance_snapshot_fetch(
    symbol: cryptofeed_core::symbol::Symbol,
) -> oneshot::Receiver<Result<(u64, cryptofeed_orderbook::L2BookSnapshot)>> {
    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        let _ = tx.send(fetch_binance_l2_snapshot(symbol).await);
    });
    rx
}

#[cfg(feature = "orderbook")]
async fn fetch_binance_l2_snapshot(
    symbol: cryptofeed_core::symbol::Symbol,
) -> Result<(u64, cryptofeed_orderbook::L2BookSnapshot)> {
    let exchange_symbol = symbol.as_str().replace('-', "");
    let url = format!("https://api.binance.com/api/v3/depth?symbol={exchange_symbol}&limit=1000");
    let response = reqwest::get(&url)
        .await
        .map_err(|e| Error::Transport(e.to_string()))?;
    let payload: Value = response
        .json()
        .await
        .map_err(|e| Error::Parse(e.to_string()))?;
    binance_parser::parse_l2_book_snapshot(&payload, &exchange_symbol, current_timestamp())
        .ok_or_else(|| Error::Parse("failed to parse binance l2 snapshot".to_owned()))
}

#[cfg(feature = "orderbook")]
async fn poll_binance_snapshot_bootstraps(
    feed: &ExchangeFeed,
    receivers: &mut std::collections::HashMap<
        String,
        oneshot::Receiver<Result<(u64, cryptofeed_orderbook::L2BookSnapshot)>>,
    >,
    pending: &mut std::collections::HashMap<String, Vec<BinanceDepthDelta>>,
) -> Result<()> {
    use tokio::sync::oneshot::error::TryRecvError;

    let keys: Vec<String> = receivers.keys().cloned().collect();
    for key in keys {
        let ready = if let Some(receiver) = receivers.get_mut(&key) {
            match receiver.try_recv() {
                Ok(result) => Some(result),
                Err(TryRecvError::Empty) => None,
                Err(TryRecvError::Closed) => {
                    return Err(Error::Transport(
                        "binance snapshot bootstrap channel closed".to_owned(),
                    ));
                }
            }
        } else {
            None
        };

        if let Some(result) = ready {
            let (last_update_id, snapshot) = result?;
            if let Some(book) = bootstrap_binance_book(
                feed,
                &key,
                last_update_id,
                snapshot,
                pending.remove(&key).unwrap_or_default(),
            )? {
                dispatch_binance_l2_book(feed, book).await;
            }
            receivers.remove(&key);
        }
    }

    Ok(())
}

#[cfg(feature = "orderbook")]
fn bootstrap_binance_book(
    feed: &ExchangeFeed,
    symbol_key: &str,
    last_update_id: u64,
    snapshot: cryptofeed_orderbook::L2BookSnapshot,
    buffered: Vec<BinanceDepthDelta>,
) -> Result<Option<cryptofeed_orderbook::L2Book>> {
    let mut syncs = feed.binance_book_syncs.lock().expect("binance sync lock");
    let sync = syncs
        .entry(symbol_key.to_owned())
        .or_insert_with(|| BinanceBookSync::new(snapshot.symbol.clone()));
    sync.bootstrap(last_update_id, snapshot.clone(), buffered)?;

    let mut states = feed.orderbook_states.lock().expect("orderbook state lock");
    states.insert(symbol_key.to_owned(), sync.state().clone());

    Ok(Some(cryptofeed_orderbook::L2Book::Snapshot(snapshot)))
}

#[cfg(feature = "orderbook")]
async fn process_binance_orderbook_message(
    feed: &ExchangeFeed,
    text: &str,
    received_ts: f64,
    receivers: &mut std::collections::HashMap<
        String,
        oneshot::Receiver<Result<(u64, cryptofeed_orderbook::L2BookSnapshot)>>,
    >,
    pending: &mut std::collections::HashMap<String, Vec<BinanceDepthDelta>>,
) -> Result<bool> {
    let message: Value = serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
    let payload = BinanceAdapter::unwrap_combined_message(&message).unwrap_or(&message);

    if payload.get("e").and_then(|v| v.as_str()) != Some("depthUpdate") {
        return Ok(false);
    }

    let update = binance_parser::parse_l2_book_update(payload, received_ts)
        .ok_or_else(|| Error::Parse("failed to parse binance depth update".to_owned()))?;
    let symbol_key = update.book.symbol.as_str().to_owned();

    if feed
        .binance_book_syncs
        .lock()
        .expect("binance sync lock")
        .contains_key(&symbol_key)
    {
        let maybe_book = {
            let mut syncs = feed.binance_book_syncs.lock().expect("binance sync lock");
            let sync = syncs.get_mut(&symbol_key).expect("binance sync state");
            sync.apply_next_delta(update.clone())
        };

        match maybe_book {
            Ok(Some(book)) => {
                dispatch_binance_l2_book(feed, book).await;
            }
            Ok(None) => {}
            Err(err) => {
                if matches!(err, Error::Parse(_)) {
                    schedule_binance_resync(feed, &symbol_key, update, receivers, pending);
                } else {
                    return Err(err);
                }
            }
        }
    } else {
        pending.entry(symbol_key).or_default().push(update);
    }

    Ok(true)
}

#[cfg(feature = "orderbook")]
fn reset_binance_sync_state(
    feed: &ExchangeFeed,
    symbol_key: &str,
    update: BinanceDepthDelta,
    pending: &mut std::collections::HashMap<String, Vec<BinanceDepthDelta>>,
) {
    feed.binance_book_syncs
        .lock()
        .expect("binance sync lock")
        .remove(symbol_key);
    pending
        .entry(symbol_key.to_owned())
        .or_default()
        .push(update);
}

#[cfg(feature = "orderbook")]
fn schedule_binance_resync(
    feed: &ExchangeFeed,
    symbol_key: &str,
    update: BinanceDepthDelta,
    receivers: &mut std::collections::HashMap<
        String,
        oneshot::Receiver<Result<(u64, cryptofeed_orderbook::L2BookSnapshot)>>,
    >,
    pending: &mut std::collections::HashMap<String, Vec<BinanceDepthDelta>>,
) {
    let symbol = update.book.symbol.clone();
    reset_binance_sync_state(feed, symbol_key, update, pending);
    receivers
        .entry(symbol_key.to_owned())
        .or_insert_with(|| spawn_binance_snapshot_fetch(symbol));
}

#[cfg_attr(not(test), allow(dead_code))]
async fn process_bitget_text_message(
    feed: &ExchangeFeed,
    text: &str,
    received_ts: f64,
) -> Result<()> {
    let message: Value = serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
    if process_bitget_orderbook_message(feed, &message, received_ts).await? {
        return Ok(());
    }
    if let Some(event) = BitgetAdapter::parse_message(&message, received_ts) {
        dispatch_bitget_event(feed, event).await;
    }
    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
async fn process_bybit_text_message(
    feed: &ExchangeFeed,
    text: &str,
    received_ts: f64,
) -> Result<()> {
    let message: Value = serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
    if process_bybit_orderbook_message(feed, &message, received_ts).await? {
        return Ok(());
    }
    if let Some(event) = BybitAdapter::parse_message(&message, received_ts) {
        dispatch_bybit_event(feed, event).await;
    }
    Ok(())
}

#[cfg(feature = "orderbook")]
async fn process_bybit_orderbook_message(
    feed: &ExchangeFeed,
    message: &Value,
    received_ts: f64,
) -> Result<bool> {
    let topic = match message.get("topic").and_then(|v| v.as_str()) {
        Some(topic) => topic,
        None => return Ok(false),
    };

    if !topic.starts_with("orderbook.") {
        return Ok(false);
    }

    let update = bybit_parser::parse_l2_book_update(message, received_ts)
        .ok_or_else(|| Error::Parse("failed to parse bybit depth update".to_owned()))?;
    let symbol_key = match &update.book {
        L2Book::Snapshot(snapshot) => snapshot.symbol.as_str().to_owned(),
        L2Book::Delta(delta) => delta.symbol.as_str().to_owned(),
    };
    let maybe_book = {
        let mut syncs = feed.bybit_book_syncs.lock().expect("bybit sync lock");
        let sync = syncs.entry(symbol_key).or_insert_with(|| {
            BybitBookSync::new(match &update.book {
                L2Book::Snapshot(snapshot) => snapshot.symbol.clone(),
                L2Book::Delta(delta) => delta.symbol.clone(),
            })
        });
        sync.apply(update)?
    };

    if let Some(book) = maybe_book {
        dispatch_bybit_l2_book(feed, book).await;
    }

    Ok(true)
}

#[cfg_attr(not(test), allow(dead_code))]
async fn process_okx_text_message(feed: &ExchangeFeed, text: &str, received_ts: f64) -> Result<()> {
    let message: Value = serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
    if process_okx_orderbook_message(feed, &message, received_ts).await? {
        return Ok(());
    }
    if let Some(event) = OkxAdapter::parse_message(&message, received_ts) {
        dispatch_okx_event(feed, event).await;
    }
    Ok(())
}

#[cfg(feature = "orderbook")]
async fn process_okx_orderbook_message(
    feed: &ExchangeFeed,
    message: &Value,
    received_ts: f64,
) -> Result<bool> {
    let arg = match message.get("arg") {
        Some(arg) => arg,
        None => return Ok(false),
    };
    let channel = match arg.get("channel").and_then(|v| v.as_str()) {
        Some(channel) => channel,
        None => return Ok(false),
    };

    if !matches!(channel, "books" | "books5" | "bbo-tbt") {
        return Ok(false);
    }

    let update = okx_parser::parse_l2_book_update(message, received_ts)
        .ok_or_else(|| Error::Parse("failed to parse okx depth update".to_owned()))?;
    let symbol_key = match &update.book {
        L2Book::Snapshot(snapshot) => snapshot.symbol.as_str().to_owned(),
        L2Book::Delta(delta) => delta.symbol.as_str().to_owned(),
    };
    let maybe_book = {
        let mut syncs = feed.okx_book_syncs.lock().expect("okx sync lock");
        let sync = syncs.entry(symbol_key).or_insert_with(|| {
            OkxBookSync::new(match &update.book {
                L2Book::Snapshot(snapshot) => snapshot.symbol.clone(),
                L2Book::Delta(delta) => delta.symbol.clone(),
            })
        });
        sync.apply(update)?
    };

    if let Some(book) = maybe_book {
        dispatch_okx_l2_book(feed, book).await;
    }

    Ok(true)
}

#[cfg(feature = "orderbook")]
async fn process_bitget_orderbook_message(
    feed: &ExchangeFeed,
    message: &Value,
    received_ts: f64,
) -> Result<bool> {
    let arg = match message.get("arg") {
        Some(arg) => arg,
        None => return Ok(false),
    };
    let topic = match arg
        .get("topic")
        .or_else(|| arg.get("channel"))
        .and_then(|v| v.as_str())
    {
        Some(topic) => topic,
        None => return Ok(false),
    };

    if !matches!(topic, "books" | "books1" | "books5" | "books50") {
        return Ok(false);
    }

    let book = bitget_parser::parse_l2_book(message, received_ts)
        .ok_or_else(|| Error::Parse("failed to parse bitget depth update".to_owned()))?;
    let data = message
        .get("data")
        .and_then(|v| v.as_array())
        .and_then(|v| v.first())
        .ok_or_else(|| Error::Parse("missing bitget depth payload".to_owned()))?;
    let seq = data
        .get("seq")
        .and_then(|v| {
            v.as_u64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .unwrap_or(0);
    let pseq = data
        .get("pseq")
        .and_then(|v| {
            v.as_u64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .unwrap_or(0);
    let action = match message.get("action").and_then(|v| v.as_str()) {
        Some("snapshot") => BitgetBookAction::Snapshot,
        _ => BitgetBookAction::Update,
    };
    let update = BitgetDepthUpdate {
        action,
        seq,
        pseq,
        book,
    };
    let symbol_key = match &update.book {
        L2Book::Snapshot(snapshot) => snapshot.symbol.as_str().to_owned(),
        L2Book::Delta(delta) => delta.symbol.as_str().to_owned(),
    };

    let maybe_book = {
        let mut syncs = feed.bitget_book_syncs.lock().expect("bitget sync lock");
        let sync = syncs.entry(symbol_key).or_insert_with(|| {
            BitgetBookSync::new(match &update.book {
                L2Book::Snapshot(snapshot) => snapshot.symbol.clone(),
                L2Book::Delta(delta) => delta.symbol.clone(),
            })
        });
        sync.apply(update)?
    };

    if let Some(book) = maybe_book {
        dispatch_bitget_l2_book(feed, book).await;
    }

    Ok(true)
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

    use super::{
        planned_connection_urls, process_binance_text_message, process_bitget_text_message,
        process_bybit_text_message, process_okx_text_message,
    };
    use crate::{
        FeedHandler,
        exchange::binance::{
            Binance,
            adapter::{BinanceAdapter, BinanceEvent},
        },
        exchange::bitget::{
            Bitget,
            adapter::{BitgetAdapter, BitgetEvent},
        },
        exchange::bybit::Bybit,
        exchange::okx::Okx,
    };
    use async_trait::async_trait;
    use cryptofeed_core::error::Error;
    #[cfg(feature = "orderbook")]
    use cryptofeed_orderbook::{L2Book, OrderBookHandler};
    #[cfg(feature = "ticker")]
    use cryptofeed_ticker::{Ticker, TickerHandler};
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

    #[test]
    fn plans_bitget_connection_urls() {
        let mut handler = FeedHandler::new();
        handler.add_feed(
            Bitget::new()
                .ticker()
                .trade()
                .l2_book()
                .symbol("BTC-USDT")
                .build(),
        );

        assert_eq!(
            planned_connection_urls(&handler),
            vec!["wss://ws.bitget.com/v3/ws/public".to_owned()]
        );
    }

    #[test]
    fn plans_bybit_connection_urls() {
        let mut handler = FeedHandler::new();
        handler.add_feed(
            Bybit::new()
                .ticker()
                .trade()
                .l2_book()
                .candles()
                .symbol("BTC-USDT")
                .build(),
        );

        assert_eq!(
            planned_connection_urls(&handler),
            vec!["wss://stream.bybit.com/v5/public/spot".to_owned()]
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
        let states = feed.orderbook_states.lock().expect("lock");
        let state = states.get("BTC-USDT").expect("btc-usdt state");
        assert_eq!(state.bids().len(), 1);
        assert_eq!(state.asks().len(), 1);
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn bootstrap_binance_book_initializes_sync_state() {
        let feed = Binance::new().l2_book().symbol("BTC-USDT").build();
        let snapshot = cryptofeed_orderbook::L2BookSnapshot {
            exchange: cryptofeed_core::exchange::ExchangeId::Binance,
            symbol: cryptofeed_core::symbol::Symbol::spot("btc", "usdt"),
            bids: vec![cryptofeed_orderbook::PriceLevel {
                price: rust_decimal::Decimal::from_str_exact("64999.10").unwrap(),
                amount: rust_decimal::Decimal::from_str_exact("1.25").unwrap(),
            }],
            asks: vec![cryptofeed_orderbook::PriceLevel {
                price: rust_decimal::Decimal::from_str_exact("65000.20").unwrap(),
                amount: rust_decimal::Decimal::from_str_exact("0.75").unwrap(),
            }],
            exchange_ts: 1.0,
            received_ts: 2.0,
        };

        super::bootstrap_binance_book(&feed, "BTC-USDT", 100, snapshot, vec![]).expect("bootstrap");

        let syncs = feed.binance_book_syncs.lock().expect("syncs");
        let sync = syncs.get("BTC-USDT").expect("book sync");
        assert_eq!(sync.last_update_id(), Some(100));
    }

    #[cfg(feature = "orderbook")]
    #[tokio::test]
    async fn bootstrap_binance_book_dispatches_snapshot() {
        let seen = Arc::new(Mutex::new(0));
        let feed = Binance::new()
            .l2_book()
            .orderbook_handler(Arc::new(TestOrderBookHandler { seen: seen.clone() }))
            .symbol("BTC-USDT")
            .build();
        let snapshot = cryptofeed_orderbook::L2BookSnapshot {
            exchange: cryptofeed_core::exchange::ExchangeId::Binance,
            symbol: cryptofeed_core::symbol::Symbol::spot("btc", "usdt"),
            bids: vec![cryptofeed_orderbook::PriceLevel {
                price: rust_decimal::Decimal::from_str_exact("64999.10").unwrap(),
                amount: rust_decimal::Decimal::from_str_exact("1.25").unwrap(),
            }],
            asks: vec![cryptofeed_orderbook::PriceLevel {
                price: rust_decimal::Decimal::from_str_exact("65000.20").unwrap(),
                amount: rust_decimal::Decimal::from_str_exact("0.75").unwrap(),
            }],
            exchange_ts: 1.0,
            received_ts: 2.0,
        };

        let book = super::bootstrap_binance_book(&feed, "BTC-USDT", 100, snapshot, vec![])
            .expect("bootstrap")
            .expect("snapshot event");
        super::dispatch_binance_l2_book(&feed, book).await;

        assert_eq!(*seen.lock().expect("lock"), 1);
        let states = feed.orderbook_states.lock().expect("lock");
        let state = states.get("BTC-USDT").expect("btc-usdt state");
        assert_eq!(state.bids().len(), 1);
        assert_eq!(state.asks().len(), 1);
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn reset_binance_sync_moves_delta_back_to_pending() {
        let feed = Binance::new().l2_book().symbol("BTC-USDT").build();
        let update = crate::exchange::binance::book_sync::BinanceDepthDelta {
            first_update_id: 102,
            last_update_id: 103,
            book: cryptofeed_orderbook::L2BookDelta {
                exchange: cryptofeed_core::exchange::ExchangeId::Binance,
                symbol: cryptofeed_core::symbol::Symbol::spot("btc", "usdt"),
                bids: vec![],
                asks: vec![],
                exchange_ts: 1.0,
                received_ts: 2.0,
            },
        };
        let mut pending = std::collections::HashMap::new();
        {
            let mut syncs = feed.binance_book_syncs.lock().expect("syncs");
            syncs.insert(
                "BTC-USDT".to_owned(),
                crate::exchange::binance::book_sync::BinanceBookSync::new(
                    cryptofeed_core::symbol::Symbol::spot("btc", "usdt"),
                ),
            );
        }

        super::reset_binance_sync_state(&feed, "BTC-USDT", update, &mut pending);

        let syncs = feed.binance_book_syncs.lock().expect("syncs");
        assert!(!syncs.contains_key("BTC-USDT"));
        let queued = pending.get("BTC-USDT").expect("pending delta");
        assert_eq!(queued.len(), 1);
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
    async fn processes_bybit_trade_message() {
        let trade_seen = Arc::new(Mutex::new(0));
        let feed = Bybit::new()
            .trade()
            .trade_handler(Arc::new(TestTradeHandler {
                seen: trade_seen.clone(),
            }))
            .symbol("BTC-USDT")
            .build();

        let message = serde_json::json!({
            "topic": "publicTrade.BTCUSDT",
            "type": "snapshot",
            "ts": 1672304486868i64,
            "data": [{
                "T": 1672304486865i64,
                "s": "BTCUSDT",
                "S": "Buy",
                "v": "0.001",
                "p": "16578.50",
                "i": "20f43950"
            }]
        });

        process_bybit_text_message(&feed, &message.to_string(), 1672304487.0)
            .await
            .expect("process bybit trade");

        assert_eq!(*trade_seen.lock().expect("lock"), 1);
    }

    #[cfg(all(feature = "ticker", feature = "trade", feature = "orderbook"))]
    #[tokio::test]
    async fn dispatches_bybit_l2_book_events() {
        let seen = Arc::new(Mutex::new(0));
        let feed = Bybit::new()
            .l2_book()
            .orderbook_handler(Arc::new(TestOrderBookHandler { seen: seen.clone() }))
            .symbol("BTC-USDT")
            .build();

        let message = serde_json::json!({
            "topic": "orderbook.50.BTCUSDT",
            "type": "snapshot",
            "ts": 1672304484978i64,
            "data": {
                "s": "BTCUSDT",
                "b": [["16493.50", "0.006"]],
                "a": [["16493.60", "0.100"]],
                "u": 18521288u64,
                "seq": 7961638724u64
            }
        });

        process_bybit_text_message(&feed, &message.to_string(), 1672304485.0)
            .await
            .expect("process bybit book");

        assert_eq!(*seen.lock().expect("lock"), 1);
        let states = feed.orderbook_states.lock().expect("lock");
        let state = states.get("BTC-USDT").expect("btc-usdt state");
        assert_eq!(state.bids().len(), 1);
        assert_eq!(state.asks().len(), 1);
    }

    #[test]
    fn plans_okx_connection_urls() {
        let mut handler = FeedHandler::new();
        handler.add_feed(
            Okx::new()
                .ticker()
                .trade()
                .l2_book()
                .candles()
                .symbol("BTC-USDT")
                .build(),
        );

        assert_eq!(
            planned_connection_urls(&handler),
            vec!["wss://ws.okx.com:8443/ws/v5/public".to_owned()]
        );
    }

    #[cfg(all(feature = "ticker", feature = "trade", feature = "orderbook"))]
    #[tokio::test]
    async fn processes_okx_trade_message() {
        let trade_seen = Arc::new(Mutex::new(0));
        let feed = Okx::new()
            .trade()
            .trade_handler(Arc::new(TestTradeHandler {
                seen: trade_seen.clone(),
            }))
            .symbol("BTC-USDT")
            .build();

        let message = serde_json::json!({
            "arg": {"channel": "trades", "instId": "BTC-USDT"},
            "data": [{ "tradeId": "1", "px": "65000.50", "sz": "0.0100", "side": "buy", "ts": "1710000000123" }]
        });

        process_okx_text_message(&feed, &message.to_string(), 1710000001.5)
            .await
            .expect("process okx trade");

        assert_eq!(*trade_seen.lock().expect("lock"), 1);
    }

    #[cfg(all(feature = "ticker", feature = "trade", feature = "orderbook"))]
    #[tokio::test]
    async fn dispatches_okx_l2_book_events() {
        let seen = Arc::new(Mutex::new(0));
        let feed = Okx::new()
            .l2_book()
            .orderbook_handler(Arc::new(TestOrderBookHandler { seen: seen.clone() }))
            .symbol("BTC-USDT")
            .build();

        let message = serde_json::json!({
            "arg": {"channel": "books", "instId": "BTC-USDT"},
            "action": "snapshot",
            "data": [{
                "bids": [["64999.10", "1.25", "0", "1"]],
                "asks": [["65000.20", "0.75", "0", "1"]],
                "ts": "1710000000456",
                "seqId": 100i64,
                "prevSeqId": -1i64
            }]
        });

        process_okx_text_message(&feed, &message.to_string(), 1710000001.5)
            .await
            .expect("process okx book");

        assert_eq!(*seen.lock().expect("lock"), 1);
        let states = feed.orderbook_states.lock().expect("lock");
        let state = states.get("BTC-USDT").expect("btc-usdt state");
        assert_eq!(state.bids().len(), 1);
        assert_eq!(state.asks().len(), 1);
    }

    #[cfg(all(feature = "ticker", feature = "trade", feature = "orderbook"))]
    #[tokio::test]
    async fn okx_book_gap_returns_error() {
        let feed = Okx::new().l2_book().symbol("BTC-USDT").build();
        let snapshot = serde_json::json!({
            "arg": {"channel": "books", "instId": "BTC-USDT"},
            "action": "snapshot",
            "data": [{
                "bids": [["64999.10", "1.25", "0", "1"]],
                "asks": [["65000.20", "0.75", "0", "1"]],
                "ts": "1710000000456",
                "seqId": 100i64,
                "prevSeqId": -1i64
            }]
        });
        let gap_update = serde_json::json!({
            "arg": {"channel": "books", "instId": "BTC-USDT"},
            "action": "update",
            "data": [{
                "bids": [["64999.10", "1.25", "0", "1"]],
                "asks": [["65000.20", "0.75", "0", "1"]],
                "ts": "1710000000457",
                "seqId": 102i64,
                "prevSeqId": 999i64
            }]
        });

        process_okx_text_message(&feed, &snapshot.to_string(), 1710000001.5)
            .await
            .expect("snapshot");
        let err = process_okx_text_message(&feed, &gap_update.to_string(), 1710000001.6)
            .await
            .expect_err("gap should fail");

        match err {
            Error::Parse(message) => assert!(message.contains("sequence gap")),
            _ => panic!("unexpected error variant"),
        }
    }

    #[cfg(all(feature = "ticker", feature = "trade", feature = "orderbook"))]
    #[tokio::test]
    async fn dispatches_bitget_ticker_events() {
        let seen = Arc::new(Mutex::new(0));
        let feed = Bitget::new()
            .ticker()
            .ticker_handler(Arc::new(TestTickerHandler { seen: seen.clone() }))
            .symbol("BTC-USDT")
            .build();

        let message = serde_json::json!({
            "arg": {"instType": "spot", "topic": "ticker", "symbol": "BTCUSDT"},
            "data": [{ "bid1Price": "64999.10", "ask1Price": "65000.20" }],
            "ts": "1710000000456"
        });

        let event = BitgetAdapter::parse_message(&message, 1710000001.5).expect("bitget ticker");
        assert!(matches!(event, BitgetEvent::Ticker(_)));

        super::dispatch_bitget_event(&feed, event).await;
        assert_eq!(*seen.lock().expect("lock"), 1);
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
        let states = feed.orderbook_states.lock().expect("lock");
        let state = states.get("BTC-USDT").expect("btc-usdt state");
        assert_eq!(state.bids().len(), 1);
        assert_eq!(state.asks().len(), 1);
    }

    #[cfg(all(feature = "ticker", feature = "trade", feature = "orderbook"))]
    #[tokio::test]
    async fn bitget_book_gap_returns_error() {
        let feed = Bitget::new().l2_book().symbol("BTC-USDT").build();
        let snapshot = serde_json::json!({
            "arg": {"instType": "spot", "topic": "books", "symbol": "BTCUSDT"},
            "action": "snapshot",
            "data": [{
                "b": [["64999.10", "1.25"]],
                "a": [["65000.20", "0.75"]],
                "seq": 100u64,
                "pseq": 0u64,
                "ts": "1710000000456"
            }]
        });
        let gap_update = serde_json::json!({
            "arg": {"instType": "spot", "topic": "books", "symbol": "BTCUSDT"},
            "action": "update",
            "data": [{
                "b": [["64999.10", "1.25"]],
                "a": [["65000.20", "0.75"]],
                "seq": 102u64,
                "pseq": 999u64,
                "ts": "1710000000457"
            }]
        });

        process_bitget_text_message(&feed, &snapshot.to_string(), 1710000001.5)
            .await
            .expect("snapshot");
        let err = process_bitget_text_message(&feed, &gap_update.to_string(), 1710000001.6)
            .await
            .expect_err("gap should fail");

        match err {
            Error::Parse(message) => assert!(message.contains("sequence gap")),
            _ => panic!("unexpected error variant"),
        }
    }
}
