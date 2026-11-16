//! Coinbase venue feed support.
//!
//! Owns the `coinbase` connection plans, session loops, wire-message parsing and
//! normalized event dispatch. Shared machinery lives in `runtime.rs` and
//! `runtime/venue_support.rs`.

use super::prelude::*;
use crate::runtime::venue_support::*;

pub(crate) async fn consume_coinbase_feed(
    mut feed: ExchangeFeed,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let url = planned_url(&feed);
    let books = if feed.channels.contains(&Channel::L2Book) {
        feed.symbols
            .iter()
            .filter(|symbol| feed.subscribes(Channel::L2Book, symbol))
            .map(|symbol| symbol.as_str().to_owned())
            .collect()
    } else {
        Vec::new()
    };
    readiness::track(&mut feed, &url, books);
    supervisor::retry_with_progress(
        feed.runtime_options.retry_limit(),
        supervisor::Backoff::new(1, 8),
        shutdown.clone(),
        feed.retry_progress.clone(),
        feed.runtime_options.startup_delay(),
        move || {
            let feed = feed.clone();
            let shutdown = shutdown.clone();
            async move { consume_coinbase_session(feed, shutdown).await }
        },
    )
    .await
    .map(|_| ())
}

async fn consume_coinbase_session(
    mut feed: ExchangeFeed,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let attempt = readiness::begin(&mut feed);
    reset_connection_books(&feed, true, &feed.symbols);
    let url = Url::parse(&planned_url(&feed)).map_err(|e| Error::Transport(e.to_string()))?;
    let connection = connection::WsConnection::new(url, feed.exchange)
        .runtime_options(feed.runtime_options)
        .transport(feed.transport.clone())
        .capture_feed(&feed);
    let mut session = match connection.connect().await {
        Ok(session) => session,
        Err(error) => {
            if let Some(attempt) = &attempt {
                attempt.failed(&error, "connect");
            }
            return Err(error);
        }
    };
    #[cfg(feature = "recording")]
    {
        feed.raw_session = session.raw_link();
    }
    session.set_readiness(attempt);
    session.set_retry_progress(feed.retry_progress.clone());
    let result = consume_coinbase_session_with(feed, shutdown, &mut session).await;
    session.record_result(&result);
    result
}

pub(crate) async fn consume_coinbase_session_with<S, E>(
    feed: ExchangeFeed,
    mut shutdown: watch::Receiver<bool>,
    session: &mut Session<S>,
) -> Result<()>
where
    S: Sink<Message, Error = E> + Stream<Item = std::result::Result<Message, E>> + Unpin,
    E: Display,
{
    session.queue_subscriptions(CoinbaseAdapter::subscription_messages(&feed)?);
    // The documented level2 stream has no in-session snapshot request; a
    // reconnect is the documented recovery and clears this feed's book state.
    while let Some(text) = session.next_text_or_shutdown(&mut shutdown).await? {
        let received_ts = session.received_timestamp();
        session.processing();
        process_coinbase_text_message(&feed, &text, received_ts).await?;
    }
    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
async fn dispatch_coinbase_event(feed: &ExchangeFeed, event: CoinbaseEvent) -> Result<()> {
    let _ = feed;
    match event {
        #[cfg(feature = "candles")]
        CoinbaseEvent::Candle(candle) => dispatch_candle(feed, candle).await,
        #[cfg(feature = "orderbook")]
        CoinbaseEvent::L1Book(book) => dispatch_l1_book(feed, book).await,
        #[cfg(feature = "ticker")]
        CoinbaseEvent::Ticker(ticker) => dispatch_ticker(feed, ticker).await,
        #[cfg(feature = "trade")]
        CoinbaseEvent::Trade(trade) => dispatch_trade(feed, trade).await,
    }
    #[cfg(any(
        feature = "ticker",
        feature = "trade",
        feature = "orderbook",
        feature = "candles",
    ))]
    Ok(())
}

pub(crate) async fn process_coinbase_text_message(
    feed: &ExchangeFeed,
    text: &str,
    received_ts: f64,
) -> Result<()> {
    let message: Value = serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
    if let Some(control) = CoinbaseAdapter::parse_control_message(&message) {
        control?;
        return Ok(());
    }
    #[cfg(feature = "orderbook")]
    if process_coinbase_orderbook_message(feed, &message, received_ts).await? {
        return Ok(());
    }
    let events = CoinbaseAdapter::parse_messages_for_feed(feed, &message, received_ts);
    let channel = coinbase_parser::channel(&message);
    let configured = coinbase_parser::event_rows(&message).iter().any(|event| {
        coinbase_parser::event_product(event)
            .is_some_and(|native| CoinbaseAdapter::symbol_for_native(feed, native).is_some())
    });
    let market_channel = matches!(
        channel,
        Some("ticker" | "market_trades" | "candles" | "l2_data")
    );
    if configured && channel.is_some() && market_channel && events.is_empty() {
        return Err(Error::MalformedData(format!(
            "Coinbase message for {} produced no normalized events",
            channel.unwrap_or("unknown channel")
        )));
    }
    for event in events {
        dispatch_coinbase_event(feed, event).await?;
    }
    Ok(())
}

#[cfg(feature = "orderbook")]
async fn process_coinbase_orderbook_message(
    feed: &ExchangeFeed,
    message: &Value,
    received_ts: f64,
) -> Result<bool> {
    if coinbase_parser::channel(message) != Some("l2_data") {
        return Ok(false);
    }
    let exchange_ts = coinbase_parser::message_timestamp(message, received_ts);
    let events = coinbase_parser::event_rows(message);
    if events.is_empty() {
        return Err(Error::Parse(
            "Coinbase l2_data message has no events".to_owned(),
        ));
    }
    for event in events {
        let native = coinbase_parser::event_product(event).ok_or_else(|| {
            Error::Parse("Coinbase l2_data event is missing product_id".to_owned())
        })?;
        let Some(symbol) = CoinbaseAdapter::symbol_for_native(feed, native).cloned() else {
            // A connection may carry products this feed does not subscribe.
            continue;
        };
        if !feed.subscribes(Channel::L2Book, &symbol) {
            continue;
        }
        let update = coinbase_parser::parse_book_event(event, exchange_ts, received_ts, &symbol)
            .ok_or_else(|| Error::Parse("failed to parse Coinbase level2 event".to_owned()))?;
        let symbol_key = symbol.as_str().to_owned();
        let outcome = {
            let mut syncs = feed
                .coinbase_book_syncs()
                .expect("coinbase book-sync state")
                .lock()
                .expect("coinbase sync lock");
            let sync = syncs
                .entry(symbol_key)
                .or_insert_with(|| CoinbaseBookSync::new(symbol));
            sync.apply(update)?
        };
        match outcome {
            CoinbaseBookOutcome::Publish(book) => dispatch_l2_book(feed, book).await?,
        }
    }
    Ok(true)
}
