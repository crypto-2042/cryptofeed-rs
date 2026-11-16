//! Upbit venue feed support.
//!
//! Owns the `upbit` connection plans, session loops, wire-message parsing and
//! normalized event dispatch. Shared machinery lives in `runtime.rs` and
//! `runtime/venue_support.rs`.

use super::prelude::*;
use crate::runtime::venue_support::*;

pub(crate) async fn consume_upbit_feed(
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
            async move { consume_upbit_session(feed, shutdown).await }
        },
    )
    .await
    .map(|_| ())
}

async fn consume_upbit_session(
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
    let result = consume_upbit_session_with(feed, shutdown, &mut session).await;
    session.record_result(&result);
    result
}

pub(crate) async fn consume_upbit_session_with<S, E>(
    feed: ExchangeFeed,
    mut shutdown: watch::Receiver<bool>,
    session: &mut Session<S>,
) -> Result<()>
where
    S: Sink<Message, Error = E> + Stream<Item = std::result::Result<Message, E>> + Unpin,
    E: Display,
{
    session.queue_subscriptions(UpbitAdapter::subscription_messages(&feed)?);
    // Snapshot-only books need no in-session resnapshot request: every push
    // replaces the book and no sequence/gap contract exists.
    while let Some(text) = session.next_text_or_shutdown(&mut shutdown).await? {
        let received_ts = session.received_timestamp();
        session.processing();
        process_upbit_text_message(&feed, &text, received_ts).await?;
    }

    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
async fn dispatch_upbit_event(feed: &ExchangeFeed, event: UpbitEvent) -> Result<()> {
    let _ = feed;
    match event {
        #[cfg(feature = "candles")]
        UpbitEvent::Candle(candle) => dispatch_candle(feed, candle).await,
        #[cfg(feature = "trade")]
        UpbitEvent::Trade(trade) => dispatch_trade(feed, trade).await,
    }
    Ok(())
}

pub(crate) async fn process_upbit_text_message(
    feed: &ExchangeFeed,
    text: &str,
    received_ts: f64,
) -> Result<()> {
    let message: Value = serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
    if UpbitAdapter::is_heartbeat_status(&message) {
        return Ok(());
    }
    if let Some(error) = UpbitAdapter::error(&message) {
        return Err(error);
    }
    #[cfg(feature = "orderbook")]
    let handled = process_upbit_orderbook_message(feed, &message, received_ts).await?;
    #[cfg(not(feature = "orderbook"))]
    let handled = false;
    if handled {
        return Ok(());
    }
    let events = UpbitAdapter::parse_messages_for_feed(feed, &message, received_ts);
    let data_type = upbit_parser::message_type(&message);
    let configured = upbit_parser::native_code(&message)
        .is_some_and(|native| UpbitAdapter::symbol_for_native(feed, native).is_some());
    if configured && data_type.is_some_and(is_upbit_market_type) && events.is_empty() {
        return Err(Error::MalformedData(format!(
            "Upbit message for {} produced no normalized events",
            data_type.unwrap_or("unknown type")
        )));
    }
    for event in events {
        dispatch_upbit_event(feed, event).await?;
    }
    Ok(())
}

fn is_upbit_market_type(data_type: &str) -> bool {
    matches!(data_type, "trade" | "orderbook") || data_type.starts_with("candle.")
}

#[cfg(feature = "orderbook")]
async fn process_upbit_orderbook_message(
    feed: &ExchangeFeed,
    message: &Value,
    received_ts: f64,
) -> Result<bool> {
    if upbit_parser::message_type(message) != Some("orderbook") {
        return Ok(false);
    }
    let native = upbit_parser::native_code(message)
        .ok_or_else(|| Error::Parse("Upbit orderbook message is missing its code".to_owned()))?;
    let Some(symbol) = UpbitAdapter::symbol_for_native(feed, native).cloned() else {
        // A connection may carry codes this feed does not subscribe.
        return Ok(true);
    };
    if !feed.subscribes(Channel::L2Book, &symbol) {
        return Ok(true);
    }
    let book = upbit_parser::parse_book_for_symbol(message, received_ts, &symbol)
        .ok_or_else(|| Error::Parse("failed to parse Upbit orderbook snapshot".to_owned()))?;
    let units = UpbitAdapter::l2_book_depth_wire(feed.l2_book_depth).ok_or_else(|| {
        Error::UnsupportedCapability(format!("Upbit l2 depth level {:?}", feed.l2_book_depth))
    })?;
    let symbol_key = symbol.as_str().to_owned();
    let event = {
        let mut syncs = feed
            .upbit_book_syncs()
            .expect("upbit book-sync state")
            .lock()
            .expect("upbit sync lock");
        let sync = syncs
            .entry(symbol_key)
            .or_insert_with(|| UpbitBookSync::new(symbol, Some(units)));
        sync.apply(book)?
    };
    dispatch_l2_book(feed, event).await?;
    Ok(true)
}
