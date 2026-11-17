//! Kraken venue feed support.
//!
//! Owns the `kraken` connection plans, session loops, wire-message parsing and
//! normalized event dispatch. Shared machinery lives in `runtime.rs` and
//! `runtime/venue_support.rs`.

use super::prelude::*;
use crate::runtime::venue_support::*;

pub(crate) async fn consume_kraken_feed(
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
            async move { consume_kraken_session(feed, shutdown).await }
        },
    )
    .await
    .map(|_| ())
}

async fn consume_kraken_session(
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
    let result = consume_kraken_session_with(feed, shutdown, &mut session).await;
    session.record_result(&result);
    result
}

pub(crate) async fn consume_kraken_session_with<S, E>(
    feed: ExchangeFeed,
    mut shutdown: watch::Receiver<bool>,
    session: &mut Session<S>,
) -> Result<()>
where
    S: Sink<Message, Error = E> + Stream<Item = std::result::Result<Message, E>> + Unpin,
    E: Display,
{
    session.queue_subscriptions(KrakenAdapter::subscription_messages(&feed)?);

    // In-session resnapshot requests for book checksum mismatches.
    let mut outbound: Vec<String> = Vec::new();
    while let Some(text) = session.next_text_or_shutdown(&mut shutdown).await? {
        let received_ts = session.received_timestamp();
        session.processing();
        outbound.clear();
        process_kraken_text_message(&feed, &text, received_ts, &mut outbound).await?;
        if !outbound.is_empty() {
            session.queue_subscriptions(std::mem::take(&mut outbound));
        }
    }

    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
async fn dispatch_kraken_event(feed: &ExchangeFeed, event: KrakenEvent) -> Result<()> {
    let _ = feed;
    match event {
        #[cfg(feature = "candles")]
        KrakenEvent::Candle(candle) => dispatch_candle(feed, candle).await,
        #[cfg(feature = "ticker")]
        KrakenEvent::Ticker(ticker) => dispatch_ticker(feed, ticker).await,
        #[cfg(feature = "trade")]
        KrakenEvent::Trade(trade) => dispatch_trade(feed, trade).await,
    }
    #[cfg(any(feature = "ticker", feature = "trade", feature = "candles",))]
    Ok(())
}

#[cfg(feature = "orderbook")]
async fn dispatch_kraken_l2_book(feed: &ExchangeFeed, book: L2Book) -> Result<()> {
    dispatch_l2_book(feed, book).await
}

pub(crate) async fn process_kraken_text_message(
    feed: &ExchangeFeed,
    text: &str,
    received_ts: f64,
    outbound: &mut Vec<String>,
) -> Result<()> {
    let message: Value = serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
    if let Some(control) = KrakenAdapter::parse_control_message(&message) {
        return control.map(|_| ());
    }
    #[cfg(feature = "orderbook")]
    let handled = process_kraken_orderbook_message(feed, &message, received_ts, outbound).await?;
    #[cfg(not(feature = "orderbook"))]
    let handled = {
        let _ = &outbound;
        false
    };
    if handled {
        return Ok(());
    }
    let events = KrakenAdapter::parse_messages_for_feed(feed, &message, received_ts);
    let channel = crate::exchange::kraken::parser::channel(&message);
    let configured_row = crate::exchange::kraken::parser::data_rows(&message)
        .iter()
        .any(|row| {
            crate::exchange::kraken::parser::row_symbol(row)
                .is_some_and(|native| KrakenAdapter::symbol_for_native(feed, native).is_some())
        });
    if channel.is_some() && configured_row && events.is_empty() {
        return Err(Error::MalformedData(format!(
            "Kraken message for {} produced no normalized events",
            channel.unwrap_or("unknown channel")
        )));
    }
    for event in events {
        dispatch_kraken_event(feed, event).await?;
    }
    Ok(())
}

#[cfg(feature = "orderbook")]
async fn process_kraken_orderbook_message(
    feed: &ExchangeFeed,
    message: &Value,
    received_ts: f64,
    outbound: &mut Vec<String>,
) -> Result<bool> {
    if kraken_parser::channel(message) != Some("book") {
        return Ok(false);
    }
    let snapshot = match kraken_parser::message_type(message) {
        Some("snapshot") => true,
        Some("update") => false,
        _ => {
            return Err(Error::Parse(
                "Kraken book message is missing its snapshot/update type".to_owned(),
            ));
        }
    };
    let depth = KrakenAdapter::l2_book_depth_wire(feed.l2_book_depth).ok_or_else(|| {
        Error::UnsupportedCapability(format!("Kraken l2 depth level {:?}", feed.l2_book_depth))
    })?;
    for row in kraken_parser::data_rows(message) {
        let native = kraken_parser::row_symbol(row)
            .ok_or_else(|| Error::Parse("Kraken book row is missing its symbol".to_owned()))?;
        let Some(symbol) = KrakenAdapter::symbol_for_native(feed, native).cloned() else {
            continue;
        };
        if !feed.subscribes(Channel::L2Book, &symbol) {
            continue;
        }
        let update = kraken_parser::parse_book_row(row, snapshot, received_ts, &symbol)
            .ok_or_else(|| Error::Parse("failed to parse Kraken book row".to_owned()))?;
        let symbol_key = symbol.as_str().to_owned();
        let outcome = {
            let mut syncs = feed
                .kraken_book_syncs()
                .expect("kraken book-sync state")
                .lock()
                .expect("kraken sync lock");
            let sync = syncs
                .entry(symbol_key)
                .or_insert_with(|| KrakenBookSync::new(symbol.clone(), depth));
            sync.apply(update)?
        };
        match outcome {
            KrakenBookOutcome::Publish(book) => dispatch_kraken_l2_book(feed, book).await?,
            KrakenBookOutcome::Ignored => {}
            KrakenBookOutcome::Resnapshot => {
                feed.invalidate_book_readiness(symbol.as_str());
                outbound.extend(KrakenAdapter::book_resubscribe_messages(feed, &symbol)?);
            }
        }
    }
    Ok(true)
}
