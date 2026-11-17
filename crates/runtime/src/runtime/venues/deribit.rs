//! Deribit venue feed support.
//!
//! Owns the `deribit` connection plans, session loops, wire-message parsing and
//! normalized event dispatch. Shared machinery lives in `runtime.rs` and
//! `runtime/venue_support.rs`.

use super::prelude::*;
use crate::runtime::venue_support::*;

pub(crate) async fn consume_deribit_feed(
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
            async move { consume_deribit_session(feed, shutdown).await }
        },
    )
    .await
    .map(|_| ())
}

async fn consume_deribit_session(
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
    let result = consume_deribit_session_with(feed, shutdown, &mut session).await;
    session.record_result(&result);
    result
}

pub(crate) async fn consume_deribit_session_with<S, E>(
    feed: ExchangeFeed,
    mut shutdown: watch::Receiver<bool>,
    session: &mut Session<S>,
) -> Result<()>
where
    S: Sink<Message, Error = E> + Stream<Item = std::result::Result<Message, E>> + Unpin,
    E: Display,
{
    session.queue_subscriptions(DeribitAdapter::subscription_messages(&feed)?);
    // In-session resnapshot requests for `book` change-id gaps and documented
    // `test_request` liveness replies share the paced outbound queue.
    let mut outbound: Vec<String> = Vec::new();
    while let Some(text) = session.next_text_or_shutdown(&mut shutdown).await? {
        let received_ts = session.received_timestamp();
        session.processing();
        outbound.clear();
        process_deribit_text_message(&feed, &text, received_ts, &mut outbound).await?;
        if !outbound.is_empty() {
            session.queue_subscriptions(std::mem::take(&mut outbound));
        }
    }
    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
async fn dispatch_deribit_event(feed: &ExchangeFeed, event: DeribitEvent) -> Result<()> {
    let _ = feed;
    match event {
        #[cfg(feature = "candles")]
        DeribitEvent::Candle(candle) => dispatch_candle(feed, candle).await,
        #[cfg(feature = "funding")]
        DeribitEvent::Funding(funding) => dispatch_funding(feed, funding).await,
        #[cfg(feature = "index")]
        DeribitEvent::IndexPrice(index_price) => dispatch_index_price(feed, index_price).await,
        #[cfg(feature = "markprice")]
        DeribitEvent::MarkPrice(mark_price) => dispatch_mark_price(feed, mark_price).await,
        #[cfg(feature = "openinterest")]
        DeribitEvent::OpenInterest(open_interest) => {
            dispatch_open_interest(feed, open_interest).await;
        }
        #[cfg(feature = "ticker")]
        DeribitEvent::Ticker(ticker) => dispatch_ticker(feed, ticker).await,
        #[cfg(feature = "trade")]
        DeribitEvent::Trade(trade) => dispatch_trade(feed, trade).await,
    }
    #[cfg(any(
        feature = "ticker",
        feature = "trade",
        feature = "candles",
        feature = "funding",
        feature = "index",
        feature = "markprice",
        feature = "openinterest",
    ))]
    Ok(())
}

#[cfg(feature = "orderbook")]
async fn dispatch_deribit_l2_book(feed: &ExchangeFeed, book: L2Book) -> Result<()> {
    dispatch_l2_book(feed, book).await
}

pub(crate) async fn process_deribit_text_message(
    feed: &ExchangeFeed,
    text: &str,
    received_ts: f64,
    outbound: &mut Vec<String>,
) -> Result<()> {
    let message: Value = serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
    if let Some(control) = DeribitAdapter::parse_control_message(&message) {
        match control? {
            DeribitControl::Subscribed | DeribitControl::Heartbeat => {}
            DeribitControl::TestRequest => {
                // The documented answer to a server test request.
                outbound.push(
                    serde_json::json!({
                        "jsonrpc": "2.0", "id": "cf-test", "method": "public/test"
                    })
                    .to_string(),
                );
            }
        }
        return Ok(());
    }
    #[cfg(feature = "orderbook")]
    if process_deribit_orderbook_message(feed, &message, received_ts, outbound).await? {
        return Ok(());
    }
    if deribit_parser::method(&message) != Some("subscription") {
        // Non-market JSON-RPC traffic (e.g. a heartbeat response filtered by an
        // older session) is not a normalized event.
        return Ok(());
    }
    let Some(channel) = deribit_parser::channel(&message) else {
        // Heartbeat/test_request notifications carry no channel and were
        // classified above.
        return Ok(());
    };
    let events = DeribitAdapter::parse_messages_for_feed(feed, &message, received_ts);
    let configured = deribit_parser::channel_instrument(channel)
        .is_some_and(|native| DeribitAdapter::symbol_for_native(feed, native).is_some());
    let market_channel = matches!(
        deribit_parser::channel_family(channel),
        "ticker" | "trades" | "chart" | "book"
    );
    if configured && market_channel && events.is_empty() {
        return Err(Error::MalformedData(format!(
            "Deribit message for {channel} produced no normalized events"
        )));
    }
    for event in events {
        dispatch_deribit_event(feed, event).await?;
    }
    Ok(())
}

#[cfg(feature = "orderbook")]
async fn process_deribit_orderbook_message(
    feed: &ExchangeFeed,
    message: &Value,
    received_ts: f64,
    outbound: &mut Vec<String>,
) -> Result<bool> {
    let Some(channel) = deribit_parser::channel(message) else {
        return Ok(false);
    };
    if deribit_parser::channel_family(channel) != "book" {
        return Ok(false);
    }
    let Some(native) = deribit_parser::channel_instrument(channel) else {
        return Err(Error::Parse(
            "Deribit book notification has no instrument".to_owned(),
        ));
    };
    let Some(symbol) = DeribitAdapter::symbol_for_native(feed, native).cloned() else {
        return Ok(true);
    };
    if !feed.subscribes(Channel::L2Book, &symbol) {
        return Ok(true);
    }
    let data = deribit_parser::data(message)
        .ok_or_else(|| Error::Parse("Deribit book notification has no data".to_owned()))?;
    let update = deribit_parser::parse_book_for_symbol(data, received_ts, &symbol)
        .ok_or_else(|| Error::Parse("failed to parse Deribit book notification".to_owned()))?;
    let symbol_key = symbol.as_str().to_owned();
    let outcome = {
        let mut syncs = feed
            .deribit_book_syncs()
            .expect("deribit book-sync state")
            .lock()
            .expect("deribit sync lock");
        let sync = syncs
            .entry(symbol_key)
            .or_insert_with(|| DeribitBookSync::new(symbol.clone()));
        sync.apply(update)?
    };
    match outcome {
        DeribitBookOutcome::Publish(book) => dispatch_deribit_l2_book(feed, book).await?,
        DeribitBookOutcome::Stale => {}
        DeribitBookOutcome::Resnapshot => {
            feed.invalidate_book_readiness(symbol.as_str());
            outbound.extend(DeribitAdapter::book_resubscribe_messages(feed, &symbol)?);
        }
    }
    Ok(true)
}
