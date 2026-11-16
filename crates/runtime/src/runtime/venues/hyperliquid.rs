//! Hyperliquid venue feed support.
//!
//! Owns the `hyperliquid` connection plans, session loops, wire-message parsing and
//! normalized event dispatch. Shared machinery lives in `runtime.rs` and
//! `runtime/venue_support.rs`.

use super::prelude::*;
use crate::runtime::venue_support::*;

pub(crate) async fn consume_hyperliquid_feed(
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
            async move { consume_hyperliquid_session(feed, shutdown).await }
        },
    )
    .await
    .map(|_| ())
}

async fn consume_hyperliquid_session(
    mut feed: ExchangeFeed,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let attempt = readiness::begin(&mut feed);
    // `l2Book` is snapshot-only, so every connection starts from a fresh local
    // book and the next push is a complete replacement.
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
    let result = consume_hyperliquid_session_with(feed, shutdown, &mut session).await;
    session.record_result(&result);
    result
}

pub(crate) async fn consume_hyperliquid_session_with<S, E>(
    feed: ExchangeFeed,
    mut shutdown: watch::Receiver<bool>,
    session: &mut Session<S>,
) -> Result<()>
where
    S: Sink<Message, Error = E> + Stream<Item = std::result::Result<Message, E>> + Unpin,
    E: Display,
{
    session.queue_subscriptions(HyperliquidAdapter::subscription_messages(&feed)?);
    // The documented client heartbeat is sent by the shared session policy;
    // there is no in-session snapshot request or resubscription contract, so a
    // reconnect is the only recovery.
    while let Some(text) = session.next_text_or_shutdown(&mut shutdown).await? {
        let received_ts = session.received_timestamp();
        session.processing();
        process_hyperliquid_text_message(&feed, &text, received_ts).await?;
    }
    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
async fn dispatch_hyperliquid_event(feed: &ExchangeFeed, event: HyperliquidEvent) -> Result<()> {
    let _ = feed;
    match event {
        #[cfg(feature = "candles")]
        HyperliquidEvent::Candle(candle) => dispatch_candle(feed, candle).await,
        #[cfg(feature = "funding")]
        HyperliquidEvent::Funding(funding) => dispatch_funding(feed, funding).await,
        #[cfg(feature = "index")]
        HyperliquidEvent::IndexPrice(index_price) => dispatch_index_price(feed, index_price).await,
        #[cfg(feature = "orderbook")]
        HyperliquidEvent::L1Book(book) => dispatch_l1_book(feed, book).await,
        #[cfg(feature = "markprice")]
        HyperliquidEvent::MarkPrice(mark_price) => dispatch_mark_price(feed, mark_price).await,
        #[cfg(feature = "openinterest")]
        HyperliquidEvent::OpenInterest(open_interest) => {
            dispatch_open_interest(feed, open_interest).await;
        }
        #[cfg(feature = "ticker")]
        HyperliquidEvent::Ticker(ticker) => dispatch_ticker(feed, ticker).await,
        #[cfg(feature = "trade")]
        HyperliquidEvent::Trade(trade) => dispatch_trade(feed, trade).await,
    }
    Ok(())
}

#[cfg(feature = "orderbook")]
async fn dispatch_hyperliquid_l2_book(feed: &ExchangeFeed, book: L2Book) -> Result<()> {
    dispatch_l2_book(feed, book).await
}

async fn process_hyperliquid_text_message(
    feed: &ExchangeFeed,
    text: &str,
    received_ts: f64,
) -> Result<()> {
    let message: Value = serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
    if let Some(control) = HyperliquidAdapter::parse_control_message(&message) {
        match control? {
            HyperliquidControl::Ack | HyperliquidControl::Pong => {}
        }
        return Ok(());
    }
    #[cfg(feature = "orderbook")]
    if process_hyperliquid_orderbook_message(feed, &message, received_ts).await? {
        return Ok(());
    }
    for event in HyperliquidAdapter::parse_messages_for_feed(feed, &message, received_ts)? {
        dispatch_hyperliquid_event(feed, event).await?;
    }
    Ok(())
}

#[cfg(feature = "orderbook")]
async fn process_hyperliquid_orderbook_message(
    feed: &ExchangeFeed,
    message: &Value,
    received_ts: f64,
) -> Result<bool> {
    let Some(channel) = hyperliquid_parser::channel(message) else {
        return Ok(false);
    };
    if channel != "l2Book" {
        return Ok(false);
    }
    let data = hyperliquid_parser::data(message)
        .ok_or_else(|| Error::Parse("Hyperliquid l2Book frame has no payload".to_owned()))?;
    let coin = hyperliquid_parser::data_coin(data)
        .ok_or_else(|| Error::Parse("Hyperliquid l2Book frame has no coin".to_owned()))?;
    let Some(symbol) = HyperliquidAdapter::symbol_for_native(feed, coin).cloned() else {
        return Ok(true);
    };
    if !feed.subscribes(Channel::L2Book, &symbol) {
        return Ok(true);
    }
    let update = hyperliquid_parser::parse_book(data, received_ts, &symbol)
        .ok_or_else(|| Error::Parse("failed to parse Hyperliquid l2Book".to_owned()))?;
    let symbol_key = symbol.as_str().to_owned();
    let outcome = {
        let mut syncs = feed
            .hyperliquid_book_syncs()
            .expect("hyperliquid book-sync state")
            .lock()
            .expect("hyperliquid sync lock");
        let sync = syncs
            .entry(symbol_key)
            .or_insert_with(|| HyperliquidBookSync::new(symbol.clone()));
        sync.apply(update)?
    };
    match outcome {
        HyperliquidBookOutcome::Publish(book) => dispatch_hyperliquid_l2_book(feed, book).await?,
    }
    Ok(true)
}
