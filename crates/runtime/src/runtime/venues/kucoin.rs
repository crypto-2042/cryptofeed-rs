//! Kucoin venue feed support.
//!
//! Owns the `kucoin` connection plans, session loops, wire-message parsing and
//! normalized event dispatch. Shared machinery lives in `runtime.rs` and
//! `runtime/venue_support.rs`.

use super::prelude::*;
use crate::runtime::venue_support::*;

pub(crate) async fn consume_kucoin_feed(
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
            async move { consume_kucoin_session(feed, shutdown).await }
        },
    )
    .await
    .map(|_| ())
}

async fn consume_kucoin_session(
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
    let result = consume_kucoin_session_with(feed, shutdown, &mut session).await;
    session.record_result(&result);
    result
}

pub(crate) async fn consume_kucoin_session_with<S, E>(
    feed: ExchangeFeed,
    mut shutdown: watch::Receiver<bool>,
    session: &mut Session<S>,
) -> Result<()>
where
    S: Sink<Message, Error = E> + Stream<Item = std::result::Result<Message, E>> + Unpin,
    E: Display,
{
    session.queue_subscriptions(KucoinAdapter::subscription_messages(&feed)?);

    // In-session resnapshot requests for `obu` gaps: unsubscribe/subscribe so
    // the next push is a documented snapshot.
    let mut outbound: Vec<String> = Vec::new();
    while let Some(text) = session.next_text_or_shutdown(&mut shutdown).await? {
        let received_ts = session.received_timestamp();
        session.processing();
        outbound.clear();
        process_kucoin_text_message(&feed, &text, received_ts, &mut outbound).await?;
        if !outbound.is_empty() {
            session.queue_subscriptions(std::mem::take(&mut outbound));
        }
    }

    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
async fn dispatch_kucoin_event(feed: &ExchangeFeed, event: KucoinEvent) -> Result<()> {
    let _ = feed;
    match event {
        #[cfg(feature = "candles")]
        KucoinEvent::Candle(candle) => dispatch_candle(feed, candle).await,
        #[cfg(feature = "funding")]
        KucoinEvent::Funding(funding) => dispatch_funding(feed, funding).await,
        #[cfg(feature = "index")]
        KucoinEvent::IndexPrice(index_price) => {
            dispatch_index_price(feed, index_price).await;
        }
        #[cfg(feature = "markprice")]
        KucoinEvent::MarkPrice(mark_price) => {
            dispatch_mark_price(feed, mark_price).await;
        }
        #[cfg(feature = "openinterest")]
        KucoinEvent::OpenInterest(open_interest) => {
            dispatch_open_interest(feed, open_interest).await;
        }
        #[cfg(feature = "orderbook")]
        KucoinEvent::L2Book(book) => {
            dispatch_kucoin_l2_book(feed, book).await?;
        }
        #[cfg(feature = "ticker")]
        KucoinEvent::Ticker(ticker) => dispatch_ticker(feed, ticker).await,
        #[cfg(feature = "trade")]
        KucoinEvent::Trade(trade) => dispatch_trade(feed, trade).await,
    }
    #[cfg(any(
        feature = "ticker",
        feature = "trade",
        feature = "orderbook",
        feature = "candles",
        feature = "funding",
        feature = "index",
        feature = "markprice",
        feature = "openinterest",
    ))]
    Ok(())
}

#[cfg(feature = "orderbook")]
async fn dispatch_kucoin_l2_book(feed: &ExchangeFeed, book: L2Book) -> Result<()> {
    dispatch_l2_book(feed, book).await
}

pub(crate) async fn process_kucoin_text_message(
    feed: &ExchangeFeed,
    text: &str,
    received_ts: f64,
    outbound: &mut Vec<String>,
) -> Result<()> {
    let message: Value = serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
    if let Some(control) = KucoinAdapter::parse_control_message(&message) {
        return control.map(|_| ());
    }
    #[cfg(feature = "orderbook")]
    let handled = process_kucoin_orderbook_message(feed, &message, received_ts, outbound).await?;
    #[cfg(not(feature = "orderbook"))]
    let handled = {
        let _ = &outbound;
        false
    };
    if handled {
        return Ok(());
    }
    let events = KucoinAdapter::parse_messages_for_feed(feed, &message, received_ts);
    let native = crate::exchange::kucoin::parser::native_symbol(&message);
    let configured =
        native.is_some_and(|native| KucoinAdapter::symbol_for_native(feed, native).is_some());
    if configured && crate::exchange::kucoin::parser::is_market_frame(&message) && events.is_empty()
    {
        return Err(Error::MalformedData(format!(
            "KuCoin message for {} produced no normalized events",
            message
                .get("T")
                .and_then(Value::as_str)
                .unwrap_or("unknown topic")
        )));
    }
    for event in events {
        dispatch_kucoin_event(feed, event).await?;
    }
    Ok(())
}

#[cfg(feature = "orderbook")]
async fn process_kucoin_orderbook_message(
    feed: &ExchangeFeed,
    message: &Value,
    received_ts: f64,
    outbound: &mut Vec<String>,
) -> Result<bool> {
    let Some(topic) = message.get("T").and_then(Value::as_str) else {
        return Ok(false);
    };
    if !topic.starts_with("obu") {
        return Ok(false);
    }
    let native = kucoin_parser::native_symbol(message)
        .ok_or_else(|| Error::Parse("KuCoin obu message is missing its symbol".to_owned()))?;
    let Some(symbol) = KucoinAdapter::symbol_for_native(feed, native).cloned() else {
        // A multiplexed connection may carry symbols this feed does not
        // subscribe; ignore them instead of failing the session.
        return Ok(true);
    };
    if !feed.subscribes(Channel::L2Book, &symbol) {
        return Ok(true);
    }
    let update = kucoin_parser::parse_book_for_symbol(message, received_ts, &symbol)
        .ok_or_else(|| Error::Parse("failed to parse KuCoin obu update".to_owned()))?;
    let symbol_key = symbol.as_str().to_owned();
    let mode = KucoinBookMode::for_depth(feed.l2_book_depth);
    let outcome = {
        let mut syncs = feed
            .kucoin_book_syncs()
            .expect("kucoin book-sync state")
            .lock()
            .expect("kucoin sync lock");
        let sync = syncs
            .entry(symbol_key)
            .or_insert_with(|| KucoinBookSync::new(symbol.clone(), mode));
        sync.apply(update)?
    };
    match outcome {
        KucoinBookOutcome::Publish(events) => {
            for book in events {
                dispatch_kucoin_l2_book(feed, book).await?;
            }
        }
        KucoinBookOutcome::Ignored => {}
        KucoinBookOutcome::Resnapshot => {
            outbound.extend(KucoinAdapter::book_resubscribe_messages(feed, &symbol)?);
        }
    }
    Ok(true)
}
