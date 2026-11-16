//! Okx venue feed support.
//!
//! Owns the `okx` connection plans, session loops, wire-message parsing and
//! normalized event dispatch. Shared machinery lives in `runtime.rs` and
//! `runtime/venue_support.rs`.

use super::prelude::*;
use crate::runtime::venue_support::*;

pub(crate) async fn consume_okx_feed(
    feed: ExchangeFeed,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut tasks = JoinSet::new();
    for url in OkxAdapter::subscription_urls(&feed) {
        let mut planned_feed = okx_feed_for_url(&feed, &url);
        let books = if planned_feed.channels.contains(&Channel::L2Book) {
            planned_feed
                .symbols
                .iter()
                .filter(|symbol| planned_feed.subscribes(Channel::L2Book, symbol))
                .map(|symbol| symbol.as_str().to_owned())
                .collect()
        } else {
            Vec::new()
        };
        readiness::track(&mut planned_feed, &url, books);
        let retry_shutdown = shutdown.clone();
        tasks.spawn(async move {
            supervisor::retry_with_progress(
                planned_feed.runtime_options.retry_limit(),
                supervisor::Backoff::new(1, 8),
                retry_shutdown.clone(),
                planned_feed.retry_progress.clone(),
                planned_feed.runtime_options.startup_delay(),
                move || {
                    let feed = planned_feed.clone();
                    let url = url.clone();
                    let shutdown = retry_shutdown.clone();
                    async move { consume_okx_session(feed, url, shutdown).await }
                },
            )
            .await
            .map(|_| ())
        });
    }
    collect_connection_results("OKX", tasks).await
}

pub(crate) fn okx_feed_for_url(feed: &ExchangeFeed, url: &str) -> ExchangeFeed {
    let mut planned = feed.clone();
    let business = url.ends_with("/business");
    planned
        .channels
        .retain(|channel| matches!(channel, Channel::Candles) == business);
    planned.prune_connection_scope();
    planned
}

async fn consume_okx_session(
    mut feed: ExchangeFeed,
    websocket_url: String,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let attempt = readiness::begin(&mut feed);
    reset_connection_books(&feed, true, &feed.symbols);
    let url = Url::parse(&websocket_url).map_err(|e| Error::Transport(e.to_string()))?;
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
    let result = consume_okx_session_with(feed, shutdown, &mut session).await;
    session.record_result(&result);
    result
}

pub(crate) async fn consume_okx_session_with<S, E>(
    feed: ExchangeFeed,
    mut shutdown: watch::Receiver<bool>,
    session: &mut Session<S>,
) -> Result<()>
where
    S: Sink<Message, Error = E> + Stream<Item = std::result::Result<Message, E>> + Unpin,
    E: Display,
{
    session.queue_subscriptions([OkxAdapter::subscription_message(&feed)]);

    while let Some(text) = session.next_text_or_shutdown(&mut shutdown).await? {
        let received_ts = session.received_timestamp();
        session.processing();
        process_okx_text_message(&feed, &text, received_ts).await?;
    }

    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) async fn dispatch_okx_event(feed: &ExchangeFeed, event: OkxEvent) -> Result<()> {
    let _ = feed;
    match event {
        #[cfg(feature = "candles")]
        OkxEvent::Candle(candle) => dispatch_candle(feed, candle).await,
        #[cfg(feature = "funding")]
        OkxEvent::Funding(funding) => dispatch_funding(feed, funding).await,
        #[cfg(feature = "liquidations")]
        OkxEvent::Liquidation(liquidation) => dispatch_liquidation(feed, liquidation).await,
        #[cfg(feature = "markprice")]
        OkxEvent::MarkPrice(mark_price) => {
            dispatch_mark_price(feed, mark_price).await;
        }
        #[cfg(feature = "openinterest")]
        OkxEvent::OpenInterest(open_interest) => {
            dispatch_open_interest(feed, open_interest).await;
        }
        #[cfg(feature = "index")]
        OkxEvent::IndexPrice(index_price) => {
            dispatch_index_price(feed, index_price).await;
        }
        #[cfg(feature = "orderbook")]
        OkxEvent::L1Book(book) => dispatch_l1_book(feed, book).await,
        #[cfg(feature = "orderbook")]
        OkxEvent::L2Book(book) => {
            dispatch_okx_l2_book(feed, book).await?;
        }
        #[cfg(feature = "ticker")]
        OkxEvent::Ticker(ticker) => dispatch_ticker(feed, ticker).await,
        #[cfg(feature = "trade")]
        OkxEvent::Trade(trade) => dispatch_trade(feed, trade).await,
    }
    Ok(())
}

#[cfg(feature = "orderbook")]
async fn dispatch_okx_l2_book(feed: &ExchangeFeed, book: L2Book) -> Result<()> {
    dispatch_l2_book(feed, book).await
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) async fn process_okx_text_message(
    feed: &ExchangeFeed,
    text: &str,
    received_ts: f64,
) -> Result<()> {
    if let Some(control) = OkxAdapter::parse_control_text(text) {
        return control.map(|_| ());
    }
    let message: Value = serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
    if let Some(error) = quota::websocket_error(feed.exchange, &message) {
        return Err(error);
    }
    if let Some(control) = OkxAdapter::parse_control_message(&message) {
        return control.map(|_| ());
    }
    #[cfg(feature = "orderbook")]
    let handled = process_okx_orderbook_message(feed, &message, received_ts).await?;
    #[cfg(not(feature = "orderbook"))]
    let handled = false;
    if handled {
        return Ok(());
    }
    for event in OkxAdapter::parse_messages_for_feed(feed, &message, received_ts) {
        dispatch_okx_event(feed, event).await?;
    }
    Ok(())
}

#[cfg(feature = "orderbook")]
pub(crate) async fn process_okx_orderbook_message(
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

    // `bbo-tbt` is the L1 (top-of-book) channel and is routed to
    // `parse_messages` as `L1Book` events, not through the L2 sync path.
    if !matches!(channel, "books" | "books5") {
        return Ok(false);
    }

    let updates = okx_parser::parse_l2_book_updates(message, received_ts);
    if updates.is_empty() {
        return Err(Error::Parse("failed to parse okx depth update".to_owned()));
    }
    for update in updates {
        if !feed.subscribes(Channel::L2Book, update.book.symbol()) {
            continue;
        }
        let symbol_key = match &update.book {
            L2Book::Snapshot(snapshot) => snapshot.symbol.as_str().to_owned(),
            L2Book::Delta(delta) => delta.symbol.as_str().to_owned(),
        };
        let maybe_book = {
            let mut syncs = feed
                .okx_book_syncs()
                .expect("okx book-sync state")
                .lock()
                .expect("okx sync lock");
            let sync = syncs.entry(symbol_key).or_insert_with(|| {
                OkxBookSync::new(match &update.book {
                    L2Book::Snapshot(snapshot) => snapshot.symbol.clone(),
                    L2Book::Delta(delta) => delta.symbol.clone(),
                })
            });
            sync.apply(update)?
        };

        if let Some(book) = maybe_book {
            dispatch_okx_l2_book(feed, book).await?;
        }
    }

    Ok(true)
}
