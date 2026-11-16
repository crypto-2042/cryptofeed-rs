//! Bitget venue feed support.
//!
//! Owns the `bitget` connection plans, session loops, wire-message parsing and
//! normalized event dispatch. Shared machinery lives in `runtime.rs` and
//! `runtime/venue_support.rs`.

use super::prelude::*;
use crate::runtime::venue_support::*;

pub(crate) async fn consume_bitget_feed(
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
            async move { consume_bitget_session(feed, shutdown).await }
        },
    )
    .await
    .map(|_| ())
}

async fn consume_bitget_session(
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
    let result = consume_bitget_session_with(feed, shutdown, &mut session).await;
    session.record_result(&result);
    result
}

pub(crate) async fn consume_bitget_session_with<S, E>(
    feed: ExchangeFeed,
    mut shutdown: watch::Receiver<bool>,
    session: &mut Session<S>,
) -> Result<()>
where
    S: Sink<Message, Error = E> + Stream<Item = std::result::Result<Message, E>> + Unpin,
    E: Display,
{
    session.queue_subscriptions([BitgetAdapter::subscription_message(&feed)]);

    while let Some(text) = session.next_text_or_shutdown(&mut shutdown).await? {
        let received_ts = session.received_timestamp();
        session.processing();
        if let Err(error) = process_bitget_text_message(&feed, &text, received_ts).await {
            let diagnostic = crate::feed::FailureDiagnostic::from(&error);
            let value = serde_json::from_str::<Value>(&text).ok();
            let channel = match value
                .as_ref()
                .and_then(|v| v.pointer("/arg/topic"))
                .and_then(Value::as_str)
            {
                Some("books") => "books",
                Some("books1") => "books1",
                Some("books5") => "books5",
                Some("books50") => "books50",
                Some("ticker") => "ticker",
                Some("trade" | "publicTrade") => "publicTrade",
                Some("kline") => "kline",
                Some("liquidation") => "liquidation",
                _ => "unknown",
            };
            tracing::warn!(exchange="Bitget", feed_id=feed.identity.map(|i| i.id.as_u64()),
                generation=feed.identity.map(|i| i.generation), product=?feed.symbols.first().map(|s| s.kind()),
                channel, category=diagnostic.category, reason=diagnostic.reason, permanent=diagnostic.permanent,
                stage="message_processing", "Bitget message processing failed");
            return Err(error);
        }
    }

    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) async fn dispatch_bitget_event(feed: &ExchangeFeed, event: BitgetEvent) -> Result<()> {
    let _ = feed;
    match event {
        #[cfg(feature = "candles")]
        BitgetEvent::Candle(candle) => dispatch_candle(feed, candle).await,
        #[cfg(feature = "funding")]
        BitgetEvent::Funding(funding) => dispatch_funding(feed, funding).await,
        #[cfg(feature = "index")]
        BitgetEvent::IndexPrice(index) => dispatch_index_price(feed, index).await,
        #[cfg(feature = "markprice")]
        BitgetEvent::MarkPrice(mark) => dispatch_mark_price(feed, mark).await,
        #[cfg(feature = "openinterest")]
        BitgetEvent::OpenInterest(oi) => dispatch_open_interest(feed, oi).await,
        #[cfg(feature = "orderbook")]
        BitgetEvent::L1Book(book) => dispatch_l1_book(feed, book).await,
        #[cfg(feature = "liquidations")]
        BitgetEvent::Liquidation(liquidation) => dispatch_liquidation(feed, liquidation).await,
        #[cfg(feature = "orderbook")]
        BitgetEvent::L2Book(book) => {
            dispatch_bitget_l2_book(feed, book).await?;
        }
        #[cfg(feature = "ticker")]
        BitgetEvent::Ticker(ticker) => dispatch_ticker(feed, ticker).await,
        #[cfg(feature = "trade")]
        BitgetEvent::Trade(trade) => dispatch_trade(feed, trade).await,
    }
    Ok(())
}

#[cfg(feature = "orderbook")]
async fn dispatch_bitget_l2_book(feed: &ExchangeFeed, book: L2Book) -> Result<()> {
    dispatch_l2_book(feed, book).await
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) async fn process_bitget_text_message(
    feed: &ExchangeFeed,
    text: &str,
    received_ts: f64,
) -> Result<()> {
    let message: Value = serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
    if let Some(error) = quota::websocket_error(feed.exchange, &message) {
        return Err(error);
    }
    if message.get("event").and_then(Value::as_str) == Some("subscribe") {
        return Ok(());
    }
    if message.get("event").and_then(Value::as_str) == Some("error") {
        let detail = message
            .get("msg")
            .or_else(|| message.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("subscription rejected");
        // A rejected subscription is a permanent configuration problem;
        // `Subscription` errors fail fast instead of burning the retries.
        return Err(Error::Subscription(format!(
            "bitget websocket error: {detail}"
        )));
    }
    #[cfg(feature = "orderbook")]
    let handled = process_bitget_orderbook_message(feed, &message, received_ts).await?;
    #[cfg(not(feature = "orderbook"))]
    let handled = false;
    if handled {
        return Ok(());
    }
    let events = BitgetAdapter::parse_messages_for_feed(feed, &message, received_ts);
    let bitget_topic = message
        .get("arg")
        .and_then(|arg| arg.get("topic").or_else(|| arg.get("channel")))
        .and_then(Value::as_str);
    if bitget_topic == Some("liquidation")
        && data_is_non_empty(&message)
        && events.is_empty()
        && BitgetAdapter::parse_messages(&message, received_ts).is_empty()
    {
        return Err(Error::MalformedData(
            "Bitget liquidation message matched the subscribed topic but produced no events"
                .to_owned(),
        ));
    }
    for event in events {
        dispatch_bitget_event(feed, event).await?;
    }
    Ok(())
}

#[cfg(feature = "orderbook")]
pub(crate) async fn process_bitget_orderbook_message(
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

    if topic == "books1" && feed.channels.contains(&Channel::L1Book) {
        for event in BitgetAdapter::parse_messages_for_feed(feed, message, received_ts) {
            if let BitgetEvent::L1Book(book) = event {
                dispatch_l1_book(feed, book).await;
            }
        }
    }
    let l2_topic = match feed.l2_book_depth {
        Some(1) => "books1",
        Some(5) => "books5",
        Some(50) => "books50",
        _ => "books",
    };
    if !feed.channels.contains(&Channel::L2Book) || topic != l2_topic {
        return Ok(true);
    }

    let rows = message
        .get("data")
        .and_then(|v| v.as_array())
        .ok_or_else(|| Error::Parse("missing bitget depth payload".to_owned()))?;
    if rows.is_empty() {
        return Err(Error::Parse("missing bitget depth payload".to_owned()));
    }
    for row in rows {
        let mut single = message.clone();
        single["data"] = Value::Array(vec![row.clone()]);
        let book = BitgetAdapter::parse_messages_for_feed(feed, &single, received_ts)
            .into_iter()
            .find_map(|event| match event {
                BitgetEvent::L2Book(book) => Some(book),
                _ => None,
            })
            .ok_or_else(|| Error::Parse("failed to parse bitget depth update".to_owned()))?;
        if !feed.subscribes(Channel::L2Book, book.symbol()) {
            continue;
        }
        let parse_sequence = |name: &str| {
            row.get(name)
                .and_then(|value| {
                    value
                        .as_u64()
                        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
                })
                .unwrap_or(0)
        };
        let update = BitgetDepthUpdate {
            action: match message.get("action").and_then(Value::as_str) {
                Some("snapshot") => BitgetBookAction::Snapshot,
                _ => BitgetBookAction::Update,
            },
            seq: parse_sequence("seq"),
            pseq: parse_sequence("pseq"),
            book,
        };
        let symbol_key = match &update.book {
            L2Book::Snapshot(snapshot) => snapshot.symbol.as_str().to_owned(),
            L2Book::Delta(delta) => delta.symbol.as_str().to_owned(),
        };

        let maybe_book = {
            let mut syncs = feed
                .bitget_book_syncs()
                .expect("bitget book-sync state")
                .lock()
                .expect("bitget sync lock");
            let sync = syncs.entry(symbol_key).or_insert_with(|| {
                BitgetBookSync::new(match &update.book {
                    L2Book::Snapshot(snapshot) => snapshot.symbol.clone(),
                    L2Book::Delta(delta) => delta.symbol.clone(),
                })
            });
            sync.apply(update)?
        };

        if let Some(book) = maybe_book {
            dispatch_bitget_l2_book(feed, book).await?;
        }
    }

    Ok(true)
}
