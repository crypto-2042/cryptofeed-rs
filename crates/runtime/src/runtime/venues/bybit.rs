//! Bybit venue feed support.
//!
//! Owns the `bybit` connection plans, session loops, wire-message parsing and
//! normalized event dispatch. Shared machinery lives in `runtime.rs` and
//! `runtime/venue_support.rs`.

use super::prelude::*;
use crate::runtime::venue_support::*;

pub(crate) async fn consume_bybit_feed(
    feed: ExchangeFeed,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut tasks = JoinSet::new();
    for url in BybitAdapter::subscription_urls(&feed) {
        let mut planned_feed = bybit_feed_for_url(&feed, &url);
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
                    async move { consume_bybit_session(feed, url, shutdown).await }
                },
            )
            .await
            .map(|_| ())
        });
    }
    collect_connection_results("Bybit", tasks).await
}

pub(crate) fn bybit_feed_for_url(feed: &ExchangeFeed, url: &str) -> ExchangeFeed {
    let product = if url.ends_with("/linear") {
        BybitProduct::Linear
    } else if url.ends_with("/inverse") {
        BybitProduct::Inverse
    } else if url.ends_with("/option") {
        BybitProduct::Option
    } else {
        BybitProduct::Spot
    };
    let mut planned = feed.clone();
    let symbols = std::mem::take(&mut planned.symbols);
    let exchange_symbols = std::mem::take(&mut planned.exchange_symbols);
    for (index, symbol) in symbols.into_iter().enumerate() {
        if BybitAdapter::product_for_symbol(&symbol) == product {
            planned.symbols.push(symbol);
            if let Some(exchange_symbol) = exchange_symbols.get(index) {
                planned.exchange_symbols.push(exchange_symbol.clone());
            }
        }
    }
    planned.prune_connection_scope();
    planned
}

async fn consume_bybit_session(
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
    let result = consume_bybit_session_with(feed, shutdown, &mut session).await;
    session.record_result(&result);
    result
}

pub(crate) async fn consume_bybit_session_with<S, E>(
    feed: ExchangeFeed,
    mut shutdown: watch::Receiver<bool>,
    session: &mut Session<S>,
) -> Result<()>
where
    S: Sink<Message, Error = E> + Stream<Item = std::result::Result<Message, E>> + Unpin,
    E: Display,
{
    session.queue_subscriptions(BybitAdapter::subscription_messages(&feed)?);

    // Ticker deltas carry only changed fields. State is per connection, so a
    // reconnect cannot reuse values from a previous session.
    let mut tickers = std::collections::HashMap::new();
    while let Some(text) = session.next_text_or_shutdown(&mut shutdown).await? {
        let received_ts = session.received_timestamp();
        session.processing();
        let mut message: Value =
            serde_json::from_str(&text).map_err(|e| Error::Parse(e.to_string()))?;
        merge_bybit_ticker_message(&feed, &mut tickers, &mut message)?;
        process_bybit_message(&feed, &message, received_ts).await?;
    }

    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) async fn dispatch_bybit_event(feed: &ExchangeFeed, event: BybitEvent) -> Result<()> {
    let _ = feed;
    match event {
        #[cfg(feature = "candles")]
        BybitEvent::Candle(candle) => dispatch_candle(feed, candle).await,
        #[cfg(feature = "funding")]
        BybitEvent::Funding(funding) => dispatch_funding(feed, funding).await,
        #[cfg(feature = "index")]
        BybitEvent::IndexPrice(index_price) => {
            dispatch_index_price(feed, index_price).await;
        }
        #[cfg(feature = "liquidations")]
        BybitEvent::Liquidation(liquidation) => dispatch_liquidation(feed, liquidation).await,
        #[cfg(feature = "markprice")]
        BybitEvent::MarkPrice(mark_price) => {
            dispatch_mark_price(feed, mark_price).await;
        }
        #[cfg(feature = "openinterest")]
        BybitEvent::OpenInterest(open_interest) => {
            dispatch_open_interest(feed, open_interest).await;
        }
        #[cfg(feature = "orderbook")]
        BybitEvent::L1Book(book) => dispatch_l1_book(feed, book).await,
        #[cfg(feature = "orderbook")]
        BybitEvent::L2Book(book) => {
            dispatch_bybit_l2_book(feed, book).await?;
        }
        #[cfg(feature = "ticker")]
        BybitEvent::Ticker(ticker) => dispatch_ticker(feed, ticker).await,
        #[cfg(feature = "trade")]
        BybitEvent::Trade(trade) => dispatch_trade(feed, trade).await,
    }
    Ok(())
}

#[cfg(feature = "orderbook")]
async fn dispatch_bybit_l2_book(feed: &ExchangeFeed, book: L2Book) -> Result<()> {
    dispatch_l2_book(feed, book).await
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) async fn process_bybit_text_message(
    feed: &ExchangeFeed,
    text: &str,
    received_ts: f64,
) -> Result<()> {
    let message: Value = serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
    process_bybit_message(feed, &message, received_ts).await
}

pub(crate) fn merge_bybit_ticker_message(
    feed: &ExchangeFeed,
    tickers: &mut std::collections::HashMap<String, serde_json::Map<String, Value>>,
    message: &mut Value,
) -> Result<()> {
    if !feed.symbols.iter().any(|symbol| {
        matches!(
            symbol.kind(),
            InstrumentKind::Perpetual | InstrumentKind::Futures
        )
    }) {
        return Ok(());
    }
    let Some(native) = message
        .get("topic")
        .and_then(Value::as_str)
        .and_then(|topic| topic.strip_prefix("tickers."))
    else {
        return Ok(());
    };
    // Bound the cache to configured symbols, including multiplexed endpoints.
    if !feed
        .exchange_symbols
        .iter()
        .any(|symbol| symbol.eq_ignore_ascii_case(native))
    {
        return Ok(());
    }
    let native = native.to_ascii_uppercase();
    let row = message
        .get("data")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            Error::MalformedData("Bybit derivative ticker data must be an object".to_owned())
        })?;
    if row
        .get("symbol")
        .and_then(Value::as_str)
        .is_some_and(|symbol| !symbol.eq_ignore_ascii_case(&native))
    {
        return Err(Error::MalformedData(
            "Bybit ticker topic and symbol disagree".to_owned(),
        ));
    }
    match message.get("type").and_then(Value::as_str) {
        Some("snapshot") => {
            tickers.insert(native.clone(), serde_json::Map::new());
        }
        Some("delta") if tickers.contains_key(&native) => {}
        Some("delta") => {
            return Err(Error::MalformedData(
                "Bybit ticker delta arrived before snapshot".to_owned(),
            ));
        }
        _ => {
            return Err(Error::MalformedData(
                "Bybit ticker is missing snapshot/delta type".to_owned(),
            ));
        }
    }
    let state = tickers
        .get_mut(&native)
        .expect("ticker snapshot initialized");
    // Only fields consumed by the normalized public models are retained.
    for field in [
        "bid1Price",
        "ask1Price",
        "fundingRate",
        "nextFundingTime",
        "markPrice",
        "indexPrice",
        "openInterest",
        "openInterestValue",
    ] {
        if let Some(value) = row.get(field) {
            state.insert(field.to_owned(), value.clone());
        }
    }
    state.insert("symbol".to_owned(), Value::String(native));
    message["data"] = Value::Object(state.clone());
    Ok(())
}

pub(crate) async fn process_bybit_message(
    feed: &ExchangeFeed,
    message: &Value,
    received_ts: f64,
) -> Result<()> {
    if let Some(control) = BybitAdapter::parse_control_message(message) {
        return control.map(|_| ());
    }
    #[cfg(feature = "orderbook")]
    let handled = process_bybit_orderbook_message(feed, message, received_ts).await?;
    #[cfg(not(feature = "orderbook"))]
    let handled = false;
    if handled {
        return Ok(());
    }
    let events = BybitAdapter::parse_messages_for_feed(feed, message, received_ts);
    let bybit_topic = message.get("topic").and_then(Value::as_str);
    let strict_topic = bybit_topic.is_some_and(|topic| topic.starts_with("allLiquidation."))
        || (bybit_topic.is_some_and(|topic| topic.starts_with("tickers."))
            && feed
                .symbols
                .iter()
                .any(|symbol| symbol.kind() == InstrumentKind::Option));
    if strict_topic && data_is_non_empty(message) && events.is_empty() {
        return Err(Error::MalformedData(format!(
            "Bybit message for {} produced no normalized events",
            bybit_topic.unwrap_or("unknown topic")
        )));
    }
    for event in events {
        dispatch_bybit_event(feed, event).await?;
    }
    Ok(())
}

#[cfg(feature = "orderbook")]
pub(crate) async fn process_bybit_orderbook_message(
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

    if topic.starts_with("orderbook.1.") {
        for event in BybitAdapter::parse_messages_for_feed(feed, message, received_ts) {
            dispatch_bybit_event(feed, event).await?;
        }
        return Ok(true);
    }

    let update = BybitAdapter::parse_l2_book_update_for_feed(feed, message, received_ts)
        .ok_or_else(|| Error::Parse("failed to parse bybit depth update".to_owned()))?;
    if !feed.subscribes(Channel::L2Book, update.book.symbol()) {
        return Ok(true);
    }
    let symbol_key = match &update.book {
        L2Book::Snapshot(snapshot) => snapshot.symbol.as_str().to_owned(),
        L2Book::Delta(delta) => delta.symbol.as_str().to_owned(),
    };
    let maybe_book = {
        let mut syncs = feed
            .bybit_book_syncs()
            .expect("bybit book-sync state")
            .lock()
            .expect("bybit sync lock");
        let sync = syncs.entry(symbol_key).or_insert_with(|| {
            BybitBookSync::new(match &update.book {
                L2Book::Snapshot(snapshot) => snapshot.symbol.clone(),
                L2Book::Delta(delta) => delta.symbol.clone(),
            })
        });
        sync.apply(update)?
    };

    if let Some(book) = maybe_book {
        dispatch_bybit_l2_book(feed, book).await?;
    }

    Ok(true)
}
