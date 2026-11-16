//! Htx venue feed support.
//!
//! Owns the `htx` connection plans, session loops, wire-message parsing and
//! normalized event dispatch. Shared machinery lives in `runtime.rs` and
//! `runtime/venue_support.rs`.

use super::prelude::*;
use crate::runtime::venue_support::*;

pub(crate) async fn consume_htx_feed(
    feed: ExchangeFeed,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut tasks = JoinSet::new();
    for url in HtxAdapter::subscription_urls(&feed) {
        let mut planned_feed = HtxAdapter::feed_for_url(&feed, &url);
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
                    async move { consume_htx_session(feed, url, shutdown).await }
                },
            )
            .await
            .map(|_| ())
        });
    }
    collect_connection_results("HTX", tasks).await
}

async fn consume_htx_session(
    mut feed: ExchangeFeed,
    url: String,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let attempt = readiness::begin(&mut feed);
    reset_connection_books(&feed, true, &feed.symbols);
    let url = Url::parse(&url).map_err(|e| Error::Transport(e.to_string()))?;
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
    let result = consume_htx_session_with(feed, shutdown, &mut session).await;
    session.record_result(&result);
    result
}

pub(crate) async fn consume_htx_session_with<S, E>(
    feed: ExchangeFeed,
    mut shutdown: watch::Receiver<bool>,
    session: &mut Session<S>,
) -> Result<()>
where
    S: Sink<Message, Error = E> + Stream<Item = std::result::Result<Message, E>> + Unpin,
    E: Display,
{
    session.queue_subscriptions(HtxAdapter::subscription_messages(&feed)?);
    // Server heartbeat pongs are answered immediately so the documented ~15 s
    // `ping check` window is never queued behind paced subscriptions; only
    // USDT-M incremental-depth resubscription pairs use the paced queue.
    let mut outbound = HtxOutbound::default();
    let mut pings = 0u64;
    let mut pongs = 0u64;
    let mut pong_failures = 0u64;
    let result = loop {
        let text = match session.next_text_or_shutdown(&mut shutdown).await {
            Ok(Some(text)) => text,
            Ok(None) => break Ok(()),
            Err(error) => break Err(error),
        };
        let received_ts = session.received_timestamp();
        session.processing();
        outbound.clear();
        if let Err(error) = process_htx_text_message(&feed, &text, received_ts, &mut outbound).await
        {
            break Err(error);
        }
        pings += outbound.pings;
        let mut pong_error = None;
        for pong in std::mem::take(&mut outbound.priority) {
            match session.send_text(&pong).await {
                Ok(()) => pongs += 1,
                Err(error) => {
                    pong_failures += 1;
                    pong_error = Some(error);
                    break;
                }
            }
        }
        if let Some(error) = pong_error {
            break Err(error);
        }
        if !outbound.subscriptions.is_empty() {
            session.queue_subscriptions(std::mem::take(&mut outbound.subscriptions));
        }
    };
    let frames = session.frame_stats();
    // Bounded, sanitized diagnostics: frame kinds/counts/bytes and heartbeat
    // correlation only; no payloads or raw close reasons.
    if result.is_err() {
        tracing::warn!(
            exchange = ?ExchangeId::Htx,
            feed_id = feed.identity.map(|identity| identity.id.as_u64()),
            text_frames = frames.text_frames,
            binary_frames = frames.binary_frames,
            text_bytes = frames.text_bytes,
            binary_bytes = frames.binary_bytes,
            max_frame_bytes = frames.max_frame_bytes,
            last_frame_age_ms = frames
                .last_frame_at
                .map(|at| at.elapsed().as_millis() as u64),
            pings,
            pongs,
            pong_failures,
            "HTX session ended with frame diagnostics"
        );
    } else {
        tracing::debug!(
            exchange = ?ExchangeId::Htx,
            feed_id = feed.identity.map(|identity| identity.id.as_u64()),
            text_frames = frames.text_frames,
            binary_frames = frames.binary_frames,
            text_bytes = frames.text_bytes,
            binary_bytes = frames.binary_bytes,
            max_frame_bytes = frames.max_frame_bytes,
            last_frame_age_ms = frames
                .last_frame_at
                .map(|at| at.elapsed().as_millis() as u64),
            pings,
            pongs,
            pong_failures,
            "HTX session frame diagnostics"
        );
    }
    result
}

/// Outbound HTX frames separated by urgency: pongs are sent immediately while
/// subscription frames stay behind the paced queue.
#[derive(Default)]
pub(crate) struct HtxOutbound {
    pub(crate) priority: Vec<String>,
    pub(crate) subscriptions: Vec<String>,
    pub(crate) pings: u64,
}

impl HtxOutbound {
    fn clear(&mut self) {
        self.priority.clear();
        self.subscriptions.clear();
        self.pings = 0;
    }
}

#[cfg_attr(not(test), allow(dead_code))]
async fn dispatch_htx_event(feed: &ExchangeFeed, event: HtxEvent) -> Result<()> {
    let _ = feed;
    match event {
        #[cfg(feature = "candles")]
        HtxEvent::Candle(candle) => dispatch_candle(feed, candle).await,
        #[cfg(feature = "funding")]
        HtxEvent::Funding(funding) => dispatch_funding(feed, funding).await,
        #[cfg(feature = "liquidations")]
        HtxEvent::Liquidation(liquidation) => dispatch_liquidation(feed, liquidation).await,
        #[cfg(feature = "orderbook")]
        HtxEvent::L1Book(book) => dispatch_l1_book(feed, book).await,
        #[cfg(feature = "ticker")]
        HtxEvent::Ticker(ticker) => dispatch_ticker(feed, ticker).await,
        #[cfg(feature = "trade")]
        HtxEvent::Trade(trade) => dispatch_trade(feed, trade).await,
    }
    Ok(())
}

#[cfg(feature = "orderbook")]
async fn dispatch_htx_l2_book(feed: &ExchangeFeed, book: L2Book) -> Result<()> {
    dispatch_l2_book(feed, book).await
}

pub(crate) async fn process_htx_text_message(
    feed: &ExchangeFeed,
    text: &str,
    received_ts: f64,
    outbound: &mut HtxOutbound,
) -> Result<()> {
    let message: Value = serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
    if let Some(control) = HtxAdapter::parse_control_message(&message) {
        match control? {
            HtxControl::Ack => {}
            HtxControl::Ping(_) => {
                outbound.pings += 1;
                if let Some(pong) = HtxAdapter::pong_message(&message) {
                    outbound.priority.push(pong);
                }
            }
        }
        return Ok(());
    }
    #[cfg(feature = "orderbook")]
    if process_htx_orderbook_message(feed, &message, received_ts, outbound).await? {
        return Ok(());
    }
    let events = HtxAdapter::parse_messages_for_feed(feed, &message, received_ts);
    let configured = htx_parser::channel(&message)
        .and_then(htx_parser::topic_parts)
        .map(|(native, _, _)| native)
        .or_else(|| htx_parser::topic(&message).and_then(htx_parser::notification_symbol))
        .is_some_and(|native| HtxAdapter::symbol_for_native(feed, native).is_some());
    let data_frame =
        htx_parser::op(&message) == Some("notify") || htx_parser::channel(&message).is_some();
    if configured && data_frame && events.is_empty() {
        return Err(Error::MalformedData(format!(
            "HTX message for {} produced no normalized events",
            htx_parser::channel(&message)
                .or_else(|| htx_parser::topic(&message))
                .unwrap_or("unknown topic")
        )));
    }
    for event in events {
        dispatch_htx_event(feed, event).await?;
    }
    Ok(())
}

#[cfg(feature = "orderbook")]
async fn process_htx_orderbook_message(
    feed: &ExchangeFeed,
    message: &Value,
    received_ts: f64,
    outbound: &mut HtxOutbound,
) -> Result<bool> {
    let Some(channel) = htx_parser::channel(message) else {
        return Ok(false);
    };
    let Some((native, "depth", _)) = htx_parser::topic_parts(channel) else {
        return Ok(false);
    };
    let Some(symbol) = HtxAdapter::symbol_for_native(feed, native).cloned() else {
        return Ok(true);
    };
    if !feed.subscribes(Channel::L2Book, &symbol) {
        return Ok(true);
    }
    let tick = message
        .get("tick")
        .ok_or_else(|| Error::Parse("HTX depth message has no tick".to_owned()))?;
    let product = HtxAdapter::product_for_feed(feed)?;
    let (update, mode) = match product {
        crate::exchange::htx::adapter::HtxProduct::Spot => (
            htx_parser::parse_spot_depth(tick, received_ts, &symbol)
                .ok_or_else(|| Error::Parse("failed to parse HTX spot depth".to_owned()))?,
            HtxBookMode::SpotSnapshot,
        ),
        crate::exchange::htx::adapter::HtxProduct::Perpetual => (
            htx_parser::parse_swap_depth(tick, received_ts, &symbol)
                .ok_or_else(|| Error::Parse("failed to parse HTX swap depth".to_owned()))?,
            HtxBookMode::SwapIncremental,
        ),
    };
    let symbol_key = symbol.as_str().to_owned();
    let outcome = {
        let mut syncs = feed
            .htx_book_syncs()
            .expect("htx book-sync state")
            .lock()
            .expect("htx sync lock");
        let sync = syncs
            .entry(symbol_key)
            .or_insert_with(|| HtxBookSync::new(symbol.clone(), mode));
        sync.apply(update)?
    };
    match outcome {
        HtxBookOutcome::Publish(book) => dispatch_htx_l2_book(feed, book).await?,
        HtxBookOutcome::Stale => {}
        HtxBookOutcome::Resnapshot => {
            outbound
                .subscriptions
                .extend(HtxAdapter::book_resubscribe_messages(feed, &symbol)?);
        }
    }
    Ok(true)
}
