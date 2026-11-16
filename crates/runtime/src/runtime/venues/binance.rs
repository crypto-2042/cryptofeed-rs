//! Binance venue feed support.
//!
//! Owns the `binance` connection plans, session loops, wire-message parsing and
//! normalized event dispatch. Shared machinery lives in `runtime.rs` and
//! `runtime/venue_support.rs`.

use super::prelude::*;
use crate::runtime::venue_support::*;

#[cfg(feature = "orderbook")]
pub(crate) type SnapshotReceivers = std::collections::HashMap<
    String,
    snapshot::SnapshotReceiver<(u64, cryptofeed_orderbook::L2BookSnapshot)>,
>;

#[cfg(feature = "orderbook")]
const BINANCE_MAX_RESNAPSHOTS: u32 = 3;

pub(crate) async fn consume_binance_feed(
    feed: ExchangeFeed,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let plans = BinanceAdapter::connection_plans(&feed)?;
    let mut tasks = JoinSet::new();
    for plan in plans {
        let mut feed = feed.clone();
        let books = if plan.snapshot_urls.is_empty() {
            Vec::new()
        } else {
            plan.instruments
                .iter()
                .filter(|instrument| feed.subscribes(Channel::L2Book, &instrument.symbol))
                .map(|instrument| instrument.symbol.as_str().to_owned())
                .collect()
        };
        readiness::track(&mut feed, &plan.websocket_url, books);
        let retry_shutdown = shutdown.clone();
        tasks.spawn(async move {
            let product = plan.product;
            let result = supervisor::retry_with_progress(
                feed.runtime_options.retry_limit(),
                supervisor::Backoff::new(1, 8),
                retry_shutdown.clone(),
                feed.retry_progress.clone(),
                feed.runtime_options.startup_delay(),
                move || {
                    let feed = feed.clone();
                    let plan = plan.clone();
                    let shutdown = retry_shutdown.clone();
                    async move { consume_binance_session(feed, plan, shutdown).await }
                },
            )
            .await
            .map(|_| ());
            (product, result)
        });
    }

    while let Some(result) = tasks.join_next().await {
        match result {
            Ok((_, Ok(()))) => {}
            Ok((product, Err(error))) => {
                tracing::error!(?product, error = %error, "binance product feed terminated");
                tasks.abort_all();
                return Err(Error::Transport(format!("Binance {product:?}: {error}")));
            }
            Err(error) => {
                tasks.abort_all();
                return Err(Error::Transport(format!("Binance task: {error}")));
            }
        }
    }
    Ok(())
}

async fn consume_binance_session(
    mut feed: ExchangeFeed,
    plan: BinanceConnectionPlan,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let attempt = readiness::begin(&mut feed);
    reset_connection_books(
        &feed,
        !plan.snapshot_urls.is_empty(),
        plan.instruments.iter().map(|instrument| &instrument.symbol),
    );
    let explicit = if attempt.is_some() {
        Some(BinanceAdapter::explicit_subscription(&plan)?)
    } else {
        None
    };
    let url = match &explicit {
        Some((url, _)) => Url::parse(url).map_err(|error| Error::Transport(error.to_string()))?,
        None => Url::parse(&plan.websocket_url).map_err(|e| Error::Transport(e.to_string()))?,
    };
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
    if let Some((_, subscribe)) = explicit {
        session.queue_subscriptions([subscribe]);
    }
    #[cfg(feature = "orderbook")]
    let snapshot_receivers = if plan.snapshot_urls.is_empty() {
        std::collections::HashMap::new()
    } else {
        spawn_binance_snapshot_fetches(
            &plan
                .instruments
                .iter()
                .filter(|instrument| feed.subscribes(Channel::L2Book, &instrument.symbol))
                .cloned()
                .collect::<Vec<_>>(),
            plan.l2_book_depth.unwrap_or(1000),
            &feed.transport,
            feed.snapshot_mode(),
        )
    };
    #[cfg(feature = "orderbook")]
    let pending_deltas: std::collections::HashMap<String, Vec<BinanceSequencedDepthDelta>> =
        std::collections::HashMap::new();
    #[cfg(feature = "orderbook")]
    let resnapshot_attempts: std::collections::HashMap<String, u32> =
        std::collections::HashMap::new();
    let result = consume_binance_session_with(
        feed,
        plan,
        shutdown,
        &mut session,
        #[cfg(feature = "orderbook")]
        snapshot_receivers,
        #[cfg(feature = "orderbook")]
        pending_deltas,
        #[cfg(feature = "orderbook")]
        resnapshot_attempts,
    )
    .await;
    session.record_result(&result);
    result
}

pub(crate) async fn consume_binance_session_with<S, E>(
    feed: ExchangeFeed,
    plan: BinanceConnectionPlan,
    mut shutdown: watch::Receiver<bool>,
    session: &mut Session<S>,
    #[cfg(feature = "orderbook")] mut snapshot_receivers: SnapshotReceivers,
    #[cfg(feature = "orderbook")] mut pending_deltas: std::collections::HashMap<
        String,
        Vec<BinanceSequencedDepthDelta>,
    >,
    #[cfg(feature = "orderbook")] mut resnapshot_attempts: std::collections::HashMap<String, u32>,
) -> Result<()>
where
    S: Sink<Message, Error = E> + Stream<Item = std::result::Result<Message, E>> + Unpin,
    E: Display,
{
    while let Some(text) = session.next_text_or_shutdown(&mut shutdown).await? {
        let received_ts = session.received_timestamp();
        #[cfg(feature = "orderbook")]
        poll_binance_snapshot_bootstraps(
            &feed,
            &plan.instruments,
            &mut snapshot_receivers,
            &mut pending_deltas,
            &mut resnapshot_attempts,
            plan.l2_book_depth.unwrap_or(1000),
            plan.l2_book_depth.is_some(),
        )
        .await?;
        #[cfg(feature = "orderbook")]
        let handled = {
            session.processing();
            process_binance_orderbook_message(
                &feed,
                &plan,
                &text,
                received_ts,
                &mut snapshot_receivers,
                &mut pending_deltas,
            )
            .await?
        };
        #[cfg(not(feature = "orderbook"))]
        let handled = {
            session.processing();
            false
        };
        if !handled {
            process_binance_text_message_for_plan(&feed, &plan, &text, received_ts).await?;
        }
    }

    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) async fn dispatch_binance_event(feed: &ExchangeFeed, event: BinanceEvent) -> Result<()> {
    let _ = feed;
    match event {
        #[cfg(feature = "candles")]
        BinanceEvent::Candle(candle) => dispatch_candle(feed, candle).await,
        #[cfg(feature = "funding")]
        BinanceEvent::Funding(funding) => dispatch_funding(feed, funding).await,
        #[cfg(feature = "index")]
        BinanceEvent::IndexPrice(index_price) => {
            dispatch_index_price(feed, index_price).await;
        }
        #[cfg(feature = "liquidations")]
        BinanceEvent::Liquidation(liquidation) => dispatch_liquidation(feed, liquidation).await,
        #[cfg(feature = "markprice")]
        BinanceEvent::MarkPrice(mark_price) => {
            dispatch_mark_price(feed, mark_price).await;
        }
        #[cfg(feature = "orderbook")]
        BinanceEvent::L1Book(book) => dispatch_l1_book(feed, book).await,
        #[cfg(feature = "orderbook")]
        BinanceEvent::L2Book(book) => dispatch_l2_book(feed, book).await?,
        #[cfg(feature = "openinterest")]
        BinanceEvent::OpenInterest(open_interest) => {
            dispatch_open_interest(feed, open_interest).await;
        }
        #[cfg(feature = "ticker")]
        BinanceEvent::Ticker(ticker) => dispatch_ticker(feed, ticker).await,
        #[cfg(feature = "trade")]
        BinanceEvent::Trade(trade) => dispatch_trade(feed, trade).await,
    }
    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) async fn process_binance_text_message(
    feed: &ExchangeFeed,
    text: &str,
    received_ts: f64,
) -> Result<()> {
    let message: Value = serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
    for event in BinanceAdapter::parse_messages(&message, received_ts) {
        dispatch_binance_event(feed, event).await?;
    }
    Ok(())
}

pub(crate) async fn process_binance_text_message_for_plan(
    feed: &ExchangeFeed,
    plan: &BinanceConnectionPlan,
    text: &str,
    received_ts: f64,
) -> Result<()> {
    let message: Value = serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
    let instrument = binance_instrument_for_message(plan, &message)?;
    for event in BinanceAdapter::parse_messages_for_instrument(&message, received_ts, instrument) {
        dispatch_binance_event(feed, event).await?;
    }
    Ok(())
}

pub(crate) fn binance_instrument_for_message<'a>(
    plan: &'a BinanceConnectionPlan,
    message: &Value,
) -> Result<&'a BinanceInstrument> {
    let payload = BinanceAdapter::unwrap_combined_message(message).unwrap_or(message);
    let exchange_symbol = payload
        .get("s")
        .or_else(|| payload.get("o").and_then(|order| order.get("s")))
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Parse("binance message is missing a symbol".to_owned()))?;
    plan.instruments
        .iter()
        .find(|instrument| {
            instrument
                .exchange_symbol
                .eq_ignore_ascii_case(exchange_symbol)
        })
        .ok_or_else(|| {
            Error::Parse(format!(
                "binance message symbol {exchange_symbol} is not in the connection plan"
            ))
        })
}

#[cfg(feature = "orderbook")]
pub(crate) async fn dispatch_binance_l2_book(feed: &ExchangeFeed, book: L2Book) -> Result<()> {
    dispatch_l2_book(feed, book).await
}

#[cfg(feature = "orderbook")]
fn spawn_binance_snapshot_fetches(
    instruments: &[BinanceInstrument],
    limit: u16,
    transport: &crate::transport::TransportConfig,
    mode: snapshot::SnapshotMode,
) -> std::collections::HashMap<
    String,
    snapshot::SnapshotReceiver<(u64, cryptofeed_orderbook::L2BookSnapshot)>,
> {
    let mut receivers = std::collections::HashMap::new();

    for instrument in instruments {
        let symbol_key = instrument.symbol.as_str().to_owned();
        let rx = spawn_binance_snapshot_fetch(instrument.clone(), limit, transport.clone(), mode);
        receivers.insert(symbol_key, rx);
    }

    receivers
}

#[cfg(feature = "orderbook")]
pub(crate) fn spawn_binance_snapshot_fetch(
    instrument: BinanceInstrument,
    limit: u16,
    transport: crate::transport::TransportConfig,
    mode: snapshot::SnapshotMode,
) -> snapshot::SnapshotReceiver<(u64, cryptofeed_orderbook::L2BookSnapshot)> {
    let (tx, rx, raw) = snapshot::SnapshotReceiver::channel(mode);
    let Some(mut tx) = tx else {
        return rx;
    };
    tokio::spawn(async move {
        tokio::select! {
            biased;
            _ = tx.closed() => {}
            result = fetch_binance_l2_snapshot(instrument, limit, transport,raw) => {
                let _ = tx.send(result);
            }
        }
    });
    rx
}

#[cfg(feature = "orderbook")]
async fn fetch_binance_l2_snapshot(
    instrument: BinanceInstrument,
    limit: u16,
    transport: crate::transport::TransportConfig,
    raw: Option<snapshot::RawSlot>,
) -> Result<(u64, cryptofeed_orderbook::L2BookSnapshot)> {
    // Partial-depth streams must be bootstrapped from a snapshot of the same
    // width; the full-depth default uses the 1000-level snapshot.
    let url = BinanceAdapter::snapshot_url(&instrument, limit);
    let payload = snapshot::fetch_json(&url, &transport, ExchangeId::Binance).await?;
    let received_ts = current_timestamp();
    let result =
        binance_parser::parse_l2_book_snapshot_for_instrument(&payload, &instrument, received_ts)
            .ok_or_else(|| Error::Parse("failed to parse binance l2 snapshot".to_owned()));
    if let Some(raw) = raw {
        *raw.lock().expect("snapshot capture") = Some(snapshot::RawSnapshot {
            payload,
            received_ts,
        });
    }
    result
}

#[cfg(feature = "orderbook")]
pub(crate) async fn poll_binance_snapshot_bootstraps(
    feed: &ExchangeFeed,
    instruments: &[BinanceInstrument],
    receivers: &mut std::collections::HashMap<
        String,
        snapshot::SnapshotReceiver<(u64, cryptofeed_orderbook::L2BookSnapshot)>,
    >,
    pending: &mut std::collections::HashMap<String, Vec<BinanceSequencedDepthDelta>>,
    resnapshot_attempts: &mut std::collections::HashMap<String, u32>,
    limit: u16,
    partial_depth: bool,
) -> Result<()> {
    use tokio::sync::oneshot::error::TryRecvError;

    let keys: Vec<String> = receivers.keys().cloned().collect();
    for key in keys {
        let ready = if let Some(receiver) = receivers.get_mut(&key) {
            match receiver.try_recv() {
                Ok(result) => Some(result),
                Err(TryRecvError::Empty) => None,
                Err(TryRecvError::Closed) => {
                    return Err(Error::Transport(
                        "binance snapshot bootstrap channel closed".to_owned(),
                    ));
                }
            }
        } else {
            None
        };

        if let Some(result) = ready {
            #[cfg(feature = "recording")]
            if let Some(link) = &feed.raw_session {
                if let Some(instrument) = instruments
                    .iter()
                    .find(|instrument| instrument.symbol.as_str() == key)
                {
                    if let Some(raw) = receivers
                        .get_mut(&key)
                        .and_then(|receiver| receiver.take_raw())
                    {
                        link.snapshot(
                            instrument.symbol.clone(),
                            Some(limit),
                            &raw.payload,
                            raw.received_ts,
                        );
                    } else if let Err(error) = &result {
                        link.snapshot_error(instrument.symbol.clone(), Some(limit), error);
                    }
                }
            }
            let (last_update_id, snapshot) = result?;
            let instrument = instruments
                .iter()
                .find(|instrument| instrument.symbol.as_str() == key)
                .ok_or_else(|| Error::Parse(format!("missing binance instrument for {key}")))?;
            let buffered = pending.remove(&key).unwrap_or_default();
            let books = match bootstrap_binance_book_events(
                feed,
                &key,
                instrument,
                last_update_id,
                snapshot,
                buffered,
                partial_depth,
            ) {
                Ok(books) => {
                    resnapshot_attempts.remove(&key);
                    books
                }
                Err(error) => {
                    // A non-bridging bootstrap triggers a bounded in-session
                    // resnapshot; only the limit-exceeded case escalates to
                    // the session-level retry.
                    let attempts = resnapshot_attempts.entry(key.clone()).or_default();
                    *attempts += 1;
                    if *attempts >= BINANCE_MAX_RESNAPSHOTS {
                        return Err(Error::Parse(format!(
                            "binance bootstrap resnapshot limit exceeded for {key}: {error}"
                        )));
                    }
                    receivers.insert(
                        key.clone(),
                        spawn_binance_snapshot_fetch(
                            instrument.clone(),
                            limit,
                            feed.transport.clone(),
                            feed.snapshot_mode(),
                        ),
                    );
                    continue;
                }
            };
            for book in books {
                dispatch_binance_l2_book(feed, book).await?;
            }
            receivers.remove(&key);
        }
    }

    Ok(())
}

#[cfg(feature = "orderbook")]
fn bootstrap_binance_book_events(
    feed: &ExchangeFeed,
    symbol_key: &str,
    instrument: &BinanceInstrument,
    last_update_id: u64,
    snapshot: cryptofeed_orderbook::L2BookSnapshot,
    buffered: Vec<BinanceSequencedDepthDelta>,
    partial_depth: bool,
) -> Result<Vec<cryptofeed_orderbook::L2Book>> {
    let mut syncs = feed
        .binance_book_syncs()
        .expect("binance book-sync state")
        .lock()
        .expect("binance sync lock");
    let sync = syncs.entry(symbol_key.to_owned()).or_insert_with(|| {
        if partial_depth {
            BinanceBookSync::new_for_partial_depth(snapshot.symbol.clone(), instrument.product)
        } else {
            BinanceBookSync::new_for_product(snapshot.symbol.clone(), instrument.product)
        }
    });
    sync.bootstrap_sequenced_events(last_update_id, snapshot, buffered)
}

#[cfg(all(feature = "orderbook", test))]
pub(crate) fn bootstrap_binance_book(
    feed: &ExchangeFeed,
    symbol_key: &str,
    last_update_id: u64,
    snapshot: cryptofeed_orderbook::L2BookSnapshot,
    buffered: Vec<BinanceDepthDelta>,
) -> Result<Option<cryptofeed_orderbook::L2Book>> {
    let mut syncs = feed
        .binance_book_syncs()
        .expect("binance book-sync state")
        .lock()
        .expect("binance sync lock");
    let sync = syncs
        .entry(symbol_key.to_owned())
        .or_insert_with(|| BinanceBookSync::new(snapshot.symbol.clone()));
    sync.bootstrap(last_update_id, snapshot.clone(), buffered)?;

    let mut states = feed.orderbook_states.lock().expect("orderbook state lock");
    states.insert(symbol_key.to_owned(), sync.state().clone());

    Ok(Some(cryptofeed_orderbook::L2Book::Snapshot(snapshot)))
}

#[cfg(feature = "orderbook")]
pub(crate) async fn process_binance_orderbook_message(
    feed: &ExchangeFeed,
    plan: &BinanceConnectionPlan,
    text: &str,
    received_ts: f64,
    receivers: &mut std::collections::HashMap<
        String,
        snapshot::SnapshotReceiver<(u64, cryptofeed_orderbook::L2BookSnapshot)>,
    >,
    pending: &mut std::collections::HashMap<String, Vec<BinanceSequencedDepthDelta>>,
) -> Result<bool> {
    let message: Value = serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
    let payload = BinanceAdapter::unwrap_combined_message(&message).unwrap_or(&message);

    if payload.get("e").and_then(|v| v.as_str()) != Some("depthUpdate") {
        // Spot partial-depth pushes (`@depth5@100ms` etc.) carry no `e`/`s`:
        // `{lastUpdateId, bids, asks}` with the instrument only in the
        // combined-stream `stream` name. Every push is the complete top-N
        // book, so it routes through book sync as a replacement snapshot.
        if BinanceAdapter::is_partial_depth_payload(payload) {
            let Some(stream) = message.get("stream").and_then(Value::as_str) else {
                return Ok(false);
            };
            let Some(instrument) = BinanceAdapter::instrument_for_stream(plan, stream) else {
                return Ok(false);
            };
            let update = binance_parser::parse_partial_depth_for_instrument(
                payload,
                received_ts,
                instrument,
            )
            .ok_or_else(|| Error::Parse("failed to parse binance partial depth push".to_owned()))?;
            let symbol_key = update.delta.book.symbol.as_str().to_owned();
            apply_binance_sequenced_update(
                feed,
                plan,
                &symbol_key,
                instrument,
                update,
                receivers,
                pending,
            )
            .await?;
            return Ok(true);
        }
        return Ok(false);
    }

    let instrument = binance_instrument_for_message(plan, &message)?;
    let update =
        binance_parser::parse_l2_book_update_for_instrument(payload, received_ts, instrument)
            .ok_or_else(|| Error::Parse("failed to parse binance depth update".to_owned()))?;
    let symbol_key = update.delta.book.symbol.as_str().to_owned();
    apply_binance_sequenced_update(
        feed,
        plan,
        &symbol_key,
        instrument,
        update,
        receivers,
        pending,
    )
    .await?;

    Ok(true)
}

/// Route one sequenced depth update (full-depth delta or partial-depth push)
/// through the local book sync, or buffer it while a snapshot bootstrap is in
/// flight. Buffered deltas are bounded: the oldest is dropped past the cap,
/// which surfaces as a non-bridging bootstrap and triggers the bounded
/// resnapshot path rather than unbounded memory growth.
#[cfg(feature = "orderbook")]
async fn apply_binance_sequenced_update(
    feed: &ExchangeFeed,
    plan: &BinanceConnectionPlan,
    symbol_key: &str,
    instrument: &BinanceInstrument,
    update: BinanceSequencedDepthDelta,
    receivers: &mut std::collections::HashMap<
        String,
        snapshot::SnapshotReceiver<(u64, cryptofeed_orderbook::L2BookSnapshot)>,
    >,
    pending: &mut std::collections::HashMap<String, Vec<BinanceSequencedDepthDelta>>,
) -> Result<()> {
    if !feed.subscribes(Channel::L2Book, &instrument.symbol) {
        return Ok(());
    }
    if feed
        .binance_book_syncs()
        .expect("binance book-sync state")
        .lock()
        .expect("binance sync lock")
        .contains_key(symbol_key)
    {
        let maybe_book = {
            let mut syncs = feed
                .binance_book_syncs()
                .expect("binance book-sync state")
                .lock()
                .expect("binance sync lock");
            let sync = syncs.get_mut(symbol_key).expect("binance sync state");
            sync.apply_next_sequenced_delta(update.clone())
        };

        match maybe_book {
            Ok(Some(book)) => {
                dispatch_binance_l2_book(feed, book).await?;
            }
            Ok(None) => {}
            Err(err) => {
                if matches!(err, Error::Parse(_)) {
                    schedule_binance_resync(
                        feed,
                        instrument,
                        symbol_key,
                        update,
                        receivers,
                        pending,
                        plan.l2_book_depth.unwrap_or(1000),
                    );
                } else {
                    return Err(err);
                }
            }
        }
    } else {
        buffer_binance_delta(pending, symbol_key, update);
    }

    Ok(())
}

#[cfg(feature = "orderbook")]
fn buffer_binance_delta(
    pending: &mut std::collections::HashMap<String, Vec<BinanceSequencedDepthDelta>>,
    symbol_key: &str,
    update: BinanceSequencedDepthDelta,
) {
    let queue = pending.entry(symbol_key.to_owned()).or_default();
    if queue.len() >= MAX_BUFFERED_DELTAS_PER_SYMBOL {
        queue.remove(0);
    }
    queue.push(update);
}

#[cfg(feature = "orderbook")]
fn reset_binance_sequenced_sync_state(
    feed: &ExchangeFeed,
    symbol_key: &str,
    update: BinanceSequencedDepthDelta,
    pending: &mut std::collections::HashMap<String, Vec<BinanceSequencedDepthDelta>>,
) {
    feed.invalidate_book_readiness(symbol_key);
    feed.binance_book_syncs()
        .expect("binance book-sync state")
        .lock()
        .expect("binance sync lock")
        .remove(symbol_key);
    buffer_binance_delta(pending, symbol_key, update);
}

#[cfg(all(feature = "orderbook", test))]
pub(crate) fn reset_binance_sync_state(
    feed: &ExchangeFeed,
    symbol_key: &str,
    update: BinanceDepthDelta,
    pending: &mut std::collections::HashMap<String, Vec<BinanceDepthDelta>>,
) {
    feed.invalidate_book_readiness(symbol_key);
    feed.binance_book_syncs()
        .expect("binance book-sync state")
        .lock()
        .expect("binance sync lock")
        .remove(symbol_key);
    pending
        .entry(symbol_key.to_owned())
        .or_default()
        .push(update);
}

#[cfg(feature = "orderbook")]
fn schedule_binance_resync(
    feed: &ExchangeFeed,
    instrument: &BinanceInstrument,
    symbol_key: &str,
    update: BinanceSequencedDepthDelta,
    receivers: &mut std::collections::HashMap<
        String,
        snapshot::SnapshotReceiver<(u64, cryptofeed_orderbook::L2BookSnapshot)>,
    >,
    pending: &mut std::collections::HashMap<String, Vec<BinanceSequencedDepthDelta>>,
    limit: u16,
) {
    reset_binance_sequenced_sync_state(feed, symbol_key, update, pending);
    receivers.remove(symbol_key);
    receivers.insert(
        symbol_key.to_owned(),
        spawn_binance_snapshot_fetch(
            instrument.clone(),
            limit,
            feed.transport.clone(),
            feed.snapshot_mode(),
        ),
    );
}
