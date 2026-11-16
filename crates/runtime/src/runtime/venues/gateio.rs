//! Gateio venue feed support.
//!
//! Owns the `gateio` connection plans, session loops, wire-message parsing and
//! normalized event dispatch. Shared machinery lives in `runtime.rs` and
//! `runtime/venue_support.rs`.

use super::prelude::*;
use crate::runtime::venue_support::*;

#[cfg(feature = "orderbook")]
pub(crate) type GateioSnapshotReceivers = std::collections::HashMap<
    String,
    snapshot::SnapshotReceiver<crate::exchange::gateio::book_sync::GateioBookSnapshot>,
>;

/// Additional Gateio snapshot operations per symbol/physical session, across
/// bootstrap failures AND later gaps. Success does not refund this budget;
/// exhaustion returns to the existing reconnect/backoff supervisor.
#[cfg(feature = "orderbook")]
pub(crate) const GATEIO_MAX_RESNAPSHOTS: u32 = 3;

#[cfg(feature = "orderbook")]
pub(crate) const GATEIO_BOOTSTRAP_DEADLINE: std::time::Duration =
    std::time::Duration::from_secs(60);

/// Cumulative requests in one physical session. Success must not reset this
/// budget: otherwise bootstrap -> immediate gap can refetch forever.
#[cfg(feature = "orderbook")]
pub(crate) struct GateioResnapshotBudget {
    pub(crate) resnapshots: u32,
    pub(crate) unsynced_since: tokio::time::Instant,
}

#[cfg(feature = "orderbook")]
pub(crate) fn gateio_snapshot_budget(
    budgets: &mut std::collections::HashMap<String, GateioResnapshotBudget>,
    symbol: &str,
    new_episode: bool,
) -> Result<u32> {
    use std::collections::hash_map::Entry;
    match budgets.entry(symbol.to_owned()) {
        Entry::Vacant(entry) => {
            entry.insert(GateioResnapshotBudget {
                resnapshots: 0,
                unsynced_since: tokio::time::Instant::now(),
            });
            Ok(1)
        }
        Entry::Occupied(mut entry) => {
            let budget = entry.get_mut();
            if new_episode {
                budget.unsynced_since = tokio::time::Instant::now();
            }
            if budget.resnapshots >= GATEIO_MAX_RESNAPSHOTS {
                return Err(Error::Parse(
                    "gateio snapshot recovery request limit exceeded".into(),
                ));
            }
            budget.resnapshots += 1;
            Ok(budget.resnapshots + 1)
        }
    }
}

pub(crate) async fn consume_gateio_feed(
    feed: ExchangeFeed,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let plans = GateioAdapter::connection_plans(&feed)?;
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
                    let shutdown = retry_shutdown.clone();
                    async move {
                        // Gate.io requires the subscription timestamp to be
                        // within 60 seconds of server time. Rebuild the plan
                        // for every session instead of replaying stale JSON.
                        let fresh_plan = GateioAdapter::connection_plans(&feed)?
                            .into_iter()
                            .find(|candidate| candidate.product == product)
                            .ok_or_else(|| {
                                Error::InvalidConfiguration(format!(
                                    "missing Gate.io {product:?} connection plan"
                                ))
                            })?;
                        consume_gateio_session(feed, fresh_plan, shutdown).await
                    }
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
                tracing::error!(?product, error = %error, "gateio product feed terminated");
                tasks.abort_all();
                return Err(Error::Transport(format!("Gateio {product:?}: {error}")));
            }
            Err(error) => {
                tasks.abort_all();
                return Err(Error::Transport(format!("Gateio task: {error}")));
            }
        }
    }
    Ok(())
}

async fn consume_gateio_session(
    mut feed: ExchangeFeed,
    plan: GateioConnectionPlan,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let attempt = readiness::begin(&mut feed);
    reset_connection_books(
        &feed,
        !plan.snapshot_urls.is_empty(),
        plan.instruments.iter().map(|instrument| &instrument.symbol),
    );
    let url = Url::parse(&plan.websocket_url).map_err(|e| Error::Transport(e.to_string()))?;
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
    #[cfg(feature = "orderbook")]
    let snapshot_receivers = std::collections::HashMap::new();
    #[cfg(feature = "orderbook")]
    let pending_deltas: std::collections::HashMap<String, Vec<GateioDepthDelta>> =
        std::collections::HashMap::new();
    let result = consume_gateio_session_with(
        feed,
        plan,
        shutdown,
        &mut session,
        #[cfg(feature = "orderbook")]
        snapshot_receivers,
        #[cfg(feature = "orderbook")]
        pending_deltas,
    )
    .await;
    session.record_result(&result);
    result
}

pub(crate) async fn consume_gateio_session_with<S, E>(
    feed: ExchangeFeed,
    plan: GateioConnectionPlan,
    mut shutdown: watch::Receiver<bool>,
    session: &mut Session<S>,
    #[cfg(feature = "orderbook")] mut snapshot_receivers: GateioSnapshotReceivers,
    #[cfg(feature = "orderbook")] mut pending_deltas: std::collections::HashMap<
        String,
        Vec<GateioDepthDelta>,
    >,
) -> Result<()>
where
    S: Sink<Message, Error = E> + Stream<Item = std::result::Result<Message, E>> + Unpin,
    E: Display,
{
    session.queue_subscriptions(plan.subscription_messages.clone());

    #[cfg(feature = "orderbook")]
    let mut resnapshot_attempts = std::collections::HashMap::new();
    loop {
        #[cfg(feature = "orderbook")]
        let next = session
            .next_event_or_shutdown(
                &mut shutdown,
                Some(tokio::time::Instant::now() + std::time::Duration::from_millis(100)),
            )
            .await?;
        #[cfg(feature = "orderbook")]
        if matches!(next, connection::SessionRead::Closed) {
            break;
        }
        #[cfg(not(feature = "orderbook"))]
        let Some(text) = session.next_text_or_shutdown(&mut shutdown).await? else {
            break;
        };
        let received_ts = session.received_timestamp();
        #[cfg(feature = "orderbook")]
        poll_gateio_snapshot_bootstraps_for_plan(
            &feed,
            &plan,
            &mut snapshot_receivers,
            &mut pending_deltas,
            &mut resnapshot_attempts,
        )
        .await?;
        #[cfg(feature = "orderbook")]
        let connection::SessionRead::Text(text) = next else {
            continue;
        };
        session.processing();
        #[cfg(feature = "orderbook")]
        if process_gateio_orderbook_message_for_plan(
            &feed,
            &plan,
            &text,
            received_ts,
            &mut snapshot_receivers,
            &mut pending_deltas,
            &mut resnapshot_attempts,
        )
        .await?
        {
            continue;
        }
        process_gateio_text_message_for_plan(&feed, &plan, &text, received_ts).await?;
    }

    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) async fn dispatch_gateio_event(feed: &ExchangeFeed, event: GateioEvent) -> Result<()> {
    let _ = feed;
    match event {
        #[cfg(feature = "candles")]
        GateioEvent::Candle(candle) => dispatch_candle(feed, candle).await,
        #[cfg(feature = "liquidations")]
        GateioEvent::Liquidation(liquidation) => dispatch_liquidation(feed, liquidation).await,
        #[cfg(feature = "funding")]
        GateioEvent::Funding(funding) => dispatch_funding(feed, funding).await,
        #[cfg(feature = "index")]
        GateioEvent::IndexPrice(index_price) => {
            dispatch_index_price(feed, index_price).await;
        }
        #[cfg(feature = "orderbook")]
        GateioEvent::L2Book(book) => dispatch_l2_book(feed, book).await?,
        #[cfg(feature = "orderbook")]
        GateioEvent::L1Book(book) => dispatch_l1_book(feed, book).await,
        #[cfg(feature = "markprice")]
        GateioEvent::MarkPrice(mark_price) => {
            dispatch_mark_price(feed, mark_price).await;
        }
        #[cfg(feature = "openinterest")]
        GateioEvent::OpenInterest(open_interest) => {
            dispatch_open_interest(feed, open_interest).await;
        }
        #[cfg(feature = "ticker")]
        GateioEvent::Ticker(ticker) => dispatch_ticker(feed, ticker).await,
        #[cfg(feature = "trade")]
        GateioEvent::Trade(trade) => dispatch_trade(feed, trade).await,
    }
    Ok(())
}

#[cfg(feature = "orderbook")]
async fn dispatch_gateio_l2_book(feed: &ExchangeFeed, book: L2Book) -> Result<()> {
    dispatch_l2_book(feed, book).await
}

#[cfg(feature = "orderbook")]
fn spawn_gateio_snapshot_fetch_for_instrument(
    instrument: GateioInstrument,
    snapshot_url: String,
    transport: crate::transport::TransportConfig,
    mode: snapshot::SnapshotMode,
) -> snapshot::SnapshotReceiver<crate::exchange::gateio::book_sync::GateioBookSnapshot> {
    let (tx, rx, raw) = snapshot::SnapshotReceiver::channel(mode);
    let Some(mut tx) = tx else {
        return rx;
    };
    tokio::spawn(async move {
        tokio::select! {
            biased;
            _ = tx.closed() => {}
            result = fetch_gateio_l2_snapshot_for_instrument(instrument, snapshot_url, transport,raw) => {
                let _ = tx.send(result);
            }
        }
    });
    rx
}

#[cfg(feature = "orderbook")]
async fn fetch_gateio_l2_snapshot_for_instrument(
    instrument: GateioInstrument,
    snapshot_url: String,
    transport: crate::transport::TransportConfig,
    raw: Option<snapshot::RawSlot>,
) -> Result<crate::exchange::gateio::book_sync::GateioBookSnapshot> {
    let payload = snapshot::fetch_json(&snapshot_url, &transport, ExchangeId::Gateio).await?;
    let received_ts = current_timestamp();
    let result =
        gateio_parser::parse_l2_book_snapshot_for_instrument(&payload, &instrument, received_ts)
            .ok_or_else(|| Error::Parse("failed to parse gateio l2 snapshot".to_owned()));
    if let Some(raw) = raw {
        *raw.lock().expect("snapshot capture") = Some(snapshot::RawSnapshot {
            payload,
            received_ts,
        });
    }
    result
}

#[cfg(feature = "orderbook")]
pub(crate) async fn poll_gateio_snapshot_bootstraps_for_plan(
    feed: &ExchangeFeed,
    plan: &GateioConnectionPlan,
    receivers: &mut GateioSnapshotReceivers,
    pending: &mut std::collections::HashMap<String, Vec<GateioDepthDelta>>,
    resnapshot_attempts: &mut std::collections::HashMap<String, GateioResnapshotBudget>,
) -> Result<()> {
    use tokio::sync::oneshot::error::TryRecvError;

    let keys: Vec<String> = receivers.keys().cloned().collect();
    for key in keys {
        if !matches!(feed.snapshot_mode(), snapshot::SnapshotMode::Replay)
            && resnapshot_attempts
                .get(&key)
                .is_some_and(|budget| budget.unsynced_since.elapsed() >= GATEIO_BOOTSTRAP_DEADLINE)
        {
            tracing::warn!(exchange="gateio", symbol=%key, reason="snapshot_recovery_deadline", "gateio bootstrap deadline");
            feed.record_book_sync(&key, |d| {
                d.last_reason = "recovery_deadline".into();
                d.last_error = Some("snapshot_recovery_deadline".into());
            });
            return Err(Error::Parse(
                "gateio snapshot recovery deadline exceeded".into(),
            ));
        }
        let ready = match receivers.get_mut(&key) {
            Some(receiver) => match receiver.try_recv() {
                Ok(result) => Some(result),
                Err(TryRecvError::Empty) => None,
                Err(TryRecvError::Closed) => {
                    return Err(Error::Transport(
                        "gateio snapshot bootstrap channel closed".to_owned(),
                    ));
                }
            },
            None => None,
        };
        let Some(result) = ready else {
            continue;
        };
        let instrument = plan
            .instruments
            .iter()
            .find(|instrument| instrument.symbol.as_str() == key)
            .ok_or_else(|| Error::Parse(format!("missing gateio instrument for {key}")))?;
        #[cfg(feature = "recording")]
        if let Some(link) = &feed.raw_session {
            if let Some(raw) = receivers
                .get_mut(&key)
                .and_then(|receiver| receiver.take_raw())
            {
                link.snapshot(
                    instrument.symbol.clone(),
                    None,
                    &raw.payload,
                    raw.received_ts,
                );
            } else if let Err(error) = &result {
                link.snapshot_error(instrument.symbol.clone(), None, error);
            }
        }
        let snapshot = match result {
            Ok(snapshot) => snapshot,
            Err(error) => {
                let code = match &error {
                    Error::HttpStatus { status, .. } => format!("http_status_{status}"),
                    Error::RateLimited { .. } => "rate_limited".into(),
                    _ => "snapshot_request_failed".into(),
                };
                tracing::warn!(exchange="gateio", symbol=%key, reason=%code, "gateio snapshot request failed");
                feed.record_book_sync(&key, |d| {
                    d.last_reason = "snapshot_failed".into();
                    d.last_error = Some(code);
                });
                return Err(error);
            }
        };
        feed.record_book_sync(&key, |d| {
            d.last_snapshot_id = snapshot.last_update_id;
            let at = |seconds| {
                std::time::Duration::try_from_secs_f64(seconds)
                    .ok()
                    .and_then(|duration| std::time::UNIX_EPOCH.checked_add(duration))
            };
            d.last_snapshot_generated_at = at(snapshot.generated_ts);
            d.last_snapshot_updated_at = at(snapshot.update_ts);
        });
        let last_update_id = snapshot.last_update_id;
        let snapshot_generated_ts = snapshot.generated_ts;
        let snapshot_update_ts = snapshot.update_ts;
        let buffered = pending.remove(&key).unwrap_or_default();
        match bootstrap_gateio_book(feed, &key, snapshot, buffered.clone()) {
            Ok(books) => {
                feed.record_book_sync(&key, |d| d.last_reason = "rest_bootstrap_applied".into());
                for book in books {
                    dispatch_gateio_l2_book(feed, book).await?;
                }
                receivers.remove(&key);
                // Keep the session-wide request budget across successful bootstraps.
            }
            Err(error) => {
                feed.record_book_sync(&key, |d| {
                    d.last_reason = "bootstrap_rejected".into();
                    d.last_error = Some(
                        if last_update_id.is_none() {
                            "missing_snapshot_id"
                        } else {
                            "snapshot_bridge_rejected"
                        }
                        .into(),
                    );
                });
                // Clear unusable native state before any budget/deadline exit.
                feed.gateio_book_syncs()
                    .expect("gateio book-sync state")
                    .lock()
                    .expect("gateio sync lock")
                    .remove(&key);
                feed.orderbook_states
                    .lock()
                    .expect("book state lock")
                    .remove(&key);
                // A failed bootstrap (typically buffered deltas that do not
                // bridge the snapshot) triggers a bounded in-session resnapshot
                // instead of tearing the session down. The buffered deltas are
                // preserved so the next snapshot can bridge them; the bridge
                // rule itself is never relaxed.
                // A direct injected/replayed snapshot may have no scheduled
                // request entry yet; account for its initial request first.
                resnapshot_attempts
                    .entry(key.clone())
                    .or_insert_with(|| GateioResnapshotBudget {
                        resnapshots: 0,
                        unsynced_since: tokio::time::Instant::now(),
                    });
                let attempts = resnapshot_attempts.get(&key).expect("budget").resnapshots;
                // Forensics: a warning-level trace records the failing
                // snapshot id and the complete buffered U/u sequence so a live
                // smoke run can capture the evidence required to validate the
                // resnapshot path against a real failure.
                tracing::warn!(
                    exchange = "gateio",
                    symbol = %key,
                    snapshot_id = ?last_update_id,
                    reason = "bootstrap_rejected",
                    snapshot_generated_ts = snapshot_generated_ts,
                    snapshot_update_ts = snapshot_update_ts,
                    buffered_sequences = %buffered
                        .iter()
                        .take(16)
                        .map(|delta| format!("{}..{}", delta.first_update_id, delta.last_update_id))
                        .collect::<Vec<_>>()
                        .join(","),
                    buffered_count = buffered.len(),
                    attempt = attempts + 1,
                    error = %error,
                    "gateio bootstrap failed; scheduling bounded resnapshot"
                );
                if attempts >= GATEIO_MAX_RESNAPSHOTS {
                    tracing::error!(
                        exchange = "gateio",
                        symbol = %key,
                        snapshot_id = ?last_update_id,
                        "gateio bootstrap resnapshot limit exceeded; session retry"
                    );
                    return Err(error);
                }
                let request = gateio_snapshot_budget(resnapshot_attempts, &key, false)?;
                feed.record_book_sync(&key, |d| {
                    d.snapshot_requests = request;
                    d.total_snapshot_requests = d.total_snapshot_requests.saturating_add(1);
                    d.last_reason = "resnapshot_requested".into();
                });
                pending.insert(key.clone(), buffered);
                let snapshot_url = plan
                    .snapshot_urls
                    .get(
                        plan.instruments
                            .iter()
                            .filter(|candidate| feed.subscribes(Channel::L2Book, &candidate.symbol))
                            .position(|candidate| candidate.symbol.as_str() == key)
                            .expect("gateio resnapshot instrument"),
                    )
                    .cloned()
                    .ok_or_else(|| {
                        Error::Parse(format!("missing gateio resnapshot URL for {key}"))
                    })?;
                receivers.remove(&key);
                receivers.insert(
                    key.clone(),
                    spawn_gateio_snapshot_fetch_for_instrument(
                        instrument.clone(),
                        snapshot_url,
                        feed.transport.clone(),
                        feed.snapshot_mode(),
                    ),
                );
            }
        }
    }
    Ok(())
}

#[cfg(feature = "orderbook")]
pub(crate) fn bootstrap_gateio_book(
    feed: &ExchangeFeed,
    symbol_key: &str,
    snapshot: crate::exchange::gateio::book_sync::GateioBookSnapshot,
    buffered: Vec<GateioDepthDelta>,
) -> Result<Vec<cryptofeed_orderbook::L2Book>> {
    let mut syncs = feed
        .gateio_book_syncs()
        .expect("gateio book-sync state")
        .lock()
        .expect("gateio sync lock");
    let sync = syncs
        .entry(symbol_key.to_owned())
        .or_insert_with(|| GateioBookSync::new(snapshot.book.symbol.clone()));
    let events = sync.bootstrap(snapshot, buffered)?;

    let mut states = feed.orderbook_states.lock().expect("orderbook state lock");
    states.insert(symbol_key.to_owned(), sync.state().clone());

    Ok(events)
}

#[cfg(all(feature = "orderbook", test))]
pub(crate) async fn process_gateio_orderbook_message(
    feed: &ExchangeFeed,
    text: &str,
    received_ts: f64,
    receivers: &mut GateioSnapshotReceivers,
    pending: &mut std::collections::HashMap<String, Vec<GateioDepthDelta>>,
) -> Result<bool> {
    let message: Value = serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
    if message.get("channel").and_then(|v| v.as_str()) != Some("spot.order_book_update") {
        return Ok(false);
    }
    if message.get("event").and_then(|v| v.as_str()) != Some("update") {
        return Ok(false);
    }

    let parsed = gateio_parser::parse_l2_book_update(&message, received_ts)
        .ok_or_else(|| Error::Parse("failed to parse gateio depth update".to_owned()))?;
    let update = match parsed {
        GateioBookUpdate::Full {
            snapshot,
            last_update_id,
        } => {
            apply_gateio_full_push(feed, snapshot, last_update_id, receivers, pending).await?;
            return Ok(true);
        }
        GateioBookUpdate::Delta(update) => update,
    };
    let symbol_key = update.book.symbol.as_str().to_owned();

    if feed
        .gateio_book_syncs()
        .expect("gateio book-sync state")
        .lock()
        .expect("gateio sync lock")
        .contains_key(&symbol_key)
    {
        let maybe_book = {
            let mut syncs = feed
                .gateio_book_syncs()
                .expect("gateio book-sync state")
                .lock()
                .expect("gateio sync lock");
            let sync = syncs.get_mut(&symbol_key).expect("gateio sync state");
            sync.apply_next_delta(update.clone())
        };

        match maybe_book {
            Ok(Some(book)) => {
                dispatch_gateio_l2_book(feed, book).await?;
            }
            Ok(None) => {}
            Err(err) => {
                if matches!(err, Error::Parse(_)) {
                    schedule_gateio_resync(feed, &symbol_key, update, receivers, pending);
                } else {
                    return Err(err);
                }
            }
        }
    } else {
        buffer_gateio_delta(pending, &symbol_key, update);
    }

    Ok(true)
}

#[cfg(feature = "orderbook")]
pub(crate) async fn process_gateio_orderbook_message_for_plan(
    feed: &ExchangeFeed,
    plan: &GateioConnectionPlan,
    text: &str,
    received_ts: f64,
    receivers: &mut GateioSnapshotReceivers,
    pending: &mut std::collections::HashMap<String, Vec<GateioDepthDelta>>,
    resnapshot_attempts: &mut std::collections::HashMap<String, GateioResnapshotBudget>,
) -> Result<bool> {
    let message: Value = serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
    let channel = message.get("channel").and_then(Value::as_str);
    if !matches!(
        channel,
        Some("spot.order_book_update" | "futures.order_book_update")
    ) {
        return Ok(false);
    }
    if message.get("event").and_then(Value::as_str) != Some("update") {
        return Ok(false);
    }
    let instrument = GateioAdapter::instrument_for_message(&message, &plan.instruments)
        .ok_or_else(|| Error::Parse("gateio message instrument is not in the plan".to_owned()))?;
    if !subscribed_to(feed, Channel::L2Book, &instrument.symbol) {
        return Ok(true);
    }
    let parsed =
        gateio_parser::parse_l2_book_update_for_instrument(&message, received_ts, instrument)
            .ok_or_else(|| Error::Parse("failed to parse gateio depth update".to_owned()))?;
    let update = match parsed {
        GateioBookUpdate::Full {
            snapshot,
            last_update_id,
        } => {
            apply_gateio_full_push(feed, snapshot, last_update_id, receivers, pending).await?;
            return Ok(true);
        }
        GateioBookUpdate::Delta(update) => update,
    };
    let symbol_key = update.book.symbol.as_str().to_owned();
    feed.record_book_sync(&symbol_key, |d| {
        d.last_delta_first = Some(update.first_update_id);
        d.last_delta_last = Some(update.last_update_id);
    });

    if feed
        .gateio_book_syncs()
        .expect("gateio book-sync state")
        .lock()
        .expect("gateio sync lock")
        .contains_key(&symbol_key)
    {
        let (maybe_book, anchor) = {
            let mut syncs = feed
                .gateio_book_syncs()
                .expect("gateio book-sync state")
                .lock()
                .expect("gateio sync lock");
            let sync = syncs.get_mut(&symbol_key).expect("gateio sync state");
            let anchor = sync.last_update_id();
            (sync.apply_next_delta(update.clone()), anchor)
        };
        match maybe_book {
            Ok(Some(book)) => dispatch_gateio_l2_book(feed, book).await?,
            Ok(None) => {}
            Err(Error::Parse(_)) => {
                feed.record_book_sync(&symbol_key, |d| {
                    d.last_reason = "sequence_resync".into();
                    d.last_error = Some("sequence_gap_or_invalid_range".into());
                    d.last_gap_anchor = anchor;
                    d.last_gap_first = Some(update.first_update_id);
                    d.last_gap_last = Some(update.last_update_id);
                });
                tracing::warn!(exchange="gateio", symbol=%symbol_key, reason="sequence_gap_or_invalid_range", previous_update_id=?anchor, first_update_id=update.first_update_id, last_update_id=update.last_update_id, "gateio sequence resync");
                schedule_gateio_resync_for_plan(
                    feed,
                    plan,
                    instrument,
                    update,
                    receivers,
                    pending,
                    resnapshot_attempts,
                )?;
            }
            Err(error) => return Err(error),
        }
    } else if receivers.contains_key(&symbol_key) {
        buffer_gateio_delta(pending, &symbol_key, update);
    } else {
        // Start REST only after this symbol's WS delta is buffered. Paced
        // subscription queues must not fetch a snapshot before subscribing.
        schedule_gateio_resync_for_plan(
            feed,
            plan,
            instrument,
            update,
            receivers,
            pending,
            resnapshot_attempts,
        )?;
    }
    Ok(true)
}

/// Applies a Gate.io `full: true` push: the push is the complete book, so
/// the local sync is re-anchored at the push's `u` and any in-flight
/// bootstrap work for the symbol is superseded.
#[cfg(feature = "orderbook")]
async fn apply_gateio_full_push(
    feed: &ExchangeFeed,
    snapshot: cryptofeed_orderbook::L2BookSnapshot,
    last_update_id: u64,
    receivers: &mut GateioSnapshotReceivers,
    pending: &mut std::collections::HashMap<String, Vec<GateioDepthDelta>>,
) -> Result<()> {
    let symbol_key = snapshot.symbol.as_str().to_owned();
    feed.record_book_sync(&symbol_key, |d| {
        d.last_reason = "full_push_applied".into();
        d.last_delta_first = None;
        d.last_delta_last = Some(last_update_id);
    });
    {
        let mut syncs = feed
            .gateio_book_syncs()
            .expect("gateio book-sync state")
            .lock()
            .expect("gateio sync lock");
        let sync = syncs
            .entry(symbol_key.clone())
            .or_insert_with(|| GateioBookSync::new(snapshot.symbol.clone()));
        sync.reset_with_snapshot(snapshot.clone(), last_update_id);
        let mut states = feed.orderbook_states.lock().expect("orderbook state lock");
        states.insert(symbol_key.clone(), sync.state().clone());
    }
    receivers.remove(&symbol_key);
    pending.remove(&symbol_key);
    dispatch_gateio_l2_book(feed, L2Book::Snapshot(snapshot)).await?;
    Ok(())
}

#[cfg(feature = "orderbook")]
fn schedule_gateio_resync_for_plan(
    feed: &ExchangeFeed,
    plan: &GateioConnectionPlan,
    instrument: &GateioInstrument,
    update: GateioDepthDelta,
    receivers: &mut GateioSnapshotReceivers,
    pending: &mut std::collections::HashMap<String, Vec<GateioDepthDelta>>,
    resnapshot_attempts: &mut std::collections::HashMap<String, GateioResnapshotBudget>,
) -> Result<()> {
    let symbol_key = update.book.symbol.as_str().to_owned();
    reset_gateio_sync_state(feed, &symbol_key, update, pending);
    let request =
        gateio_snapshot_budget(resnapshot_attempts, &symbol_key, true).inspect_err(|_| {
            tracing::warn!(exchange="gateio",symbol=%symbol_key,reason="snapshot_request_limit","gateio snapshot budget exhausted");
            feed.record_book_sync(&symbol_key, |d| {
                d.last_reason = "request_budget_exhausted".into();
                d.last_error = Some("snapshot_request_limit".into());
            });
        })?;
    feed.record_book_sync(&symbol_key, |d| {
        d.snapshot_requests = request;
        d.total_snapshot_requests = d.total_snapshot_requests.saturating_add(1);
        d.last_reason = "snapshot_requested".into();
    });
    let index = plan
        .instruments
        .iter()
        .filter(|candidate| feed.subscribes(Channel::L2Book, &candidate.symbol))
        .position(|candidate| candidate == instrument)
        .ok_or_else(|| Error::Parse("missing gateio resync instrument".to_owned()))?;
    let snapshot_url = plan
        .snapshot_urls
        .get(index)
        .cloned()
        .ok_or_else(|| Error::Parse("missing gateio resync snapshot URL".to_owned()))?;
    receivers.remove(&symbol_key);
    receivers.insert(
        symbol_key.to_owned(),
        spawn_gateio_snapshot_fetch_for_instrument(
            instrument.clone(),
            snapshot_url,
            feed.transport.clone(),
            feed.snapshot_mode(),
        ),
    );
    Ok(())
}

#[cfg(feature = "orderbook")]
fn reset_gateio_sync_state(
    feed: &ExchangeFeed,
    symbol_key: &str,
    update: GateioDepthDelta,
    pending: &mut std::collections::HashMap<String, Vec<GateioDepthDelta>>,
) {
    feed.invalidate_book_readiness(symbol_key);
    feed.orderbook_states
        .lock()
        .expect("book state lock")
        .remove(symbol_key);
    feed.gateio_book_syncs()
        .expect("gateio book-sync state")
        .lock()
        .expect("gateio sync lock")
        .remove(symbol_key);
    buffer_gateio_delta(pending, symbol_key, update);
}

#[cfg(feature = "orderbook")]
fn buffer_gateio_delta(
    pending: &mut std::collections::HashMap<String, Vec<GateioDepthDelta>>,
    symbol_key: &str,
    update: GateioDepthDelta,
) {
    let queue = pending.entry(symbol_key.to_owned()).or_default();
    if queue.len() >= MAX_BUFFERED_DELTAS_PER_SYMBOL {
        queue.remove(0);
    }
    queue.push(update);
}

#[cfg(all(feature = "orderbook", test))]
fn schedule_gateio_resync(
    feed: &ExchangeFeed,
    symbol_key: &str,
    update: GateioDepthDelta,
    receivers: &mut GateioSnapshotReceivers,
    pending: &mut std::collections::HashMap<String, Vec<GateioDepthDelta>>,
) {
    reset_gateio_sync_state(feed, symbol_key, update, pending);
    if let Some(instrument) = GateioAdapter::instruments(feed)
        .ok()
        .and_then(|instruments| {
            instruments
                .into_iter()
                .find(|instrument| instrument.symbol.as_str() == symbol_key)
        })
    {
        let snapshot_url = GateioAdapter::snapshot_url(&instrument, 100);
        receivers.remove(symbol_key);
        receivers.insert(
            symbol_key.to_owned(),
            spawn_gateio_snapshot_fetch_for_instrument(
                instrument,
                snapshot_url,
                feed.transport.clone(),
                feed.snapshot_mode(),
            ),
        );
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) async fn process_gateio_text_message(
    feed: &ExchangeFeed,
    text: &str,
    received_ts: f64,
) -> Result<()> {
    let message: Value = serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
    if let Some(error) = quota::websocket_error(feed.exchange, &message) {
        return Err(error);
    }
    if let Some(detail) = gateio_error_detail(&message) {
        // A rejected subscription is a permanent configuration problem;
        // `Subscription` errors fail fast instead of burning the retries.
        return Err(Error::Subscription(format!(
            "gateio websocket error: {detail}"
        )));
    }
    for event in GateioAdapter::parse_messages(&message, received_ts) {
        dispatch_gateio_event(feed, event).await?;
    }
    Ok(())
}

pub(crate) async fn process_gateio_text_message_for_plan(
    feed: &ExchangeFeed,
    plan: &GateioConnectionPlan,
    text: &str,
    received_ts: f64,
) -> Result<()> {
    let message: Value = serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
    if let Some(error) = quota::websocket_error(feed.exchange, &message) {
        return Err(error);
    }
    if let Some(detail) = gateio_error_detail(&message) {
        // A rejected subscription is a permanent configuration problem;
        // `Subscription` errors fail fast instead of burning the retries.
        return Err(Error::Subscription(format!(
            "gateio websocket error: {detail}"
        )));
    }
    if message.get("event").and_then(Value::as_str) == Some("subscribe") {
        return Ok(());
    }
    // Public liquidation batches may contain several contracts. Resolve each
    // row independently instead of binding the whole batch to its first row.
    #[cfg(feature = "liquidations")]
    if message.get("channel").and_then(Value::as_str) == Some("futures.public_liquidates") {
        if let Some(rows) = message.get("result").and_then(Value::as_array) {
            for row in rows {
                let mut single = message.clone();
                single["result"] = Value::Array(vec![row.clone()]);
                if let Some(instrument) =
                    GateioAdapter::instrument_for_message(&single, &plan.instruments)
                {
                    for event in GateioAdapter::parse_messages_for_instrument(
                        &single,
                        received_ts,
                        instrument,
                    ) {
                        dispatch_gateio_event(feed, event).await?;
                    }
                }
            }
        }
        return Ok(());
    }
    let instrument = GateioAdapter::instrument_for_message(&message, &plan.instruments)
        .ok_or_else(|| Error::Parse("gateio message instrument is not in the plan".to_owned()))?;
    for event in GateioAdapter::parse_messages_for_instrument(&message, received_ts, instrument) {
        dispatch_gateio_event(feed, event).await?;
    }
    Ok(())
}

pub(crate) fn gateio_error_detail(message: &Value) -> Option<&str> {
    let error = message.get("error");
    let rejected = error.is_some_and(|value| !value.is_null())
        || message.get("event").and_then(Value::as_str) == Some("error");
    rejected.then(|| {
        error
            .and_then(|value| value.get("message"))
            .or_else(|| message.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("subscription rejected")
    })
}
