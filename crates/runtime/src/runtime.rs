pub mod connection;
pub mod supervisor;

#[cfg(all(feature = "orderbook", test))]
use crate::exchange::binance::book_sync::BinanceDepthDelta;
#[cfg(feature = "orderbook")]
use crate::exchange::gateio::adapter::GateioInstrument;
use crate::exchange::{
    ExchangeFeed,
    binance::adapter::{BinanceAdapter, BinanceConnectionPlan, BinanceEvent, BinanceInstrument},
    bitget::adapter::{BitgetAdapter, BitgetEvent},
    bybit::adapter::{BybitAdapter, BybitEvent, BybitProduct},
    gateio::adapter::{GateioAdapter, GateioConnectionPlan, GateioEvent},
    okx::adapter::{OkxAdapter, OkxEvent},
};
#[cfg(feature = "orderbook")]
use crate::exchange::{
    binance::{
        book_sync::{BinanceBookSync, BinanceSequencedDepthDelta},
        parser as binance_parser,
    },
    bitget::book_sync::{BitgetBookAction, BitgetBookSync, BitgetDepthUpdate},
    bybit::book_sync::BybitBookSync,
    gateio::{
        book_sync::{GateioBookSync, GateioBookUpdate, GateioDepthDelta},
        parser as gateio_parser,
    },
    okx::{book_sync::OkxBookSync, parser as okx_parser},
};
use crate::feed::FeedHandler;
use crate::markets;
use crate::runtime::connection::Session;
use cryptofeed_core::{
    error::{Error, Result},
    exchange::{Channel, ExchangeId},
    symbol::InstrumentKind,
};
#[cfg(feature = "orderbook")]
use cryptofeed_orderbook::L2Book;
use futures::{Sink, Stream};
use serde_json::Value;
use std::fmt::Display;
use std::future::Future;
#[cfg(feature = "orderbook")]
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::Message;
use url::Url;

#[cfg(feature = "orderbook")]
type SnapshotReceivers = std::collections::HashMap<
    String,
    oneshot::Receiver<Result<(u64, cryptofeed_orderbook::L2BookSnapshot)>>,
>;
#[cfg(feature = "orderbook")]
type GateioSnapshotReceivers = std::collections::HashMap<
    String,
    oneshot::Receiver<Result<crate::exchange::gateio::book_sync::GateioBookSnapshot>>,
>;

#[cfg(test)]
const SHUTDOWN_GRACE_PERIOD: std::time::Duration = std::time::Duration::from_millis(100);
#[cfg(not(test))]
const SHUTDOWN_GRACE_PERIOD: std::time::Duration = std::time::Duration::from_secs(5);
const HANDLER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// In-session Gate.io snapshot bootstrap retries per symbol. A failed
/// bootstrap (e.g. buffered deltas that cannot bridge the snapshot) re-fetches
/// the snapshot up to this many times before the session-level retry takes
/// over as the backstop. Sequence validation is never weakened.
#[cfg(feature = "orderbook")]
const GATEIO_MAX_RESNAPSHOTS: u32 = 3;
#[cfg(feature = "orderbook")]
const BINANCE_MAX_RESNAPSHOTS: u32 = 3;
#[cfg(feature = "orderbook")]
const MAX_BUFFERED_DELTAS_PER_SYMBOL: usize = 512;
/// REST snapshot fetches must not hang the session: a stalled request would
/// otherwise leak the fetch task and grow the pending-delta buffer forever.
const SNAPSHOT_HTTP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);
#[cfg(feature = "orderbook")]
const MAX_SNAPSHOT_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

/// Runs the feeds until Ctrl-C triggers a clean shutdown.
pub async fn run(handler: FeedHandler) -> Result<()> {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = shutdown_tx.send(true);
    });
    run_with_shutdown(handler, shutdown_rx).await
}

/// Runs the feeds until `shutdown` is set, or a feed fails terminally.
/// Library users embedding `FeedHandler` in a service can drive the
/// `watch::Receiver` themselves instead of relying on Ctrl-C.
pub async fn run_with_shutdown(
    handler: FeedHandler,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    if *shutdown.borrow() {
        return Ok(());
    }
    let hydration = hydrate_feed_symbols(handler.into_feeds());
    tokio::pin!(hydration);
    let feeds = loop {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                match changed {
                    Ok(()) if !*shutdown.borrow() => continue,
                    Ok(()) | Err(_) => return Ok(()),
                }
            }
            result = &mut hydration => break result?,
        }
    };
    run_feeds_until_shutdown(feeds, shutdown, |feed, shutdown| async move {
        match feed.exchange {
            ExchangeId::Binance => consume_binance_feed(feed, shutdown).await,
            ExchangeId::Bitget => consume_bitget_feed(feed, shutdown).await,
            ExchangeId::Bybit => consume_bybit_feed(feed, shutdown).await,
            ExchangeId::Okx => consume_okx_feed(feed, shutdown).await,
            ExchangeId::Gateio => consume_gateio_feed(feed, shutdown).await,
            // Builders that exist without a live runtime (Coinbase/Kraken)
            // and any future exchange are rejected by capability validation
            // before a feed reaches this point.
            _ => Ok(()),
        }
    })
    .await?;
    Ok(())
}

async fn hydrate_feed_symbols(feeds: Vec<ExchangeFeed>) -> Result<Vec<ExchangeFeed>> {
    // Catalogs are fetched concurrently: with several feeds (e.g. the
    // multi-megabyte Binance exchangeInfo plus Bybit pages), resolving
    // sequentially would stall every feed on the slowest endpoint. All
    // feeds still fail together if any resolution fails.
    let mut hydrated: Vec<Result<ExchangeFeed>> =
        futures::future::join_all(feeds.into_iter().map(|mut feed| async move {
            feed.exchange_symbols = markets::resolve_feed_symbols(&feed).await?;
            Ok(feed)
        }))
        .await;
    let mut result = Vec::with_capacity(hydrated.len());
    for feed in hydrated.drain(..) {
        result.push(feed?);
    }
    Ok(result)
}

#[cfg_attr(not(test), allow(dead_code))]
fn planned_connection_urls(handler: &FeedHandler) -> Vec<String> {
    handler.feeds().iter().map(planned_url).collect()
}

fn planned_url(feed: &ExchangeFeed) -> String {
    match feed.exchange {
        ExchangeId::Binance => BinanceAdapter::connection_plans(feed)
            .ok()
            .and_then(|plans| plans.into_iter().next())
            .map_or_else(String::new, |plan| plan.websocket_url),
        ExchangeId::Bitget => BitgetAdapter::subscription_url(feed),
        ExchangeId::Bybit => BybitAdapter::subscription_url(feed),
        ExchangeId::Coinbase => String::new(),
        ExchangeId::Gateio => GateioAdapter::connection_plans(feed)
            .ok()
            .and_then(|plans| plans.into_iter().next())
            .map_or_else(String::new, |plan| plan.websocket_url),
        ExchangeId::Kraken => String::new(),
        ExchangeId::Okx => OkxAdapter::subscription_url(feed),
        _ => String::new(),
    }
}

async fn run_feeds_until_shutdown<F, Fut>(
    feeds: Vec<ExchangeFeed>,
    mut shutdown: watch::Receiver<bool>,
    consume: F,
) -> Result<()>
where
    F: Fn(ExchangeFeed, watch::Receiver<bool>) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    let status_sender = feeds.iter().find_map(|feed| feed.status_sender.clone());
    let mut tasks = JoinSet::new();
    let mut terminal_errors = Vec::new();

    for feed in feeds {
        let consume = consume.clone();
        let exchange = feed.exchange;
        let feed_shutdown = shutdown.clone();
        tasks.spawn(async move { (exchange, consume(feed, feed_shutdown).await) });
    }

    while !tasks.is_empty() {
        tokio::select! {
            changed = shutdown.changed() => {
                match changed {
                    Ok(()) if *shutdown.borrow() => {
                        let drain = async {
                            while let Some(result) = tasks.join_next().await {
                                record_feed_result(result, &mut terminal_errors, status_sender.as_ref());
                            }
                        };
                        if tokio::time::timeout(SHUTDOWN_GRACE_PERIOD, drain).await.is_err() {
                            tasks.abort_all();
                        }
                        return terminal_result(terminal_errors);
                    }
                    Ok(()) => {}
                    Err(_) => {
                        let drain = async {
                            while let Some(result) = tasks.join_next().await {
                                record_feed_result(result, &mut terminal_errors, status_sender.as_ref());
                            }
                        };
                        if tokio::time::timeout(SHUTDOWN_GRACE_PERIOD, drain).await.is_err() {
                            tasks.abort_all();
                        }
                        return terminal_result(terminal_errors);
                    }
                }
            }
            result = tasks.join_next() => {
                if let Some(result) = result {
                    record_feed_result(result, &mut terminal_errors, status_sender.as_ref());
                }
            }
        }
    }

    terminal_result(terminal_errors)
}

fn record_feed_result(
    result: std::result::Result<(ExchangeId, Result<()>), tokio::task::JoinError>,
    terminal_errors: &mut Vec<String>,
    status_sender: Option<&tokio::sync::broadcast::Sender<crate::feed::FeedStatus>>,
) {
    match result {
        Ok((_, Ok(()))) => {}
        Ok((exchange, Err(error))) => {
            let message = format!("{exchange:?}: {error}");
            tracing::error!(exchange = ?exchange, error = %error, "feed terminated after retry exhaustion");
            if let Some(sender) = status_sender {
                let _ = sender.send(crate::feed::FeedStatus::Terminated {
                    exchange,
                    error: error.to_string(),
                });
            }
            terminal_errors.push(message);
        }
        Err(error) => {
            tracing::error!(error = %error, "feed task terminated unexpectedly");
            if let Some(sender) = status_sender {
                let _ = sender.send(crate::feed::FeedStatus::TaskPanicked {
                    error: error.to_string(),
                });
            }
            terminal_errors.push(format!("feed task: {error}"));
        }
    }
}

fn terminal_result(errors: Vec<String>) -> Result<()> {
    if errors.is_empty() {
        Ok(())
    } else {
        Err(Error::Transport(format!(
            "terminal feed failures: {}",
            errors.join("; ")
        )))
    }
}

async fn consume_binance_feed(feed: ExchangeFeed, shutdown: watch::Receiver<bool>) -> Result<()> {
    let plans = BinanceAdapter::connection_plans(&feed)?;
    let mut tasks = JoinSet::new();
    for plan in plans {
        let feed = feed.clone();
        let retry_shutdown = shutdown.clone();
        tasks.spawn(async move {
            let product = plan.product;
            let result = supervisor::retry_with_backoff_until_shutdown(
                None,
                supervisor::Backoff::new(1, 8),
                retry_shutdown.clone(),
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

async fn consume_bitget_feed(feed: ExchangeFeed, shutdown: watch::Receiver<bool>) -> Result<()> {
    supervisor::retry_with_backoff_until_shutdown(
        None,
        supervisor::Backoff::new(1, 8),
        shutdown.clone(),
        move || {
            let feed = feed.clone();
            let shutdown = shutdown.clone();
            async move { consume_bitget_session(feed, shutdown).await }
        },
    )
    .await
    .map(|_| ())
}

async fn consume_bybit_feed(feed: ExchangeFeed, shutdown: watch::Receiver<bool>) -> Result<()> {
    let mut tasks = JoinSet::new();
    for url in BybitAdapter::subscription_urls(&feed) {
        let planned_feed = bybit_feed_for_url(&feed, &url);
        let retry_shutdown = shutdown.clone();
        tasks.spawn(async move {
            supervisor::retry_with_backoff_until_shutdown(
                None,
                supervisor::Backoff::new(1, 8),
                retry_shutdown.clone(),
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

async fn consume_okx_feed(feed: ExchangeFeed, shutdown: watch::Receiver<bool>) -> Result<()> {
    let mut tasks = JoinSet::new();
    for url in OkxAdapter::subscription_urls(&feed) {
        let mut planned_feed = feed.clone();
        let business = url.ends_with("/business");
        planned_feed.channels.retain(|channel| {
            matches!(channel, cryptofeed_core::exchange::Channel::Candles) == business
        });
        let retry_shutdown = shutdown.clone();
        tasks.spawn(async move {
            supervisor::retry_with_backoff_until_shutdown(
                None,
                supervisor::Backoff::new(1, 8),
                retry_shutdown.clone(),
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

async fn collect_connection_results(label: &str, mut tasks: JoinSet<Result<()>>) -> Result<()> {
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::error!(feed = label, error = %error, "connection plan terminated");
                tasks.abort_all();
                return Err(Error::Transport(format!("{label}: {error}")));
            }
            Err(error) => {
                tasks.abort_all();
                return Err(Error::Transport(format!("{label} task: {error}")));
            }
        }
    }
    Ok(())
}

fn bybit_feed_for_url(feed: &ExchangeFeed, url: &str) -> ExchangeFeed {
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
    planned
}

async fn consume_gateio_feed(feed: ExchangeFeed, shutdown: watch::Receiver<bool>) -> Result<()> {
    let plans = GateioAdapter::connection_plans(&feed)?;
    let mut tasks = JoinSet::new();
    for plan in plans {
        let feed = feed.clone();
        let retry_shutdown = shutdown.clone();
        tasks.spawn(async move {
            let product = plan.product;
            let result = supervisor::retry_with_backoff_until_shutdown(
                None,
                supervisor::Backoff::new(1, 8),
                retry_shutdown.clone(),
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

async fn consume_binance_session(
    feed: ExchangeFeed,
    plan: BinanceConnectionPlan,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let url = Url::parse(&plan.websocket_url).map_err(|e| Error::Transport(e.to_string()))?;
    let connection = connection::WsConnection::new(url, feed.exchange);
    let mut session = connection.connect().await?;
    #[cfg(feature = "orderbook")]
    let snapshot_receivers = if plan.snapshot_urls.is_empty() {
        std::collections::HashMap::new()
    } else {
        spawn_binance_snapshot_fetches(&plan.instruments, plan.l2_book_depth.unwrap_or(1000))
    };
    #[cfg(feature = "orderbook")]
    let pending_deltas: std::collections::HashMap<String, Vec<BinanceSequencedDepthDelta>> =
        std::collections::HashMap::new();
    #[cfg(feature = "orderbook")]
    let resnapshot_attempts: std::collections::HashMap<String, u32> =
        std::collections::HashMap::new();
    consume_binance_session_with(
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
    .await
}

async fn consume_binance_session_with<S, E>(
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
        let handled = process_binance_orderbook_message(
            &feed,
            &plan,
            &text,
            current_timestamp(),
            &mut snapshot_receivers,
            &mut pending_deltas,
        )
        .await?;
        #[cfg(not(feature = "orderbook"))]
        let handled = false;
        if !handled {
            process_binance_text_message_for_plan(&feed, &plan, &text, current_timestamp()).await?;
        }
    }

    Ok(())
}

async fn consume_bitget_session(feed: ExchangeFeed, shutdown: watch::Receiver<bool>) -> Result<()> {
    let url = Url::parse(&planned_url(&feed)).map_err(|e| Error::Transport(e.to_string()))?;
    let connection = connection::WsConnection::new(url, feed.exchange);
    let mut session = connection.connect().await?;
    consume_bitget_session_with(feed, shutdown, &mut session).await
}

async fn consume_bitget_session_with<S, E>(
    feed: ExchangeFeed,
    mut shutdown: watch::Receiver<bool>,
    session: &mut Session<S>,
) -> Result<()>
where
    S: Sink<Message, Error = E> + Stream<Item = std::result::Result<Message, E>> + Unpin,
    E: Display,
{
    let subscribe = BitgetAdapter::subscription_message(&feed);
    session.send_text(&subscribe).await?;

    while let Some(text) = session.next_text_or_shutdown(&mut shutdown).await? {
        process_bitget_text_message(&feed, &text, current_timestamp()).await?;
    }

    Ok(())
}

async fn consume_bybit_session(
    feed: ExchangeFeed,
    websocket_url: String,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let url = Url::parse(&websocket_url).map_err(|e| Error::Transport(e.to_string()))?;
    let connection = connection::WsConnection::new(url, feed.exchange);
    let mut session = connection.connect().await?;
    consume_bybit_session_with(feed, shutdown, &mut session).await
}

async fn consume_bybit_session_with<S, E>(
    feed: ExchangeFeed,
    mut shutdown: watch::Receiver<bool>,
    session: &mut Session<S>,
) -> Result<()>
where
    S: Sink<Message, Error = E> + Stream<Item = std::result::Result<Message, E>> + Unpin,
    E: Display,
{
    let subscribe = BybitAdapter::subscription_message(&feed);
    session.send_text(&subscribe).await?;

    // Ticker deltas carry only changed fields. State is per connection, so a
    // reconnect cannot reuse values from a previous session.
    let mut tickers = std::collections::HashMap::new();
    while let Some(text) = session.next_text_or_shutdown(&mut shutdown).await? {
        let received_ts = current_timestamp();
        let mut message: Value =
            serde_json::from_str(&text).map_err(|e| Error::Parse(e.to_string()))?;
        merge_bybit_ticker_message(&feed, &mut tickers, &mut message)?;
        process_bybit_message(&feed, &message, received_ts).await?;
    }

    Ok(())
}

async fn consume_okx_session(
    feed: ExchangeFeed,
    websocket_url: String,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let url = Url::parse(&websocket_url).map_err(|e| Error::Transport(e.to_string()))?;
    let connection = connection::WsConnection::new(url, feed.exchange);
    let mut session = connection.connect().await?;
    consume_okx_session_with(feed, shutdown, &mut session).await
}

async fn consume_okx_session_with<S, E>(
    feed: ExchangeFeed,
    mut shutdown: watch::Receiver<bool>,
    session: &mut Session<S>,
) -> Result<()>
where
    S: Sink<Message, Error = E> + Stream<Item = std::result::Result<Message, E>> + Unpin,
    E: Display,
{
    let subscribe = OkxAdapter::subscription_message(&feed);
    session.send_text(&subscribe).await?;

    while let Some(text) = session.next_text_or_shutdown(&mut shutdown).await? {
        process_okx_text_message(&feed, &text, current_timestamp()).await?;
    }

    Ok(())
}

async fn consume_gateio_session(
    feed: ExchangeFeed,
    plan: GateioConnectionPlan,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let url = Url::parse(&plan.websocket_url).map_err(|e| Error::Transport(e.to_string()))?;
    let connection = connection::WsConnection::new(url, feed.exchange);
    let mut session = connection.connect().await?;
    #[cfg(feature = "orderbook")]
    let snapshot_receivers = spawn_gateio_snapshot_fetches_for_plan(&plan)?;
    #[cfg(feature = "orderbook")]
    let pending_deltas: std::collections::HashMap<String, Vec<GateioDepthDelta>> =
        std::collections::HashMap::new();
    consume_gateio_session_with(
        feed,
        plan,
        shutdown,
        &mut session,
        #[cfg(feature = "orderbook")]
        snapshot_receivers,
        #[cfg(feature = "orderbook")]
        pending_deltas,
    )
    .await
}

async fn consume_gateio_session_with<S, E>(
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
    for subscribe in &plan.subscription_messages {
        session.send_text(subscribe).await?;
    }

    #[cfg(feature = "orderbook")]
    let mut resnapshot_attempts = std::collections::HashMap::new();
    while let Some(text) = session.next_text_or_shutdown(&mut shutdown).await? {
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
        if process_gateio_orderbook_message_for_plan(
            &feed,
            &plan,
            &text,
            current_timestamp(),
            &mut snapshot_receivers,
            &mut pending_deltas,
        )
        .await?
        {
            continue;
        }
        process_gateio_text_message_for_plan(&feed, &plan, &text, current_timestamp()).await?;
    }

    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
async fn dispatch_binance_event(feed: &ExchangeFeed, event: BinanceEvent) {
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
        BinanceEvent::L2Book(book) => dispatch_l2_book(feed, book).await,
        #[cfg(feature = "openinterest")]
        BinanceEvent::OpenInterest(open_interest) => {
            dispatch_open_interest(feed, open_interest).await;
        }
        #[cfg(feature = "ticker")]
        BinanceEvent::Ticker(ticker) => dispatch_ticker(feed, ticker).await,
        #[cfg(feature = "trade")]
        BinanceEvent::Trade(trade) => dispatch_trade(feed, trade).await,
    }
}

#[cfg_attr(not(test), allow(dead_code))]
async fn dispatch_bitget_event(feed: &ExchangeFeed, event: BitgetEvent) {
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
            dispatch_bitget_l2_book(feed, book).await;
        }
        #[cfg(feature = "ticker")]
        BitgetEvent::Ticker(ticker) => dispatch_ticker(feed, ticker).await,
        #[cfg(feature = "trade")]
        BitgetEvent::Trade(trade) => dispatch_trade(feed, trade).await,
    }
}

#[cfg(feature = "orderbook")]
async fn dispatch_bitget_l2_book(feed: &ExchangeFeed, book: L2Book) {
    dispatch_l2_book(feed, book).await;
}

#[cfg_attr(not(test), allow(dead_code))]
async fn dispatch_bybit_event(feed: &ExchangeFeed, event: BybitEvent) {
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
            dispatch_bybit_l2_book(feed, book).await;
        }
        #[cfg(feature = "ticker")]
        BybitEvent::Ticker(ticker) => dispatch_ticker(feed, ticker).await,
        #[cfg(feature = "trade")]
        BybitEvent::Trade(trade) => dispatch_trade(feed, trade).await,
    }
}

#[cfg(feature = "orderbook")]
async fn dispatch_bybit_l2_book(feed: &ExchangeFeed, book: L2Book) {
    dispatch_l2_book(feed, book).await;
}

#[cfg_attr(not(test), allow(dead_code))]
async fn dispatch_okx_event(feed: &ExchangeFeed, event: OkxEvent) {
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
            dispatch_okx_l2_book(feed, book).await;
        }
        #[cfg(feature = "ticker")]
        OkxEvent::Ticker(ticker) => dispatch_ticker(feed, ticker).await,
        #[cfg(feature = "trade")]
        OkxEvent::Trade(trade) => dispatch_trade(feed, trade).await,
    }
}

#[cfg_attr(not(test), allow(dead_code))]
async fn dispatch_gateio_event(feed: &ExchangeFeed, event: GateioEvent) {
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
        GateioEvent::L2Book(book) => dispatch_l2_book(feed, book).await,
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
}

#[cfg(feature = "orderbook")]
async fn dispatch_gateio_l2_book(feed: &ExchangeFeed, book: L2Book) {
    dispatch_l2_book(feed, book).await;
}

#[cfg(feature = "orderbook")]
async fn dispatch_okx_l2_book(feed: &ExchangeFeed, book: L2Book) {
    dispatch_l2_book(feed, book).await;
}

#[cfg_attr(not(test), allow(dead_code))]
async fn process_binance_text_message(
    feed: &ExchangeFeed,
    text: &str,
    received_ts: f64,
) -> Result<()> {
    let message: Value = serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
    for event in BinanceAdapter::parse_messages(&message, received_ts) {
        dispatch_binance_event(feed, event).await;
    }
    Ok(())
}

async fn process_binance_text_message_for_plan(
    feed: &ExchangeFeed,
    plan: &BinanceConnectionPlan,
    text: &str,
    received_ts: f64,
) -> Result<()> {
    let message: Value = serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
    let instrument = binance_instrument_for_message(plan, &message)?;
    for event in BinanceAdapter::parse_messages_for_instrument(&message, received_ts, instrument) {
        dispatch_binance_event(feed, event).await;
    }
    Ok(())
}

fn binance_instrument_for_message<'a>(
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
fn apply_orderbook_state(feed: &ExchangeFeed, book: &cryptofeed_orderbook::L2Book) {
    use cryptofeed_orderbook::L2Book;

    let symbol = match book {
        L2Book::Snapshot(snapshot) => snapshot.symbol.as_str().to_owned(),
        L2Book::Delta(delta) => delta.symbol.as_str().to_owned(),
    };

    let mut states = feed.orderbook_states.lock().expect("orderbook state lock");
    let state = states.entry(symbol.clone()).or_insert_with(|| {
        cryptofeed_orderbook::L2BookState::new(match book {
            L2Book::Snapshot(snapshot) => snapshot.symbol.clone(),
            L2Book::Delta(delta) => delta.symbol.clone(),
        })
    });
    state.apply(book.clone());
}

#[cfg(feature = "orderbook")]
async fn dispatch_binance_l2_book(feed: &ExchangeFeed, book: L2Book) {
    dispatch_l2_book(feed, book).await;
}

#[cfg(feature = "candles")]
async fn dispatch_candle(feed: &ExchangeFeed, candle: cryptofeed_candles::Candle) {
    if !subscribed_to(feed, Channel::Candles, &candle.symbol) {
        return;
    }
    feed.publish_event(crate::feed::FeedEvent::Candle(candle.clone()));
    if let Some(handler) = &feed.candle_handler {
        await_handler("candle", handler.on_candle(candle)).await;
    }
}

#[cfg(feature = "funding")]
async fn dispatch_funding(feed: &ExchangeFeed, funding: cryptofeed_funding::Funding) {
    if !subscribed_to(feed, Channel::Funding, &funding.symbol) {
        return;
    }
    feed.publish_event(crate::feed::FeedEvent::Funding(funding.clone()));
    if let Some(handler) = &feed.funding_handler {
        await_handler("funding", handler.on_funding(funding)).await;
    }
}

#[cfg(feature = "liquidations")]
async fn dispatch_liquidation(
    feed: &ExchangeFeed,
    liquidation: cryptofeed_liquidations::Liquidation,
) {
    if !subscribed_to(feed, Channel::Liquidations, &liquidation.symbol) {
        return;
    }
    feed.publish_event(crate::feed::FeedEvent::Liquidation(liquidation.clone()));
    if let Some(handler) = &feed.liquidation_handler {
        await_handler("liquidation", handler.on_liquidation(liquidation)).await;
    }
}

#[cfg(feature = "markprice")]
async fn dispatch_mark_price(feed: &ExchangeFeed, mark_price: cryptofeed_markprice::MarkPrice) {
    if !subscribed_to(feed, Channel::MarkPrice, &mark_price.symbol) {
        return;
    }
    feed.publish_event(crate::feed::FeedEvent::MarkPrice(mark_price.clone()));
    if let Some(handler) = &feed.mark_price_handler {
        await_handler("mark_price", handler.on_mark_price(mark_price)).await;
    }
}

#[cfg(feature = "openinterest")]
async fn dispatch_open_interest(
    feed: &ExchangeFeed,
    open_interest: cryptofeed_openinterest::OpenInterest,
) {
    if !subscribed_to(feed, Channel::OpenInterest, &open_interest.symbol) {
        return;
    }
    feed.publish_event(crate::feed::FeedEvent::OpenInterest(open_interest.clone()));
    if let Some(handler) = &feed.open_interest_handler {
        await_handler("open_interest", handler.on_open_interest(open_interest)).await;
    }
}

#[cfg(feature = "index")]
async fn dispatch_index_price(feed: &ExchangeFeed, index_price: cryptofeed_index::IndexPrice) {
    if !subscribed_to(feed, Channel::Index, &index_price.symbol) {
        return;
    }
    feed.publish_event(crate::feed::FeedEvent::IndexPrice(index_price.clone()));
    if let Some(handler) = &feed.index_price_handler {
        await_handler("index_price", handler.on_index_price(index_price)).await;
    }
}

#[cfg(feature = "ticker")]
async fn dispatch_ticker(feed: &ExchangeFeed, ticker: cryptofeed_ticker::Ticker) {
    if !subscribed_to(feed, Channel::Ticker, &ticker.symbol) {
        return;
    }
    feed.publish_event(crate::feed::FeedEvent::Ticker(ticker.clone()));
    if let Some(handler) = &feed.ticker_handler {
        await_handler("ticker", handler.on_ticker(ticker)).await;
    }
}

#[cfg(feature = "trade")]
async fn dispatch_trade(feed: &ExchangeFeed, trade: cryptofeed_trade::Trade) {
    if !subscribed_to(feed, Channel::Trade, &trade.symbol) {
        return;
    }
    feed.publish_event(crate::feed::FeedEvent::Trade(trade.clone()));
    if let Some(handler) = &feed.trade_handler {
        await_handler("trade", handler.on_trade(trade)).await;
    }
}

#[cfg(feature = "orderbook")]
async fn dispatch_l2_book(feed: &ExchangeFeed, book: L2Book) {
    if !subscribed_to(feed, Channel::L2Book, book.symbol()) {
        return;
    }
    apply_orderbook_state(feed, &book);
    feed.publish_event(crate::feed::FeedEvent::L2Book(book.clone()));
    if let Some(handler) = &feed.orderbook_handler {
        await_handler("l2_book", handler.on_l2_book(book)).await;
    }
}

#[cfg(feature = "orderbook")]
async fn dispatch_l1_book(feed: &ExchangeFeed, book: cryptofeed_orderbook::L1Book) {
    if !subscribed_to(feed, Channel::L1Book, &book.symbol) {
        return;
    }
    feed.publish_event(crate::feed::FeedEvent::L1Book(book.clone()));
    if let Some(handler) = &feed.orderbook_handler {
        await_handler("l1_book", handler.on_l1_book(book)).await;
    }
}

fn subscribed_to(
    feed: &ExchangeFeed,
    channel: Channel,
    symbol: &cryptofeed_core::symbol::Symbol,
) -> bool {
    feed.channels.contains(&channel) && feed.symbols.iter().any(|candidate| candidate == symbol)
}

async fn await_handler<F>(channel: &'static str, future: F)
where
    F: Future<Output = ()>,
{
    if tokio::time::timeout(HANDLER_TIMEOUT, future).await.is_err() {
        tracing::warn!(channel, "handler timed out; event callback cancelled");
    }
}

#[cfg(feature = "orderbook")]
fn spawn_binance_snapshot_fetches(
    instruments: &[BinanceInstrument],
    limit: u16,
) -> std::collections::HashMap<
    String,
    oneshot::Receiver<Result<(u64, cryptofeed_orderbook::L2BookSnapshot)>>,
> {
    let mut receivers = std::collections::HashMap::new();

    for instrument in instruments {
        let symbol_key = instrument.symbol.as_str().to_owned();
        let rx = spawn_binance_snapshot_fetch(instrument.clone(), limit);
        receivers.insert(symbol_key, rx);
    }

    receivers
}

#[cfg(feature = "orderbook")]
fn spawn_binance_snapshot_fetch(
    instrument: BinanceInstrument,
    limit: u16,
) -> oneshot::Receiver<Result<(u64, cryptofeed_orderbook::L2BookSnapshot)>> {
    let (mut tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        tokio::select! {
            biased;
            _ = tx.closed() => {}
            result = fetch_binance_l2_snapshot(instrument, limit) => {
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
) -> Result<(u64, cryptofeed_orderbook::L2BookSnapshot)> {
    // Partial-depth streams must be bootstrapped from a snapshot of the same
    // width; the full-depth default uses the 1000-level snapshot.
    let url = BinanceAdapter::snapshot_url(&instrument, limit);
    let client = reqwest::Client::builder()
        .timeout(SNAPSHOT_HTTP_TIMEOUT)
        .build()
        .map_err(|e| Error::Transport(e.to_string()))?;
    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|e| Error::Transport(e.to_string()))?;
    let payload = read_bounded_snapshot_json(response, &url).await?;
    binance_parser::parse_l2_book_snapshot_for_instrument(
        &payload,
        &instrument,
        current_timestamp(),
    )
    .ok_or_else(|| Error::Parse("failed to parse binance l2 snapshot".to_owned()))
}

#[cfg(feature = "orderbook")]
async fn poll_binance_snapshot_bootstraps(
    feed: &ExchangeFeed,
    instruments: &[BinanceInstrument],
    receivers: &mut std::collections::HashMap<
        String,
        oneshot::Receiver<Result<(u64, cryptofeed_orderbook::L2BookSnapshot)>>,
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
                        spawn_binance_snapshot_fetch(instrument.clone(), limit),
                    );
                    continue;
                }
            };
            for book in books {
                dispatch_binance_l2_book(feed, book).await;
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
    let mut syncs = feed.binance_book_syncs.lock().expect("binance sync lock");
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
fn bootstrap_binance_book(
    feed: &ExchangeFeed,
    symbol_key: &str,
    last_update_id: u64,
    snapshot: cryptofeed_orderbook::L2BookSnapshot,
    buffered: Vec<BinanceDepthDelta>,
) -> Result<Option<cryptofeed_orderbook::L2Book>> {
    let mut syncs = feed.binance_book_syncs.lock().expect("binance sync lock");
    let sync = syncs
        .entry(symbol_key.to_owned())
        .or_insert_with(|| BinanceBookSync::new(snapshot.symbol.clone()));
    sync.bootstrap(last_update_id, snapshot.clone(), buffered)?;

    let mut states = feed.orderbook_states.lock().expect("orderbook state lock");
    states.insert(symbol_key.to_owned(), sync.state().clone());

    Ok(Some(cryptofeed_orderbook::L2Book::Snapshot(snapshot)))
}

#[cfg(feature = "orderbook")]
async fn process_binance_orderbook_message(
    feed: &ExchangeFeed,
    plan: &BinanceConnectionPlan,
    text: &str,
    received_ts: f64,
    receivers: &mut std::collections::HashMap<
        String,
        oneshot::Receiver<Result<(u64, cryptofeed_orderbook::L2BookSnapshot)>>,
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
        oneshot::Receiver<Result<(u64, cryptofeed_orderbook::L2BookSnapshot)>>,
    >,
    pending: &mut std::collections::HashMap<String, Vec<BinanceSequencedDepthDelta>>,
) -> Result<()> {
    if feed
        .binance_book_syncs
        .lock()
        .expect("binance sync lock")
        .contains_key(symbol_key)
    {
        let maybe_book = {
            let mut syncs = feed.binance_book_syncs.lock().expect("binance sync lock");
            let sync = syncs.get_mut(symbol_key).expect("binance sync state");
            sync.apply_next_sequenced_delta(update.clone())
        };

        match maybe_book {
            Ok(Some(book)) => {
                dispatch_binance_l2_book(feed, book).await;
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
    feed.binance_book_syncs
        .lock()
        .expect("binance sync lock")
        .remove(symbol_key);
    buffer_binance_delta(pending, symbol_key, update);
}

#[cfg(all(feature = "orderbook", test))]
fn reset_binance_sync_state(
    feed: &ExchangeFeed,
    symbol_key: &str,
    update: BinanceDepthDelta,
    pending: &mut std::collections::HashMap<String, Vec<BinanceDepthDelta>>,
) {
    feed.binance_book_syncs
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
        oneshot::Receiver<Result<(u64, cryptofeed_orderbook::L2BookSnapshot)>>,
    >,
    pending: &mut std::collections::HashMap<String, Vec<BinanceSequencedDepthDelta>>,
    limit: u16,
) {
    reset_binance_sequenced_sync_state(feed, symbol_key, update, pending);
    receivers.remove(symbol_key);
    receivers.insert(
        symbol_key.to_owned(),
        spawn_binance_snapshot_fetch(instrument.clone(), limit),
    );
}

#[cfg(feature = "orderbook")]
fn spawn_gateio_snapshot_fetches_for_plan(
    plan: &GateioConnectionPlan,
) -> Result<GateioSnapshotReceivers> {
    if plan.snapshot_urls.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    if plan.instruments.len() != plan.snapshot_urls.len() {
        return Err(Error::InvalidConfiguration(
            "gateio instruments and snapshot URL counts differ".to_owned(),
        ));
    }
    Ok(plan
        .instruments
        .iter()
        .cloned()
        .zip(plan.snapshot_urls.iter().cloned())
        .map(|(instrument, url)| {
            (
                instrument.symbol.as_str().to_owned(),
                spawn_gateio_snapshot_fetch_for_instrument(instrument, url),
            )
        })
        .collect())
}

#[cfg(feature = "orderbook")]
fn spawn_gateio_snapshot_fetch_for_instrument(
    instrument: GateioInstrument,
    snapshot_url: String,
) -> oneshot::Receiver<Result<crate::exchange::gateio::book_sync::GateioBookSnapshot>> {
    let (mut tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        tokio::select! {
            biased;
            _ = tx.closed() => {}
            result = fetch_gateio_l2_snapshot_for_instrument(instrument, snapshot_url) => {
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
) -> Result<crate::exchange::gateio::book_sync::GateioBookSnapshot> {
    let client = reqwest::Client::builder()
        .timeout(SNAPSHOT_HTTP_TIMEOUT)
        .build()
        .map_err(|error| Error::Transport(error.to_string()))?;
    let response = client
        .get(&snapshot_url)
        .send()
        .await
        .map_err(|error| Error::Transport(error.to_string()))?;
    let payload = read_bounded_snapshot_json(response, &snapshot_url).await?;
    gateio_parser::parse_l2_book_snapshot_for_instrument(&payload, &instrument, current_timestamp())
        .ok_or_else(|| Error::Parse("failed to parse gateio l2 snapshot".to_owned()))
}

#[cfg(feature = "orderbook")]
async fn read_bounded_snapshot_json(response: reqwest::Response, url: &str) -> Result<Value> {
    let mut response = response
        .error_for_status()
        .map_err(|error| Error::Transport(format!("{url}: {error}")))?;
    if response
        .content_length()
        .is_some_and(|length| length > MAX_SNAPSHOT_RESPONSE_BYTES as u64)
    {
        return Err(Error::MalformedData(format!(
            "{url}: snapshot exceeds {MAX_SNAPSHOT_RESPONSE_BYTES} bytes"
        )));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| Error::Transport(format!("{url}: {error}")))?
    {
        if body.len().saturating_add(chunk.len()) > MAX_SNAPSHOT_RESPONSE_BYTES {
            return Err(Error::MalformedData(format!(
                "{url}: snapshot exceeds {MAX_SNAPSHOT_RESPONSE_BYTES} bytes"
            )));
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).map_err(|error| Error::Parse(format!("{url}: {error}")))
}

#[cfg(feature = "orderbook")]
async fn poll_gateio_snapshot_bootstraps_for_plan(
    feed: &ExchangeFeed,
    plan: &GateioConnectionPlan,
    receivers: &mut GateioSnapshotReceivers,
    pending: &mut std::collections::HashMap<String, Vec<GateioDepthDelta>>,
    resnapshot_attempts: &mut std::collections::HashMap<String, u32>,
) -> Result<()> {
    use tokio::sync::oneshot::error::TryRecvError;

    let keys: Vec<String> = receivers.keys().cloned().collect();
    for key in keys {
        // Current Gate.io REST snapshots do not carry a sequence id. Do not
        // consume a completed snapshot until at least one WebSocket delta has
        // been buffered; that delta provides the only provable sequence
        // anchor between the REST snapshot and the live stream.
        if pending.get(&key).is_none_or(Vec::is_empty) {
            continue;
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
        let snapshot = result?;
        let last_update_id = snapshot.last_update_id;
        let buffered = pending.remove(&key).unwrap_or_default();
        match bootstrap_gateio_book(feed, &key, snapshot, buffered.clone()) {
            Ok(books) => {
                for book in books {
                    dispatch_gateio_l2_book(feed, book).await;
                }
                receivers.remove(&key);
                resnapshot_attempts.remove(&key);
            }
            Err(error) => {
                // A failed bootstrap (typically buffered deltas that do not
                // bridge the snapshot) triggers a bounded in-session resnapshot
                // instead of tearing the session down. The buffered deltas are
                // preserved so the next snapshot can bridge them; the bridge
                // rule itself is never relaxed.
                let attempts = resnapshot_attempts.entry(key.clone()).or_default();
                // Forensics: a warning-level trace records the failing
                // snapshot id and the complete buffered U/u sequence so a live
                // smoke run can capture the evidence required to validate the
                // resnapshot path against a real failure.
                tracing::warn!(
                    exchange = "gateio",
                    symbol = %key,
                    snapshot_id = ?last_update_id,
                    buffered_sequences = %buffered
                        .iter()
                        .map(|delta| format!("{}..{}", delta.first_update_id, delta.last_update_id))
                        .collect::<Vec<_>>()
                        .join(","),
                    attempt = *attempts + 1,
                    error = %error,
                    "gateio bootstrap failed; scheduling bounded resnapshot"
                );
                if *attempts >= GATEIO_MAX_RESNAPSHOTS {
                    tracing::error!(
                        exchange = "gateio",
                        symbol = %key,
                        snapshot_id = ?last_update_id,
                        "gateio bootstrap resnapshot limit exceeded; session retry"
                    );
                    return Err(error);
                }
                *attempts += 1;
                feed.gateio_book_syncs
                    .lock()
                    .expect("gateio sync lock")
                    .remove(&key);
                pending.insert(key.clone(), buffered);
                let snapshot_url = plan
                    .snapshot_urls
                    .get(
                        plan.instruments
                            .iter()
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
                    spawn_gateio_snapshot_fetch_for_instrument(instrument.clone(), snapshot_url),
                );
            }
        }
    }
    Ok(())
}

#[cfg(feature = "orderbook")]
fn bootstrap_gateio_book(
    feed: &ExchangeFeed,
    symbol_key: &str,
    snapshot: crate::exchange::gateio::book_sync::GateioBookSnapshot,
    buffered: Vec<GateioDepthDelta>,
) -> Result<Vec<cryptofeed_orderbook::L2Book>> {
    let mut syncs = feed.gateio_book_syncs.lock().expect("gateio sync lock");
    let sync = syncs
        .entry(symbol_key.to_owned())
        .or_insert_with(|| GateioBookSync::new(snapshot.book.symbol.clone()));
    let events = sync.bootstrap(snapshot, buffered)?;

    let mut states = feed.orderbook_states.lock().expect("orderbook state lock");
    states.insert(symbol_key.to_owned(), sync.state().clone());

    Ok(events)
}

#[cfg(all(feature = "orderbook", test))]
async fn process_gateio_orderbook_message(
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
        .gateio_book_syncs
        .lock()
        .expect("gateio sync lock")
        .contains_key(&symbol_key)
    {
        let maybe_book = {
            let mut syncs = feed.gateio_book_syncs.lock().expect("gateio sync lock");
            let sync = syncs.get_mut(&symbol_key).expect("gateio sync state");
            sync.apply_next_delta(update.clone())
        };

        match maybe_book {
            Ok(Some(book)) => {
                dispatch_gateio_l2_book(feed, book).await;
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
async fn process_gateio_orderbook_message_for_plan(
    feed: &ExchangeFeed,
    plan: &GateioConnectionPlan,
    text: &str,
    received_ts: f64,
    receivers: &mut GateioSnapshotReceivers,
    pending: &mut std::collections::HashMap<String, Vec<GateioDepthDelta>>,
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

    if feed
        .gateio_book_syncs
        .lock()
        .expect("gateio sync lock")
        .contains_key(&symbol_key)
    {
        let maybe_book = {
            let mut syncs = feed.gateio_book_syncs.lock().expect("gateio sync lock");
            let sync = syncs.get_mut(&symbol_key).expect("gateio sync state");
            sync.apply_next_delta(update.clone())
        };
        match maybe_book {
            Ok(Some(book)) => dispatch_gateio_l2_book(feed, book).await,
            Ok(None) => {}
            Err(Error::Parse(_)) => {
                schedule_gateio_resync_for_plan(
                    feed,
                    plan,
                    instrument,
                    &symbol_key,
                    update,
                    receivers,
                    pending,
                )?;
            }
            Err(error) => return Err(error),
        }
    } else {
        buffer_gateio_delta(pending, &symbol_key, update);
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
    {
        let mut syncs = feed.gateio_book_syncs.lock().expect("gateio sync lock");
        let sync = syncs
            .entry(symbol_key.clone())
            .or_insert_with(|| GateioBookSync::new(snapshot.symbol.clone()));
        sync.reset_with_snapshot(snapshot.clone(), last_update_id);
        let mut states = feed.orderbook_states.lock().expect("orderbook state lock");
        states.insert(symbol_key.clone(), sync.state().clone());
    }
    receivers.remove(&symbol_key);
    pending.remove(&symbol_key);
    dispatch_gateio_l2_book(feed, L2Book::Snapshot(snapshot)).await;
    Ok(())
}

#[cfg(feature = "orderbook")]
fn schedule_gateio_resync_for_plan(
    feed: &ExchangeFeed,
    plan: &GateioConnectionPlan,
    instrument: &GateioInstrument,
    symbol_key: &str,
    update: GateioDepthDelta,
    receivers: &mut GateioSnapshotReceivers,
    pending: &mut std::collections::HashMap<String, Vec<GateioDepthDelta>>,
) -> Result<()> {
    reset_gateio_sync_state(feed, symbol_key, update, pending);
    let index = plan
        .instruments
        .iter()
        .position(|candidate| candidate == instrument)
        .ok_or_else(|| Error::Parse("missing gateio resync instrument".to_owned()))?;
    let snapshot_url = plan
        .snapshot_urls
        .get(index)
        .cloned()
        .ok_or_else(|| Error::Parse("missing gateio resync snapshot URL".to_owned()))?;
    receivers.remove(symbol_key);
    receivers.insert(
        symbol_key.to_owned(),
        spawn_gateio_snapshot_fetch_for_instrument(instrument.clone(), snapshot_url),
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
    feed.gateio_book_syncs
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
            spawn_gateio_snapshot_fetch_for_instrument(instrument, snapshot_url),
        );
    }
}

#[cfg_attr(not(test), allow(dead_code))]
async fn process_bitget_text_message(
    feed: &ExchangeFeed,
    text: &str,
    received_ts: f64,
) -> Result<()> {
    let message: Value = serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
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
        dispatch_bitget_event(feed, event).await;
    }
    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
async fn process_bybit_text_message(
    feed: &ExchangeFeed,
    text: &str,
    received_ts: f64,
) -> Result<()> {
    let message: Value = serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
    process_bybit_message(feed, &message, received_ts).await
}

fn merge_bybit_ticker_message(
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

async fn process_bybit_message(
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
        dispatch_bybit_event(feed, event).await;
    }
    Ok(())
}

#[cfg(feature = "orderbook")]
async fn process_bybit_orderbook_message(
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
            dispatch_bybit_event(feed, event).await;
        }
        return Ok(true);
    }

    let update = BybitAdapter::parse_l2_book_update_for_feed(feed, message, received_ts)
        .ok_or_else(|| Error::Parse("failed to parse bybit depth update".to_owned()))?;
    let symbol_key = match &update.book {
        L2Book::Snapshot(snapshot) => snapshot.symbol.as_str().to_owned(),
        L2Book::Delta(delta) => delta.symbol.as_str().to_owned(),
    };
    let maybe_book = {
        let mut syncs = feed.bybit_book_syncs.lock().expect("bybit sync lock");
        let sync = syncs.entry(symbol_key).or_insert_with(|| {
            BybitBookSync::new(match &update.book {
                L2Book::Snapshot(snapshot) => snapshot.symbol.clone(),
                L2Book::Delta(delta) => delta.symbol.clone(),
            })
        });
        sync.apply(update)?
    };

    if let Some(book) = maybe_book {
        dispatch_bybit_l2_book(feed, book).await;
    }

    Ok(true)
}

#[cfg_attr(not(test), allow(dead_code))]
async fn process_okx_text_message(feed: &ExchangeFeed, text: &str, received_ts: f64) -> Result<()> {
    if let Some(control) = OkxAdapter::parse_control_text(text) {
        return control.map(|_| ());
    }
    let message: Value = serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
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
        dispatch_okx_event(feed, event).await;
    }
    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
async fn process_gateio_text_message(
    feed: &ExchangeFeed,
    text: &str,
    received_ts: f64,
) -> Result<()> {
    let message: Value = serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
    if let Some(detail) = gateio_error_detail(&message) {
        // A rejected subscription is a permanent configuration problem;
        // `Subscription` errors fail fast instead of burning the retries.
        return Err(Error::Subscription(format!(
            "gateio websocket error: {detail}"
        )));
    }
    for event in GateioAdapter::parse_messages(&message, received_ts) {
        dispatch_gateio_event(feed, event).await;
    }
    Ok(())
}

async fn process_gateio_text_message_for_plan(
    feed: &ExchangeFeed,
    plan: &GateioConnectionPlan,
    text: &str,
    received_ts: f64,
) -> Result<()> {
    let message: Value = serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
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
                        dispatch_gateio_event(feed, event).await;
                    }
                }
            }
        }
        return Ok(());
    }
    let instrument = GateioAdapter::instrument_for_message(&message, &plan.instruments)
        .ok_or_else(|| Error::Parse("gateio message instrument is not in the plan".to_owned()))?;
    for event in GateioAdapter::parse_messages_for_instrument(&message, received_ts, instrument) {
        dispatch_gateio_event(feed, event).await;
    }
    Ok(())
}

fn gateio_error_detail(message: &Value) -> Option<&str> {
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

fn data_is_non_empty(message: &Value) -> bool {
    match message.get("data") {
        Some(Value::Array(rows)) => !rows.is_empty(),
        Some(Value::Object(fields)) => !fields.is_empty(),
        _ => false,
    }
}

#[cfg(feature = "orderbook")]
async fn process_okx_orderbook_message(
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
        let symbol_key = match &update.book {
            L2Book::Snapshot(snapshot) => snapshot.symbol.as_str().to_owned(),
            L2Book::Delta(delta) => delta.symbol.as_str().to_owned(),
        };
        let maybe_book = {
            let mut syncs = feed.okx_book_syncs.lock().expect("okx sync lock");
            let sync = syncs.entry(symbol_key).or_insert_with(|| {
                OkxBookSync::new(match &update.book {
                    L2Book::Snapshot(snapshot) => snapshot.symbol.clone(),
                    L2Book::Delta(delta) => delta.symbol.clone(),
                })
            });
            sync.apply(update)?
        };

        if let Some(book) = maybe_book {
            dispatch_okx_l2_book(feed, book).await;
        }
    }

    Ok(true)
}

#[cfg(feature = "orderbook")]
async fn process_bitget_orderbook_message(
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
            let mut syncs = feed.bitget_book_syncs.lock().expect("bitget sync lock");
            let sync = syncs.entry(symbol_key).or_insert_with(|| {
                BitgetBookSync::new(match &update.book {
                    L2Book::Snapshot(snapshot) => snapshot.symbol.clone(),
                    L2Book::Delta(delta) => delta.symbol.clone(),
                })
            });
            sync.apply(update)?
        };

        if let Some(book) = maybe_book {
            dispatch_bitget_l2_book(feed, book).await;
        }
    }

    Ok(true)
}

fn current_timestamp() -> f64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::connection::{HeartbeatPolicy, Session};
    use super::{
        planned_connection_urls, process_binance_text_message, process_bitget_text_message,
        process_bybit_text_message, process_gateio_text_message, process_okx_text_message,
    };
    use crate::{
        FeedHandler,
        exchange::binance::{
            Binance,
            adapter::{BinanceAdapter, BinanceEvent},
        },
        exchange::bitget::{
            Bitget,
            adapter::{BitgetAdapter, BitgetEvent},
        },
        exchange::bybit::Bybit,
        exchange::gateio::Gateio,
        exchange::okx::Okx,
    };
    use async_trait::async_trait;
    #[cfg(feature = "candles")]
    use cryptofeed_candles::{Candle, CandleHandler};
    use cryptofeed_core::{error::Error, exchange::ExchangeId};
    #[cfg(feature = "funding")]
    use cryptofeed_funding::{Funding, FundingHandler};
    #[cfg(feature = "index")]
    use cryptofeed_index::{IndexPrice, IndexPriceHandler};
    #[cfg(feature = "liquidations")]
    use cryptofeed_liquidations::{Liquidation, LiquidationHandler};
    #[cfg(feature = "openinterest")]
    use cryptofeed_openinterest::{OpenInterest, OpenInterestHandler};
    #[cfg(feature = "orderbook")]
    use cryptofeed_orderbook::{L2Book, OrderBookHandler};
    #[cfg(feature = "ticker")]
    use cryptofeed_ticker::{Ticker, TickerHandler};
    #[cfg(feature = "trade")]
    use cryptofeed_trade::{Trade, TradeHandler};
    use futures::{SinkExt, StreamExt};
    use tokio::io::duplex;
    use tokio::sync::{oneshot, watch};
    use tokio::time::Instant;
    use tokio_tungstenite::{
        WebSocketStream,
        tungstenite::{Message, protocol::Role},
    };
    use url::Url;

    #[test]
    fn gateio_subscribe_response_with_error_is_rejected() {
        let message = serde_json::json!({
            "event": "subscribe",
            "error": {"code": 2, "message": "invalid request time"},
            "result": null
        });

        assert_eq!(
            super::gateio_error_detail(&message),
            Some("invalid request time")
        );
    }

    #[tokio::test]
    async fn shutdown_preempts_symbol_hydration() {
        let mut handler = FeedHandler::new();
        handler.add_feed(Binance::new().ticker().symbol("BTC-USDT").build());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        shutdown_tx.send(true).expect("shutdown");

        tokio::time::timeout(
            Duration::from_millis(100),
            super::run_with_shutdown(handler, shutdown_rx),
        )
        .await
        .expect("shutdown must not wait for HTTP hydration")
        .expect("clean shutdown");
    }

    #[test]
    fn plans_binance_connection_urls() {
        let mut handler = FeedHandler::new();
        handler.add_feed(Binance::new().ticker().trade().symbol("BTC-USDT").build());

        assert_eq!(
            planned_connection_urls(&handler),
            vec![
                "wss://stream.binance.com:9443/stream?streams=btcusdt@bookTicker/btcusdt@aggTrade"
                    .to_owned()
            ]
        );
    }

    #[test]
    fn plans_bitget_connection_urls() {
        let mut handler = FeedHandler::new();
        handler.add_feed(
            Bitget::new()
                .ticker()
                .trade()
                .l2_book()
                .symbol("BTC-USDT")
                .build(),
        );

        assert_eq!(
            planned_connection_urls(&handler),
            vec!["wss://ws.bitget.com/v3/ws/public".to_owned()]
        );
    }

    #[test]
    fn plans_bybit_connection_urls() {
        let mut handler = FeedHandler::new();
        handler.add_feed(
            Bybit::new()
                .ticker()
                .trade()
                .l2_book()
                .candles()
                .symbol("BTC-USDT")
                .build(),
        );

        assert_eq!(
            planned_connection_urls(&handler),
            vec!["wss://stream.bybit.com/v5/public/spot".to_owned()]
        );
    }

    #[test]
    fn plans_gateio_connection_urls() {
        let mut handler = FeedHandler::new();
        handler.add_feed(
            Gateio::new()
                .ticker()
                .trade()
                .l2_book()
                .candles()
                .symbol("BTC-USDT")
                .build(),
        );

        assert_eq!(
            planned_connection_urls(&handler),
            vec!["wss://api.gateio.ws/ws/v4/".to_owned()]
        );
    }

    #[cfg(feature = "ticker")]
    struct TestTickerHandler {
        seen: Arc<Mutex<usize>>,
    }

    #[cfg(feature = "ticker")]
    #[async_trait]
    impl TickerHandler for TestTickerHandler {
        async fn on_ticker(&self, _ticker: Ticker) {
            *self.seen.lock().expect("lock") += 1;
        }
    }

    #[cfg(feature = "trade")]
    struct TestTradeHandler {
        seen: Arc<Mutex<usize>>,
    }

    #[cfg(feature = "trade")]
    #[async_trait]
    impl TradeHandler for TestTradeHandler {
        async fn on_trade(&self, _trade: Trade) {
            *self.seen.lock().expect("lock") += 1;
        }
    }

    #[cfg(feature = "orderbook")]
    struct TestOrderBookHandler {
        seen: Arc<Mutex<usize>>,
    }

    #[cfg(feature = "orderbook")]
    #[async_trait]
    impl OrderBookHandler for TestOrderBookHandler {
        async fn on_l2_book(&self, _book: L2Book) {
            *self.seen.lock().expect("lock") += 1;
        }
    }

    #[cfg(feature = "orderbook")]
    struct TestL1BookHandler {
        seen: Arc<Mutex<usize>>,
    }

    #[cfg(feature = "orderbook")]
    #[async_trait]
    impl OrderBookHandler for TestL1BookHandler {
        async fn on_l2_book(&self, _book: L2Book) {}

        async fn on_l1_book(&self, _book: cryptofeed_orderbook::L1Book) {
            *self.seen.lock().expect("lock") += 1;
        }
    }

    #[cfg(feature = "candles")]
    struct TestCandleHandler {
        seen: Arc<Mutex<usize>>,
    }

    #[cfg(feature = "candles")]
    #[async_trait]
    impl CandleHandler for TestCandleHandler {
        async fn on_candle(&self, _candle: Candle) {
            *self.seen.lock().expect("lock") += 1;
        }
    }

    #[cfg(feature = "funding")]
    struct TestFundingHandler {
        seen: Arc<Mutex<usize>>,
    }

    #[cfg(feature = "funding")]
    #[async_trait]
    impl FundingHandler for TestFundingHandler {
        async fn on_funding(&self, _funding: Funding) {
            *self.seen.lock().expect("lock") += 1;
        }
    }

    #[cfg(feature = "liquidations")]
    struct TestLiquidationHandler {
        seen: Arc<Mutex<usize>>,
    }

    #[cfg(feature = "liquidations")]
    #[async_trait]
    impl LiquidationHandler for TestLiquidationHandler {
        async fn on_liquidation(&self, _liquidation: Liquidation) {
            *self.seen.lock().expect("lock") += 1;
        }
    }

    #[cfg(feature = "openinterest")]
    struct TestOpenInterestHandler {
        seen: Arc<Mutex<usize>>,
    }

    #[cfg(feature = "openinterest")]
    #[async_trait]
    impl OpenInterestHandler for TestOpenInterestHandler {
        async fn on_open_interest(&self, _open_interest: OpenInterest) {
            *self.seen.lock().expect("lock") += 1;
        }
    }

    #[cfg(feature = "index")]
    struct TestIndexPriceHandler {
        seen: Arc<Mutex<usize>>,
    }

    #[cfg(feature = "index")]
    #[async_trait]
    impl IndexPriceHandler for TestIndexPriceHandler {
        async fn on_index_price(&self, _index_price: IndexPrice) {
            *self.seen.lock().expect("lock") += 1;
        }
    }

    #[cfg(all(feature = "ticker", feature = "trade"))]
    #[tokio::test]
    async fn dispatches_binance_trade_and_ticker_events() {
        let ticker_seen = Arc::new(Mutex::new(0));
        let trade_seen = Arc::new(Mutex::new(0));
        let feed = Binance::new()
            .ticker()
            .trade()
            .ticker_handler(Arc::new(TestTickerHandler {
                seen: ticker_seen.clone(),
            }))
            .trade_handler(Arc::new(TestTradeHandler {
                seen: trade_seen.clone(),
            }))
            .symbol("BTC-USDT")
            .build();

        let trade_message = serde_json::json!({
            "e": "aggTrade",
            "s": "BTCUSDT",
            "a": 12345,
            "p": "65000.50",
            "q": "0.01000000",
            "T": 1710000000123u64,
            "m": false
        });
        let ticker_message = serde_json::json!({
            "e": "bookTicker",
            "s": "BTCUSDT",
            "b": "64999.10",
            "a": "65000.20",
            "E": 1710000000456u64
        });

        let trade_event =
            BinanceAdapter::parse_message(&trade_message, 1710000001.5).expect("trade event");
        let ticker_event =
            BinanceAdapter::parse_message(&ticker_message, 1710000001.5).expect("ticker event");

        assert!(matches!(trade_event, BinanceEvent::Trade(_)));
        assert!(matches!(ticker_event, BinanceEvent::Ticker(_)));

        super::dispatch_binance_event(&feed, trade_event).await;
        super::dispatch_binance_event(&feed, ticker_event).await;

        assert_eq!(*trade_seen.lock().expect("lock"), 1);
        assert_eq!(*ticker_seen.lock().expect("lock"), 1);
    }

    #[cfg(all(feature = "ticker", feature = "orderbook"))]
    #[tokio::test]
    async fn multiplexed_stream_does_not_emit_unsubscribed_l1_event() {
        let mut handler = FeedHandler::new();
        let mut events = handler.subscribe();
        handler.add_feed(
            Binance::new()
                .ticker()
                .symbol("BTC-USDT")
                .exchange_symbol("BTCUSDT")
                .build(),
        );
        let feed = handler.into_feeds().pop().expect("feed");
        let message = serde_json::json!({
            "u": 400900217u64,
            "s": "BTCUSDT",
            "b": "64999.10",
            "B": "1.25",
            "a": "65000.20",
            "A": "0.75"
        });

        process_binance_text_message(&feed, &message.to_string(), 1710000001.5)
            .await
            .expect("process ticker");
        assert!(matches!(
            events.recv().await.expect("ticker event"),
            crate::feed::FeedEvent::Ticker(_)
        ));
        assert!(matches!(
            events.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
    }

    #[cfg(all(feature = "ticker", feature = "trade"))]
    #[tokio::test]
    async fn processes_combined_binance_trade_message() {
        let trade_seen = Arc::new(Mutex::new(0));
        let feed = Binance::new()
            .trade()
            .trade_handler(Arc::new(TestTradeHandler {
                seen: trade_seen.clone(),
            }))
            .symbol("BTC-USDT")
            .build();

        let message = serde_json::json!({
            "stream": "btcusdt@aggTrade",
            "data": {
                "e": "aggTrade",
                "s": "BTCUSDT",
                "a": 12345,
                "p": "65000.50",
                "q": "0.01000000",
                "T": 1710000000123u64,
                "m": false
            }
        });

        process_binance_text_message(&feed, &message.to_string(), 1710000001.5)
            .await
            .expect("process message");

        assert_eq!(*trade_seen.lock().expect("lock"), 1);
    }

    #[cfg(feature = "orderbook")]
    #[tokio::test]
    async fn dispatches_binance_l2_book_events() {
        let seen = Arc::new(Mutex::new(0));
        let feed = Binance::new()
            .l2_book()
            .orderbook_handler(Arc::new(TestOrderBookHandler { seen: seen.clone() }))
            .symbol("BTC-USDT")
            .build();

        let message = serde_json::json!({
            "e": "depthUpdate",
            "s": "BTCUSDT",
            "E": 1710000000456u64,
            "b": [["64999.10", "1.25"]],
            "a": [["65000.20", "0.75"]]
        });

        let event = BinanceAdapter::parse_message(&message, 1710000001.5).expect("book event");
        assert!(matches!(event, BinanceEvent::L2Book(_)));

        super::dispatch_binance_event(&feed, event).await;
        assert_eq!(*seen.lock().expect("lock"), 1);
        let states = feed.orderbook_states.lock().expect("lock");
        let state = states.get("BTC-USDT").expect("btc-usdt state");
        assert_eq!(state.bids().len(), 1);
        assert_eq!(state.asks().len(), 1);
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn bootstrap_binance_book_initializes_sync_state() {
        let feed = Binance::new().l2_book().symbol("BTC-USDT").build();
        let snapshot = cryptofeed_orderbook::L2BookSnapshot {
            exchange: cryptofeed_core::exchange::ExchangeId::Binance,
            symbol: cryptofeed_core::symbol::Symbol::spot("btc", "usdt"),
            bids: vec![cryptofeed_orderbook::PriceLevel {
                price: rust_decimal::Decimal::from_str_exact("64999.10").unwrap(),
                amount: rust_decimal::Decimal::from_str_exact("1.25").unwrap(),
            }],
            asks: vec![cryptofeed_orderbook::PriceLevel {
                price: rust_decimal::Decimal::from_str_exact("65000.20").unwrap(),
                amount: rust_decimal::Decimal::from_str_exact("0.75").unwrap(),
            }],
            exchange_ts: 1.0,
            received_ts: 2.0,
        };

        super::bootstrap_binance_book(&feed, "BTC-USDT", 100, snapshot, vec![]).expect("bootstrap");

        let syncs = feed.binance_book_syncs.lock().expect("syncs");
        let sync = syncs.get("BTC-USDT").expect("book sync");
        assert_eq!(sync.last_update_id(), Some(100));
    }

    #[cfg(feature = "orderbook")]
    #[tokio::test]
    async fn bootstrap_binance_book_dispatches_snapshot() {
        let seen = Arc::new(Mutex::new(0));
        let feed = Binance::new()
            .l2_book()
            .orderbook_handler(Arc::new(TestOrderBookHandler { seen: seen.clone() }))
            .symbol("BTC-USDT")
            .build();
        let snapshot = cryptofeed_orderbook::L2BookSnapshot {
            exchange: cryptofeed_core::exchange::ExchangeId::Binance,
            symbol: cryptofeed_core::symbol::Symbol::spot("btc", "usdt"),
            bids: vec![cryptofeed_orderbook::PriceLevel {
                price: rust_decimal::Decimal::from_str_exact("64999.10").unwrap(),
                amount: rust_decimal::Decimal::from_str_exact("1.25").unwrap(),
            }],
            asks: vec![cryptofeed_orderbook::PriceLevel {
                price: rust_decimal::Decimal::from_str_exact("65000.20").unwrap(),
                amount: rust_decimal::Decimal::from_str_exact("0.75").unwrap(),
            }],
            exchange_ts: 1.0,
            received_ts: 2.0,
        };

        let book = super::bootstrap_binance_book(&feed, "BTC-USDT", 100, snapshot, vec![])
            .expect("bootstrap")
            .expect("snapshot event");
        super::dispatch_binance_l2_book(&feed, book).await;

        assert_eq!(*seen.lock().expect("lock"), 1);
        let states = feed.orderbook_states.lock().expect("lock");
        let state = states.get("BTC-USDT").expect("btc-usdt state");
        assert_eq!(state.bids().len(), 1);
        assert_eq!(state.asks().len(), 1);
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn reset_binance_sync_moves_delta_back_to_pending() {
        let feed = Binance::new().l2_book().symbol("BTC-USDT").build();
        let update = crate::exchange::binance::book_sync::BinanceDepthDelta {
            first_update_id: 102,
            last_update_id: 103,
            book: cryptofeed_orderbook::L2BookDelta {
                exchange: cryptofeed_core::exchange::ExchangeId::Binance,
                symbol: cryptofeed_core::symbol::Symbol::spot("btc", "usdt"),
                bids: vec![],
                asks: vec![],
                exchange_ts: 1.0,
                received_ts: 2.0,
            },
        };
        let mut pending = std::collections::HashMap::new();
        {
            let mut syncs = feed.binance_book_syncs.lock().expect("syncs");
            syncs.insert(
                "BTC-USDT".to_owned(),
                crate::exchange::binance::book_sync::BinanceBookSync::new(
                    cryptofeed_core::symbol::Symbol::spot("btc", "usdt"),
                ),
            );
        }

        super::reset_binance_sync_state(&feed, "BTC-USDT", update, &mut pending);

        let syncs = feed.binance_book_syncs.lock().expect("syncs");
        assert!(!syncs.contains_key("BTC-USDT"));
        let queued = pending.get("BTC-USDT").expect("pending delta");
        assert_eq!(queued.len(), 1);
    }

    #[tokio::test]
    async fn runs_multiple_feeds_concurrently() {
        let feeds = vec![
            Binance::new().ticker().symbol("BTC-USDT").build(),
            Binance::new().trade().symbol("ETH-USDT").build(),
        ];

        let started = Instant::now();
        let (_tx, rx) = watch::channel(false);
        super::run_feeds_until_shutdown(feeds, rx, |_, _| async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(())
        })
        .await
        .expect("concurrent run");

        assert!(started.elapsed() < Duration::from_millis(90));
    }

    #[tokio::test]
    async fn stops_feeds_on_shutdown_signal() {
        let feeds = vec![
            Binance::new().ticker().symbol("BTC-USDT").build(),
            Bitget::new().trade().symbol("BTC-USDT").build(),
        ];
        let (tx, rx) = watch::channel(false);
        let started = Arc::new(Mutex::new(0usize));
        let stopped = Arc::new(Mutex::new(0usize));

        let started_clone = started.clone();
        let stopped_clone = stopped.clone();
        let run = tokio::spawn(async move {
            super::run_feeds_until_shutdown(feeds, rx, move |_, mut shutdown| {
                let started = started_clone.clone();
                let stopped = stopped_clone.clone();
                async move {
                    *started.lock().expect("lock") += 1;
                    loop {
                        match shutdown.changed().await {
                            Ok(()) if *shutdown.borrow() => break,
                            Ok(()) => {}
                            Err(_) => break,
                        }
                    }
                    *stopped.lock().expect("lock") += 1;
                    Ok(())
                }
            })
            .await
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        tx.send(true).expect("shutdown signal");
        run.await.expect("join").expect("shutdown run");

        assert_eq!(*started.lock().expect("lock"), 2);
        assert_eq!(*stopped.lock().expect("lock"), 2);
    }

    #[tokio::test]
    async fn terminal_feed_failure_does_not_cancel_healthy_feed() {
        let mut handler = FeedHandler::new();
        let mut statuses = handler.subscribe_status();
        handler.add_feed(Binance::new().ticker().symbol("BTC-USDT").build());
        handler.add_feed(Bitget::new().trade().symbol("BTC-USDT").build());
        let feeds = handler.into_feeds();
        let (_tx, rx) = watch::channel(false);
        let healthy_completed = Arc::new(Mutex::new(false));
        let healthy_completed_clone = healthy_completed.clone();

        let result = super::run_feeds_until_shutdown(feeds, rx, move |feed, _| {
            let healthy_completed = healthy_completed_clone.clone();
            async move {
                if feed.exchange == ExchangeId::Binance {
                    Err(Error::Transport("terminal binance failure".to_owned()))
                } else {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    *healthy_completed.lock().expect("lock") = true;
                    Ok(())
                }
            }
        })
        .await;

        assert!(*healthy_completed.lock().expect("lock"));
        assert!(
            matches!(result, Err(Error::Transport(message)) if message.contains("terminal binance failure"))
        );
        assert!(matches!(
            statuses.try_recv().expect("terminal status"),
            crate::feed::FeedStatus::Terminated {
                exchange: ExchangeId::Binance,
                error
            } if error.contains("terminal binance failure")
        ));
    }

    #[tokio::test]
    async fn shutdown_is_bounded_when_a_feed_does_not_cooperate() {
        let feeds = vec![Binance::new().ticker().symbol("BTC-USDT").build()];
        let (tx, rx) = watch::channel(false);
        let started = Arc::new(tokio::sync::Notify::new());
        let started_by_feed = started.clone();
        let run = tokio::spawn(async move {
            super::run_feeds_until_shutdown(feeds, rx, move |_, _| {
                let started = started_by_feed.clone();
                async move {
                    started.notify_one();
                    std::future::pending::<cryptofeed_core::error::Result<()>>().await
                }
            })
            .await
        });

        started.notified().await;
        tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_millis(500), run)
            .await
            .expect("bounded shutdown")
            .expect("join")
            .expect("shutdown result");
    }

    #[cfg(all(feature = "ticker", feature = "trade", feature = "orderbook"))]
    #[tokio::test]
    async fn processes_bitget_trade_message() {
        let trade_seen = Arc::new(Mutex::new(0));
        let feed = Bitget::new()
            .trade()
            .trade_handler(Arc::new(TestTradeHandler {
                seen: trade_seen.clone(),
            }))
            .symbol("BTC-USDT")
            .build();

        let message = serde_json::json!({
            "arg": {"channel": "trade", "instId": "BTCUSDT"},
            "data": [[ "1710000000123", "65000.50", "0.0100", "buy" ]]
        });

        process_bitget_text_message(&feed, &message.to_string(), 1710000001.5)
            .await
            .expect("process bitget trade");

        assert_eq!(*trade_seen.lock().expect("lock"), 1);
    }

    #[cfg(all(feature = "ticker", feature = "trade", feature = "orderbook"))]
    #[tokio::test]
    async fn processes_bybit_trade_message() {
        let trade_seen = Arc::new(Mutex::new(0));
        let feed = Bybit::new()
            .trade()
            .trade_handler(Arc::new(TestTradeHandler {
                seen: trade_seen.clone(),
            }))
            .symbol("BTC-USDT")
            .build();

        let message = serde_json::json!({
            "topic": "publicTrade.BTCUSDT",
            "type": "snapshot",
            "ts": 1672304486868i64,
            "data": [{
                "T": 1672304486865i64,
                "s": "BTCUSDT",
                "S": "Buy",
                "v": "0.001",
                "p": "16578.50",
                "i": "20f43950"
            }]
        });

        process_bybit_text_message(&feed, &message.to_string(), 1672304487.0)
            .await
            .expect("process bybit trade");

        assert_eq!(*trade_seen.lock().expect("lock"), 1);
    }

    #[cfg(all(feature = "ticker", feature = "trade", feature = "orderbook"))]
    #[tokio::test]
    async fn dispatches_bybit_l2_book_events() {
        let seen = Arc::new(Mutex::new(0));
        let feed = Bybit::new()
            .l2_book()
            .orderbook_handler(Arc::new(TestOrderBookHandler { seen: seen.clone() }))
            .symbol("BTC-USDT")
            .build();

        let message = serde_json::json!({
            "topic": "orderbook.50.BTCUSDT",
            "type": "snapshot",
            "ts": 1672304484978i64,
            "data": {
                "s": "BTCUSDT",
                "b": [["16493.50", "0.006"]],
                "a": [["16493.60", "0.100"]],
                "u": 18521288u64,
                "seq": 7961638724u64
            }
        });

        process_bybit_text_message(&feed, &message.to_string(), 1672304485.0)
            .await
            .expect("process bybit book");

        assert_eq!(*seen.lock().expect("lock"), 1);
        let states = feed.orderbook_states.lock().expect("lock");
        let state = states.get("BTC-USDT").expect("btc-usdt state");
        assert_eq!(state.bids().len(), 1);
        assert_eq!(state.asks().len(), 1);
    }

    #[cfg(all(feature = "ticker", feature = "trade", feature = "orderbook"))]
    #[tokio::test]
    async fn keeps_bybit_spot_bbo_and_l2_routes_independent() {
        let ticker_seen = Arc::new(Mutex::new(0));
        let book_seen = Arc::new(Mutex::new(0));
        let feed = Bybit::new()
            .ticker()
            .l2_book()
            .ticker_handler(Arc::new(TestTickerHandler {
                seen: ticker_seen.clone(),
            }))
            .orderbook_handler(Arc::new(TestOrderBookHandler {
                seen: book_seen.clone(),
            }))
            .symbol("BTC-USDT")
            .build();
        let bbo = serde_json::json!({
            "topic": "orderbook.1.BTCUSDT",
            "type": "snapshot",
            "ts": 1672304484900i64,
            "data": { "s": "BTCUSDT", "b": [["16493.50", "0.006"]], "a": [["16493.60", "0.100"]], "u": 1u64, "seq": 10u64 }
        });
        let l2 = serde_json::json!({
            "topic": "orderbook.50.BTCUSDT",
            "type": "snapshot",
            "ts": 1672304484978i64,
            "data": { "s": "BTCUSDT", "b": [["16493.40", "0.020"]], "a": [["16493.70", "0.030"]], "u": 2u64, "seq": 11u64 }
        });

        process_bybit_text_message(&feed, &bbo.to_string(), 1672304485.0)
            .await
            .expect("process bbo");
        process_bybit_text_message(&feed, &l2.to_string(), 1672304485.0)
            .await
            .expect("process l2");

        assert_eq!(*ticker_seen.lock().expect("lock"), 1);
        assert_eq!(*book_seen.lock().expect("lock"), 1);
    }

    #[cfg(all(
        feature = "ticker",
        feature = "trade",
        feature = "orderbook",
        feature = "candles"
    ))]
    #[tokio::test]
    async fn processes_bybit_public_session_messages() {
        let ticker_seen = Arc::new(Mutex::new(0));
        let trade_seen = Arc::new(Mutex::new(0));
        let book_seen = Arc::new(Mutex::new(0));
        let candle_seen = Arc::new(Mutex::new(0));
        let feed = Bybit::new()
            .ticker()
            .trade()
            .l2_book()
            .candles()
            .ticker_handler(Arc::new(TestTickerHandler {
                seen: ticker_seen.clone(),
            }))
            .trade_handler(Arc::new(TestTradeHandler {
                seen: trade_seen.clone(),
            }))
            .orderbook_handler(Arc::new(TestOrderBookHandler {
                seen: book_seen.clone(),
            }))
            .candle_handler(Arc::new(TestCandleHandler {
                seen: candle_seen.clone(),
            }))
            .symbol("BTC-USDT")
            .build();

        let ticker = serde_json::json!({
            "topic": "orderbook.1.BTCUSDT",
            "type": "snapshot",
            "ts": 1672304486868i64,
            "data": { "s": "BTCUSDT", "b": [["16578.50", "0.001"]], "a": [["16579.00", "0.002"]], "u": 1u64, "seq": 7961638723u64 }
        });
        let trade = serde_json::json!({
            "topic": "publicTrade.BTCUSDT",
            "type": "snapshot",
            "ts": 1672304486868i64,
            "data": [{ "T": 1672304486865i64, "s": "BTCUSDT", "S": "Buy", "v": "0.001", "p": "16578.50", "i": "20f43950" }]
        });
        let book = serde_json::json!({
            "topic": "orderbook.50.BTCUSDT",
            "type": "snapshot",
            "ts": 1672304484978i64,
            "data": { "s": "BTCUSDT", "b": [["16493.50", "0.006"]], "a": [["16493.60", "0.100"]], "u": 18521288u64, "seq": 7961638724u64 }
        });
        let candle = serde_json::json!({
            "topic": "kline.1.BTCUSDT",
            "type": "snapshot",
            "ts": 1672324988882i64,
            "data": [{ "start": 1672324800000i64, "end": 1672324859999i64, "interval": "1", "open": "16649.5", "close": "16677", "high": "16677", "low": "16608", "volume": "2.081", "confirm": false }]
        });

        for message in [ticker, trade, book, candle] {
            process_bybit_text_message(&feed, &message.to_string(), 1672304487.0)
                .await
                .expect("process bybit public message");
        }

        assert_eq!(*ticker_seen.lock().expect("lock"), 1);
        assert_eq!(*trade_seen.lock().expect("lock"), 1);
        assert_eq!(*book_seen.lock().expect("lock"), 1);
        assert_eq!(*candle_seen.lock().expect("lock"), 1);
    }

    #[cfg(all(feature = "funding", feature = "liquidations"))]
    #[tokio::test]
    async fn processes_bybit_funding_and_liquidation_messages() {
        let funding_seen = Arc::new(Mutex::new(0));
        let liquidation_seen = Arc::new(Mutex::new(0));
        let feed = Bybit::new()
            .funding()
            .liquidations()
            .funding_handler(Arc::new(TestFundingHandler {
                seen: funding_seen.clone(),
            }))
            .liquidation_handler(Arc::new(TestLiquidationHandler {
                seen: liquidation_seen.clone(),
            }))
            .symbol("BTC-USDT-PERP")
            .exchange_symbol("BTCUSDT")
            .build();

        let funding = serde_json::json!({
            "topic": "tickers.BTCUSDT",
            "type": "snapshot",
            "ts": 1672304486868i64,
            "data": [{
                "symbol": "BTCUSDT",
                "fundingRate": "0.0001",
                "markPrice": "50000",
                "indexPrice": "49950"
            }]
        });
        let liquidation = serde_json::json!({
            "topic": "allLiquidation.BTCUSDT",
            "type": "snapshot",
            "ts": 1672304486868i64,
            "data": [{
                "T": 1672304486868i64,
                "s": "BTCUSDT",
                "S": "Sell",
                "v": "0.001",
                "p": "49500"
            }]
        });

        for message in [funding, liquidation] {
            process_bybit_text_message(&feed, &message.to_string(), 1672304487.0)
                .await
                .expect("process bybit funding and liquidation");
        }

        assert_eq!(*funding_seen.lock().expect("lock"), 1);
        assert_eq!(*liquidation_seen.lock().expect("lock"), 1);
        let subscribe = crate::exchange::bybit::adapter::BybitAdapter::subscription_message(&feed);
        assert!(subscribe.contains("tickers.BTCUSDT"));
        assert!(subscribe.contains("allLiquidation.BTCUSDT"));
    }

    #[cfg(feature = "liquidations")]
    #[tokio::test]
    async fn rejects_legacy_bybit_liquidation_shape_instead_of_silent_drop() {
        let feed = Bybit::new()
            .liquidations()
            .symbol("BTC-USDT-PERP")
            .exchange_symbol("BTCUSDT")
            .build();
        let legacy = serde_json::json!({
            "topic": "allLiquidation.BTCUSDT",
            "type": "snapshot",
            "data": [{
                "updatedTime": 1672304486868i64,
                "symbol": "BTCUSDT",
                "side": "Sell",
                "size": "0.001",
                "price": "49500"
            }]
        });

        assert!(matches!(
            process_bybit_text_message(&feed, &legacy.to_string(), 1672304487.0).await,
            Err(Error::MalformedData(_))
        ));
    }

    #[cfg(all(feature = "openinterest", feature = "ticker"))]
    #[tokio::test]
    async fn processes_bybit_open_interest_from_tickers_stream() {
        let oi_seen = Arc::new(Mutex::new(0));
        let feed = Bybit::new()
            .open_interest()
            .ticker()
            .open_interest_handler(Arc::new(TestOpenInterestHandler {
                seen: oi_seen.clone(),
            }))
            .symbol("BTC-USDT-PERP")
            .exchange_symbol("BTCUSDT")
            .build();

        // Bybit carries open interest inside the derivative tickers stream.
        let tickers = serde_json::json!({
            "topic": "tickers.BTCUSDT",
            "type": "snapshot",
            "ts": 1672304486868i64,
            "data": {
                "symbol": "BTCUSDT",
                "bid1Price": "16578.50",
                "ask1Price": "16579.00",
                "openInterest": "1234.5",
                "openInterestValue": "20000000",
                "indexPrice": "16577.00"
            }
        });

        process_bybit_text_message(&feed, &tickers.to_string(), 1672304487.0)
            .await
            .expect("process bybit tickers with open interest");

        assert_eq!(*oi_seen.lock().expect("lock"), 1);
        let subscribe = crate::exchange::bybit::adapter::BybitAdapter::subscription_message(&feed);
        assert!(subscribe.contains("tickers.BTCUSDT"));
    }

    #[cfg(feature = "orderbook")]
    #[tokio::test]
    async fn processes_bybit_l1_book_message() {
        let l1_seen = Arc::new(Mutex::new(0));
        let feed = Bybit::new()
            .l1_book()
            .orderbook_handler(Arc::new(TestL1BookHandler {
                seen: l1_seen.clone(),
            }))
            .symbol("BTC-USDT")
            .exchange_symbol("BTCUSDT")
            .build();

        let message = serde_json::json!({
            "topic": "orderbook.1.BTCUSDT",
            "type": "snapshot",
            "ts": 1672304486868i64,
            "data": {
                "s": "BTCUSDT",
                "b": [["16578.50", "0.001"]],
                "a": [["16579.00", "0.002"]],
                "u": 123u64,
                "seq": 456u64
            }
        });

        process_bybit_text_message(&feed, &message.to_string(), 1672304487.0)
            .await
            .expect("process bybit l1 book");

        assert_eq!(*l1_seen.lock().expect("lock"), 1);
        assert!(
            crate::exchange::bybit::adapter::BybitAdapter::subscription_message(&feed)
                .contains("orderbook.1.BTCUSDT")
        );
    }

    #[cfg(feature = "ticker")]
    #[tokio::test]
    async fn processes_bybit_option_ticker_message() {
        let ticker_seen = Arc::new(Mutex::new(0));
        let feed = Bybit::new()
            .ticker()
            .ticker_handler(Arc::new(TestTickerHandler {
                seen: ticker_seen.clone(),
            }))
            .instrument(cryptofeed_core::symbol::Symbol::option(
                "BTC", "USDC", "30DEC22", "18000", "C",
            ))
            .exchange_symbol("BTC-30DEC22-18000-C")
            .build();

        // Option tickers carry the native instId; feed context resolves it.
        let message = serde_json::json!({
            "topic": "tickers.BTC-30DEC22-18000-C",
            "type": "snapshot",
            "ts": 1672304486868i64,
            "data": {
                "symbol": "BTC-30DEC22-18000-C",
                "bidPrice": "0.001",
                "askPrice": "0.002",
                "markPriceIv": "0.495"
            }
        });

        process_bybit_text_message(&feed, &message.to_string(), 1672304487.0)
            .await
            .expect("process bybit option ticker");

        assert_eq!(*ticker_seen.lock().expect("lock"), 1);
        let subscribe = crate::exchange::bybit::adapter::BybitAdapter::subscription_message(&feed);
        assert!(subscribe.contains("tickers.BTC-30DEC22-18000-C"));
    }

    #[cfg(feature = "ticker")]
    #[tokio::test]
    async fn rejects_linear_ticker_fields_on_bybit_option_topic() {
        let feed = Bybit::new()
            .ticker()
            .instrument(cryptofeed_core::symbol::Symbol::option(
                "BTC", "USDC", "30DEC22", "18000", "C",
            ))
            .exchange_symbol("BTC-30DEC22-18000-C")
            .build();
        let wrong_shape = serde_json::json!({
            "topic": "tickers.BTC-30DEC22-18000-C",
            "type": "snapshot",
            "data": {
                "symbol": "BTC-30DEC22-18000-C",
                "bid1Price": "0.001",
                "ask1Price": "0.002"
            }
        });

        assert!(matches!(
            process_bybit_text_message(&feed, &wrong_shape.to_string(), 1672304487.0).await,
            Err(Error::MalformedData(_))
        ));
    }

    #[cfg(all(feature = "trade", feature = "ticker"))]
    #[tokio::test]
    async fn processes_bybit_option_trade_from_base_stream() {
        let trade_seen = Arc::new(Mutex::new(0));
        let feed = Bybit::new()
            .trade()
            .trade_handler(Arc::new(TestTradeHandler {
                seen: trade_seen.clone(),
            }))
            .instrument(cryptofeed_core::symbol::Symbol::option(
                "BTC", "USDC", "30JUN26", "65000", "C",
            ))
            .exchange_symbol("BTC-30JUN26-65000-C")
            .build();

        // Option trades arrive on the base-coin stream; the full option
        // symbol is per row in `data.s`.
        let message = serde_json::json!({
            "topic": "publicTrade.BTC",
            "type": "snapshot",
            "ts": 1672304486868i64,
            "data": [{
                "T": 1672304486865i64,
                "s": "BTC-30JUN26-65000-C",
                "S": "Buy",
                "v": "0.5",
                "p": "0.001",
                "i": "opt-trade-1",
                "mP": "0.0011",
                "iP": "65000",
                "mIv": "0.5",
                "iv": "0.49"
            }]
        });

        process_bybit_text_message(&feed, &message.to_string(), 1672304487.0)
            .await
            .expect("process bybit option trade");

        assert_eq!(*trade_seen.lock().expect("lock"), 1);
        // The base-coin stream subscription deduplicates across symbols.
        let subscribe = crate::exchange::bybit::adapter::BybitAdapter::subscription_message(&feed);
        assert!(subscribe.contains("publicTrade.BTC"));
    }

    #[test]
    fn plans_okx_connection_urls() {
        let mut handler = FeedHandler::new();
        handler.add_feed(
            Okx::new()
                .ticker()
                .trade()
                .l2_book()
                .candles()
                .symbol("BTC-USDT")
                .build(),
        );

        assert_eq!(
            planned_connection_urls(&handler),
            vec!["wss://ws.okx.com/ws/v5/public".to_owned()]
        );
    }

    #[cfg(all(feature = "ticker", feature = "trade", feature = "orderbook"))]
    #[tokio::test]
    async fn processes_okx_trade_message() {
        let trade_seen = Arc::new(Mutex::new(0));
        let feed = Okx::new()
            .trade()
            .trade_handler(Arc::new(TestTradeHandler {
                seen: trade_seen.clone(),
            }))
            .symbol("BTC-USDT")
            .build();

        let message = serde_json::json!({
            "arg": {"channel": "trades", "instId": "BTC-USDT"},
            "data": [{ "tradeId": "1", "px": "65000.50", "sz": "0.0100", "side": "buy", "ts": "1710000000123" }]
        });

        process_okx_text_message(&feed, &message.to_string(), 1710000001.5)
            .await
            .expect("process okx trade");

        assert_eq!(*trade_seen.lock().expect("lock"), 1);
    }

    #[cfg(all(feature = "ticker", feature = "trade", feature = "orderbook"))]
    #[tokio::test]
    async fn dispatches_okx_l2_book_events() {
        let seen = Arc::new(Mutex::new(0));
        let feed = Okx::new()
            .l2_book()
            .orderbook_handler(Arc::new(TestOrderBookHandler { seen: seen.clone() }))
            .symbol("BTC-USDT")
            .build();

        let message = serde_json::json!({
            "arg": {"channel": "books", "instId": "BTC-USDT"},
            "action": "snapshot",
            "data": [{
                "bids": [["64999.10", "1.25", "0", "1"]],
                "asks": [["65000.20", "0.75", "0", "1"]],
                "ts": "1710000000456",
                "seqId": 100i64,
                "prevSeqId": -1i64
            }]
        });

        process_okx_text_message(&feed, &message.to_string(), 1710000001.5)
            .await
            .expect("process okx book");

        assert_eq!(*seen.lock().expect("lock"), 1);
        let states = feed.orderbook_states.lock().expect("lock");
        let state = states.get("BTC-USDT").expect("btc-usdt state");
        assert_eq!(state.bids().len(), 1);
        assert_eq!(state.asks().len(), 1);
    }

    #[cfg(all(
        feature = "ticker",
        feature = "trade",
        feature = "orderbook",
        feature = "candles"
    ))]
    #[tokio::test]
    async fn processes_okx_public_session_messages() {
        let ticker_seen = Arc::new(Mutex::new(0));
        let trade_seen = Arc::new(Mutex::new(0));
        let book_seen = Arc::new(Mutex::new(0));
        let candle_seen = Arc::new(Mutex::new(0));
        let feed = Okx::new()
            .ticker()
            .trade()
            .l2_book()
            .candles()
            .ticker_handler(Arc::new(TestTickerHandler {
                seen: ticker_seen.clone(),
            }))
            .trade_handler(Arc::new(TestTradeHandler {
                seen: trade_seen.clone(),
            }))
            .orderbook_handler(Arc::new(TestOrderBookHandler {
                seen: book_seen.clone(),
            }))
            .candle_handler(Arc::new(TestCandleHandler {
                seen: candle_seen.clone(),
            }))
            .symbol("BTC-USDT")
            .build();

        let ticker = serde_json::json!({
            "arg": {"channel": "tickers", "instId": "BTC-USDT"},
            "data": [{ "bidPx": "64999.10", "askPx": "65000.20", "ts": "1710000000456" }]
        });
        let trade = serde_json::json!({
            "arg": {"channel": "trades", "instId": "BTC-USDT"},
            "data": [{ "tradeId": "1", "px": "65000.50", "sz": "0.0100", "side": "buy", "ts": "1710000000123" }]
        });
        let book = serde_json::json!({
            "arg": {"channel": "books", "instId": "BTC-USDT"},
            "action": "snapshot",
            "data": [{ "bids": [["64999.10", "1.25", "0", "1"]], "asks": [["65000.20", "0.75", "0", "1"]], "ts": "1710000000456", "seqId": 100i64, "prevSeqId": -1i64 }]
        });
        let candle = serde_json::json!({
            "arg": {"channel": "candle1m", "instId": "BTC-USDT"},
            "data": [["1710000000000", "65000.00", "65100.00", "64900.00", "65050.00", "12.50", "0", "0", "1"]]
        });

        for message in [ticker, trade, book, candle] {
            process_okx_text_message(&feed, &message.to_string(), 1710000001.5)
                .await
                .expect("process okx public message");
        }

        assert_eq!(*ticker_seen.lock().expect("lock"), 1);
        assert_eq!(*trade_seen.lock().expect("lock"), 1);
        assert_eq!(*book_seen.lock().expect("lock"), 1);
        assert_eq!(*candle_seen.lock().expect("lock"), 1);
    }

    #[cfg(all(feature = "funding", feature = "liquidations"))]
    #[tokio::test]
    async fn processes_okx_funding_and_liquidation_messages() {
        let funding_seen = Arc::new(Mutex::new(0));
        let liquidation_seen = Arc::new(Mutex::new(0));
        let feed = Okx::new()
            .funding()
            .liquidations()
            .funding_handler(Arc::new(TestFundingHandler {
                seen: funding_seen.clone(),
            }))
            .liquidation_handler(Arc::new(TestLiquidationHandler {
                seen: liquidation_seen.clone(),
            }))
            .symbol("BTC-USDT-PERP")
            .exchange_symbol("BTC-USDT-SWAP")
            .build();

        let funding = serde_json::json!({
            "arg": {"channel": "funding-rate", "instId": "BTC-USDT-SWAP"},
            "data": [{
                "instId": "BTC-USDT-SWAP",
                "fundingRate": "0.0001",
                "fundingTime": "1710000000123",
                "nextFundingRate": "0.0002",
                "nextFundingTime": "1710003600123"
            }]
        });
        let liquidation = serde_json::json!({
            "arg": {"channel": "liquidation-orders", "instId": "BTC-USDT-SWAP"},
            "data": [{
                "instId": "BTC-USDT-SWAP",
                "px": "64000",
                "sz": "1.5",
                "side": "sell",
                "ts": "1710000000123",
                "ordId": "123456"
            }]
        });

        for message in [funding, liquidation] {
            process_okx_text_message(&feed, &message.to_string(), 1710000001.5)
                .await
                .expect("process okx funding and liquidation");
        }

        assert_eq!(*funding_seen.lock().expect("lock"), 1);
        assert_eq!(*liquidation_seen.lock().expect("lock"), 1);
        let subscribe = crate::exchange::okx::adapter::OkxAdapter::subscription_message(&feed);
        assert!(subscribe.contains("funding-rate"));
        assert!(subscribe.contains("liquidation-orders"));
    }

    #[cfg(feature = "openinterest")]
    #[tokio::test]
    async fn processes_okx_open_interest_message() {
        let oi_seen = Arc::new(Mutex::new(0));
        let feed = Okx::new()
            .open_interest()
            .open_interest_handler(Arc::new(TestOpenInterestHandler {
                seen: oi_seen.clone(),
            }))
            .symbol("BTC-USDT-PERP")
            .exchange_symbol("BTC-USDT-SWAP")
            .build();

        let message = serde_json::json!({
            "arg": {"channel": "open-interest", "instId": "BTC-USDT-SWAP"},
            "data": [{
                "instType": "SWAP",
                "instId": "BTC-USDT-SWAP",
                "oi": "12345.6",
                "oiCcy": "BTC",
                "oiUsd": "800000000",
                "ts": "1710000000123"
            }]
        });

        process_okx_text_message(&feed, &message.to_string(), 1710000001.5)
            .await
            .expect("process okx open interest");

        assert_eq!(*oi_seen.lock().expect("lock"), 1);
        let subscribe = crate::exchange::okx::adapter::OkxAdapter::subscription_message(&feed);
        assert!(subscribe.contains("open-interest"));
    }

    #[cfg(feature = "index")]
    #[tokio::test]
    async fn processes_okx_index_price_message() {
        let index_seen = Arc::new(Mutex::new(0));
        let feed = Okx::new()
            .index()
            .index_price_handler(Arc::new(TestIndexPriceHandler {
                seen: index_seen.clone(),
            }))
            .symbol("BTC-USD")
            .exchange_symbol("BTC-USD")
            .build();

        let message = serde_json::json!({
            "arg": {"channel": "index-tickers", "instId": "BTC-USD"},
            "data": [{
                "instId": "BTC-USD",
                "idxPx": "65000.5",
                "open24h": "64000",
                "high24h": "65500",
                "low24h": "63500",
                "ts": "1710000000123"
            }]
        });

        process_okx_text_message(&feed, &message.to_string(), 1710000001.5)
            .await
            .expect("process okx index price");

        assert_eq!(*index_seen.lock().expect("lock"), 1);
        let subscribe = crate::exchange::okx::adapter::OkxAdapter::subscription_message(&feed);
        assert!(subscribe.contains("index-tickers"));
    }

    #[cfg(feature = "orderbook")]
    #[tokio::test]
    async fn processes_okx_l1_book_message() {
        let l1_seen = Arc::new(Mutex::new(0));
        let feed = Okx::new()
            .l1_book()
            .orderbook_handler(Arc::new(TestL1BookHandler {
                seen: l1_seen.clone(),
            }))
            .symbol("BTC-USDT")
            .build();

        let message = serde_json::json!({
            "arg": {"channel": "bbo-tbt", "instId": "BTC-USDT"},
            "data": [{
                "bids": [["64999.10", "1.25", "0", "1"]],
                "asks": [["65000.20", "0.75", "0", "1"]],
                "ts": "1710000000456"
            }]
        });

        process_okx_text_message(&feed, &message.to_string(), 1710000001.5)
            .await
            .expect("process okx l1 book");

        assert_eq!(*l1_seen.lock().expect("lock"), 1);
        assert!(
            crate::exchange::okx::adapter::OkxAdapter::subscription_message(&feed)
                .contains("bbo-tbt")
        );
    }

    #[cfg(all(feature = "ticker", feature = "trade"))]
    #[tokio::test]
    async fn processes_okx_option_messages() {
        let ticker_seen = Arc::new(Mutex::new(0));
        let trade_seen = Arc::new(Mutex::new(0));
        let feed = Okx::new()
            .ticker()
            .trade()
            .ticker_handler(Arc::new(TestTickerHandler {
                seen: ticker_seen.clone(),
            }))
            .trade_handler(Arc::new(TestTradeHandler {
                seen: trade_seen.clone(),
            }))
            .instrument(cryptofeed_core::symbol::Symbol::option(
                "BTC", "USD", "250627", "100000", "C",
            ))
            .exchange_symbol("BTC-USD-250627-100000-C")
            .build();

        let ticker = serde_json::json!({
            "arg": {"channel": "tickers", "instId": "BTC-USD-250627-100000-C"},
            "data": [{ "bidPx": "0.001", "askPx": "0.002", "ts": "1710000000456" }]
        });
        let trade = serde_json::json!({
            "arg": {"channel": "trades", "instId": "BTC-USD-250627-100000-C"},
            "data": [{ "tradeId": "1", "px": "0.0015", "sz": "1", "side": "buy", "ts": "1710000000123" }]
        });

        for message in [ticker, trade] {
            process_okx_text_message(&feed, &message.to_string(), 1710000001.5)
                .await
                .expect("process okx option message");
        }

        assert_eq!(*ticker_seen.lock().expect("lock"), 1);
        assert_eq!(*trade_seen.lock().expect("lock"), 1);
        assert!(
            crate::exchange::okx::adapter::OkxAdapter::subscription_message(&feed)
                .contains("BTC-USD-250627-100000-C")
        );
    }

    #[cfg(all(feature = "ticker", feature = "trade"))]
    #[tokio::test]
    async fn processes_binance_option_messages() {
        let ticker_seen = Arc::new(Mutex::new(0));
        let trade_seen = Arc::new(Mutex::new(0));
        let feed = Binance::new()
            .ticker()
            .trade()
            .ticker_handler(Arc::new(TestTickerHandler {
                seen: ticker_seen.clone(),
            }))
            .trade_handler(Arc::new(TestTradeHandler {
                seen: trade_seen.clone(),
            }))
            .instrument(cryptofeed_core::symbol::Symbol::option(
                "BTC", "USDT", "250627", "100000", "C",
            ))
            .exchange_symbol("BTC-250627-100000-C")
            .build();
        let plan = BinanceAdapter::connection_plans(&feed)
            .expect("connection plan")
            .remove(0);

        let ticker = serde_json::json!({
            "e": "24hrTicker",
            "E": 1710000000456u64,
            "s": "BTC-250627-100000-C",
            "bidOpenPrice": "0.001",
            "askOpenPrice": "0.002",
            "volatility": "0.5"
        });
        let trade = serde_json::json!({
            "e": "trade",
            "E": 1710000000456u64,
            "s": "BTC-250627-100000-C",
            "tradeId": "100",
            "price": "0.0015",
            "quantity": "1",
            "side": -1i64,
            "tradeTime": 1710000000123u64
        });

        for message in [ticker, trade] {
            super::process_binance_text_message_for_plan(
                &feed,
                &plan,
                &message.to_string(),
                1710000001.5,
            )
            .await
            .expect("process binance option message");
        }

        assert_eq!(*ticker_seen.lock().expect("lock"), 1);
        assert_eq!(*trade_seen.lock().expect("lock"), 1);
        assert!(plan.websocket_url.contains("eoptions"));
    }

    #[cfg(all(
        feature = "ticker",
        feature = "trade",
        feature = "orderbook",
        feature = "candles"
    ))]
    #[tokio::test]
    async fn processes_gateio_public_session_messages() {
        let ticker_seen = Arc::new(Mutex::new(0));
        let trade_seen = Arc::new(Mutex::new(0));
        let book_seen = Arc::new(Mutex::new(0));
        let candle_seen = Arc::new(Mutex::new(0));
        let feed = Gateio::new()
            .ticker()
            .trade()
            .l2_book()
            .candles()
            .ticker_handler(Arc::new(TestTickerHandler {
                seen: ticker_seen.clone(),
            }))
            .trade_handler(Arc::new(TestTradeHandler {
                seen: trade_seen.clone(),
            }))
            .orderbook_handler(Arc::new(TestOrderBookHandler {
                seen: book_seen.clone(),
            }))
            .candle_handler(Arc::new(TestCandleHandler {
                seen: candle_seen.clone(),
            }))
            .symbol("BTC-USDT")
            .build();

        let ticker = serde_json::json!({
            "channel": "spot.book_ticker",
            "event": "update",
            "result": {"s": "BTC_USDT", "b": "64999.10", "a": "65000.20", "t": 1710000000}
        });
        let trade = serde_json::json!({
            "channel": "spot.trades",
            "event": "update",
            "result": [{ "id": "1", "currency_pair": "BTC_USDT", "price": "65000.50", "amount": "0.0100", "side": "buy", "create_time_ms": "1710000000123" }]
        });
        let book = serde_json::json!({
            "channel": "spot.order_book_update",
            "event": "update",
            "result": {"s": "BTC_USDT", "b": [["64999.10", "1.25"]], "a": [["65000.20", "0.75"]], "t": 1710000000}
        });
        let candle = serde_json::json!({
            "channel": "spot.candlesticks",
            "event": "update",
            "result": {"t": "1710000000", "v": "12.50", "c": "65050.00", "h": "65100.00", "l": "64900.00", "o": "65000.00", "n": "1m_BTC_USDT", "w": true}
        });

        for message in [ticker, trade, book, candle] {
            process_gateio_text_message(&feed, &message.to_string(), 1710000001.5)
                .await
                .expect("process gateio public message");
        }

        assert_eq!(*ticker_seen.lock().expect("lock"), 1);
        assert_eq!(*trade_seen.lock().expect("lock"), 1);
        assert_eq!(*book_seen.lock().expect("lock"), 1);
        assert_eq!(*candle_seen.lock().expect("lock"), 1);
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn bootstrap_gateio_book_dispatches_snapshot() {
        let feed = Gateio::new().l2_book().symbol("BTC-USDT").build();
        let snapshot = cryptofeed_orderbook::L2BookSnapshot {
            exchange: ExchangeId::Gateio,
            symbol: cryptofeed_core::symbol::Symbol::spot("btc", "usdt"),
            bids: vec![cryptofeed_orderbook::PriceLevel {
                price: rust_decimal::Decimal::from_str_exact("64999.10").unwrap(),
                amount: rust_decimal::Decimal::from_str_exact("1.25").unwrap(),
            }],
            asks: vec![cryptofeed_orderbook::PriceLevel {
                price: rust_decimal::Decimal::from_str_exact("65000.20").unwrap(),
                amount: rust_decimal::Decimal::from_str_exact("0.75").unwrap(),
            }],
            exchange_ts: 1.0,
            received_ts: 2.0,
        };

        let books = super::bootstrap_gateio_book(
            &feed,
            "BTC-USDT",
            crate::exchange::gateio::book_sync::GateioBookSnapshot {
                last_update_id: Some(100),
                generated_ts: 1.0,
                update_ts: 1.0,
                book: snapshot,
            },
            vec![],
        )
        .expect("bootstrap");

        assert_eq!(books.len(), 1);
        assert!(matches!(books[0], L2Book::Snapshot(_)));
        let states = feed.orderbook_states.lock().expect("lock");
        assert_eq!(states.get("BTC-USDT").expect("state").bids().len(), 1);
    }

    #[cfg(feature = "orderbook")]
    #[tokio::test]
    async fn gateio_book_gap_schedules_resync() {
        let feed = Gateio::new().l2_book().symbol("BTC-USDT").build();
        let snapshot = cryptofeed_orderbook::L2BookSnapshot {
            exchange: ExchangeId::Gateio,
            symbol: cryptofeed_core::symbol::Symbol::spot("btc", "usdt"),
            bids: vec![cryptofeed_orderbook::PriceLevel {
                price: rust_decimal::Decimal::from_str_exact("64999.10").unwrap(),
                amount: rust_decimal::Decimal::from_str_exact("1.25").unwrap(),
            }],
            asks: vec![cryptofeed_orderbook::PriceLevel {
                price: rust_decimal::Decimal::from_str_exact("65000.20").unwrap(),
                amount: rust_decimal::Decimal::from_str_exact("0.75").unwrap(),
            }],
            exchange_ts: 1.0,
            received_ts: 2.0,
        };
        super::bootstrap_gateio_book(
            &feed,
            "BTC-USDT",
            crate::exchange::gateio::book_sync::GateioBookSnapshot {
                last_update_id: Some(100),
                generated_ts: 1.0,
                update_ts: 1.0,
                book: snapshot,
            },
            vec![],
        )
        .expect("bootstrap");

        let gap_update = serde_json::json!({
            "channel": "spot.order_book_update",
            "event": "update",
            "result": {
                "s": "BTC_USDT",
                "U": 102u64,
                "u": 103u64,
                "b": [["64998.50", "2.00"]],
                "a": [],
                "t": 1710000000456u64
            }
        });
        let mut receivers = std::collections::HashMap::new();
        let mut pending = std::collections::HashMap::new();

        let handled = super::process_gateio_orderbook_message(
            &feed,
            &gap_update.to_string(),
            1710000001.5,
            &mut receivers,
            &mut pending,
        )
        .await
        .expect("gap schedules resync");

        assert!(handled);
        assert!(
            !feed
                .gateio_book_syncs
                .lock()
                .expect("lock")
                .contains_key("BTC-USDT")
        );
        assert_eq!(pending.get("BTC-USDT").expect("pending").len(), 1);
        assert!(receivers.contains_key("BTC-USDT"));
    }

    #[cfg(feature = "orderbook")]
    #[tokio::test]
    async fn gateio_full_push_replaces_book_without_tearing_down_session() {
        // Gate.io periodically pushes `full: true` complete books (mainnet
        // 2026-05-06). A full push must replace the local book and re-anchor
        // the sequence instead of erroring the session.
        let feed = Gateio::new().l2_book().symbol("BTC-USDT").build();
        let snapshot = cryptofeed_orderbook::L2BookSnapshot {
            exchange: ExchangeId::Gateio,
            symbol: cryptofeed_core::symbol::Symbol::spot("btc", "usdt"),
            bids: vec![cryptofeed_orderbook::PriceLevel {
                price: rust_decimal::Decimal::from_str_exact("64999.10").unwrap(),
                amount: rust_decimal::Decimal::from_str_exact("1.25").unwrap(),
            }],
            asks: vec![cryptofeed_orderbook::PriceLevel {
                price: rust_decimal::Decimal::from_str_exact("65000.20").unwrap(),
                amount: rust_decimal::Decimal::from_str_exact("0.75").unwrap(),
            }],
            exchange_ts: 1.0,
            received_ts: 2.0,
        };
        super::bootstrap_gateio_book(
            &feed,
            "BTC-USDT",
            crate::exchange::gateio::book_sync::GateioBookSnapshot {
                last_update_id: Some(100),
                generated_ts: 1.0,
                update_ts: 1.0,
                book: snapshot,
            },
            vec![],
        )
        .expect("bootstrap");

        let full_push = serde_json::json!({
            "channel": "spot.order_book_update",
            "event": "update",
            "result": {
                "t": 1710000001456u64,
                "U": 101u64,
                "u": 150u64,
                "s": "BTC_USDT",
                "b": [["64998.00", "3.00"]],
                "a": [["65001.00", "2.00"]],
                "full": true
            }
        });
        let mut receivers = std::collections::HashMap::new();
        let mut pending = std::collections::HashMap::new();

        let handled = super::process_gateio_orderbook_message(
            &feed,
            &full_push.to_string(),
            1710000001.5,
            &mut receivers,
            &mut pending,
        )
        .await
        .expect("full push applies in-session");

        assert!(handled);
        let state = feed
            .orderbook_states
            .lock()
            .expect("lock")
            .get("BTC-USDT")
            .expect("state")
            .clone();
        assert_eq!(state.bids().len(), 1);
        assert_eq!(state.bids()[0].price.to_string(), "64998.00");

        // The delta right after the full push bridges from the new anchor.
        let next_delta = serde_json::json!({
            "channel": "spot.order_book_update",
            "event": "update",
            "result": {
                "t": 1710000002456u64,
                "U": 151u64,
                "u": 155u64,
                "s": "BTC_USDT",
                "b": [["64997.00", "4.00"]],
                "a": [],
            }
        });
        let handled = super::process_gateio_orderbook_message(
            &feed,
            &next_delta.to_string(),
            1710000002.5,
            &mut receivers,
            &mut pending,
        )
        .await
        .expect("delta after full push applies");

        assert!(handled);
        assert_eq!(
            feed.gateio_book_syncs
                .lock()
                .expect("lock")
                .get("BTC-USDT")
                .expect("sync")
                .last_update_id(),
            Some(155)
        );
    }

    #[cfg(feature = "orderbook")]
    #[tokio::test]
    async fn gateio_non_bridging_bootstrap_schedules_bounded_resnapshot() {
        // Reproduces the 2026-08-04 live smoke failure signature: a REST
        // snapshot whose id cannot be bridged by the buffered deltas. The
        // bootstrap now triggers a bounded in-session resnapshot instead of
        // tearing the session down; the bridge rule is never weakened.
        let feed = Gateio::new().l2_book().symbol("BTC-USDT").build();
        let plan = crate::exchange::gateio::adapter::GateioAdapter::connection_plans(&feed)
            .expect("connection plan")
            .remove(0);
        let snapshot = cryptofeed_orderbook::L2BookSnapshot {
            exchange: ExchangeId::Gateio,
            symbol: cryptofeed_core::symbol::Symbol::spot("btc", "usdt"),
            bids: vec![cryptofeed_orderbook::PriceLevel {
                price: rust_decimal::Decimal::from_str_exact("64999.10").unwrap(),
                amount: rust_decimal::Decimal::from_str_exact("1.25").unwrap(),
            }],
            asks: vec![cryptofeed_orderbook::PriceLevel {
                price: rust_decimal::Decimal::from_str_exact("65000.20").unwrap(),
                amount: rust_decimal::Decimal::from_str_exact("0.75").unwrap(),
            }],
            exchange_ts: 1.0,
            received_ts: 2.0,
        };
        let non_bridging = crate::exchange::gateio::book_sync::GateioDepthDelta {
            first_update_id: 102,
            last_update_id: 103,
            ts: 1.0,
            book: cryptofeed_orderbook::L2BookDelta {
                exchange: ExchangeId::Gateio,
                symbol: cryptofeed_core::symbol::Symbol::spot("btc", "usdt"),
                bids: vec![cryptofeed_orderbook::PriceLevel {
                    price: rust_decimal::Decimal::from_str_exact("64998.50").unwrap(),
                    amount: rust_decimal::Decimal::from_str_exact("2.00").unwrap(),
                }],
                asks: vec![],
                exchange_ts: 1.0,
                received_ts: 2.0,
            },
        };
        let (tx, rx) = tokio::sync::oneshot::channel();
        tx.send(Ok(crate::exchange::gateio::book_sync::GateioBookSnapshot {
            last_update_id: Some(100),
            generated_ts: 1.0,
            update_ts: 1.0,
            book: snapshot.clone(),
        }))
        .expect("snapshot result");
        let mut receivers = std::collections::HashMap::new();
        receivers.insert("BTC-USDT".to_owned(), rx);
        let mut pending = std::collections::HashMap::new();
        pending.insert("BTC-USDT".to_owned(), vec![non_bridging]);
        let mut attempts = std::collections::HashMap::new();

        // First poll: bootstrap fails, the resnapshot is scheduled in-session
        // and the buffered deltas are preserved for the next attempt.
        super::poll_gateio_snapshot_bootstraps_for_plan(
            &feed,
            &plan,
            &mut receivers,
            &mut pending,
            &mut attempts,
        )
        .await
        .expect("failure schedules a bounded resnapshot");
        assert!(receivers.contains_key("BTC-USDT"));
        assert_eq!(pending.get("BTC-USDT").expect("pending").len(), 1);
        assert_eq!(attempts.get("BTC-USDT"), Some(&1));

        // The fresh snapshot arrives at a lower id than the deltas; the
        // preserved deltas now bridge and the book becomes consistent.
        let (tx2, rx2) = tokio::sync::oneshot::channel();
        tx2.send(Ok(crate::exchange::gateio::book_sync::GateioBookSnapshot {
            last_update_id: Some(101),
            generated_ts: 1.0,
            update_ts: 1.0,
            book: snapshot,
        }))
        .expect("resnapshot result");
        receivers.insert("BTC-USDT".to_owned(), rx2);
        super::poll_gateio_snapshot_bootstraps_for_plan(
            &feed,
            &plan,
            &mut receivers,
            &mut pending,
            &mut attempts,
        )
        .await
        .expect("bridged bootstrap");
        assert!(!receivers.contains_key("BTC-USDT"));
        assert!(!attempts.contains_key("BTC-USDT"));
        let states = feed.orderbook_states.lock().expect("lock");
        let state = states.get("BTC-USDT").expect("btc-usdt state");
        assert_eq!(state.bids().len(), 2);
        assert_eq!(state.bids()[0].price.to_string(), "64999.10");
        assert_eq!(state.bids()[1].price.to_string(), "64998.50");
    }

    #[cfg(feature = "orderbook")]
    #[tokio::test]
    async fn gateio_resnapshot_limit_exceeded_propagates_session_error() {
        let feed = Gateio::new().l2_book().symbol("BTC-USDT").build();
        let plan = crate::exchange::gateio::adapter::GateioAdapter::connection_plans(&feed)
            .expect("connection plan")
            .remove(0);
        let snapshot = cryptofeed_orderbook::L2BookSnapshot {
            exchange: ExchangeId::Gateio,
            symbol: cryptofeed_core::symbol::Symbol::spot("btc", "usdt"),
            bids: vec![cryptofeed_orderbook::PriceLevel {
                price: rust_decimal::Decimal::from_str_exact("64999.10").unwrap(),
                amount: rust_decimal::Decimal::from_str_exact("1.25").unwrap(),
            }],
            asks: vec![cryptofeed_orderbook::PriceLevel {
                price: rust_decimal::Decimal::from_str_exact("65000.20").unwrap(),
                amount: rust_decimal::Decimal::from_str_exact("0.75").unwrap(),
            }],
            exchange_ts: 1.0,
            received_ts: 2.0,
        };
        let non_bridging = crate::exchange::gateio::book_sync::GateioDepthDelta {
            first_update_id: 102,
            last_update_id: 103,
            ts: 1.0,
            book: cryptofeed_orderbook::L2BookDelta {
                exchange: ExchangeId::Gateio,
                symbol: cryptofeed_core::symbol::Symbol::spot("btc", "usdt"),
                bids: vec![],
                asks: vec![],
                exchange_ts: 1.0,
                received_ts: 2.0,
            },
        };
        let (tx, rx) = tokio::sync::oneshot::channel();
        tx.send(Ok(crate::exchange::gateio::book_sync::GateioBookSnapshot {
            last_update_id: Some(100),
            generated_ts: 1.0,
            update_ts: 1.0,
            book: snapshot,
        }))
        .expect("snapshot result");
        let mut receivers = std::collections::HashMap::new();
        receivers.insert("BTC-USDT".to_owned(), rx);
        let mut pending = std::collections::HashMap::new();
        pending.insert("BTC-USDT".to_owned(), vec![non_bridging]);
        let mut attempts = std::collections::HashMap::new();
        attempts.insert("BTC-USDT".to_owned(), super::GATEIO_MAX_RESNAPSHOTS);

        let err = super::poll_gateio_snapshot_bootstraps_for_plan(
            &feed,
            &plan,
            &mut receivers,
            &mut pending,
            &mut attempts,
        )
        .await
        .expect_err("resnapshot limit exceeded must fail the session");

        assert!(matches!(
            err,
            Error::Parse(message) if message.contains("does not bridge")
        ));
    }

    #[cfg(feature = "orderbook")]
    #[tokio::test]
    async fn gateio_derivative_plan_keeps_product_aware_book_key() {
        let feed = Gateio::new()
            .l2_book()
            .symbol("BTC-USDT-PERP")
            .exchange_symbol("BTC_USDT")
            .build();
        let plan = crate::exchange::gateio::adapter::GateioAdapter::connection_plans(&feed)
            .expect("connection plan")
            .remove(0);
        let message = serde_json::json!({
            "channel": "futures.order_book_update",
            "event": "update",
            "result": {
                "contract": "BTC_USDT",
                "s": "BTC_USDT",
                "U": 101u64,
                "u": 102u64,
                "b": [["64999.10", "1"]],
                "a": [["65000.20", "1"]],
                "t": 1710000000456u64
            }
        });
        let mut receivers = std::collections::HashMap::new();
        let mut pending = std::collections::HashMap::new();

        let handled = super::process_gateio_orderbook_message_for_plan(
            &feed,
            &plan,
            &message.to_string(),
            1710000001.5,
            &mut receivers,
            &mut pending,
        )
        .await
        .expect("derivative update");

        assert!(handled);
        assert!(pending.contains_key("BTC-USDT-PERP"));
        assert!(!pending.contains_key("BTC-USDT"));
    }

    #[cfg(all(feature = "ticker", feature = "trade", feature = "orderbook"))]
    #[tokio::test]
    async fn okx_book_gap_returns_error() {
        let feed = Okx::new().l2_book().symbol("BTC-USDT").build();
        let snapshot = serde_json::json!({
            "arg": {"channel": "books", "instId": "BTC-USDT"},
            "action": "snapshot",
            "data": [{
                "bids": [["64999.10", "1.25", "0", "1"]],
                "asks": [["65000.20", "0.75", "0", "1"]],
                "ts": "1710000000456",
                "seqId": 100i64,
                "prevSeqId": -1i64
            }]
        });
        let gap_update = serde_json::json!({
            "arg": {"channel": "books", "instId": "BTC-USDT"},
            "action": "update",
            "data": [{
                "bids": [["64999.10", "1.25", "0", "1"]],
                "asks": [["65000.20", "0.75", "0", "1"]],
                "ts": "1710000000457",
                "seqId": 102i64,
                "prevSeqId": 999i64
            }]
        });

        process_okx_text_message(&feed, &snapshot.to_string(), 1710000001.5)
            .await
            .expect("snapshot");
        let err = process_okx_text_message(&feed, &gap_update.to_string(), 1710000001.6)
            .await
            .expect_err("gap should fail");

        match err {
            Error::Parse(message) => assert!(message.contains("sequence gap")),
            _ => panic!("unexpected error variant"),
        }
    }

    #[cfg(all(feature = "ticker", feature = "trade", feature = "orderbook"))]
    #[tokio::test]
    async fn dispatches_bitget_ticker_events() {
        let seen = Arc::new(Mutex::new(0));
        let feed = Bitget::new()
            .ticker()
            .ticker_handler(Arc::new(TestTickerHandler { seen: seen.clone() }))
            .symbol("BTC-USDT")
            .build();

        let message = serde_json::json!({
            "arg": {"instType": "spot", "topic": "ticker", "symbol": "BTCUSDT"},
            "data": [{ "bid1Price": "64999.10", "ask1Price": "65000.20" }],
            "ts": "1710000000456"
        });

        let event = BitgetAdapter::parse_message(&message, 1710000001.5).expect("bitget ticker");
        assert!(matches!(event, BitgetEvent::Ticker(_)));

        super::dispatch_bitget_event(&feed, event).await;
        assert_eq!(*seen.lock().expect("lock"), 1);
    }

    #[cfg(all(feature = "ticker", feature = "trade", feature = "orderbook"))]
    #[tokio::test]
    async fn ignores_bitget_subscribe_ack_message() {
        let trade_seen = Arc::new(Mutex::new(0));
        let feed = Bitget::new()
            .trade()
            .trade_handler(Arc::new(TestTradeHandler {
                seen: trade_seen.clone(),
            }))
            .symbol("BTC-USDT")
            .build();

        let message = serde_json::json!({
            "event": "subscribe",
            "arg": { "instType": "spot", "topic": "books", "symbol": "BTCUSDT" },
            "connId": "4a87f8f5"
        });

        process_bitget_text_message(&feed, &message.to_string(), 1710000001.5)
            .await
            .expect("process subscribe ack");

        assert_eq!(*trade_seen.lock().expect("lock"), 0);
    }

    #[cfg(all(feature = "ticker", feature = "trade", feature = "orderbook"))]
    #[tokio::test]
    async fn dispatches_bitget_l2_book_events() {
        let seen = Arc::new(Mutex::new(0));
        let feed = Bitget::new()
            .l2_book()
            .orderbook_handler(Arc::new(TestOrderBookHandler { seen: seen.clone() }))
            .symbol("BTC-USDT")
            .build();

        let message = serde_json::json!({
            "arg": {"channel": "books", "instId": "BTCUSDT"},
            "data": [{
                "bids": [["64999.10", "1.25"]],
                "asks": [["65000.20", "0.75"]],
                "ts": "1710000000456"
            }]
        });

        let event = BitgetAdapter::parse_message(&message, 1710000001.5).expect("bitget book");
        assert!(matches!(event, BitgetEvent::L2Book(_)));

        super::dispatch_bitget_event(&feed, event).await;
        assert_eq!(*seen.lock().expect("lock"), 1);
        let states = feed.orderbook_states.lock().expect("lock");
        let state = states.get("BTC-USDT").expect("btc-usdt state");
        assert_eq!(state.bids().len(), 1);
        assert_eq!(state.asks().len(), 1);
    }

    #[cfg(all(feature = "ticker", feature = "trade", feature = "orderbook"))]
    #[tokio::test]
    async fn bitget_book_gap_returns_error() {
        let feed = Bitget::new().l2_book().symbol("BTC-USDT").build();
        let snapshot = serde_json::json!({
            "arg": {"instType": "spot", "topic": "books", "symbol": "BTCUSDT"},
            "action": "snapshot",
            "data": [{
                "b": [["64999.10", "1.25"]],
                "a": [["65000.20", "0.75"]],
                "seq": 100u64,
                "pseq": 0u64,
                "ts": "1710000000456"
            }]
        });
        let gap_update = serde_json::json!({
            "arg": {"instType": "spot", "topic": "books", "symbol": "BTCUSDT"},
            "action": "update",
            "data": [{
                "b": [["64999.10", "1.25"]],
                "a": [["65000.20", "0.75"]],
                "seq": 102u64,
                "pseq": 999u64,
                "ts": "1710000000457"
            }]
        });

        process_bitget_text_message(&feed, &snapshot.to_string(), 1710000001.5)
            .await
            .expect("snapshot");
        let err = process_bitget_text_message(&feed, &gap_update.to_string(), 1710000001.6)
            .await
            .expect_err("gap should fail");

        match err {
            Error::Parse(message) => assert!(message.contains("sequence gap")),
            _ => panic!("unexpected error variant"),
        }
    }

    #[cfg(all(feature = "liquidations", feature = "orderbook"))]
    #[tokio::test]
    async fn processes_bitget_liquidation_message() {
        let liquidation_seen = Arc::new(Mutex::new(0));
        let feed = Bitget::new()
            .liquidations()
            .liquidation_handler(Arc::new(TestLiquidationHandler {
                seen: liquidation_seen.clone(),
            }))
            .symbol("BTC-USDT-PERP")
            .exchange_symbol("BTCUSDT")
            .build();

        // The v3 liquidation stream is instType-scoped: no symbol in the
        // subscription arg, product identity resolved per row.
        let message = serde_json::json!({
            "arg": {"instType": "usdt-futures", "topic": "liquidation"},
            "data": [{
                "symbol": "BTCUSDT",
                "side": "buy",
                "price": "64000",
                "amount": "37.722858",
                "ts": "1736371332162"
            }],
            "action": "update",
            "ts": 1736371332162i64
        });

        process_bitget_text_message(&feed, &message.to_string(), 1736371332.2)
            .await
            .expect("process bitget liquidation");

        assert_eq!(*liquidation_seen.lock().expect("lock"), 1);
        let subscribe =
            crate::exchange::bitget::adapter::BitgetAdapter::subscription_message(&feed);
        assert!(subscribe.contains("\"topic\":\"liquidation\""));
        assert!(!subscribe.contains("\"symbol\""));
    }

    #[cfg(feature = "liquidations")]
    #[tokio::test]
    async fn bitget_market_wide_liquidations_are_filtered_to_feed_symbols() {
        let mut handler = FeedHandler::new();
        let mut events = handler.subscribe();
        handler.add_feed(
            Bitget::new()
                .liquidations()
                .symbol("BTC-USDT-PERP")
                .exchange_symbol("BTCUSDT")
                .build(),
        );
        let feed = handler.into_feeds().pop().expect("feed");
        let message = serde_json::json!({
            "arg": {"instType": "usdt-futures", "topic": "liquidation"},
            "data": [
                {"symbol": "ETHUSDT", "side": "sell", "price": "3000", "amount": "300", "ts": "1736371332161"},
                {"symbol": "BTCUSDT", "side": "buy", "price": "60000", "amount": "60", "ts": "1736371332162"}
            ],
            "action": "update"
        });

        process_bitget_text_message(&feed, &message.to_string(), 1736371332.2)
            .await
            .expect("process liquidations");
        match events.recv().await.expect("BTC liquidation") {
            crate::feed::FeedEvent::Liquidation(value) => {
                assert_eq!(value.symbol.as_str(), "BTC-USDT-PERP");
            }
            _ => panic!("expected liquidation"),
        }
        assert!(matches!(
            events.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
    }

    // ---------- full-session WebSocket doubles ----------

    type DoubleStream = WebSocketStream<tokio::io::DuplexStream>;

    /// A deterministic session pair: the client side is driven by the
    /// `consume_*_session_with` helpers, the server side by the test.
    async fn duplex_session(exchange: ExchangeId) -> (Session<DoubleStream>, DoubleStream) {
        let (client, server) = duplex(4096);
        let client = WebSocketStream::from_raw_socket(client, Role::Client, None);
        let server = WebSocketStream::from_raw_socket(server, Role::Server, None);
        let (client, server) = tokio::join!(client, server);
        let url = Url::parse("wss://double.test/ws").expect("test url");
        (
            Session::new(client, HeartbeatPolicy::for_exchange(exchange, &url)),
            server,
        )
    }

    /// Polls a handler-side condition until it holds or the timeout fires.
    async fn wait_for_seen(check: impl Fn() -> bool) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if check() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("condition not reached in time");
    }

    #[cfg(all(feature = "ticker", feature = "trade", feature = "orderbook"))]
    #[tokio::test]
    async fn binance_full_session_double_delivers_data_and_bootstraps_book() {
        let ticker_seen = Arc::new(Mutex::new(0));
        let trade_seen = Arc::new(Mutex::new(0));
        let book_seen = Arc::new(Mutex::new(0));
        let feed = Binance::new()
            .ticker()
            .trade()
            .l2_book()
            .ticker_handler(Arc::new(TestTickerHandler {
                seen: ticker_seen.clone(),
            }))
            .trade_handler(Arc::new(TestTradeHandler {
                seen: trade_seen.clone(),
            }))
            .orderbook_handler(Arc::new(TestOrderBookHandler {
                seen: book_seen.clone(),
            }))
            .symbol("BTC-USDT")
            .build();
        let plan = BinanceAdapter::connection_plans(&feed)
            .expect("connection plan")
            .remove(0);
        let (mut client, mut server) = duplex_session(ExchangeId::Binance).await;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (snapshot_tx, snapshot_rx) = oneshot::channel();
        let mut receivers = std::collections::HashMap::new();
        receivers.insert("BTC-USDT".to_owned(), snapshot_rx);
        let pending_deltas = std::collections::HashMap::new();
        let feed_for_task = feed.clone();

        let session = tokio::spawn(async move {
            super::consume_binance_session_with(
                feed_for_task,
                plan,
                shutdown_rx,
                &mut client,
                receivers,
                pending_deltas,
                std::collections::HashMap::new(),
            )
            .await
        });

        // The depth update is buffered while the snapshot fetch is in flight.
        let depth_update = serde_json::json!({
            "e": "depthUpdate",
            "s": "BTCUSDT",
            "U": 101u64,
            "u": 102u64,
            "b": [["64999.10", "0"], ["64998.50", "2.00"]],
            "a": [["65000.20", "1.00"]]
        });
        server
            .send(Message::Text(depth_update.to_string()))
            .await
            .expect("send depth update");
        // The snapshot result lands; the next loop poll bootstraps the book.
        let snapshot = cryptofeed_orderbook::L2BookSnapshot {
            exchange: ExchangeId::Binance,
            symbol: cryptofeed_core::symbol::Symbol::spot("btc", "usdt"),
            bids: vec![cryptofeed_orderbook::PriceLevel {
                price: rust_decimal::Decimal::from_str_exact("64999.10").unwrap(),
                amount: rust_decimal::Decimal::from_str_exact("1.25").unwrap(),
            }],
            asks: vec![cryptofeed_orderbook::PriceLevel {
                price: rust_decimal::Decimal::from_str_exact("65000.20").unwrap(),
                amount: rust_decimal::Decimal::from_str_exact("0.75").unwrap(),
            }],
            exchange_ts: 1.0,
            received_ts: 2.0,
        };
        snapshot_tx
            .send(Ok((100u64, snapshot)))
            .expect("snapshot result");
        // Spot book ticker has no fabricated `e`/`E` fields.
        let ticker = serde_json::json!({
            "u": 1u64,
            "s": "BTCUSDT",
            "b": "64999.10",
            "B": "1.25",
            "a": "65000.20",
            "A": "0.75"
        });
        let trade = serde_json::json!({
            "e": "aggTrade",
            "s": "BTCUSDT",
            "a": 12345,
            "p": "65000.50",
            "q": "0.01000000",
            "T": 1710000000123u64,
            "m": false
        });
        server
            .send(Message::Text(ticker.to_string()))
            .await
            .expect("send ticker");
        server
            .send(Message::Text(trade.to_string()))
            .await
            .expect("send trade");

        wait_for_seen(|| {
            *book_seen.lock().expect("lock") == 2
                && *ticker_seen.lock().expect("lock") == 1
                && *trade_seen.lock().expect("lock") == 1
        })
        .await;

        shutdown_tx.send(true).expect("shutdown");
        session
            .await
            .expect("session task")
            .expect("clean session shutdown");
        assert!(matches!(
            server.next().await,
            Some(Ok(Message::Close(_))) | None
        ));

        let states = feed.orderbook_states.lock().expect("lock");
        let state = states.get("BTC-USDT").expect("btc-usdt state");
        assert_eq!(state.bids().len(), 1);
        assert_eq!(state.bids()[0].price.to_string(), "64998.50");
        assert_eq!(state.asks().len(), 1);
    }

    #[cfg(feature = "trade")]
    #[tokio::test]
    async fn bitget_full_session_subscribes_acknowledges_and_delivers() {
        let trade_seen = Arc::new(Mutex::new(0));
        let feed = Bitget::new()
            .trade()
            .trade_handler(Arc::new(TestTradeHandler {
                seen: trade_seen.clone(),
            }))
            .symbol("BTC-USDT")
            .build();
        let (mut client, mut server) = duplex_session(ExchangeId::Bitget).await;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let session = tokio::spawn(async move {
            super::consume_bitget_session_with(feed, shutdown_rx, &mut client).await
        });

        let subscribe = match server.next().await {
            Some(Ok(Message::Text(text))) => text,
            other => panic!("expected subscribe frame, got {other:?}"),
        };
        assert!(subscribe.contains("subscribe"));
        server
            .send(Message::Text(
                r#"{"event":"subscribe","arg":{"instType":"spot","channel":"trade","symbol":"BTCUSDT"},"connId":"4a87f8f5"}"#
                    .into(),
            ))
            .await
            .expect("send subscribe ack");
        server
            .send(Message::Text(
                r#"{"arg":{"channel":"trade","instId":"BTCUSDT"},"data":[["1710000000123","65000.50","0.0100","buy"]]}"#
                    .into(),
            ))
            .await
            .expect("send trade");

        wait_for_seen(|| *trade_seen.lock().expect("lock") == 1).await;

        shutdown_tx.send(true).expect("shutdown");
        session
            .await
            .expect("session task")
            .expect("clean session shutdown");
        assert!(matches!(
            server.next().await,
            Some(Ok(Message::Close(_))) | None
        ));
    }

    #[tokio::test]
    async fn bybit_option_feed_keeps_symbols_on_the_option_url() {
        // Regression: `bybit_feed_for_url` classified the `/option` URL as
        // Spot, filtering out every option symbol — the option session then
        // subscribed to an empty args list and terminally failed.
        let feed = Bybit::new()
            .ticker()
            .instrument(cryptofeed_core::symbol::Symbol::option(
                "BTC", "USDC", "30JUN26", "70000", "C",
            ))
            .exchange_symbol("BTC-30JUN26-70000-C")
            .build();
        let urls = crate::exchange::bybit::adapter::BybitAdapter::subscription_urls(&feed);
        assert_eq!(urls.len(), 1);
        assert!(urls[0].ends_with("/option"));

        let planned = super::bybit_feed_for_url(&feed, &urls[0]);
        assert_eq!(planned.symbols.len(), 1);
        assert_eq!(planned.symbols[0].as_str(), "BTC-USDC-30JUN26-70000-C");

        // The same feed planned against the spot URL keeps nothing.
        let planned_spot =
            super::bybit_feed_for_url(&feed, "wss://stream.bybit.com/v5/public/spot");
        assert!(planned_spot.symbols.is_empty());
    }

    #[cfg(feature = "trade")]
    #[tokio::test]
    async fn bybit_full_session_subscribes_acknowledges_and_delivers() {
        let trade_seen = Arc::new(Mutex::new(0));
        let feed = Bybit::new()
            .trade()
            .trade_handler(Arc::new(TestTradeHandler {
                seen: trade_seen.clone(),
            }))
            .symbol("BTC-USDT")
            .build();
        let (mut client, mut server) = duplex_session(ExchangeId::Bybit).await;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let session = tokio::spawn(async move {
            super::consume_bybit_session_with(feed, shutdown_rx, &mut client).await
        });

        let subscribe = match server.next().await {
            Some(Ok(Message::Text(text))) => text,
            other => panic!("expected subscribe frame, got {other:?}"),
        };
        assert!(subscribe.contains("subscribe"));
        server
            .send(Message::Text(
                r#"{"success":true,"ret_msg":"","op":"subscribe","conn_id":"a1b2c3"}"#.into(),
            ))
            .await
            .expect("send subscribe ack");
        let trade = serde_json::json!({
            "topic": "publicTrade.BTCUSDT",
            "type": "snapshot",
            "ts": 1672304486868i64,
            "data": [{
                "T": 1672304486865i64,
                "s": "BTCUSDT",
                "S": "Buy",
                "v": "0.001",
                "p": "16578.50",
                "i": "20f43950"
            }]
        });
        server
            .send(Message::Text(trade.to_string()))
            .await
            .expect("send trade");

        wait_for_seen(|| *trade_seen.lock().expect("lock") == 1).await;

        shutdown_tx.send(true).expect("shutdown");
        session
            .await
            .expect("session task")
            .expect("clean session shutdown");
        assert!(matches!(
            server.next().await,
            Some(Ok(Message::Close(_))) | None
        ));
    }

    #[cfg(feature = "trade")]
    #[tokio::test]
    async fn okx_full_session_subscribes_acknowledges_and_delivers() {
        let trade_seen = Arc::new(Mutex::new(0));
        let feed = Okx::new()
            .trade()
            .trade_handler(Arc::new(TestTradeHandler {
                seen: trade_seen.clone(),
            }))
            .symbol("BTC-USDT")
            .build();
        let (mut client, mut server) = duplex_session(ExchangeId::Okx).await;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let session = tokio::spawn(async move {
            super::consume_okx_session_with(feed, shutdown_rx, &mut client).await
        });

        let subscribe = match server.next().await {
            Some(Ok(Message::Text(text))) => text,
            other => panic!("expected subscribe frame, got {other:?}"),
        };
        assert!(subscribe.contains("subscribe"));
        server
            .send(Message::Text(
                r#"{"event":"subscribe","arg":{"channel":"trades","instId":"BTC-USDT"},"connId":"9z1x2c"}"#
                    .into(),
            ))
            .await
            .expect("send subscribe ack");
        let trade = serde_json::json!({
            "arg": {"channel": "trades", "instId": "BTC-USDT"},
            "data": [{ "tradeId": "1", "px": "65000.50", "sz": "0.0100", "side": "buy", "ts": "1710000000123" }]
        });
        server
            .send(Message::Text(trade.to_string()))
            .await
            .expect("send trade");

        wait_for_seen(|| *trade_seen.lock().expect("lock") == 1).await;

        shutdown_tx.send(true).expect("shutdown");
        session
            .await
            .expect("session task")
            .expect("clean session shutdown");
        assert!(matches!(
            server.next().await,
            Some(Ok(Message::Close(_))) | None
        ));
    }

    #[cfg(feature = "trade")]
    #[tokio::test]
    async fn gateio_full_session_subscribes_acknowledges_and_delivers() {
        let trade_seen = Arc::new(Mutex::new(0));
        let feed = Gateio::new()
            .trade()
            .trade_handler(Arc::new(TestTradeHandler {
                seen: trade_seen.clone(),
            }))
            .symbol("BTC-USDT")
            .build();
        let plan = crate::exchange::gateio::adapter::GateioAdapter::connection_plans(&feed)
            .expect("connection plan")
            .remove(0);
        let (mut client, mut server) = duplex_session(ExchangeId::Gateio).await;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let receivers = std::collections::HashMap::new();
        let pending_deltas = std::collections::HashMap::new();

        let session = tokio::spawn(async move {
            super::consume_gateio_session_with(
                feed,
                plan,
                shutdown_rx,
                &mut client,
                receivers,
                pending_deltas,
            )
            .await
        });

        let subscribe = match server.next().await {
            Some(Ok(Message::Text(text))) => text,
            other => panic!("expected subscribe frame, got {other:?}"),
        };
        assert!(subscribe.contains("spot.trades"));
        server
            .send(Message::Text(
                r#"{"time":1710000000,"channel":"spot.trades","event":"subscribe","result":{"status":"success"}}"#
                    .into(),
            ))
            .await
            .expect("send subscribe ack");
        let trade = serde_json::json!({
            "channel": "spot.trades",
            "event": "update",
            "result": [{ "id": "1", "currency_pair": "BTC_USDT", "price": "65000.50", "amount": "0.0100", "side": "buy", "create_time_ms": "1710000000123" }]
        });
        server
            .send(Message::Text(trade.to_string()))
            .await
            .expect("send trade");

        wait_for_seen(|| *trade_seen.lock().expect("lock") == 1).await;

        shutdown_tx.send(true).expect("shutdown");
        session
            .await
            .expect("session task")
            .expect("clean session shutdown");
        assert!(matches!(
            server.next().await,
            Some(Ok(Message::Close(_))) | None
        ));
    }
    #[cfg(all(
        feature = "orderbook",
        feature = "funding",
        feature = "index",
        feature = "markprice",
        feature = "openinterest"
    ))]
    #[tokio::test]
    async fn bitget_new_channels_dispatch_without_ticker_or_l2_subscription() {
        let mut handler = FeedHandler::new();
        let mut events = handler.subscribe();
        handler.add_feed(
            Bitget::new()
                .l1_book()
                .funding()
                .open_interest()
                .index()
                .mark_price()
                .symbol("BTC-USDT-PERP")
                .exchange_symbol("BTCUSDT")
                .build(),
        );
        let feed = handler.into_feeds().pop().unwrap();
        let ticker = serde_json::json!({"arg":{"instType":"usdt-futures","topic":"ticker","symbol":"BTCUSDT"},
            "data":[{"fundingRate":"0.0001","markPrice":"65000","indexPrice":"64999","openInterest":"123","nextFundingTime":"1736373600000"}],"ts":1736371332162i64});
        process_bitget_text_message(&feed, &ticker.to_string(), 1736371332.2)
            .await
            .unwrap();
        let book = serde_json::json!({"arg":{"instType":"usdt-futures","topic":"books1","symbol":"BTCUSDT"},"action":"snapshot",
            "data":[{"b":[["64999","1"]],"a":[["65000","2"]],"seq":7,"ts":"1736371332162"}]});
        process_bitget_text_message(&feed, &book.to_string(), 1736371332.2)
            .await
            .unwrap();
        let mut counts = [0; 5];
        while let Ok(event) = events.try_recv() {
            match event {
                crate::feed::FeedEvent::Funding(_) => counts[0] += 1,
                crate::feed::FeedEvent::OpenInterest(_) => counts[1] += 1,
                crate::feed::FeedEvent::IndexPrice(_) => counts[2] += 1,
                crate::feed::FeedEvent::MarkPrice(_) => counts[3] += 1,
                crate::feed::FeedEvent::L1Book(_) => counts[4] += 1,
                _ => panic!("unsubscribed event"),
            }
        }
        assert_eq!(counts, [1; 5]);
        assert!(feed.bitget_book_syncs.lock().unwrap().is_empty());
    }

    #[cfg(feature = "orderbook")]
    #[tokio::test]
    async fn bitget_l1_and_l2_keep_independent_books_and_shared_books1_emits_once() {
        for depth in [None, Some(1)] {
            let mut handler = FeedHandler::new();
            let mut events = handler.subscribe();
            let mut feed = Bitget::new().l1_book().l2_book().symbol("BTC-USDT").build();
            feed.l2_book_depth = depth;
            handler.add_feed(feed);
            let feed = handler.into_feeds().pop().unwrap();
            let subscribe: serde_json::Value =
                serde_json::from_str(&BitgetAdapter::subscription_message(&feed)).unwrap();
            assert_eq!(
                subscribe["args"].as_array().unwrap().len(),
                if depth.is_some() { 1 } else { 2 }
            );
            let l1 = serde_json::json!({"arg":{"instType":"spot","topic":"books1","symbol":"BTCUSDT"},"action":"snapshot",
                "data":[{"b":[["64999","1"]],"a":[["65000","2"]],"seq":99,"ts":"1736371332162"}]});
            process_bitget_text_message(&feed, &l1.to_string(), 1736371332.2)
                .await
                .unwrap();
            assert!(matches!(
                events.try_recv().unwrap(),
                crate::feed::FeedEvent::L1Book(_)
            ));
            if depth.is_some() {
                assert!(matches!(
                    events.try_recv().unwrap(),
                    crate::feed::FeedEvent::L2Book(_)
                ));
            } else {
                assert!(feed.bitget_book_syncs.lock().unwrap().is_empty());
                let mut l2 = l1.clone();
                l2["arg"]["topic"] = serde_json::json!("books");
                l2["data"][0]["seq"] = serde_json::json!(1);
                process_bitget_text_message(&feed, &l2.to_string(), 1736371332.2)
                    .await
                    .unwrap();
                assert!(matches!(
                    events.try_recv().unwrap(),
                    crate::feed::FeedEvent::L2Book(_)
                ));
                assert_eq!(feed.bitget_book_syncs.lock().unwrap().len(), 1);
            }
            assert!(events.try_recv().is_err());
        }
    }

    #[cfg(feature = "liquidations")]
    #[tokio::test]
    async fn gateio_public_liquidation_batches_resolve_each_contract_and_filter_unsubscribed() {
        let mut handler = FeedHandler::new();
        let mut events = handler.subscribe();
        handler.add_feed(
            Gateio::new()
                .liquidations()
                .symbol("BTC-USDT-PERP")
                .exchange_symbol("BTC_USDT")
                .symbol("ETH-USDT-PERP")
                .exchange_symbol("ETH_USDT")
                .build(),
        );
        let feed = handler.into_feeds().pop().unwrap();
        let plan = super::GateioAdapter::connection_plans(&feed)
            .unwrap()
            .remove(0);
        let message = serde_json::json!({"channel":"futures.public_liquidates","event":"update","result":[
            {"contract":"SOL_USDT","size":"-1","price":"150","time_ms":1736371332161i64},
            {"contract":"ETH_USDT","size":"2","price":"3000","time_ms":1736371332162i64},
            {"contract":"BTC_USDT","size":"-3","price":"65000","time_ms":1736371332163i64}]});
        super::process_gateio_text_message_for_plan(
            &feed,
            &plan,
            &message.to_string(),
            1736371332.2,
        )
        .await
        .unwrap();
        for symbol in ["ETH-USDT-PERP", "BTC-USDT-PERP"] {
            let crate::feed::FeedEvent::Liquidation(value) = events.try_recv().unwrap() else {
                panic!("liquidation");
            };
            assert_eq!(value.symbol.as_str(), symbol);
        }
        assert!(events.try_recv().is_err());
        let ack = serde_json::json!({"channel":"futures.public_liquidates","event":"subscribe","result":{"status":"success"}});
        super::process_gateio_text_message_for_plan(&feed, &plan, &ack.to_string(), 1.0)
            .await
            .unwrap();
        assert!(events.try_recv().is_err());
    }

    #[cfg(feature = "index")]
    #[tokio::test]
    async fn okx_index_dispatches_to_each_subscribed_dated_contract() {
        let mut handler = FeedHandler::new();
        let mut events = handler.subscribe();
        handler.add_feed(
            Okx::new()
                .index()
                .symbol("BTC-USD-261225")
                .exchange_symbol("BTC-USD-261225")
                .symbol("BTC-USD-270326")
                .exchange_symbol("BTC-USD-270326")
                .build(),
        );
        let feed = handler.into_feeds().pop().unwrap();
        let message = serde_json::json!({"arg":{"channel":"index-tickers","instId":"BTC-USD"},
            "data":[{"instId":"BTC-USD","idxPx":"65000.1","ts":"1736371332162"}]});
        process_okx_text_message(&feed, &message.to_string(), 1736371332.2)
            .await
            .unwrap();
        for symbol in ["BTC-USD-261225", "BTC-USD-270326"] {
            let crate::feed::FeedEvent::IndexPrice(value) = events.try_recv().unwrap() else {
                panic!("index");
            };
            assert_eq!(value.symbol.as_str(), symbol);
        }
        assert!(events.try_recv().is_err());
    }
    #[cfg(all(feature = "orderbook", feature = "liquidations"))]
    #[tokio::test]
    async fn bitget_dated_books_and_liquidations_retain_catalog_identity() {
        let mut handler = FeedHandler::new();
        let mut events = handler.subscribe();
        handler.add_feed(
            Bitget::new()
                .l2_book()
                .liquidations()
                .symbol("BTC-USDT-261225")
                .exchange_symbol("BTCUSDT261225")
                .build(),
        );
        let feed = handler.into_feeds().pop().unwrap();
        let snapshot = serde_json::json!({"arg":{"instType":"usdt-futures","topic":"books","symbol":"BTCUSDT261225"},
            "action":"snapshot","data":[{"b":[["65000","1"]],"a":[["65001","2"]],"seq":7,"pseq":0,"ts":"1736371332162"}]});
        process_bitget_text_message(&feed, &snapshot.to_string(), 1736371332.2)
            .await
            .unwrap();
        let crate::feed::FeedEvent::L2Book(book) = events.try_recv().expect("dated snapshot")
        else {
            panic!("book");
        };
        assert_eq!(book.symbol().as_str(), "BTC-USDT-261225");
        let liquidation = serde_json::json!({"arg":{"instType":"usdt-futures","topic":"liquidation"},"action":"update",
            "data":[{"symbol":"BTCUSDT","side":"buy","price":"65000","amount":"65000","ts":"1736371332162"},
                    {"symbol":"BTCUSDT261225","side":"sell","price":"65000","amount":"65000","ts":"1736371332163"}]});
        process_bitget_text_message(&feed, &liquidation.to_string(), 1736371332.2)
            .await
            .unwrap();
        let crate::feed::FeedEvent::Liquidation(value) =
            events.try_recv().expect("dated liquidation")
        else {
            panic!("liquidation");
        };
        assert_eq!(value.symbol.as_str(), "BTC-USDT-261225");
        assert!(events.try_recv().is_err());
    }
    #[cfg(all(
        feature = "ticker",
        feature = "funding",
        feature = "markprice",
        feature = "openinterest",
        feature = "index"
    ))]
    #[tokio::test]
    async fn bybit_derivative_ticker_deltas_reconstruct_latest_state() {
        let mut handler = FeedHandler::new();
        let mut events = handler.subscribe();
        handler.add_feed(
            Bybit::new()
                .ticker()
                .funding()
                .mark_price()
                .open_interest()
                .index()
                .symbol("BTC-USDT-PERP")
                .exchange_symbol("BTCUSDT")
                .build(),
        );
        let feed = handler.into_feeds().pop().unwrap();
        let mut state = std::collections::HashMap::new();
        let mut snapshot = serde_json::json!({"topic":"tickers.BTCUSDT","type":"snapshot","ts":1736371332162i64,
            "data":{"symbol":"BTCUSDT","bid1Price":"64999","ask1Price":"65000","markPrice":"65000.1","indexPrice":"64999.1",
                "openInterest":"123","openInterestValue":"7995012.3","fundingRate":"0.0001","nextFundingTime":"1736373600000"}});
        super::merge_bybit_ticker_message(&feed, &mut state, &mut snapshot).unwrap();
        super::process_bybit_message(&feed, &snapshot, 1736371332.2)
            .await
            .unwrap();
        while events.try_recv().is_ok() {}
        let mut delta = serde_json::json!({"topic":"tickers.BTCUSDT","type":"delta","ts":1736371332262i64,
            "data":{"bid1Price":"64998.12345678","openInterestValue":"7996000.12345678"}});
        super::merge_bybit_ticker_message(&feed, &mut state, &mut delta).unwrap();
        super::process_bybit_message(&feed, &delta, 1736371332.3)
            .await
            .unwrap();
        let mut count = 0;
        while let Ok(event) = events.try_recv() {
            count += 1;
            match event {
                crate::feed::FeedEvent::Ticker(value) => {
                    assert_eq!(
                        value.bid,
                        rust_decimal::Decimal::from_str_exact("64998.12345678").unwrap()
                    );
                    assert_eq!(value.ask, rust_decimal::Decimal::from(65000));
                    assert_eq!(value.exchange_ts, 1736371332.262);
                }
                crate::feed::FeedEvent::OpenInterest(value) => {
                    assert_eq!(value.open_interest, rust_decimal::Decimal::from(123));
                    assert_eq!(
                        value.value_usd,
                        Some(rust_decimal::Decimal::from_str_exact("7996000.12345678").unwrap())
                    );
                }
                crate::feed::FeedEvent::Funding(value) => {
                    assert_eq!(value.next_funding_time, Some(1736373600.0))
                }
                _ => {}
            }
        }
        assert_eq!(count, 5);
        let mut replacement = serde_json::json!({"topic":"tickers.BTCUSDT","type":"snapshot","data":{"symbol":"BTCUSDT","markPrice":"65001"}});
        super::merge_bybit_ticker_message(&feed, &mut state, &mut replacement).unwrap();
        assert!(replacement["data"].get("fundingRate").is_none());
        let mut fresh = std::collections::HashMap::new();
        assert!(super::merge_bybit_ticker_message(&feed, &mut fresh, &mut delta).is_err());
        let mut unrelated = serde_json::json!({"topic":"tickers.ETHUSDT","type":"snapshot","data":{"symbol":"ETHUSDT","markPrice":"3000"}});
        super::merge_bybit_ticker_message(&feed, &mut state, &mut unrelated).unwrap();
        assert_eq!(state.len(), 1);
    }
}
