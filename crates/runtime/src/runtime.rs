#[cfg(feature = "orderbook")]
mod book_depth;
#[cfg(all(test, feature = "orderbook"))]
mod book_depth_tests;
mod budget;
#[cfg(all(test, feature = "recording", feature = "orderbook"))]
mod http_replay_tests;
pub(crate) mod quota;
#[cfg(feature = "recording")]
mod raw_replay;
#[cfg(test)]
mod scope_tests;
mod weights;
#[cfg(feature = "recording")]
pub(crate) use raw_replay::RawParserSession;
#[cfg(all(test, feature = "orderbook"))]
mod bitget_failure_tests;
pub mod connection;
mod control;
#[cfg(all(test, feature = "orderbook"))]
mod cross_check_tests;
#[cfg(all(test, feature = "orderbook", feature = "recording"))]
mod gateio_recovery_tests;
#[cfg(all(test, feature = "trade"))]
pub(crate) use control::tests::start as test_managed_runtime;
pub(crate) mod planning;
pub(crate) mod readiness;
mod send_budget;
pub(crate) mod snapshot;
pub mod supervisor;

#[cfg(all(feature = "orderbook", test))]
use crate::exchange::binance::book_sync::BinanceDepthDelta;
#[cfg(feature = "orderbook")]
use crate::exchange::bithumb::book_sync::BithumbBookSync;
#[cfg(feature = "orderbook")]
use crate::exchange::gateio::adapter::GateioInstrument;
use crate::exchange::{
    ExchangeFeed,
    binance::adapter::{BinanceAdapter, BinanceConnectionPlan, BinanceEvent, BinanceInstrument},
    bitget::adapter::{BitgetAdapter, BitgetEvent},
    bithumb::adapter::{BithumbAdapter, BithumbEvent},
    bybit::adapter::{BybitAdapter, BybitEvent, BybitProduct},
    coinbase::adapter::{CoinbaseAdapter, CoinbaseEvent},
    deribit::adapter::{DeribitAdapter, DeribitControl, DeribitEvent},
    gateio::adapter::{GateioAdapter, GateioConnectionPlan, GateioEvent},
    htx::adapter::{HtxAdapter, HtxControl, HtxEvent},
    hyperliquid::adapter::{HyperliquidAdapter, HyperliquidControl, HyperliquidEvent},
    kraken::adapter::{KrakenAdapter, KrakenEvent},
    kucoin::adapter::{KucoinAdapter, KucoinEvent},
    okx::adapter::{OkxAdapter, OkxEvent},
    upbit::adapter::{UpbitAdapter, UpbitEvent},
};
#[cfg(feature = "orderbook")]
use crate::exchange::{
    binance::{
        book_sync::{BinanceBookSync, BinanceSequencedDepthDelta},
        parser as binance_parser,
    },
    bitget::book_sync::{BitgetBookAction, BitgetBookSync, BitgetDepthUpdate},
    bybit::book_sync::BybitBookSync,
    coinbase::book_sync::{CoinbaseBookOutcome, CoinbaseBookSync},
    deribit::book_sync::{DeribitBookOutcome, DeribitBookSync},
    gateio::{
        book_sync::{GateioBookSync, GateioBookUpdate, GateioDepthDelta},
        parser as gateio_parser,
    },
    htx::book_sync::{HtxBookMode, HtxBookOutcome, HtxBookSync},
    hyperliquid::book_sync::{HyperliquidBookOutcome, HyperliquidBookSync},
    kraken::{
        book_sync::{KrakenBookOutcome, KrakenBookSync},
        parser as kraken_parser,
    },
    kucoin::{
        book_sync::{KucoinBookMode, KucoinBookOutcome, KucoinBookSync},
        parser as kucoin_parser,
    },
    okx::{book_sync::OkxBookSync, parser as okx_parser},
    upbit::book_sync::UpbitBookSync,
};
// The wire-frame helpers used by the always-built text-message entry points;
// the book-sync types stay orderbook-gated.
use crate::exchange::{
    bithumb::parser as bithumb_parser, coinbase::parser as coinbase_parser,
    deribit::parser as deribit_parser, htx::parser as htx_parser,
    hyperliquid::parser as hyperliquid_parser, upbit::parser as upbit_parser,
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
use futures::{FutureExt, Sink, Stream};
use serde_json::Value;
use std::fmt::Display;
use std::future::Future;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::Message;
use url::Url;

#[cfg(feature = "orderbook")]
type SnapshotReceivers = std::collections::HashMap<
    String,
    snapshot::SnapshotReceiver<(u64, cryptofeed_orderbook::L2BookSnapshot)>,
>;
#[cfg(feature = "orderbook")]
type GateioSnapshotReceivers = std::collections::HashMap<
    String,
    snapshot::SnapshotReceiver<crate::exchange::gateio::book_sync::GateioBookSnapshot>,
>;

#[cfg(test)]
const SHUTDOWN_GRACE_PERIOD: std::time::Duration = std::time::Duration::from_millis(100);
#[cfg(not(test))]
const SHUTDOWN_GRACE_PERIOD: std::time::Duration = std::time::Duration::from_secs(5);

/// Additional Gateio snapshot operations per symbol/physical session, across
/// bootstrap failures AND later gaps. Success does not refund this budget;
/// exhaustion returns to the existing reconnect/backoff supervisor.
#[cfg(feature = "orderbook")]
const GATEIO_MAX_RESNAPSHOTS: u32 = 3;
#[cfg(feature = "orderbook")]
const GATEIO_BOOTSTRAP_DEADLINE: std::time::Duration = std::time::Duration::from_secs(60);

/// Cumulative requests in one physical session. Success must not reset this
/// budget: otherwise bootstrap -> immediate gap can refetch forever.
#[cfg(feature = "orderbook")]
pub(crate) struct GateioResnapshotBudget {
    resnapshots: u32,
    unsynced_since: tokio::time::Instant,
}

#[cfg(feature = "orderbook")]
fn gateio_snapshot_budget(
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

#[cfg(feature = "orderbook")]
const BINANCE_MAX_RESNAPSHOTS: u32 = 3;
#[cfg(feature = "orderbook")]
const MAX_BUFFERED_DELTAS_PER_SYMBOL: usize = 512;
/// Runs the feeds until Ctrl-C triggers a clean shutdown.
pub async fn run(handler: FeedHandler) -> Result<()> {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let runtime = run_with_shutdown(handler, shutdown_rx);
    tokio::pin!(runtime);
    tokio::select! {
        result = &mut runtime => result,
        _ = tokio::signal::ctrl_c() => {
            let _ = shutdown_tx.send(true);
            runtime.await
        }
    }
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
    if handler.has_control() {
        return control::run(handler, shutdown).await;
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
    run_feeds_until_shutdown(feeds, shutdown, consume_feed).await
}

async fn consume_feed(feed: ExchangeFeed, shutdown: watch::Receiver<bool>) -> Result<()> {
    match feed.exchange {
        ExchangeId::Binance => consume_binance_feed(feed, shutdown).await,
        ExchangeId::Bitget => consume_bitget_feed(feed, shutdown).await,
        ExchangeId::Bybit => consume_bybit_feed(feed, shutdown).await,
        ExchangeId::Okx => consume_okx_feed(feed, shutdown).await,
        ExchangeId::Gateio => consume_gateio_feed(feed, shutdown).await,
        ExchangeId::Kucoin => consume_kucoin_feed(feed, shutdown).await,
        ExchangeId::Kraken => consume_kraken_feed(feed, shutdown).await,
        ExchangeId::Upbit => consume_upbit_feed(feed, shutdown).await,
        ExchangeId::Bithumb => consume_bithumb_feed(feed, shutdown).await,
        ExchangeId::Coinbase => consume_coinbase_feed(feed, shutdown).await,
        ExchangeId::Deribit => consume_deribit_feed(feed, shutdown).await,
        ExchangeId::Htx => consume_htx_feed(feed, shutdown).await,
        ExchangeId::Hyperliquid => consume_hyperliquid_feed(feed, shutdown).await,
        // Any future exchange is rejected by capability validation before a
        // feed reaches this point.
        _ => Ok(()),
    }
}

async fn hydrate_feed_symbols(feeds: Vec<ExchangeFeed>) -> Result<Vec<ExchangeFeed>> {
    // Reject invalid logical configurations before starting any catalog fetch.
    for feed in &feeds {
        markets::validate_feed(feed)?;
    }
    // Resolve the union once, preserving global native-mapping validation,
    // then partition exact channel/symbol sets for existing session paths.
    let hydrated: Vec<Result<Vec<ExchangeFeed>>> =
        futures::future::join_all(feeds.into_iter().map(|mut feed| async move {
            feed.exchange_symbols = markets::resolve_feed_symbols(&feed).await?;
            planning::prepare(feed).await
        }))
        .await;
    let mut result = Vec::new();
    for feed in hydrated {
        result.extend(feed?);
    }
    planning::validate_connection_counts(&result)?;
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
        ExchangeId::Coinbase => CoinbaseAdapter::subscription_url(feed),
        ExchangeId::Deribit => DeribitAdapter::subscription_url(feed),
        ExchangeId::Htx => HtxAdapter::subscription_url(feed),
        ExchangeId::Hyperliquid => HyperliquidAdapter::subscription_url(feed),
        ExchangeId::Gateio => GateioAdapter::connection_plans(feed)
            .ok()
            .and_then(|plans| plans.into_iter().next())
            .map_or_else(String::new, |plan| plan.websocket_url),
        ExchangeId::Kraken => KrakenAdapter::subscription_url(feed),
        ExchangeId::Kucoin => KucoinAdapter::subscription_url(feed),
        ExchangeId::Upbit => UpbitAdapter::subscription_url(feed),
        ExchangeId::Bithumb => BithumbAdapter::subscription_url(feed),
        ExchangeId::Okx => OkxAdapter::subscription_url(feed),
        _ => String::new(),
    }
}

async fn run_feeds_until_shutdown<F, Fut>(
    feeds: Vec<ExchangeFeed>,
    shutdown: watch::Receiver<bool>,
    consume: F,
) -> Result<()>
where
    F: Fn(ExchangeFeed, watch::Receiver<bool>) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    run_feeds_until_shutdown_tracked(feeds, shutdown, consume, None).await
}

type FeedTaskRoutes = std::collections::HashMap<
    tokio::task::Id,
    (Option<std::sync::Arc<readiness::FeedMonitor>>, ExchangeId),
>;
type FeedTaskResult = std::result::Result<
    (
        Option<std::sync::Arc<readiness::FeedMonitor>>,
        ExchangeId,
        Result<()>,
    ),
    tokio::task::JoinError,
>;

async fn run_feeds_until_shutdown_tracked<F, Fut>(
    feeds: Vec<ExchangeFeed>,
    mut shutdown: watch::Receiver<bool>,
    consume: F,
    forced: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
) -> Result<()>
where
    F: Fn(ExchangeFeed, watch::Receiver<bool>) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    if *shutdown.borrow() {
        return Ok(());
    }
    let status_sender = feeds.iter().find_map(|feed| feed.status_sender.clone());
    let mut tasks = JoinSet::new();
    let mut routes = FeedTaskRoutes::new();
    let mut terminal_errors = Vec::new();
    for feed in feeds {
        let consume = consume.clone();
        let exchange = feed.exchange;
        let identity = if feed.managed {
            feed.monitor.clone()
        } else {
            None
        };
        let feed_shutdown = shutdown.clone();
        let routed = identity.clone();
        let task =
            tasks.spawn(async move { (identity, exchange, consume(feed, feed_shutdown).await) });
        routes.insert(task.id(), (routed, exchange));
    }
    while !tasks.is_empty() {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_ok() && !*shutdown.borrow() { continue; }
                let drain = async {
                    while let Some(result) = tasks.join_next().await {
                        record_feed_result(result, &mut terminal_errors, status_sender.as_ref(), &routes);
                    }
                };
                if tokio::time::timeout(SHUTDOWN_GRACE_PERIOD, drain).await.is_err() {
                    if let Some(forced) = &forced { forced.store(true, std::sync::atomic::Ordering::Relaxed); }
                    tasks.abort_all();
                    // A stop acknowledgement must follow child cancellation,
                    // not just dropping a JoinSet whose tasks may still run.
                    while let Some(result) = tasks.join_next().await {
                        if result.as_ref().is_err_and(|error| error.is_cancelled()) { continue; }
                        record_feed_result(result, &mut terminal_errors, status_sender.as_ref(), &routes);
                    }
                }
                return terminal_result(terminal_errors);
            }
            result = tasks.join_next() => {
                if let Some(result) = result { record_feed_result(result, &mut terminal_errors, status_sender.as_ref(), &routes); }
            }
        }
    }
    terminal_result(terminal_errors)
}

fn record_feed_result(
    result: FeedTaskResult,
    terminal_errors: &mut Vec<String>,
    status_sender: Option<&tokio::sync::broadcast::Sender<crate::feed::FeedStatus>>,
    routes: &FeedTaskRoutes,
) {
    match result {
        Ok((_, _, Ok(()))) => {}
        Ok((identity, exchange, Err(error))) => {
            let diagnostic = crate::feed::FailureDiagnostic::from(&error);
            tracing::error!(
                ?exchange,
                feed_id = identity.as_ref().map(|m| m.identity.id.as_u64()),
                generation = identity.as_ref().map(|m| m.identity.generation),
                category = diagnostic.category,
                reason = diagnostic.reason,
                permanent = diagnostic.permanent,
                outcome = if diagnostic.permanent {
                    "permanent_failure"
                } else {
                    "feed_failed"
                },
                "feed terminated"
            );
            if let Some(sender) = status_sender {
                let _ = sender.send(crate::feed::FeedStatus::Terminated {
                    exchange,
                    error: error.to_string(),
                });
            }
            if let Some(monitor) = identity {
                monitor.lifecycle(crate::feed::FeedState::Degraded {
                    error: error.to_string(),
                });
            }
            terminal_errors.push(format!("{exchange:?}: {error}"));
        }
        Err(error) => {
            let route = routes.get(&error.id());
            tracing::error!(
                exchange = ?route.map(|(_, exchange)| exchange),
                feed_id = route.and_then(|(monitor, _)| monitor.as_ref()).map(|m| m.identity.id.as_u64()),
                generation = route.and_then(|(monitor, _)| monitor.as_ref()).map(|m| m.identity.generation),
                outcome = if error.is_panic() {
                    "task_panicked"
                } else {
                    "task_cancelled"
                },
                "feed task terminated unexpectedly"
            );
            if let Some(sender) = status_sender {
                let _ = sender.send(crate::feed::FeedStatus::TaskPanicked {
                    error: error.to_string(),
                });
            }
            if let Some((Some(monitor), _)) = routes.get(&error.id()) {
                monitor.lifecycle(crate::feed::FeedState::Degraded {
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

async fn consume_bitget_feed(
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

async fn consume_bybit_feed(feed: ExchangeFeed, shutdown: watch::Receiver<bool>) -> Result<()> {
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

async fn consume_okx_feed(feed: ExchangeFeed, shutdown: watch::Receiver<bool>) -> Result<()> {
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

async fn consume_kucoin_feed(
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

async fn consume_kraken_feed(
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
            async move { consume_kraken_session(feed, shutdown).await }
        },
    )
    .await
    .map(|_| ())
}

async fn consume_upbit_feed(mut feed: ExchangeFeed, shutdown: watch::Receiver<bool>) -> Result<()> {
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
            async move { consume_upbit_session(feed, shutdown).await }
        },
    )
    .await
    .map(|_| ())
}

async fn consume_bithumb_feed(
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
            async move { consume_bithumb_session(feed, shutdown).await }
        },
    )
    .await
    .map(|_| ())
}

async fn consume_coinbase_feed(
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
            async move { consume_coinbase_session(feed, shutdown).await }
        },
    )
    .await
    .map(|_| ())
}

async fn consume_deribit_feed(
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

async fn consume_htx_feed(feed: ExchangeFeed, shutdown: watch::Receiver<bool>) -> Result<()> {
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

async fn consume_hyperliquid_feed(
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

async fn consume_hyperliquid_session_with<S, E>(
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

fn okx_feed_for_url(feed: &ExchangeFeed, url: &str) -> ExchangeFeed {
    let mut planned = feed.clone();
    let business = url.ends_with("/business");
    planned
        .channels
        .retain(|channel| matches!(channel, Channel::Candles) == business);
    planned.prune_connection_scope();
    planned
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
    planned.prune_connection_scope();
    planned
}

async fn consume_gateio_feed(feed: ExchangeFeed, shutdown: watch::Receiver<bool>) -> Result<()> {
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

fn reset_connection_books<'a>(
    feed: &ExchangeFeed,
    book_route: bool,
    candidates: impl IntoIterator<Item = &'a cryptofeed_core::symbol::Symbol>,
) {
    if !book_route {
        return;
    }
    let owned: Vec<_> = candidates
        .into_iter()
        .filter(|symbol| feed.subscribes(Channel::L2Book, symbol))
        .cloned()
        .collect();
    feed.clear_connection_books(&owned);
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

async fn consume_bitget_session_with<S, E>(
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

async fn consume_bybit_session_with<S, E>(
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

async fn consume_okx_session_with<S, E>(
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

async fn consume_kucoin_session_with<S, E>(
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

async fn consume_kraken_session(
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
    let result = consume_kraken_session_with(feed, shutdown, &mut session).await;
    session.record_result(&result);
    result
}

async fn consume_kraken_session_with<S, E>(
    feed: ExchangeFeed,
    mut shutdown: watch::Receiver<bool>,
    session: &mut Session<S>,
) -> Result<()>
where
    S: Sink<Message, Error = E> + Stream<Item = std::result::Result<Message, E>> + Unpin,
    E: Display,
{
    session.queue_subscriptions(KrakenAdapter::subscription_messages(&feed)?);

    // In-session resnapshot requests for book checksum mismatches.
    let mut outbound: Vec<String> = Vec::new();
    while let Some(text) = session.next_text_or_shutdown(&mut shutdown).await? {
        let received_ts = session.received_timestamp();
        session.processing();
        outbound.clear();
        process_kraken_text_message(&feed, &text, received_ts, &mut outbound).await?;
        if !outbound.is_empty() {
            session.queue_subscriptions(std::mem::take(&mut outbound));
        }
    }

    Ok(())
}

async fn consume_upbit_session(
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
    let result = consume_upbit_session_with(feed, shutdown, &mut session).await;
    session.record_result(&result);
    result
}

async fn consume_upbit_session_with<S, E>(
    feed: ExchangeFeed,
    mut shutdown: watch::Receiver<bool>,
    session: &mut Session<S>,
) -> Result<()>
where
    S: Sink<Message, Error = E> + Stream<Item = std::result::Result<Message, E>> + Unpin,
    E: Display,
{
    session.queue_subscriptions(UpbitAdapter::subscription_messages(&feed)?);
    // Snapshot-only books need no in-session resnapshot request: every push
    // replaces the book and no sequence/gap contract exists.
    while let Some(text) = session.next_text_or_shutdown(&mut shutdown).await? {
        let received_ts = session.received_timestamp();
        session.processing();
        process_upbit_text_message(&feed, &text, received_ts).await?;
    }

    Ok(())
}

async fn consume_bithumb_session(
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
    let result = consume_bithumb_session_with(feed, shutdown, &mut session).await;
    session.record_result(&result);
    result
}

async fn consume_bithumb_session_with<S, E>(
    feed: ExchangeFeed,
    mut shutdown: watch::Receiver<bool>,
    session: &mut Session<S>,
) -> Result<()>
where
    S: Sink<Message, Error = E> + Stream<Item = std::result::Result<Message, E>> + Unpin,
    E: Display,
{
    session.queue_subscriptions(BithumbAdapter::subscription_messages(&feed)?);
    while let Some(text) = session.next_text_or_shutdown(&mut shutdown).await? {
        let received_ts = session.received_timestamp();
        session.processing();
        process_bithumb_text_message(&feed, &text, received_ts).await?;
    }

    Ok(())
}

async fn consume_coinbase_session(
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
    let result = consume_coinbase_session_with(feed, shutdown, &mut session).await;
    session.record_result(&result);
    result
}

async fn consume_coinbase_session_with<S, E>(
    feed: ExchangeFeed,
    mut shutdown: watch::Receiver<bool>,
    session: &mut Session<S>,
) -> Result<()>
where
    S: Sink<Message, Error = E> + Stream<Item = std::result::Result<Message, E>> + Unpin,
    E: Display,
{
    session.queue_subscriptions(CoinbaseAdapter::subscription_messages(&feed)?);
    // The documented level2 stream has no in-session snapshot request; a
    // reconnect is the documented recovery and clears this feed's book state.
    while let Some(text) = session.next_text_or_shutdown(&mut shutdown).await? {
        let received_ts = session.received_timestamp();
        session.processing();
        process_coinbase_text_message(&feed, &text, received_ts).await?;
    }
    Ok(())
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

async fn consume_deribit_session_with<S, E>(
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

async fn consume_htx_session_with<S, E>(
    feed: ExchangeFeed,
    mut shutdown: watch::Receiver<bool>,
    session: &mut Session<S>,
) -> Result<()>
where
    S: Sink<Message, Error = E> + Stream<Item = std::result::Result<Message, E>> + Unpin,
    E: Display,
{
    session.queue_subscriptions(HtxAdapter::subscription_messages(&feed)?);
    // Server heartbeat pongs and USDT-M incremental-depth resubscription pairs
    // share the paced outbound queue.
    let mut outbound: Vec<String> = Vec::new();
    while let Some(text) = session.next_text_or_shutdown(&mut shutdown).await? {
        let received_ts = session.received_timestamp();
        session.processing();
        outbound.clear();
        process_htx_text_message(&feed, &text, received_ts, &mut outbound).await?;
        if !outbound.is_empty() {
            session.queue_subscriptions(std::mem::take(&mut outbound));
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
async fn dispatch_binance_event(feed: &ExchangeFeed, event: BinanceEvent) -> Result<()> {
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
async fn dispatch_bitget_event(feed: &ExchangeFeed, event: BitgetEvent) -> Result<()> {
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
async fn dispatch_bybit_event(feed: &ExchangeFeed, event: BybitEvent) -> Result<()> {
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
async fn dispatch_okx_event(feed: &ExchangeFeed, event: OkxEvent) -> Result<()> {
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
    Ok(())
}

#[cfg(feature = "orderbook")]
async fn dispatch_kucoin_l2_book(feed: &ExchangeFeed, book: L2Book) -> Result<()> {
    dispatch_l2_book(feed, book).await
}

#[cfg_attr(not(test), allow(dead_code))]
async fn dispatch_kraken_event(feed: &ExchangeFeed, event: KrakenEvent) -> Result<()> {
    let _ = feed;
    match event {
        #[cfg(feature = "candles")]
        KrakenEvent::Candle(candle) => dispatch_candle(feed, candle).await,
        #[cfg(feature = "ticker")]
        KrakenEvent::Ticker(ticker) => dispatch_ticker(feed, ticker).await,
        #[cfg(feature = "trade")]
        KrakenEvent::Trade(trade) => dispatch_trade(feed, trade).await,
    }
    Ok(())
}

#[cfg(feature = "orderbook")]
async fn dispatch_kraken_l2_book(feed: &ExchangeFeed, book: L2Book) -> Result<()> {
    dispatch_l2_book(feed, book).await
}

#[cfg_attr(not(test), allow(dead_code))]
async fn dispatch_upbit_event(feed: &ExchangeFeed, event: UpbitEvent) -> Result<()> {
    let _ = feed;
    match event {
        #[cfg(feature = "candles")]
        UpbitEvent::Candle(candle) => dispatch_candle(feed, candle).await,
        #[cfg(feature = "trade")]
        UpbitEvent::Trade(trade) => dispatch_trade(feed, trade).await,
    }
    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
async fn dispatch_bithumb_event(feed: &ExchangeFeed, event: BithumbEvent) -> Result<()> {
    let _ = feed;
    match event {
        #[cfg(feature = "trade")]
        BithumbEvent::Trade(trade) => dispatch_trade(feed, trade).await,
    }
    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
async fn dispatch_coinbase_event(feed: &ExchangeFeed, event: CoinbaseEvent) -> Result<()> {
    let _ = feed;
    match event {
        #[cfg(feature = "candles")]
        CoinbaseEvent::Candle(candle) => dispatch_candle(feed, candle).await,
        #[cfg(feature = "orderbook")]
        CoinbaseEvent::L1Book(book) => dispatch_l1_book(feed, book).await,
        #[cfg(feature = "ticker")]
        CoinbaseEvent::Ticker(ticker) => dispatch_ticker(feed, ticker).await,
        #[cfg(feature = "trade")]
        CoinbaseEvent::Trade(trade) => dispatch_trade(feed, trade).await,
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
    Ok(())
}

#[cfg(feature = "orderbook")]
async fn dispatch_deribit_l2_book(feed: &ExchangeFeed, book: L2Book) -> Result<()> {
    dispatch_l2_book(feed, book).await
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

#[cfg_attr(not(test), allow(dead_code))]
async fn dispatch_gateio_event(feed: &ExchangeFeed, event: GateioEvent) -> Result<()> {
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
async fn dispatch_okx_l2_book(feed: &ExchangeFeed, book: L2Book) -> Result<()> {
    dispatch_l2_book(feed, book).await
}

#[cfg_attr(not(test), allow(dead_code))]
async fn process_binance_text_message(
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

async fn process_binance_text_message_for_plan(
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
fn apply_orderbook_state(
    feed: &ExchangeFeed,
    book: &cryptofeed_orderbook::L2Book,
) -> Result<L2Book> {
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
    let result = if feed.l2_book_cross_check && state.is_crossed() {
        Err(Error::MalformedData(format!(
            "crossed L2 book: {}",
            book.symbol().as_str()
        )))
    } else if let Some(maximum) = feed.l2_book_max_depth {
        let exchange = match book {
            L2Book::Snapshot(value) => value.exchange,
            L2Book::Delta(value) => value.exchange,
        };
        let current = state.to_snapshot_with_depth(
            exchange,
            book.exchange_ts(),
            book.received_ts(),
            maximum.get(),
        );
        let mut projections = feed.book_projections.lock().expect("book projection lock");
        let result = book_depth::project(book, &current, projections.get(&symbol));
        if result.is_ok() {
            projections.insert(symbol, current);
        }
        result
    } else {
        Ok(book.clone())
    };
    drop(states);
    if result.is_err() {
        feed.clear_connection_books(std::slice::from_ref(book.symbol()));
    }
    result
}

#[cfg(feature = "orderbook")]
async fn dispatch_binance_l2_book(feed: &ExchangeFeed, book: L2Book) -> Result<()> {
    dispatch_l2_book(feed, book).await
}

#[cfg(feature = "candles")]
async fn dispatch_candle(feed: &ExchangeFeed, candle: cryptofeed_candles::Candle) {
    if !subscribed_to(feed, Channel::Candles, &candle.symbol) {
        return;
    }
    use crate::exchange::CandlePolicy;
    let deliver = match feed.candle_policy {
        CandlePolicy::All => true,
        CandlePolicy::ClosedOnly => candle.closed == Some(true),
        CandlePolicy::ClosedOrUnknown => candle.closed != Some(false),
    };
    if !deliver {
        return;
    }
    feed.publish_event(crate::feed::FeedEvent::Candle(candle.clone()));
    for handler in feed
        .candle_handler
        .iter()
        .chain(&feed.additional_candle_handlers)
    {
        await_handler(feed.runtime_options.callback_deadline(), "candle", async {
            handler.on_candle(candle.clone()).await
        })
        .await;
    }
}

#[cfg(feature = "funding")]
async fn dispatch_funding(feed: &ExchangeFeed, funding: cryptofeed_funding::Funding) {
    if !subscribed_to(feed, Channel::Funding, &funding.symbol) {
        return;
    }
    feed.publish_event(crate::feed::FeedEvent::Funding(funding.clone()));
    for handler in feed
        .funding_handler
        .iter()
        .chain(&feed.additional_funding_handlers)
    {
        await_handler(feed.runtime_options.callback_deadline(), "funding", async {
            handler.on_funding(funding.clone()).await
        })
        .await;
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
    for handler in feed
        .liquidation_handler
        .iter()
        .chain(&feed.additional_liquidation_handlers)
    {
        await_handler(
            feed.runtime_options.callback_deadline(),
            "liquidation",
            async { handler.on_liquidation(liquidation.clone()).await },
        )
        .await;
    }
}

#[cfg(feature = "markprice")]
async fn dispatch_mark_price(feed: &ExchangeFeed, mark_price: cryptofeed_markprice::MarkPrice) {
    if !subscribed_to(feed, Channel::MarkPrice, &mark_price.symbol) {
        return;
    }
    feed.publish_event(crate::feed::FeedEvent::MarkPrice(mark_price.clone()));
    for handler in feed
        .mark_price_handler
        .iter()
        .chain(&feed.additional_mark_price_handlers)
    {
        await_handler(
            feed.runtime_options.callback_deadline(),
            "mark_price",
            async { handler.on_mark_price(mark_price.clone()).await },
        )
        .await;
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
    for handler in feed
        .open_interest_handler
        .iter()
        .chain(&feed.additional_open_interest_handlers)
    {
        await_handler(
            feed.runtime_options.callback_deadline(),
            "open_interest",
            async { handler.on_open_interest(open_interest.clone()).await },
        )
        .await;
    }
}

#[cfg(feature = "index")]
async fn dispatch_index_price(feed: &ExchangeFeed, index_price: cryptofeed_index::IndexPrice) {
    if !subscribed_to(feed, Channel::Index, &index_price.symbol) {
        return;
    }
    feed.publish_event(crate::feed::FeedEvent::IndexPrice(index_price.clone()));
    for handler in feed
        .index_price_handler
        .iter()
        .chain(&feed.additional_index_price_handlers)
    {
        await_handler(
            feed.runtime_options.callback_deadline(),
            "index_price",
            async { handler.on_index_price(index_price.clone()).await },
        )
        .await;
    }
}

#[cfg(feature = "ticker")]
async fn dispatch_ticker(feed: &ExchangeFeed, ticker: cryptofeed_ticker::Ticker) {
    if !subscribed_to(feed, Channel::Ticker, &ticker.symbol) {
        return;
    }
    feed.publish_event(crate::feed::FeedEvent::Ticker(ticker.clone()));
    for handler in feed
        .ticker_handler
        .iter()
        .chain(&feed.additional_ticker_handlers)
    {
        await_handler(feed.runtime_options.callback_deadline(), "ticker", async {
            handler.on_ticker(ticker.clone()).await
        })
        .await;
    }
}

#[cfg(feature = "trade")]
async fn dispatch_trade(feed: &ExchangeFeed, trade: cryptofeed_trade::Trade) {
    if !subscribed_to(feed, Channel::Trade, &trade.symbol) {
        return;
    }
    feed.publish_event(crate::feed::FeedEvent::Trade(trade.clone()));
    for handler in feed
        .trade_handler
        .iter()
        .chain(&feed.additional_trade_handlers)
    {
        await_handler(feed.runtime_options.callback_deadline(), "trade", async {
            handler.on_trade(trade.clone()).await
        })
        .await;
    }
}

#[cfg(feature = "orderbook")]
async fn dispatch_l2_book(feed: &ExchangeFeed, book: L2Book) -> Result<()> {
    if !subscribed_to(feed, Channel::L2Book, book.symbol()) {
        return Ok(());
    }
    let book = apply_orderbook_state(feed, &book)?;
    if let (Some(monitor), Some(tracker), Some(epoch)) = (
        &feed.monitor,
        &feed.connection_tracker,
        feed.connection_epoch,
    ) {
        if let Some(books) = &monitor.books {
            books.apply(monitor.identity, tracker.id, epoch, &book);
        }
    }
    if matches!(&book, L2Book::Snapshot(_)) {
        feed.book_synchronized(book.symbol().as_str());
    }
    feed.publish_event(crate::feed::FeedEvent::L2Book(book.clone()));
    for handler in feed
        .orderbook_handler
        .iter()
        .chain(&feed.additional_orderbook_handlers)
    {
        await_handler(feed.runtime_options.callback_deadline(), "l2_book", async {
            handler.on_l2_book(book.clone()).await
        })
        .await;
    }
    Ok(())
}

#[cfg(feature = "orderbook")]
async fn dispatch_l1_book(feed: &ExchangeFeed, book: cryptofeed_orderbook::L1Book) {
    if !subscribed_to(feed, Channel::L1Book, &book.symbol) {
        return;
    }
    feed.publish_event(crate::feed::FeedEvent::L1Book(book.clone()));
    for handler in feed
        .orderbook_handler
        .iter()
        .chain(&feed.additional_orderbook_handlers)
    {
        await_handler(feed.runtime_options.callback_deadline(), "l1_book", async {
            handler.on_l1_book(book.clone()).await
        })
        .await;
    }
}

fn subscribed_to(
    feed: &ExchangeFeed,
    channel: Channel,
    symbol: &cryptofeed_core::symbol::Symbol,
) -> bool {
    feed.subscribes(channel, symbol)
}

async fn await_handler<F>(timeout: std::time::Duration, channel: &'static str, future: F)
where
    F: Future<Output = ()>,
{
    match tokio::time::timeout(timeout, std::panic::AssertUnwindSafe(future).catch_unwind()).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => tracing::error!(channel, "handler panicked; continuing remaining callbacks"),
        Err(_) => tracing::warn!(channel, "handler timed out; event callback cancelled"),
    }
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
fn spawn_binance_snapshot_fetch(
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
async fn poll_binance_snapshot_bootstraps(
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
    feed.invalidate_book_readiness(symbol_key);
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
async fn poll_gateio_snapshot_bootstraps_for_plan(
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
                feed.gateio_book_syncs
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
async fn process_gateio_orderbook_message_for_plan(
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
        .gateio_book_syncs
        .lock()
        .expect("gateio sync lock")
        .contains_key(&symbol_key)
    {
        let (maybe_book, anchor) = {
            let mut syncs = feed.gateio_book_syncs.lock().expect("gateio sync lock");
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
async fn process_bitget_text_message(
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
        dispatch_bybit_event(feed, event).await?;
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
        dispatch_bybit_l2_book(feed, book).await?;
    }

    Ok(true)
}

#[cfg_attr(not(test), allow(dead_code))]
async fn process_okx_text_message(feed: &ExchangeFeed, text: &str, received_ts: f64) -> Result<()> {
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

async fn process_kucoin_text_message(
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
        let mut syncs = feed.kucoin_book_syncs.lock().expect("kucoin sync lock");
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

async fn process_kraken_text_message(
    feed: &ExchangeFeed,
    text: &str,
    received_ts: f64,
    outbound: &mut Vec<String>,
) -> Result<()> {
    let message: Value = serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
    if let Some(control) = KrakenAdapter::parse_control_message(&message) {
        return control.map(|_| ());
    }
    #[cfg(feature = "orderbook")]
    let handled = process_kraken_orderbook_message(feed, &message, received_ts, outbound).await?;
    #[cfg(not(feature = "orderbook"))]
    let handled = {
        let _ = &outbound;
        false
    };
    if handled {
        return Ok(());
    }
    let events = KrakenAdapter::parse_messages_for_feed(feed, &message, received_ts);
    let channel = crate::exchange::kraken::parser::channel(&message);
    let configured_row = crate::exchange::kraken::parser::data_rows(&message)
        .iter()
        .any(|row| {
            crate::exchange::kraken::parser::row_symbol(row)
                .is_some_and(|native| KrakenAdapter::symbol_for_native(feed, native).is_some())
        });
    if channel.is_some() && configured_row && events.is_empty() {
        return Err(Error::MalformedData(format!(
            "Kraken message for {} produced no normalized events",
            channel.unwrap_or("unknown channel")
        )));
    }
    for event in events {
        dispatch_kraken_event(feed, event).await?;
    }
    Ok(())
}

#[cfg(feature = "orderbook")]
async fn process_kraken_orderbook_message(
    feed: &ExchangeFeed,
    message: &Value,
    received_ts: f64,
    outbound: &mut Vec<String>,
) -> Result<bool> {
    if kraken_parser::channel(message) != Some("book") {
        return Ok(false);
    }
    let snapshot = match kraken_parser::message_type(message) {
        Some("snapshot") => true,
        Some("update") => false,
        _ => {
            return Err(Error::Parse(
                "Kraken book message is missing its snapshot/update type".to_owned(),
            ));
        }
    };
    let depth = KrakenAdapter::l2_book_depth_wire(feed.l2_book_depth).ok_or_else(|| {
        Error::UnsupportedCapability(format!("Kraken l2 depth level {:?}", feed.l2_book_depth))
    })?;
    for row in kraken_parser::data_rows(message) {
        let native = kraken_parser::row_symbol(row)
            .ok_or_else(|| Error::Parse("Kraken book row is missing its symbol".to_owned()))?;
        let Some(symbol) = KrakenAdapter::symbol_for_native(feed, native).cloned() else {
            continue;
        };
        if !feed.subscribes(Channel::L2Book, &symbol) {
            continue;
        }
        let update = kraken_parser::parse_book_row(row, snapshot, received_ts, &symbol)
            .ok_or_else(|| Error::Parse("failed to parse Kraken book row".to_owned()))?;
        let symbol_key = symbol.as_str().to_owned();
        let outcome = {
            let mut syncs = feed.kraken_book_syncs.lock().expect("kraken sync lock");
            let sync = syncs
                .entry(symbol_key)
                .or_insert_with(|| KrakenBookSync::new(symbol.clone(), depth));
            sync.apply(update)?
        };
        match outcome {
            KrakenBookOutcome::Publish(book) => dispatch_kraken_l2_book(feed, book).await?,
            KrakenBookOutcome::Ignored => {}
            KrakenBookOutcome::Resnapshot => {
                outbound.extend(KrakenAdapter::book_resubscribe_messages(feed, &symbol)?);
            }
        }
    }
    Ok(true)
}

async fn process_upbit_text_message(
    feed: &ExchangeFeed,
    text: &str,
    received_ts: f64,
) -> Result<()> {
    let message: Value = serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
    if UpbitAdapter::is_heartbeat_status(&message) {
        return Ok(());
    }
    if let Some(error) = UpbitAdapter::error(&message) {
        return Err(error);
    }
    #[cfg(feature = "orderbook")]
    let handled = process_upbit_orderbook_message(feed, &message, received_ts).await?;
    #[cfg(not(feature = "orderbook"))]
    let handled = false;
    if handled {
        return Ok(());
    }
    let events = UpbitAdapter::parse_messages_for_feed(feed, &message, received_ts);
    let data_type = upbit_parser::message_type(&message);
    let configured = upbit_parser::native_code(&message)
        .is_some_and(|native| UpbitAdapter::symbol_for_native(feed, native).is_some());
    if configured && data_type.is_some_and(is_upbit_market_type) && events.is_empty() {
        return Err(Error::MalformedData(format!(
            "Upbit message for {} produced no normalized events",
            data_type.unwrap_or("unknown type")
        )));
    }
    for event in events {
        dispatch_upbit_event(feed, event).await?;
    }
    Ok(())
}

fn is_upbit_market_type(data_type: &str) -> bool {
    matches!(data_type, "trade" | "orderbook") || data_type.starts_with("candle.")
}

#[cfg(feature = "orderbook")]
async fn process_upbit_orderbook_message(
    feed: &ExchangeFeed,
    message: &Value,
    received_ts: f64,
) -> Result<bool> {
    if upbit_parser::message_type(message) != Some("orderbook") {
        return Ok(false);
    }
    let native = upbit_parser::native_code(message)
        .ok_or_else(|| Error::Parse("Upbit orderbook message is missing its code".to_owned()))?;
    let Some(symbol) = UpbitAdapter::symbol_for_native(feed, native).cloned() else {
        // A connection may carry codes this feed does not subscribe.
        return Ok(true);
    };
    if !feed.subscribes(Channel::L2Book, &symbol) {
        return Ok(true);
    }
    let book = upbit_parser::parse_book_for_symbol(message, received_ts, &symbol)
        .ok_or_else(|| Error::Parse("failed to parse Upbit orderbook snapshot".to_owned()))?;
    let units = UpbitAdapter::l2_book_depth_wire(feed.l2_book_depth).ok_or_else(|| {
        Error::UnsupportedCapability(format!("Upbit l2 depth level {:?}", feed.l2_book_depth))
    })?;
    let symbol_key = symbol.as_str().to_owned();
    let event = {
        let mut syncs = feed.upbit_book_syncs.lock().expect("upbit sync lock");
        let sync = syncs
            .entry(symbol_key)
            .or_insert_with(|| UpbitBookSync::new(symbol, Some(units)));
        sync.apply(book)?
    };
    dispatch_l2_book(feed, event).await?;
    Ok(true)
}

async fn process_bithumb_text_message(
    feed: &ExchangeFeed,
    text: &str,
    received_ts: f64,
) -> Result<()> {
    let message: Value = serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
    if BithumbAdapter::is_heartbeat_status(&message) {
        return Ok(());
    }
    if let Some(error) = BithumbAdapter::error(&message) {
        return Err(error);
    }
    #[cfg(feature = "orderbook")]
    let handled = process_bithumb_orderbook_message(feed, &message, received_ts).await?;
    #[cfg(not(feature = "orderbook"))]
    let handled = false;
    if handled {
        return Ok(());
    }
    let events = BithumbAdapter::parse_messages_for_feed(feed, &message, received_ts);
    let data_type = bithumb_parser::message_type(&message);
    let configured = bithumb_parser::native_code(&message)
        .is_some_and(|native| BithumbAdapter::symbol_for_native(feed, native).is_some());
    if configured
        && data_type.is_some_and(|data_type| matches!(data_type, "trade" | "orderbook"))
        && events.is_empty()
    {
        return Err(Error::MalformedData(format!(
            "Bithumb message for {} produced no normalized events",
            data_type.unwrap_or("unknown type")
        )));
    }
    for event in events {
        dispatch_bithumb_event(feed, event).await?;
    }
    Ok(())
}

#[cfg(feature = "orderbook")]
async fn process_bithumb_orderbook_message(
    feed: &ExchangeFeed,
    message: &Value,
    received_ts: f64,
) -> Result<bool> {
    if bithumb_parser::message_type(message) != Some("orderbook") {
        return Ok(false);
    }
    let native = bithumb_parser::native_code(message)
        .ok_or_else(|| Error::Parse("Bithumb orderbook message is missing its code".to_owned()))?;
    let Some(symbol) = BithumbAdapter::symbol_for_native(feed, native).cloned() else {
        return Ok(true);
    };
    if !feed.subscribes(Channel::L2Book, &symbol) {
        return Ok(true);
    }
    let book = bithumb_parser::parse_book_for_symbol(message, received_ts, &symbol)
        .ok_or_else(|| Error::Parse("failed to parse Bithumb orderbook snapshot".to_owned()))?;
    let symbol_key = symbol.as_str().to_owned();
    let event = {
        let mut syncs = feed.bithumb_book_syncs.lock().expect("bithumb sync lock");
        let sync = syncs
            .entry(symbol_key)
            .or_insert_with(|| BithumbBookSync::new(symbol));
        sync.apply(book)?
    };
    dispatch_l2_book(feed, event).await?;
    Ok(true)
}

async fn process_coinbase_text_message(
    feed: &ExchangeFeed,
    text: &str,
    received_ts: f64,
) -> Result<()> {
    let message: Value = serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
    if let Some(control) = CoinbaseAdapter::parse_control_message(&message) {
        control?;
        return Ok(());
    }
    #[cfg(feature = "orderbook")]
    if process_coinbase_orderbook_message(feed, &message, received_ts).await? {
        return Ok(());
    }
    let events = CoinbaseAdapter::parse_messages_for_feed(feed, &message, received_ts);
    let channel = coinbase_parser::channel(&message);
    let configured = coinbase_parser::event_rows(&message).iter().any(|event| {
        coinbase_parser::event_product(event)
            .is_some_and(|native| CoinbaseAdapter::symbol_for_native(feed, native).is_some())
    });
    let market_channel = matches!(
        channel,
        Some("ticker" | "market_trades" | "candles" | "l2_data")
    );
    if configured && channel.is_some() && market_channel && events.is_empty() {
        return Err(Error::MalformedData(format!(
            "Coinbase message for {} produced no normalized events",
            channel.unwrap_or("unknown channel")
        )));
    }
    for event in events {
        dispatch_coinbase_event(feed, event).await?;
    }
    Ok(())
}

#[cfg(feature = "orderbook")]
async fn process_coinbase_orderbook_message(
    feed: &ExchangeFeed,
    message: &Value,
    received_ts: f64,
) -> Result<bool> {
    if coinbase_parser::channel(message) != Some("l2_data") {
        return Ok(false);
    }
    let exchange_ts = coinbase_parser::message_timestamp(message, received_ts);
    let events = coinbase_parser::event_rows(message);
    if events.is_empty() {
        return Err(Error::Parse(
            "Coinbase l2_data message has no events".to_owned(),
        ));
    }
    for event in events {
        let native = coinbase_parser::event_product(event).ok_or_else(|| {
            Error::Parse("Coinbase l2_data event is missing product_id".to_owned())
        })?;
        let Some(symbol) = CoinbaseAdapter::symbol_for_native(feed, native).cloned() else {
            // A connection may carry products this feed does not subscribe.
            continue;
        };
        if !feed.subscribes(Channel::L2Book, &symbol) {
            continue;
        }
        let update = coinbase_parser::parse_book_event(event, exchange_ts, received_ts, &symbol)
            .ok_or_else(|| Error::Parse("failed to parse Coinbase level2 event".to_owned()))?;
        let symbol_key = symbol.as_str().to_owned();
        let outcome = {
            let mut syncs = feed.coinbase_book_syncs.lock().expect("coinbase sync lock");
            let sync = syncs
                .entry(symbol_key)
                .or_insert_with(|| CoinbaseBookSync::new(symbol));
            sync.apply(update)?
        };
        match outcome {
            CoinbaseBookOutcome::Publish(book) => dispatch_l2_book(feed, book).await?,
        }
    }
    Ok(true)
}

async fn process_deribit_text_message(
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
        #[cfg(feature = "orderbook")]
        if deribit_parser::channel_family(channel) == "book" {
            // The book path above owns this family; an empty event list here is
            // impossible for a configured instrument.
        }
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
        let mut syncs = feed.deribit_book_syncs.lock().expect("deribit sync lock");
        let sync = syncs
            .entry(symbol_key)
            .or_insert_with(|| DeribitBookSync::new(symbol.clone()));
        sync.apply(update)?
    };
    match outcome {
        DeribitBookOutcome::Publish(book) => dispatch_deribit_l2_book(feed, book).await?,
        DeribitBookOutcome::Stale => {}
        DeribitBookOutcome::Resnapshot => {
            outbound.extend(DeribitAdapter::book_resubscribe_messages(feed, &symbol)?);
        }
    }
    Ok(true)
}

async fn process_htx_text_message(
    feed: &ExchangeFeed,
    text: &str,
    received_ts: f64,
    outbound: &mut Vec<String>,
) -> Result<()> {
    let message: Value = serde_json::from_str(text).map_err(|e| Error::Parse(e.to_string()))?;
    if let Some(control) = HtxAdapter::parse_control_message(&message) {
        match control? {
            HtxControl::Ack => {}
            HtxControl::Ping(_) => {
                if let Some(pong) = HtxAdapter::pong_message(&message) {
                    outbound.push(pong);
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
    outbound: &mut Vec<String>,
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
        let mut syncs = feed.htx_book_syncs.lock().expect("htx sync lock");
        let sync = syncs
            .entry(symbol_key)
            .or_insert_with(|| HtxBookSync::new(symbol.clone(), mode));
        sync.apply(update)?
    };
    match outcome {
        HtxBookOutcome::Publish(book) => dispatch_htx_l2_book(feed, book).await?,
        HtxBookOutcome::Stale => {}
        HtxBookOutcome::Resnapshot => {
            outbound.extend(HtxAdapter::book_resubscribe_messages(feed, &symbol)?);
        }
    }
    Ok(true)
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
            .hyperliquid_book_syncs
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

#[cfg_attr(not(test), allow(dead_code))]
async fn process_gateio_text_message(
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

async fn process_gateio_text_message_for_plan(
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
        if !feed.subscribes(Channel::L2Book, update.book.symbol()) {
            continue;
        }
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
            dispatch_okx_l2_book(feed, book).await?;
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
            dispatch_bitget_l2_book(feed, book).await?;
        }
    }

    Ok(true)
}

pub(crate) fn current_timestamp() -> f64 {
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
        process_bithumb_text_message, process_bybit_text_message, process_gateio_text_message,
        process_kraken_text_message, process_kucoin_text_message, process_okx_text_message,
        process_upbit_text_message,
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
        exchange::bithumb::Bithumb,
        exchange::bybit::Bybit,
        exchange::coinbase::Coinbase,
        exchange::deribit::Deribit,
        exchange::gateio::Gateio,
        exchange::htx::Htx,
        exchange::hyperliquid::Hyperliquid,
        exchange::kraken::Kraken,
        exchange::kucoin::Kucoin,
        exchange::okx::Okx,
        exchange::upbit::Upbit,
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

    #[test]
    fn plans_korean_connection_urls() {
        let mut handler = FeedHandler::new();
        handler.add_feed(Upbit::new().trade().symbol("BTC-KRW").build());
        assert_eq!(
            planned_connection_urls(&handler),
            vec!["wss://api.upbit.com/websocket/v1".to_owned()]
        );

        let mut handler = FeedHandler::new();
        handler.add_feed(Bithumb::new().trade().symbol("BTC-KRW").build());
        assert_eq!(
            planned_connection_urls(&handler),
            vec!["wss://ws-api.bithumb.com/websocket/v1".to_owned()]
        );
    }

    #[test]
    fn plans_kucoin_connection_urls_per_product() {
        let mut spot = FeedHandler::new();
        spot.add_feed(
            Kucoin::new()
                .ticker()
                .trade()
                .l2_book()
                .symbol("BTC-USDT")
                .build(),
        );
        assert_eq!(
            planned_connection_urls(&spot),
            vec!["wss://x-push-spot.kucoin.com".to_owned()]
        );

        let mut futures = FeedHandler::new();
        futures.add_feed(
            Kucoin::new()
                .ticker()
                .trade()
                .mark_price()
                .instrument(cryptofeed_core::symbol::Symbol::perpetual("BTC", "USDT"))
                .build(),
        );
        assert_eq!(
            planned_connection_urls(&futures),
            vec!["wss://x-push-futures.kucoin.com".to_owned()]
        );
    }

    #[test]
    fn plans_kraken_connection_urls() {
        let mut handler = FeedHandler::new();
        handler.add_feed(
            Kraken::new()
                .ticker()
                .trade()
                .l2_book()
                .candles()
                .symbol("BTC-USD")
                .build(),
        );
        assert_eq!(
            planned_connection_urls(&handler),
            vec!["wss://ws.kraken.com/v2".to_owned()]
        );
    }

    #[test]
    fn plans_hyperliquid_connection_urls() {
        let mut handler = FeedHandler::new();
        handler.add_feed(
            Hyperliquid::new()
                .ticker()
                .trade()
                .l2_book()
                .symbol("BTC-USD-PERP")
                .build(),
        );
        assert_eq!(
            planned_connection_urls(&handler),
            vec!["wss://api.hyperliquid.xyz/ws".to_owned()]
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

    #[cfg(feature = "trade")]
    struct OrderedTradeHandler {
        name: &'static str,
        seen: Arc<Mutex<Vec<&'static str>>>,
        behavior: u8,
    }
    #[cfg(feature = "trade")]
    #[async_trait]
    impl TradeHandler for OrderedTradeHandler {
        async fn on_trade(&self, mut trade: Trade) {
            assert_eq!(trade.price, rust_decimal::Decimal::ONE);
            trade.price = rust_decimal::Decimal::ZERO; // Each callback receives an independent model.
            let first = {
                let mut seen = self.seen.lock().unwrap();
                seen.push(self.name);
                seen.iter().filter(|name| **name == self.name).count() == 1
            };
            match self.behavior {
                1 => panic!("scripted handler failure"),
                2 if first => std::future::pending().await,
                _ => tokio::task::yield_now().await,
            }
        }
    }
    #[cfg(feature = "trade")]
    fn handler_trade() -> Trade {
        Trade {
            exchange: ExchangeId::Binance,
            symbol: cryptofeed_core::symbol::Symbol::spot("BTC", "USDT"),
            side: cryptofeed_trade::Side::Buy,
            amount: rust_decimal::Decimal::ONE,
            price: rust_decimal::Decimal::ONE,
            exchange_ts: 1.0,
            received_ts: 2.0,
            id: None,
            implied_volatility: None,
        }
    }
    #[cfg(feature = "trade")]
    #[tokio::test]
    async fn multiple_handlers_preserve_order_primary_replacement_and_single_publication() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let probe = |name| {
            Arc::new(OrderedTradeHandler {
                name,
                seen: seen.clone(),
                behavior: 0,
            })
        };
        let feed = Binance::new()
            .trade()
            .symbol("BTC-USDT")
            .trade_handler(probe("replaced"))
            .add_trade_handler(probe("second"))
            .trade_handler(probe("primary"))
            .add_trade_handler(probe("third"))
            .build();
        let mut handler = FeedHandler::new();
        let mut events = handler.subscribe();
        let counters = handler.event_counters();
        handler.add_feed(feed);
        let feed = handler.into_feeds().pop().unwrap();
        super::dispatch_trade(&feed, handler_trade()).await;
        assert_eq!(*seen.lock().unwrap(), ["primary", "second", "third"]);
        assert_eq!(counters.count(cryptofeed_core::exchange::Channel::Trade), 1);
        assert!(events.try_recv().is_ok());
        assert!(events.try_recv().is_err());
        let extra_only = Binance::new().trade().add_trade_handler(probe("only"));
        assert!(extra_only.has_handler_for(cryptofeed_core::exchange::Channel::Trade));
        super::dispatch_trade(&extra_only.symbol("BTC-USDT").build(), handler_trade()).await;
        assert_eq!(seen.lock().unwrap().last(), Some(&"only"));
    }
    #[cfg(feature = "trade")]
    #[tokio::test]
    async fn handler_panic_and_timeout_allow_later_handlers_and_future_events() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let probe = |name, behavior| {
            Arc::new(OrderedTradeHandler {
                name,
                seen: seen.clone(),
                behavior,
            })
        };
        let feed = Binance::new()
            .trade()
            .symbol("BTC-USDT")
            .trade_handler(probe("panic", 1))
            .add_trade_handler(probe("slow", 2))
            .add_trade_handler(probe("last", 0))
            .build();
        tokio::time::timeout(
            Duration::from_secs(7),
            super::dispatch_trade(&feed, handler_trade()),
        )
        .await
        .unwrap();
        assert_eq!(*seen.lock().unwrap(), ["panic", "slow", "last"]);
        super::dispatch_trade(&feed, handler_trade()).await;
        assert_eq!(
            *seen.lock().unwrap(),
            ["panic", "slow", "last", "panic", "slow", "last"]
        );
    }
    #[cfg(feature = "trade")]
    #[tokio::test]
    async fn configured_callback_deadline_reaches_dispatch_after_handler_registration() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let options = crate::options::RuntimeOptions::default()
            .handler_timeout(Duration::from_millis(10))
            .unwrap();
        let feed = Binance::new()
            .trade()
            .symbol("BTC-USDT")
            .runtime_options(options)
            .add_trade_handler(Arc::new(OrderedTradeHandler {
                name: "slow",
                seen: seen.clone(),
                behavior: 2,
            }))
            .add_trade_handler(Arc::new(OrderedTradeHandler {
                name: "later",
                seen: seen.clone(),
                behavior: 0,
            }))
            .build();
        let mut handler = FeedHandler::new();
        handler.add_feed(feed);
        let feed = handler.into_feeds().pop().unwrap();
        tokio::time::timeout(
            Duration::from_secs(1),
            super::dispatch_trade(&feed, handler_trade()),
        )
        .await
        .unwrap();
        assert_eq!(*seen.lock().unwrap(), ["slow", "later"]);
    }

    #[cfg(feature = "trade")]
    #[tokio::test]
    async fn shutdown_cancels_slow_handler_before_remaining_callbacks() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let feed = Binance::new()
            .trade()
            .symbol("BTC-USDT")
            .add_trade_handler(Arc::new(OrderedTradeHandler {
                name: "slow",
                seen: seen.clone(),
                behavior: 2,
            }))
            .add_trade_handler(Arc::new(OrderedTradeHandler {
                name: "later",
                seen: seen.clone(),
                behavior: 0,
            }))
            .build();
        let (stop, shutdown) = watch::channel(false);
        let running = tokio::spawn(super::run_feeds_until_shutdown(
            vec![feed],
            shutdown,
            |feed, _| async move {
                super::dispatch_trade(&feed, handler_trade()).await;
                Ok(())
            },
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            while seen.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        stop.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), running)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(*seen.lock().unwrap(), ["slow"]);
    }

    #[cfg(feature = "orderbook")]
    #[tokio::test]
    async fn l2_recovery_runtime_hooks_fence_resync_disconnect_and_stop() {
        use crate::feed::FeedState;
        let mut handler = FeedHandler::new();
        let id = handler.add_feed_with_id(Okx::new().l2_book().symbol("BTC-USDT").build());
        let handle = handler.l2_book_handle(); // May be enabled after initial registration.
        let (_, context, _commands) = handler.into_control_parts();
        let identity = crate::feed::FeedIdentity { id, generation: 1 };
        let monitor = context.monitor(identity, ExchangeId::Okx);
        monitor.lifecycle(FeedState::Started);
        let mut feed = Okx::new().l2_book().symbol("BTC-USDT").build();
        feed.identity = Some(identity);
        feed.monitor = Some(monitor.clone());
        super::readiness::track(
            &mut feed,
            "wss://ws.okx.com/ws/v5/public",
            vec!["BTC-USDT".into()],
        );
        let attempt = super::readiness::begin(&mut feed).unwrap();
        attempt.connected();
        let symbol = feed.symbols[0].clone();
        super::dispatch_l2_book(&feed, crate::books::tests::book(true, 1))
            .await
            .unwrap();
        assert!(handle.recover(identity, &symbol).snapshot.is_some());
        feed.invalidate_book_readiness("BTC-USDT");
        super::dispatch_l2_book(&feed, crate::books::tests::book(false, 2))
            .await
            .unwrap();
        assert!(handle.recover(identity, &symbol).snapshot.is_none());
        super::dispatch_l2_book(&feed, crate::books::tests::book(true, 3))
            .await
            .unwrap();
        assert!(handle.recover(identity, &symbol).snapshot.is_some());
        drop(attempt);
        assert!(handle.recover(identity, &symbol).snapshot.is_none());
        let attempt = super::readiness::begin(&mut feed).unwrap();
        attempt.connected();
        super::dispatch_l2_book(&feed, crate::books::tests::book(true, 4))
            .await
            .unwrap();
        assert_eq!(
            handle
                .recover(identity, &symbol)
                .snapshot
                .unwrap()
                .anchor
                .epoch,
            2
        );
        monitor.lifecycle(FeedState::Stopping);
        super::dispatch_l2_book(&feed, crate::books::tests::book(true, 9))
            .await
            .unwrap();
        assert!(handle.recover(identity, &symbol).snapshot.is_none());
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

    #[cfg(feature = "candles")]
    #[tokio::test]
    async fn candle_completion_policy_filters_every_delivery_surface_without_guessing() {
        use crate::exchange::CandlePolicy;
        use cryptofeed_core::{exchange::Channel, symbol::Symbol};
        use rust_decimal::Decimal;
        for (policy, expected) in [
            (CandlePolicy::All, vec![Some(false), None, Some(true)]),
            (CandlePolicy::ClosedOnly, vec![Some(true)]),
            (CandlePolicy::ClosedOrUnknown, vec![None, Some(true)]),
        ] {
            let seen = Arc::new(Mutex::new(0));
            let mut handler = FeedHandler::new();
            let mut events = handler.subscribe();
            let mut identified = handler.subscribe_identified();
            let counters = handler.event_counters();
            handler.add_feed(
                Binance::new()
                    .candles()
                    .symbol("BTC-USDT")
                    .candle_policy(policy)
                    .candle_handler(Arc::new(TestCandleHandler { seen: seen.clone() }))
                    .build(),
            );
            let feed = handler.into_feeds().pop().unwrap();
            let candle = Candle {
                exchange: ExchangeId::Binance,
                symbol: Symbol::spot("BTC", "USDT"),
                start: 1.0,
                end: 61.0,
                interval: "1m".into(),
                trades: None,
                open: Decimal::ONE,
                close: Decimal::ONE,
                high: Decimal::ONE,
                low: Decimal::ONE,
                volume: Decimal::ONE,
                closed: None,
                exchange_ts: 2.0,
                received_ts: 100.0, // Receipt after end must not imply completion.
            };
            for closed in [Some(false), None, Some(true)] {
                let mut next = candle.clone();
                next.closed = closed;
                super::dispatch_candle(&feed, next).await;
            }
            assert_eq!(*seen.lock().unwrap(), expected.len());
            assert_eq!(counters.count(Channel::Candles), expected.len() as u64);
            for closed in expected {
                let crate::feed::FeedEvent::Candle(actual) = events.try_recv().unwrap() else {
                    panic!("candle")
                };
                let mut wanted = candle.clone();
                wanted.closed = closed;
                assert_eq!(actual, wanted); // Unknown stays unknown; no model rewrite.
                let scoped = identified.try_recv().unwrap();
                assert_eq!(Some(scoped.identity), feed.identity());
                assert!(
                    matches!(scoped.event, crate::feed::FeedEvent::Candle(value) if value == wanted)
                );
            }
            assert!(events.try_recv().is_err());
            assert!(identified.try_recv().is_err());
        }
        assert_eq!(
            Binance::new().candles().build().candle_policy,
            CandlePolicy::All
        );
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

        super::dispatch_binance_event(&feed, trade_event)
            .await
            .unwrap();
        super::dispatch_binance_event(&feed, ticker_event)
            .await
            .unwrap();

        assert_eq!(*trade_seen.lock().expect("lock"), 1);
        assert_eq!(*ticker_seen.lock().expect("lock"), 1);
    }

    #[cfg(all(feature = "trade", feature = "orderbook"))]
    #[tokio::test]
    async fn channel_subscriptions_hydrate_and_filter_handlers_events_and_books() {
        use cryptofeed_core::exchange::Channel;
        let trade_seen = Arc::new(Mutex::new(0));
        let book_seen = Arc::new(Mutex::new(0));
        let mut handler = FeedHandler::new();
        let mut events = handler.subscribe();
        let counters = handler.event_counters();
        handler.add_feed(
            Binance::new()
                .subscription(Channel::Trade, ["BTC-USDT", "ETH-USDT"])
                .subscription(Channel::L2Book, ["BTC-USDT"])
                .exchange_symbol("BTCUSDT")
                .exchange_symbol("ETHUSDT")
                .trade_handler(Arc::new(TestTradeHandler {
                    seen: trade_seen.clone(),
                }))
                .orderbook_handler(Arc::new(TestOrderBookHandler {
                    seen: book_seen.clone(),
                }))
                .build(),
        );
        assert_eq!(handler.feed_count(), 1);
        let groups = super::hydrate_feed_symbols(handler.into_feeds())
            .await
            .unwrap();
        assert_eq!(groups.len(), 1);
        let trade = serde_json::json!({"e":"aggTrade","s":"ETHUSDT","a":1,"p":"100","q":"2","m":false,"T":1710000000123u64});
        for feed in &groups {
            super::process_binance_text_message(feed, &trade.to_string(), 1.0)
                .await
                .unwrap();
            for base in ["BTC", "ETH"] {
                super::dispatch_l2_book(
                    feed,
                    cryptofeed_orderbook::L2Book::Snapshot(cryptofeed_orderbook::L2BookSnapshot {
                        exchange: ExchangeId::Binance,
                        symbol: cryptofeed_core::symbol::Symbol::spot(base, "USDT"),
                        bids: vec![],
                        asks: vec![],
                        exchange_ts: 1.0,
                        received_ts: 2.0,
                    }),
                )
                .await
                .unwrap();
            }
        }
        assert_eq!(*trade_seen.lock().unwrap(), 1);
        assert_eq!(*book_seen.lock().unwrap(), 1);
        assert_eq!(counters.count(Channel::Trade), 1);
        assert_eq!(counters.count(Channel::L2Book), 1);
        assert!(matches!(
            events.try_recv().unwrap(),
            crate::feed::FeedEvent::Trade(_)
        ));
        assert!(matches!(
            events.try_recv().unwrap(),
            crate::feed::FeedEvent::L2Book(_)
        ));
        assert!(events.try_recv().is_err());
        let states = groups[0].orderbook_states.lock().unwrap();
        assert!(states.contains_key("BTC-USDT"));
        assert!(!states.contains_key("ETH-USDT"));
    }

    #[cfg(all(feature = "trade", feature = "orderbook"))]
    #[tokio::test]
    async fn packed_trade_only_symbol_does_not_create_book_sync_or_rest_work() {
        use cryptofeed_core::exchange::Channel;
        let feed = |exchange| {
            crate::exchange::ExchangeFeedBuilder::new(exchange)
                .subscription(Channel::Trade, ["ETH-USDT"])
                .subscription(Channel::L2Book, ["BTC-USDT"])
                .build()
        };
        let binance = feed(ExchangeId::Binance);
        let plan = BinanceAdapter::connection_plans(&binance)
            .unwrap()
            .remove(0);
        let mut receivers = std::collections::HashMap::new();
        let mut pending = std::collections::HashMap::new();
        let message = serde_json::json!({"e":"depthUpdate","s":"ETHUSDT","U":101,"u":102,"b":[["100","1"]],"a":[]});
        super::process_binance_orderbook_message(
            &binance,
            &plan,
            &message.to_string(),
            1.0,
            &mut receivers,
            &mut pending,
        )
        .await
        .unwrap();
        assert!(receivers.is_empty() && pending.is_empty());
        assert!(binance.binance_book_syncs.lock().unwrap().is_empty());
        let bybit = feed(ExchangeId::Bybit);
        let message = serde_json::json!({"topic":"orderbook.50.ETHUSDT","type":"delta","ts":1710000000000i64,"data":{"s":"ETHUSDT","b":[["100","1"]],"a":[],"u":102,"seq":102}});
        super::process_bybit_orderbook_message(&bybit, &message, 1.0)
            .await
            .unwrap();
        assert!(bybit.bybit_book_syncs.lock().unwrap().is_empty());
        let okx = feed(ExchangeId::Okx);
        let message = serde_json::json!({"arg":{"channel":"books","instId":"ETH-USDT"},"action":"update","data":[{"bids":[["100","1","0","1"]],"asks":[],"ts":"1710000000000","seqId":102,"prevSeqId":100}]});
        super::process_okx_orderbook_message(&okx, &message, 1.0)
            .await
            .unwrap();
        assert!(okx.okx_book_syncs.lock().unwrap().is_empty());
        let bitget = feed(ExchangeId::Bitget);
        let message = serde_json::json!({"arg":{"instType":"spot","topic":"books","symbol":"ETHUSDT"},"action":"update","data":[{"b":[["100","1"]],"a":[],"ts":"1710000000000","seq":102,"pseq":100}]});
        super::process_bitget_orderbook_message(&bitget, &message, 1.0)
            .await
            .unwrap();
        assert!(bitget.bitget_book_syncs.lock().unwrap().is_empty());
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

        super::dispatch_binance_event(&feed, event).await.unwrap();
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
        super::dispatch_binance_l2_book(&feed, book).await.unwrap();

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
    async fn gateio_initial_delta_starts_one_snapshot_after_buffering() {
        let feed = Gateio::new().l2_book().symbol("BTC-USDT").build();
        let mut plan = super::GateioAdapter::connection_plans(&feed)
            .unwrap()
            .remove(0);
        plan.snapshot_urls[0] = "test://snapshot-after-delta".to_owned();
        let mut receivers = std::collections::HashMap::new();
        let mut pending = std::collections::HashMap::new();
        for id in [101u64, 102] {
            let text = serde_json::json!({"channel":"spot.order_book_update","event":"update", "result":{"t":1710000000123u64,"s":"BTC_USDT","U":id,"u":id,"b":[["100","1"]],"a":[]}}).to_string();
            super::process_gateio_orderbook_message_for_plan(
                &feed,
                &plan,
                &text,
                1.0,
                &mut receivers,
                &mut pending,
                &mut Default::default(),
            )
            .await
            .unwrap();
        }
        assert_eq!(receivers.len(), 1);
        assert_eq!(
            pending["BTC-USDT"]
                .iter()
                .map(|delta| delta.last_update_id)
                .collect::<Vec<_>>(),
            [101, 102]
        );
    }

    #[cfg(all(feature = "orderbook", feature = "trade"))]
    #[tokio::test]
    async fn gateio_unrequested_book_does_not_start_snapshot_or_buffer() {
        let feed = Gateio::new().trade().symbol("BTC-USDT").build();
        let plan = super::GateioAdapter::connection_plans(&feed)
            .unwrap()
            .remove(0);
        let mut receivers = std::collections::HashMap::new();
        let mut pending = std::collections::HashMap::new();
        let text = serde_json::json!({"channel":"spot.order_book_update","event":"update", "result":{"t":1710000000123u64,"s":"BTC_USDT","U":101,"u":101,"b":[["100","1"]],"a":[]}}).to_string();
        super::process_gateio_orderbook_message_for_plan(
            &feed,
            &plan,
            &text,
            1.0,
            &mut receivers,
            &mut pending,
            &mut Default::default(),
        )
        .await
        .unwrap();
        assert!(receivers.is_empty());
        assert!(pending.is_empty());
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
        receivers.insert("BTC-USDT".to_owned(), rx.into());
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
        assert_eq!(attempts.get("BTC-USDT").map(|b| b.resnapshots), Some(1));

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
        receivers.insert("BTC-USDT".to_owned(), rx2.into());
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
        assert_eq!(attempts.get("BTC-USDT").map(|b| b.resnapshots), Some(1));
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
        receivers.insert("BTC-USDT".to_owned(), rx.into());
        let mut pending = std::collections::HashMap::new();
        pending.insert("BTC-USDT".to_owned(), vec![non_bridging]);
        let mut attempts = std::collections::HashMap::new();
        attempts.insert(
            "BTC-USDT".to_owned(),
            super::GateioResnapshotBudget {
                resnapshots: super::GATEIO_MAX_RESNAPSHOTS,
                unsynced_since: tokio::time::Instant::now(),
            },
        );

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
            &mut Default::default(),
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

        super::dispatch_bitget_event(&feed, event).await.unwrap();
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

        super::dispatch_bitget_event(&feed, event).await.unwrap();
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

    /// GZIP-compressed binary frame, as every HTX market/notification push
    /// (including acknowledgements) is transmitted.
    fn htx_frame(value: serde_json::Value) -> Message {
        use std::io::Write;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder
            .write_all(value.to_string().as_bytes())
            .expect("gzip write");
        Message::Binary(encoder.finish().expect("gzip finish").into())
    }

    #[cfg(all(feature = "trade", feature = "orderbook"))]
    #[tokio::test]
    async fn htx_spot_full_session_subscribes_acknowledges_and_delivers() {
        let trade_seen = Arc::new(Mutex::new(0));
        let book_seen = Arc::new(Mutex::new(0));
        let feed = Htx::new()
            .trade()
            .l2_book()
            .trade_handler(Arc::new(TestTradeHandler {
                seen: trade_seen.clone(),
            }))
            .orderbook_handler(Arc::new(TestOrderBookHandler {
                seen: book_seen.clone(),
            }))
            .symbol("BTC-USDT")
            .exchange_symbol("BTCUSDT")
            .build();
        let observed = feed.clone();
        let (mut client, mut server) = duplex_session(ExchangeId::Htx).await;
        client.set_exchange_for_test(ExchangeId::Htx);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let session = tokio::spawn(async move {
            super::consume_htx_session_with(feed, shutdown_rx, &mut client).await
        });

        let mut topics = Vec::new();
        for _ in 0..2 {
            let frame = match server.next().await {
                Some(Ok(Message::Text(text))) => text.to_string(),
                other => panic!("expected text subscribe frame, got {other:?}"),
            };
            let frame: serde_json::Value = serde_json::from_str(&frame).expect("json");
            topics.push(frame["sub"].as_str().unwrap().to_owned());
        }
        assert_eq!(
            topics,
            vec![
                "market.btcusdt.trade.detail".to_owned(),
                "market.btcusdt.depth.step0".to_owned()
            ]
        );
        server
            .send(htx_frame(serde_json::json!({
                "id": "cf-1", "status": "ok", "subbed": "market.btcusdt.trade.detail",
                "ts": 1793757092943_i64
            })))
            .await
            .expect("trade ack");
        server
            .send(htx_frame(serde_json::json!({
                "ch": "market.btcusdt.depth.step0", "ts": 1793757092943_i64,
                "tick": {
                    "bids": [[76760.06, 0.5]], "asks": [[76760.07, 0.25]],
                    "version": 136_998_124_622_u64, "ts": 1793757092940_i64
                }
            })))
            .await
            .expect("depth");
        server
            .send(htx_frame(serde_json::json!({
                "ch": "market.btcusdt.trade.detail", "ts": 1793757092950_i64,
                "tick": {"id": 1370054451093592864_i64, "ts": 1793757092948_i64,
                    "data": [{"id": 1370054451093592864_i64,
                        "ts": 1793757092948_i64, "tradeId": 102523573486_i64,
                        "amount": 0.006754, "price": 52648.62, "direction": "buy"}]}
            })))
            .await
            .expect("trade");
        wait_for_seen(|| {
            *trade_seen.lock().expect("lock") == 1 && *book_seen.lock().expect("lock") == 1
        })
        .await;
        shutdown_tx.send(true).expect("shutdown");
        session
            .await
            .expect("session task")
            .expect("clean session shutdown");
        let states = observed.orderbook_states.lock().expect("lock");
        let state = states.get("BTC-USDT").expect("book state");
        assert_eq!(state.bids()[0].amount.to_string(), "0.5");
        assert_eq!(state.asks()[0].amount.to_string(), "0.25");
    }

    #[cfg(feature = "orderbook")]
    #[tokio::test]
    async fn htx_swap_incremental_depth_gap_resubscribes_and_replaces() {
        let book_seen = Arc::new(Mutex::new(0));
        let feed = Htx::new()
            .l2_book()
            .orderbook_handler(Arc::new(TestOrderBookHandler {
                seen: book_seen.clone(),
            }))
            .symbol("BTC-USDT-PERP")
            .exchange_symbol("BTC-USDT")
            .build();
        let observed = feed.clone();
        let (mut client, mut server) = duplex_session(ExchangeId::Htx).await;
        client.set_exchange_for_test(ExchangeId::Htx);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let session = tokio::spawn(async move {
            super::consume_htx_session_with(feed, shutdown_rx, &mut client).await
        });
        let subscribe = match server.next().await {
            Some(Ok(Message::Text(text))) => text.to_string(),
            other => panic!("expected subscribe, got {other:?}"),
        };
        let subscribe: serde_json::Value = serde_json::from_str(&subscribe).expect("json");
        assert_eq!(subscribe["sub"], "market.BTC-USDT.depth.size_20.high_freq");
        assert_eq!(subscribe["data_type"], "incremental");
        server
            .send(htx_frame(
                serde_json::json!({"ch": subscribe["sub"].as_str().unwrap(),
                "ts": 1_i64, "tick": {"asks": [[101, 1]], "bids": [[100, 1]],
                "event": "snapshot", "version": 10_u64, "ts": 1_i64}}),
            ))
            .await
            .expect("snapshot");
        server
            .send(htx_frame(
                serde_json::json!({"ch": subscribe["sub"].as_str().unwrap(),
                "ts": 2_i64, "tick": {"asks": [], "bids": [[100, 2]],
                "event": "update", "version": 11_u64, "ts": 2_i64}}),
            ))
            .await
            .expect("update");
        server
            .send(htx_frame(
                serde_json::json!({"ch": subscribe["sub"].as_str().unwrap(),
                "ts": 4_i64, "tick": {"asks": [], "bids": [[100, 3]],
                "event": "update", "version": 13_u64, "ts": 4_i64}}),
            ))
            .await
            .expect("gap update");
        let unsubscribe = match server.next().await {
            Some(Ok(Message::Text(text))) => text.to_string(),
            other => panic!("expected unsubscribe, got {other:?}"),
        };
        let unsubscribe: serde_json::Value = serde_json::from_str(&unsubscribe).expect("json");
        assert_eq!(
            unsubscribe["unsub"],
            "market.BTC-USDT.depth.size_20.high_freq"
        );
        let resubscribe = match server.next().await {
            Some(Ok(Message::Text(text))) => text.to_string(),
            other => panic!("expected resubscribe, got {other:?}"),
        };
        let resubscribe: serde_json::Value = serde_json::from_str(&resubscribe).expect("json");
        assert_eq!(resubscribe["data_type"], "incremental");
        server
            .send(htx_frame(
                serde_json::json!({"ch": resubscribe["sub"].as_str().unwrap(),
                "ts": 5_i64, "tick": {"asks": [[102, 4]], "bids": [[99, 5]],
                "event": "snapshot", "version": 20_u64, "ts": 5_i64}}),
            ))
            .await
            .expect("replacement snapshot");
        wait_for_seen(|| *book_seen.lock().expect("lock") == 3).await;
        shutdown_tx.send(true).expect("shutdown");
        session
            .await
            .expect("session task")
            .expect("clean session shutdown");
        let states = observed.orderbook_states.lock().expect("lock");
        let state = states.get("BTC-USDT-PERP").expect("book state");
        assert_eq!(state.bids()[0].amount.to_string(), "5");
        assert_eq!(state.asks()[0].amount.to_string(), "4");
    }

    #[cfg(all(feature = "funding", feature = "liquidations"))]
    #[tokio::test]
    async fn htx_swap_notification_session_delivers_funding_and_liquidations() {
        let funding_seen = Arc::new(Mutex::new(0));
        let liquidation_seen = Arc::new(Mutex::new(0));
        let feed = Htx::new()
            .funding()
            .liquidations()
            .funding_handler(Arc::new(TestFundingHandler {
                seen: funding_seen.clone(),
            }))
            .liquidation_handler(Arc::new(TestLiquidationHandler {
                seen: liquidation_seen.clone(),
            }))
            .symbol("BTC-USDT-PERP")
            .exchange_symbol("BTC-USDT")
            .build();
        let (mut client, mut server) = duplex_session(ExchangeId::Htx).await;
        client.set_exchange_for_test(ExchangeId::Htx);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let session = tokio::spawn(async move {
            super::consume_htx_session_with(feed, shutdown_rx, &mut client).await
        });
        let mut topics = Vec::new();
        for _ in 0..2 {
            let frame = match server.next().await {
                Some(Ok(Message::Text(text))) => text.to_string(),
                other => panic!("expected text subscribe, got {other:?}"),
            };
            let frame: serde_json::Value = serde_json::from_str(&frame).expect("json");
            assert_eq!(frame["op"], "sub");
            topics.push(frame["topic"].as_str().unwrap().to_owned());
        }
        assert_eq!(
            topics,
            vec![
                "public.BTC-USDT.funding_rate".to_owned(),
                "public.BTC-USDT.liquidation_orders".to_owned()
            ]
        );
        server
            .send(htx_frame(serde_json::json!({
                "op": "sub", "cid": "cf-1", "topic": "public.BTC-USDT.funding_rate",
                "ts": 1793757247456_i64, "err-code": 0
            })))
            .await
            .expect("funding ack");
        server
            .send(htx_frame(serde_json::json!({
                "op": "notify", "topic": "public.BTC-USDT.funding_rate",
                "ts": 1793757246436_i64, "data": [{
                    "symbol": "BTC", "contract_code": "BTC-USDT", "fee_asset": "USDT",
                    "funding_time": "1793757236000", "funding_rate": "0.0001",
                    "estimated_rate": null, "settlement_time": "1793779200000"
                }]
            })))
            .await
            .expect("funding");
        server
            .send(htx_frame(serde_json::json!({
                "op": "notify", "topic": "public.BTC-USDT.liquidation_orders",
                "ts": 1793757247456_i64, "data": [{
                    "symbol": "BTC", "contract_code": "BTC-USDT", "direction": "sell",
                    "offset": "close", "volume": 432, "price": 0.7858,
                    "created_at": 1793757247000_i64, "amount": 432,
                    "trade_turnover": 339.4656, "contract_type": "swap",
                    "pair": "BTC-USDT", "business_type": "swap"
                }]
            })))
            .await
            .expect("liquidation");
        wait_for_seen(|| {
            *funding_seen.lock().expect("lock") == 1 && *liquidation_seen.lock().expect("lock") == 1
        })
        .await;
        shutdown_tx.send(true).expect("shutdown");
        session
            .await
            .expect("session task")
            .expect("clean session shutdown");
    }

    #[cfg(feature = "trade")]
    #[tokio::test]
    async fn htx_market_ping_is_answered_with_the_same_integer() {
        let feed = Htx::new()
            .trade()
            .trade_handler(Arc::new(TestTradeHandler {
                seen: Arc::new(Mutex::new(0)),
            }))
            .symbol("BTC-USDT")
            .exchange_symbol("BTCUSDT")
            .build();
        let (mut client, mut server) = duplex_session(ExchangeId::Htx).await;
        client.set_exchange_for_test(ExchangeId::Htx);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let session = tokio::spawn(async move {
            super::consume_htx_session_with(feed, shutdown_rx, &mut client).await
        });
        assert!(matches!(server.next().await, Some(Ok(Message::Text(_)))));
        server
            .send(htx_frame(serde_json::json!({"ping": 1793757093533_i64})))
            .await
            .expect("ping");
        let pong = match server.next().await {
            Some(Ok(Message::Text(text))) => text.to_string(),
            other => panic!("expected pong, got {other:?}"),
        };
        let pong: serde_json::Value = serde_json::from_str(&pong).expect("json");
        assert_eq!(pong, serde_json::json!({"pong": 1793757093533_i64}));
        shutdown_tx.send(true).expect("shutdown");
        let _ = session.await;
    }

    #[cfg(feature = "orderbook")]
    #[tokio::test]
    async fn bybit_managed_session_confirms_subscription_then_synchronizes_book() {
        use crate::feed::{FeedId, FeedIdentity, FeedState};
        let monitor = Arc::new(super::readiness::FeedMonitor::new(
            FeedIdentity {
                id: FeedId::allocate(),
                generation: 1,
            },
            ExchangeId::Bybit,
            None,
        ));
        monitor.configure(1, vec!["BTC-USDT".into()]);
        monitor.lifecycle(FeedState::Started);
        let mut feed = Bybit::new()
            .l2_book()
            .symbol("BTC-USDT")
            .exchange_symbol("BTCUSDT")
            .build();
        feed.monitor = Some(monitor.clone());
        super::readiness::track(
            &mut feed,
            "wss://stream.bybit.com/v5/public/spot",
            vec!["BTC-USDT".into()],
        );
        let attempt = super::readiness::begin(&mut feed);
        let (mut client, mut server) = duplex_session(ExchangeId::Bybit).await;
        client.set_readiness(attempt);
        let (stop, receiver) = watch::channel(false);
        let task = tokio::spawn(async move {
            super::consume_bybit_session_with(feed, receiver, &mut client).await
        });
        let Message::Text(request) = server.next().await.unwrap().unwrap() else {
            panic!("subscription expected");
        };
        let request: serde_json::Value = serde_json::from_str(&request).unwrap();
        server
            .send(Message::Text(
                serde_json::json!({"op":"subscribe","success":true,"req_id":request["req_id"]})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        wait_for_seen(|| monitor.snapshot().state == FeedState::Subscribed).await;
        assert_eq!(monitor.snapshot().books_synchronized, 0);
        server.send(Message::Text(serde_json::json!({"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1710000000123u64,"data":{"s":"BTCUSDT","b":[["100","1"]],"a":[["101","2"]],"u":1,"seq":1}}).to_string().into())).await.unwrap();
        wait_for_seen(|| monitor.snapshot().is_ready()).await;
        assert_eq!(monitor.snapshot().books_synchronized, 1);
        assert_eq!(monitor.snapshot().observed_pairs, 1);
        assert!(monitor.snapshot().last_event_at.is_some());
        assert!(monitor.snapshot().connections[0].last_received_at.is_some());
        stop.send(true).unwrap();
        task.await.unwrap().unwrap();
        assert!(!monitor.snapshot().is_ready());
        assert_eq!(monitor.snapshot().books_synchronized, 0);
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
        receivers.insert("BTC-USDT".to_owned(), snapshot_rx.into());
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
            .send(Message::Text(depth_update.to_string().into()))
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
            .send(Message::Text(ticker.to_string().into()))
            .await
            .expect("send ticker");
        server
            .send(Message::Text(trade.to_string().into()))
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

    #[cfg(feature = "trade")]
    #[tokio::test]
    async fn kucoin_full_session_subscribes_acknowledges_and_delivers() {
        let trade_seen = Arc::new(Mutex::new(0));
        let feed = Kucoin::new()
            .trade()
            .trade_handler(Arc::new(TestTradeHandler {
                seen: trade_seen.clone(),
            }))
            .symbol("BTC-USDT")
            .exchange_symbol("BTC-USDT")
            .build();
        let (mut client, mut server) = duplex_session(ExchangeId::Kucoin).await;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let session = tokio::spawn(async move {
            super::consume_kucoin_session_with(feed, shutdown_rx, &mut client).await
        });

        let subscribe = match server.next().await {
            Some(Ok(Message::Text(text))) => text,
            other => panic!("expected subscribe frame, got {other:?}"),
        };
        let subscribe: serde_json::Value = serde_json::from_str(&subscribe).expect("json");
        assert_eq!(subscribe["action"], "SUBSCRIBE");
        assert_eq!(subscribe["channel"], "trade");
        assert_eq!(subscribe["tradeType"], "SPOT");
        assert_eq!(subscribe["symbols"], serde_json::json!(["BTC-USDT"]));
        server
            .send(Message::Text(
                serde_json::json!({"id": subscribe["id"], "result": true})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send subscribe ack");
        let trade = serde_json::json!({
            "T": "trade.SPOT", "P": 1793689090235247913_u64,
            "d": {"E": 25082366677762048_u64, "M": 1793689090234000000_u64,
                  "S": "buy", "p": "76592.5", "q": "0.00009772",
                  "s": "BTC-USDT", "ti": "25082366677762048"}
        });
        server
            .send(Message::Text(trade.to_string().into()))
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

    #[cfg(all(feature = "trade", feature = "orderbook"))]
    #[tokio::test]
    async fn upbit_full_session_subscribes_and_delivers_trade_and_snapshot_book() {
        let trade_seen = Arc::new(Mutex::new(0));
        let book_seen = Arc::new(Mutex::new(0));
        let feed = Upbit::new()
            .trade()
            .l2_book()
            .trade_handler(Arc::new(TestTradeHandler {
                seen: trade_seen.clone(),
            }))
            .orderbook_handler(Arc::new(TestOrderBookHandler {
                seen: book_seen.clone(),
            }))
            .symbol("BTC-KRW")
            .exchange_symbol("KRW-BTC")
            .build();
        let (mut client, mut server) = duplex_session(ExchangeId::Upbit).await;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let observed = feed.clone();

        let session = tokio::spawn(async move {
            super::consume_upbit_session_with(feed, shutdown_rx, &mut client).await
        });

        let subscribe = match server.next().await {
            Some(Ok(Message::Text(text))) => text,
            other => panic!("expected subscription frame, got {other:?}"),
        };
        let subscribe: serde_json::Value = serde_json::from_str(&subscribe).expect("json");
        assert_eq!(subscribe[0]["ticket"], "cryptofeed-rs");
        assert_eq!(subscribe[1]["type"], "trade");
        assert_eq!(subscribe[1]["codes"], serde_json::json!(["KRW-BTC"]));
        assert_eq!(subscribe[2]["type"], "orderbook");
        assert_eq!(subscribe[2]["codes"], serde_json::json!(["KRW-BTC"]));
        assert_eq!(subscribe[3]["format"], "DEFAULT");

        let trade = serde_json::json!({
            "type": "trade", "code": "KRW-BTC",
            "trade_price": 48970000.0, "trade_volume": 1.4825, "ask_bid": "BID",
            "trade_timestamp": 1725929934373_u64, "sequential_id": 17259299343730000_u64,
            "timestamp": 1725929934483_u64, "stream_type": "REALTIME"
        });
        server
            .send(Message::Text(trade.to_string().into()))
            .await
            .expect("send trade");
        let book = serde_json::json!({
            "type": "orderbook", "code": "KRW-BTC", "timestamp": 1746602359173_u64,
            "total_ask_size": 0.68780013, "total_bid_size": 0.78754733,
            "orderbook_units": [
                {"ask_price": 125056000.0, "bid_price": 124743000.0, "ask_size": 0.17, "bid_size": 0.17},
                {"ask_price": 125207000.0, "bid_price": 124332000.0, "ask_size": 0.09, "bid_size": 0.09}
            ],
            "stream_type": "REALTIME"
        });
        server
            .send(Message::Text(book.to_string().into()))
            .await
            .expect("send orderbook");
        // The documented liveness frames are filtered, not parsed as data.
        server
            .send(Message::Text(r#"{"status":"UP"}"#.into()))
            .await
            .expect("send heartbeat");

        wait_for_seen(|| {
            *book_seen.lock().expect("lock") == 1 && *trade_seen.lock().expect("lock") == 1
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

        let states = observed.orderbook_states.lock().expect("lock");
        let state = states.get("BTC-KRW").expect("BTC-KRW state");
        assert_eq!(state.bids().len(), 2);
        assert_eq!(state.bids()[0].price.to_string(), "124743000.0");
        assert_eq!(state.asks()[1].price.to_string(), "125207000.0");
    }

    #[cfg(all(feature = "trade", feature = "orderbook"))]
    #[tokio::test]
    async fn bithumb_full_session_subscribes_and_delivers_trade_and_snapshot_book() {
        let trade_seen = Arc::new(Mutex::new(0));
        let book_seen = Arc::new(Mutex::new(0));
        let feed = Bithumb::new()
            .trade()
            .l2_book()
            .trade_handler(Arc::new(TestTradeHandler {
                seen: trade_seen.clone(),
            }))
            .orderbook_handler(Arc::new(TestOrderBookHandler {
                seen: book_seen.clone(),
            }))
            .symbol("BTC-KRW")
            .exchange_symbol("KRW-BTC")
            .build();
        let (mut client, mut server) = duplex_session(ExchangeId::Bithumb).await;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let observed = feed.clone();

        let session = tokio::spawn(async move {
            super::consume_bithumb_session_with(feed, shutdown_rx, &mut client).await
        });

        let subscribe = match server.next().await {
            Some(Ok(Message::Text(text))) => text,
            other => panic!("expected subscription frame, got {other:?}"),
        };
        let subscribe: serde_json::Value = serde_json::from_str(&subscribe).expect("json");
        assert_eq!(subscribe[0]["ticket"], "cryptofeed-rs");
        assert_eq!(subscribe[1]["type"], "trade");
        assert_eq!(subscribe[1]["codes"], serde_json::json!(["KRW-BTC"]));
        assert_eq!(subscribe[2]["type"], "orderbook");
        assert_eq!(subscribe[3]["format"], "DEFAULT");

        let trade = serde_json::json!({
            "type": "trade", "code": "KRW-BTC", "trade_price": 48970000.0,
            "trade_volume": 1.4825, "ask_bid": "BID",
            "trade_timestamp": 1725929934373_u64, "sequential_id": 17259299343730000_u64,
            "timestamp": 1725929934483_u64, "stream_type": "REALTIME"
        });
        server
            .send(Message::Text(trade.to_string().into()))
            .await
            .expect("send trade");
        let book = serde_json::json!({
            "type": "orderbook", "code": "KRW-BTC",
            "total_ask_size": 450.3526, "total_bid_size": 63.3006,
            "orderbook_units": [
                {"ask_price": 47880000.0, "bid_price": 47830000.0, "ask_size": 4.3478, "bid_size": 5.6370},
                {"ask_price": 48970000.0, "bid_price": 47790000.0, "ask_size": 2.3642, "bid_size": 0.9705}
            ],
            "timestamp": 1725929934483_u64, "stream_type": "REALTIME"
        });
        server
            .send(Message::Text(book.to_string().into()))
            .await
            .expect("send orderbook");

        wait_for_seen(|| {
            *book_seen.lock().expect("lock") == 1 && *trade_seen.lock().expect("lock") == 1
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

        let states = observed.orderbook_states.lock().expect("lock");
        let state = states.get("BTC-KRW").expect("BTC-KRW state");
        assert_eq!(state.bids().len(), 2);
        assert_eq!(state.bids()[0].price.to_string(), "47830000.0");
    }

    #[cfg(feature = "orderbook")]
    #[tokio::test]
    async fn upbit_snapshot_replaces_the_book_and_oversized_units_fail_closed() {
        let feed = Upbit::new()
            .l2_book()
            .l2_book_depth(1)
            .symbol("BTC-KRW")
            .exchange_symbol("KRW-BTC")
            .build();
        let first = serde_json::json!({
            "type": "orderbook", "code": "KRW-BTC.1", "timestamp": 1746602359173_u64,
            "orderbook_units": [
                {"ask_price": 125056000.0, "bid_price": 124743000.0, "ask_size": 0.17, "bid_size": 0.17}
            ]
        });
        process_upbit_text_message(&feed, &first.to_string(), 1.0)
            .await
            .expect("first snapshot");
        {
            let states = feed.orderbook_states.lock().expect("lock");
            let state = states.get("BTC-KRW").expect("state");
            assert_eq!(state.bids().len(), 1);
            assert_eq!(state.bids()[0].price.to_string(), "124743000.0");
        }
        let second = serde_json::json!({
            "type": "orderbook", "code": "KRW-BTC.1", "timestamp": 1746602359273_u64,
            "orderbook_units": [
                {"ask_price": 125056000.0, "bid_price": 124700000.0, "ask_size": 0.17, "bid_size": 1.17},
                {"ask_price": 125207000.0, "bid_price": 124332000.0, "ask_size": 0.09, "bid_size": 0.09}
            ]
        });
        assert!(matches!(
            process_upbit_text_message(&feed, &second.to_string(), 2.0).await,
            Err(Error::Parse(message)) if message.contains("but 1 were requested")
        ));
        {
            let states = feed.orderbook_states.lock().expect("lock");
            assert_eq!(states.get("BTC-KRW").expect("state").bids().len(), 1);
        }

        // A delta-shaped frame (no documented unit array) fails closed.
        let delta = serde_json::json!({
            "type": "orderbook", "code": "KRW-BTC", "stream_type": "REALTIME",
            "bids": [["124743000", "0.17"]], "asks": [["125056000", "0.17"]]
        });
        assert!(matches!(
            process_upbit_text_message(&feed, &delta.to_string(), 3.0).await,
            Err(Error::Parse(message)) if message.contains("failed to parse Upbit orderbook")
        ));
    }

    #[cfg(feature = "orderbook")]
    #[tokio::test]
    async fn bithumb_delta_shaped_book_frame_fails_closed() {
        let feed = Bithumb::new()
            .l2_book()
            .symbol("BTC-KRW")
            .exchange_symbol("KRW-BTC")
            .build();
        let delta = serde_json::json!({
            "type": "orderbook", "code": "KRW-BTC", "stream_type": "REALTIME",
            "bids": [["47830000", "5.637"]], "asks": [["47880000", "4.3478"]]
        });
        assert!(matches!(
            process_bithumb_text_message(&feed, &delta.to_string(), 1.0).await,
            Err(Error::Parse(message)) if message.contains("failed to parse Bithumb orderbook")
        ));
        assert!(feed.orderbook_states.lock().expect("lock").is_empty());
    }

    #[cfg(all(feature = "candles", feature = "trade"))]
    #[tokio::test]
    async fn upbit_candle_frame_dispatches_a_normalized_candle() {
        let candle_seen = Arc::new(Mutex::new(0));
        let feed = Upbit::new()
            .candles()
            .candles_interval("1h")
            .candle_handler(Arc::new(TestCandleHandler {
                seen: candle_seen.clone(),
            }))
            .symbol("BTC-KRW")
            .exchange_symbol("KRW-BTC")
            .build();
        let candle = serde_json::json!({
            "type": "candle.60m", "code": "KRW-BTC",
            "candle_date_time_utc": "2025-01-02T04:00:00",
            "opening_price": 130000000.0, "high_price": 131000000.0,
            "low_price": 129000000.0, "trade_price": 130500000.0,
            "candle_acc_trade_volume": 12.5, "candle_acc_trade_price": 1.6e9,
            "timestamp": 1735790400000_u64, "stream_type": "REALTIME"
        });
        process_upbit_text_message(&feed, &candle.to_string(), 1.0)
            .await
            .expect("candle");
        assert_eq!(*candle_seen.lock().expect("lock"), 1);
    }

    #[tokio::test]
    async fn korean_rejected_subscriptions_are_terminal() {
        for (exchange, feed) in [
            (
                ExchangeId::Upbit,
                Upbit::new().trade().symbol("BTC-KRW").build(),
            ),
            (
                ExchangeId::Bithumb,
                Bithumb::new().trade().symbol("BTC-KRW").build(),
            ),
        ] {
            let error = if exchange == ExchangeId::Upbit {
                process_upbit_text_message(
                    &feed,
                    r#"{"error":{"name":"WRONG_FORMAT","message":"invalid"}}"#,
                    1.0,
                )
                .await
                .expect_err("rejected subscription")
            } else {
                process_bithumb_text_message(
                    &feed,
                    r#"{"error":{"name":"WRONG_FORMAT","message":"invalid"}}"#,
                    1.0,
                )
                .await
                .expect_err("rejected subscription")
            };
            assert!(matches!(error, Error::Subscription(_)), "{exchange:?}");
            assert!(super::supervisor::is_permanent(&error));
        }
    }

    #[cfg(all(feature = "markprice", feature = "index", feature = "openinterest"))]
    #[tokio::test]
    async fn kucoin_mark_price_payload_dispatches_three_normalized_categories() {
        let mut handler = FeedHandler::new();
        let mut events = handler.subscribe();
        handler.add_feed(
            Kucoin::new()
                .mark_price()
                .index()
                .open_interest()
                .instrument(cryptofeed_core::symbol::Symbol::perpetual("BTC", "USDT"))
                .exchange_symbol("XBTUSDTM")
                .build(),
        );
        let feed = handler.into_feeds().pop().unwrap();
        let message = serde_json::json!({
            "T": "mark-price", "P": 1731899129000_u64,
            "d": {"s": "XBTUSDTM", "mp": "90445", "ip": "90445", "oi": "23445", "ts": 1731899128999_u64}
        });
        let mut outbound = Vec::new();
        process_kucoin_text_message(&feed, &message.to_string(), 1731899129.5, &mut outbound)
            .await
            .expect("process mark price");
        assert!(outbound.is_empty());
        let mut counts = [0; 3];
        while let Ok(event) = events.try_recv() {
            match event {
                crate::feed::FeedEvent::MarkPrice(value) => {
                    assert_eq!(value.symbol.as_str(), "BTC-USDT-PERP");
                    counts[0] += 1;
                }
                crate::feed::FeedEvent::IndexPrice(_) => counts[1] += 1,
                crate::feed::FeedEvent::OpenInterest(_) => counts[2] += 1,
                _ => panic!("unsubscribed event"),
            }
        }
        assert_eq!(counts, [1; 3]);
    }

    #[cfg(feature = "orderbook")]
    #[tokio::test]
    async fn kucoin_book_gap_requests_a_resnapshot_and_bridges_the_replacement() {
        let mut handler = FeedHandler::new();
        let mut events = handler.subscribe();
        handler.add_feed(
            Kucoin::new()
                .l2_book()
                .symbol("BTC-USDT")
                .exchange_symbol("BTC-USDT")
                .build(),
        );
        let feed = handler.into_feeds().pop().unwrap();
        let snapshot = serde_json::json!({
            "T": "obu.SPOT", "dp": "increment@10ms", "t": "snapshot",
            "P": 1768217874990007701_u64,
            "d": {"C": 100_u64, "M": 1768217874986000000_u64, "O": 100_u64,
                  "a": [["90701.2", "0.5771583"]],
                  "b": [["90701.1", "0.13918404"]], "s": "BTC-USDT"}
        });
        let mut outbound = Vec::new();
        process_kucoin_text_message(&feed, &snapshot.to_string(), 1.0, &mut outbound)
            .await
            .expect("snapshot");
        assert!(outbound.is_empty());
        assert!(matches!(
            events.try_recv().expect("snapshot event"),
            crate::feed::FeedEvent::L2Book(cryptofeed_orderbook::L2Book::Snapshot(_))
        ));

        let gap = serde_json::json!({
            "T": "obu.SPOT", "dp": "increment@10ms", "t": "delta",
            "P": 1768217909684719896_u64,
            "d": {"C": 106_u64, "M": 1768217909683000000_u64, "O": 105_u64,
                  "a": [], "b": [["90700.0", "2.0"]], "s": "BTC-USDT"}
        });
        process_kucoin_text_message(&feed, &gap.to_string(), 2.0, &mut outbound)
            .await
            .expect("gap");
        assert_eq!(outbound.len(), 2, "{outbound:?}");
        let unsubscribe: serde_json::Value = serde_json::from_str(&outbound[0]).expect("json");
        let resubscribe: serde_json::Value = serde_json::from_str(&outbound[1]).expect("json");
        assert_eq!(unsubscribe["action"], "UNSUBSCRIBE");
        assert_eq!(resubscribe["action"], "SUBSCRIBE");
        assert_eq!(resubscribe["depth"], "increment@10ms");
        assert!(matches!(
            events.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));

        let replacement = serde_json::json!({
            "T": "obu.SPOT", "dp": "increment@10ms", "t": "snapshot",
            "P": 1768217875990007701_u64,
            "d": {"C": 104_u64, "M": 1768217875986000000_u64, "O": 104_u64,
                  "a": [["90701.2", "0.5771583"]],
                  "b": [["90701.0", "0.5"]], "s": "BTC-USDT"}
        });
        process_kucoin_text_message(&feed, &replacement.to_string(), 3.0, &mut outbound)
            .await
            .expect("replacement snapshot");
        // The buffered delta (O=105) bridges the snapshot (C=104) and is
        // applied after it.
        assert!(matches!(
            events.try_recv().expect("snapshot"),
            crate::feed::FeedEvent::L2Book(cryptofeed_orderbook::L2Book::Snapshot(_))
        ));
        assert!(matches!(
            events.try_recv().expect("bridged delta"),
            crate::feed::FeedEvent::L2Book(cryptofeed_orderbook::L2Book::Delta(_))
        ));
        let states = feed.orderbook_states.lock().expect("lock");
        let state = states.get("BTC-USDT").expect("state");
        // The replacement snapshot level and the bridged delta level are both
        // present; the best bid is the snapshot's higher level.
        assert_eq!(state.bids()[0].price.to_string(), "90701.0");
        assert_eq!(state.bids()[1].price.to_string(), "90700.0");
        assert_eq!(state.bids().len(), 2);
    }

    #[cfg(feature = "orderbook")]
    #[tokio::test]
    async fn kucoin_snapshot_only_depth_replaces_the_book_without_sequence_state() {
        let mut handler = FeedHandler::new();
        let mut events = handler.subscribe();
        let mut feed = Kucoin::new()
            .l2_book()
            .l2_book_depth(5)
            .symbol("BTC-USDT")
            .exchange_symbol("BTC-USDT")
            .build();
        feed.l2_book_depth = Some(5);
        handler.add_feed(feed);
        let feed = handler.into_feeds().pop().unwrap();
        let first = serde_json::json!({
            "T": "obu.SPOT", "dp": "5", "t": "snapshot", "P": 1768217874990007701_u64,
            "d": {"C": 100_u64, "M": 1768217874986000000_u64, "O": 100_u64,
                  "a": [["90701.2", "0.5771583"]], "b": [["90701.1", "0.13918404"]], "s": "BTC-USDT"}
        });
        let mut outbound = Vec::new();
        process_kucoin_text_message(&feed, &first.to_string(), 1.0, &mut outbound)
            .await
            .expect("snapshot");
        assert!(matches!(
            events.try_recv().expect("event"),
            crate::feed::FeedEvent::L2Book(cryptofeed_orderbook::L2Book::Snapshot(_))
        ));
        let second = serde_json::json!({
            "T": "obu.SPOT", "dp": "5", "t": "snapshot", "P": 1768217875990007701_u64,
            "d": {"C": 101_u64, "M": 1768217875986000000_u64, "O": 101_u64,
                  "a": [["90701.2", "0.5771583"]], "b": [["90700.0", "0.25"]], "s": "BTC-USDT"}
        });
        process_kucoin_text_message(&feed, &second.to_string(), 2.0, &mut outbound)
            .await
            .expect("replacement");
        assert!(matches!(
            events.try_recv().expect("event"),
            crate::feed::FeedEvent::L2Book(cryptofeed_orderbook::L2Book::Snapshot(_))
        ));
        {
            let states = feed.orderbook_states.lock().expect("lock");
            let state = states.get("BTC-USDT").expect("state");
            assert_eq!(state.bids()[0].price.to_string(), "90700.0");
        }

        let delta = serde_json::json!({
            "T": "obu.SPOT", "dp": "5", "t": "delta", "P": 1768217876990007701_u64,
            "d": {"C": 102_u64, "M": 1768217876986000000_u64, "O": 102_u64,
                  "a": [], "b": [["90700.0", "0"]], "s": "BTC-USDT"}
        });
        assert!(matches!(
            process_kucoin_text_message(&feed, &delta.to_string(), 3.0, &mut outbound).await,
            Err(Error::Parse(message)) if message.contains("snapshot-only")
        ));
    }

    #[tokio::test]
    async fn kucoin_rejected_subscription_is_terminal() {
        let feed = Kucoin::new()
            .trade()
            .symbol("BTC-USDT")
            .exchange_symbol("BTC-USDT")
            .build();
        let mut outbound = Vec::new();
        assert!(matches!(
            process_kucoin_text_message(
                &feed,
                r#"{"id":"cf-0","result":false,"message":"invalid request data"}"#,
                1.0,
                &mut outbound
            )
            .await,
            Err(Error::Subscription(message)) if message.contains("invalid request data")
        ));
    }

    #[cfg(feature = "trade")]
    #[tokio::test]
    async fn kraken_full_session_subscribes_acknowledges_and_delivers() {
        let trade_seen = Arc::new(Mutex::new(0));
        let feed = Kraken::new()
            .trade()
            .trade_handler(Arc::new(TestTradeHandler {
                seen: trade_seen.clone(),
            }))
            .symbol("BTC-USD")
            .exchange_symbol("BTC/USD")
            .build();
        let (mut client, mut server) = duplex_session(ExchangeId::Kraken).await;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let session = tokio::spawn(async move {
            super::consume_kraken_session_with(feed, shutdown_rx, &mut client).await
        });

        let subscribe = match server.next().await {
            Some(Ok(Message::Text(text))) => text,
            other => panic!("expected subscribe frame, got {other:?}"),
        };
        let subscribe: serde_json::Value = serde_json::from_str(&subscribe).expect("json");
        assert_eq!(subscribe["method"], "subscribe");
        assert_eq!(subscribe["params"]["channel"], "trade");
        assert_eq!(
            subscribe["params"]["symbol"],
            serde_json::json!(["BTC/USD"])
        );
        server
            .send(Message::Text(
                serde_json::json!({
                    "method": "subscribe",
                    "req_id": subscribe["req_id"],
                    "result": {"channel": "trade", "snapshot": false, "symbol": "BTC/USD"},
                    "success": true,
                    "time_in": "2026-11-03T07:02:36.769747Z",
                    "time_out": "2026-11-03T07:02:36.769804Z"
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send subscribe ack");
        let trade = serde_json::json!({
            "channel": "trade", "type": "update",
            "data": [{"symbol": "BTC/USD", "side": "buy", "price": 76449.6,
                      "qty": 0.00043059, "ord_type": "limit",
                      "trade_id": 114005505,
                      "timestamp": "2026-11-03T07:02:37.171033Z"}]
        });
        server
            .send(Message::Text(trade.to_string().into()))
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

    #[cfg(all(feature = "trade", feature = "orderbook"))]
    #[tokio::test]
    async fn coinbase_full_session_subscribes_acknowledges_and_delivers() {
        let trade_seen = Arc::new(Mutex::new(0));
        let book_seen = Arc::new(Mutex::new(0));
        let feed = Coinbase::new()
            .trade()
            .l2_book()
            .trade_handler(Arc::new(TestTradeHandler {
                seen: trade_seen.clone(),
            }))
            .orderbook_handler(Arc::new(TestOrderBookHandler {
                seen: book_seen.clone(),
            }))
            .symbol("BTC-USD")
            .build();
        let observed = feed.clone();
        let (mut client, mut server) = duplex_session(ExchangeId::Coinbase).await;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let session = tokio::spawn(async move {
            super::consume_coinbase_session_with(feed, shutdown_rx, &mut client).await
        });

        // One channel per documented subscribe frame, then heartbeats.
        let mut channels = Vec::new();
        for _ in 0..3 {
            let frame = match server.next().await {
                Some(Ok(Message::Text(text))) => text,
                other => panic!("expected subscribe frame, got {other:?}"),
            };
            let frame: serde_json::Value = serde_json::from_str(&frame).expect("json");
            assert_eq!(frame["type"], "subscribe");
            channels.push(frame["channel"].as_str().unwrap().to_owned());
        }
        assert_eq!(channels, vec!["market_trades", "level2", "heartbeats"]);

        server
            .send(Message::Text(
                r#"{"channel":"subscriptions","timestamp":"2026-11-04T01:46:46.055143824Z","sequence_num":1,"events":[{"subscriptions":{"market_trades":["BTC-USD"],"level2":["BTC-USD"]}}]}"#
                    .into(),
            ))
            .await
            .expect("send subscribe ack");
        server
            .send(Message::Text(
                r#"{"channel":"l2_data","timestamp":"2026-11-04T01:47:08.029879Z","sequence_num":2,"events":[{"type":"snapshot","product_id":"BTC-USD","updates":[{"side":"bid","event_time":"2026-11-04T01:47:08.029879Z","price_level":"76760.06","new_quantity":"0.00522916"},{"side":"offer","event_time":"2026-11-04T01:47:08.029879Z","price_level":"76760.07","new_quantity":"0.02"}]}]}"#
                    .into(),
            ))
            .await
            .expect("send level2 snapshot");
        server
            .send(Message::Text(
                r#"{"channel":"l2_data","timestamp":"2026-11-04T01:47:08.078018Z","sequence_num":3,"events":[{"type":"update","product_id":"BTC-USD","updates":[{"side":"bid","event_time":"2026-11-04T01:47:08.078018Z","price_level":"76760.06","new_quantity":"0.03306503"},{"side":"offer","event_time":"2026-11-04T01:47:08.078018Z","price_level":"76760.07","new_quantity":"0"}]}]}"#
                    .into(),
            ))
            .await
            .expect("send level2 update");
        server
            .send(Message::Text(
                r#"{"channel":"market_trades","timestamp":"2026-11-04T01:46:46.384055093Z","sequence_num":4,"events":[{"type":"update","trades":[{"product_id":"BTC-USD","trade_id":"1114678923","price":"76778.5","size":"0.00000006","time":"2026-11-04T01:46:46.265419Z","side":"BUY"}]}]}"#
                    .into(),
            ))
            .await
            .expect("send trade");

        wait_for_seen(|| {
            *book_seen.lock().expect("lock") == 2 && *trade_seen.lock().expect("lock") == 1
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

        let states = observed.orderbook_states.lock().expect("lock");
        let state = states.get("BTC-USD").expect("BTC-USD state");
        assert_eq!(state.bids().len(), 1);
        assert_eq!(state.bids()[0].amount.to_string(), "0.03306503");
        assert_eq!(state.asks().len(), 0);
    }

    #[cfg(feature = "orderbook")]
    #[tokio::test]
    async fn coinbase_out_of_order_update_fails_the_session() {
        let feed = Coinbase::new()
            .l2_book()
            .orderbook_handler(Arc::new(TestOrderBookHandler {
                seen: Arc::new(Mutex::new(0)),
            }))
            .symbol("BTC-USD")
            .build();
        let (mut client, mut server) = duplex_session(ExchangeId::Coinbase).await;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let session = tokio::spawn(async move {
            super::consume_coinbase_session_with(feed, shutdown_rx, &mut client).await
        });
        // level2 subscribe frame, then heartbeats.
        assert!(server.next().await.is_some());
        assert!(server.next().await.is_some());
        server
            .send(Message::Text(
                r#"{"channel":"l2_data","timestamp":"2026-11-04T01:47:08.078018Z","sequence_num":1,"events":[{"type":"update","product_id":"BTC-USD","updates":[{"side":"bid","event_time":"2026-11-04T01:47:08.078018Z","price_level":"76760.06","new_quantity":"1"}]}]}"#
                    .into(),
            ))
            .await
            .expect("send update before snapshot");
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), session)
            .await
            .expect("session timeout")
            .expect("task");
        assert!(matches!(result, Err(Error::Parse(_))));
        let _ = shutdown_tx.send(true);
    }

    #[cfg(all(feature = "ticker", feature = "funding"))]
    #[tokio::test]
    async fn deribit_full_session_subscribes_acknowledges_and_splits_ticker_categories() {
        let ticker_seen = Arc::new(Mutex::new(0));
        let funding_seen = Arc::new(Mutex::new(0));
        let feed = Deribit::new()
            .ticker()
            .funding()
            .ticker_handler(Arc::new(TestTickerHandler {
                seen: ticker_seen.clone(),
            }))
            .funding_handler(Arc::new(TestFundingHandler {
                seen: funding_seen.clone(),
            }))
            .symbol("BTC-USD-PERP")
            .exchange_symbol("BTC-PERPETUAL")
            .build();
        let (mut client, mut server) = duplex_session(ExchangeId::Deribit).await;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let session = tokio::spawn(async move {
            super::consume_deribit_session_with(feed, shutdown_rx, &mut client).await
        });

        let heartbeat = match server.next().await {
            Some(Ok(Message::Text(text))) => text,
            other => panic!("expected heartbeat frame, got {other:?}"),
        };
        let heartbeat: serde_json::Value = serde_json::from_str(&heartbeat).expect("json");
        assert_eq!(heartbeat["method"], "public/set_heartbeat");
        let subscribe = match server.next().await {
            Some(Ok(Message::Text(text))) => text,
            other => panic!("expected subscribe frame, got {other:?}"),
        };
        let subscribe: serde_json::Value = serde_json::from_str(&subscribe).expect("json");
        assert_eq!(subscribe["method"], "public/subscribe");
        assert_eq!(
            subscribe["params"]["channels"],
            serde_json::json!(["ticker.BTC-PERPETUAL.100ms"])
        );
        server
            .send(Message::Text(
                serde_json::json!({
                    "jsonrpc": "2.0", "id": subscribe["id"],
                    "result": ["ticker.BTC-PERPETUAL.100ms"]
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send subscribe ack");
        let ticker = serde_json::json!({
            "jsonrpc": "2.0", "method": "subscription",
            "params": {"channel": "ticker.BTC-PERPETUAL.100ms", "data": {
                "timestamp": 1793757174238_i64, "state": "open",
                "index_price": 76833.47, "instrument_name": "BTC-PERPETUAL",
                "open_interest": 657029240, "mark_price": 76862.37,
                "current_funding": 1.2614e-4, "funding_8h": 7.185e-5,
                "best_ask_price": 76864.0, "best_bid_price": 76863.5,
                "best_ask_amount": 44630.0, "best_bid_amount": 17530.0
            }}
        });
        server
            .send(Message::Text(ticker.to_string().into()))
            .await
            .expect("send ticker");

        wait_for_seen(|| {
            *ticker_seen.lock().expect("lock") == 1 && *funding_seen.lock().expect("lock") == 1
        })
        .await;

        shutdown_tx.send(true).expect("shutdown");
        session
            .await
            .expect("session task")
            .expect("clean session shutdown");
    }

    #[cfg(feature = "orderbook")]
    #[tokio::test]
    async fn deribit_book_gap_triggers_bounded_in_session_resnapshot() {
        let book_seen = Arc::new(Mutex::new(0));
        let feed = Deribit::new()
            .l2_book()
            .orderbook_handler(Arc::new(TestOrderBookHandler {
                seen: book_seen.clone(),
            }))
            .symbol("BTC-USD-PERP")
            .exchange_symbol("BTC-PERPETUAL")
            .build();
        let observed = feed.clone();
        let (mut client, mut server) = duplex_session(ExchangeId::Deribit).await;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let session = tokio::spawn(async move {
            super::consume_deribit_session_with(feed, shutdown_rx, &mut client).await
        });
        assert!(server.next().await.is_some());
        let subscribe = match server.next().await {
            Some(Ok(Message::Text(text))) => text,
            other => panic!("expected subscribe frame, got {other:?}"),
        };
        let subscribe: serde_json::Value = serde_json::from_str(&subscribe).expect("json");
        server
            .send(Message::Text(
                serde_json::json!({"jsonrpc":"2.0","id":subscribe["id"],
                    "result":["book.BTC-PERPETUAL.100ms"]})
                .to_string()
                .into(),
            ))
            .await
            .expect("ack");
        server
            .send(Message::Text(
                serde_json::json!({"jsonrpc":"2.0","method":"subscription","params":{
                    "channel":"book.BTC-PERPETUAL.100ms","data":{
                        "timestamp": 1793757173936_i64, "type": "snapshot",
                        "change_id": 100_u64, "instrument_name": "BTC-PERPETUAL",
                        "bids": [["new", 76863.5, 17530.0]],
                        "asks": [["new", 76864.0, 44630.0]]}}})
                .to_string()
                .into(),
            ))
            .await
            .expect("snapshot");
        // A gap (prev_change_id 105 after anchor 100) requests a replacement.
        server
            .send(Message::Text(
                serde_json::json!({"jsonrpc":"2.0","method":"subscription","params":{
                    "channel":"book.BTC-PERPETUAL.100ms","data":{
                        "timestamp": 1793757173999_i64, "type": "change",
                        "change_id": 106_u64, "prev_change_id": 105_u64,
                        "instrument_name": "BTC-PERPETUAL",
                        "bids": [["change", 76863.5, 900.0]], "asks": []}}})
                .to_string()
                .into(),
            ))
            .await
            .expect("gap change");
        let unsubscribe = match server.next().await {
            Some(Ok(Message::Text(text))) => text,
            other => panic!("expected resnapshot frame, got {other:?}"),
        };
        let unsubscribe: serde_json::Value = serde_json::from_str(&unsubscribe).expect("json");
        assert_eq!(unsubscribe["method"], "public/unsubscribe");
        let resubscribe = match server.next().await {
            Some(Ok(Message::Text(text))) => text,
            other => panic!("expected resubscribe frame, got {other:?}"),
        };
        let resubscribe: serde_json::Value = serde_json::from_str(&resubscribe).expect("json");
        assert_eq!(resubscribe["method"], "public/subscribe");
        server
            .send(Message::Text(
                serde_json::json!({"jsonrpc":"2.0","method":"subscription","params":{
                    "channel":"book.BTC-PERPETUAL.100ms","data":{
                        "timestamp": 1793757174000_i64, "type": "snapshot",
                        "change_id": 200_u64, "instrument_name": "BTC-PERPETUAL",
                        "bids": [["new", 76860.0, 42.0]],
                        "asks": [["new", 76870.0, 7.0]]}}})
                .to_string()
                .into(),
            ))
            .await
            .expect("replacement snapshot");
        wait_for_seen(|| *book_seen.lock().expect("lock") == 2).await;

        shutdown_tx.send(true).expect("shutdown");
        session
            .await
            .expect("session task")
            .expect("clean session shutdown");
        let states = observed.orderbook_states.lock().expect("lock");
        let state = states.get("BTC-USD-PERP").expect("book state");
        assert_eq!(state.bids()[0].amount.to_string(), "42.0");
        assert_eq!(state.asks()[0].amount.to_string(), "7.0");
    }

    #[cfg(all(feature = "ticker", feature = "candles"))]
    #[tokio::test]
    async fn kraken_ticker_and_ohlc_dispatch_normalized_events() {
        let mut handler = FeedHandler::new();
        let mut events = handler.subscribe();
        handler.add_feed(
            Kraken::new()
                .ticker()
                .candles()
                .candles_interval("1m")
                .symbol("BTC-USD")
                .exchange_symbol("BTC/USD")
                .build(),
        );
        let feed = handler.into_feeds().pop().unwrap();
        let ticker = serde_json::json!({
            "channel": "ticker", "type": "snapshot",
            "data": [{"symbol": "BTC/USD", "bid": 76497.5, "bid_qty": 0.00065022,
                      "ask": 76497.6, "ask_qty": 0.83895981, "last": 76497.6,
                      "timestamp": "2026-11-03T07:01:22.016355Z"}]
        });
        let mut outbound = Vec::new();
        process_kraken_text_message(&feed, &ticker.to_string(), 1.0, &mut outbound)
            .await
            .expect("ticker");
        let crate::feed::FeedEvent::Ticker(value) = events.try_recv().expect("ticker event") else {
            panic!("expected ticker");
        };
        assert_eq!(value.symbol.as_str(), "BTC-USD");
        assert_eq!(value.bid.to_string(), "76497.5");

        let ohlc = serde_json::json!({
            "channel": "ohlc", "type": "snapshot",
            "timestamp": "2026-11-03T07:01:24.187297694Z",
            "data": [{"symbol": "BTC/USD", "open": 76381.0, "high": 76399.4,
                      "low": 76376.7, "close": 76399.4, "trades": 70,
                      "volume": 0.63763428, "vwap": 76384.9,
                      "interval_begin": "2026-11-03T06:52:00.000000000Z",
                      "interval": 1,
                      "timestamp": "2026-11-03T06:53:00.000000Z"}]
        });
        process_kraken_text_message(&feed, &ohlc.to_string(), 2.0, &mut outbound)
            .await
            .expect("ohlc");
        let crate::feed::FeedEvent::Candle(value) = events.try_recv().expect("candle event") else {
            panic!("expected candle");
        };
        assert_eq!(value.symbol.as_str(), "BTC-USD");
        assert_eq!(value.interval, "1m");
        assert_eq!(value.end - value.start, 60.0);
        assert_eq!(value.trades, Some(70));
        assert_eq!(value.closed, None);
    }

    #[cfg(feature = "orderbook")]
    #[tokio::test]
    async fn kraken_checksum_mismatch_requests_resnapshot_and_never_publishes_corrupt_book() {
        use crate::exchange::kraken::book_sync::checksum_of_state;

        let mut handler = FeedHandler::new();
        let mut events = handler.subscribe();
        handler.add_feed(
            Kraken::new()
                .l2_book()
                .symbol("BTC-USD")
                .exchange_symbol("BTC/USD")
                .build(),
        );
        let feed = handler.into_feeds().pop().unwrap();
        let mut state = cryptofeed_orderbook::L2BookState::new(
            cryptofeed_core::symbol::Symbol::spot("BTC", "USD"),
        );
        state.apply(cryptofeed_orderbook::L2Book::Snapshot(
            cryptofeed_orderbook::L2BookSnapshot {
                exchange: ExchangeId::Kraken,
                symbol: cryptofeed_core::symbol::Symbol::spot("BTC", "USD"),
                bids: vec![cryptofeed_orderbook::PriceLevel {
                    price: rust_decimal::Decimal::from_str_exact("100.0").unwrap(),
                    amount: rust_decimal::Decimal::from_str_exact("1.0").unwrap(),
                }],
                asks: vec![cryptofeed_orderbook::PriceLevel {
                    price: rust_decimal::Decimal::from_str_exact("101.0").unwrap(),
                    amount: rust_decimal::Decimal::from_str_exact("2.0").unwrap(),
                }],
                exchange_ts: 1.0,
                received_ts: 2.0,
            },
        ));
        let checksum = checksum_of_state(&state);
        let snapshot = serde_json::json!({
            "channel": "book", "type": "snapshot",
            "data": [{
                "symbol": "BTC/USD",
                "bids": [{"price": 100.0, "qty": 1.0}],
                "asks": [{"price": 101.0, "qty": 2.0}],
                "checksum": checksum,
                "timestamp": "2026-11-03T07:01:24.552449Z"
            }]
        });
        let mut outbound = Vec::new();
        process_kraken_text_message(&feed, &snapshot.to_string(), 1.0, &mut outbound)
            .await
            .expect("snapshot");
        assert!(outbound.is_empty());
        assert!(matches!(
            events.try_recv().expect("snapshot event"),
            crate::feed::FeedEvent::L2Book(cryptofeed_orderbook::L2Book::Snapshot(_))
        ));

        // A corrupt update must not publish; it clears the book and asks for a
        // replacement snapshot.
        let corrupt = serde_json::json!({
            "channel": "book", "type": "update",
            "data": [{
                "symbol": "BTC/USD",
                "bids": [{"price": 100.0, "qty": 0.0}],
                "asks": [],
                "checksum": 0xDEAD_BEEFu32,
                "timestamp": "2026-11-03T07:01:24.564465Z"
            }]
        });
        process_kraken_text_message(&feed, &corrupt.to_string(), 2.0, &mut outbound)
            .await
            .expect("corrupt update");
        assert_eq!(outbound.len(), 2, "{outbound:?}");
        let unsubscribe: serde_json::Value = serde_json::from_str(&outbound[0]).expect("json");
        let resubscribe: serde_json::Value = serde_json::from_str(&outbound[1]).expect("json");
        assert_eq!(unsubscribe["method"], "unsubscribe");
        assert_eq!(resubscribe["method"], "subscribe");
        assert_eq!(resubscribe["params"]["snapshot"], true);
        assert!(matches!(
            events.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
        assert!(
            feed.kraken_book_syncs
                .lock()
                .expect("lock")
                .get("BTC-USD")
                .expect("sync")
                .awaiting_snapshot()
        );

        // The replacement snapshot is verified and published.
        process_kraken_text_message(&feed, &snapshot.to_string(), 3.0, &mut outbound)
            .await
            .expect("replacement snapshot");
        assert!(matches!(
            events.try_recv().expect("snapshot"),
            crate::feed::FeedEvent::L2Book(cryptofeed_orderbook::L2Book::Snapshot(_))
        ));
    }

    #[tokio::test]
    async fn kraken_rejected_subscription_is_terminal() {
        let feed = Kraken::new()
            .trade()
            .symbol("BTC-USD")
            .exchange_symbol("BTC/USD")
            .build();
        let mut outbound = Vec::new();
        assert!(matches!(
            process_kraken_text_message(
                &feed,
                r#"{"error":"Currency pair not supported XBT/USD","method":"subscribe","req_id":1,"success":false,"symbol":"XBT/USD"}"#,
                1.0,
                &mut outbound
            )
            .await,
            Err(Error::Subscription(message)) if message.contains("Currency pair not supported")
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
            .send(Message::Text(trade.to_string().into()))
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
            .send(Message::Text(trade.to_string().into()))
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
        #[cfg(feature = "orderbook")]
        let receivers = std::collections::HashMap::new();
        #[cfg(feature = "orderbook")]
        let pending_deltas = std::collections::HashMap::new();

        let session = tokio::spawn(async move {
            super::consume_gateio_session_with(
                feed,
                plan,
                shutdown_rx,
                &mut client,
                #[cfg(feature = "orderbook")]
                receivers,
                #[cfg(feature = "orderbook")]
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
            .send(Message::Text(trade.to_string().into()))
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

    #[cfg(all(feature = "ticker", feature = "trade", feature = "orderbook"))]
    #[tokio::test]
    async fn hyperliquid_full_session_subscribes_acknowledges_and_delivers() {
        let ticker_seen = Arc::new(Mutex::new(0));
        let trade_seen = Arc::new(Mutex::new(0));
        let book_seen = Arc::new(Mutex::new(0));
        let l1_seen = Arc::new(Mutex::new(0));
        let feed = Hyperliquid::new()
            .ticker()
            .trade()
            .l2_book()
            .l1_book()
            .ticker_handler(Arc::new(TestTickerHandler {
                seen: ticker_seen.clone(),
            }))
            .trade_handler(Arc::new(TestTradeHandler {
                seen: trade_seen.clone(),
            }))
            .orderbook_handler(Arc::new(TestOrderBookHandler {
                seen: book_seen.clone(),
            }))
            .add_orderbook_handler(Arc::new(TestL1BookHandler {
                seen: l1_seen.clone(),
            }))
            .symbol("BTC-USD-PERP")
            .exchange_symbol("BTC")
            .build();
        let observed = feed.clone();
        let (mut client, mut server) = duplex_session(ExchangeId::Hyperliquid).await;
        client.set_exchange_for_test(ExchangeId::Hyperliquid);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let session = tokio::spawn(async move {
            super::consume_hyperliquid_session_with(feed, shutdown_rx, &mut client).await
        });

        // ticker and l1_book share one documented `bbo` subscription.
        let mut subscriptions = Vec::new();
        for _ in 0..3 {
            let frame = match server.next().await {
                Some(Ok(Message::Text(text))) => text.to_string(),
                other => panic!("expected text subscribe frame, got {other:?}"),
            };
            let frame: serde_json::Value = serde_json::from_str(&frame).expect("json");
            assert_eq!(frame["method"], "subscribe");
            subscriptions.push(frame["subscription"].clone());
        }
        assert!(subscriptions.contains(&serde_json::json!({"type": "bbo", "coin": "BTC"})));
        assert!(subscriptions.contains(&serde_json::json!({"type": "trades", "coin": "BTC"})));
        assert!(subscriptions.contains(&serde_json::json!({"type": "l2Book", "coin": "BTC"})));
        server
            .send(Message::Text(
                serde_json::json!({
                    "channel": "subscriptionResponse",
                    "data": {"method": "subscribe",
                             "subscription": {"type": "l2Book", "coin": "BTC"}}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("ack");
        server
            .send(Message::Text(
                serde_json::json!({
                    "channel": "l2Book",
                    "data": {"coin": "BTC", "time": 1_793_782_877_454_i64,
                        "levels": [
                            [{"px": "78454.0", "sz": "0.09296", "n": 5}],
                            [{"px": "78455.0", "sz": "20.3117", "n": 62}]
                        ]}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("book");
        server
            .send(Message::Text(
                serde_json::json!({
                    "channel": "trades",
                    "data": [{"coin": "BTC", "side": "A", "px": "78468.0",
                        "sz": "0.10569", "time": 1_793_782_868_033_i64,
                        "hash": "0x00", "tid": 1_i64, "users": ["a", "b"]}]
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("trade");
        server
            .send(Message::Text(
                serde_json::json!({
                    "channel": "bbo",
                    "data": {"coin": "BTC", "time": 1_793_782_877_739_i64,
                        "bbo": [{"px": "78451.0", "sz": "0.22297", "n": 1},
                                {"px": "78452.0", "sz": "13.21482", "n": 74}]}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("bbo");
        wait_for_seen(|| {
            *trade_seen.lock().expect("lock") == 1
                && *book_seen.lock().expect("lock") == 1
                && *ticker_seen.lock().expect("lock") == 1
                && *l1_seen.lock().expect("lock") == 1
        })
        .await;
        shutdown_tx.send(true).expect("shutdown");
        session
            .await
            .expect("session task")
            .expect("clean session shutdown");
        let states = observed.orderbook_states.lock().expect("lock");
        let state = states.get("BTC-USD-PERP").expect("book state");
        assert_eq!(state.bids()[0].amount.to_string(), "0.09296");
        assert_eq!(state.asks()[0].amount.to_string(), "20.3117");
    }

    #[cfg(feature = "ticker")]
    #[tokio::test(start_paused = true)]
    async fn hyperliquid_documented_ping_heartbeat_is_sent_and_pong_is_filtered() {
        let feed = Hyperliquid::new()
            .ticker()
            .ticker_handler(Arc::new(TestTickerHandler {
                seen: Arc::new(Mutex::new(0)),
            }))
            .symbol("BTC-USD-PERP")
            .exchange_symbol("BTC")
            .build();
        let (mut client, mut server) = duplex_session(ExchangeId::Hyperliquid).await;
        client.set_exchange_for_test(ExchangeId::Hyperliquid);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let session = tokio::spawn(async move {
            super::consume_hyperliquid_session_with(feed, shutdown_rx, &mut client).await
        });
        assert!(matches!(server.next().await, Some(Ok(Message::Text(_)))));
        // With time paused the 30 s client heartbeat deadline auto-advances.
        let ping = match server.next().await {
            Some(Ok(Message::Text(text))) => text.to_string(),
            other => panic!("expected heartbeat ping, got {other:?}"),
        };
        assert_eq!(ping, r#"{"method":"ping"}"#);
        // The documented `pong` reply is liveness, not market data.
        server
            .send(Message::Text(r#"{"channel":"pong"}"#.into()))
            .await
            .expect("pong");
        shutdown_tx.send(true).expect("shutdown");
        session
            .await
            .expect("session task")
            .expect("clean session shutdown");
    }

    #[cfg(feature = "trade")]
    #[tokio::test]
    async fn hyperliquid_error_channel_fails_the_session() {
        let feed = Hyperliquid::new()
            .trade()
            .trade_handler(Arc::new(TestTradeHandler {
                seen: Arc::new(Mutex::new(0)),
            }))
            .symbol("BTC-USD-PERP")
            .exchange_symbol("BTC")
            .build();
        let (mut client, mut server) = duplex_session(ExchangeId::Hyperliquid).await;
        client.set_exchange_for_test(ExchangeId::Hyperliquid);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let session = tokio::spawn(async move {
            super::consume_hyperliquid_session_with(feed, shutdown_rx, &mut client).await
        });
        assert!(matches!(server.next().await, Some(Ok(Message::Text(_)))));
        server
            .send(Message::Text(
                serde_json::json!({
                    "channel": "error",
                    "data": "Error parsing JSON into valid websocket request"
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("error");
        let error = session
            .await
            .expect("session task")
            .expect_err("error channel must fail the session");
        assert!(matches!(error, Error::Subscription(_)));
        drop(shutdown_tx);
    }
}
