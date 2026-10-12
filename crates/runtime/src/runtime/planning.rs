mod optimal;
use crate::exchange::{
    ExchangeFeed, binance::adapter::BinanceAdapter, bitget::adapter::BitgetAdapter,
    bybit::adapter::BybitAdapter, gateio::adapter::GateioAdapter, okx::adapter::OkxAdapter,
};
use cryptofeed_core::{
    error::{Error, Result},
    exchange::ExchangeId,
};
use serde_json::Value;

const BINANCE_STREAMS: usize = 1024;
const BITGET_TOPICS: usize = 49;
const BYBIT_BYTES: usize = 21000;
const OKX_BYTES: usize = 64 * 1024;
const GATE_MESSAGES: usize = 1024;
const GATE_MESSAGE_BYTES: usize = 64 * 1024;
const REQUEST_METADATA_BYTES: usize = 64;

fn check_cancelled(cancelled: Option<&std::sync::atomic::AtomicBool>) -> Result<()> {
    if cancelled.is_some_and(|flag| flag.load(std::sync::atomic::Ordering::Acquire)) {
        return Err(Error::Transport("connection planning cancelled".into()));
    }
    Ok(())
}

// Official limits and conservative SDK budgets are distinguished in the
// protocol baseline. Count generated native topics, not normalized channels.
fn fits(feed: &ExchangeFeed) -> Result<bool> {
    match feed.exchange {
        ExchangeId::Binance => Ok(BinanceAdapter::connection_plans(feed)?.iter().all(|plan| {
            plan.websocket_url
                .split_once("?streams=")
                .is_some_and(|(_, streams)| streams.split('/').count() <= BINANCE_STREAMS)
        })),
        ExchangeId::Bitget => {
            let payload: Value = serde_json::from_str(&BitgetAdapter::subscription_message(feed))
                .map_err(|error| Error::Parse(error.to_string()))?;
            Ok(payload["args"]
                .as_array()
                .is_some_and(|args| args.len() <= BITGET_TOPICS))
        }
        ExchangeId::Bybit => Ok(BybitAdapter::subscription_urls(feed).iter().all(|url| {
            let planned = super::bybit_feed_for_url(feed, url);
            BybitAdapter::subscription_message(&planned).len() + REQUEST_METADATA_BYTES
                <= BYBIT_BYTES
        })),
        ExchangeId::Okx => Ok(OkxAdapter::subscription_urls(feed).iter().all(|url| {
            let planned = super::okx_feed_for_url(feed, url);
            OkxAdapter::subscription_message(&planned).len() <= OKX_BYTES
        })),
        ExchangeId::Gateio => Ok(GateioAdapter::connection_plans(feed)?.iter().all(|plan| {
            // SDK resource ceilings; these are not claimed as Gate limits.
            plan.subscription_messages.len() <= GATE_MESSAGES
                && plan
                    .subscription_messages
                    .iter()
                    .all(|message| message.len() + REQUEST_METADATA_BYTES <= GATE_MESSAGE_BYTES)
        })),
        _ => Err(Error::UnsupportedExchange(format!("{:?}", feed.exchange))),
    }
}

fn native_routes(feed: &ExchangeFeed) -> Result<Vec<String>> {
    let urls = match feed.exchange {
        ExchangeId::Binance => BinanceAdapter::connection_plans(feed)?
            .into_iter()
            .map(|plan| plan.websocket_url)
            .collect(),
        ExchangeId::Bybit => BybitAdapter::subscription_urls(feed),
        ExchangeId::Okx => OkxAdapter::subscription_urls(feed),
        ExchangeId::Gateio => GateioAdapter::connection_plans(feed)?
            .into_iter()
            .map(|plan| plan.websocket_url)
            .collect(),
        ExchangeId::Bitget => vec![BitgetAdapter::websocket_url().into()],
        _ => return Err(Error::UnsupportedExchange(format!("{:?}", feed.exchange))),
    };
    Ok(urls
        .into_iter()
        .map(|url| {
            if feed.exchange == ExchangeId::Binance {
                match url.find("?streams=") {
                    Some(index) => url[..index].to_owned(),
                    None => url,
                }
            } else {
                url
            }
        })
        .collect())
}

pub(crate) fn physical_connection_count(feed: &ExchangeFeed) -> Result<usize> {
    Ok(native_routes(feed)?.len())
}

// Only oversized multi-route feeds need this projection. Native adapters remain
// the routing authority; grouping never invents an endpoint or subscription pair.
fn route_feeds(
    feed: &ExchangeFeed,
    cancelled: Option<&std::sync::atomic::AtomicBool>,
) -> Result<Vec<ExchangeFeed>> {
    check_cancelled(cancelled)?;
    let routes = native_routes(feed)?;
    if routes.len() <= 1 {
        return Ok(vec![feed.clone()]);
    }
    let membership: Vec<_> = feed
        .channels
        .iter()
        .map(|channel| {
            if feed.channel_subscriptions.is_empty() {
                None
            } else {
                Some(
                    feed.channel_subscriptions
                        .iter()
                        .filter(|(entry, _)| entry == channel)
                        .flat_map(|(_, symbols)| symbols.iter())
                        .collect::<std::collections::HashSet<_>>(),
                )
            }
        })
        .collect();
    let mut template = feed.clone();
    template.symbols.clear();
    template.exchange_symbols.clear();
    template.channels.clear();
    template.channel_subscriptions.clear();
    let mut groups: Vec<_> = routes
        .iter()
        .map(|_| {
            let mut planned = template.clone();
            planned.channels = feed.channels.clone();
            planned.channel_subscriptions = feed
                .channels
                .iter()
                .map(|channel| (*channel, Vec::new()))
                .collect();
            (planned, vec![false; feed.symbols.len()])
        })
        .collect();
    let mut pair = template;
    for (symbol_index, symbol) in feed.symbols.iter().enumerate() {
        check_cancelled(cancelled)?;
        pair.symbols.clear();
        pair.symbols.push(symbol.clone());
        pair.exchange_symbols.clear();
        if !feed.exchange_symbols.is_empty() {
            pair.exchange_symbols
                .push(feed.exchange_symbols[symbol_index].clone());
        }
        for (channel_index, channel) in feed.channels.iter().enumerate() {
            if !membership[channel_index]
                .as_ref()
                .is_none_or(|selected| selected.contains(symbol))
            {
                continue;
            }
            pair.channels.clear();
            pair.channels.push(*channel);
            for route in native_routes(&pair)? {
                let index = routes
                    .iter()
                    .position(|known| known == &route)
                    .ok_or_else(|| {
                        Error::InvalidConfiguration(
                            "pair route is absent from logical connection plan".into(),
                        )
                    })?;
                let (planned, seen) = &mut groups[index];
                if !seen[symbol_index] {
                    seen[symbol_index] = true;
                    planned.symbols.push(symbol.clone());
                    if !feed.exchange_symbols.is_empty() {
                        planned
                            .exchange_symbols
                            .push(feed.exchange_symbols[symbol_index].clone());
                    }
                }
                planned.channel_subscriptions[channel_index]
                    .1
                    .push(symbol.clone());
            }
        }
    }
    Ok(groups
        .into_iter()
        .filter_map(|(mut planned, _)| {
            planned
                .channel_subscriptions
                .retain(|(_, symbols)| !symbols.is_empty());
            planned.channels.retain(|channel| {
                planned
                    .channel_subscriptions
                    .iter()
                    .any(|(entry, _)| entry == channel)
            });
            (!planned.symbols.is_empty()).then_some(planned)
        })
        .collect())
}

pub(crate) fn validate_connection_counts(feeds: &[ExchangeFeed]) -> Result<()> {
    let mut counts = std::collections::HashMap::new();
    let mut scoped = std::collections::HashMap::new();
    for feed in feeds {
        let connections = physical_connection_count(feed)?;
        if let Some(budget) = feed.transport.resource_budget() {
            let count = scoped.entry((budget.id(), feed.exchange)).or_insert(0);
            *count += connections;
            if *count > budget.policy().websocket_capacity() {
                return Err(Error::InvalidConfiguration(
                    "planned connections exceed shared resource WebSocket budget".into(),
                ));
            }
        }
        let count = counts.entry(feed.exchange).or_insert(0);
        *count += connections;
        if *count > super::budget::MAX_CONNECTIONS_PER_EXCHANGE {
            return Err(Error::InvalidConfiguration(format!(
                "{:?}: planned connections exceed SDK budget of {}",
                feed.exchange,
                super::budget::MAX_CONNECTIONS_PER_EXCHANGE
            )));
        }
    }
    Ok(())
}

fn slice(feed: &ExchangeFeed, start: usize, len: usize) -> ExchangeFeed {
    let mut planned = feed.clone();
    planned.symbols = feed.symbols[start..start + len].to_vec();
    if !feed.exchange_symbols.is_empty() {
        planned.exchange_symbols = feed.exchange_symbols[start..start + len].to_vec();
    }
    planned.prune_connection_scope();
    planned
}

#[cfg(test)]
pub(crate) fn shard(feed: &ExchangeFeed) -> Result<Vec<ExchangeFeed>> {
    shard_cancelable(feed, None)
}
pub(crate) fn shard_cancelable(
    feed: &ExchangeFeed,
    cancelled: Option<&std::sync::atomic::AtomicBool>,
) -> Result<Vec<ExchangeFeed>> {
    check_cancelled(cancelled)?;
    if !feed.exchange_symbols.is_empty() && feed.exchange_symbols.len() != feed.symbols.len() {
        return Err(Error::InvalidConfiguration(
            "normalized and exchange symbol counts must match".to_owned(),
        ));
    }
    if let Some(limit) = feed.runtime_options.packing_evaluation_limit() {
        return optimal::shard(feed, limit, cancelled);
    }
    if fits(feed)? {
        return Ok(vec![feed.clone()]);
    }
    let mut result = Vec::new();
    for route in route_feeds(feed, cancelled)? {
        result.extend(shard_route(&route, cancelled)?);
    }
    Ok(result)
}

fn shard_route(
    feed: &ExchangeFeed,
    cancelled: Option<&std::sync::atomic::AtomicBool>,
) -> Result<Vec<ExchangeFeed>> {
    check_cancelled(cancelled)?;
    if fits(feed)? {
        return Ok(vec![feed.clone()]);
    }
    let mut result = Vec::new();
    let mut start = 0;
    while start < feed.symbols.len() {
        check_cancelled(cancelled)?;
        if !fits(&slice(feed, start, 1))? {
            return Err(Error::InvalidConfiguration(format!(
                "{:?}: one instrument exceeds the subscription budget",
                feed.exchange
            )));
        }
        let (mut low, mut high) = (1, feed.symbols.len() - start);
        // Generated topic/message sizes are monotonic as symbols are appended.
        while low < high {
            check_cancelled(cancelled)?;
            let middle = low + (high - low).div_ceil(2);
            if fits(&slice(feed, start, middle))? {
                low = middle;
            } else {
                high = middle - 1;
            }
        }
        result.push(slice(feed, start, low));
        start += low;
    }
    Ok(result)
}

struct CancelPlanning(std::sync::Arc<std::sync::atomic::AtomicBool>);
impl Drop for CancelPlanning {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::Release);
    }
}
/// Exact search stays off async socket/command workers. At most two SDK async
/// planning jobs own blocking workers; cancelled jobs retain their permit until exit.
pub(crate) async fn prepare(feed: ExchangeFeed) -> Result<Vec<ExchangeFeed>> {
    if feed.runtime_options.packing_evaluation_limit().is_none() {
        return feed.connection_feeds();
    }
    on_planning_worker(move |cancelled| feed.connection_feeds_cancelable(Some(&cancelled))).await
}
async fn on_planning_worker<T, F>(work: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce(std::sync::Arc<std::sync::atomic::AtomicBool>) -> Result<T> + Send + 'static,
{
    static SLOTS: std::sync::OnceLock<std::sync::Arc<tokio::sync::Semaphore>> =
        std::sync::OnceLock::new();
    let permit = SLOTS
        .get_or_init(|| std::sync::Arc::new(tokio::sync::Semaphore::new(2)))
        .clone()
        .acquire_owned()
        .await
        .expect("planning admission stays open");
    let cancelled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let _guard = CancelPlanning(cancelled.clone());
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        work(cancelled)
    })
    .await
    .map_err(|_| Error::Transport("connection planning worker failed".into()))?
}

#[cfg(all(
    test,
    feature = "trade",
    feature = "funding",
    feature = "index",
    feature = "markprice"
))]
mod tests {
    use super::*;
    use crate::prelude::*;

    fn feed(exchange: ExchangeId, count: usize) -> ExchangeFeed {
        ExchangeFeedBuilder::new(exchange)
            .trade()
            .instruments((0..count).map(|i| Symbol::spot(&format!("S{i}"), "USDT")))
            .build()
    }

    #[test]
    fn binance_exact_stream_limit_and_overflow() {
        assert_eq!(shard(&feed(ExchangeId::Binance, 1024)).unwrap().len(), 1);
        let groups = shard(&feed(ExchangeId::Binance, 1025)).unwrap();
        assert_eq!(
            groups.iter().map(|g| g.symbols.len()).collect::<Vec<_>>(),
            [1024, 1]
        );
    }

    #[test]
    fn bitget_uses_the_recommended_channel_budget_after_deduplication() {
        let groups = shard(&feed(ExchangeId::Bitget, 50)).unwrap();
        assert_eq!(
            groups.iter().map(|g| g.symbols.len()).collect::<Vec<_>>(),
            [49, 1]
        );
        let shared = Bitget::new()
            .funding()
            .index()
            .mark_price()
            .instruments((0..49).map(|i| Symbol::perpetual(&format!("S{i}"), "USDT")))
            .build();
        assert_eq!(shard(&shared).unwrap().len(), 1);
    }

    #[test]
    fn bybit_character_budget_preserves_native_pairs() {
        let names: Vec<_> = (0..30)
            .map(|i| format!("S{i}{}", "X".repeat(1000)))
            .collect();
        let mut input = feed(ExchangeId::Bybit, names.len());
        input.exchange_symbols = names.clone();
        let groups = shard(&input).unwrap();
        assert!(groups.len() > 1);
        assert_eq!(
            groups
                .iter()
                .flat_map(|g| g.exchange_symbols.clone())
                .collect::<Vec<_>>(),
            names
        );
        assert!(groups.iter().all(|g| fits(g).unwrap()));
    }

    #[test]
    fn okx_and_gate_resource_boundaries_split_without_losing_symbols() {
        let mut okx = feed(ExchangeId::Okx, 100);
        okx.exchange_symbols = (0..100)
            .map(|i| format!("S{i}{}", "X".repeat(1000)))
            .collect();
        let groups = shard(&okx).unwrap();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups.iter().map(|g| g.symbols.len()).sum::<usize>(), 100);
        assert!(
            groups
                .iter()
                .all(|g| OkxAdapter::subscription_message(g).len() <= 64 * 1024)
        );
        let groups = shard(&feed(ExchangeId::Gateio, 1025)).unwrap();
        assert_eq!(
            groups.iter().map(|g| g.symbols.len()).collect::<Vec<_>>(),
            [1024, 1]
        );
    }

    #[test]
    fn shared_binance_price_topics_are_counted_once() {
        let input = crate::prelude::Binance::new()
            .funding()
            .index()
            .mark_price()
            .instruments((0..1024).map(|i| Symbol::perpetual(&format!("S{i}"), "USDT")))
            .build();
        assert_eq!(shard(&input).unwrap().len(), 1);
    }

    #[test]
    fn planned_connection_count_rejects_over_budget_before_connecting() {
        let feeds: Vec<_> = (0..100).map(|_| feed(ExchangeId::Bitget, 1)).collect();
        validate_connection_counts(&feeds).unwrap();
        let mut too_many = feeds;
        too_many.push(feed(ExchangeId::Bitget, 1));
        assert!(validate_connection_counts(&too_many).is_err());
    }

    #[test]
    fn oversized_single_symbol_is_rejected_without_connecting() {
        let mut input = feed(ExchangeId::Okx, 1);
        input.exchange_symbols = vec!["X".repeat(65536)];
        assert!(shard(&input).is_err());
    }
}

#[cfg(all(test, feature = "trade"))]
mod route_tests;

#[cfg(test)]
mod minimum_worker_tests {
    use super::*;
    #[tokio::test(flavor = "current_thread")]
    async fn minimum_packing_workers_are_bounded_and_dropped_futures_cancel_work() {
        use std::{
            sync::{
                Arc,
                atomic::{AtomicBool, Ordering},
            },
            time::{Duration, Instant},
        };
        let mut jobs = Vec::new();
        for _ in 0..2 {
            let (started, wait) = tokio::sync::oneshot::channel();
            let (done, finished) = tokio::sync::oneshot::channel();
            let job = tokio::spawn(on_planning_worker(move |cancelled| {
                let _ = started.send(());
                let deadline = Instant::now() + Duration::from_secs(2);
                while !cancelled.load(Ordering::Acquire) && Instant::now() < deadline {
                    std::thread::yield_now();
                }
                let _ = done.send(cancelled.load(Ordering::Acquire));
                Ok(())
            }));
            tokio::time::timeout(Duration::from_secs(1), wait)
                .await
                .unwrap()
                .unwrap();
            jobs.push((job, finished));
        }
        let third_started = Arc::new(AtomicBool::new(false));
        let flag = third_started.clone();
        let third = tokio::spawn(on_planning_worker(move |_| {
            flag.store(true, Ordering::Release);
            Ok(())
        }));
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(!third_started.load(Ordering::Acquire));
        third.abort();
        assert!(third.await.unwrap_err().is_cancelled());
        for (job, finished) in jobs {
            job.abort();
            assert!(job.await.unwrap_err().is_cancelled());
            assert!(
                tokio::time::timeout(Duration::from_secs(1), finished)
                    .await
                    .unwrap()
                    .unwrap()
            );
        }
    }
}
