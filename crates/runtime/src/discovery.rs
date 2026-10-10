//! Opt-in directory reconciliation for managed feeds.
use crate::{
    catalog::MarketCatalog,
    exchange::ExchangeFeed,
    feed::{FeedIdentity, FeedState, RuntimeControl},
};
use cryptofeed_core::{
    error::{Error, Result},
    exchange::Channel,
    symbol::{InstrumentKind, Symbol},
};
use std::{
    collections::HashSet,
    future::Future,
    time::{Duration, SystemTime},
};
use tokio::sync::watch;

const MIN_INTERVAL: Duration = Duration::from_secs(60);

/// A symbol-free feed template plus product-qualified directory patterns.
#[derive(Clone)]
pub struct DiscoveryFeed {
    template: ExchangeFeed,
    product: InstrumentKind,
    rules: Vec<(Channel, Vec<String>)>,
    interval: Duration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DiscoveryState {
    Current,
    Refreshing,
    Backoff,
    OwnershipLost { actual: Option<FeedIdentity> },
    Stopped,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct DiscoverySnapshot {
    pub identity: FeedIdentity,
    pub selected: Vec<(Channel, Vec<Symbol>)>,
    pub state: DiscoveryState,
    pub refreshes: u64,
    pub updates: u64,
    pub consecutive_failures: u64,
    pub next_delay: Duration,
    pub last_refresh_at: Option<SystemTime>,
    pub last_error: Option<String>,
}

/// Retain this owner while discovery should run. Stop leaves the managed feed
/// registered. Dropping stops future polls; an accepted update may finish.
pub struct DiscoveryHandle {
    state: watch::Receiver<DiscoverySnapshot>,
    stop: watch::Sender<bool>,
    task: Option<tokio::task::JoinHandle<DiscoverySnapshot>>,
}

impl DiscoveryHandle {
    pub fn snapshot(&self) -> DiscoverySnapshot {
        self.state.borrow().clone()
    }
    pub fn subscribe(&self) -> watch::Receiver<DiscoverySnapshot> {
        self.state.clone()
    }
    pub async fn stop(mut self) -> Result<DiscoverySnapshot> {
        let _ = self.stop.send(true);
        self.task
            .take()
            .expect("discovery task")
            .await
            .map_err(|error| Error::Transport(error.to_string()))
    }
}
impl Drop for DiscoveryHandle {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}

fn patterns<I, S>(input: I) -> Result<Vec<String>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let patterns: Vec<_> = input
        .into_iter()
        .map(|pattern| pattern.as_ref().to_ascii_uppercase())
        .collect();
    if patterns.is_empty() || patterns.iter().any(|pattern| pattern.trim().is_empty()) {
        return Err(Error::InvalidConfiguration(
            "discovery requires nonempty symbol patterns".to_owned(),
        ));
    }
    Ok(patterns)
}

struct Desired {
    feed: ExchangeFeed,
    selected: Vec<(Channel, Vec<Symbol>)>,
}

impl DiscoveryFeed {
    /// Template may contain channels, handlers and settings, but no symbols,
    /// native mappings, per-channel symbol sets or registered runtime identity.
    pub fn new<I, S>(template: ExchangeFeed, product: InstrumentKind, input: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        if !template.symbols.is_empty()
            || !template.exchange_symbols.is_empty()
            || !template.channel_subscriptions.is_empty()
            || template.identity().is_some()
        {
            return Err(Error::InvalidConfiguration(
                "discovery requires an unregistered symbol-free shared-channel template".to_owned(),
            ));
        }
        crate::markets::validate_product_channels(&template, product)?;
        let input = patterns(input)?;
        let mut seen = HashSet::new();
        let rules = template
            .channels
            .iter()
            .filter(|channel| seen.insert(**channel))
            .map(|channel| (*channel, input.clone()))
            .collect();
        Ok(Self {
            template,
            product,
            rules,
            interval: Duration::from_secs(300),
        })
    }
    /// Changes the cadence. The SDK floor is one minute, not an exchange quota.
    pub fn interval(mut self, interval: Duration) -> Result<Self> {
        if interval < MIN_INTERVAL {
            return Err(Error::InvalidConfiguration(
                "discovery interval must be at least 60 seconds".to_owned(),
            ));
        }
        self.interval = interval;
        Ok(self)
    }
    /// Overrides patterns for a channel already present in the template.
    pub fn channel_patterns<I, S>(mut self, channel: Channel, input: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let rule = self
            .rules
            .iter_mut()
            .find(|(candidate, _)| *candidate == channel)
            .ok_or_else(|| {
                Error::InvalidConfiguration(
                    "discovery channel is absent from its template".to_owned(),
                )
            })?;
        rule.1 = patterns(input)?;
        Ok(self)
    }
    fn resolve(&self, catalog: &MarketCatalog, initial: bool) -> Result<Desired> {
        let mut feed = self.template.clone();
        feed.channels.clear();
        let mut selected = Vec::new();
        let mut seen = HashSet::new();
        for (channel, patterns) in &self.rules {
            let refs: Vec<_> = patterns.iter().map(String::as_str).collect();
            let symbols = if initial {
                catalog.select(&refs)?
            } else {
                catalog.select_discovered(&refs)?
            };
            if symbols.iter().any(|symbol| symbol.kind() != self.product) {
                return Err(Error::MalformedData(
                    "discovery catalog has the wrong product".to_owned(),
                ));
            }
            if symbols.is_empty() {
                continue;
            }
            feed.channels.push(*channel);
            feed.channel_subscriptions.push((*channel, symbols.clone()));
            for symbol in &symbols {
                if seen.insert(symbol.clone()) {
                    feed.symbols.push(symbol.clone());
                    feed.exchange_symbols
                        .push(catalog.exchange_symbol(symbol)?.to_owned());
                }
            }
            selected.push((*channel, symbols));
        }
        if feed.symbols.is_empty() {
            return Err(Error::UnsupportedSymbol(
                "discovery matches no symbols; preserving last nonempty feed".to_owned(),
            ));
        }
        crate::markets::validate_feed(&feed)?;
        Ok(Desired { feed, selected })
    }
    /// Refreshes once and registers the initial feed before starting periodic
    /// reconciliation. Run the managed runtime concurrently with this call.
    pub async fn start(self, control: RuntimeControl) -> Result<DiscoveryHandle> {
        let exchange = self.template.exchange;
        let product = self.product;
        let transport = self.template.transport.clone();
        self.start_with(control, move || {
            let transport = transport.clone();
            async move { MarketCatalog::refresh_with_transport(exchange, product, &transport).await }
        }).await
    }
    async fn start_with<L, F>(self, control: RuntimeControl, mut load: L) -> Result<DiscoveryHandle>
    where
        L: FnMut() -> F + Send + 'static,
        F: Future<Output = Result<MarketCatalog>> + Send + 'static,
    {
        let catalog = load().await?;
        let desired = self.resolve(&catalog, true)?;
        let native = desired.feed.exchange_symbols.clone();
        let identity = control.add_feed(desired.feed).await?;
        let snapshot = DiscoverySnapshot {
            identity,
            selected: desired.selected,
            state: DiscoveryState::Current,
            refreshes: 1,
            updates: 0,
            consecutive_failures: 0,
            next_delay: self.interval,
            last_refresh_at: Some(SystemTime::now()),
            last_error: None,
        };
        let (sender, state) = watch::channel(snapshot.clone());
        let (stop, receiver) = watch::channel(false);
        let task = tokio::spawn(self.drive(control, load, snapshot, native, sender, receiver));
        Ok(DiscoveryHandle {
            state,
            stop,
            task: Some(task),
        })
    }
    async fn drive<L, F>(
        self,
        control: RuntimeControl,
        mut load: L,
        mut state: DiscoverySnapshot,
        mut native: Vec<String>,
        sender: watch::Sender<DiscoverySnapshot>,
        mut stop: watch::Receiver<bool>,
    ) -> DiscoverySnapshot
    where
        L: FnMut() -> F + Send + 'static,
        F: Future<Output = Result<MarketCatalog>> + Send + 'static,
    {
        loop {
            if cancel(&control, &mut stop, tokio::time::sleep(state.next_delay))
                .await
                .is_none()
            {
                break;
            }
            let Some(current) = cancel(&control, &mut stop, control.state(state.identity.id)).await
            else {
                break;
            };
            let current = match current {
                Ok(current) => current,
                Err(error) => {
                    state.next_delay = Duration::ZERO;
                    state.last_error = Some(error.to_string());
                    state.state = DiscoveryState::OwnershipLost { actual: None };
                    sender.send_replace(state.clone());
                    return state;
                }
            };
            if current.identity != state.identity {
                state.next_delay = Duration::ZERO;
                state.state = DiscoveryState::OwnershipLost {
                    actual: Some(current.identity),
                };
                sender.send_replace(state.clone());
                return state;
            }
            state.state = DiscoveryState::Refreshing;
            state.refreshes += 1;
            sender.send_replace(state.clone());
            let Some(catalog) = cancel(&control, &mut stop, load()).await else {
                break;
            };
            let result = catalog.and_then(|catalog| {
                state.last_refresh_at = Some(SystemTime::now());
                self.resolve(&catalog, false)
            });
            let result = match result {
                Ok(desired) => {
                    let changed = desired.selected != state.selected
                        || desired.feed.exchange_symbols != native;
                    let restart = matches!(
                        current.state,
                        FeedState::Failed { .. }
                            | FeedState::Degraded { .. }
                            | FeedState::Stopped { .. }
                    );
                    if changed || restart {
                        let next_native = desired.feed.exchange_symbols.clone();
                        // Do not abandon an accepted replacement on stop: wait for
                        // its result, then stop before another directory cycle.
                        match control
                            .replace_feed_if_current(state.identity, desired.feed)
                            .await
                        {
                            Ok(identity) => {
                                state.identity = identity;
                                state.selected = desired.selected;
                                native = next_native;
                                state.updates += 1;
                                Ok(())
                            }
                            Err(error) => Err(error),
                        }
                    } else {
                        Ok(())
                    }
                }
                Err(error) => Err(error),
            };
            match result {
                Ok(()) => {
                    state.state = DiscoveryState::Current;
                    state.consecutive_failures = 0;
                    state.last_error = None;
                    state.next_delay = self.interval;
                }
                Err(error) => {
                    // A concurrent manual replacement owns the ID now; never
                    // overwrite it on a later polling cycle.
                    match control.state(state.identity.id).await {
                        Ok(actual) if actual.identity != state.identity => {
                            state.next_delay = Duration::ZERO;
                            state.state = DiscoveryState::OwnershipLost {
                                actual: Some(actual.identity),
                            };
                            state.last_error = Some(error.to_string());
                            sender.send_replace(state.clone());
                            return state;
                        }
                        Err(_) => {
                            state.next_delay = Duration::ZERO;
                            state.state = DiscoveryState::OwnershipLost { actual: None };
                            state.last_error = Some(error.to_string());
                            sender.send_replace(state.clone());
                            return state;
                        }
                        Ok(_) => {}
                    }
                    state.state = DiscoveryState::Backoff;
                    state.consecutive_failures += 1;
                    state.last_error = Some(error.to_string());
                    let multiplier = 1u32 << (state.consecutive_failures - 1).min(16) as u32;
                    state.next_delay = self
                        .interval
                        .saturating_mul(multiplier)
                        .min(self.interval.max(Duration::from_secs(3600)));
                }
            }
            sender.send_replace(state.clone());
        }
        state.state = DiscoveryState::Stopped;
        state.next_delay = Duration::ZERO;
        sender.send_replace(state.clone());
        state
    }
}

async fn cancel<T>(
    control: &RuntimeControl,
    stop: &mut watch::Receiver<bool>,
    future: impl Future<Output = T>,
) -> Option<T> {
    if *stop.borrow() || control.sender.is_closed() {
        return None;
    }
    tokio::select! {
        biased;
        _ = stop.changed() => None,
        _ = control.sender.closed() => None,
        result = future => Some(result),
    }
}

#[cfg(all(test, feature = "trade"))]
mod tests {
    use super::*;
    use crate::prelude::*;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    fn catalog(rows: &[(&str, &str)]) -> MarketCatalog {
        let mut registry = crate::markets::SymbolRegistry::default();
        for (base, native) in rows {
            registry.insert(Symbol::spot(base, "USDT"), native).unwrap();
        }
        MarketCatalog::from_registry(ExchangeId::Binance, InstrumentKind::Spot, registry)
    }
    fn config() -> DiscoveryFeed {
        let mut config = DiscoveryFeed::new(
            Binance::new().trade().build(),
            InstrumentKind::Spot,
            ["*-USDT"],
        )
        .unwrap();
        config.interval = Duration::from_millis(5); // Deterministic engine clock, public floor is tested separately.
        config
    }
    async fn until(handle: &DiscoveryHandle, check: impl Fn(&DiscoverySnapshot) -> bool) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if check(&handle.snapshot()) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
    }

    #[test]
    fn configuration_rejects_ambiguous_templates_bad_patterns_and_too_fast_polls() {
        assert!(
            DiscoveryFeed::new(
                Binance::new().trade().symbol("BTC-USDT").build(),
                InstrumentKind::Spot,
                ["*"]
            )
            .is_err()
        );
        assert!(DiscoveryFeed::new(Binance::new().build(), InstrumentKind::Spot, ["*"]).is_err());
        assert!(
            DiscoveryFeed::new(
                Binance::new().trade().build(),
                InstrumentKind::Option,
                ["*"]
            )
            .is_err()
        );
        assert!(
            DiscoveryFeed::new(Binance::new().trade().build(), InstrumentKind::Spot, [""]).is_err()
        );
        assert!(config().interval(Duration::from_secs(59)).is_err());
        assert!(config().interval(Duration::from_secs(60)).is_ok());
    }

    #[test]
    fn channel_rules_drop_empty_channels_but_preserve_the_last_nonempty_feed() {
        let config = DiscoveryFeed::new(
            Binance::new().trade().ticker().build(),
            InstrumentKind::Spot,
            ["*"],
        )
        .unwrap()
        .channel_patterns(Channel::Trade, ["BTC-*"])
        .unwrap()
        .channel_patterns(Channel::Ticker, ["ETH-*"])
        .unwrap();
        let initial = config
            .resolve(&catalog(&[("BTC", "BTCUSDT"), ("ETH", "ETHUSDT")]), true)
            .unwrap();
        assert_eq!(initial.selected.len(), 2);
        let next = config
            .resolve(&catalog(&[("BTC", "BTCUSDT")]), false)
            .unwrap();
        assert_eq!(next.feed.channels, [Channel::Trade]);
        assert!(config.resolve(&catalog(&[]), false).is_err());
        assert!(
            config
                .resolve(&catalog(&[("BTC", "BTCUSDT")]), true)
                .is_err()
        );
    }

    #[tokio::test]
    async fn metadata_only_refresh_does_not_restart_subscriptions() {
        let (control, _events, _statuses, running, _stop) =
            crate::runtime::test_managed_runtime(FeedHandler::new());
        let mut first = true;
        let handle = config().start_with(control.clone(), move || {
            let tick = if first { first = false; "0.1" } else { "0.2" };
            let mut registry = crate::markets::SymbolRegistry::default();
            registry.insert_market(Symbol::spot("BTC", "USDT"), "BTCUSDT", ExchangeId::Binance,
                &serde_json::json!({"filters":[{"filterType":"PRICE_FILTER","tickSize":tick}]}), None).unwrap();
            let catalog = MarketCatalog::from_registry(ExchangeId::Binance, InstrumentKind::Spot, registry);
            async move { Ok(catalog) }
        }).await.unwrap();
        until(&handle, |state| {
            state.refreshes >= 3 && state.state == DiscoveryState::Current
        })
        .await;
        let stopped = handle.stop().await.unwrap();
        assert_eq!(stopped.updates, 0);
        assert_eq!(stopped.identity.generation, 1);
        control.remove_feed(stopped.identity.id).await.unwrap();
        control.shutdown().await.unwrap();
        running.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn additions_removals_and_native_changes_update_only_when_needed() {
        let (control, _events, _statuses, running, _stop) =
            crate::runtime::test_managed_runtime(FeedHandler::new());
        let mut snapshots = std::collections::VecDeque::from([
            catalog(&[("BTC", "BTCUSDT")]),
            catalog(&[("BTC", "BTCUSDT"), ("ETH", "ETHUSDT")]),
            catalog(&[("ETH", "ETHUSDT")]),
            catalog(&[("ETH", "ALTETHUSDT")]),
        ]);
        let handle = config()
            .start_with(control.clone(), move || {
                let next = snapshots
                    .pop_front()
                    .unwrap_or_else(|| catalog(&[("ETH", "ALTETHUSDT")]));
                async move { Ok(next) }
            })
            .await
            .unwrap();
        until(&handle, |state| state.updates == 3 && state.refreshes >= 6).await;
        let snapshot = handle.stop().await.unwrap();
        assert_eq!(snapshot.identity.generation, 4);
        assert_eq!(snapshot.updates, 3);
        assert_eq!(snapshot.selected[0].1, [Symbol::spot("ETH", "USDT")]);
        assert_eq!(
            control.state(snapshot.identity.id).await.unwrap().identity,
            snapshot.identity
        );
        control.remove_feed(snapshot.identity.id).await.unwrap();
        control.shutdown().await.unwrap();
        running.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn refresh_failure_backs_off_without_replacing_old_configuration() {
        let (control, mut events, _statuses, running, _stop) =
            crate::runtime::test_managed_runtime(FeedHandler::new());
        let recovered = Arc::new(AtomicBool::new(false));
        let flag = recovered.clone();
        let mut first = true;
        let handle = config()
            .start_with(control.clone(), move || {
                let value = if first {
                    first = false;
                    Ok(catalog(&[("BTC", "BTCUSDT")]))
                } else if flag.load(Ordering::SeqCst) {
                    Ok(catalog(&[("BTC", "BTCUSDT"), ("ETH", "ETHUSDT")]))
                } else {
                    Err(Error::Transport("scripted directory outage".into()))
                };
                async move { value }
            })
            .await
            .unwrap();
        let original = handle.snapshot().identity;
        until(&handle, |state| {
            state.state == DiscoveryState::Backoff && state.consecutive_failures >= 2
        })
        .await;
        let failed = handle.snapshot();
        assert_eq!(failed.identity, original);
        assert_eq!(failed.updates, 0);
        assert!(failed.next_delay >= Duration::from_millis(10));
        assert_eq!(control.state(original.id).await.unwrap().identity, original);
        while events.try_recv().is_ok() {}
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), events.recv())
                .await
                .unwrap()
                .unwrap()
                .identity,
            original
        );
        recovered.store(true, Ordering::SeqCst);
        until(&handle, |state| {
            state.updates == 1 && state.consecutive_failures == 0
        })
        .await;
        let stopped = handle.stop().await.unwrap();
        control.remove_feed(stopped.identity.id).await.unwrap();
        control.shutdown().await.unwrap();
        running.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn stop_cancels_blocked_directory_fetch_and_keeps_managed_feed() {
        let (control, _events, _statuses, running, _stop) =
            crate::runtime::test_managed_runtime(FeedHandler::new());
        let started = Arc::new(AtomicBool::new(false));
        let signal = started.clone();
        let mut first = true;
        let handle = config()
            .start_with(control.clone(), move || {
                let initial = first;
                first = false;
                let signal = signal.clone();
                async move {
                    if initial {
                        return Ok(catalog(&[("BTC", "BTCUSDT")]));
                    }
                    signal.store(true, Ordering::SeqCst);
                    std::future::pending::<Result<MarketCatalog>>().await
                }
            })
            .await
            .unwrap();
        until(&handle, |_| started.load(Ordering::SeqCst)).await;
        let stopped = tokio::time::timeout(Duration::from_millis(100), handle.stop())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stopped.state, DiscoveryState::Stopped);
        assert_eq!(control.feeds().await.unwrap().len(), 1);
        control.remove_feed(stopped.identity.id).await.unwrap();
        control.shutdown().await.unwrap();
        running.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn stop_waits_for_committed_replacement_and_reports_its_identity() {
        let (control, _events, mut statuses, running, _stop) =
            crate::runtime::test_managed_runtime(FeedHandler::new());
        let mut first = true;
        let handle = config()
            .start_with(control.clone(), move || {
                let next = if first {
                    first = false;
                    catalog(&[("SLOW", "SLOWUSDT")])
                } else {
                    catalog(&[("ETH", "ETHUSDT")])
                };
                async move { Ok(next) }
            })
            .await
            .unwrap();
        let old = handle.snapshot().identity;
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let crate::feed::FeedStatus::Lifecycle {
                    identity,
                    state: crate::feed::FeedState::Stopping,
                    ..
                } = statuses.recv().await.unwrap()
                {
                    if identity == old {
                        break;
                    }
                }
            }
        })
        .await
        .unwrap();
        let stopped = tokio::time::timeout(Duration::from_secs(1), handle.stop())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stopped.identity.generation, old.generation + 1);
        assert_eq!(
            control.state(old.id).await.unwrap().identity,
            stopped.identity
        );
        assert_eq!(stopped.updates, 1);
        control.remove_feed(old.id).await.unwrap();
        control.shutdown().await.unwrap();
        running.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn empty_directory_keeps_last_selection_and_runtime_shutdown_stops_polling() {
        let (control, _events, _statuses, running, _stop) =
            crate::runtime::test_managed_runtime(FeedHandler::new());
        let mut first = true;
        let handle = config()
            .start_with(control.clone(), move || {
                let next = if first {
                    first = false;
                    catalog(&[("BTC", "BTCUSDT")])
                } else {
                    catalog(&[])
                };
                async move { Ok(next) }
            })
            .await
            .unwrap();
        until(&handle, |state| state.state == DiscoveryState::Backoff).await;
        assert_eq!(handle.snapshot().identity.generation, 1);
        assert_eq!(
            handle.snapshot().selected[0].1,
            [Symbol::spot("BTC", "USDT")]
        );
        control.shutdown().await.unwrap();
        let stopped = tokio::time::timeout(Duration::from_secs(1), handle.stop())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stopped.state, DiscoveryState::Stopped);
        running.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn manual_replacement_during_refresh_is_not_overwritten() {
        let (control, _events, _statuses, running, _stop) =
            crate::runtime::test_managed_runtime(FeedHandler::new());
        let release = Arc::new(AtomicBool::new(false));
        let flag = release.clone();
        let entered = Arc::new(AtomicBool::new(false));
        let signal = entered.clone();
        let mut first = true;
        let handle = config()
            .start_with(control.clone(), move || {
                let initial = first;
                first = false;
                let flag = flag.clone();
                let signal = signal.clone();
                async move {
                    if initial {
                        return Ok(catalog(&[("BTC", "BTCUSDT")]));
                    }
                    signal.store(true, Ordering::SeqCst);
                    while !flag.load(Ordering::SeqCst) {
                        tokio::task::yield_now().await;
                    }
                    Ok(catalog(&[("BTC", "BTCUSDT"), ("SOL", "SOLUSDT")]))
                }
            })
            .await
            .unwrap();
        let owned = handle.snapshot().identity;
        until(&handle, |_| entered.load(Ordering::SeqCst)).await;
        let manual = control
            .replace_feed(
                owned.id,
                Binance::new()
                    .trade()
                    .symbol("ETH-USDT")
                    .exchange_symbol("ETHUSDT")
                    .build(),
            )
            .await
            .unwrap();
        release.store(true, Ordering::SeqCst);
        until(&handle, |state| matches!(state.state, DiscoveryState::OwnershipLost { actual: Some(identity) } if identity == manual)).await;
        assert_eq!(control.state(owned.id).await.unwrap().identity, manual);
        let stopped = handle.stop().await.unwrap();
        assert_eq!(stopped.updates, 0);
        control.remove_feed(manual.id).await.unwrap();
        control.shutdown().await.unwrap();
        running.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn dropping_owner_stops_future_polls() {
        let (control, _events, _statuses, running, _stop) =
            crate::runtime::test_managed_runtime(FeedHandler::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let handle = config()
            .start_with(control.clone(), move || {
                observed.fetch_add(1, Ordering::SeqCst);
                async { Ok(catalog(&[("BTC", "BTCUSDT")])) }
            })
            .await
            .unwrap();
        let id = handle.snapshot().identity.id;
        drop(handle);
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        control.remove_feed(id).await.unwrap();
        control.shutdown().await.unwrap();
        running.await.unwrap().unwrap();
    }
}
