//! Caller-owned budgets that do not change exchange protocol policies.
use cryptofeed_core::error::{Error, Result};
use std::time::Duration;

/// Transport receipt watchdog; application heartbeats remain active.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum IdlePolicy {
    #[default]
    ExchangeDefault,
    After(Duration),
    Disabled,
}

/// Ordered delivery policy for user handler callbacks of one logical feed.
///
/// `Inline` (the default) awaits each callback on the session read path, which
/// preserves today's lossless, in-session ordering but lets a slow handler
/// block parsing, heartbeats and other channels. `Async` opts into one bounded
/// background dispatch task per logical feed; ingestion enqueues without
/// awaiting user code. See the repository's `docs/backpressure.md` for the
/// delivery guarantees.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum HandlerDispatch {
    /// Await each callback inline in the session read loop (default).
    #[default]
    Inline,
    /// Enqueue callbacks on one bounded task per logical feed.
    ///
    /// `capacity` bounds each event channel's ring. State channels keep only
    /// the latest value per symbol; event channels (trade, liquidation and,
    /// without a configured book store, raw L2) drop the oldest pending event
    /// and count the loss. Capacity is validated by
    /// [`RuntimeOptions::handler_dispatch`].
    Async {
        /// Maximum pending events per event-channel ring; must be in
        /// `1..=HandlerDispatch::MAX_CAPACITY`.
        capacity: usize,
    },
}

impl HandlerDispatch {
    /// Largest accepted event-ring capacity (one million events).
    pub const MAX_CAPACITY: usize = 1_000_000;

    /// Ring capacity for `Async`, or `None` for `Inline`.
    pub fn capacity(self) -> Option<usize> {
        match self {
            Self::Inline => None,
            Self::Async { capacity } => Some(capacity),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeOptions {
    max_retries: Option<usize>,
    start_delay: Duration,
    idle_policy: IdlePolicy,
    connect_timeout: Duration,
    handler_timeout: Duration,
    handler_dispatch: HandlerDispatch,
    packing_evaluations: Option<u64>,
}
impl Default for RuntimeOptions {
    fn default() -> Self {
        Self {
            max_retries: None,
            start_delay: Duration::ZERO,
            idle_policy: IdlePolicy::ExchangeDefault,
            connect_timeout: Duration::from_secs(20),
            handler_timeout: Duration::from_secs(5),
            handler_dispatch: HandlerDispatch::Inline,
            packing_evaluations: None,
        }
    }
}
impl RuntimeOptions {
    /// Opt into proven minimum physical connections under existing route/budget
    /// and atomic-instrument constraints. None preserves contiguous planning.
    /// Exhaustion returns an error rather than an unproven layout.
    pub fn minimum_connections(mut self, maximum_evaluations: Option<u64>) -> Result<Self> {
        if maximum_evaluations == Some(0) {
            return Err(Error::InvalidConfiguration(
                "minimum packing needs a positive evaluation budget".into(),
            ));
        }
        self.packing_evaluations = maximum_evaluations;
        Ok(self)
    }
    pub fn packing_evaluation_limit(self) -> Option<u64> {
        self.packing_evaluations
    }
    /// Maximum transient retries since the last successful subscription send.
    /// None is unbounded; Some(0) permits only the initial session attempt.
    pub fn max_retries(mut self, limit: Option<usize>) -> Self {
        self.max_retries = limit;
        self
    }
    /// Positive timeout for DNS/TCP/TLS/WebSocket establishment, excluding
    /// waiting for shared connection admission/pacing.
    pub fn connect_timeout(mut self, timeout: Duration) -> Result<Self> {
        positive(timeout)?;
        self.connect_timeout = timeout;
        Ok(self)
    }
    /// Positive timeout for each individual callback invocation.
    pub fn handler_timeout(mut self, timeout: Duration) -> Result<Self> {
        positive(timeout)?;
        self.handler_timeout = timeout;
        Ok(self)
    }
    /// Opts into bounded asynchronous handler dispatch for this logical feed.
    /// Defaults to [`HandlerDispatch::Inline`]. Async capacity must be in
    /// `1..=HandlerDispatch::MAX_CAPACITY`; invalid capacities are rejected
    /// before a feed is built.
    pub fn handler_dispatch(mut self, dispatch: HandlerDispatch) -> Result<Self> {
        if let HandlerDispatch::Async { capacity } = dispatch {
            if capacity == 0 || capacity > HandlerDispatch::MAX_CAPACITY {
                return Err(Error::InvalidConfiguration(format!(
                    "handler dispatch capacity must be between 1 and {}",
                    HandlerDispatch::MAX_CAPACITY
                )));
            }
        }
        self.handler_dispatch = dispatch;
        Ok(self)
    }
    /// Current handler dispatch policy; `Inline` preserves the historical
    /// in-session callback await.
    pub fn handler_dispatch_policy(self) -> HandlerDispatch {
        self.handler_dispatch
    }

    /// Initial per-physical-connection delay, before admission. Retry backoff
    /// does not repeat it; a replacement creates a new supervisor and delay.
    pub fn start_delay(mut self, delay: Duration) -> Self {
        self.start_delay = delay;
        self
    }
    /// Overrides the receipt watchdog without changing heartbeat payload/cadence.
    /// Short deadlines can reconnect healthy quiet markets; disabling is explicit.
    pub fn idle_policy(mut self, policy: IdlePolicy) -> Result<Self> {
        if let IdlePolicy::After(timeout) = policy {
            positive(timeout)?;
        }
        self.idle_policy = policy;
        Ok(self)
    }
    pub fn startup_delay(self) -> Duration {
        self.start_delay
    }
    pub fn receipt_policy(self) -> IdlePolicy {
        self.idle_policy
    }
    pub fn retry_limit(self) -> Option<usize> {
        self.max_retries
    }
    pub fn connection_deadline(self) -> Duration {
        self.connect_timeout
    }
    pub fn callback_deadline(self) -> Duration {
        self.handler_timeout
    }
}
fn positive(timeout: Duration) -> Result<()> {
    if timeout.is_zero() {
        Err(Error::InvalidConfiguration(
            "runtime timeout must be positive".into(),
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn defaults_preserve_existing_behavior_and_zero_deadlines_are_rejected() {
        let options = RuntimeOptions::default();
        assert_eq!(options.retry_limit(), None);
        assert_eq!(options.startup_delay(), Duration::ZERO);
        assert_eq!(options.receipt_policy(), IdlePolicy::ExchangeDefault);
        assert!(
            options
                .idle_policy(IdlePolicy::After(Duration::ZERO))
                .is_err()
        );
        assert_eq!(options.connection_deadline(), Duration::from_secs(20));
        assert_eq!(options.callback_deadline(), Duration::from_secs(5));
        assert!(options.connect_timeout(Duration::ZERO).is_err());
        assert!(options.handler_timeout(Duration::ZERO).is_err());
        assert_eq!(options.max_retries(Some(0)).retry_limit(), Some(0));
        assert_eq!(options.handler_dispatch_policy(), HandlerDispatch::Inline);
        let async_options = RuntimeOptions::default()
            .handler_dispatch(HandlerDispatch::Async { capacity: 64 })
            .unwrap();
        assert_eq!(
            async_options.handler_dispatch_policy(),
            HandlerDispatch::Async { capacity: 64 }
        );
        assert!(
            RuntimeOptions::default()
                .handler_dispatch(HandlerDispatch::Async { capacity: 0 })
                .is_err()
        );
        assert!(
            RuntimeOptions::default()
                .handler_dispatch(HandlerDispatch::Async {
                    capacity: HandlerDispatch::MAX_CAPACITY + 1
                })
                .is_err()
        );
        assert_eq!(
            RuntimeOptions::default()
                .handler_dispatch(HandlerDispatch::Async {
                    capacity: HandlerDispatch::MAX_CAPACITY
                })
                .unwrap()
                .handler_dispatch_policy()
                .capacity(),
            Some(HandlerDispatch::MAX_CAPACITY)
        );
    }
    #[tokio::test]
    async fn finite_retry_budget_counts_initial_attempt_separately() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        for limit in [0, 2] {
            let options = RuntimeOptions::default().max_retries(Some(limit));
            let attempts = AtomicUsize::new(0);
            let (_stop, shutdown) = tokio::sync::watch::channel(false);
            let result: Result<Option<()>> =
                crate::runtime::supervisor::retry_with_backoff_until_shutdown(
                    options.retry_limit(),
                    crate::runtime::supervisor::Backoff::new(0, 0),
                    shutdown,
                    || async {
                        attempts.fetch_add(1, Ordering::SeqCst);
                        Err(Error::Transport("scripted outage".into()))
                    },
                )
                .await;
            assert!(result.is_err());
            assert_eq!(attempts.load(Ordering::SeqCst), limit + 1);
        }
    }
    #[cfg(all(feature = "trade", feature = "ticker"))]
    #[test]
    fn options_survive_sparse_connection_planning() {
        use crate::prelude::*;
        let options = RuntimeOptions::default()
            .max_retries(Some(2))
            .start_delay(Duration::from_millis(5))
            .idle_policy(IdlePolicy::Disabled)
            .unwrap()
            .connect_timeout(Duration::from_secs(3))
            .unwrap()
            .handler_timeout(Duration::from_millis(50))
            .unwrap();
        let transport = TransportConfig::http_proxy("http://localhost:8080").unwrap();
        let feed = Binance::new()
            .transport(transport.clone())
            .runtime_options(options)
            .subscription(Channel::Trade, ["BTC-USDT", "ETH-USDT"])
            .subscription(Channel::Ticker, ["BTC-USDT"])
            .exchange_symbol("BTCUSDT")
            .exchange_symbol("ETHUSDT")
            .build();
        let groups = feed.connection_feeds().unwrap();
        assert_eq!(groups.len(), 1);
        assert!(groups.iter().all(|group| group.runtime_options == options));
        assert!(groups.iter().all(|group| group.transport.cache_key("catalog") == transport.cache_key("catalog")));
    }
}
