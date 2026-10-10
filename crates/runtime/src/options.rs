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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeOptions {
    max_retries: Option<usize>,
    start_delay: Duration,
    idle_policy: IdlePolicy,
    connect_timeout: Duration,
    handler_timeout: Duration,
}
impl Default for RuntimeOptions {
    fn default() -> Self {
        Self {
            max_retries: None,
            start_delay: Duration::ZERO,
            idle_policy: IdlePolicy::ExchangeDefault,
            connect_timeout: Duration::from_secs(20),
            handler_timeout: Duration::from_secs(5),
        }
    }
}
impl RuntimeOptions {
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
    fn options_survive_per_channel_partitioning() {
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
        let feed = Binance::new()
            .runtime_options(options)
            .subscription(Channel::Trade, ["BTC-USDT", "ETH-USDT"])
            .subscription(Channel::Ticker, ["BTC-USDT"])
            .exchange_symbol("BTCUSDT")
            .exchange_symbol("ETHUSDT")
            .build();
        let groups = feed.connection_feeds().unwrap();
        assert_eq!(groups.len(), 2);
        assert!(groups.iter().all(|group| group.runtime_options == options));
    }
}
