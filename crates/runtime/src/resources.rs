//! Caller-shared stricter admission layered on process-wide SDK safeguards.
use cryptofeed_core::{
    error::{Error, Result},
    exchange::ExchangeId,
};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{sync::Semaphore, time::Instant};
pub(crate) const HTTP_LIMIT: usize = 4;
pub(crate) const HTTP_INTERVAL: Duration = Duration::from_secs(1);
pub(crate) const WS_LIMIT: usize = 100;
pub(crate) const WS_INTERVAL: Duration = Duration::from_millis(1100);
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResourcePolicy {
    http_limit: usize,
    http_interval: Duration,
    ws_limit: usize,
    ws_interval: Duration,
    binance_weight_ceiling: u32,
}
impl Default for ResourcePolicy {
    fn default() -> Self {
        Self {
            http_limit: HTTP_LIMIT,
            http_interval: HTTP_INTERVAL,
            ws_limit: WS_LIMIT,
            ws_interval: WS_INTERVAL,
            binance_weight_ceiling: 6000,
        }
    }
}
fn validate(limit: usize, maximum: usize, interval: Duration, minimum: Duration) -> Result<()> {
    if limit == 0 || limit > maximum || interval < minimum || interval > Duration::from_secs(86400)
    {
        return Err(Error::InvalidConfiguration(
            "resource policy must tighten SDK capacity/interval bounds; interval at most one day"
                .into(),
        ));
    }
    Ok(())
}
impl ResourcePolicy {
    pub fn http(mut self, maximum_active: usize, start_interval: Duration) -> Result<Self> {
        validate(maximum_active, HTTP_LIMIT, start_interval, HTTP_INTERVAL)?;
        self.http_limit = maximum_active;
        self.http_interval = start_interval;
        Ok(self)
    }
    pub fn websocket(
        mut self,
        maximum_per_exchange: usize,
        start_interval: Duration,
    ) -> Result<Self> {
        validate(maximum_per_exchange, WS_LIMIT, start_interval, WS_INTERVAL)?;
        self.ws_limit = maximum_per_exchange;
        self.ws_interval = start_interval;
        Ok(self)
    }
    /// Lower ceiling against shared Binance process usage, per SDK bucket window.
    pub fn binance_weight_ceiling(mut self, ceiling: u32) -> Result<Self> {
        if !(1..=6000).contains(&ceiling) {
            return Err(Error::InvalidConfiguration(
                "Binance weight ceiling must be 1..=6000".into(),
            ));
        }
        self.binance_weight_ceiling = ceiling;
        Ok(self)
    }
    pub fn weight_ceiling(self) -> u32 {
        self.binance_weight_ceiling
    }
    pub fn http_capacity(self) -> usize {
        self.http_limit
    }
    pub fn http_start_interval(self) -> Duration {
        self.http_interval
    }
    pub fn websocket_capacity(self) -> usize {
        self.ws_limit
    }
    pub fn websocket_start_interval(self) -> Duration {
        self.ws_interval
    }
}
pub(crate) struct Gate {
    pub slots: Arc<Semaphore>,
    pub next: tokio::sync::Mutex<Instant>,
    pub interval: Duration,
}
impl Gate {
    pub(crate) fn new(limit: usize, interval: Duration) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(limit)),
            next: tokio::sync::Mutex::new(Instant::now()),
            interval,
        }
    }
}
struct Shared {
    id: u64,
    policy: ResourcePolicy,
    http: Gate,
    websockets: Mutex<HashMap<ExchangeId, Arc<Gate>>>,
}
/// Clones share counters/clocks; separately constructed budgets are independent
/// but still constrained by SDK global safeguards. No background worker or I/O.
#[derive(Clone)]
pub struct ResourceBudget(Arc<Shared>);
impl std::fmt::Debug for ResourceBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResourceBudget")
            .field("policy", &self.0.policy)
            .finish_non_exhaustive()
    }
}
impl ResourceBudget {
    pub fn new(policy: ResourcePolicy) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(Arc::new(Shared {
            id: NEXT.fetch_add(1, Ordering::Relaxed),
            policy,
            http: Gate::new(policy.http_limit, policy.http_interval),
            websockets: Mutex::new(HashMap::new()),
        }))
    }
    pub fn policy(&self) -> ResourcePolicy {
        self.0.policy
    }
    pub(crate) fn id(&self) -> u64 {
        self.0.id
    }
    pub(crate) fn http(&self) -> &Gate {
        &self.0.http
    }
    pub(crate) fn websocket(&self, exchange: ExchangeId) -> Arc<Gate> {
        self.0
            .websockets
            .lock()
            .expect("resource budget lock")
            .entry(exchange)
            .or_insert_with(|| {
                Arc::new(Gate::new(self.0.policy.ws_limit, self.0.policy.ws_interval))
            })
            .clone()
    }
}
#[cfg(test)]
mod tests;
