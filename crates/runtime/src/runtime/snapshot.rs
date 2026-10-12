use crate::transport::retry_after;
use cryptofeed_core::{
    error::{Error, Result},
    exchange::ExchangeId,
};
use std::{
    collections::HashMap,
    future::Future,
    time::{Duration, SystemTime},
};
use tokio::{sync::Semaphore, time::Instant};

// All SDK directory/REST/bootstrap callers share admission, across transports.
struct Clock {
    next: Instant,
    cooldowns: HashMap<ExchangeId, Instant>,
    weights: super::weights::Tracker,
}
struct HttpRequests {
    slots: Semaphore,
    clock: tokio::sync::Mutex<Clock>,
    interval: Duration,
}
impl HttpRequests {
    fn new(limit: usize, interval: Duration) -> Self {
        Self {
            slots: Semaphore::new(limit),
            clock: tokio::sync::Mutex::new(Clock {
                next: Instant::now(),
                cooldowns: HashMap::new(),
                weights: Default::default(),
            }),
            interval,
        }
    }
    async fn defer(&self, exchange: ExchangeId, delay: Duration) -> Result<()> {
        let until = Instant::now().checked_add(delay).ok_or_else(|| {
            Error::Protocol("Retry-After exceeds the supported clock range".into())
        })?;
        let mut clock = self.clock.lock().await;
        clock
            .cooldowns
            .entry(exchange)
            .and_modify(|old| *old = (*old).max(until))
            .or_insert(until);
        Ok(())
    }
    #[cfg(test)]
    async fn run<F: Future>(&self, exchange: ExchangeId, request: F) -> F::Output {
        self.run_scoped(exchange, None, request).await
    }
    #[cfg(test)]
    async fn run_scoped<F: Future>(
        &self,
        exchange: ExchangeId,
        scope: Option<&crate::resources::Gate>,
        request: F,
    ) -> F::Output {
        self.run_admitted(exchange, scope, None, 6000, request)
            .await
            .expect("unweighted admission")
    }
    async fn run_admitted<F: Future>(
        &self,
        exchange: ExchangeId,
        scope: Option<&crate::resources::Gate>,
        charge: Option<super::weights::Charge>,
        ceiling: u32,
        request: F,
    ) -> Result<F::Output> {
        if let Some(charge) = charge {
            self.clock
                .lock()
                .await
                .weights
                .ready(charge, ceiling, Instant::now())?;
        }
        loop {
            // Cooldown waiters must not consume global network slots.
            let cooling = {
                let clock = self.clock.lock().await;
                clock.cooldowns.get(&exchange).copied()
            };
            if let Some(until) = cooling.filter(|until| *until > Instant::now()) {
                tokio::time::sleep_until(until).await;
                continue;
            }
            let scoped = match scope {
                Some(scope) => Some(
                    scope
                        .slots
                        .clone()
                        .acquire_owned()
                        .await
                        .expect("resource semaphore open"),
                ),
                None => None,
            };
            let permit = self
                .slots
                .acquire()
                .await
                .expect("HTTP semaphore stays open");
            let wait = {
                let mut clock = self.clock.lock().await;
                let mut local = match scope {
                    Some(scope) => Some(scope.next.lock().await),
                    None => None,
                };
                let now = Instant::now();
                clock.cooldowns.retain(|_, until| *until > now);
                let weight_deadline = match charge {
                    Some(charge) => clock.weights.ready(charge, ceiling, now)?,
                    None => now,
                };
                let deadline = clock
                    .next
                    .max(clock.cooldowns.get(&exchange).copied().unwrap_or(now))
                    .max(local.as_deref().copied().unwrap_or(now))
                    .max(weight_deadline);
                if deadline > now {
                    Some(deadline)
                } else {
                    if let Some(charge) = charge {
                        clock.weights.commit(charge, now);
                    }
                    clock.next = now + self.interval;
                    if let (Some(local), Some(scope)) = (local.as_mut(), scope) {
                        **local = now + scope.interval;
                    }
                    None
                }
            };
            if let Some(until) = wait {
                drop(permit);
                drop(scoped);
                tokio::time::sleep_until(until).await;
                continue;
            }
            let result = request.await;
            drop(permit);
            return Ok(result);
        }
    }
}
fn requests() -> &'static HttpRequests {
    static REQUESTS: std::sync::OnceLock<HttpRequests> = std::sync::OnceLock::new();
    REQUESTS.get_or_init(|| {
        HttpRequests::new(
            crate::resources::HTTP_LIMIT,
            crate::resources::HTTP_INTERVAL,
        )
    })
}
const DEFAULT_RATE_COOLDOWN: Duration = Duration::from_secs(60);
#[cfg(any(
    feature = "ticker",
    feature = "orderbook",
    feature = "funding",
    feature = "candles",
    feature = "trade"
))]
pub(crate) async fn fetch_json(
    url: &str,
    transport: &crate::transport::TransportConfig,
    exchange: ExchangeId,
) -> Result<serde_json::Value> {
    fetch_json_with_limits(
        url,
        transport,
        exchange,
        Duration::from_secs(15),
        8 * 1024 * 1024,
    )
    .await
}
pub(crate) async fn fetch_json_with_limits(
    url: &str,
    transport: &crate::transport::TransportConfig,
    exchange: ExchangeId,
    timeout: Duration,
    max_bytes: usize,
) -> Result<serde_json::Value> {
    #[cfg(feature = "recording")]
    let observation = transport.begin_http(url, exchange);
    #[cfg(feature = "recording")]
    if let Some(result) = transport.replay_http(url, exchange, max_bytes) {
        if let Some(observation) = observation {
            observation.finish(&result);
        }
        return result;
    }
    let result = async {
        let charge = super::weights::charge(exchange, url)?;
        requests()
            .run_admitted(
                exchange,
                transport.resource_budget().map(|budget| budget.http()),
                charge,
                transport
                    .resource_budget()
                    .map_or(6000, |budget| budget.policy().weight_ceiling()),
                async {
                    let response = transport
                        .http_client()?
                        .get(url)
                        .header(reqwest::header::USER_AGENT, "cryptofeed-rs/0.1")
                        .timeout(timeout)
                        .send()
                        .await
                        .map_err(|error| Error::Transport(error.to_string()))?;
                    read_response(response, url, exchange, max_bytes, requests()).await
                },
            )
            .await?
    }
    .await;
    #[cfg(feature = "recording")]
    if let Some(observation) = observation {
        observation.finish(&result);
    }
    result
}

async fn read_response(
    mut response: reqwest::Response,
    url: &str,
    exchange: ExchangeId,
    max_bytes: usize,
    limiter: &HttpRequests,
) -> Result<serde_json::Value> {
    let status = response.status();
    if !status.is_success() {
        let retry = retry_after(
            response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok()),
            SystemTime::now(),
        );
        if matches!(status.as_u16(), 418 | 429) || retry.is_some() {
            limiter
                .defer(exchange, retry.unwrap_or(DEFAULT_RATE_COOLDOWN))
                .await?;
        }
        return Err(Error::HttpStatus {
            status: status.as_u16(),
            retry_after: retry,
        });
    }
    if response
        .content_length()
        .is_some_and(|length| length > max_bytes as u64)
    {
        return Err(Error::MalformedData(format!(
            "{url}: response exceeds {max_bytes} bytes"
        )));
    }
    let retry = retry_after(
        response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok()),
        SystemTime::now(),
    );
    let reset = if exchange == ExchangeId::Bybit {
        response
            .headers()
            .get("x-bapi-limit-reset-timestamp")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse::<u64>().ok())
    } else {
        None
    };
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| Error::Transport(error.to_string()))?
    {
        append_body(&mut bytes, &chunk, max_bytes, url)?;
    }
    let value = serde_json::from_slice(&bytes)
        .map_err(|error| Error::MalformedData(format!("{url}: {error}")))?;
    limiter
        .clock
        .lock()
        .await
        .weights
        .observe_catalog(exchange, url, &value);
    apply_native_cooldown(limiter, exchange, &value, retry, reset, SystemTime::now()).await?;
    Ok(value)
}
async fn apply_native_cooldown(
    limiter: &HttpRequests,
    exchange: ExchangeId,
    value: &serde_json::Value,
    retry: Option<Duration>,
    reset: Option<u64>,
    now: SystemTime,
) -> Result<()> {
    if super::quota::native_code(exchange, value).is_some() {
        let reset = if exchange == ExchangeId::Bybit {
            reset.and_then(|timestamp| super::quota::reset_delay(timestamp, now))
        } else {
            None
        };
        let usable = |delay: &Duration| Instant::now().checked_add(*delay).is_some();
        limiter
            .defer(
                exchange,
                retry
                    .filter(usable)
                    .or(reset.filter(usable))
                    .unwrap_or(DEFAULT_RATE_COOLDOWN),
            )
            .await?;
    }
    Ok(())
}
fn append_body(bytes: &mut Vec<u8>, chunk: &[u8], limit: usize, url: &str) -> Result<()> {
    if bytes.len().saturating_add(chunk.len()) > limit {
        return Err(Error::MalformedData(format!(
            "{url}: response exceeds {limit} bytes"
        )));
    }
    bytes.extend_from_slice(chunk);
    Ok(())
}

#[cfg(feature = "orderbook")]
mod receiver {
    /// A replay placeholder never spawns an HTTP task; live capture retains only consumed bodies.
    #[derive(Clone, Copy)]
    #[cfg_attr(not(feature = "recording"), allow(dead_code))]
    pub(crate) enum SnapshotMode {
        Live(bool),
        Replay,
    }
    #[derive(Clone)]
    #[cfg_attr(not(feature = "recording"), allow(dead_code))]
    pub(crate) struct RawSnapshot {
        pub payload: serde_json::Value,
        pub received_ts: f64,
    }
    pub(crate) type RawSlot = std::sync::Arc<std::sync::Mutex<Option<RawSnapshot>>>;
    pub(crate) struct SnapshotReceiver<T> {
        inner: tokio::sync::oneshot::Receiver<cryptofeed_core::error::Result<T>>,
        #[cfg_attr(not(feature = "recording"), allow(dead_code))]
        raw: Option<RawSlot>,
        _pending: Option<tokio::sync::oneshot::Sender<cryptofeed_core::error::Result<T>>>,
    }
    impl<T> From<tokio::sync::oneshot::Receiver<cryptofeed_core::error::Result<T>>>
        for SnapshotReceiver<T>
    {
        fn from(inner: tokio::sync::oneshot::Receiver<cryptofeed_core::error::Result<T>>) -> Self {
            Self {
                inner,
                raw: None,
                _pending: None,
            }
        }
    }
    impl<T> SnapshotReceiver<T> {
        pub(crate) fn channel(
            mode: SnapshotMode,
        ) -> (
            Option<tokio::sync::oneshot::Sender<cryptofeed_core::error::Result<T>>>,
            Self,
            Option<RawSlot>,
        ) {
            let (tx, inner) = tokio::sync::oneshot::channel();
            let raw = match mode {
                SnapshotMode::Live(true) => Some(Default::default()),
                _ => None,
            };
            let mut receiver = Self {
                inner,
                raw: raw.clone(),
                _pending: None,
            };
            if matches!(mode, SnapshotMode::Replay) {
                receiver._pending = Some(tx);
                (None, receiver, raw)
            } else {
                (Some(tx), receiver, raw)
            }
        }
        pub(crate) fn try_recv(
            &mut self,
        ) -> std::result::Result<
            cryptofeed_core::error::Result<T>,
            tokio::sync::oneshot::error::TryRecvError,
        > {
            self.inner.try_recv()
        }
        #[cfg(feature = "recording")]
        pub(crate) fn take_raw(&mut self) -> Option<RawSnapshot> {
            self.raw
                .as_ref()
                .and_then(|slot| slot.lock().expect("snapshot capture").take())
        }
        #[cfg(feature = "recording")]
        pub(crate) fn ready(value: cryptofeed_core::error::Result<T>) -> Self {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let _ = tx.send(value);
            rx.into()
        }
    }
}
#[cfg(feature = "orderbook")]
pub(crate) use receiver::{RawSlot, RawSnapshot, SnapshotMode, SnapshotReceiver};

#[cfg(test)]
mod tests {
    use super::HttpRequests;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[tokio::test]
    async fn request_concurrency_is_bounded_across_callers() {
        let requests = HttpRequests::new(2, std::time::Duration::ZERO);
        let active = AtomicUsize::new(0);
        let peak = AtomicUsize::new(0);
        let barrier = tokio::sync::Barrier::new(2);
        let futures = (0..12).map(|_| {
            requests.run(cryptofeed_core::exchange::ExchangeId::Binance, async {
                let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(current, Ordering::SeqCst);
                barrier.wait().await;
                active.fetch_sub(1, Ordering::SeqCst);
            })
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            futures::future::join_all(futures),
        )
        .await
        .unwrap();
        assert_eq!(peak.load(Ordering::SeqCst), 2);
        assert_eq!(active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn request_starts_are_paced_independently_of_response_time() {
        let requests = HttpRequests::new(4, std::time::Duration::from_millis(10));
        let start = tokio::time::Instant::now();
        futures::future::join_all(
            (0..3).map(|_| requests.run(cryptofeed_core::exchange::ExchangeId::Binance, async {})),
        )
        .await;
        assert!(start.elapsed() >= std::time::Duration::from_millis(20));
    }

    #[tokio::test]
    async fn cancelling_an_active_request_releases_capacity() {
        let requests = Arc::new(HttpRequests::new(1, std::time::Duration::ZERO));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let task = {
            let requests = requests.clone();
            tokio::spawn(async move {
                requests
                    .run(cryptofeed_core::exchange::ExchangeId::Binance, async {
                        started_tx.send(()).unwrap();
                        std::future::pending::<()>().await;
                    })
                    .await;
            })
        };
        started_rx.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                requests.run(cryptofeed_core::exchange::ExchangeId::Binance, async { 42 })
            )
            .await
            .unwrap(),
            42
        );
    }

    #[tokio::test]
    async fn cancelled_waiter_never_starts_network_work() {
        let requests = Arc::new(HttpRequests::new(1, std::time::Duration::ZERO));
        let permit = requests.slots.acquire().await.unwrap();
        let started = Arc::new(AtomicUsize::new(0));
        let task = {
            let requests = requests.clone();
            let started = started.clone();
            tokio::spawn(async move {
                requests
                    .run(cryptofeed_core::exchange::ExchangeId::Binance, async {
                        started.fetch_add(1, Ordering::SeqCst);
                    })
                    .await
            })
        };
        tokio::task::yield_now().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        drop(permit);
        assert_eq!(started.load(Ordering::SeqCst), 0);
        requests
            .run(cryptofeed_core::exchange::ExchangeId::Binance, async {})
            .await;
    }
    #[tokio::test(start_paused = true)]
    async fn cooldown_rechecks_queued_requests_and_does_not_block_other_venues() {
        use cryptofeed_core::exchange::ExchangeId;
        let requests = Arc::new(HttpRequests::new(1, std::time::Duration::from_secs(1)));
        requests.run(ExchangeId::Binance, async {}).await;
        let started = tokio::time::Instant::now();
        let queued = {
            let requests = requests.clone();
            tokio::spawn(async move {
                requests
                    .run(ExchangeId::Binance, async { tokio::time::Instant::now() })
                    .await
            })
        };
        tokio::task::yield_now().await;
        requests
            .defer(ExchangeId::Binance, std::time::Duration::from_secs(10))
            .await
            .unwrap();
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert!(!queued.is_finished());
        assert_eq!(requests.run(ExchangeId::Bybit, async { 42 }).await, 42);
        tokio::time::advance(std::time::Duration::from_secs(9)).await;
        assert!(
            queued.await.unwrap().duration_since(started) >= std::time::Duration::from_secs(10)
        );
    }
    #[tokio::test(start_paused = true)]
    async fn cancelling_cooldown_waiter_does_not_start_work_or_occupy_slots() {
        use cryptofeed_core::exchange::ExchangeId;
        let requests = Arc::new(HttpRequests::new(1, std::time::Duration::ZERO));
        requests
            .defer(ExchangeId::Binance, std::time::Duration::from_secs(60))
            .await
            .unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let queued = {
            let requests = requests.clone();
            let count = count.clone();
            tokio::spawn(async move {
                requests
                    .run(ExchangeId::Binance, async {
                        count.fetch_add(1, Ordering::SeqCst);
                    })
                    .await
            })
        };
        tokio::task::yield_now().await;
        assert_eq!(requests.slots.available_permits(), 1);
        queued.abort();
        queued.await.unwrap_err();
        assert_eq!(count.load(Ordering::SeqCst), 0);
        assert_eq!(requests.run(ExchangeId::Gateio, async { 42 }).await, 42);
    }
    #[test]
    fn retry_after_handles_seconds_http_dates_and_missing_values() {
        use std::time::{Duration, SystemTime};
        assert_eq!(
            super::retry_after(Some("12"), SystemTime::UNIX_EPOCH),
            Some(Duration::from_secs(12))
        );
        assert_eq!(
            super::retry_after(
                Some("Thu, 01 Jan 1970 00:00:10 GMT"),
                SystemTime::UNIX_EPOCH
            ),
            Some(Duration::from_secs(10))
        );
        assert_eq!(
            super::retry_after(
                Some("Thu, 01 Jan 1970 00:00:10 GMT"),
                SystemTime::UNIX_EPOCH + Duration::from_secs(20)
            ),
            Some(Duration::ZERO)
        );
        assert_eq!(
            super::retry_after(Some("invalid"), SystemTime::UNIX_EPOCH),
            None
        );
    }
    #[tokio::test(start_paused = true)]
    async fn http_status_and_response_bounds_are_checked_without_echoing_bodies() {
        use cryptofeed_core::{error::Error, exchange::ExchangeId};
        let requests = HttpRequests::new(1, std::time::Duration::ZERO);
        let response: reqwest::Response = http::Response::builder()
            .status(429)
            .header("Retry-After", "2")
            .body("secret response body")
            .unwrap()
            .into();
        let error = super::read_response(
            response,
            "https://public.invalid",
            ExchangeId::Binance,
            100,
            &requests,
        )
        .await
        .unwrap_err();
        assert!(
            matches!(error, Error::HttpStatus { status:429, retry_after:Some(delay) } if delay == std::time::Duration::from_secs(2))
        );
        assert!(!error.to_string().contains("secret"));
        let response: reqwest::Response = http::Response::builder()
            .header("Content-Length", "100")
            .body("x".repeat(100))
            .unwrap()
            .into();
        assert!(
            super::read_response(
                response,
                "https://public.invalid",
                ExchangeId::Bybit,
                10,
                &requests
            )
            .await
            .is_err()
        );
        let mut bytes = Vec::new();
        super::append_body(&mut bytes, b"12345", 8, "test").unwrap();
        assert!(super::append_body(&mut bytes, b"6789", 8, "test").is_err());
        assert_eq!(bytes, b"12345");
        let response: reqwest::Response = http::Response::new("not JSON").into();
        assert!(
            super::read_response(
                response,
                "https://public.invalid",
                ExchangeId::Bybit,
                100,
                &requests
            )
            .await
            .is_err()
        );
    }
}

#[cfg(test)]
mod scoped_budget_tests {
    use super::*;
    #[tokio::test(start_paused = true)]
    async fn composite_http_pacing_does_not_bunch_after_global_cooldown() {
        let global = HttpRequests::new(4, Duration::from_secs(1));
        let local = crate::resources::Gate::new(4, Duration::from_secs(3));
        let start = Instant::now();
        global
            .defer(ExchangeId::Binance, Duration::from_secs(10))
            .await
            .unwrap();
        let times = futures::future::join_all((0..2).map(|_| {
            global.run_scoped(ExchangeId::Binance, Some(&local), async { Instant::now() })
        }))
        .await;
        assert_eq!(times[0] - start, Duration::from_secs(10));
        assert!(times[1] - times[0] >= Duration::from_secs(3));
    }
    #[tokio::test(start_paused = true)]
    async fn waiting_for_scoped_pacing_holds_no_network_slots() {
        let global = HttpRequests::new(4, Duration::from_secs(1));
        let local = crate::resources::Gate::new(1, Duration::from_secs(3));
        global
            .run_scoped(ExchangeId::Okx, Some(&local), async {})
            .await;
        {
            let waiting = global.run_scoped(ExchangeId::Okx, Some(&local), async {});
            tokio::pin!(waiting);
            assert!(futures::poll!(waiting.as_mut()).is_pending());
            assert_eq!(global.slots.available_permits(), 4);
            assert_eq!(local.slots.available_permits(), 1);
        }
        assert_eq!(global.slots.available_permits(), 4);
        assert_eq!(local.slots.available_permits(), 1);
    }
    #[tokio::test(start_paused = true)]
    async fn cancellation_during_active_scoped_request_releases_both_budgets() {
        let global = HttpRequests::new(4, Duration::from_secs(1));
        let local = crate::resources::Gate::new(1, Duration::from_secs(3));
        {
            let active =
                global.run_scoped(ExchangeId::Okx, Some(&local), std::future::pending::<()>());
            tokio::pin!(active);
            assert!(futures::poll!(active.as_mut()).is_pending());
            assert_eq!(global.slots.available_permits(), 3);
            assert_eq!(local.slots.available_permits(), 0);
        }
        assert_eq!(global.slots.available_permits(), 4);
        assert_eq!(local.slots.available_permits(), 1);
    }
}

#[cfg(test)]
mod public_frequency_audit {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn default_http_spacing_covers_mixed_public_venues_and_independent_scopes() {
        let policy = crate::resources::ResourcePolicy::default();
        let limiter = HttpRequests::new(policy.http_capacity(), policy.http_start_interval());
        let first = crate::resources::Gate::new(4, Duration::from_secs(1));
        let second = crate::resources::Gate::new(4, Duration::from_secs(1));
        let start = Instant::now();
        let venues = [
            ExchangeId::Bitget,
            ExchangeId::Bybit,
            ExchangeId::Okx,
            ExchangeId::Gateio,
        ];
        let calls = (0..40).map(|index| {
            let limiter = &limiter;
            let scope = if index % 2 == 0 { &first } else { &second };
            let exchange = venues[index % venues.len()];
            async move {
                limiter
                    .run_scoped(exchange, Some(scope), async { Instant::now() })
                    .await
            }
        });
        let mut starts = futures::future::join_all(calls).await;
        starts.sort_unstable();
        assert_eq!(starts[0], start);
        assert_eq!(starts[39] - start, Duration::from_secs(39));
        assert!(
            starts
                .windows(2)
                .all(|pair| pair[1] - pair[0] >= Duration::from_secs(1))
        );
        assert_eq!(limiter.slots.available_permits(), policy.http_capacity());
        assert_eq!(first.slots.available_permits(), 4);
        assert_eq!(second.slots.available_permits(), 4);
    }
}

#[cfg(test)]
mod native_quota_tests {
    use super::*;
    use serde_json::json;
    #[tokio::test(start_paused = true)]
    async fn successful_http_with_native_quota_preserves_body_and_defers_only_its_exchange() {
        let limiter = HttpRequests::new(4, Duration::ZERO);
        let local = crate::resources::Gate::new(2, Duration::ZERO);
        let body = json!({"retCode":10006,"retMsg":"synthetic message"});
        let response: reqwest::Response = http::Response::builder()
            .status(200)
            .header("Retry-After", "2")
            .body(body.to_string())
            .unwrap()
            .into();
        assert_eq!(
            read_response(response, "synthetic", ExchangeId::Bybit, 4096, &limiter)
                .await
                .unwrap(),
            body
        );
        let start = Instant::now();
        let queued = limiter.run_scoped(ExchangeId::Bybit, Some(&local), async { Instant::now() });
        tokio::pin!(queued);
        assert!(futures::poll!(queued.as_mut()).is_pending());
        assert_eq!(limiter.slots.available_permits(), 4);
        assert_eq!(local.slots.available_permits(), 2);
        assert_eq!(
            limiter.run(ExchangeId::Okx, async { Instant::now() }).await,
            start
        );
        assert_eq!(queued.await - start, Duration::from_secs(2));
    }
    #[tokio::test(start_paused = true)]
    async fn native_header_priority_reset_fallback_and_existing_deadline_never_shorten() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let body = json!({"retCode":10006});
        for (retry, reset, expected) in [
            (Some(Duration::from_secs(7)), Some(12500), 7_000),
            (None, Some(12500), 2500),
            (None, Some(9999), 60_000),
            (None, None, 60_000),
            (Some(Duration::ZERO), None, 0),
            (Some(Duration::MAX), Some(12500), 2500),
            (Some(Duration::MAX), None, 60_000),
        ] {
            let limiter = HttpRequests::new(1, Duration::ZERO);
            let start = Instant::now();
            apply_native_cooldown(&limiter, ExchangeId::Bybit, &body, retry, reset, now)
                .await
                .unwrap();
            assert_eq!(
                limiter.clock.lock().await.cooldowns[&ExchangeId::Bybit] - start,
                Duration::from_millis(expected)
            );
        }
        let limiter = HttpRequests::new(1, Duration::ZERO);
        let start = Instant::now();
        apply_native_cooldown(
            &limiter,
            ExchangeId::Bybit,
            &body,
            Some(Duration::from_secs(10)),
            None,
            now,
        )
        .await
        .unwrap();
        apply_native_cooldown(
            &limiter,
            ExchangeId::Bybit,
            &body,
            Some(Duration::from_secs(1)),
            None,
            now,
        )
        .await
        .unwrap();
        assert_eq!(
            limiter.clock.lock().await.cooldowns[&ExchangeId::Bybit] - start,
            Duration::from_secs(10)
        );
    }
    #[tokio::test(start_paused = true)]
    async fn normal_payload_and_other_error_scopes_do_not_apply_rate_headers() {
        for (exchange, body) in [
            (
                ExchangeId::Bybit,
                json!({"retCode":0,"retMsg":"10006 too many requests"}),
            ),
            (ExchangeId::Okx, json!({"code":"50061"})),
            (ExchangeId::Bitget, json!({"code":"25004"})),
            (ExchangeId::Gateio, json!({"label":"TOO_MANY_REQUESTS"})),
        ] {
            let limiter = HttpRequests::new(1, Duration::ZERO);
            let response: reqwest::Response = http::Response::builder()
                .header("Retry-After", "99")
                .header("X-Bapi-Limit-Reset-Timestamp", "9999999999999")
                .body(body.to_string())
                .unwrap()
                .into();
            assert_eq!(
                read_response(response, "synthetic", exchange, 4096, &limiter)
                    .await
                    .unwrap(),
                body
            );
            assert!(limiter.clock.lock().await.cooldowns.is_empty());
        }
    }
    #[tokio::test(start_paused = true)]
    async fn malformed_reset_header_falls_back_without_parsing_server_diagnostics() {
        let limiter = HttpRequests::new(1, Duration::ZERO);
        let start = Instant::now();
        let response: reqwest::Response = http::Response::builder()
            .header("X-Bapi-Limit-Reset-Timestamp", "not-a-time")
            .body(r#"{"retCode":10006,"retMsg":"IP ban until 9999999999999"}"#)
            .unwrap()
            .into();
        read_response(response, "synthetic", ExchangeId::Bybit, 4096, &limiter)
            .await
            .unwrap();
        assert_eq!(
            limiter.clock.lock().await.cooldowns[&ExchangeId::Bybit] - start,
            Duration::from_secs(60)
        );
    }
    #[cfg(feature = "recording")]
    #[tokio::test(start_paused = true)]
    async fn archived_native_rate_payload_does_not_cool_down_live_http() {
        use crate::recording::{RecordingEnd, RecordingLimits, http::*};
        let url = "https://api.bybit.com/v5/market/instruments-info?category=spot&status=Trading";
        let mut bytes = Vec::new();
        let mut writer = HttpRecordingWriter::new(&mut bytes, RecordingLimits::default())
            .await
            .unwrap();
        for (index, kind) in [
            HttpObservationKind::Request {
                request_id: 1,
                exchange: ExchangeId::Bybit,
                url: url.into(),
            },
            HttpObservationKind::Finished {
                request_id: 1,
                outcome: HttpOutcome::Json {
                    value: json!({"retCode":10006}),
                    redacted: false,
                },
            },
        ]
        .into_iter()
        .enumerate()
        {
            writer
                .append(HttpObservation {
                    sequence: index as u64 + 1,
                    elapsed_ns: index as u64,
                    observed_ts: 1.0,
                    kind,
                })
                .await
                .unwrap();
        }
        writer.finish(RecordingEnd::Complete).await.unwrap();
        let replay = HttpRecordingReader::new(bytes.as_slice(), RecordingLimits::default())
            .into_replay()
            .await
            .unwrap();
        let transport = crate::TransportConfig::direct().with_http_replay(replay.clone());
        let before = requests()
            .clock
            .lock()
            .await
            .cooldowns
            .get(&ExchangeId::Bybit)
            .copied();
        assert_eq!(
            fetch_json_with_limits(
                url,
                &transport,
                ExchangeId::Bybit,
                Duration::from_secs(1),
                4096
            )
            .await
            .unwrap()["retCode"],
            10006
        );
        assert_eq!(
            requests()
                .clock
                .lock()
                .await
                .cooldowns
                .get(&ExchangeId::Bybit)
                .copied(),
            before
        );
        assert!(replay.verify_complete().is_ok());
    }
}

#[cfg(test)]
mod native_reset_header_test {
    use super::*;
    #[tokio::test(start_paused = true)]
    async fn valid_bybit_reset_header_is_used_only_after_native_rate_error() {
        let limiter = HttpRequests::new(1, Duration::ZERO);
        let start = Instant::now();
        let millis = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            + 120_000;
        let response: reqwest::Response = http::Response::builder()
            .header("X-Bapi-Limit-Reset-Timestamp", millis.to_string())
            .body(r#"{"retCode":10006}"#)
            .unwrap()
            .into();
        read_response(response, "synthetic", ExchangeId::Bybit, 4096, &limiter)
            .await
            .unwrap();
        let delay = limiter.clock.lock().await.cooldowns[&ExchangeId::Bybit] - start;
        assert!(delay > Duration::from_secs(60));
        assert!(delay <= Duration::from_secs(120));
    }
}

#[cfg(test)]
mod weighted_admission_tests {
    use super::super::weights::{Bucket, Charge};
    use super::*;
    #[tokio::test(start_paused = true)]
    async fn weight_wait_releases_both_slots_and_commits_only_when_work_starts() {
        let limiter = HttpRequests::new(4, Duration::ZERO);
        let local = crate::resources::Gate::new(2, Duration::ZERO);
        let charge = Charge {
            bucket: Bucket::Spot,
            cost: 250,
        };
        limiter
            .run_admitted(
                ExchangeId::Binance,
                Some(&local),
                Some(charge),
                250,
                async {},
            )
            .await
            .unwrap();
        let start = Instant::now();
        let waiting = limiter.run_admitted(
            ExchangeId::Binance,
            Some(&local),
            Some(charge),
            250,
            async { Instant::now() },
        );
        tokio::pin!(waiting);
        assert!(futures::poll!(waiting.as_mut()).is_pending());
        assert_eq!(limiter.slots.available_permits(), 4);
        assert_eq!(local.slots.available_permits(), 2);
        assert_eq!(waiting.await.unwrap() - start, Duration::from_secs(60));
    }
    #[tokio::test(start_paused = true)]
    async fn cancellation_before_admission_does_not_charge_but_started_work_is_not_refunded() {
        let limiter = HttpRequests::new(1, Duration::ZERO);
        let charge = Charge {
            bucket: Bucket::Spot,
            cost: 250,
        };
        {
            let active = limiter.run_admitted(
                ExchangeId::Binance,
                None,
                Some(charge),
                250,
                std::future::pending::<()>(),
            );
            tokio::pin!(active);
            assert!(futures::poll!(active.as_mut()).is_pending());
        }
        let start = Instant::now();
        {
            let waiting =
                limiter.run_admitted(ExchangeId::Binance, None, Some(charge), 250, async {});
            tokio::pin!(waiting);
            assert!(futures::poll!(waiting.as_mut()).is_pending());
        }
        assert_eq!(limiter.slots.available_permits(), 1);
        assert_eq!(
            limiter
                .run_admitted(ExchangeId::Binance, None, Some(charge), 250, async {
                    Instant::now()
                })
                .await
                .unwrap()
                - start,
            Duration::from_secs(60)
        );
    }
    #[tokio::test(start_paused = true)]
    async fn weight_and_start_clocks_are_joint_and_impossible_cost_fails_before_cooldown() {
        let limiter = HttpRequests::new(4, Duration::from_secs(1));
        let local = crate::resources::Gate::new(2, Duration::from_secs(3));
        let charge = Charge {
            bucket: Bucket::Spot,
            cost: 250,
        };
        limiter
            .defer(ExchangeId::Binance, Duration::from_secs(10))
            .await
            .unwrap();
        let start = Instant::now();
        assert!(
            limiter
                .run_admitted(ExchangeId::Binance, Some(&local), Some(charge), 1, async {})
                .await
                .is_err()
        );
        assert_eq!(Instant::now(), start);
        let times = futures::future::join_all((0..2).map(|_| {
            limiter.run_admitted(
                ExchangeId::Binance,
                Some(&local),
                Some(charge),
                250,
                async { Instant::now() },
            )
        }))
        .await;
        assert_eq!(
            times[0].as_ref().unwrap().duration_since(start),
            Duration::from_secs(10)
        );
        assert_eq!(
            times[1].as_ref().unwrap().duration_since(start),
            Duration::from_secs(70)
        );
    }
    #[tokio::test(start_paused = true)]
    async fn weighted_window_does_not_hold_other_exchange_operations() {
        let limiter = HttpRequests::new(4, Duration::ZERO);
        let charge = Charge {
            bucket: Bucket::Spot,
            cost: 250,
        };
        limiter
            .run_admitted(ExchangeId::Binance, None, Some(charge), 250, async {})
            .await
            .unwrap();
        let waiting = limiter.run_admitted(ExchangeId::Binance, None, Some(charge), 250, async {});
        tokio::pin!(waiting);
        assert!(futures::poll!(waiting.as_mut()).is_pending());
        let start = Instant::now();
        assert_eq!(
            limiter.run(ExchangeId::Okx, async { Instant::now() }).await,
            start
        );
    }
}

#[cfg(all(test, feature = "recording"))]
mod weighted_replay_tests {
    use super::*;
    #[tokio::test(start_paused = true)]
    async fn replay_ignores_impossible_live_weight_ceiling_and_does_not_learn_live_limits() {
        use crate::{
            ResourceBudget, ResourcePolicy, TransportConfig,
            recording::{RecordingEnd, RecordingLimits, http::*},
        };
        let url = "https://api.binance.com/api/v3/exchangeInfo";
        let value = serde_json::json!({"symbols":[],"rateLimits":[{"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":1}]});
        let mut bytes = Vec::new();
        let mut writer = HttpRecordingWriter::new(&mut bytes, RecordingLimits::default())
            .await
            .unwrap();
        for (index, kind) in [
            HttpObservationKind::Request {
                request_id: 1,
                exchange: ExchangeId::Binance,
                url: url.into(),
            },
            HttpObservationKind::Finished {
                request_id: 1,
                outcome: HttpOutcome::Json {
                    value: value.clone(),
                    redacted: false,
                },
            },
        ]
        .into_iter()
        .enumerate()
        {
            writer
                .append(HttpObservation {
                    sequence: index as u64 + 1,
                    elapsed_ns: index as u64,
                    observed_ts: 1.0,
                    kind,
                })
                .await
                .unwrap();
        }
        writer.finish(RecordingEnd::Complete).await.unwrap();
        let replay = HttpRecordingReader::new(bytes.as_slice(), RecordingLimits::default())
            .into_replay()
            .await
            .unwrap();
        let budget =
            ResourceBudget::new(ResourcePolicy::default().binance_weight_ceiling(1).unwrap());
        let transport = TransportConfig::direct()
            .with_resource_budget(budget.clone())
            .with_http_replay(replay.clone());
        let clock = *budget.http().next.lock().await;
        let start = Instant::now();
        assert_eq!(
            fetch_json_with_limits(
                url,
                &transport,
                ExchangeId::Binance,
                Duration::from_secs(1),
                4096
            )
            .await
            .unwrap(),
            value
        );
        assert_eq!(Instant::now(), start);
        assert_eq!(*budget.http().next.lock().await, clock);
        assert!(replay.verify_complete().is_ok());
    }
}
