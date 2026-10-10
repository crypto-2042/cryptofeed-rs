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
    async fn run<F: Future>(&self, exchange: ExchangeId, request: F) -> F::Output {
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
            let permit = self
                .slots
                .acquire()
                .await
                .expect("HTTP semaphore stays open");
            let wait = {
                let mut clock = self.clock.lock().await;
                let now = Instant::now();
                clock.cooldowns.retain(|_, until| *until > now);
                let deadline = clock
                    .next
                    .max(clock.cooldowns.get(&exchange).copied().unwrap_or(now));
                if deadline > now {
                    Some(deadline)
                } else {
                    clock.next = now + self.interval;
                    None
                }
            };
            if let Some(until) = wait {
                drop(permit);
                tokio::time::sleep_until(until).await;
                continue;
            }
            let result = request.await;
            drop(permit);
            return result;
        }
    }
}
fn requests() -> &'static HttpRequests {
    static REQUESTS: std::sync::OnceLock<HttpRequests> = std::sync::OnceLock::new();
    REQUESTS.get_or_init(|| HttpRequests::new(4, Duration::from_secs(1)))
}
fn retry_after(header: Option<&str>, now: SystemTime) -> Option<Duration> {
    let value = header?.trim();
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    httpdate::parse_http_date(value)
        .ok()
        .map(|time| time.duration_since(now).unwrap_or_default())
}
#[cfg(any(feature = "ticker", feature = "orderbook"))]
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
    requests()
        .run(exchange, async {
            let response = transport
                .http_client()?
                .get(url)
                .header(reqwest::header::USER_AGENT, "cryptofeed-rs/0.1")
                .timeout(timeout)
                .send()
                .await
                .map_err(|error| Error::Transport(error.to_string()))?;
            read_response(response, url, exchange, max_bytes, requests()).await
        })
        .await
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
                .defer(exchange, retry.unwrap_or(Duration::from_secs(60)))
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
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| Error::Transport(error.to_string()))?
    {
        append_body(&mut bytes, &chunk, max_bytes, url)?;
    }
    serde_json::from_slice(&bytes).map_err(|error| Error::MalformedData(format!("{url}: {error}")))
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
