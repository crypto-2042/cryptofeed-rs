use cryptofeed_core::error::{Error, Result};
use std::future::Future;
use tokio::sync::Semaphore;

// Shared across feeds and exchanges, including reconnect/resnapshot requests.
// This bounds concurrency, not exchange-specific request weight per minute.
const MAX_CONCURRENT_SNAPSHOTS: usize = 4;

struct SnapshotRequests {
    slots: Semaphore,
    next: tokio::sync::Mutex<tokio::time::Instant>,
    interval: std::time::Duration,
}

impl SnapshotRequests {
    fn new(limit: usize, interval: std::time::Duration) -> Self {
        Self {
            slots: Semaphore::new(limit),
            next: tokio::sync::Mutex::new(tokio::time::Instant::now()),
            interval,
        }
    }

    async fn run<F: Future>(&self, request: F) -> F::Output {
        let _permit = self
            .slots
            .acquire()
            .await
            .expect("snapshot semaphore stays open");
        {
            let mut next = self.next.lock().await;
            tokio::time::sleep_until(*next).await;
            *next = tokio::time::Instant::now() + self.interval;
        }
        request.await
    }
}

pub(super) async fn fetch_json(
    url: &str,
    transport: &crate::transport::TransportConfig,
) -> Result<serde_json::Value> {
    static REQUESTS: std::sync::OnceLock<SnapshotRequests> = std::sync::OnceLock::new();
    let requests = REQUESTS.get_or_init(|| {
        SnapshotRequests::new(MAX_CONCURRENT_SNAPSHOTS, std::time::Duration::from_secs(1))
    });
    requests
        .run(async {
            let response = transport
                .http_client()?
                .get(url)
                .timeout(super::SNAPSHOT_HTTP_TIMEOUT)
                .send()
                .await
                .map_err(|error| Error::Transport(error.to_string()))?;
            super::read_bounded_snapshot_json(response, url).await
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::SnapshotRequests;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[tokio::test]
    async fn request_concurrency_is_bounded_across_callers() {
        let requests = SnapshotRequests::new(2, std::time::Duration::ZERO);
        let active = AtomicUsize::new(0);
        let peak = AtomicUsize::new(0);
        let barrier = tokio::sync::Barrier::new(2);
        let futures = (0..12).map(|_| {
            requests.run(async {
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
        let requests = SnapshotRequests::new(4, std::time::Duration::from_millis(10));
        let start = tokio::time::Instant::now();
        futures::future::join_all((0..3).map(|_| requests.run(async {}))).await;
        assert!(start.elapsed() >= std::time::Duration::from_millis(20));
    }

    #[tokio::test]
    async fn cancelling_an_active_request_releases_capacity() {
        let requests = Arc::new(SnapshotRequests::new(1, std::time::Duration::ZERO));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let task = {
            let requests = requests.clone();
            tokio::spawn(async move {
                requests
                    .run(async {
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
                requests.run(async { 42 })
            )
            .await
            .unwrap(),
            42
        );
    }

    #[tokio::test]
    async fn cancelled_waiter_never_starts_network_work() {
        let requests = Arc::new(SnapshotRequests::new(1, std::time::Duration::ZERO));
        let permit = requests.slots.acquire().await.unwrap();
        let started = Arc::new(AtomicUsize::new(0));
        let task = {
            let requests = requests.clone();
            let started = started.clone();
            tokio::spawn(async move {
                requests
                    .run(async {
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
        requests.run(async {}).await;
    }
}
