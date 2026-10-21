pub struct Backoff {
    initial: u64,
    current: u64,
    max: u64,
}

impl Backoff {
    pub fn new(initial: u64, max: u64) -> Self {
        Self {
            initial,
            current: initial,
            max,
        }
    }

    fn reset(&mut self) {
        self.current = self.initial;
    }

    pub fn next_delay_secs(&mut self) -> u64 {
        let value = self.current;
        self.current = (self.current * 2).min(self.max);
        value
    }
}

/// Errors that retrying cannot fix: rejected subscriptions, unsupported
/// capability/symbol/exchange configurations, and invalid configurations are
/// permanent and fail fast instead of burning the retry budget.
pub fn is_permanent(error: &cryptofeed_core::error::Error) -> bool {
    matches!(
        error,
        cryptofeed_core::error::Error::HttpStatus {
            status: 400 | 401 | 403 | 404 | 405 | 410 | 422,
            ..
        } | cryptofeed_core::error::Error::UnsupportedExchange(_)
            | cryptofeed_core::error::Error::UnsupportedChannel(_)
            | cryptofeed_core::error::Error::UnsupportedSymbol(_)
            | cryptofeed_core::error::Error::AmbiguousSymbol(_)
            | cryptofeed_core::error::Error::UnsupportedCapability(_)
            | cryptofeed_core::error::Error::InvalidConfiguration(_)
            | cryptofeed_core::error::Error::Subscription(_)
            | cryptofeed_core::error::Error::WebSocketClosed {
                retryable: false,
                ..
            }
    )
}

pub async fn retry_with_backoff<T, F, Fut>(
    max_retries: usize,
    mut backoff: Backoff,
    operation: F,
) -> cryptofeed_core::error::Result<T>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = cryptofeed_core::error::Result<T>>,
{
    let mut attempts = 0usize;

    loop {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                if is_permanent(&err) || attempts >= max_retries {
                    return Err(err);
                }
                let delay = retry_delay(
                    &err,
                    std::time::Duration::from_secs(backoff.next_delay_secs()),
                );
                attempts += 1;
                tokio::time::sleep(delay).await;
            }
        }
    }
}

#[derive(Default)]
pub(crate) struct RetryProgress(std::sync::atomic::AtomicBool);
impl RetryProgress {
    pub fn subscribed(&self) {
        self.0.store(true, std::sync::atomic::Ordering::Release);
    }
    pub fn take(&self) -> bool {
        self.0.swap(false, std::sync::atomic::Ordering::AcqRel)
    }
}

pub async fn retry_with_backoff_until_shutdown<T, F, Fut>(
    max_retries: Option<usize>,
    backoff: Backoff,
    shutdown: tokio::sync::watch::Receiver<bool>,
    operation: F,
) -> cryptofeed_core::error::Result<Option<T>>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = cryptofeed_core::error::Result<T>>,
{
    retry_with_progress(
        max_retries,
        backoff,
        shutdown,
        None,
        std::time::Duration::ZERO,
        operation,
    )
    .await
}

pub(crate) async fn retry_with_progress<T, F, Fut>(
    max_retries: Option<usize>,
    mut backoff: Backoff,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    progress: Option<std::sync::Arc<RetryProgress>>,
    start_delay: std::time::Duration,
    operation: F,
) -> cryptofeed_core::error::Result<Option<T>>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = cryptofeed_core::error::Result<T>>,
{
    if !start_delay.is_zero() {
        let delay = tokio::time::sleep(start_delay);
        tokio::pin!(delay);
        loop {
            if *shutdown.borrow() {
                return Ok(None);
            }
            tokio::select! {
                biased;
                changed = shutdown.changed() => match changed {
                    Ok(()) if !*shutdown.borrow() => continue,
                    _ => return Ok(None),
                },
                _ = &mut delay => break,
            }
        }
    }
    let mut attempts = 0usize;

    loop {
        if *shutdown.borrow() {
            return Ok(None);
        }
        let result = tokio::select! {
            biased;
            changed = shutdown.changed() => {
                match changed {
                    Ok(()) if !*shutdown.borrow() => continue,
                    Ok(()) | Err(_) => return Ok(None),
                }
            }
            result = operation() => result,
        };

        match result {
            Ok(value) => return Ok(Some(value)),
            Err(error) => {
                // A shutdown requested while the final attempt was failing is
                // a clean stop, not a terminal feed failure; permanent errors
                // (rejected subscriptions, unsupported configurations) fail
                // fast instead of burning the retry budget.
                if *shutdown.borrow() || is_permanent(&error) {
                    return if *shutdown.borrow() {
                        Ok(None)
                    } else {
                        Err(error)
                    };
                }
                if progress.as_ref().is_some_and(|progress| progress.take()) {
                    attempts = 0;
                    backoff.reset();
                }
                if max_retries.is_some_and(|limit| attempts >= limit) {
                    let diagnostic = crate::feed::FailureDiagnostic::from(&error);
                    tracing::error!(
                        outcome = "retry_exhausted",
                        attempt = attempts,
                        category = diagnostic.category,
                        reason = diagnostic.reason,
                        "configured transient retry budget exhausted"
                    );
                    return Err(error);
                }
                let delay = retry_delay(&error, jittered_delay(backoff.next_delay_secs()));
                attempts = attempts.saturating_add(1);
                tracing::warn!(attempt = attempts, retry_delay = ?delay, error = %error,
                    "transient feed failure; retrying");
                let waiting = tokio::time::sleep(delay);
                tokio::pin!(waiting);
                loop {
                    tokio::select! {
                        biased;
                        changed = shutdown.changed() => {
                            match changed {
                                Ok(()) if !*shutdown.borrow() => continue,
                                Ok(()) | Err(_) => return Ok(None),
                            }
                        }
                        _ = &mut waiting => break,
                    }
                }
            }
        }
    }
}

fn retry_delay(
    error: &cryptofeed_core::error::Error,
    backoff: std::time::Duration,
) -> std::time::Duration {
    match error {
        cryptofeed_core::error::Error::RateLimited { retry_after, .. } => backoff.max(*retry_after),
        cryptofeed_core::error::Error::HttpStatus { .. } => {
            super::quota::handshake_delay(error).map_or(backoff, |delay| backoff.max(delay))
        }
        _ => backoff,
    }
}

fn jittered_delay(seconds: u64) -> std::time::Duration {
    if seconds == 0 {
        return std::time::Duration::ZERO;
    }
    let jitter = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(100, |duration| 80 + u64::from(duration.subsec_nanos() % 41));
    std::time::Duration::from_millis(seconds.saturating_mul(1000).saturating_mul(jitter) / 100)
}

#[cfg(test)]
mod tests {
    use super::Backoff;
    use cryptofeed_core::error::{Error, Result};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::sync::watch;

    #[tokio::test]
    async fn successful_subscription_resets_budget_but_not_permanent_errors() {
        let progress = Arc::new(super::RetryProgress::default());
        let attempts = AtomicUsize::new(0);
        let (_stop, shutdown) = watch::channel(false);
        let result: Result<Option<()>> = super::retry_with_progress(
            Some(1),
            Backoff::new(0, 0),
            shutdown,
            Some(progress.clone()),
            std::time::Duration::ZERO,
            || async {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                if attempt == 1 {
                    progress.subscribed();
                }
                Err(Error::Transport("scripted failure".into()))
            },
        )
        .await;
        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 3); // Failure, successful subscription then disconnect, failure.
        let (_stop, shutdown) = watch::channel(false);
        let permanent = AtomicUsize::new(0);
        let result: Result<Option<()>> = super::retry_with_progress(
            Some(100),
            Backoff::new(0, 0),
            shutdown,
            Some(progress.clone()),
            std::time::Duration::ZERO,
            || async {
                permanent.fetch_add(1, Ordering::SeqCst);
                progress.subscribed();
                Err(Error::Subscription("rejected".into()))
            },
        )
        .await;
        assert!(matches!(result, Err(Error::Subscription(_))));
        assert_eq!(permanent.load(Ordering::SeqCst), 1);
        let mut backoff = Backoff::new(1, 8);
        assert_eq!(backoff.next_delay_secs(), 1);
        assert_eq!(backoff.next_delay_secs(), 2);
        backoff.reset();
        assert_eq!(backoff.next_delay_secs(), 1);
    }

    #[tokio::test]
    async fn startup_delay_is_cancellable_before_any_operation() {
        let (stop, shutdown) = watch::channel(false);
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = attempts.clone();
        let task = tokio::spawn(async move {
            super::retry_with_progress(
                Some(0),
                Backoff::new(0, 0),
                shutdown,
                None,
                std::time::Duration::from_secs(60),
                || async {
                    observed.fetch_add(1, Ordering::SeqCst);
                    Ok::<(), Error>(())
                },
            )
            .await
        });
        tokio::task::yield_now().await;
        stop.send(true).unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .is_none()
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 0);
    }
    #[tokio::test]
    async fn false_shutdown_notifications_do_not_restart_startup_timer() {
        let (stop, shutdown) = watch::channel(false);
        let notifications = tokio::spawn(async move {
            for _ in 0..100 {
                let _ = stop.send(false);
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        });
        let attempts = AtomicUsize::new(0);
        let started = tokio::time::Instant::now();
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            super::retry_with_progress(
                Some(1),
                Backoff::new(0, 0),
                shutdown,
                None,
                std::time::Duration::from_millis(20),
                || async {
                    assert!(started.elapsed() >= std::time::Duration::from_millis(20));
                    if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        Err(Error::Transport("first".into()))
                    } else {
                        Ok(42)
                    }
                },
            ),
        )
        .await
        .unwrap()
        .unwrap();
        notifications.abort();
        assert_eq!(result, Some(42));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn backoff_doubles_until_cap() {
        let mut backoff = Backoff::new(1, 8);
        assert_eq!(backoff.next_delay_secs(), 1);
        assert_eq!(backoff.next_delay_secs(), 2);
        assert_eq!(backoff.next_delay_secs(), 4);
        assert_eq!(backoff.next_delay_secs(), 8);
        assert_eq!(backoff.next_delay_secs(), 8);
    }

    #[tokio::test]
    async fn retries_until_success() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let counter = attempts.clone();

        let result = super::retry_with_backoff(3, Backoff::new(0, 0), move || {
            let counter = counter.clone();
            async move {
                let current = counter.fetch_add(1, Ordering::SeqCst);
                if current < 2 {
                    Err(Error::Transport("temporary".to_owned()))
                } else {
                    Ok::<_, Error>(42)
                }
            }
        })
        .await;

        assert_eq!(result.expect("success"), 42);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn returns_last_error_after_retry_limit() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let counter = attempts.clone();

        let result: Result<()> = super::retry_with_backoff(1, Backoff::new(0, 0), move || {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Err(Error::Transport("still failing".to_owned()))
            }
        })
        .await;

        assert!(matches!(result, Err(Error::Transport(_))));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn permanent_errors_fail_fast_without_retries() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let counter = attempts.clone();

        let result: Result<()> = super::retry_with_backoff(3, Backoff::new(0, 0), move || {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Err(Error::Subscription("rejected".to_owned()))
            }
        })
        .await;

        assert!(matches!(result, Err(Error::Subscription(_))));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn http_request_failures_fail_fast_but_throttling_stays_transient() {
        for (status, expected) in [(400, 1), (429, 2)] {
            let attempts = AtomicUsize::new(0);
            let result: Result<()> = super::retry_with_backoff(1, Backoff::new(0, 0), || async {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err(Error::HttpStatus {
                    status,
                    retry_after: None,
                })
            })
            .await;
            assert!(matches!(result,Err(Error::HttpStatus {status:actual,..}) if actual==status));
            assert_eq!(attempts.load(Ordering::SeqCst), expected);
        }
    }

    #[tokio::test]
    async fn shutdown_interrupts_backoff_without_another_attempt() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let counter = attempts.clone();
        let attempted = Arc::new(tokio::sync::Notify::new());
        let attempted_by_task = attempted.clone();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            super::retry_with_backoff_until_shutdown(
                Some(3),
                Backoff::new(60, 60),
                shutdown_rx,
                move || {
                    let counter = counter.clone();
                    let attempted = attempted_by_task.clone();
                    async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        attempted.notify_one();
                        Err::<(), _>(Error::Transport("temporary".to_owned()))
                    }
                },
            )
            .await
        });

        attempted.notified().await;
        shutdown_tx.send(true).unwrap();
        assert!(task.await.unwrap().is_ok());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn unlimited_retry_budget_survives_more_than_three_disconnects() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let counter = attempts.clone();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let result = super::retry_with_backoff_until_shutdown(
            None,
            Backoff::new(0, 0),
            shutdown_rx,
            move || {
                let counter = counter.clone();
                async move {
                    let current = counter.fetch_add(1, Ordering::SeqCst);
                    if current < 5 {
                        Err(Error::Transport("temporary".to_owned()))
                    } else {
                        Ok(42)
                    }
                }
            },
        )
        .await
        .expect("retry")
        .expect("operation result");

        assert_eq!(result, 42);
        assert_eq!(attempts.load(Ordering::SeqCst), 6);
    }
}

#[cfg(test)]
mod ws_quota_tests {
    use super::*;
    use cryptofeed_core::error::Error;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };
    fn limited() -> Error {
        Error::RateLimited {
            code: "60014".into(),
            retry_after: Duration::from_secs(60),
        }
    }
    #[tokio::test(start_paused = true)]
    async fn ws_quota_retry_floor_survives_false_shutdown_notifications() {
        let (tx, rx) = tokio::sync::watch::channel(false);
        let attempts = AtomicUsize::new(0);
        let start = tokio::time::Instant::now();
        let run = retry_with_backoff_until_shutdown(Some(1), Backoff::new(1, 8), rx, || async {
            if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(limited())
            } else {
                Ok(tokio::time::Instant::now())
            }
        });
        tokio::pin!(run);
        assert!(futures::poll!(run.as_mut()).is_pending());
        tokio::time::advance(Duration::from_secs(5)).await;
        tx.send(false).unwrap();
        assert!(futures::poll!(run.as_mut()).is_pending());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert_eq!(run.await.unwrap().unwrap() - start, Duration::from_secs(60));
    }
    #[tokio::test(start_paused = true)]
    async fn ws_quota_retry_wait_is_shutdown_interruptible() {
        let (tx, rx) = tokio::sync::watch::channel(false);
        let start = tokio::time::Instant::now();
        let run =
            retry_with_backoff_until_shutdown::<(), _, _>(None, Backoff::new(1, 8), rx, || async {
                Err(limited())
            });
        tokio::pin!(run);
        assert!(futures::poll!(run.as_mut()).is_pending());
        tx.send(true).unwrap();
        assert!(run.await.unwrap().is_none());
        assert_eq!(tokio::time::Instant::now(), start);
    }
    #[tokio::test(start_paused = true)]
    async fn ws_quota_bounded_retry_respects_floor_and_preserves_terminal_error() {
        let attempts = AtomicUsize::new(0);
        let start = tokio::time::Instant::now();
        let result = retry_with_backoff::<(), _, _>(1, Backoff::new(1, 8), || async {
            attempts.fetch_add(1, Ordering::SeqCst);
            Err(limited())
        })
        .await;
        assert!(matches!(result, Err(Error::RateLimited { .. })));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(tokio::time::Instant::now() - start, Duration::from_secs(60));
    }
}

#[cfg(test)]
mod ws_lifecycle_tests {
    use super::*;
    use cryptofeed_core::error::Error;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };
    #[tokio::test(start_paused = true)]
    async fn ws_lifecycle_http_status_retry_uses_hint_or_quota_fallback_without_early_false_updates()
     {
        for (status, hint, seconds) in [(429, None, 60), (503, Some(Duration::from_secs(17)), 17)] {
            let attempts = AtomicUsize::new(0);
            let (tx, rx) = tokio::sync::watch::channel(false);
            let start = tokio::time::Instant::now();
            let run =
                retry_with_backoff_until_shutdown(Some(1), Backoff::new(1, 8), rx, || async {
                    if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        Err(Error::HttpStatus {
                            status,
                            retry_after: hint,
                        })
                    } else {
                        Ok(())
                    }
                });
            tokio::pin!(run);
            assert!(futures::poll!(run.as_mut()).is_pending());
            tx.send(false).unwrap();
            assert!(futures::poll!(run.as_mut()).is_pending());
            assert!(run.await.unwrap().is_some());
            assert_eq!(
                tokio::time::Instant::now() - start,
                Duration::from_secs(seconds)
            );
        }
    }
    #[tokio::test(start_paused = true)]
    async fn ws_lifecycle_terminal_close_and_handshake_status_fail_once_while_normal_close_retries()
    {
        for error in [
            Error::WebSocketClosed {
                code: Some(1009),
                retryable: false,
            },
            Error::HttpStatus {
                status: 403,
                retry_after: Some(Duration::from_secs(60)),
            },
        ] {
            let attempts = AtomicUsize::new(0);
            let start = tokio::time::Instant::now();
            assert!(
                retry_with_backoff::<(), _, _>(10, Backoff::new(1, 8), || async {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err(error.clone())
                })
                .await
                .is_err()
            );
            assert_eq!(attempts.load(Ordering::SeqCst), 1);
            assert_eq!(tokio::time::Instant::now(), start);
        }
        let attempts = AtomicUsize::new(0);
        retry_with_backoff(1, Backoff::new(0, 0), || async {
            if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(Error::WebSocketClosed {
                    code: Some(1000),
                    retryable: true,
                })
            } else {
                Ok(())
            }
        })
        .await
        .unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }
}
