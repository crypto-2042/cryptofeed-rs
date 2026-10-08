pub struct Backoff {
    current: u64,
    max: u64,
}

impl Backoff {
    pub fn new(initial: u64, max: u64) -> Self {
        Self {
            current: initial,
            max,
        }
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
        cryptofeed_core::error::Error::UnsupportedExchange(_)
            | cryptofeed_core::error::Error::UnsupportedChannel(_)
            | cryptofeed_core::error::Error::UnsupportedSymbol(_)
            | cryptofeed_core::error::Error::AmbiguousSymbol(_)
            | cryptofeed_core::error::Error::UnsupportedCapability(_)
            | cryptofeed_core::error::Error::InvalidConfiguration(_)
            | cryptofeed_core::error::Error::Subscription(_)
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
                let delay = backoff.next_delay_secs();
                attempts += 1;
                tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
            }
        }
    }
}

pub async fn retry_with_backoff_until_shutdown<T, F, Fut>(
    max_retries: Option<usize>,
    mut backoff: Backoff,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    operation: F,
) -> cryptofeed_core::error::Result<Option<T>>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = cryptofeed_core::error::Result<T>>,
{
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
                if max_retries.is_some_and(|limit| attempts >= limit) {
                    return Err(error);
                }
                let delay = jittered_delay(backoff.next_delay_secs());
                attempts = attempts.saturating_add(1);
                tracing::warn!(attempt = attempts, retry_delay = ?delay, error = %error,
                    "transient feed failure; retrying");
                tokio::select! {
                    biased;
                    changed = shutdown.changed() => {
                        match changed {
                            Ok(()) if !*shutdown.borrow() => {}
                            Ok(()) | Err(_) => return Ok(None),
                        }
                    }
                    _ = tokio::time::sleep(delay) => {}
                }
            }
        }
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
