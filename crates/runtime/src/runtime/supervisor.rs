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
                if attempts >= max_retries {
                    return Err(err);
                }
                let delay = backoff.next_delay_secs();
                attempts += 1;
                tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Backoff;
    use cryptofeed_core::error::{Error, Result};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

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

        let result: Result<()> =
            super::retry_with_backoff(1, Backoff::new(0, 0), move || {
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
}
