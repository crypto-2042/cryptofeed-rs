use cryptofeed_core::{
    error::{Error, Result},
    exchange::ExchangeId,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};

// Bitget documents 100 simultaneous connections per IP. This is also the
// SDK's process-local resource ceiling for the other exchanges, not a claim
// about their remote limits or the activity of other processes sharing an IP.
pub(crate) const MAX_CONNECTIONS_PER_EXCHANGE: usize = 100;
const CONNECTION_INTERVAL: Duration = Duration::from_millis(1100);

struct ConnectionBudget {
    slots: Arc<Semaphore>,
    next: tokio::sync::Mutex<Instant>,
    interval: Duration,
}

impl ConnectionBudget {
    fn new(limit: usize, interval: Duration) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(limit)),
            next: tokio::sync::Mutex::new(Instant::now()),
            interval,
        }
    }

    async fn acquire(&self) -> Result<OwnedSemaphorePermit> {
        let permit = self.slots.clone().try_acquire_owned().map_err(|_| {
            Error::InvalidConfiguration(
                "process-local WebSocket connection budget exhausted".to_owned(),
            )
        })?;
        let mut next = self.next.lock().await;
        tokio::time::sleep_until(*next).await;
        *next = Instant::now() + self.interval;
        Ok(permit)
    }
}

pub(super) async fn acquire(exchange: ExchangeId) -> Result<OwnedSemaphorePermit> {
    static BUDGETS: OnceLock<Mutex<HashMap<ExchangeId, Arc<ConnectionBudget>>>> = OnceLock::new();
    let budget = BUDGETS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("connection budgets lock")
        .entry(exchange)
        .or_insert_with(|| {
            Arc::new(ConnectionBudget::new(
                MAX_CONNECTIONS_PER_EXCHANGE,
                CONNECTION_INTERVAL,
            ))
        })
        .clone();
    budget.acquire().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn active_connection_budget_fails_explicitly_and_recovers_on_drop() {
        let budget = ConnectionBudget::new(1, Duration::ZERO);
        let permit = budget.acquire().await.unwrap();
        assert!(budget.acquire().await.is_err());
        drop(permit);
        assert!(budget.acquire().await.is_ok());
    }

    #[tokio::test]
    async fn connection_attempts_are_paced() {
        let budget = ConnectionBudget::new(3, Duration::from_millis(10));
        let start = Instant::now();
        let permits = futures::future::join_all((0..3).map(|_| budget.acquire())).await;
        assert!(permits.iter().all(|permit| permit.is_ok()));
        assert!(start.elapsed() >= Duration::from_millis(20));
    }

    #[tokio::test]
    async fn cancellation_during_pacing_releases_the_slot() {
        let budget = Arc::new(ConnectionBudget::new(1, Duration::from_secs(60)));
        drop(budget.acquire().await.unwrap());
        let task = {
            let budget = budget.clone();
            tokio::spawn(async move { budget.acquire().await })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while budget.slots.available_permits() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(budget.slots.available_permits(), 1);
    }
}
