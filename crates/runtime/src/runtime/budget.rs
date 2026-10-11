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
pub(crate) const MAX_CONNECTIONS_PER_EXCHANGE: usize = crate::resources::WS_LIMIT;
const CONNECTION_INTERVAL: Duration = crate::resources::WS_INTERVAL;
#[derive(Debug)]
pub(super) struct ConnectionSlots {
    _global: OwnedSemaphorePermit,
    _scope: Option<OwnedSemaphorePermit>,
}

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

    #[cfg(test)]
    async fn acquire(&self) -> Result<ConnectionSlots> {
        self.acquire_scoped(None).await
    }
    async fn acquire_scoped(
        &self,
        scope: Option<&crate::resources::Gate>,
    ) -> Result<ConnectionSlots> {
        let local = match scope {
            Some(scope) => Some(scope.slots.clone().try_acquire_owned().map_err(|_| {
                Error::InvalidConfiguration("shared resource WebSocket budget exhausted".into())
            })?),
            None => None,
        };
        let global = self.slots.clone().try_acquire_owned().map_err(|_| {
            Error::InvalidConfiguration(
                "process-local WebSocket connection budget exhausted".into(),
            )
        })?;
        loop {
            let mut next = self.next.lock().await;
            let mut scoped = match scope {
                Some(scope) => Some(scope.next.lock().await),
                None => None,
            };
            let now = Instant::now();
            let deadline = (*next).max(scoped.as_deref().copied().unwrap_or(now));
            if deadline <= now {
                *next = now + self.interval;
                if let (Some(scoped), Some(scope)) = (scoped.as_mut(), scope) {
                    **scoped = now + scope.interval;
                }
                return Ok(ConnectionSlots {
                    _global: global,
                    _scope: local,
                });
            }
            drop(scoped);
            drop(next);
            tokio::time::sleep_until(deadline).await;
        }
    }
}

pub(super) async fn acquire(
    exchange: ExchangeId,
    scope: Option<Arc<crate::resources::Gate>>,
) -> Result<ConnectionSlots> {
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
    budget.acquire_scoped(scope.as_deref()).await
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

#[cfg(test)]
mod scoped_tests {
    use super::*;
    #[tokio::test(start_paused = true)]
    async fn composite_start_clocks_prevent_burst_after_global_delay() {
        let global = ConnectionBudget::new(4, Duration::from_secs(1));
        let local = crate::resources::Gate::new(4, Duration::from_secs(3));
        let start = Instant::now();
        *global.next.lock().await = start + Duration::from_secs(10);
        let tasks = (0..2).map(|_| async {
            let slot = global.acquire_scoped(Some(&local)).await.unwrap();
            (Instant::now(), slot)
        });
        let results = futures::future::join_all(tasks).await;
        assert_eq!(results[0].0 - start, Duration::from_secs(10));
        assert!(results[1].0 - results[0].0 >= Duration::from_secs(3));
    }
    #[tokio::test(start_paused = true)]
    async fn scoped_exhaustion_is_explicit_and_drop_releases_both_slots() {
        let global = ConnectionBudget::new(3, Duration::ZERO);
        let local = crate::resources::Gate::new(1, Duration::ZERO);
        let first = global.acquire_scoped(Some(&local)).await.unwrap();
        assert!(global.acquire_scoped(Some(&local)).await.is_err());
        assert_eq!(global.slots.available_permits(), 2);
        drop(first);
        assert_eq!(global.slots.available_permits(), 3);
        assert_eq!(local.slots.available_permits(), 1);
    }
    #[tokio::test(start_paused = true)]
    async fn cancelling_scoped_pacing_releases_live_and_queued_capacity() {
        let global = Arc::new(ConnectionBudget::new(3, Duration::from_secs(10)));
        let local = Arc::new(crate::resources::Gate::new(1, Duration::from_secs(10)));
        drop(global.acquire_scoped(Some(&local)).await.unwrap());
        {
            let waiting = global.acquire_scoped(Some(&local));
            tokio::pin!(waiting);
            assert!(futures::poll!(waiting.as_mut()).is_pending());
            assert_eq!(local.slots.available_permits(), 0);
            assert_eq!(global.slots.available_permits(), 2);
        }
        assert_eq!(local.slots.available_permits(), 1);
        assert_eq!(global.slots.available_permits(), 3);
    }
}
