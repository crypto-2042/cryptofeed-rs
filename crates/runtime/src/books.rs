//! Recoverable, revision-anchored views of managed L2 feeds.
use crate::feed::{FeedId, FeedIdentity};
use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
use cryptofeed_orderbook::{L2Book, L2BookSnapshot, L2BookState};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::sync::broadcast;

/// Local delivery anchor, not an exchange sequence number. Revisions are
/// contiguous for a symbol within one identity/connection/epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BookAnchor {
    pub identity: FeedIdentity,
    pub connection: u64,
    pub epoch: u64,
    pub revision: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BookSnapshot {
    pub anchor: BookAnchor,
    pub book: L2BookSnapshot,
}

/// None withdraws a previously usable book. A later snapshot reinitializes it.
#[derive(Clone, Debug, PartialEq)]
pub struct BookUpdate {
    pub anchor: BookAnchor,
    pub symbol: Symbol,
    pub book: Option<L2Book>,
}

/// Snapshot and subsequent stream acquired atomically. On Lagged or an anchor
/// discontinuity, discard the local book and call recover again.
pub struct BookRecovery {
    pub snapshot: Option<BookSnapshot>,
    pub updates: BookUpdates,
}

/// Bounded stream scoped to one configuration identity and symbol.
pub struct BookUpdates {
    receiver: broadcast::Receiver<BookUpdate>,
    identity: FeedIdentity,
    symbol: Symbol,
}
impl BookUpdates {
    pub async fn recv(&mut self) -> Result<BookUpdate, broadcast::error::RecvError> {
        loop {
            let update = self.receiver.recv().await?;
            if update.anchor.identity == self.identity && update.symbol == self.symbol {
                return Ok(update);
            }
        }
    }
}

/// Retained access to opt-in managed L2 recovery state.
#[derive(Clone)]
pub struct L2BookHandle(pub(crate) Arc<BookStore>);
impl L2BookHandle {
    /// Atomically copies a synchronized snapshot and starts the subsequent
    /// bounded update stream. None means unavailable, unknown or retired.
    pub fn recover(&self, identity: FeedIdentity, symbol: &Symbol) -> BookRecovery {
        let state = self.0.state.lock().expect("book recovery lock");
        let receiver = self.0.sender.subscribe();
        let snapshot = state
            .entries
            .get(&(identity.id, symbol.as_str().to_owned()))
            .filter(|entry| entry.anchor.identity == identity)
            .and_then(|entry| {
                let levels = entry.state.as_ref()?;
                let (exchange, exchange_ts, received_ts) = entry.meta?;
                Some(BookSnapshot {
                    anchor: entry.anchor,
                    book: L2BookSnapshot {
                        exchange,
                        symbol: levels.symbol().clone(),
                        bids: levels.bids(),
                        asks: levels.asks(),
                        exchange_ts,
                        received_ts,
                    },
                })
            });
        BookRecovery {
            snapshot,
            updates: BookUpdates {
                receiver,
                identity,
                symbol: symbol.clone(),
            },
        }
    }
}

struct Entry {
    anchor: BookAnchor,
    enabled: bool,
    state: Option<L2BookState>,
    meta: Option<(ExchangeId, f64, f64)>,
}
#[derive(Default)]
struct State {
    active: HashMap<FeedId, FeedIdentity>,
    entries: HashMap<(FeedId, String), Entry>,
}
pub(crate) struct BookStore {
    state: Mutex<State>,
    sender: broadcast::Sender<BookUpdate>,
}
impl Default for BookStore {
    fn default() -> Self {
        Self::with_capacity(1024)
    }
}
impl BookStore {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            state: Mutex::new(State::default()),
            sender: broadcast::channel(capacity).0,
        }
    }
    pub fn activate(&self, identity: FeedIdentity) {
        self.state
            .lock()
            .expect("book recovery lock")
            .active
            .insert(identity.id, identity);
    }
    fn withdraw(&self, entry: &mut Entry) {
        if let Some(levels) = entry.state.take() {
            entry.meta = None;
            entry.anchor.revision += 1;
            let _ = self.sender.send(BookUpdate {
                anchor: entry.anchor,
                symbol: levels.symbol().clone(),
                book: None,
            });
        }
    }

    pub fn retire(&self, identity: FeedIdentity) {
        let mut state = self.state.lock().expect("book recovery lock");
        if state.active.get(&identity.id) == Some(&identity) {
            state.active.remove(&identity.id);
        }
        state.entries.retain(|_, entry| {
            if entry.anchor.identity == identity {
                self.withdraw(entry);
                false
            } else {
                true
            }
        });
    }
    pub fn begin(
        &self,
        identity: FeedIdentity,
        connection: u64,
        epoch: u64,
        symbols: &std::collections::HashSet<String>,
    ) {
        let mut state = self.state.lock().expect("book recovery lock");
        if state.active.get(&identity.id) != Some(&identity) {
            return;
        }
        for symbol in symbols {
            let key = (identity.id, symbol.clone());
            if let Some(old) = state.entries.get_mut(&key) {
                self.withdraw(old);
            }
            state.entries.insert(
                key,
                Entry {
                    anchor: BookAnchor {
                        identity,
                        connection,
                        epoch,
                        revision: 0,
                    },
                    enabled: true,
                    state: None,
                    meta: None,
                },
            );
        }
    }
    pub fn invalidate(
        &self,
        identity: FeedIdentity,
        connection: u64,
        epoch: u64,
        symbol: Option<&str>,
    ) {
        let mut state = self.state.lock().expect("book recovery lock");
        for ((_, key), entry) in &mut state.entries {
            if entry.anchor.identity == identity
                && entry.anchor.connection == connection
                && entry.anchor.epoch == epoch
                && symbol.is_none_or(|symbol| symbol == key)
            {
                self.withdraw(entry);
                if symbol.is_none() {
                    entry.enabled = false;
                }
            }
        }
    }
    pub fn apply(&self, identity: FeedIdentity, connection: u64, epoch: u64, book: &L2Book) {
        let mut state = self.state.lock().expect("book recovery lock");
        if state.active.get(&identity.id) != Some(&identity) {
            return;
        }
        let Some(entry) = state
            .entries
            .get_mut(&(identity.id, book.symbol().as_str().to_owned()))
        else {
            return;
        };
        if !entry.enabled
            || entry.anchor.connection != connection
            || entry.anchor.epoch != epoch
            || entry.anchor.identity != identity
        {
            return;
        }
        if matches!(book, L2Book::Snapshot(_)) {
            entry.state = Some(L2BookState::new(book.symbol().clone()));
        }
        let Some(levels) = entry.state.as_mut() else {
            return;
        };
        levels.apply(book.clone());
        entry.anchor.revision += 1;
        entry.meta = Some((
            match book {
                L2Book::Snapshot(value) => value.exchange,
                L2Book::Delta(value) => value.exchange,
            },
            book.exchange_ts(),
            book.received_ts(),
        ));
        let _ = self.sender.send(BookUpdate {
            anchor: entry.anchor,
            symbol: book.symbol().clone(),
            book: Some(book.clone()),
        });
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::feed::FeedId;
    use cryptofeed_orderbook::{L2BookDelta, PriceLevel};
    use rust_decimal::Decimal;
    fn identity(generation: u64) -> FeedIdentity {
        FeedIdentity {
            id: FeedId::allocate(),
            generation,
        }
    }
    fn symbol() -> Symbol {
        Symbol::spot("BTC", "USDT")
    }
    pub(crate) fn book(snapshot: bool, amount: i64) -> L2Book {
        let bids = vec![PriceLevel {
            price: Decimal::ONE,
            amount: Decimal::from(amount),
        }];
        if snapshot {
            L2Book::Snapshot(L2BookSnapshot {
                exchange: ExchangeId::Okx,
                symbol: symbol(),
                bids,
                asks: vec![],
                exchange_ts: 1.0,
                received_ts: 2.0,
            })
        } else {
            L2Book::Delta(L2BookDelta {
                exchange: ExchangeId::Okx,
                symbol: symbol(),
                bids,
                asks: vec![],
                exchange_ts: 3.0,
                received_ts: 4.0,
            })
        }
    }
    fn setup(capacity: usize) -> (L2BookHandle, FeedIdentity) {
        let handle = L2BookHandle(Arc::new(BookStore::with_capacity(capacity)));
        let id = identity(1);
        handle.0.activate(id);
        handle.0.begin(
            id,
            1,
            1,
            &std::collections::HashSet::from([symbol().as_str().to_owned()]),
        );
        (handle, id)
    }
    #[tokio::test]
    async fn snapshots_and_updates_share_contiguous_anchors_and_preserve_deletions() {
        let (handle, id) = setup(16);
        handle.0.apply(id, 1, 1, &book(false, 9)); // No initialized snapshot yet.
        assert!(handle.recover(id, &symbol()).snapshot.is_none());
        handle.0.apply(id, 1, 1, &book(true, 2));
        let mut recovery = handle.recover(id, &symbol());
        assert_eq!(recovery.snapshot.as_ref().unwrap().anchor.revision, 1);
        handle.0.apply(id, 1, 1, &book(false, 0));
        let update = recovery.updates.recv().await.unwrap();
        assert_eq!(update.anchor.revision, 2);
        assert_eq!(update.book, Some(book(false, 0)));
        let latest = handle.recover(id, &symbol()).snapshot.unwrap();
        assert!(latest.book.bids.is_empty());
        assert_eq!(latest.book.exchange_ts, 3.0);
        assert_eq!(latest.book.received_ts, 4.0);
    }
    #[tokio::test]
    async fn lag_recovery_replaces_queue_and_resumes_after_full_snapshot() {
        let (handle, id) = setup(2);
        handle.0.apply(id, 1, 1, &book(true, 1));
        let mut old = handle.recover(id, &symbol());
        for amount in 2..10 {
            handle.0.apply(id, 1, 1, &book(false, amount));
        }
        assert!(matches!(
            old.updates.recv().await,
            Err(broadcast::error::RecvError::Lagged(_))
        ));
        let mut fresh = handle.recover(id, &symbol());
        let snapshot = fresh.snapshot.unwrap();
        assert_eq!(snapshot.book.bids[0].amount, Decimal::from(9));
        handle.0.apply(id, 1, 1, &book(false, 10));
        let next = fresh.updates.recv().await.unwrap();
        assert_eq!(next.anchor.revision, snapshot.anchor.revision + 1);
    }
    #[tokio::test]
    async fn resync_withdraws_cache_until_new_snapshot_and_keeps_revision() {
        let (handle, id) = setup(16);
        handle.0.apply(id, 1, 1, &book(true, 1));
        let mut old = handle.recover(id, &symbol());
        handle.0.invalidate(id, 1, 1, Some(symbol().as_str()));
        let withdrawn = old.updates.recv().await.unwrap();
        assert!(withdrawn.book.is_none());
        assert_eq!(withdrawn.anchor.revision, 2);
        handle.0.apply(id, 1, 1, &book(false, 2));
        assert!(handle.recover(id, &symbol()).snapshot.is_none());
        handle.0.apply(id, 1, 1, &book(true, 3));
        let next = old.updates.recv().await.unwrap();
        assert_eq!(next.anchor.revision, 3);
        assert!(matches!(next.book, Some(L2Book::Snapshot(_))));
    }
    #[test]
    fn reconnect_epoch_and_connection_scoping_reject_stale_progress() {
        let (handle, id) = setup(16);
        let eth = Symbol::spot("ETH", "USDT");
        handle.0.begin(
            id,
            2,
            1,
            &std::collections::HashSet::from([eth.as_str().to_owned()]),
        );
        let L2Book::Snapshot(mut eth_book) = book(true, 2) else {
            unreachable!()
        };
        eth_book.symbol = eth.clone();
        handle.0.apply(id, 2, 1, &L2Book::Snapshot(eth_book));
        handle.0.apply(id, 1, 1, &book(true, 1));
        handle.0.invalidate(id, 1, 1, None);
        handle.0.apply(id, 1, 1, &book(true, 9)); // Same-epoch progress after disconnect is forbidden.
        assert!(handle.recover(id, &symbol()).snapshot.is_none());
        assert!(handle.recover(id, &eth).snapshot.is_some());
        handle.0.begin(
            id,
            1,
            2,
            &std::collections::HashSet::from([symbol().as_str().to_owned()]),
        );
        handle.0.apply(id, 1, 2, &book(true, 2));
        handle.0.invalidate(id, 1, 1, None); // Old attempt Drop.
        handle.0.apply(id, 1, 1, &book(true, 9));
        assert_eq!(
            handle.recover(id, &symbol()).snapshot.unwrap().anchor.epoch,
            2
        );
    }
    #[test]
    fn candidate_cancellation_does_not_retire_current_and_replacement_fences_old_generation() {
        let (handle, id) = setup(16);
        let next = FeedIdentity {
            generation: 2,
            ..id
        };
        handle.0.apply(id, 1, 1, &book(true, 1));
        handle.0.retire(next);
        assert!(handle.recover(id, &symbol()).snapshot.is_some());
        handle.0.retire(id);
        assert!(handle.recover(id, &symbol()).snapshot.is_none());
        handle.0.activate(next);
        handle.0.begin(
            next,
            1,
            1,
            &std::collections::HashSet::from([symbol().as_str().to_owned()]),
        );
        handle.0.apply(next, 1, 1, &book(true, 2));
        handle.0.apply(id, 1, 1, &book(true, 9));
        handle.0.retire(id);
        assert_eq!(
            handle.recover(next, &symbol()).snapshot.unwrap().book.bids[0].amount,
            Decimal::from(2)
        );
        handle.0.retire(next);
        assert!(handle.0.state.lock().unwrap().entries.is_empty());
    }
    #[tokio::test]
    async fn concurrent_recovery_has_no_snapshot_to_subscription_hole() {
        let (handle, id) = setup(2048);
        handle.0.apply(id, 1, 1, &book(true, 1));
        let writer = handle.clone();
        let task = std::thread::spawn(move || {
            for value in 2..=1000 {
                writer.0.apply(id, 1, 1, &book(false, value));
            }
        });
        let mut recovery = handle.recover(id, &symbol());
        let snapshot = recovery.snapshot.unwrap();
        let mut revision = snapshot.anchor.revision;
        let mut local = L2BookState::new(symbol());
        local.apply(L2Book::Snapshot(snapshot.book));
        while revision < 1000 {
            let update = recovery.updates.recv().await.unwrap();
            assert_eq!(update.anchor.revision, revision + 1);
            revision = update.anchor.revision;
            local.apply(update.book.unwrap());
        }
        task.join().unwrap();
        assert_eq!(
            local.bids(),
            handle.recover(id, &symbol()).snapshot.unwrap().book.bids
        );
    }
}
