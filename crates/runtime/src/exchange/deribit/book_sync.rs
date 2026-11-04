//! Deribit order-book `change_id`/`prev_change_id` bridging.
//!
//! The documented `book.{instrument}.{interval}` contract is: first notification
//! `type:"snapshot"` with every level, later notifications `type:"change"` with
//! `change_id` and `prev_change_id`; `prev_change_id` equal to the previous
//! `change_id` proves no message was missed. Stale changes are dropped; a gap
//! enters a bounded in-session unsubscribe/subscribe resnapshot so the next push
//! is a replacement snapshot. Malformed rows never reach this module.

use cryptofeed_core::error::{Error, Result};
use cryptofeed_core::exchange::ExchangeId;
use cryptofeed_core::symbol::Symbol;
use cryptofeed_orderbook::{L2Book, L2BookDelta, L2BookSnapshot, L2BookState};

use super::parser::BookUpdate;

/// Cumulative resnapshot requests per connection before the reconnect/backoff
/// supervisor takes over.
pub const MAX_DERIBIT_RESNAPSHOTS: u32 = 3;

#[derive(Clone, Debug, PartialEq)]
pub enum DeribitBookOutcome {
    Publish(L2Book),
    /// Change with a non-increasing `change_id`; stale, dropped.
    Stale,
    /// The caller must send unsubscribe/subscribe for the same channel.
    Resnapshot,
}

pub struct DeribitBookSync {
    symbol: Symbol,
    state: L2BookState,
    last_change_id: Option<u64>,
    awaiting_snapshot: bool,
    resnapshot_requests: u32,
}

impl DeribitBookSync {
    pub fn new(symbol: Symbol) -> Self {
        Self {
            state: L2BookState::new(symbol.clone()),
            symbol,
            last_change_id: None,
            awaiting_snapshot: true,
            resnapshot_requests: 0,
        }
    }

    pub fn state(&self) -> &L2BookState {
        &self.state
    }

    pub fn last_change_id(&self) -> Option<u64> {
        self.last_change_id
    }

    pub fn awaiting_snapshot(&self) -> bool {
        self.awaiting_snapshot
    }

    pub fn resnapshot_requests(&self) -> u32 {
        self.resnapshot_requests
    }

    pub fn apply(&mut self, update: BookUpdate) -> Result<DeribitBookOutcome> {
        if update.snapshot {
            let snapshot = L2BookSnapshot {
                exchange: ExchangeId::Deribit,
                symbol: update.symbol.clone(),
                bids: update.bids,
                asks: update.asks,
                exchange_ts: update.exchange_ts,
                received_ts: update.received_ts,
            };
            self.state.apply(L2Book::Snapshot(snapshot.clone()));
            self.last_change_id = Some(update.change_id);
            self.awaiting_snapshot = false;
            return Ok(DeribitBookOutcome::Publish(L2Book::Snapshot(snapshot)));
        }

        if self.awaiting_snapshot {
            return Err(Error::Parse(format!(
                "Deribit {} book change received before the snapshot",
                self.symbol.as_str()
            )));
        }
        let Some(last) = self.last_change_id else {
            return Err(Error::Parse(format!(
                "Deribit {} book change has no snapshot anchor",
                self.symbol.as_str()
            )));
        };
        if update.change_id <= last {
            // Stale change: a duplicate or an out-of-order frame that cannot be
            // applied without corrupting the book.
            return Ok(DeribitBookOutcome::Stale);
        }
        if update.prev_change_id != Some(last) {
            return self.reset_for_resnapshot();
        }
        let delta = L2BookDelta {
            exchange: ExchangeId::Deribit,
            symbol: update.symbol,
            bids: update.bids,
            asks: update.asks,
            exchange_ts: update.exchange_ts,
            received_ts: update.received_ts,
        };
        self.state.apply(L2Book::Delta(delta.clone()));
        self.last_change_id = Some(update.change_id);
        Ok(DeribitBookOutcome::Publish(L2Book::Delta(delta)))
    }

    fn reset_for_resnapshot(&mut self) -> Result<DeribitBookOutcome> {
        self.resnapshot_requests += 1;
        if self.resnapshot_requests > MAX_DERIBIT_RESNAPSHOTS {
            return Err(Error::Parse(
                "deribit book resnapshot request limit exceeded".to_owned(),
            ));
        }
        self.state = L2BookState::new(self.symbol.clone());
        self.last_change_id = None;
        self.awaiting_snapshot = true;
        Ok(DeribitBookOutcome::Resnapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cryptofeed_orderbook::PriceLevel;
    use rust_decimal::Decimal;

    fn symbol() -> Symbol {
        Symbol::perpetual("BTC", "USD")
    }

    fn level(price: &str, amount: &str) -> PriceLevel {
        PriceLevel {
            price: Decimal::from_str_exact(price).unwrap(),
            amount: Decimal::from_str_exact(amount).unwrap(),
        }
    }

    fn snapshot(change_id: u64) -> BookUpdate {
        BookUpdate {
            snapshot: true,
            symbol: symbol(),
            change_id,
            prev_change_id: None,
            bids: vec![level("100", "1")],
            asks: vec![level("101", "1")],
            exchange_ts: 1.0,
            received_ts: 2.0,
        }
    }

    fn change(change_id: u64, prev: Option<u64>) -> BookUpdate {
        BookUpdate {
            snapshot: false,
            symbol: symbol(),
            change_id,
            prev_change_id: prev,
            bids: vec![level("100", "2")],
            asks: Vec::new(),
            exchange_ts: 1.0,
            received_ts: 2.0,
        }
    }

    #[test]
    fn applies_snapshot_then_chained_changes_and_drops_stale_ids() {
        let mut sync = DeribitBookSync::new(symbol());
        let DeribitBookOutcome::Publish(L2Book::Snapshot(book)) = sync.apply(snapshot(10)).unwrap()
        else {
            panic!("expected snapshot");
        };
        assert_eq!(book.bids[0].amount, Decimal::ONE);
        let DeribitBookOutcome::Publish(L2Book::Delta(delta)) =
            sync.apply(change(11, Some(10))).unwrap()
        else {
            panic!("expected delta");
        };
        assert_eq!(delta.bids[0].amount, Decimal::from(2));
        assert_eq!(sync.last_change_id(), Some(11));
        assert_eq!(
            sync.apply(change(11, Some(10))).unwrap(),
            DeribitBookOutcome::Stale
        );
        assert_eq!(
            sync.apply(change(5, Some(4))).unwrap(),
            DeribitBookOutcome::Stale
        );
    }

    #[test]
    fn rejects_change_before_snapshot_and_resnapshots_on_gaps() {
        let mut sync = DeribitBookSync::new(symbol());
        assert!(sync.apply(change(1, None)).is_err());
        sync.apply(snapshot(10)).unwrap();
        assert_eq!(
            sync.apply(change(13, Some(12))).unwrap(),
            DeribitBookOutcome::Resnapshot
        );
        assert!(sync.awaiting_snapshot());
        let DeribitBookOutcome::Publish(L2Book::Snapshot(_)) = sync.apply(snapshot(20)).unwrap()
        else {
            panic!("expected replacement snapshot");
        };
        assert_eq!(sync.last_change_id(), Some(20));
    }

    #[test]
    fn bounds_the_in_session_resnapshot_budget() {
        let mut sync = DeribitBookSync::new(symbol());
        let mut version = 1u64;
        sync.apply(snapshot(version)).unwrap();
        for _ in 0..MAX_DERIBIT_RESNAPSHOTS {
            let gap = version + 10;
            assert_eq!(
                sync.apply(change(gap, Some(gap - 1))).unwrap(),
                DeribitBookOutcome::Resnapshot
            );
            version += 1_000;
            sync.apply(snapshot(version)).unwrap();
        }
        assert!(sync.apply(change(version + 10, Some(version + 9))).is_err());
    }
}
