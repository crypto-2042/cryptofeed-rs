//! HTX order-book snapshot/version bridging.
//!
//! Two documented shapes exist:
//!
//! - **Spot `depth.stepX`** is a replacement snapshot per push. `version` is the
//!   documented monotonic dedupe anchor ("use version to de-duplicate and
//!   discard the smaller data"); it is not a delta sequence and no checksum
//!   exists.
//! - **USDT-M `depth.size_20.high_freq` with `data_type=incremental`** pushes a
//!   snapshot first (`event:"snapshot"`) and then `event:"update"` rows with
//!   absolute sizes. `version` auto-increments within one connection; a
//!   non-consecutive update version is a gap and enters a bounded in-session
//!   unsubscribe/resubscribe so the next push is a replacement snapshot.

use cryptofeed_core::error::{Error, Result};
use cryptofeed_core::exchange::ExchangeId;
use cryptofeed_core::symbol::Symbol;
use cryptofeed_orderbook::{L2Book, L2BookDelta, L2BookSnapshot, L2BookState};

use super::parser::BookUpdate;

/// Cumulative resnapshot requests per connection before the reconnect/backoff
/// supervisor takes over.
pub const MAX_HTX_RESNAPSHOTS: u32 = 3;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HtxBookMode {
    /// Spot `depth.stepX`: complete replacement snapshot per push.
    SpotSnapshot,
    /// USDT-M `depth.size_20.high_freq`: snapshot + versioned updates.
    SwapIncremental,
}

#[derive(Clone, Debug, PartialEq)]
pub enum HtxBookOutcome {
    Publish(L2Book),
    /// Version not greater than the local anchor; dropped.
    Stale,
    /// The caller must send unsubscribe/subscribe for the incremental topic.
    Resnapshot,
}

pub struct HtxBookSync {
    symbol: Symbol,
    mode: HtxBookMode,
    state: L2BookState,
    last_version: Option<u64>,
    awaiting_snapshot: bool,
    resnapshot_requests: u32,
}

impl HtxBookSync {
    pub fn new(symbol: Symbol, mode: HtxBookMode) -> Self {
        Self {
            state: L2BookState::new(symbol.clone()),
            symbol,
            mode,
            last_version: None,
            awaiting_snapshot: mode == HtxBookMode::SwapIncremental,
            resnapshot_requests: 0,
        }
    }

    pub fn state(&self) -> &L2BookState {
        &self.state
    }

    pub fn last_version(&self) -> Option<u64> {
        self.last_version
    }

    pub fn awaiting_snapshot(&self) -> bool {
        self.awaiting_snapshot
    }

    pub fn resnapshot_requests(&self) -> u32 {
        self.resnapshot_requests
    }

    pub fn apply(&mut self, update: BookUpdate) -> Result<HtxBookOutcome> {
        match self.mode {
            HtxBookMode::SpotSnapshot => self.apply_spot(update),
            HtxBookMode::SwapIncremental => self.apply_swap(update),
        }
    }

    fn apply_spot(&mut self, update: BookUpdate) -> Result<HtxBookOutcome> {
        if self.last_version.is_some_and(|last| update.version <= last) {
            return Ok(HtxBookOutcome::Stale);
        }
        let snapshot = L2BookSnapshot {
            exchange: ExchangeId::Htx,
            symbol: update.symbol,
            bids: update.bids,
            asks: update.asks,
            exchange_ts: update.exchange_ts,
            received_ts: update.received_ts,
        };
        self.state.apply(L2Book::Snapshot(snapshot.clone()));
        self.last_version = Some(update.version);
        Ok(HtxBookOutcome::Publish(L2Book::Snapshot(snapshot)))
    }

    fn apply_swap(&mut self, update: BookUpdate) -> Result<HtxBookOutcome> {
        if update.snapshot {
            let snapshot = L2BookSnapshot {
                exchange: ExchangeId::Htx,
                symbol: update.symbol.clone(),
                bids: update.bids,
                asks: update.asks,
                exchange_ts: update.exchange_ts,
                received_ts: update.received_ts,
            };
            self.state.apply(L2Book::Snapshot(snapshot.clone()));
            self.last_version = Some(update.version);
            self.awaiting_snapshot = false;
            return Ok(HtxBookOutcome::Publish(L2Book::Snapshot(snapshot)));
        }
        if self.awaiting_snapshot {
            return Err(Error::Parse(format!(
                "HTX {} incremental depth update before the snapshot",
                self.symbol.as_str()
            )));
        }
        let Some(last) = self.last_version else {
            return Err(Error::Parse(format!(
                "HTX {} incremental depth update has no snapshot anchor",
                self.symbol.as_str()
            )));
        };
        if update.version <= last {
            return Ok(HtxBookOutcome::Stale);
        }
        if update.version != last + 1 {
            return self.reset_for_resnapshot();
        }
        let delta = L2BookDelta {
            exchange: ExchangeId::Htx,
            symbol: update.symbol,
            bids: update.bids,
            asks: update.asks,
            exchange_ts: update.exchange_ts,
            received_ts: update.received_ts,
        };
        self.state.apply(L2Book::Delta(delta.clone()));
        self.last_version = Some(update.version);
        Ok(HtxBookOutcome::Publish(L2Book::Delta(delta)))
    }

    fn reset_for_resnapshot(&mut self) -> Result<HtxBookOutcome> {
        self.resnapshot_requests += 1;
        if self.resnapshot_requests > MAX_HTX_RESNAPSHOTS {
            return Err(Error::Parse(
                "htx book resnapshot request limit exceeded".to_owned(),
            ));
        }
        self.state = L2BookState::new(self.symbol.clone());
        self.last_version = None;
        self.awaiting_snapshot = true;
        Ok(HtxBookOutcome::Resnapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cryptofeed_orderbook::PriceLevel;
    use rust_decimal::Decimal;

    fn spot() -> Symbol {
        Symbol::spot("BTC", "USDT")
    }

    fn perp() -> Symbol {
        Symbol::perpetual("BTC", "USDT")
    }

    fn level(price: &str, amount: &str) -> PriceLevel {
        PriceLevel {
            price: Decimal::from_str_exact(price).unwrap(),
            amount: Decimal::from_str_exact(amount).unwrap(),
        }
    }

    fn update(snapshot: bool, version: u64) -> BookUpdate {
        BookUpdate {
            snapshot,
            version,
            symbol: perp(),
            bids: vec![level("100", "2")],
            asks: Vec::new(),
            exchange_ts: 1.0,
            received_ts: 2.0,
        }
    }

    #[test]
    fn spot_snapshot_replaces_and_drops_stale_versions() {
        let mut sync = HtxBookSync::new(spot(), HtxBookMode::SpotSnapshot);
        let mut first = update(true, 10);
        first.symbol = spot();
        let HtxBookOutcome::Publish(L2Book::Snapshot(book)) = sync.apply(first).unwrap() else {
            panic!("expected snapshot");
        };
        assert_eq!(book.bids[0].amount, Decimal::from(2));
        let mut stale = update(true, 9);
        stale.symbol = spot();
        assert_eq!(sync.apply(stale).unwrap(), HtxBookOutcome::Stale);
        let mut replacement = update(true, 11);
        replacement.symbol = spot();
        replacement.bids = vec![level("99", "5")];
        let HtxBookOutcome::Publish(L2Book::Snapshot(book)) = sync.apply(replacement).unwrap()
        else {
            panic!("expected replacement snapshot");
        };
        assert_eq!(book.bids[0].price, Decimal::from(99));
        assert_eq!(sync.last_version(), Some(11));
    }

    #[test]
    fn swap_snapshot_then_consecutive_versions_and_gap_resnapshot() {
        let mut sync = HtxBookSync::new(perp(), HtxBookMode::SwapIncremental);
        assert!(sync.awaiting_snapshot());
        assert!(sync.apply(update(false, 1)).is_err());
        sync.apply(update(true, 100)).unwrap();
        let HtxBookOutcome::Publish(L2Book::Delta(delta)) = sync.apply(update(false, 101)).unwrap()
        else {
            panic!("expected delta");
        };
        assert_eq!(delta.bids[0].amount, Decimal::from(2));
        assert_eq!(sync.last_version(), Some(101));
        assert_eq!(
            sync.apply(update(false, 101)).unwrap(),
            HtxBookOutcome::Stale
        );
        assert_eq!(
            sync.apply(update(false, 105)).unwrap(),
            HtxBookOutcome::Resnapshot
        );
        assert!(sync.awaiting_snapshot());
        let HtxBookOutcome::Publish(L2Book::Snapshot(_)) = sync.apply(update(true, 200)).unwrap()
        else {
            panic!("expected replacement snapshot");
        };
    }

    #[test]
    fn bounds_the_in_session_resnapshot_budget() {
        let mut sync = HtxBookSync::new(perp(), HtxBookMode::SwapIncremental);
        let mut version = 1u64;
        sync.apply(update(true, version)).unwrap();
        for _ in 0..MAX_HTX_RESNAPSHOTS {
            let gap = version + 10;
            assert_eq!(
                sync.apply(update(false, gap)).unwrap(),
                HtxBookOutcome::Resnapshot
            );
            version += 1_000;
            sync.apply(update(true, version)).unwrap();
        }
        assert!(sync.apply(update(false, version + 10)).is_err());
    }
}
