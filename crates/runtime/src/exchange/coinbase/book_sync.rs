//! Coinbase Advanced Trade `level2` snapshot/update bridging.
//!
//! The documented contract is a per-product snapshot followed by absolute-size
//! updates (`new_quantity` is the updated size, not a delta; `0` removes the
//! level). There is no per-product sequence number and no checksum, so the sync
//! state machine enforces only the documented ordering and shape rules:
//!
//! - the first event for a product must be a snapshot;
//! - an update before the snapshot is an out-of-order frame and fails the
//!   session;
//! - a row with an unknown side or an unparsable price/quantity is rejected by
//!   the parser before reaching this module;
//! - reconnects clear the local state, and recovery is the documented
//!   reconnect + fresh snapshot (no in-session snapshot request exists).

use cryptofeed_core::error::{Error, Result};
use cryptofeed_core::exchange::ExchangeId;
use cryptofeed_core::symbol::Symbol;
use cryptofeed_orderbook::{L2Book, L2BookDelta, L2BookSnapshot, L2BookState};

use super::parser::BookUpdate;

#[derive(Clone, Debug, PartialEq)]
pub enum CoinbaseBookOutcome {
    Publish(L2Book),
}

pub struct CoinbaseBookSync {
    symbol: Symbol,
    state: L2BookState,
    awaiting_snapshot: bool,
}

impl CoinbaseBookSync {
    pub fn new(symbol: Symbol) -> Self {
        Self {
            state: L2BookState::new(symbol.clone()),
            symbol,
            awaiting_snapshot: true,
        }
    }

    pub fn state(&self) -> &L2BookState {
        &self.state
    }

    pub fn awaiting_snapshot(&self) -> bool {
        self.awaiting_snapshot
    }

    pub fn apply(&mut self, update: BookUpdate) -> Result<CoinbaseBookOutcome> {
        if update.snapshot {
            let snapshot = L2BookSnapshot {
                exchange: ExchangeId::Coinbase,
                symbol: update.symbol.clone(),
                bids: update.bids,
                asks: update.asks,
                exchange_ts: update.exchange_ts,
                received_ts: update.received_ts,
            };
            self.state.apply(L2Book::Snapshot(snapshot.clone()));
            self.awaiting_snapshot = false;
            return Ok(CoinbaseBookOutcome::Publish(L2Book::Snapshot(snapshot)));
        }

        if self.awaiting_snapshot {
            return Err(Error::Parse(format!(
                "Coinbase {} level2 update received before the snapshot",
                self.symbol.as_str()
            )));
        }

        let delta = L2BookDelta {
            exchange: ExchangeId::Coinbase,
            symbol: update.symbol,
            bids: update.bids,
            asks: update.asks,
            exchange_ts: update.exchange_ts,
            received_ts: update.received_ts,
        };
        self.state.apply(L2Book::Delta(delta.clone()));
        Ok(CoinbaseBookOutcome::Publish(L2Book::Delta(delta)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cryptofeed_orderbook::PriceLevel;
    use rust_decimal::Decimal;

    fn symbol() -> Symbol {
        Symbol::spot("BTC", "USD")
    }

    fn level(price: &str, amount: &str) -> PriceLevel {
        PriceLevel {
            price: Decimal::from_str_exact(price).unwrap(),
            amount: Decimal::from_str_exact(amount).unwrap(),
        }
    }

    fn update(snapshot: bool, bids: Vec<PriceLevel>, asks: Vec<PriceLevel>) -> BookUpdate {
        BookUpdate {
            snapshot,
            symbol: symbol(),
            bids,
            asks,
            exchange_ts: 1.0,
            received_ts: 2.0,
        }
    }

    #[test]
    fn publishes_documented_snapshot_then_absolute_updates() {
        let mut sync = CoinbaseBookSync::new(symbol());
        assert!(sync.awaiting_snapshot());
        let CoinbaseBookOutcome::Publish(L2Book::Snapshot(snapshot)) = sync
            .apply(update(
                true,
                vec![level("100", "1.5")],
                vec![level("101", "2.5")],
            ))
            .unwrap()
        else {
            panic!("expected snapshot");
        };
        assert_eq!(snapshot.bids[0].amount, Decimal::new(15, 1));
        assert!(!sync.awaiting_snapshot());

        let CoinbaseBookOutcome::Publish(L2Book::Delta(delta)) = sync
            .apply(update(
                false,
                vec![level("100", "0")],
                vec![level("101", "3")],
            ))
            .unwrap()
        else {
            panic!("expected delta");
        };
        assert_eq!(delta.bids[0].amount, Decimal::ZERO);
        assert_eq!(sync.state().bids().len(), 0);
        assert_eq!(sync.state().asks()[0].amount, Decimal::from(3));
    }

    #[test]
    fn rejects_update_before_snapshot_and_accepts_replacement_snapshot() {
        let mut sync = CoinbaseBookSync::new(symbol());
        assert!(matches!(
            sync.apply(update(false, vec![level("100", "1")], Vec::new())),
            Err(Error::Parse(_))
        ));
        assert!(sync.awaiting_snapshot());
        sync.apply(update(true, vec![level("99", "1")], Vec::new()))
            .expect("snapshot after rejected update");
        assert!(!sync.awaiting_snapshot());

        // A replacement snapshot resets the book instead of merging.
        let CoinbaseBookOutcome::Publish(L2Book::Snapshot(snapshot)) = sync
            .apply(update(
                true,
                vec![level("98", "2")],
                vec![level("100", "1")],
            ))
            .unwrap()
        else {
            panic!("expected replacement snapshot");
        };
        assert_eq!(snapshot.bids.len(), 1);
        assert_eq!(sync.state().bids()[0].price, Decimal::from(98));
    }
}
