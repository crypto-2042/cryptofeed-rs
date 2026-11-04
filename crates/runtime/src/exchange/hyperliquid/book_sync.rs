//! Hyperliquid `l2Book` whole-book snapshot bridging.
//!
//! The documented `WsBook` push is a complete replacement snapshot: `levels`
//! is `[bids, asks]` with at most 20 levels per side (5 when the subscription
//! requests the documented `fast` flag), pushed on each block that is at least
//! 0.5 seconds after the previous push. There is no sequence number, checksum
//! or delta form, so this module:
//!
//! - applies every push as an authoritative replacement, never as a delta;
//! - does not invent an ordering anchor, dedupe version or staleness rule;
//! - clears local state on reconnect (the caller resets the feed's books) and
//!   treats the next push as a fresh snapshot.

use cryptofeed_core::error::Result;
use cryptofeed_core::exchange::ExchangeId;
use cryptofeed_core::symbol::Symbol;
use cryptofeed_orderbook::{L2Book, L2BookSnapshot, L2BookState};

use super::parser::BookUpdate;

#[derive(Clone, Debug, PartialEq)]
pub enum HyperliquidBookOutcome {
    Publish(L2Book),
}

pub struct HyperliquidBookSync {
    state: L2BookState,
}

impl HyperliquidBookSync {
    pub fn new(symbol: Symbol) -> Self {
        Self {
            state: L2BookState::new(symbol),
        }
    }

    pub fn state(&self) -> &L2BookState {
        &self.state
    }

    pub fn apply(&mut self, update: BookUpdate) -> Result<HyperliquidBookOutcome> {
        let snapshot = L2BookSnapshot {
            exchange: ExchangeId::Hyperliquid,
            symbol: update.symbol,
            bids: update.bids,
            asks: update.asks,
            exchange_ts: update.exchange_ts,
            received_ts: update.received_ts,
        };
        self.state.apply(L2Book::Snapshot(snapshot.clone()));
        Ok(HyperliquidBookOutcome::Publish(L2Book::Snapshot(snapshot)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cryptofeed_orderbook::PriceLevel;
    use rust_decimal::Decimal;

    fn perp() -> Symbol {
        Symbol::perpetual("BTC", "USD")
    }

    fn level(price: &str, amount: &str) -> PriceLevel {
        PriceLevel {
            price: Decimal::from_str_exact(price).unwrap(),
            amount: Decimal::from_str_exact(amount).unwrap(),
        }
    }

    fn update(bids: Vec<PriceLevel>, asks: Vec<PriceLevel>, exchange_ts: f64) -> BookUpdate {
        BookUpdate {
            symbol: perp(),
            bids,
            asks,
            exchange_ts,
            received_ts: 2.0,
        }
    }

    #[test]
    fn every_push_replaces_the_whole_book_without_an_invented_sequence() {
        let mut sync = HyperliquidBookSync::new(perp());
        let HyperliquidBookOutcome::Publish(L2Book::Snapshot(book)) = sync
            .apply(update(
                vec![level("100", "2"), level("99", "3")],
                vec![level("101", "4")],
                1.0,
            ))
            .unwrap()
        else {
            panic!("expected snapshot");
        };
        assert_eq!(book.bids.len(), 2);
        assert_eq!(book.exchange_ts, 1.0);
        // A later replacement snapshot drops the levels it no longer carries;
        // no stale level from the previous push is retained.
        let HyperliquidBookOutcome::Publish(L2Book::Snapshot(book)) = sync
            .apply(update(vec![level("98", "5")], vec![level("102", "6")], 1.5))
            .unwrap()
        else {
            panic!("expected replacement snapshot");
        };
        assert_eq!(book.bids.len(), 1);
        assert_eq!(book.bids[0].price, Decimal::from(98));
        assert_eq!(sync.state().bids().len(), 1);
        assert_eq!(sync.state().asks()[0].amount, Decimal::from(6));
    }

    #[test]
    fn empty_sides_are_published_as_documented_snapshots() {
        let mut sync = HyperliquidBookSync::new(perp());
        let HyperliquidBookOutcome::Publish(L2Book::Snapshot(book)) = sync
            .apply(update(Vec::new(), vec![level("101", "4")], 1.0))
            .unwrap()
        else {
            panic!("expected snapshot");
        };
        assert!(book.bids.is_empty());
        assert_eq!(book.asks.len(), 1);
    }
}
