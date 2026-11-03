//! Bithumb order-book snapshot handling.
//!
//! Bithumb public v1 publishes complete order-book snapshots of at most 15 units
//! and no depth parameter. No delta, sequence or checksum shape is documented, so
//! every push replaces the local book and a non-snapshot push is a protocol
//! violation that fails closed. A snapshot with unpaired sides, no levels, or
//! more than the documented 15 units is malformed.

use cryptofeed_core::error::{Error, Result};
use cryptofeed_core::symbol::Symbol;
use cryptofeed_orderbook::{L2Book, L2BookState};

use super::parser::MAX_ORDERBOOK_UNITS;

pub struct BithumbBookSync {
    symbol: Symbol,
    state: L2BookState,
    snapshots: u64,
}

impl BithumbBookSync {
    pub fn new(symbol: Symbol) -> Self {
        Self {
            state: L2BookState::new(symbol.clone()),
            symbol,
            snapshots: 0,
        }
    }

    pub fn state(&self) -> &L2BookState {
        &self.state
    }

    pub fn snapshots(&self) -> u64 {
        self.snapshots
    }

    /// Applies one full-book push and returns the event to publish.
    pub fn apply(&mut self, book: L2Book) -> Result<L2Book> {
        let L2Book::Snapshot(snapshot) = &book else {
            return Err(Error::Parse(
                "bithumb orderbook delta is not documented; snapshots only".to_owned(),
            ));
        };
        if snapshot.symbol != self.symbol {
            return Err(Error::Parse(
                "bithumb orderbook snapshot symbol does not match its subscription".to_owned(),
            ));
        }
        if snapshot.bids.is_empty() || snapshot.asks.is_empty() {
            return Err(Error::Parse(
                "bithumb orderbook snapshot is missing a side".to_owned(),
            ));
        }
        if snapshot.bids.len() != snapshot.asks.len() {
            return Err(Error::Parse(
                "bithumb orderbook snapshot has unpaired bid/ask units".to_owned(),
            ));
        }
        if snapshot.bids.len() > usize::from(MAX_ORDERBOOK_UNITS) {
            return Err(Error::Parse(format!(
                "bithumb orderbook snapshot has {} units but the documented maximum is {MAX_ORDERBOOK_UNITS}",
                snapshot.bids.len()
            )));
        }
        self.state.apply(book.clone());
        self.snapshots += 1;
        Ok(book)
    }
}

#[cfg(test)]
mod tests {
    use super::BithumbBookSync;
    use cryptofeed_core::{error::Error, exchange::ExchangeId, symbol::Symbol};
    use cryptofeed_orderbook::{L2Book, L2BookDelta, L2BookSnapshot, PriceLevel};
    use rust_decimal::Decimal;

    fn level(price: &str, amount: &str) -> PriceLevel {
        PriceLevel {
            price: Decimal::from_str_exact(price).unwrap(),
            amount: Decimal::from_str_exact(amount).unwrap(),
        }
    }

    fn symbol() -> Symbol {
        Symbol::spot("BTC", "KRW")
    }

    fn snapshot(bid: &str, amount: &str) -> L2Book {
        L2Book::Snapshot(L2BookSnapshot {
            exchange: ExchangeId::Bithumb,
            symbol: symbol(),
            bids: vec![level(bid, amount)],
            asks: vec![level("47880000", "4.3478")],
            exchange_ts: 1.0,
            received_ts: 2.0,
        })
    }

    #[test]
    fn every_snapshot_replaces_the_local_book() {
        let mut sync = BithumbBookSync::new(symbol());
        sync.apply(snapshot("47830000", "5.637")).expect("first");
        assert_eq!(sync.state().bids()[0].price.to_string(), "47830000");
        sync.apply(snapshot("47790000", "0.9705")).expect("second");
        assert_eq!(sync.state().bids().len(), 1);
        assert_eq!(sync.state().bids()[0].price.to_string(), "47790000");
        assert_eq!(sync.snapshots(), 2);
    }

    #[test]
    fn deltas_fail_closed() {
        let mut sync = BithumbBookSync::new(symbol());
        let delta = L2Book::Delta(L2BookDelta {
            exchange: ExchangeId::Bithumb,
            symbol: symbol(),
            bids: vec![level("47830000", "5.637")],
            asks: vec![],
            exchange_ts: 1.0,
            received_ts: 2.0,
        });
        assert!(matches!(
            sync.apply(delta),
            Err(Error::Parse(message)) if message.contains("snapshots only")
        ));
        assert_eq!(sync.snapshots(), 0);
    }

    #[test]
    fn invalid_snapshots_fail_closed() {
        let mut sync = BithumbBookSync::new(symbol());
        let unpaired = L2Book::Snapshot(L2BookSnapshot {
            exchange: ExchangeId::Bithumb,
            symbol: symbol(),
            bids: vec![level("1", "1"), level("2", "2")],
            asks: vec![level("3", "3")],
            exchange_ts: 1.0,
            received_ts: 2.0,
        });
        assert!(matches!(
            sync.apply(unpaired),
            Err(Error::Parse(message)) if message.contains("unpaired")
        ));
        let empty = L2Book::Snapshot(L2BookSnapshot {
            exchange: ExchangeId::Bithumb,
            symbol: symbol(),
            bids: vec![],
            asks: vec![],
            exchange_ts: 1.0,
            received_ts: 2.0,
        });
        assert!(matches!(
            sync.apply(empty),
            Err(Error::Parse(message)) if message.contains("missing a side")
        ));
        let oversized = L2Book::Snapshot(L2BookSnapshot {
            exchange: ExchangeId::Bithumb,
            symbol: symbol(),
            bids: (0..16)
                .map(|index| level("1", &format!("{index}")))
                .collect(),
            asks: (0..16)
                .map(|index| level("2", &format!("{index}")))
                .collect(),
            exchange_ts: 1.0,
            received_ts: 2.0,
        });
        assert!(matches!(
            sync.apply(oversized),
            Err(Error::Parse(message)) if message.contains("documented maximum")
        ));
        assert_eq!(sync.snapshots(), 0);
    }

    #[test]
    fn foreign_symbol_snapshot_is_rejected() {
        let mut sync = BithumbBookSync::new(symbol());
        let foreign = L2Book::Snapshot(L2BookSnapshot {
            exchange: ExchangeId::Bithumb,
            symbol: Symbol::spot("ETH", "KRW"),
            bids: vec![level("1", "1")],
            asks: vec![level("2", "2")],
            exchange_ts: 1.0,
            received_ts: 2.0,
        });
        assert!(matches!(
            sync.apply(foreign),
            Err(Error::Parse(message)) if message.contains("does not match its subscription")
        ));
    }
}
