//! Upbit order-book snapshot handling.
//!
//! Upbit publishes complete order-book snapshots with a configurable unit count
//! (documented `1`, `5`, `15`, `30`; plain `codes` means 30). No delta, sequence
//! or checksum shape is documented for this stream, so every push replaces the
//! local book and a non-snapshot push is a protocol violation that fails closed.
//! A snapshot with unpaired sides, no levels, or more units than requested is
//! malformed: the venue cannot send more units than the subscription asked for.

use cryptofeed_core::error::{Error, Result};
use cryptofeed_core::symbol::Symbol;
use cryptofeed_orderbook::{L2Book, L2BookState};

use super::parser::DEFAULT_ORDERBOOK_UNITS;

pub struct UpbitBookSync {
    symbol: Symbol,
    units: u16,
    state: L2BookState,
    snapshots: u64,
}

impl UpbitBookSync {
    pub fn new(symbol: Symbol, units: Option<u16>) -> Self {
        Self {
            state: L2BookState::new(symbol.clone()),
            symbol,
            units: units.unwrap_or(DEFAULT_ORDERBOOK_UNITS),
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
                "upbit orderbook delta is not documented; snapshots only".to_owned(),
            ));
        };
        if snapshot.symbol != self.symbol {
            return Err(Error::Parse(
                "upbit orderbook snapshot symbol does not match its subscription".to_owned(),
            ));
        }
        if snapshot.bids.is_empty() || snapshot.asks.is_empty() {
            return Err(Error::Parse(
                "upbit orderbook snapshot is missing a side".to_owned(),
            ));
        }
        // Documented units pair one ask with one bid per array entry.
        if snapshot.bids.len() != snapshot.asks.len() {
            return Err(Error::Parse(
                "upbit orderbook snapshot has unpaired bid/ask units".to_owned(),
            ));
        }
        if snapshot.bids.len() > usize::from(self.units) {
            return Err(Error::Parse(format!(
                "upbit orderbook snapshot has {} units but {} were requested",
                snapshot.bids.len(),
                self.units
            )));
        }
        self.state.apply(book.clone());
        self.snapshots += 1;
        Ok(book)
    }
}

#[cfg(test)]
mod tests {
    use super::UpbitBookSync;
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
            exchange: ExchangeId::Upbit,
            symbol: symbol(),
            bids: vec![level(bid, amount)],
            asks: vec![level("125056000", "0.17")],
            exchange_ts: 1.0,
            received_ts: 2.0,
        })
    }

    #[test]
    fn every_snapshot_replaces_the_local_book() {
        let mut sync = UpbitBookSync::new(symbol(), None);
        sync.apply(snapshot("124743000", "0.17")).expect("first");
        assert_eq!(sync.state().bids()[0].price.to_string(), "124743000");
        sync.apply(snapshot("124332000", "0.09")).expect("second");
        assert_eq!(sync.state().bids().len(), 1);
        assert_eq!(sync.state().bids()[0].price.to_string(), "124332000");
        assert_eq!(sync.snapshots(), 2);
    }

    #[test]
    fn default_units_are_the_documented_thirty() {
        let sync = UpbitBookSync::new(symbol(), None);
        assert_eq!(sync.units, 30);
        let mut sync = UpbitBookSync::new(symbol(), Some(5));
        sync.apply(snapshot("124743000", "0.17")).expect("snapshot");
        assert_eq!(sync.units, 5);
    }

    #[test]
    fn deltas_fail_closed() {
        let mut sync = UpbitBookSync::new(symbol(), None);
        let delta = L2Book::Delta(L2BookDelta {
            exchange: ExchangeId::Upbit,
            symbol: symbol(),
            bids: vec![level("124743000", "0.17")],
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
    fn unpaired_empty_and_oversized_snapshots_fail_closed() {
        let mut sync = UpbitBookSync::new(symbol(), Some(1));
        let unpaired = L2Book::Snapshot(L2BookSnapshot {
            exchange: ExchangeId::Upbit,
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
            exchange: ExchangeId::Upbit,
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
            exchange: ExchangeId::Upbit,
            symbol: symbol(),
            bids: vec![level("1", "1"), level("2", "2")],
            asks: vec![level("3", "3"), level("4", "4")],
            exchange_ts: 1.0,
            received_ts: 2.0,
        });
        assert!(matches!(
            sync.apply(oversized),
            Err(Error::Parse(message)) if message.contains("but 1 were requested")
        ));
        assert_eq!(sync.snapshots(), 0);
    }

    #[test]
    fn foreign_symbol_snapshot_is_rejected() {
        let mut sync = UpbitBookSync::new(symbol(), None);
        let foreign = L2Book::Snapshot(L2BookSnapshot {
            exchange: ExchangeId::Upbit,
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
