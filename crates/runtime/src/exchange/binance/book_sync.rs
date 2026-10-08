use super::adapter::BinanceProduct;
use cryptofeed_core::error::{Error, Result};
use cryptofeed_core::symbol::Symbol;
use cryptofeed_orderbook::{L2Book, L2BookDelta, L2BookSnapshot, L2BookState};

#[derive(Clone, Debug)]
pub struct BinanceDepthDelta {
    pub first_update_id: u64,
    pub last_update_id: u64,
    pub book: L2BookDelta,
}

#[derive(Clone, Debug)]
pub struct BinanceSequencedDepthDelta {
    pub delta: BinanceDepthDelta,
    pub previous_update_id: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct BinanceBookSync {
    state: L2BookState,
    last_update_id: Option<u64>,
    product: BinanceProduct,
    /// Partial-depth mode (`@depth5/10/20`): every push carries the complete
    /// top-N book, so each applied push REPLACES the local book instead of
    /// merging levels. Spot partial ids jump arbitrarily between pushes
    /// (monotonicity is the only requirement); USD-M/CoinM partial pushes
    /// keep consecutive `U`/`pu` sequence checks.
    partial: bool,
}

impl BinanceBookSync {
    pub fn new(symbol: Symbol) -> Self {
        Self::new_for_product(symbol, BinanceProduct::Spot)
    }

    pub fn new_for_product(symbol: Symbol, product: BinanceProduct) -> Self {
        Self {
            state: L2BookState::new(symbol),
            last_update_id: None,
            product,
            partial: false,
        }
    }

    pub fn new_for_partial_depth(symbol: Symbol, product: BinanceProduct) -> Self {
        Self {
            state: L2BookState::new(symbol),
            last_update_id: None,
            product,
            partial: true,
        }
    }

    pub fn bootstrap(
        &mut self,
        snapshot_last_update_id: u64,
        snapshot: L2BookSnapshot,
        buffered: Vec<BinanceDepthDelta>,
    ) -> Result<()> {
        let _ = self.bootstrap_events(snapshot_last_update_id, snapshot, buffered)?;
        Ok(())
    }

    pub fn bootstrap_events(
        &mut self,
        snapshot_last_update_id: u64,
        snapshot: L2BookSnapshot,
        buffered: Vec<BinanceDepthDelta>,
    ) -> Result<Vec<L2Book>> {
        self.bootstrap_sequenced_events(
            snapshot_last_update_id,
            snapshot,
            buffered
                .into_iter()
                .map(|delta| BinanceSequencedDepthDelta {
                    delta,
                    previous_update_id: None,
                })
                .collect(),
        )
    }

    pub fn bootstrap_sequenced_events(
        &mut self,
        snapshot_last_update_id: u64,
        snapshot: L2BookSnapshot,
        buffered: Vec<BinanceSequencedDepthDelta>,
    ) -> Result<Vec<L2Book>> {
        let snapshot_event = L2Book::Snapshot(snapshot);
        self.state.apply(snapshot_event.clone());
        self.last_update_id = Some(snapshot_last_update_id);
        let mut events = vec![snapshot_event];
        let mut bridged = false;

        for delta in buffered {
            let event = if bridged {
                self.apply_next_sequenced_delta(delta)?
            } else {
                let event = self.apply_sequenced_delta(delta)?;
                if event.is_some() {
                    bridged = true;
                }
                event
            };
            if let Some(event) = event {
                events.push(event);
            }
        }

        Ok(events)
    }

    pub fn apply_delta(&mut self, delta: BinanceDepthDelta) -> Result<Option<L2Book>> {
        self.apply_sequenced_delta(BinanceSequencedDepthDelta {
            delta,
            previous_update_id: None,
        })
    }

    pub fn apply_sequenced_delta(
        &mut self,
        update: BinanceSequencedDepthDelta,
    ) -> Result<Option<L2Book>> {
        let delta = update.delta;
        let current = self
            .last_update_id
            .ok_or_else(|| Error::Parse("binance book sync not initialized".to_owned()))?;

        let stale = match self.product {
            BinanceProduct::Spot | BinanceProduct::Option => delta.last_update_id <= current,
            BinanceProduct::UsdM | BinanceProduct::CoinM => delta.last_update_id < current,
        };
        if stale {
            return Ok(None);
        }

        let bridge_id = match self.product {
            BinanceProduct::Spot | BinanceProduct::Option => current + 1,
            BinanceProduct::UsdM | BinanceProduct::CoinM => current,
        };
        if delta.first_update_id <= bridge_id && delta.last_update_id >= bridge_id {
            self.last_update_id = Some(delta.last_update_id);
            return Ok(Some(self.apply_book(delta)));
        }

        // Spot partial-depth pushes are complete top-N books whose ids jump
        // arbitrarily between pushes — there is no bridge requirement, only
        // monotonicity (checked above as the stale rule).
        if self.partial && self.product == BinanceProduct::Spot {
            self.last_update_id = Some(delta.last_update_id);
            return Ok(Some(self.apply_book(delta)));
        }

        Err(Error::Parse(
            "binance initial delta does not bridge snapshot".to_owned(),
        ))
    }

    pub fn apply_next_delta(&mut self, delta: BinanceDepthDelta) -> Result<Option<L2Book>> {
        self.apply_next_sequenced_delta(BinanceSequencedDepthDelta {
            delta,
            previous_update_id: None,
        })
    }

    pub fn apply_next_sequenced_delta(
        &mut self,
        update: BinanceSequencedDepthDelta,
    ) -> Result<Option<L2Book>> {
        let BinanceSequencedDepthDelta {
            delta,
            previous_update_id,
        } = update;
        let current = self
            .last_update_id
            .ok_or_else(|| Error::Parse("binance book sync not initialized".to_owned()))?;

        if delta.last_update_id <= current {
            return Ok(None);
        }

        // Spot partial-depth pushes are complete top-N books; ids are not
        // consecutive, so only monotonicity applies.
        if self.partial && self.product == BinanceProduct::Spot {
            self.last_update_id = Some(delta.last_update_id);
            return Ok(Some(self.apply_book(delta)));
        }

        let continuous = match self.product {
            BinanceProduct::Spot | BinanceProduct::Option => delta.first_update_id == current + 1,
            BinanceProduct::UsdM | BinanceProduct::CoinM => previous_update_id == Some(current),
        };
        if !continuous {
            return Err(Error::Parse(
                "binance book sequence gap detected".to_owned(),
            ));
        }

        self.last_update_id = Some(delta.last_update_id);
        Ok(Some(self.apply_book(delta)))
    }

    /// Apply a delta to the local book. Partial-depth pushes replace the
    /// whole top-N book (every push is complete); full-depth deltas merge.
    fn apply_book(&mut self, delta: BinanceDepthDelta) -> L2Book {
        if self.partial {
            let snapshot = L2BookSnapshot {
                exchange: delta.book.exchange,
                symbol: delta.book.symbol,
                bids: delta.book.bids,
                asks: delta.book.asks,
                exchange_ts: delta.book.exchange_ts,
                received_ts: delta.book.received_ts,
            };
            let book = L2Book::Snapshot(snapshot);
            self.state.apply(book.clone());
            book
        } else {
            let book = L2Book::Delta(delta.book);
            self.state.apply(book.clone());
            book
        }
    }

    pub fn state(&self) -> &L2BookState {
        &self.state
    }

    pub fn last_update_id(&self) -> Option<u64> {
        self.last_update_id
    }
}

#[cfg(test)]
mod tests {
    use super::{BinanceBookSync, BinanceDepthDelta, BinanceSequencedDepthDelta};
    use crate::exchange::binance::adapter::BinanceProduct;
    use cryptofeed_core::{error::Error, exchange::ExchangeId, symbol::Symbol};
    use cryptofeed_orderbook::{L2BookDelta, L2BookSnapshot, PriceLevel};
    use rust_decimal::Decimal;

    fn level(price: &str, amount: &str) -> PriceLevel {
        PriceLevel {
            price: Decimal::from_str_exact(price).unwrap(),
            amount: Decimal::from_str_exact(amount).unwrap(),
        }
    }

    fn snapshot() -> L2BookSnapshot {
        L2BookSnapshot {
            exchange: ExchangeId::Binance,
            symbol: Symbol::spot("btc", "usdt"),
            bids: vec![level("64999.10", "1.25")],
            asks: vec![level("65000.20", "0.75")],
            exchange_ts: 1.0,
            received_ts: 2.0,
        }
    }

    fn delta(
        first: u64,
        last: u64,
        bids: Vec<PriceLevel>,
        asks: Vec<PriceLevel>,
    ) -> BinanceDepthDelta {
        BinanceDepthDelta {
            first_update_id: first,
            last_update_id: last,
            book: L2BookDelta {
                exchange: ExchangeId::Binance,
                symbol: Symbol::spot("btc", "usdt"),
                bids,
                asks,
                exchange_ts: 3.0,
                received_ts: 4.0,
            },
        }
    }

    fn futures_delta(
        first: u64,
        last: u64,
        previous: u64,
        bids: Vec<PriceLevel>,
    ) -> BinanceSequencedDepthDelta {
        BinanceSequencedDepthDelta {
            delta: delta(first, last, bids, vec![]),
            previous_update_id: Some(previous),
        }
    }

    #[test]
    fn bootstraps_with_bridging_delta() {
        let mut sync = BinanceBookSync::new(Symbol::spot("btc", "usdt"));
        sync.bootstrap(
            100,
            snapshot(),
            vec![delta(
                100,
                101,
                vec![level("64999.10", "0")],
                vec![level("65000.20", "1.00")],
            )],
        )
        .expect("bootstrap");

        assert_eq!(sync.last_update_id(), Some(101));
        assert_eq!(sync.state().bids().len(), 0);
        assert_eq!(
            sync.state().asks()[0].amount,
            Decimal::from_str_exact("1.00").unwrap()
        );
    }

    #[test]
    fn bootstrap_events_reproduce_internal_state() {
        let mut sync = BinanceBookSync::new(Symbol::spot("btc", "usdt"));
        let events = sync
            .bootstrap_events(
                100,
                snapshot(),
                vec![delta(100, 101, vec![level("64999.10", "0")], vec![])],
            )
            .expect("bootstrap");

        assert!(matches!(
            events[0],
            cryptofeed_orderbook::L2Book::Snapshot(_)
        ));
        assert!(matches!(events[1], cryptofeed_orderbook::L2Book::Delta(_)));
        let mut replay = cryptofeed_orderbook::L2BookState::new(Symbol::spot("btc", "usdt"));
        for event in events {
            replay.apply(event);
        }
        assert_eq!(replay.bids().len(), sync.state().bids().len());
        assert_eq!(replay.asks().len(), sync.state().asks().len());
    }

    #[test]
    fn futures_continuity_uses_previous_final_update_id() {
        let mut sync = BinanceBookSync::new_for_product(
            Symbol::perpetual("btc", "usd"),
            BinanceProduct::CoinM,
        );
        sync.bootstrap_sequenced_events(
            100,
            L2BookSnapshot {
                symbol: Symbol::perpetual("btc", "usd"),
                ..snapshot()
            },
            vec![futures_delta(100, 101, 99, vec![])],
        )
        .expect("bootstrap");

        sync.apply_next_sequenced_delta(futures_delta(102, 105, 101, vec![]))
            .expect("continuous");
        let error = sync
            .apply_next_sequenced_delta(futures_delta(106, 108, 103, vec![]))
            .expect_err("pu gap");
        assert!(error.to_string().contains("sequence gap"));
    }

    #[test]
    fn partial_depth_pushes_replace_the_book() {
        let mut sync = BinanceBookSync::new_for_partial_depth(
            Symbol::spot("btc", "usdt"),
            BinanceProduct::Spot,
        );
        sync.bootstrap(
            100,
            snapshot(),
            vec![delta(
                157,
                157,
                vec![level("64998.50", "2.00")],
                vec![level("65001.00", "0.90")],
            )],
        )
        .expect("bootstrap");

        // The buffered push jumped from 100 to 157 — no bridge required for
        // spot partial depth — and replaced the book.
        assert_eq!(sync.last_update_id(), Some(157));
        assert_eq!(sync.state().bids().len(), 1);
        assert_eq!(
            sync.state().bids()[0].price,
            Decimal::from_str_exact("64998.50").unwrap()
        );

        // A subsequent push with a higher id replaces again.
        let event = sync
            .apply_next_delta(delta(
                213,
                213,
                vec![level("64997.00", "3.50")],
                vec![level("65000.00", "1.10")],
            ))
            .expect("apply partial push");
        assert!(matches!(
            event,
            Some(cryptofeed_orderbook::L2Book::Snapshot(_))
        ));
        assert_eq!(sync.last_update_id(), Some(213));
        assert_eq!(sync.state().asks().len(), 1);
        assert_eq!(
            sync.state().asks()[0].price,
            Decimal::from_str_exact("65000.00").unwrap()
        );
    }

    #[test]
    fn partial_depth_stale_and_out_of_order_pushes_are_ignored() {
        let mut sync = BinanceBookSync::new_for_partial_depth(
            Symbol::spot("btc", "usdt"),
            BinanceProduct::Spot,
        );
        sync.bootstrap(100, snapshot(), vec![]).expect("bootstrap");

        // Stale push (id <= snapshot id) is dropped, not an error.
        let applied = sync
            .apply_next_delta(delta(90, 90, vec![level("1.00", "0")], vec![]))
            .expect("stale push");
        assert!(applied.is_none());

        // Out-of-order push older than the applied id is dropped too.
        sync.apply_next_delta(delta(150, 150, vec![level("64998.50", "2.00")], vec![]))
            .expect("newer push");
        let applied = sync
            .apply_next_delta(delta(140, 140, vec![level("1.00", "0")], vec![]))
            .expect("out-of-order push");
        assert!(applied.is_none());
        assert_eq!(sync.last_update_id(), Some(150));
    }

    #[test]
    fn ignores_stale_delta() {
        let mut sync = BinanceBookSync::new(Symbol::spot("btc", "usdt"));
        sync.bootstrap(100, snapshot(), vec![]).expect("bootstrap");

        let applied = sync
            .apply_delta(delta(90, 100, vec![level("64998.50", "2.00")], vec![]))
            .expect("stale delta");

        assert!(applied.is_none());
        assert_eq!(sync.state().bids().len(), 1);
    }

    #[test]
    fn detects_gap_on_subsequent_delta() {
        let mut sync = BinanceBookSync::new(Symbol::spot("btc", "usdt"));
        sync.bootstrap(
            100,
            snapshot(),
            vec![delta(100, 101, vec![level("64999.10", "0")], vec![])],
        )
        .expect("bootstrap");

        let err = sync
            .apply_next_delta(delta(103, 104, vec![level("64998.50", "2.00")], vec![]))
            .expect_err("gap should fail");

        match err {
            Error::Parse(message) => assert!(message.contains("sequence gap")),
            _ => panic!("unexpected error variant"),
        }
    }
}
