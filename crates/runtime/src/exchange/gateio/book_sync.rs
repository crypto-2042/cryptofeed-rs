use cryptofeed_core::error::{Error, Result};
use cryptofeed_core::symbol::Symbol;
use cryptofeed_orderbook::{L2Book, L2BookDelta, L2BookSnapshot, L2BookState};

#[derive(Clone, Debug)]
pub struct GateioDepthDelta {
    pub first_update_id: u64,
    pub last_update_id: u64,
    pub book: L2BookDelta,
}

#[derive(Clone, Debug)]
pub struct GateioBookSync {
    state: L2BookState,
    last_update_id: Option<u64>,
}

impl GateioBookSync {
    pub fn new(symbol: Symbol) -> Self {
        Self {
            state: L2BookState::new(symbol),
            last_update_id: None,
        }
    }

    pub fn bootstrap(
        &mut self,
        snapshot_last_update_id: u64,
        snapshot: L2BookSnapshot,
        buffered: Vec<GateioDepthDelta>,
    ) -> Result<()> {
        self.state.apply(L2Book::Snapshot(snapshot));
        self.last_update_id = Some(snapshot_last_update_id);

        for delta in buffered {
            let _ = self.apply_delta(delta)?;
        }

        Ok(())
    }

    pub fn apply_delta(&mut self, delta: GateioDepthDelta) -> Result<Option<L2Book>> {
        let current = self
            .last_update_id
            .ok_or_else(|| Error::Parse("gateio book sync not initialized".to_owned()))?;

        if delta.last_update_id <= current {
            return Ok(None);
        }

        let expected_next = current + 1;
        if delta.first_update_id <= expected_next && delta.last_update_id >= expected_next {
            self.last_update_id = Some(delta.last_update_id);
            let book = L2Book::Delta(delta.book);
            self.state.apply(book.clone());
            return Ok(Some(book));
        }

        Err(Error::Parse(
            "gateio initial delta does not bridge snapshot".to_owned(),
        ))
    }

    pub fn apply_next_delta(&mut self, delta: GateioDepthDelta) -> Result<Option<L2Book>> {
        let current = self
            .last_update_id
            .ok_or_else(|| Error::Parse("gateio book sync not initialized".to_owned()))?;

        if delta.last_update_id <= current {
            return Ok(None);
        }

        if delta.first_update_id != current + 1 {
            return Err(Error::Parse("gateio book sequence gap detected".to_owned()));
        }

        self.last_update_id = Some(delta.last_update_id);
        let book = L2Book::Delta(delta.book);
        self.state.apply(book.clone());
        Ok(Some(book))
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
    use cryptofeed_core::{error::Error, exchange::ExchangeId, symbol::Symbol};
    use cryptofeed_orderbook::{L2BookDelta, L2BookSnapshot, PriceLevel};
    use rust_decimal::Decimal;

    use super::{GateioBookSync, GateioDepthDelta};

    fn level(price: &str, amount: &str) -> PriceLevel {
        PriceLevel {
            price: Decimal::from_str_exact(price).unwrap(),
            amount: Decimal::from_str_exact(amount).unwrap(),
        }
    }

    fn snapshot() -> L2BookSnapshot {
        L2BookSnapshot {
            exchange: ExchangeId::Gateio,
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
    ) -> GateioDepthDelta {
        GateioDepthDelta {
            first_update_id: first,
            last_update_id: last,
            book: L2BookDelta {
                exchange: ExchangeId::Gateio,
                symbol: Symbol::spot("btc", "usdt"),
                bids,
                asks,
                exchange_ts: 3.0,
                received_ts: 4.0,
            },
        }
    }

    #[test]
    fn bootstraps_with_delta_that_bridges_snapshot_id() {
        let mut sync = GateioBookSync::new(Symbol::spot("btc", "usdt"));
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
    fn ignores_delta_older_than_snapshot_id() {
        let mut sync = GateioBookSync::new(Symbol::spot("btc", "usdt"));
        sync.bootstrap(100, snapshot(), vec![]).expect("bootstrap");

        let applied = sync
            .apply_delta(delta(90, 100, vec![level("64998.50", "2.00")], vec![]))
            .expect("stale delta");

        assert!(applied.is_none());
        assert_eq!(sync.state().bids().len(), 1);
    }

    #[test]
    fn detects_gap_after_bootstrap() {
        let mut sync = GateioBookSync::new(Symbol::spot("btc", "usdt"));
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
