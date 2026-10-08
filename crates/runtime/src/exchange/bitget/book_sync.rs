use cryptofeed_core::error::{Error, Result};
use cryptofeed_core::symbol::Symbol;
use cryptofeed_orderbook::{L2Book, L2BookState};

#[derive(Clone, Debug)]
pub enum BitgetBookAction {
    Snapshot,
    Update,
}

#[derive(Clone, Debug)]
pub struct BitgetDepthUpdate {
    pub action: BitgetBookAction,
    pub seq: u64,
    pub pseq: u64,
    pub book: L2Book,
}

#[derive(Clone, Debug)]
pub struct BitgetBookSync {
    state: L2BookState,
    seq: Option<u64>,
    awaiting_first_update: bool,
}

impl BitgetBookSync {
    pub fn new(symbol: Symbol) -> Self {
        Self {
            state: L2BookState::new(symbol),
            seq: None,
            awaiting_first_update: false,
        }
    }

    pub fn apply(&mut self, update: BitgetDepthUpdate) -> Result<Option<L2Book>> {
        match update.action {
            BitgetBookAction::Snapshot => {
                self.seq = Some(update.seq);
                self.awaiting_first_update = true;
                self.state.apply(update.book.clone());
                Ok(Some(update.book))
            }
            BitgetBookAction::Update => {
                let current = self
                    .seq
                    .ok_or_else(|| Error::Parse("bitget book sync not initialized".to_owned()))?;

                let continuous = if self.awaiting_first_update {
                    update.pseq <= current && current <= update.seq
                } else {
                    update.pseq == current
                };
                if !continuous {
                    return Err(Error::Parse("bitget book sequence gap detected".to_owned()));
                }

                self.seq = Some(update.seq);
                self.awaiting_first_update = false;
                self.state.apply(update.book.clone());
                Ok(Some(update.book))
            }
        }
    }

    pub fn state(&self) -> &L2BookState {
        &self.state
    }

    pub fn seq(&self) -> Option<u64> {
        self.seq
    }
}

#[cfg(test)]
mod tests {
    use super::{BitgetBookAction, BitgetBookSync, BitgetDepthUpdate};
    use cryptofeed_core::{error::Error, exchange::ExchangeId, symbol::Symbol};
    use cryptofeed_orderbook::{L2Book, L2BookDelta, PriceLevel};
    use rust_decimal::Decimal;

    fn level(price: &str, amount: &str) -> PriceLevel {
        PriceLevel {
            price: Decimal::from_str_exact(price).unwrap(),
            amount: Decimal::from_str_exact(amount).unwrap(),
        }
    }

    fn delta(seq: u64, pseq: u64, action: BitgetBookAction) -> BitgetDepthUpdate {
        BitgetDepthUpdate {
            action,
            seq,
            pseq,
            book: L2Book::Delta(L2BookDelta {
                exchange: ExchangeId::Bitget,
                symbol: Symbol::spot("btc", "usdt"),
                bids: vec![level("64999.10", "1.25")],
                asks: vec![level("65000.20", "0.75")],
                exchange_ts: 1.0,
                received_ts: 2.0,
            }),
        }
    }

    #[test]
    fn snapshot_initializes_sync() {
        let mut sync = BitgetBookSync::new(Symbol::spot("btc", "usdt"));
        let applied = sync
            .apply(delta(100, 0, BitgetBookAction::Snapshot))
            .expect("snapshot");

        assert!(applied.is_some());
        assert_eq!(sync.seq(), Some(100));
        assert_eq!(sync.state().bids().len(), 1);
    }

    #[test]
    fn update_applies_when_pseq_matches() {
        let mut sync = BitgetBookSync::new(Symbol::spot("btc", "usdt"));
        sync.apply(delta(100, 0, BitgetBookAction::Snapshot))
            .expect("snapshot");
        let applied = sync
            .apply(delta(101, 100, BitgetBookAction::Update))
            .expect("update");

        assert!(applied.is_some());
        assert_eq!(sync.seq(), Some(101));
    }

    #[test]
    fn first_update_may_bridge_snapshot_sequence() {
        let mut sync = BitgetBookSync::new(Symbol::spot("btc", "usdt"));
        sync.apply(delta(100, 0, BitgetBookAction::Snapshot))
            .expect("snapshot");

        let applied = sync
            .apply(delta(105, 95, BitgetBookAction::Update))
            .expect("first update bridges snapshot sequence");

        assert!(applied.is_some());
        assert_eq!(sync.seq(), Some(105));
    }

    #[test]
    fn update_detects_gap() {
        let mut sync = BitgetBookSync::new(Symbol::spot("btc", "usdt"));
        sync.apply(delta(100, 0, BitgetBookAction::Snapshot))
            .expect("snapshot");
        let err = sync
            .apply(delta(102, 1000, BitgetBookAction::Update))
            .expect_err("gap");

        match err {
            Error::Parse(message) => assert!(message.contains("sequence gap")),
            _ => panic!("unexpected error variant"),
        }
    }
}
