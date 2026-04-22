use cryptofeed_core::error::{Error, Result};
use cryptofeed_core::symbol::Symbol;
use cryptofeed_orderbook::{L2Book, L2BookState};

#[derive(Clone, Debug)]
pub enum OkxBookAction {
    Snapshot,
    Update,
}

#[derive(Clone, Debug)]
pub struct OkxDepthUpdate {
    pub action: OkxBookAction,
    pub seq_id: i64,
    pub prev_seq_id: i64,
    pub book: L2Book,
}

#[derive(Clone, Debug)]
pub struct OkxBookSync {
    state: L2BookState,
    seq_id: Option<i64>,
}

impl OkxBookSync {
    pub fn new(symbol: Symbol) -> Self {
        Self {
            state: L2BookState::new(symbol),
            seq_id: None,
        }
    }

    pub fn apply(&mut self, update: OkxDepthUpdate) -> Result<Option<L2Book>> {
        match update.action {
            OkxBookAction::Snapshot => {
                self.seq_id = Some(update.seq_id);
                self.state.apply(update.book.clone());
                Ok(Some(update.book))
            }
            OkxBookAction::Update => {
                let current = self
                    .seq_id
                    .ok_or_else(|| Error::Parse("okx book sync not initialized".to_owned()))?;

                if update.seq_id == current && update.prev_seq_id == current {
                    return Ok(None);
                }

                if update.prev_seq_id != current {
                    return Err(Error::Parse("okx book sequence gap detected".to_owned()));
                }

                self.seq_id = Some(update.seq_id);
                self.state.apply(update.book.clone());
                Ok(Some(update.book))
            }
        }
    }

    pub fn state(&self) -> &L2BookState {
        &self.state
    }

    pub fn seq_id(&self) -> Option<i64> {
        self.seq_id
    }
}

#[cfg(test)]
mod tests {
    use super::{OkxBookAction, OkxBookSync, OkxDepthUpdate};
    use cryptofeed_core::{error::Error, exchange::ExchangeId, symbol::Symbol};
    use cryptofeed_orderbook::{L2Book, L2BookDelta, PriceLevel};
    use rust_decimal::Decimal;

    fn level(price: &str, amount: &str) -> PriceLevel {
        PriceLevel {
            price: Decimal::from_str_exact(price).unwrap(),
            amount: Decimal::from_str_exact(amount).unwrap(),
        }
    }

    fn update(
        action: OkxBookAction,
        seq_id: i64,
        prev_seq_id: i64,
        amount: &str,
    ) -> OkxDepthUpdate {
        OkxDepthUpdate {
            action,
            seq_id,
            prev_seq_id,
            book: L2Book::Delta(L2BookDelta {
                exchange: ExchangeId::Okx,
                symbol: Symbol::spot("btc", "usdt"),
                bids: vec![level("64999.10", amount)],
                asks: vec![level("65000.20", "0.75")],
                exchange_ts: 1.0,
                received_ts: 2.0,
            }),
        }
    }

    #[test]
    fn snapshot_initializes_sync() {
        let mut sync = OkxBookSync::new(Symbol::spot("btc", "usdt"));
        sync.apply(update(OkxBookAction::Snapshot, 100, -1, "1.25"))
            .expect("snapshot");

        assert_eq!(sync.seq_id(), Some(100));
        assert_eq!(sync.state().bids().len(), 1);
    }

    #[test]
    fn update_applies_when_prev_seq_matches() {
        let mut sync = OkxBookSync::new(Symbol::spot("btc", "usdt"));
        sync.apply(update(OkxBookAction::Snapshot, 100, -1, "1.25"))
            .expect("snapshot");
        sync.apply(update(OkxBookAction::Update, 101, 100, "0"))
            .expect("update");

        assert_eq!(sync.seq_id(), Some(101));
        assert_eq!(sync.state().bids().len(), 0);
    }

    #[test]
    fn update_detects_gap() {
        let mut sync = OkxBookSync::new(Symbol::spot("btc", "usdt"));
        sync.apply(update(OkxBookAction::Snapshot, 100, -1, "1.25"))
            .expect("snapshot");
        let err = sync
            .apply(update(OkxBookAction::Update, 102, 999, "1.25"))
            .expect_err("gap");

        match err {
            Error::Parse(message) => assert!(message.contains("sequence gap")),
            _ => panic!("unexpected error variant"),
        }
    }
}
