use cryptofeed_core::error::{Error, Result};
use cryptofeed_core::symbol::Symbol;
use cryptofeed_orderbook::{L2Book, L2BookState};

#[derive(Clone, Debug)]
pub enum BybitBookAction {
    Snapshot,
    Delta,
}

#[derive(Clone, Debug)]
pub struct BybitDepthUpdate {
    pub action: BybitBookAction,
    pub update_id: u64,
    pub seq: Option<u64>,
    pub book: L2Book,
}

#[derive(Clone, Debug)]
pub struct BybitBookSync {
    state: L2BookState,
    update_id: Option<u64>,
    seq: Option<u64>,
}

impl BybitBookSync {
    pub fn new(symbol: Symbol) -> Self {
        Self {
            state: L2BookState::new(symbol),
            update_id: None,
            seq: None,
        }
    }

    pub fn apply(&mut self, update: BybitDepthUpdate) -> Result<Option<L2Book>> {
        match update.action {
            BybitBookAction::Snapshot => {
                self.update_id = Some(update.update_id);
                self.seq = update.seq;
                self.state.apply(update.book.clone());
                Ok(Some(update.book))
            }
            BybitBookAction::Delta => {
                let current = self
                    .update_id
                    .ok_or_else(|| Error::Parse("bybit book sync not initialized".to_owned()))?;

                if update.update_id <= current {
                    return Ok(None);
                }

                self.update_id = Some(update.update_id);
                self.seq = update.seq;
                self.state.apply(update.book.clone());
                Ok(Some(update.book))
            }
        }
    }

    pub fn state(&self) -> &L2BookState {
        &self.state
    }

    pub fn update_id(&self) -> Option<u64> {
        self.update_id
    }
}

#[cfg(test)]
mod tests {
    use super::{BybitBookAction, BybitBookSync, BybitDepthUpdate};
    use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
    use cryptofeed_orderbook::{L2Book, L2BookDelta, PriceLevel};
    use rust_decimal::Decimal;

    fn level(price: &str, amount: &str) -> PriceLevel {
        PriceLevel {
            price: Decimal::from_str_exact(price).unwrap(),
            amount: Decimal::from_str_exact(amount).unwrap(),
        }
    }

    fn update(action: BybitBookAction, update_id: u64, amount: &str) -> BybitDepthUpdate {
        BybitDepthUpdate {
            action,
            update_id,
            seq: Some(update_id + 1000),
            book: L2Book::Delta(L2BookDelta {
                exchange: ExchangeId::Bybit,
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
        let mut sync = BybitBookSync::new(Symbol::spot("btc", "usdt"));
        sync.apply(update(BybitBookAction::Snapshot, 100, "1.25"))
            .expect("snapshot");

        assert_eq!(sync.update_id(), Some(100));
        assert_eq!(sync.state().bids().len(), 1);
    }

    #[test]
    fn delta_updates_state() {
        let mut sync = BybitBookSync::new(Symbol::spot("btc", "usdt"));
        sync.apply(update(BybitBookAction::Snapshot, 100, "1.25"))
            .expect("snapshot");
        sync.apply(update(BybitBookAction::Delta, 101, "0"))
            .expect("delta");

        assert_eq!(sync.update_id(), Some(101));
        assert_eq!(sync.state().bids().len(), 0);
    }
}
