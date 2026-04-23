use crate::model::Trade;
use async_trait::async_trait;

/// Handles normalized trade updates.
#[async_trait]
pub trait TradeHandler: Send + Sync {
    async fn on_trade(&self, trade: Trade);
}
