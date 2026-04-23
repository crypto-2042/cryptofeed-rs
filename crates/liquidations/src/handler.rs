use crate::model::Liquidation;
use async_trait::async_trait;

/// Handles normalized liquidation updates.
#[async_trait]
pub trait LiquidationHandler: Send + Sync {
    async fn on_liquidation(&self, liquidation: Liquidation);
}
