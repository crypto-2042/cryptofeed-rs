use crate::model::Ticker;
use async_trait::async_trait;

/// Handles normalized ticker updates.
#[async_trait]
pub trait TickerHandler: Send + Sync {
    async fn on_ticker(&self, ticker: Ticker);
}
