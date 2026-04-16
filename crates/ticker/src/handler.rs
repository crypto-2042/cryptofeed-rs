use async_trait::async_trait;
use crate::model::Ticker;

#[async_trait]
pub trait TickerHandler: Send + Sync {
    async fn on_ticker(&self, ticker: Ticker);
}
