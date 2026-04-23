use crate::model::Candle;
use async_trait::async_trait;

/// Handles normalized candle updates.
#[async_trait]
pub trait CandleHandler: Send + Sync {
    async fn on_candle(&self, candle: Candle);
}
