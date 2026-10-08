use crate::model::MarkPrice;
use async_trait::async_trait;

/// Handles normalized mark-price updates.
#[async_trait]
pub trait MarkPriceHandler: Send + Sync {
    async fn on_mark_price(&self, mark_price: MarkPrice);
}
