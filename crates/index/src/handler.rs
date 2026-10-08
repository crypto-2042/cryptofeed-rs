use crate::model::IndexPrice;
use async_trait::async_trait;

/// Handles normalized index-price updates.
#[async_trait]
pub trait IndexPriceHandler: Send + Sync {
    async fn on_index_price(&self, index_price: IndexPrice);
}
