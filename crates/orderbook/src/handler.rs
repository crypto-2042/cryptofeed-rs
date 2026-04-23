use crate::model::L2Book;
use async_trait::async_trait;

/// Handles normalized level-2 order book updates.
#[async_trait]
pub trait OrderBookHandler: Send + Sync {
    async fn on_l2_book(&self, book: L2Book);
}
