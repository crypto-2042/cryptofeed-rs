use async_trait::async_trait;
use crate::model::L2Book;

#[async_trait]
pub trait OrderBookHandler: Send + Sync {
    async fn on_l2_book(&self, book: L2Book);
}
