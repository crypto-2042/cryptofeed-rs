use crate::model::{L1Book, L2Book};
use async_trait::async_trait;

/// Handles normalized order book updates (level-1 top of book and level-2).
#[async_trait]
pub trait OrderBookHandler: Send + Sync {
    async fn on_l2_book(&self, book: L2Book);

    /// Top-of-book updates are opt-in: the default is a no-op so existing
    /// handlers keep working when L1 is subscribed.
    async fn on_l1_book(&self, _book: L1Book) {}
}
