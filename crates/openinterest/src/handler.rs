use crate::model::OpenInterest;
use async_trait::async_trait;

/// Handles normalized open-interest updates.
#[async_trait]
pub trait OpenInterestHandler: Send + Sync {
    async fn on_open_interest(&self, open_interest: OpenInterest);
}
