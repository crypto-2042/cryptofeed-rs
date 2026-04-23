use crate::model::Funding;
use async_trait::async_trait;

/// Handles normalized funding updates.
#[async_trait]
pub trait FundingHandler: Send + Sync {
    async fn on_funding(&self, funding: Funding);
}
