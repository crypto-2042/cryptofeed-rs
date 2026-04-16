pub mod connection;
pub mod router;
pub mod supervisor;

use cryptofeed_core::error::Result;
use crate::feed::FeedHandler;

pub async fn run(_handler: FeedHandler) -> Result<()> {
    let _router = router::Router::default();
    Ok(())
}
