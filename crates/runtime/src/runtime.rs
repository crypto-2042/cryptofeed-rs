pub mod connection;
pub mod router;
pub mod supervisor;

use crate::feed::FeedHandler;
use cryptofeed_core::error::Result;

pub async fn run(_handler: FeedHandler) -> Result<()> {
    let _router = router::Router::default();
    Ok(())
}
