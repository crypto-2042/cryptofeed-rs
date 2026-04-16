use cryptofeed_core::error::Result;
use url::Url;

pub struct WsConnection {
    pub url: Url,
}

impl WsConnection {
    pub fn new(url: Url) -> Self {
        Self { url }
    }

    pub async fn connect(&self) -> Result<()> {
        Ok(())
    }
}
