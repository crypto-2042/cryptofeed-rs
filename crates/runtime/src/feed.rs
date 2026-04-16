use crate::exchange::ExchangeFeed;

pub struct FeedHandler {
    feeds: Vec<ExchangeFeed>,
}

impl FeedHandler {
    pub fn new() -> Self {
        Self { feeds: Vec::new() }
    }

    pub fn add_feed(&mut self, feed: ExchangeFeed) {
        self.feeds.push(feed);
    }

    pub fn feed_count(&self) -> usize {
        self.feeds.len()
    }

    pub async fn run(self) -> cryptofeed_core::error::Result<()> {
        crate::runtime::run(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::FeedHandler;

    #[test]
    fn starts_with_no_feeds() {
        let handler = FeedHandler::new();
        assert_eq!(handler.feed_count(), 0);
    }
}
