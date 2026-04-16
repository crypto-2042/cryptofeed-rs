#[derive(Default)]
pub struct Router {
    subscriptions: usize,
}

impl Router {
    pub fn subscription_count(&self) -> usize {
        self.subscriptions
    }
}

#[cfg(test)]
mod tests {
    use super::Router;

    #[test]
    fn router_initializes_empty() {
        let router = Router::default();
        assert_eq!(router.subscription_count(), 0);
    }
}
