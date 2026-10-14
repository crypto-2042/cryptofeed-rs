use super::FeedEnvelope;
use tokio::sync::broadcast::{
    Receiver,
    error::{RecvError, TryRecvError},
};

/// Receiver-local observations, not an atomic snapshot with producer counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct SubscriptionDiagnostics {
    pub received: u64,
    /// Only losses reported by recv/try_recv; not a prediction of future loss.
    pub lagged: u64,
    pub lag_notifications: u64,
    /// Unconsumed positions, including overwritten entries not yet acknowledged.
    pub pending: usize,
    /// Largest backlog sampled by this receiver; can miss inter-sample peaks.
    pub sampled_pending_high_water: usize,
    pub effective_capacity: usize,
    /// Producers are gone; retained events may still need draining.
    pub closed: bool,
}

/// A single identified receiver with bounded-ring delivery and sticky loss evidence.
/// Drain to Closed after producers stop before comparing final producer/consumer
/// counts. Never infer the feed/channel of overwritten events from a lag count.
pub struct IdentifiedSubscription {
    receiver: Receiver<FeedEnvelope>,
    diagnostics: SubscriptionDiagnostics,
}

impl IdentifiedSubscription {
    pub(crate) fn new(receiver: Receiver<FeedEnvelope>, effective_capacity: usize) -> Self {
        Self {
            receiver,
            diagnostics: SubscriptionDiagnostics {
                effective_capacity,
                ..SubscriptionDiagnostics::default()
            },
        }
    }

    pub fn diagnostics(&mut self) -> SubscriptionDiagnostics {
        self.sample();
        self.diagnostics
    }

    /// Cancellation-safe with the same semantics as broadcast::Receiver::recv.
    pub async fn recv(&mut self) -> Result<FeedEnvelope, RecvError> {
        self.sample();
        let result = self.receiver.recv().await;
        match &result {
            Ok(_) => self.record(true, 0),
            Err(RecvError::Lagged(n)) => self.record(false, *n),
            Err(RecvError::Closed) => self.sample(),
        }
        result
    }

    pub fn try_recv(&mut self) -> Result<FeedEnvelope, TryRecvError> {
        self.sample();
        let result = self.receiver.try_recv();
        match &result {
            Ok(_) => self.record(true, 0),
            Err(TryRecvError::Lagged(n)) => self.record(false, *n),
            Err(_) => self.sample(),
        }
        result
    }

    fn record(&mut self, received: bool, lagged: u64) {
        if received {
            self.diagnostics.received = self.diagnostics.received.saturating_add(1);
        }
        if lagged > 0 {
            self.diagnostics.lagged = self.diagnostics.lagged.saturating_add(lagged);
            self.diagnostics.lag_notifications =
                self.diagnostics.lag_notifications.saturating_add(1);
        }
        self.sample();
    }

    fn sample(&mut self) {
        self.diagnostics.pending = self.receiver.len();
        self.diagnostics.sampled_pending_high_water = self
            .diagnostics
            .sampled_pending_high_water
            .max(self.diagnostics.pending);
        self.diagnostics.closed = self.receiver.is_closed();
    }
}

#[cfg(all(test, feature = "trade"))]
mod tests {
    use super::*;
    use crate::{FeedHandler, exchange::binance::Binance, feed::FeedEvent};
    use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
    use cryptofeed_trade::{Side, Trade};

    fn event(id: usize) -> FeedEvent {
        FeedEvent::Trade(Trade {
            exchange: ExchangeId::Binance,
            symbol: Symbol::spot("BTC", "USDT"),
            side: Side::Buy,
            amount: 1.into(),
            price: 100.into(),
            exchange_ts: 1.0,
            received_ts: 2.0,
            id: Some(id.to_string()),
            implied_volatility: None,
        })
    }

    #[tokio::test]
    async fn subscription_burst_pause_and_final_drain_account_exactly() {
        let mut handler = FeedHandler::new();
        let mut receiver = handler.subscribe_identified_with_diagnostics();
        handler.add_feed(Binance::new().trade().symbol("BTC-USDT").build());
        let feed = handler.into_feeds().pop().unwrap();
        // Deterministic paused consumer: no timing, sleeps or live exchanges.
        for id in 0..2345 {
            feed.publish_event(event(id));
        }
        assert_eq!(receiver.diagnostics().pending, 2345);
        assert_eq!(receiver.recv().await.unwrap_err(), RecvError::Lagged(1321));
        for id in 2345..2427 {
            feed.publish_event(event(id));
        }
        assert_eq!(receiver.try_recv().unwrap_err(), TryRecvError::Lagged(82));
        drop(feed);
        let mut first = None;
        while let Ok(envelope) = receiver.recv().await {
            if let FeedEvent::Trade(trade) = envelope.event {
                first.get_or_insert(trade.id.unwrap());
            }
        }
        assert_eq!(first.as_deref(), Some("1403"));
        let d = receiver.diagnostics();
        assert_eq!((d.received, d.lagged, d.lag_notifications), (1024, 1403, 2));
        assert_eq!(d.received + d.lagged, 2427);
        assert_eq!(d.pending, 0);
        assert!(d.closed);
    }

    #[test]
    fn subscription_configured_capacity_is_shared_and_rounded() {
        let mut handler = FeedHandler::with_event_capacity(3).unwrap();
        let mut fast = handler.subscribe_identified_with_diagnostics();
        let mut slow = handler.subscribe_identified_with_diagnostics();
        let mut legacy = handler.subscribe_identified();
        let mut plain = handler.subscribe();
        handler.add_feed(Binance::new().trade().symbol("BTC-USDT").build());
        let feed = handler.feeds().first().unwrap();
        for id in 0..8 {
            feed.publish_event(event(id));
            assert!(fast.try_recv().is_ok());
            assert!(legacy.try_recv().is_ok());
            assert!(plain.try_recv().is_ok());
        }
        assert_eq!(slow.try_recv().unwrap_err(), TryRecvError::Lagged(4));
        for _ in 0..4 {
            assert!(slow.try_recv().is_ok());
        }
        assert_eq!(slow.diagnostics().effective_capacity, 4);
        assert_eq!(slow.diagnostics().lagged, 4);
        assert_eq!(fast.diagnostics().lagged, 0);
        assert_eq!(fast.diagnostics().received, 8);
    }

    #[tokio::test]
    async fn subscription_cancelled_receive_keeps_accounting_and_later_events() {
        let mut handler = FeedHandler::new();
        let mut receiver = handler.subscribe_identified_with_diagnostics();
        handler.add_feed(Binance::new().trade().symbol("BTC-USDT").build());
        tokio::select! { biased;
            event = receiver.recv() => panic!("unexpected event {event:?}"),
            _ = std::future::ready(()) => {}
        }
        assert_eq!(receiver.diagnostics().received, 0);
        handler.feeds()[0].publish_event(event(0));
        assert!(receiver.recv().await.is_ok());
        assert_eq!(receiver.diagnostics().received, 1);
    }

    #[test]
    fn subscription_capacity_validation_rejects_tokio_panic_inputs() {
        assert!(FeedHandler::with_event_capacity(0).is_err());
        assert!(FeedHandler::with_event_capacity(usize::MAX).is_err());
    }
}
