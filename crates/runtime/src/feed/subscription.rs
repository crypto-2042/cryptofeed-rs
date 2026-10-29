use super::{AnchoredEnvelope, FeedEnvelope};
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
pub struct IdentifiedSubscription<T: Clone = FeedEnvelope> {
    receiver: Receiver<T>,
    diagnostics: SubscriptionDiagnostics,
}

/// Receiver diagnostics for the opt-in connection-anchored stream.
pub type AnchoredSubscription = IdentifiedSubscription<AnchoredEnvelope>;

impl<T: Clone> IdentifiedSubscription<T> {
    pub(crate) fn new(receiver: Receiver<T>, effective_capacity: usize) -> Self {
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
    pub async fn recv(&mut self) -> Result<T, RecvError> {
        self.sample();
        let result = self.receiver.recv().await;
        match &result {
            Ok(_) => self.record(true, 0),
            Err(RecvError::Lagged(n)) => self.record(false, *n),
            Err(RecvError::Closed) => self.sample(),
        }
        result
    }

    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
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

#[cfg(all(test, feature = "trade"))]
mod anchored_tests {
    use super::*;
    use crate::runtime::readiness;
    use crate::{FeedHandler, exchange::binance::Binance, feed::FeedState};
    use serde_json::json;

    fn event() -> crate::feed::FeedEvent {
        crate::feed::FeedEvent::Trade(cryptofeed_trade::Trade {
            exchange: cryptofeed_core::exchange::ExchangeId::Binance,
            symbol: cryptofeed_core::symbol::Symbol::spot("BTC", "USDT"),
            side: cryptofeed_trade::Side::Buy,
            amount: 1.into(),
            price: 100.into(),
            exchange_ts: 1.0,
            received_ts: 2.0,
            id: None,
            implied_volatility: None,
        })
    }

    fn ready(attempt: &mut readiness::ConnectionAttempt) {
        attempt.connected();
        let messages = attempt.prepare_messages(vec![
            json!({
                "method":"SUBSCRIBE", "params":["btcusdt@aggTrade"], "id":1
            })
            .to_string(),
        ]);
        attempt.sent(&messages[0], tokio::time::Instant::now());
        attempt.control(&json!({"result":null,"id":1})).unwrap();
    }

    #[test]
    fn anchored_buffer_and_late_producer_keep_original_epoch_after_ready_reset() {
        let mut handler = FeedHandler::new();
        // Attach before and after registration; both receivers must stay live.
        let mut receiver = handler.subscribe_anchored_with_diagnostics();
        handler.add_feed(Binance::new().trade().symbol("BTC-USDT").build());
        let mut second = handler.subscribe_anchored();
        let mut statuses = handler.subscribe_status();
        let (mut feeds, context, _commands) = handler.into_control_parts();
        let mut feed = feeds.pop().unwrap();
        let identity = feed.identity.unwrap();
        let monitor = context.monitor(identity, feed.exchange);
        monitor.configure(1, vec![]);
        monitor.lifecycle(FeedState::Started);
        feed.monitor = Some(monitor.clone());
        readiness::track(&mut feed, "wss://synthetic/", vec![]);
        let mut old = readiness::begin(&mut feed).unwrap();
        ready(&mut old);
        let old_producer = feed.clone();
        old_producer.publish_event(event()); // Still queued when reset happens.
        for _ in 0..80 {
            // Lose all old lifecycle notifications.
            monitor.lifecycle(FeedState::Started);
        }
        assert!(matches!(statuses.try_recv(), Err(TryRecvError::Lagged(_))));
        let mut current = readiness::begin(&mut feed).unwrap();
        ready(&mut current);
        let state = monitor.snapshot();
        assert!(state.is_ready());
        let accepted = crate::feed::EventAnchor {
            identity: state.identity,
            connection: state.connections[0].id,
            epoch: state.connections[0].epoch,
        };
        feed.publish_event(event());
        old_producer.publish_event(event()); // Late old publication after new Ready.
        drop(old);
        assert!(monitor.snapshot().is_ready());
        feed.publish_event(event());
        let anchors: Vec<_> = (0..4)
            .map(|_| receiver.try_recv().unwrap().anchor)
            .collect();
        assert_eq!(anchors.iter().filter(|a| **a == accepted).count(), 2);
        assert_eq!(
            anchors.iter().map(|a| a.epoch).collect::<Vec<_>>(),
            [1, 2, 1, 2]
        );
        assert!(anchors.iter().all(|a| a.identity == identity));
        for anchor in anchors {
            assert_eq!(second.try_recv().unwrap().anchor, anchor);
        }
        assert_eq!(receiver.diagnostics().received, 4);
        assert_eq!(receiver.diagnostics().lagged, 0);
    }

    #[tokio::test]
    async fn anchored_multi_connection_replacement_retirement_and_drain_accounting() {
        let mut handler = FeedHandler::with_event_capacity(3).unwrap();
        let mut receiver = handler.subscribe_anchored_with_diagnostics();
        handler.add_feed(Binance::new().trade().symbol("BTC-USDT").build());
        let (mut feeds, context, commands) = handler.into_control_parts();
        let mut feed = feeds.pop().unwrap();
        let identity = feed.identity.unwrap();
        let monitor = context.monitor(identity, feed.exchange);
        feed.monitor = Some(monitor.clone());
        readiness::track(&mut feed, "wss://first/", vec![]);
        let first = readiness::begin(&mut feed).unwrap();
        let old_producer = feed.clone();
        readiness::track(&mut feed, "wss://second/", vec![]);
        let second = readiness::begin(&mut feed).unwrap();
        old_producer.publish_event(event());
        feed.publish_event(event());
        let a = receiver.try_recv().unwrap().anchor;
        let b = receiver.try_recv().unwrap().anchor;
        assert_eq!(a.identity, b.identity);
        assert_ne!(a.connection, b.connection);
        assert_eq!((a.epoch, b.epoch), (1, 1));
        let replacement = crate::feed::FeedIdentity {
            generation: 2,
            ..identity
        };
        context.attach(&mut feed, replacement);
        feed.monitor = Some(context.monitor(replacement, feed.exchange));
        readiness::track(&mut feed, "wss://replacement/", vec![]);
        let new_attempt = readiness::begin(&mut feed).unwrap();
        for _ in 0..7 {
            feed.publish_event(event());
        }
        assert_eq!(receiver.try_recv().unwrap_err(), TryRecvError::Lagged(3));
        // Deliberate application discard is delivered, not overwritten.
        assert_eq!(receiver.try_recv().unwrap().anchor.identity, replacement);
        monitor.lifecycle(FeedState::Stopped { forced: false });
        old_producer.publish_event(event());
        drop((
            feed,
            old_producer,
            context,
            commands,
            monitor,
            first,
            second,
            new_attempt,
        ));
        let mut retired = 0;
        while let Ok(envelope) = receiver.recv().await {
            if envelope.anchor.identity == identity {
                retired += 1;
            }
        }
        assert_eq!(retired, 1);
        let d = receiver.diagnostics();
        assert_eq!((d.received, d.lagged, d.pending), (7, 3, 0));
        assert_eq!(d.received + d.lagged, 10);
        assert!(d.closed);
    }

    #[test]
    fn static_identified_does_not_enable_control_but_anchored_does() {
        let mut handler = FeedHandler::new();
        let _legacy = handler.subscribe_identified();
        assert!(!handler.has_control());
        let _anchored = handler.subscribe_anchored();
        assert!(handler.has_control());
    }
}
