use crate::exchange::ExchangeFeed;
#[cfg(feature = "candles")]
use cryptofeed_candles::Candle;
use cryptofeed_core::exchange::{Channel, ExchangeId};
#[cfg(feature = "funding")]
use cryptofeed_funding::Funding;
#[cfg(feature = "index")]
use cryptofeed_index::IndexPrice;
#[cfg(feature = "liquidations")]
use cryptofeed_liquidations::Liquidation;
#[cfg(feature = "markprice")]
use cryptofeed_markprice::MarkPrice;
#[cfg(feature = "openinterest")]
use cryptofeed_openinterest::OpenInterest;
#[cfg(feature = "orderbook")]
use cryptofeed_orderbook::{L1Book, L2Book};
#[cfg(feature = "ticker")]
use cryptofeed_ticker::Ticker;
#[cfg(feature = "trade")]
use cryptofeed_trade::Trade;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::broadcast;

/// A normalized event delivered through the `FeedHandler` event stream.
///
/// `FeedHandler::subscribe()` returns a `broadcast::Receiver` of these
/// events. The stream is a complement to the handler-trait callbacks: it
/// supports multiple consumers and does not block the feed loop, but a slow
/// consumer that never drains its receiver will lag (the broadcast channel
/// drops the oldest events once the bounded buffer is full).
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum FeedEvent {
    #[cfg(feature = "ticker")]
    Ticker(Ticker),
    #[cfg(feature = "trade")]
    Trade(Trade),
    #[cfg(feature = "orderbook")]
    L2Book(L2Book),
    #[cfg(feature = "orderbook")]
    L1Book(L1Book),
    #[cfg(feature = "candles")]
    Candle(Candle),
    #[cfg(feature = "funding")]
    Funding(Funding),
    #[cfg(feature = "liquidations")]
    Liquidation(Liquidation),
    #[cfg(feature = "openinterest")]
    OpenInterest(OpenInterest),
    #[cfg(feature = "index")]
    IndexPrice(IndexPrice),
    #[cfg(feature = "markprice")]
    MarkPrice(MarkPrice),
}

/// Programmatic lifecycle notifications for terminal feed failures.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum FeedStatus {
    Terminated { exchange: ExchangeId, error: String },
    TaskPanicked { error: String },
}

impl FeedEvent {
    pub fn channel(&self) -> Channel {
        // Match on the owned form so an all-features-disabled build (empty
        // enum) stays exhaustive without a wildcard arm.
        match *self {
            #[cfg(feature = "ticker")]
            FeedEvent::Ticker(_) => Channel::Ticker,
            #[cfg(feature = "trade")]
            FeedEvent::Trade(_) => Channel::Trade,
            #[cfg(feature = "orderbook")]
            FeedEvent::L2Book(_) => Channel::L2Book,
            #[cfg(feature = "orderbook")]
            FeedEvent::L1Book(_) => Channel::L1Book,
            #[cfg(feature = "candles")]
            FeedEvent::Candle(_) => Channel::Candles,
            #[cfg(feature = "funding")]
            FeedEvent::Funding(_) => Channel::Funding,
            #[cfg(feature = "liquidations")]
            FeedEvent::Liquidation(_) => Channel::Liquidations,
            #[cfg(feature = "openinterest")]
            FeedEvent::OpenInterest(_) => Channel::OpenInterest,
            #[cfg(feature = "index")]
            FeedEvent::IndexPrice(_) => Channel::Index,
            #[cfg(feature = "markprice")]
            FeedEvent::MarkPrice(_) => Channel::MarkPrice,
        }
    }

    #[cfg(any(
        feature = "ticker",
        feature = "trade",
        feature = "orderbook",
        feature = "candles",
        feature = "funding",
        feature = "liquidations",
        feature = "markprice",
        feature = "openinterest",
        feature = "index"
    ))]
    pub fn exchange(&self) -> ExchangeId {
        match self {
            #[cfg(feature = "ticker")]
            FeedEvent::Ticker(value) => value.exchange,
            #[cfg(feature = "trade")]
            FeedEvent::Trade(value) => value.exchange,
            #[cfg(feature = "orderbook")]
            FeedEvent::L2Book(value) => match value {
                L2Book::Snapshot(snapshot) => snapshot.exchange,
                L2Book::Delta(delta) => delta.exchange,
            },
            #[cfg(feature = "orderbook")]
            FeedEvent::L1Book(value) => value.exchange,
            #[cfg(feature = "candles")]
            FeedEvent::Candle(value) => value.exchange,
            #[cfg(feature = "funding")]
            FeedEvent::Funding(value) => value.exchange,
            #[cfg(feature = "liquidations")]
            FeedEvent::Liquidation(value) => value.exchange,
            #[cfg(feature = "openinterest")]
            FeedEvent::OpenInterest(value) => value.exchange,
            #[cfg(feature = "index")]
            FeedEvent::IndexPrice(value) => value.exchange,
            #[cfg(feature = "markprice")]
            FeedEvent::MarkPrice(value) => value.exchange,
        }
    }

    #[cfg(any(
        feature = "ticker",
        feature = "trade",
        feature = "orderbook",
        feature = "candles",
        feature = "funding",
        feature = "liquidations",
        feature = "markprice",
        feature = "openinterest",
        feature = "index"
    ))]
    pub fn symbol(&self) -> &cryptofeed_core::symbol::Symbol {
        match self {
            #[cfg(feature = "ticker")]
            FeedEvent::Ticker(value) => &value.symbol,
            #[cfg(feature = "trade")]
            FeedEvent::Trade(value) => &value.symbol,
            #[cfg(feature = "orderbook")]
            FeedEvent::L2Book(value) => value.symbol(),
            #[cfg(feature = "orderbook")]
            FeedEvent::L1Book(value) => &value.symbol,
            #[cfg(feature = "candles")]
            FeedEvent::Candle(value) => &value.symbol,
            #[cfg(feature = "funding")]
            FeedEvent::Funding(value) => &value.symbol,
            #[cfg(feature = "liquidations")]
            FeedEvent::Liquidation(value) => &value.symbol,
            #[cfg(feature = "openinterest")]
            FeedEvent::OpenInterest(value) => &value.symbol,
            #[cfg(feature = "index")]
            FeedEvent::IndexPrice(value) => &value.symbol,
            #[cfg(feature = "markprice")]
            FeedEvent::MarkPrice(value) => &value.symbol,
        }
    }
}

/// Per-channel event counters shared by all feeds of a `FeedHandler`.
#[derive(Default)]
pub struct EventCounters {
    counts: [AtomicU64; SLOT_COUNT],
}

const SLOT_COUNT: usize = 10;

impl EventCounters {
    fn index(channel: Channel) -> usize {
        match channel {
            Channel::Ticker => 0,
            Channel::Trade => 1,
            Channel::L2Book => 2,
            Channel::L1Book => 3,
            Channel::Candles => 4,
            Channel::Funding => 5,
            Channel::Liquidations => 6,
            Channel::OpenInterest => 7,
            Channel::Index => 8,
            Channel::MarkPrice => 9,
            // Future channels count into the trailing (unused) slot rather
            // than panicking on the counter array index.
            _ => SLOT_COUNT - 1,
        }
    }

    pub(crate) fn bump(&self, channel: Channel) {
        self.counts[Self::index(channel)].fetch_add(1, Ordering::Relaxed);
    }

    pub fn count(&self, channel: Channel) -> u64 {
        self.counts[Self::index(channel)].load(Ordering::Relaxed)
    }
}

pub struct FeedHandler {
    feeds: Vec<ExchangeFeed>,
    event_sender: Option<broadcast::Sender<FeedEvent>>,
    status_sender: Option<broadcast::Sender<FeedStatus>>,
    counters: Arc<EventCounters>,
}

impl Default for FeedHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl FeedHandler {
    pub fn new() -> Self {
        Self {
            feeds: Vec::new(),
            event_sender: None,
            status_sender: None,
            counters: Arc::new(EventCounters::default()),
        }
    }

    /// Subscribes to the normalized event stream. Call before `add_feed` so
    /// the feeds deliver into the stream; returns a bounded broadcast
    /// receiver (capacity 1024 per subscriber). Calling `subscribe` again
    /// returns an additional receiver on the same stream — the first
    /// receiver keeps working and feeds added in between are not orphaned.
    pub fn subscribe(&mut self) -> broadcast::Receiver<FeedEvent> {
        if self.event_sender.is_none() {
            let (sender, _) = broadcast::channel(1024);
            self.event_sender = Some(sender);
        }
        self.event_sender
            .as_ref()
            .expect("event sender")
            .subscribe()
    }

    /// Subscribes to terminal feed/task failures without requiring a tracing
    /// subscriber. Call before `add_feed`, like [`FeedHandler::subscribe`].
    pub fn subscribe_status(&mut self) -> broadcast::Receiver<FeedStatus> {
        if self.status_sender.is_none() {
            let (sender, _) = broadcast::channel(64);
            self.status_sender = Some(sender);
        }
        self.status_sender
            .as_ref()
            .expect("status sender")
            .subscribe()
    }

    pub fn add_feed(&mut self, mut feed: ExchangeFeed) {
        feed.event_sender = self.event_sender.clone();
        feed.status_sender = self.status_sender.clone();
        feed.event_counts = Some(self.counters.clone());
        self.feeds.push(feed);
    }

    pub fn feed_count(&self) -> usize {
        self.feeds.len()
    }

    /// Total normalized events produced for a channel across all feeds.
    /// This counter advances even when no broadcast receiver is attached or a
    /// receiver has lagged; use it as producer health, not delivery telemetry.
    pub fn event_count(&self, channel: Channel) -> u64 {
        self.counters.count(channel)
    }

    /// Retains producer counters while `run(self)` owns the handler.
    /// Obtain this handle before running; it remains readable after shutdown.
    pub fn event_counters(&self) -> Arc<EventCounters> {
        self.counters.clone()
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn feeds(&self) -> &[ExchangeFeed] {
        &self.feeds
    }

    pub(crate) fn into_feeds(self) -> Vec<ExchangeFeed> {
        self.feeds
    }

    pub async fn run(self) -> cryptofeed_core::error::Result<()> {
        crate::runtime::run(self).await
    }
}

#[cfg(all(test, feature = "ticker"))]
mod tests {
    use super::{FeedEvent, FeedHandler};
    use cryptofeed_core::exchange::Channel;

    #[test]
    fn starts_with_no_feeds() {
        let handler = FeedHandler::new();
        assert_eq!(handler.feed_count(), 0);
    }

    #[test]
    fn subscribe_twice_keeps_both_receivers_on_the_same_stream() {
        use cryptofeed_core::exchange::ExchangeId;
        use cryptofeed_core::symbol::Symbol;
        let mut handler = FeedHandler::new();
        let mut first = handler.subscribe();
        let mut second = handler.subscribe();

        let feed = crate::exchange::binance::Binance::new()
            .ticker()
            .symbol("BTC-USDT")
            .build();
        handler.add_feed(feed);

        // Feed added after both subscriptions must deliver into both
        // receivers (regression: the second subscribe used to replace the
        // sender and orphan the first receiver).
        let feed = handler.feeds().first().expect("feed");
        let ticker = cryptofeed_ticker::Ticker {
            exchange: ExchangeId::Binance,
            symbol: Symbol::spot("BTC", "USDT"),
            bid: rust_decimal::Decimal::from_str_exact("64999.10").unwrap(),
            ask: rust_decimal::Decimal::from_str_exact("65000.20").unwrap(),
            exchange_ts: 1.0,
            received_ts: 2.0,
            implied_volatility: None,
        };
        feed.publish_event(FeedEvent::Ticker(ticker));

        assert!(matches!(first.try_recv(), Ok(FeedEvent::Ticker(_))));
        assert!(matches!(second.try_recv(), Ok(FeedEvent::Ticker(_))));
    }

    #[test]
    fn event_stream_delivers_normalized_events() {
        use crate::exchange::binance::Binance;
        let mut handler = FeedHandler::new();
        let mut receiver = handler.subscribe();
        handler.add_feed(Binance::new().ticker().trade().symbol("BTC-USDT").build());
        assert_eq!(handler.feed_count(), 1);
        assert_eq!(handler.event_count(Channel::Ticker), 0);

        let symbol = cryptofeed_core::symbol::Symbol::spot("BTC", "USDT");
        let feed = handler.feeds().first().expect("feed");
        #[cfg(feature = "ticker")]
        {
            let ticker = cryptofeed_ticker::Ticker {
                exchange: cryptofeed_core::exchange::ExchangeId::Binance,
                symbol: symbol.clone(),
                bid: rust_decimal::Decimal::from_str_exact("64999.10").unwrap(),
                ask: rust_decimal::Decimal::from_str_exact("65000.20").unwrap(),
                exchange_ts: 1.0,
                received_ts: 2.0,
                implied_volatility: None,
            };
            // The dispatch path: publish_event feeds both the event stream
            // and the per-channel counters.
            feed.publish_event(FeedEvent::Ticker(ticker.clone()));
            feed.publish_event(FeedEvent::Ticker(ticker));
            match receiver.try_recv() {
                Ok(FeedEvent::Ticker(evt)) => assert_eq!(evt.symbol.as_str(), "BTC-USDT"),
                Ok(event) => panic!("expected ticker event, got {:?}", event.channel()),
                Err(_) => panic!("expected event in stream"),
            }
            assert_eq!(handler.event_count(Channel::Ticker), 2);
        }
    }
    #[test]
    fn counters_remain_readable_after_handler_is_consumed() {
        let mut handler = FeedHandler::new();
        let counters = handler.event_counters();
        handler.add_feed(
            crate::exchange::binance::Binance::new()
                .ticker()
                .symbol("BTC-USDT")
                .build(),
        );
        let feed = handler.into_feeds().pop().unwrap();
        feed.publish_event(FeedEvent::Ticker(cryptofeed_ticker::Ticker {
            exchange: cryptofeed_core::exchange::ExchangeId::Binance,
            symbol: cryptofeed_core::symbol::Symbol::spot("BTC", "USDT"),
            bid: rust_decimal::Decimal::from(1),
            ask: rust_decimal::Decimal::from(2),
            exchange_ts: 1.0,
            received_ts: 2.0,
            implied_volatility: None,
        }));
        assert_eq!(counters.count(Channel::Ticker), 1);
        assert_eq!(counters.count(Channel::Trade), 0);
        drop(feed);
        assert_eq!(counters.count(Channel::Ticker), 1);
    }
}
