//! Opt-in bounded asynchronous handler dispatch.
//!
//! In [`HandlerDispatch::Inline`](crate::options::HandlerDispatch::Inline)
//! mode (the default) each `dispatch_*` function awaits user callbacks on the
//! session read path. In `Async` mode one background task per logical feed
//! executes callbacks sequentially; ingestion only performs a short,
//! non-blocking enqueue. See the repository's `docs/backpressure.md` for the
//! delivery guarantees.
//!
//! Coalescing and overflow are observable, never silent: every superseded
//! state update, every dropped oldest event and every delivered event is
//! counted in [`HandlerDispatchStats`].

#[cfg(feature = "orderbook")]
use crate::books::BookStore;
use crate::exchange::ExchangeFeed;
#[cfg(feature = "orderbook")]
use crate::feed::FeedIdentity;
#[cfg(feature = "candles")]
use cryptofeed_candles::{Candle, CandleHandler};
use cryptofeed_core::exchange::Channel;
#[cfg(any(
    feature = "candles",
    feature = "funding",
    feature = "index",
    feature = "markprice",
    feature = "openinterest",
    feature = "orderbook",
    feature = "ticker"
))]
use cryptofeed_core::symbol::Symbol;
#[cfg(feature = "funding")]
use cryptofeed_funding::{Funding, FundingHandler};
#[cfg(feature = "index")]
use cryptofeed_index::{IndexPrice, IndexPriceHandler};
#[cfg(feature = "liquidations")]
use cryptofeed_liquidations::{Liquidation, LiquidationHandler};
#[cfg(feature = "markprice")]
use cryptofeed_markprice::{MarkPrice, MarkPriceHandler};
#[cfg(feature = "openinterest")]
use cryptofeed_openinterest::{OpenInterest, OpenInterestHandler};
#[cfg(feature = "orderbook")]
use cryptofeed_orderbook::{L1Book, L2Book, L2BookSnapshot, OrderBookHandler};
#[cfg(feature = "ticker")]
use cryptofeed_ticker::{Ticker, TickerHandler};
#[cfg(feature = "trade")]
use cryptofeed_trade::{Trade, TradeHandler};
use futures::FutureExt;
#[cfg(any(
    feature = "candles",
    feature = "funding",
    feature = "index",
    feature = "markprice",
    feature = "openinterest",
    feature = "orderbook",
    feature = "ticker"
))]
use std::collections::HashMap;
#[cfg(any(feature = "liquidations", feature = "orderbook", feature = "trade"))]
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;

/// Number of channel slots used by the diagnostic arrays.
const SLOT_COUNT: usize = 10;
/// Maximum event entries moved into one user-visible delivery batch.
#[cfg(any(feature = "liquidations", feature = "orderbook", feature = "trade"))]
const EVENT_BATCH: usize = 256;
/// Extra bounded drain passes attempted when the consumer task is stopped.
const FLUSH_PASSES: usize = 4;
/// Minimum interval between per-dispatcher drop warnings.
const DROP_WARN_INTERVAL: Duration = Duration::from_secs(5);

/// Shared per-callback execution: bounded timeout plus panic isolation.
async fn await_handler<F>(timeout: Duration, channel: &'static str, future: F)
where
    F: std::future::Future<Output = ()>,
{
    match tokio::time::timeout(timeout, std::panic::AssertUnwindSafe(future).catch_unwind()).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => tracing::error!(channel, "handler panicked; continuing remaining callbacks"),
        Err(_) => tracing::warn!(channel, "handler timed out; event callback cancelled"),
    }
}

/// Generates one channel's shared handler-invocation path used by both inline
/// dispatch and the asynchronous consumer. Type-correct per channel: no
/// dynamic `Any` or type-erased callback dispatch.
#[cfg(any(
    feature = "candles",
    feature = "funding",
    feature = "index",
    feature = "liquidations",
    feature = "markprice",
    feature = "openinterest",
    feature = "orderbook",
    feature = "ticker",
    feature = "trade"
))]
macro_rules! deliver_channel {
    ($name:ident, $handler_ty:ty, $method:ident, $event_ty:ty, $label:expr) => {
        pub(crate) async fn $name(
            primary: Option<&Arc<$handler_ty>>,
            additional: &[Arc<$handler_ty>],
            deadline: Duration,
            event: $event_ty,
        ) {
            for handler in primary.into_iter().chain(additional.iter()) {
                await_handler(deadline, $label, async {
                    handler.$method(event.clone()).await
                })
                .await;
            }
        }
    };
}

#[cfg(feature = "ticker")]
deliver_channel!(
    deliver_ticker,
    dyn TickerHandler,
    on_ticker,
    Ticker,
    "ticker"
);
#[cfg(feature = "trade")]
deliver_channel!(deliver_trade, dyn TradeHandler, on_trade, Trade, "trade");
#[cfg(feature = "orderbook")]
deliver_channel!(
    deliver_l2_book,
    dyn OrderBookHandler,
    on_l2_book,
    L2Book,
    "l2_book"
);
#[cfg(feature = "orderbook")]
deliver_channel!(
    deliver_l1_book,
    dyn OrderBookHandler,
    on_l1_book,
    L1Book,
    "l1_book"
);
#[cfg(feature = "candles")]
deliver_channel!(
    deliver_candle,
    dyn CandleHandler,
    on_candle,
    Candle,
    "candle"
);
#[cfg(feature = "funding")]
deliver_channel!(
    deliver_funding,
    dyn FundingHandler,
    on_funding,
    Funding,
    "funding"
);
#[cfg(feature = "liquidations")]
deliver_channel!(
    deliver_liquidation,
    dyn LiquidationHandler,
    on_liquidation,
    Liquidation,
    "liquidation"
);
#[cfg(feature = "markprice")]
deliver_channel!(
    deliver_mark_price,
    dyn MarkPriceHandler,
    on_mark_price,
    MarkPrice,
    "mark_price"
);
#[cfg(feature = "openinterest")]
deliver_channel!(
    deliver_open_interest,
    dyn OpenInterestHandler,
    on_open_interest,
    OpenInterest,
    "open_interest"
);
#[cfg(feature = "index")]
deliver_channel!(
    deliver_index_price,
    dyn IndexPriceHandler,
    on_index_price,
    IndexPrice,
    "index_price"
);

fn slot(channel: Channel) -> Option<usize> {
    Some(match channel {
        Channel::Candles => 0,
        Channel::Funding => 1,
        Channel::Index => 2,
        Channel::L1Book => 3,
        Channel::Liquidations => 4,
        Channel::MarkPrice => 5,
        Channel::OpenInterest => 6,
        Channel::Ticker => 7,
        Channel::Trade => 8,
        Channel::L2Book => 9,
        // Non-exhaustive: unknown future channels are not counted.
        _ => return None,
    })
}

/// Point-in-time handler-dispatch diagnostics for one `FeedHandler`.
///
/// Counters aggregate every logical feed of the handler. They describe the
/// opt-in asynchronous dispatcher only: inline mode leaves them at zero.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HandlerDispatchStats {
    delivered: [u64; SLOT_COUNT],
    coalesced: [u64; SLOT_COUNT],
    dropped_oldest: [u64; SLOT_COUNT],
    queued: u64,
    peak_queued: u64,
}

impl HandlerDispatchStats {
    /// Events handed to at least one registered handler for `channel`.
    pub fn delivered(&self, channel: Channel) -> u64 {
        slot(channel).map_or(0, |index| self.delivered[index])
    }

    /// State updates superseded by a newer pending value before delivery.
    ///
    /// Expected loss for state channels (ticker, L1 book, funding, open
    /// interest, index, mark price, candles) and for book-store-backed L2
    /// coalescing: only the latest value is meaningful.
    pub fn coalesced(&self, channel: Channel) -> u64 {
        slot(channel).map_or(0, |index| self.coalesced[index])
    }

    /// Events dropped because an event-channel ring overflowed.
    ///
    /// The oldest pending event is dropped first and the newest is retained.
    /// Applies to trade, liquidation and raw (no book store) L2 rings.
    pub fn dropped_oldest(&self, channel: Channel) -> u64 {
        slot(channel).map_or(0, |index| self.dropped_oldest[index])
    }

    /// Total entries currently pending across all queues of the handler.
    pub fn queued(&self) -> u64 {
        self.queued
    }

    /// Largest aggregate queue depth observed since counter creation.
    pub fn peak_queued(&self) -> u64 {
        self.peak_queued
    }

    /// Total events lost to coalescing plus oldest-first ring overflow.
    pub fn lost(&self) -> u64 {
        self.coalesced.iter().sum::<u64>() + self.dropped_oldest.iter().sum::<u64>()
    }
}

/// Cloneable handle to the async dispatch counters of one `FeedHandler`.
///
/// Obtain it before `FeedHandler::run`/`run_with_shutdown` consumes the
/// handler; it remains readable while feeds run and after they stop.
#[derive(Clone, Debug, Default)]
pub struct HandlerDispatchStatsHandle(Arc<DispatchCounters>);

impl HandlerDispatchStatsHandle {
    pub(crate) fn new(counters: Arc<DispatchCounters>) -> Self {
        Self(counters)
    }

    /// Latest aggregated counter snapshot.
    pub fn snapshot(&self) -> HandlerDispatchStats {
        self.0.snapshot()
    }
}

#[derive(Debug, Default)]
pub(crate) struct DispatchCounters {
    delivered: [AtomicU64; SLOT_COUNT],
    coalesced: [AtomicU64; SLOT_COUNT],
    dropped_oldest: [AtomicU64; SLOT_COUNT],
    queued: AtomicU64,
    peak_queued: AtomicU64,
}

impl DispatchCounters {
    fn bump_delivered(&self, channel: Channel) {
        if let Some(index) = slot(channel) {
            self.delivered[index].fetch_add(1, Ordering::Relaxed);
        }
    }

    fn bump_coalesced(&self, channel: Channel) {
        if let Some(index) = slot(channel) {
            self.coalesced[index].fetch_add(1, Ordering::Relaxed);
        }
    }

    fn bump_dropped_oldest(&self, channel: Channel) {
        if let Some(index) = slot(channel) {
            self.dropped_oldest[index].fetch_add(1, Ordering::Relaxed);
        }
    }

    fn queued_added(&self) {
        let depth = self.queued.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak_queued.fetch_max(depth, Ordering::Relaxed);
    }

    fn queued_removed(&self, count: usize) {
        if count > 0 {
            self.queued.fetch_sub(count as u64, Ordering::Relaxed);
        }
    }

    pub(crate) fn snapshot(&self) -> HandlerDispatchStats {
        HandlerDispatchStats {
            delivered: std::array::from_fn(|index| self.delivered[index].load(Ordering::Relaxed)),
            coalesced: std::array::from_fn(|index| self.coalesced[index].load(Ordering::Relaxed)),
            dropped_oldest: std::array::from_fn(|index| {
                self.dropped_oldest[index].load(Ordering::Relaxed)
            }),
            queued: self.queued.load(Ordering::Relaxed),
            peak_queued: self.peak_queued.load(Ordering::Relaxed),
        }
    }
}

/// Handler clones captured when the dispatcher is created.
///
/// Handlers registered after `run` starts are not observed by an async
/// dispatcher; register them before adding/running the feed.
#[derive(Default)]
struct Handlers {
    #[cfg(feature = "ticker")]
    ticker: Vec<Arc<dyn TickerHandler>>,
    #[cfg(feature = "trade")]
    trade: Vec<Arc<dyn TradeHandler>>,
    #[cfg(feature = "orderbook")]
    orderbook: Vec<Arc<dyn OrderBookHandler>>,
    #[cfg(feature = "candles")]
    candles: Vec<Arc<dyn CandleHandler>>,
    #[cfg(feature = "funding")]
    funding: Vec<Arc<dyn FundingHandler>>,
    #[cfg(feature = "liquidations")]
    liquidations: Vec<Arc<dyn LiquidationHandler>>,
    #[cfg(feature = "markprice")]
    mark_price: Vec<Arc<dyn MarkPriceHandler>>,
    #[cfg(feature = "openinterest")]
    open_interest: Vec<Arc<dyn OpenInterestHandler>>,
    #[cfg(feature = "index")]
    index: Vec<Arc<dyn IndexPriceHandler>>,
}

impl Handlers {
    fn capture(feed: &ExchangeFeed) -> Self {
        Self {
            #[cfg(feature = "ticker")]
            ticker: feed
                .ticker_handler
                .iter()
                .chain(&feed.additional_ticker_handlers)
                .cloned()
                .collect(),
            #[cfg(feature = "trade")]
            trade: feed
                .trade_handler
                .iter()
                .chain(&feed.additional_trade_handlers)
                .cloned()
                .collect(),
            #[cfg(feature = "orderbook")]
            orderbook: feed
                .orderbook_handler
                .iter()
                .chain(&feed.additional_orderbook_handlers)
                .cloned()
                .collect(),
            #[cfg(feature = "candles")]
            candles: feed
                .candle_handler
                .iter()
                .chain(&feed.additional_candle_handlers)
                .cloned()
                .collect(),
            #[cfg(feature = "funding")]
            funding: feed
                .funding_handler
                .iter()
                .chain(&feed.additional_funding_handlers)
                .cloned()
                .collect(),
            #[cfg(feature = "liquidations")]
            liquidations: feed
                .liquidation_handler
                .iter()
                .chain(&feed.additional_liquidation_handlers)
                .cloned()
                .collect(),
            #[cfg(feature = "markprice")]
            mark_price: feed
                .mark_price_handler
                .iter()
                .chain(&feed.additional_mark_price_handlers)
                .cloned()
                .collect(),
            #[cfg(feature = "openinterest")]
            open_interest: feed
                .open_interest_handler
                .iter()
                .chain(&feed.additional_open_interest_handlers)
                .cloned()
                .collect(),
            #[cfg(feature = "index")]
            index: feed
                .index_price_handler
                .iter()
                .chain(&feed.additional_index_price_handlers)
                .cloned()
                .collect(),
        }
    }

    fn is_empty(&self) -> bool {
        let mut empty = true;
        #[cfg(feature = "ticker")]
        {
            empty &= self.ticker.is_empty();
        }
        #[cfg(feature = "trade")]
        {
            empty &= self.trade.is_empty();
        }
        #[cfg(feature = "orderbook")]
        {
            empty &= self.orderbook.is_empty();
        }
        #[cfg(feature = "candles")]
        {
            empty &= self.candles.is_empty();
        }
        #[cfg(feature = "funding")]
        {
            empty &= self.funding.is_empty();
        }
        #[cfg(feature = "liquidations")]
        {
            empty &= self.liquidations.is_empty();
        }
        #[cfg(feature = "markprice")]
        {
            empty &= self.mark_price.is_empty();
        }
        #[cfg(feature = "openinterest")]
        {
            empty &= self.open_interest.is_empty();
        }
        #[cfg(feature = "index")]
        {
            empty &= self.index.is_empty();
        }
        empty
    }
}

/// L2 delivery policy chosen at dispatcher creation.
#[cfg(feature = "orderbook")]
enum L2Queue {
    /// A configured `BookStore` exists: keep only the latest pending symbol.
    Coalesced(HashMap<Symbol, ()>),
    /// No book store: behave as a bounded FIFO of raw events.
    Events(VecDeque<L2Book>),
}

#[cfg(feature = "orderbook")]
impl Default for L2Queue {
    fn default() -> Self {
        Self::Coalesced(HashMap::new())
    }
}

#[derive(Default)]
struct Queues {
    #[cfg(feature = "ticker")]
    ticker: HashMap<Symbol, Ticker>,
    #[cfg(feature = "trade")]
    trade: VecDeque<Trade>,
    #[cfg(feature = "orderbook")]
    l2: L2Queue,
    #[cfg(feature = "orderbook")]
    l1_book: HashMap<Symbol, L1Book>,
    #[cfg(feature = "candles")]
    candles: HashMap<(Symbol, String), Candle>,
    #[cfg(feature = "funding")]
    funding: HashMap<Symbol, Funding>,
    #[cfg(feature = "liquidations")]
    liquidations: VecDeque<Liquidation>,
    #[cfg(feature = "openinterest")]
    open_interest: HashMap<Symbol, OpenInterest>,
    #[cfg(feature = "index")]
    index: HashMap<Symbol, IndexPrice>,
    #[cfg(feature = "markprice")]
    mark_price: HashMap<Symbol, MarkPrice>,
}

/// Whether an enqueue inserted, superseded or overflowed an entry.
///
/// Variant construction depends on enabled data features; the diagnostic
/// combinator remains compiled for every feature subset.
#[derive(Clone, Copy)]
#[allow(dead_code)]
enum EnqueueOutcome {
    Added,
    Coalesced,
    Dropped,
}

/// One delivery unit drained from the queues.
#[derive(Default)]
struct Batch {
    #[cfg(feature = "ticker")]
    ticker: Vec<(Symbol, Ticker)>,
    #[cfg(feature = "trade")]
    trade: Vec<Trade>,
    #[cfg(feature = "orderbook")]
    l2_symbols: Vec<Symbol>,
    #[cfg(feature = "orderbook")]
    l2_books: Vec<L2Book>,
    #[cfg(feature = "orderbook")]
    l1_book: Vec<(Symbol, L1Book)>,
    #[cfg(feature = "candles")]
    candles: Vec<((Symbol, String), Candle)>,
    #[cfg(feature = "funding")]
    funding: Vec<(Symbol, Funding)>,
    #[cfg(feature = "liquidations")]
    liquidations: Vec<Liquidation>,
    #[cfg(feature = "openinterest")]
    open_interest: Vec<(Symbol, OpenInterest)>,
    #[cfg(feature = "index")]
    index: Vec<(Symbol, IndexPrice)>,
    #[cfg(feature = "markprice")]
    mark_price: Vec<(Symbol, MarkPrice)>,
    count: usize,
}

impl Batch {
    fn is_empty(&self) -> bool {
        self.count == 0
    }
}

struct Inner {
    capacity: usize,
    deadline: Duration,
    #[cfg(feature = "orderbook")]
    books: Option<Arc<BookStore>>,
    #[cfg(feature = "orderbook")]
    identity: Option<FeedIdentity>,
    handlers: Handlers,
    queues: Mutex<Queues>,
    notify: Notify,
    counters: Arc<DispatchCounters>,
    last_drop_warning: Mutex<Option<Instant>>,
}

impl Inner {
    fn lock_queues(&self) -> std::sync::MutexGuard<'_, Queues> {
        self.queues.lock().expect("handler dispatch queue lock")
    }

    fn note(&self, channel: Channel, outcome: EnqueueOutcome) {
        match outcome {
            EnqueueOutcome::Added => self.counters.queued_added(),
            EnqueueOutcome::Coalesced => self.counters.bump_coalesced(channel),
            EnqueueOutcome::Dropped => {
                self.counters.bump_dropped_oldest(channel);
                self.warn_on_drop(channel);
            }
        }
        // A permit is retained if the consumer is currently delivering, so a
        // pending batch is never stranded between drains.
        self.notify.notify_one();
    }

    /// At most one overflow warning per `DROP_WARN_INTERVAL` per dispatcher.
    fn warn_on_drop(&self, channel: Channel) {
        let now = Instant::now();
        let due = {
            let mut last = self
                .last_drop_warning
                .lock()
                .expect("handler dispatch warning lock");
            match *last {
                Some(at) if now.duration_since(at) < DROP_WARN_INTERVAL => false,
                _ => {
                    *last = Some(now);
                    true
                }
            }
        };
        if due {
            tracing::warn!(
                ?channel,
                capacity = self.capacity,
                dropped_oldest = self.counters.snapshot().dropped_oldest(channel),
                "async handler dispatch dropped the oldest pending event; increase capacity or drain handlers faster"
            );
        }
    }

    /// Moves all coalesced state and up to `EVENT_BATCH` event entries into a
    /// batch. Returns whether more event entries remain.
    fn take_batch(&self) -> (Batch, bool) {
        let mut queues = self.lock_queues();
        let mut batch = Batch::default();
        #[cfg(feature = "candles")]
        {
            for (key, value) in queues.candles.drain() {
                batch.candles.push((key, value));
                batch.count += 1;
            }
            batch.candles.sort_by(|left, right| {
                left.0
                    .0
                    .as_str()
                    .cmp(right.0.0.as_str())
                    .then_with(|| left.0.1.cmp(&right.0.1))
            });
        }
        #[cfg(feature = "funding")]
        {
            for (key, value) in queues.funding.drain() {
                batch.funding.push((key, value));
                batch.count += 1;
            }
            batch
                .funding
                .sort_by(|left, right| left.0.as_str().cmp(right.0.as_str()));
        }
        #[cfg(feature = "index")]
        {
            for (key, value) in queues.index.drain() {
                batch.index.push((key, value));
                batch.count += 1;
            }
            batch
                .index
                .sort_by(|left, right| left.0.as_str().cmp(right.0.as_str()));
        }
        #[cfg(feature = "markprice")]
        {
            for (key, value) in queues.mark_price.drain() {
                batch.mark_price.push((key, value));
                batch.count += 1;
            }
            batch
                .mark_price
                .sort_by(|left, right| left.0.as_str().cmp(right.0.as_str()));
        }
        #[cfg(feature = "openinterest")]
        {
            for (key, value) in queues.open_interest.drain() {
                batch.open_interest.push((key, value));
                batch.count += 1;
            }
            batch
                .open_interest
                .sort_by(|left, right| left.0.as_str().cmp(right.0.as_str()));
        }
        #[cfg(feature = "ticker")]
        {
            for (key, value) in queues.ticker.drain() {
                batch.ticker.push((key, value));
                batch.count += 1;
            }
            batch
                .ticker
                .sort_by(|left, right| left.0.as_str().cmp(right.0.as_str()));
        }
        #[cfg(feature = "orderbook")]
        {
            for (key, value) in queues.l1_book.drain() {
                batch.l1_book.push((key, value));
                batch.count += 1;
            }
            batch
                .l1_book
                .sort_by(|left, right| left.0.as_str().cmp(right.0.as_str()));
            if let L2Queue::Coalesced(symbols) = &mut queues.l2 {
                for (symbol, ()) in symbols.drain() {
                    batch.l2_symbols.push(symbol);
                    batch.count += 1;
                }
                batch
                    .l2_symbols
                    .sort_by(|left, right| left.as_str().cmp(right.as_str()));
            }
        }

        #[cfg(any(feature = "liquidations", feature = "orderbook", feature = "trade"))]
        let mut budget = EVENT_BATCH;
        #[cfg(feature = "trade")]
        {
            let take = budget.min(queues.trade.len());
            for _ in 0..take {
                if let Some(value) = queues.trade.pop_front() {
                    batch.trade.push(value);
                    batch.count += 1;
                }
            }
            budget -= take;
        }
        #[cfg(feature = "liquidations")]
        {
            let take = budget.min(queues.liquidations.len());
            for _ in 0..take {
                if let Some(value) = queues.liquidations.pop_front() {
                    batch.liquidations.push(value);
                    batch.count += 1;
                }
            }
            budget -= take;
        }
        #[cfg(feature = "orderbook")]
        {
            if let L2Queue::Events(events) = &mut queues.l2 {
                let take = budget.min(events.len());
                for _ in 0..take {
                    if let Some(value) = events.pop_front() {
                        batch.l2_books.push(value);
                        batch.count += 1;
                    }
                }
                budget -= take;
            }
        }

        #[cfg(any(feature = "liquidations", feature = "orderbook", feature = "trade"))]
        let more = budget == 0 && {
            let mut pending = false;
            #[cfg(feature = "trade")]
            {
                pending |= !queues.trade.is_empty();
            }
            #[cfg(feature = "liquidations")]
            {
                pending |= !queues.liquidations.is_empty();
            }
            #[cfg(feature = "orderbook")]
            {
                pending |= match &queues.l2 {
                    L2Queue::Coalesced(symbols) => !symbols.is_empty(),
                    L2Queue::Events(events) => !events.is_empty(),
                };
            }
            pending
        };
        #[cfg(not(any(feature = "liquidations", feature = "orderbook", feature = "trade")))]
        let more = false;

        drop(queues);
        self.counters.queued_removed(batch.count);
        (batch, more)
    }

    #[cfg(feature = "orderbook")]
    fn applied_l2_snapshot(&self, symbol: &Symbol) -> Option<L2BookSnapshot> {
        self.books.as_ref()?.snapshot(self.identity?, symbol)
    }

    /// Delivers one batch in a fixed order: state channels first (ordered by
    /// key), then event channels in FIFO ring order.
    async fn deliver(&self, batch: Batch) {
        let deadline = self.deadline;
        #[cfg(feature = "candles")]
        for (_, candle) in batch.candles {
            self.counters.bump_delivered(Channel::Candles);
            deliver_candle(None, &self.handlers.candles, deadline, candle).await;
        }
        #[cfg(feature = "funding")]
        for (_, funding) in batch.funding {
            self.counters.bump_delivered(Channel::Funding);
            deliver_funding(None, &self.handlers.funding, deadline, funding).await;
        }
        #[cfg(feature = "index")]
        for (_, index_price) in batch.index {
            self.counters.bump_delivered(Channel::Index);
            deliver_index_price(None, &self.handlers.index, deadline, index_price).await;
        }
        #[cfg(feature = "orderbook")]
        for (_, book) in batch.l1_book {
            self.counters.bump_delivered(Channel::L1Book);
            deliver_l1_book(None, &self.handlers.orderbook, deadline, book).await;
        }
        #[cfg(feature = "markprice")]
        for (_, mark_price) in batch.mark_price {
            self.counters.bump_delivered(Channel::MarkPrice);
            deliver_mark_price(None, &self.handlers.mark_price, deadline, mark_price).await;
        }
        #[cfg(feature = "openinterest")]
        for (_, open_interest) in batch.open_interest {
            self.counters.bump_delivered(Channel::OpenInterest);
            deliver_open_interest(None, &self.handlers.open_interest, deadline, open_interest)
                .await;
        }
        #[cfg(feature = "ticker")]
        for (_, ticker) in batch.ticker {
            self.counters.bump_delivered(Channel::Ticker);
            deliver_ticker(None, &self.handlers.ticker, deadline, ticker).await;
        }
        #[cfg(feature = "orderbook")]
        for symbol in batch.l2_symbols {
            if let Some(snapshot) = self.applied_l2_snapshot(&symbol) {
                self.counters.bump_delivered(Channel::L2Book);
                deliver_l2_book(
                    None,
                    &self.handlers.orderbook,
                    deadline,
                    L2Book::Snapshot(snapshot),
                )
                .await;
            }
        }
        #[cfg(feature = "orderbook")]
        for book in batch.l2_books {
            self.counters.bump_delivered(Channel::L2Book);
            deliver_l2_book(None, &self.handlers.orderbook, deadline, book).await;
        }
        #[cfg(feature = "trade")]
        for trade in batch.trade {
            self.counters.bump_delivered(Channel::Trade);
            deliver_trade(None, &self.handlers.trade, deadline, trade).await;
        }
        #[cfg(feature = "liquidations")]
        for liquidation in batch.liquidations {
            self.counters.bump_delivered(Channel::Liquidations);
            deliver_liquidation(None, &self.handlers.liquidations, deadline, liquidation).await;
        }
    }
}

/// Per-feed asynchronous handler dispatcher; enqueue is synchronous.
pub(crate) struct HandlerDispatcher {
    inner: Arc<Inner>,
}

// The macro is expanded only for feature-enabled state channels.
#[allow(unused_macros)]
macro_rules! state_enqueue {
    ($name:ident, $queue_field:ident, $handler_field:ident, $channel:expr, $type:ty, $key:expr) => {
        /// Coalesces this state-channel event to the latest value per key.
        pub(crate) fn $name(&self, value: $type) {
            if self.inner.handlers.$handler_field.is_empty() {
                return;
            }
            let replaced = {
                let mut queues = self.inner.lock_queues();
                let key = $key(&value);
                queues.$queue_field.insert(key, value).is_some()
            };
            self.inner.note(
                $channel,
                if replaced {
                    EnqueueOutcome::Coalesced
                } else {
                    EnqueueOutcome::Added
                },
            );
        }
    };
}

// The macro is expanded only for feature-enabled event channels.
#[allow(unused_macros)]
macro_rules! event_enqueue {
    ($name:ident, $queue_field:ident, $handler_field:ident, $channel:expr, $type:ty) => {
        /// Enqueues this event-channel event, dropping the oldest on overflow.
        pub(crate) fn $name(&self, value: $type) {
            if self.inner.handlers.$handler_field.is_empty() {
                return;
            }
            let dropped = {
                let mut queues = self.inner.lock_queues();
                let mut dropped = false;
                while queues.$queue_field.len() >= self.inner.capacity {
                    dropped |= queues.$queue_field.pop_front().is_some();
                }
                queues.$queue_field.push_back(value);
                dropped
            };
            self.inner.note(
                $channel,
                if dropped {
                    EnqueueOutcome::Dropped
                } else {
                    EnqueueOutcome::Added
                },
            );
        }
    };
}

impl HandlerDispatcher {
    #[cfg(feature = "ticker")]
    state_enqueue!(
        enqueue_ticker,
        ticker,
        ticker,
        Channel::Ticker,
        Ticker,
        |value: &Ticker| value.symbol.clone()
    );

    #[cfg(feature = "candles")]
    state_enqueue!(
        enqueue_candle,
        candles,
        candles,
        Channel::Candles,
        Candle,
        |value: &Candle| (value.symbol.clone(), value.interval.clone())
    );

    #[cfg(feature = "funding")]
    state_enqueue!(
        enqueue_funding,
        funding,
        funding,
        Channel::Funding,
        Funding,
        |value: &Funding| value.symbol.clone()
    );

    #[cfg(feature = "markprice")]
    state_enqueue!(
        enqueue_mark_price,
        mark_price,
        mark_price,
        Channel::MarkPrice,
        MarkPrice,
        |value: &MarkPrice| value.symbol.clone()
    );

    #[cfg(feature = "openinterest")]
    state_enqueue!(
        enqueue_open_interest,
        open_interest,
        open_interest,
        Channel::OpenInterest,
        OpenInterest,
        |value: &OpenInterest| value.symbol.clone()
    );

    #[cfg(feature = "index")]
    state_enqueue!(
        enqueue_index_price,
        index,
        index,
        Channel::Index,
        IndexPrice,
        |value: &IndexPrice| value.symbol.clone()
    );

    #[cfg(feature = "orderbook")]
    state_enqueue!(
        enqueue_l1_book,
        l1_book,
        orderbook,
        Channel::L1Book,
        L1Book,
        |value: &L1Book| value.symbol.clone()
    );

    #[cfg(feature = "trade")]
    event_enqueue!(enqueue_trade, trade, trade, Channel::Trade, Trade);

    #[cfg(feature = "liquidations")]
    event_enqueue!(
        enqueue_liquidation,
        liquidations,
        liquidations,
        Channel::Liquidations,
        Liquidation
    );

    /// Enqueues an L2 book. With a configured book store the symbol is
    /// coalesced and the applied full snapshot is delivered at drain time;
    /// otherwise the raw event is kept in a bounded FIFO ring.
    #[cfg(feature = "orderbook")]
    pub(crate) fn enqueue_l2_book(&self, book: L2Book) {
        if self.inner.handlers.orderbook.is_empty() {
            return;
        }
        let outcome = {
            let mut queues = self.inner.lock_queues();
            match &mut queues.l2 {
                L2Queue::Coalesced(symbols) => {
                    if symbols.insert(book.symbol().clone(), ()).is_some() {
                        EnqueueOutcome::Coalesced
                    } else {
                        EnqueueOutcome::Added
                    }
                }
                L2Queue::Events(events) => {
                    let mut dropped = false;
                    while events.len() >= self.inner.capacity {
                        dropped |= events.pop_front().is_some();
                    }
                    events.push_back(book);
                    if dropped {
                        EnqueueOutcome::Dropped
                    } else {
                        EnqueueOutcome::Added
                    }
                }
            }
        };
        self.inner.note(Channel::L2Book, outcome);
    }

    fn counters(&self) -> Arc<DispatchCounters> {
        self.inner.counters.clone()
    }
}

/// Environment captured once per logical feed when constructing a dispatcher.
#[derive(Clone, Default)]
pub(crate) struct DispatchEnvironment {
    /// Retained L2 recovery store, when managed control created one.
    #[cfg(feature = "orderbook")]
    pub books: Option<Arc<BookStore>>,
}

/// Handle to the consumer task of one dispatcher.
pub(crate) struct DispatcherTask {
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl DispatcherTask {
    /// Requests a graceful stop and waits for the consumer, aborting it when
    /// the shared shutdown grace period expires.
    pub(crate) async fn shutdown(mut self) {
        let _ = self.stop.send(true);
        if tokio::time::timeout(super::SHUTDOWN_GRACE_PERIOD, &mut self.task)
            .await
            .is_err()
        {
            self.task.abort();
            let _ = (&mut self.task).await;
        }
    }
}

impl Drop for DispatcherTask {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        self.task.abort();
    }
}

/// Consumer tasks created for one runtime scope.
///
/// Dropping the registry aborts them; [`DispatchTasks::shutdown`] drains them
/// within the shared shutdown grace period.
#[derive(Default)]
pub(crate) struct DispatchTasks {
    tasks: Vec<DispatcherTask>,
}

impl DispatchTasks {
    /// Creates and spawns a dispatcher for one logical feed when async dispatch
    /// is configured and at least one handler is registered.
    pub(crate) fn attach(
        &mut self,
        feed: &mut ExchangeFeed,
        shutdown: &watch::Receiver<bool>,
        environment: &DispatchEnvironment,
    ) {
        if let Some(task) = attach(feed, shutdown, environment) {
            self.tasks.push(task);
        }
    }

    /// Drains every registered consumer within the shared shutdown grace.
    pub(crate) async fn shutdown(mut self) {
        for task in self.tasks.drain(..) {
            task.shutdown().await;
        }
    }
}

fn counters_for(feed: &ExchangeFeed) -> Arc<DispatchCounters> {
    feed.dispatch_counters
        .clone()
        .unwrap_or_else(|| Arc::new(DispatchCounters::default()))
}

fn build_dispatcher(
    feed: &ExchangeFeed,
    shutdown: &watch::Receiver<bool>,
    environment: &DispatchEnvironment,
) -> Option<(Arc<HandlerDispatcher>, DispatcherTask)> {
    let _ = environment;
    let capacity = feed.runtime_options.handler_dispatch_policy().capacity()?;
    let handlers = Handlers::capture(feed);
    if handlers.is_empty() {
        return None;
    }
    let counters = counters_for(feed);

    #[cfg(feature = "orderbook")]
    let books = environment.books.clone();
    #[cfg(feature = "orderbook")]
    let coalesce_l2 = books.is_some() && feed.identity.is_some();

    #[cfg(feature = "orderbook")]
    let queues = {
        let mut queues = Queues::default();
        if !coalesce_l2 {
            queues.l2 = L2Queue::Events(VecDeque::new());
        }
        queues
    };
    #[cfg(not(feature = "orderbook"))]
    let queues = Queues::default();

    let inner = Arc::new(Inner {
        capacity,
        deadline: feed.runtime_options.callback_deadline(),
        #[cfg(feature = "orderbook")]
        books,
        #[cfg(feature = "orderbook")]
        identity: feed.identity,
        handlers,
        queues: Mutex::new(queues),
        notify: Notify::new(),
        counters,
        last_drop_warning: Mutex::new(None),
    });
    let (stop, stop_rx) = watch::channel(false);
    let task = tokio::spawn(consume(inner.clone(), shutdown.clone(), stop_rx));
    Some((
        Arc::new(HandlerDispatcher { inner }),
        DispatcherTask { stop, task },
    ))
}

/// Attaches a dispatcher to one logical feed and returns its consumer task.
pub(crate) fn attach(
    feed: &mut ExchangeFeed,
    shutdown: &watch::Receiver<bool>,
    environment: &DispatchEnvironment,
) -> Option<DispatcherTask> {
    let (dispatcher, task) = build_dispatcher(feed, shutdown, environment)?;
    feed.dispatch_counters = Some(dispatcher.counters());
    feed.handler_dispatcher = Some(dispatcher);
    Some(task)
}

/// Attaches one dispatcher shared across all connection shards of a logical
/// feed (managed runtime worker scope).
pub(crate) fn attach_shared(
    feeds: &mut [ExchangeFeed],
    shutdown: &watch::Receiver<bool>,
    environment: &DispatchEnvironment,
) -> DispatchTasks {
    let mut tasks = DispatchTasks::default();
    let Some(first) = feeds.first() else {
        return tasks;
    };
    let Some((dispatcher, task)) = build_dispatcher(first, shutdown, environment) else {
        return tasks;
    };
    let counters = dispatcher.counters();
    for feed in feeds.iter_mut() {
        feed.dispatch_counters = Some(counters.clone());
        feed.handler_dispatcher = Some(dispatcher.clone());
    }
    tasks.tasks.push(task);
    tasks
}

async fn shutdown_signal(receiver: &mut watch::Receiver<bool>) {
    loop {
        if *receiver.borrow() {
            return;
        }
        if receiver.changed().await.is_err() {
            return;
        }
    }
}

async fn consume(
    inner: Arc<Inner>,
    mut external: watch::Receiver<bool>,
    mut stop: watch::Receiver<bool>,
) {
    loop {
        let (batch, more) = inner.take_batch();
        if !batch.is_empty() {
            inner.deliver(batch).await;
        }
        if more {
            continue;
        }
        tokio::select! {
            biased;
            () = shutdown_signal(&mut external) => break,
            () = shutdown_signal(&mut stop) => break,
            () = inner.notify.notified() => {}
        }
    }
    // Best-effort flush after a stop request: queued entries that fit in the
    // remaining shutdown grace are still delivered.
    for _ in 0..FLUSH_PASSES {
        let (batch, more) = inner.take_batch();
        let empty = batch.is_empty();
        if !empty {
            inner.deliver(batch).await;
        }
        if empty && !more {
            break;
        }
    }
}

#[cfg(all(
    test,
    any(feature = "orderbook", feature = "ticker", feature = "trade")
))]
mod tests {
    use super::*;
    use crate::exchange::binance::Binance;
    use crate::feed::FeedHandler;
    use crate::options::{HandlerDispatch, RuntimeOptions};
    use cryptofeed_core::exchange::ExchangeId;
    use cryptofeed_core::symbol::Symbol;
    use std::collections::HashSet;
    use tokio::sync::Semaphore;

    async fn wait_until(mut condition: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !condition() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("condition met before timeout");
    }

    fn attachment(
        mut feed: ExchangeFeed,
        environment: DispatchEnvironment,
    ) -> (
        ExchangeFeed,
        Arc<DispatchCounters>,
        DispatcherTask,
        watch::Sender<bool>,
    ) {
        let counters = match &feed.dispatch_counters {
            Some(counters) => counters.clone(),
            None => {
                let counters = Arc::new(DispatchCounters::default());
                feed.dispatch_counters = Some(counters.clone());
                counters
            }
        };
        let (stop, shutdown) = watch::channel(false);
        let task = attach(&mut feed, &shutdown, &environment).expect("dispatcher attached");
        (feed, counters, task, stop)
    }

    #[test]
    fn inline_or_handlerless_feeds_create_no_dispatcher() {
        let mut inline = Binance::new().symbol("BTC-USDT").build();
        let (_, shutdown) = watch::channel(false);
        assert!(attach(&mut inline, &shutdown, &DispatchEnvironment::default()).is_none());

        let async_options = RuntimeOptions::default()
            .handler_dispatch(HandlerDispatch::Async { capacity: 8 })
            .unwrap();
        let mut handlerless = Binance::new()
            .symbol("BTC-USDT")
            .runtime_options(async_options)
            .build();
        assert!(attach(&mut handlerless, &shutdown, &DispatchEnvironment::default()).is_none());
    }

    #[cfg(feature = "ticker")]
    struct GatedTickerHandler {
        seen: Arc<Mutex<Vec<Ticker>>>,
        started: Arc<Semaphore>,
        release: Arc<Semaphore>,
    }

    #[cfg(feature = "ticker")]
    #[async_trait::async_trait]
    impl TickerHandler for GatedTickerHandler {
        async fn on_ticker(&self, ticker: Ticker) {
            self.started.add_permits(1);
            self.release.acquire().await.expect("release").forget();
            self.seen.lock().expect("seen lock").push(ticker);
        }
    }

    #[cfg(feature = "ticker")]
    fn ticker_with_bid(bid: u64) -> Ticker {
        Ticker {
            exchange: ExchangeId::Binance,
            symbol: Symbol::spot("BTC", "USDT"),
            bid: rust_decimal::Decimal::from(bid),
            ask: rust_decimal::Decimal::from(bid + 1),
            exchange_ts: bid as f64,
            received_ts: bid as f64,
            implied_volatility: None,
        }
    }

    #[cfg(feature = "ticker")]
    #[tokio::test]
    async fn state_channel_coalesces_to_latest_value() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let started = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let options = RuntimeOptions::default()
            .handler_dispatch(HandlerDispatch::Async { capacity: 8 })
            .unwrap();
        let feed = Binance::new()
            .ticker()
            .symbol("BTC-USDT")
            .runtime_options(options)
            .ticker_handler(Arc::new(GatedTickerHandler {
                seen: seen.clone(),
                started: started.clone(),
                release: release.clone(),
            }))
            .build();
        let (feed, counters, task, _stop) = attachment(feed, DispatchEnvironment::default());
        let dispatcher = feed.handler_dispatcher.as_ref().expect("dispatcher");

        // The first update is taken by the consumer and blocks the handler.
        dispatcher.enqueue_ticker(ticker_with_bid(1));
        started.acquire().await.expect("started").forget();
        // Later updates for the same key coalesce to the newest value.
        dispatcher.enqueue_ticker(ticker_with_bid(2));
        dispatcher.enqueue_ticker(ticker_with_bid(3));
        release.add_permits(2);

        wait_until(|| seen.lock().expect("seen lock").len() == 2).await;
        let bids: Vec<_> = seen
            .lock()
            .expect("seen lock")
            .iter()
            .map(|ticker| ticker.bid)
            .collect();
        assert_eq!(
            bids,
            [
                rust_decimal::Decimal::from(1),
                rust_decimal::Decimal::from(3)
            ]
        );
        let stats = counters.snapshot();
        assert_eq!(stats.delivered(Channel::Ticker), 2);
        assert_eq!(stats.coalesced(Channel::Ticker), 1);
        assert_eq!(stats.dropped_oldest(Channel::Ticker), 0);
        assert_eq!(stats.queued(), 0);
        task.shutdown().await;
    }

    #[cfg(feature = "trade")]
    struct GatedTradeHandler {
        seen: Arc<Mutex<Vec<Option<String>>>>,
        started: Arc<Semaphore>,
        release: Arc<Semaphore>,
    }

    #[cfg(feature = "trade")]
    #[async_trait::async_trait]
    impl TradeHandler for GatedTradeHandler {
        async fn on_trade(&self, trade: Trade) {
            self.started.add_permits(1);
            self.release.acquire().await.expect("release").forget();
            self.seen.lock().expect("seen lock").push(trade.id);
        }
    }

    #[cfg(feature = "trade")]
    fn trade_with_id(id: u64) -> Trade {
        Trade {
            exchange: ExchangeId::Binance,
            symbol: Symbol::spot("BTC", "USDT"),
            side: cryptofeed_trade::Side::Buy,
            amount: rust_decimal::Decimal::ONE,
            price: rust_decimal::Decimal::ONE,
            exchange_ts: id as f64,
            received_ts: id as f64,
            id: Some(id.to_string()),
            implied_volatility: None,
        }
    }

    #[cfg(feature = "trade")]
    #[tokio::test]
    async fn event_channel_ring_drops_oldest_and_counts() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let started = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let options = RuntimeOptions::default()
            .handler_dispatch(HandlerDispatch::Async { capacity: 2 })
            .unwrap();
        let feed = Binance::new()
            .trade()
            .symbol("BTC-USDT")
            .runtime_options(options)
            .trade_handler(Arc::new(GatedTradeHandler {
                seen: seen.clone(),
                started: started.clone(),
                release: release.clone(),
            }))
            .build();
        let (feed, counters, task, _stop) = attachment(feed, DispatchEnvironment::default());
        let dispatcher = feed.handler_dispatcher.as_ref().expect("dispatcher");

        dispatcher.enqueue_trade(trade_with_id(1));
        started.acquire().await.expect("started").forget();
        // The handler is blocked, so the ring overflows with the newest kept.
        for id in 2..=4 {
            dispatcher.enqueue_trade(trade_with_id(id));
        }
        release.add_permits(3);

        wait_until(|| seen.lock().expect("seen lock").len() == 3).await;
        assert_eq!(
            *seen.lock().expect("seen lock"),
            [
                Some("1".to_owned()),
                Some("3".to_owned()),
                Some("4".to_owned())
            ]
        );
        let stats = counters.snapshot();
        assert_eq!(stats.delivered(Channel::Trade), 3);
        assert_eq!(stats.dropped_oldest(Channel::Trade), 1);
        assert_eq!(stats.coalesced(Channel::Trade), 0);
        assert_eq!(stats.queued(), 0);
        task.shutdown().await;
    }

    #[cfg(feature = "orderbook")]
    struct GatedBookHandler {
        seen: Arc<Mutex<Vec<L2Book>>>,
        started: Arc<Semaphore>,
        release: Arc<Semaphore>,
    }

    #[cfg(feature = "orderbook")]
    #[async_trait::async_trait]
    impl OrderBookHandler for GatedBookHandler {
        async fn on_l2_book(&self, book: L2Book) {
            self.started.add_permits(1);
            self.release.acquire().await.expect("release").forget();
            self.seen.lock().expect("seen lock").push(book);
        }

        async fn on_l1_book(&self, _book: L1Book) {
            unreachable!("l1 is not exercised by this test");
        }
    }

    #[cfg(feature = "orderbook")]
    fn l2_snapshot(symbol: &Symbol, exchange_ts: f64, bid: u64) -> L2Book {
        L2Book::Snapshot(L2BookSnapshot {
            exchange: ExchangeId::Binance,
            symbol: symbol.clone(),
            bids: vec![cryptofeed_orderbook::PriceLevel {
                price: rust_decimal::Decimal::from(bid),
                amount: rust_decimal::Decimal::ONE,
            }],
            asks: vec![cryptofeed_orderbook::PriceLevel {
                price: rust_decimal::Decimal::from(bid + 1),
                amount: rust_decimal::Decimal::ONE,
            }],
            exchange_ts,
            received_ts: exchange_ts,
        })
    }

    #[cfg(feature = "orderbook")]
    #[tokio::test]
    async fn l2_with_book_store_coalesces_applied_snapshots() {
        let symbol = Symbol::spot("BTC", "USDT");
        let identity = FeedIdentity {
            id: crate::feed::FeedId::allocate(),
            generation: 1,
        };
        let store = Arc::new(BookStore::default());
        store.activate(identity);
        store.begin(identity, 1, 1, &HashSet::from([symbol.as_str().to_owned()]));
        store.apply(identity, 1, 1, &l2_snapshot(&symbol, 1.0, 100));

        let seen = Arc::new(Mutex::new(Vec::new()));
        let started = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let options = RuntimeOptions::default()
            .handler_dispatch(HandlerDispatch::Async { capacity: 8 })
            .unwrap();
        let mut feed = Binance::new()
            .l2_book()
            .symbol("BTC-USDT")
            .runtime_options(options)
            .orderbook_handler(Arc::new(GatedBookHandler {
                seen: seen.clone(),
                started: started.clone(),
                release: release.clone(),
            }))
            .build();
        feed.identity = Some(identity);
        let (feed, counters, task, _stop) = attachment(
            feed,
            DispatchEnvironment {
                books: Some(store.clone()),
            },
        );
        let dispatcher = feed.handler_dispatcher.as_ref().expect("dispatcher");

        dispatcher.enqueue_l2_book(l2_snapshot(&symbol, 1.0, 100));
        started.acquire().await.expect("started").forget();
        // The applied state advances while the handler is busy; two pending
        // updates for one symbol coalesce so only the latest applied full
        // snapshot is delivered, not intermediate deltas.
        store.apply(identity, 1, 1, &l2_snapshot(&symbol, 2.0, 200));
        dispatcher.enqueue_l2_book(l2_snapshot(&symbol, 2.0, 200));
        store.apply(identity, 1, 1, &l2_snapshot(&symbol, 3.0, 300));
        dispatcher.enqueue_l2_book(l2_snapshot(&symbol, 3.0, 300));
        release.add_permits(2);

        wait_until(|| seen.lock().expect("seen lock").len() == 2).await;
        {
            let books = seen.lock().expect("seen lock");
            assert!(matches!(books[0], L2Book::Snapshot(_)));
            assert!(matches!(books[1], L2Book::Snapshot(_)));
            assert_eq!(books[0].exchange_ts(), 1.0);
            assert_eq!(books[1].exchange_ts(), 3.0);
            assert_eq!(books[1].bids()[0].price, rust_decimal::Decimal::from(300));
        }
        let stats = counters.snapshot();
        assert_eq!(stats.delivered(Channel::L2Book), 2);
        assert_eq!(stats.coalesced(Channel::L2Book), 1);
        assert_eq!(stats.dropped_oldest(Channel::L2Book), 0);
        task.shutdown().await;
    }

    #[cfg(feature = "orderbook")]
    #[tokio::test]
    async fn l2_without_book_store_is_a_bounded_raw_event_ring() {
        let symbol = Symbol::spot("BTC", "USDT");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let started = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let options = RuntimeOptions::default()
            .handler_dispatch(HandlerDispatch::Async { capacity: 2 })
            .unwrap();
        let feed = Binance::new()
            .l2_book()
            .symbol("BTC-USDT")
            .runtime_options(options)
            .orderbook_handler(Arc::new(GatedBookHandler {
                seen: seen.clone(),
                started: started.clone(),
                release: release.clone(),
            }))
            .build();
        let (feed, counters, task, _stop) = attachment(feed, DispatchEnvironment::default());
        let dispatcher = feed.handler_dispatcher.as_ref().expect("dispatcher");

        dispatcher.enqueue_l2_book(l2_snapshot(&symbol, 1.0, 100));
        started.acquire().await.expect("started").forget();
        for ts in [2.0, 3.0, 4.0] {
            dispatcher.enqueue_l2_book(l2_snapshot(&symbol, ts, ts as u64 * 100));
        }
        release.add_permits(3);

        wait_until(|| seen.lock().expect("seen lock").len() == 3).await;
        let timestamps: Vec<_> = seen
            .lock()
            .expect("seen lock")
            .iter()
            .map(L2Book::exchange_ts)
            .collect();
        assert_eq!(timestamps, [1.0, 3.0, 4.0]);
        let stats = counters.snapshot();
        assert_eq!(stats.delivered(Channel::L2Book), 3);
        assert_eq!(stats.dropped_oldest(Channel::L2Book), 1);
        task.shutdown().await;
    }

    #[cfg(feature = "ticker")]
    #[tokio::test]
    async fn dispatcher_shutdown_ends_consumer_task() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let started = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let options = RuntimeOptions::default()
            .handler_dispatch(HandlerDispatch::Async { capacity: 4 })
            .unwrap();
        let feed = Binance::new()
            .ticker()
            .symbol("BTC-USDT")
            .runtime_options(options)
            .ticker_handler(Arc::new(GatedTickerHandler {
                seen,
                started,
                release,
            }))
            .build();
        let (feed, _counters, task, stop) = attachment(feed, DispatchEnvironment::default());
        let dispatcher = feed.handler_dispatcher.as_ref().expect("dispatcher");
        dispatcher.enqueue_ticker(ticker_with_bid(1));
        tokio::time::timeout(Duration::from_secs(1), task.shutdown())
            .await
            .expect("consumer stopped");
        drop(stop);
    }

    #[cfg(feature = "ticker")]
    #[tokio::test]
    async fn one_dispatcher_is_shared_by_all_connection_shards() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let started = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let options = RuntimeOptions::default()
            .handler_dispatch(HandlerDispatch::Async { capacity: 8 })
            .unwrap();
        let logical = Binance::new()
            .ticker()
            .symbol("BTC-USDT")
            .symbol("ETH-USDT")
            .runtime_options(options)
            .ticker_handler(Arc::new(GatedTickerHandler {
                seen: seen.clone(),
                started: started.clone(),
                release: release.clone(),
            }))
            .build();
        let counters = Arc::new(DispatchCounters::default());
        let mut shards = vec![logical.clone(), logical];
        for shard in &mut shards {
            shard.dispatch_counters = Some(counters.clone());
        }
        let (stop, shutdown) = watch::channel(false);
        let tasks = attach_shared(&mut shards, &shutdown, &DispatchEnvironment::default());
        let first = shards[0].handler_dispatcher.clone().expect("dispatcher");
        let second = shards[1].handler_dispatcher.clone().expect("dispatcher");
        assert!(Arc::ptr_eq(&first, &second));

        first.enqueue_ticker(ticker_with_bid(1));
        started.acquire().await.expect("started").forget();
        second.enqueue_ticker(ticker_with_bid(2));
        release.add_permits(2);
        wait_until(|| seen.lock().expect("seen lock").len() == 2).await;
        assert_eq!(counters.snapshot().delivered(Channel::Ticker), 2);
        tasks.shutdown().await;
        drop(stop);
    }

    #[cfg(feature = "ticker")]
    #[tokio::test]
    async fn feed_handler_exposes_dispatch_counters() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let started = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let options = RuntimeOptions::default()
            .handler_dispatch(HandlerDispatch::Async { capacity: 4 })
            .unwrap();
        let feed = Binance::new()
            .ticker()
            .symbol("BTC-USDT")
            .runtime_options(options)
            .ticker_handler(Arc::new(GatedTickerHandler {
                seen,
                started,
                release: release.clone(),
            }))
            .build();
        let mut handler = FeedHandler::new();
        handler.add_feed(feed);
        let stats_handle = handler.handler_dispatch_stats_handle();
        assert_eq!(stats_handle.snapshot(), HandlerDispatchStats::default());
        let (feed, _counters, task, _stop) = attachment(
            handler.into_feeds().pop().expect("feed"),
            DispatchEnvironment::default(),
        );
        let dispatcher = feed.handler_dispatcher.as_ref().expect("dispatcher");
        dispatcher.enqueue_ticker(ticker_with_bid(1));
        release.add_permits(1);
        wait_until(|| stats_handle.snapshot().delivered(Channel::Ticker) == 1).await;
        task.shutdown().await;
    }
}
