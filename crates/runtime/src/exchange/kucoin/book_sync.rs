//! KuCoin `obu` order-book sequencing.
//!
//! `depth=increment@10ms` starts with a snapshot and continues with deltas
//! whose `O`/`C` range is contiguous: `O` is the first sequence of the push and
//! `C` the last. A delta whose `O` is greater than the local anchor + 1 is a
//! gap; the caller enters the bounded in-session resnapshot path
//! (unsubscribe/subscribe, so the next push is a documented snapshot) and
//! buffers deltas meanwhile. A delta whose `C` is not greater than the anchor
//! is stale and dropped. `depth=1/5/50` pushes are snapshot-only and replace
//! the local book on every push; a delta on those subscriptions is a protocol
//! violation and fails closed.

use std::collections::VecDeque;

use cryptofeed_core::error::{Error, Result};
use cryptofeed_core::symbol::Symbol;
use cryptofeed_orderbook::{L2Book, L2BookState};

use super::parser::BookMessage;

/// Cumulative resnapshot requests per connection before the existing
/// reconnect/backoff supervisor takes over. Success does not refund the
/// budget: a snapshot followed by an immediate gap must not loop forever.
pub const MAX_KUCOIN_RESNAPSHOTS: u32 = 3;
const MAX_BUFFERED_DELTAS: usize = 512;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KucoinBookMode {
    /// `increment@10ms`: snapshot followed by sequenced deltas.
    Incremental,
    /// `1`/`5`/`50`: every push is a full replacement.
    SnapshotOnly,
}

impl KucoinBookMode {
    pub fn for_depth(depth: Option<u16>) -> Self {
        if depth.is_some() {
            KucoinBookMode::SnapshotOnly
        } else {
            KucoinBookMode::Incremental
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum KucoinBookOutcome {
    /// Ordered events to publish: a snapshot followed by any bridged deltas.
    Publish(Vec<L2Book>),
    /// No publishable event (buffered delta or stale delta).
    Ignored,
    /// The caller must send the documented unsubscribe/subscribe pair so the
    /// next push is a snapshot.
    Resnapshot,
}

pub struct KucoinBookSync {
    symbol: Symbol,
    mode: KucoinBookMode,
    state: L2BookState,
    anchor: Option<u64>,
    awaiting_snapshot: bool,
    pending: VecDeque<BookMessage>,
    resnapshot_requests: u32,
}

impl KucoinBookSync {
    pub fn new(symbol: Symbol, mode: KucoinBookMode) -> Self {
        Self {
            state: L2BookState::new(symbol.clone()),
            symbol,
            mode,
            anchor: None,
            awaiting_snapshot: false,
            pending: VecDeque::new(),
            resnapshot_requests: 0,
        }
    }

    pub fn state(&self) -> &L2BookState {
        &self.state
    }

    pub fn anchor(&self) -> Option<u64> {
        self.anchor
    }

    pub fn awaiting_snapshot(&self) -> bool {
        self.awaiting_snapshot
    }

    pub fn resnapshot_requests(&self) -> u32 {
        self.resnapshot_requests
    }

    pub fn apply(&mut self, message: BookMessage) -> Result<KucoinBookOutcome> {
        match self.mode {
            KucoinBookMode::SnapshotOnly => {
                if !message.snapshot {
                    return Err(Error::Parse(
                        "kucoin obu delta on a snapshot-only depth subscription".to_owned(),
                    ));
                }
                self.state.apply(message.book.clone());
                self.anchor = message.last_seq.or(message.first_seq);
                Ok(KucoinBookOutcome::Publish(vec![message.book]))
            }
            KucoinBookMode::Incremental => {
                if message.snapshot {
                    return self.apply_snapshot(message);
                }
                self.apply_delta(message)
            }
        }
    }

    fn apply_snapshot(&mut self, message: BookMessage) -> Result<KucoinBookOutcome> {
        let anchor = message
            .last_seq
            .or(message.first_seq)
            .ok_or_else(|| Error::Parse("kucoin obu snapshot is missing C".to_owned()))?;
        self.state.apply(message.book.clone());
        self.anchor = Some(anchor);
        self.awaiting_snapshot = false;
        let mut events = vec![message.book];

        while let Some(buffered) = self.pending.pop_front() {
            let Some(last) = buffered.last_seq else {
                self.pending.clear();
                return self.reset_for_resnapshot();
            };
            if last <= anchor {
                // The buffered delta belongs to the replaced stream.
                continue;
            }
            let first = buffered
                .first_seq
                .ok_or_else(|| Error::Parse("kucoin obu delta is missing O".to_owned()))?;
            if first > self.anchor.unwrap_or(anchor) + 1 {
                self.pending.clear();
                return self.reset_for_resnapshot();
            }
            self.state.apply(buffered.book.clone());
            self.anchor = Some(last);
            events.push(buffered.book);
        }

        Ok(KucoinBookOutcome::Publish(events))
    }

    fn apply_delta(&mut self, message: BookMessage) -> Result<KucoinBookOutcome> {
        let (Some(first), Some(last)) = (message.first_seq, message.last_seq) else {
            return Err(Error::Parse(
                "kucoin obu delta is missing its O/C sequence range".to_owned(),
            ));
        };
        if self.awaiting_snapshot {
            self.buffer(message);
            return Ok(KucoinBookOutcome::Ignored);
        }
        let Some(anchor) = self.anchor else {
            // The documented incremental stream always starts with a snapshot;
            // recover through the bounded resnapshot path instead of applying
            // a delta to an empty book.
            self.buffer(message);
            return self.reset_for_resnapshot();
        };
        if last <= anchor {
            return Ok(KucoinBookOutcome::Ignored);
        }
        if first > anchor + 1 {
            self.buffer(message);
            return self.reset_for_resnapshot();
        }
        self.state.apply(message.book.clone());
        self.anchor = Some(last);
        Ok(KucoinBookOutcome::Publish(vec![message.book]))
    }

    fn buffer(&mut self, message: BookMessage) {
        if self.pending.len() >= MAX_BUFFERED_DELTAS {
            self.pending.pop_front();
        }
        self.pending.push_back(message);
    }

    fn reset_for_resnapshot(&mut self) -> Result<KucoinBookOutcome> {
        self.resnapshot_requests += 1;
        if self.resnapshot_requests > MAX_KUCOIN_RESNAPSHOTS {
            return Err(Error::Parse(
                "kucoin book resnapshot request limit exceeded".to_owned(),
            ));
        }
        // Nothing stale may be published while the replacement snapshot is in
        // flight.
        self.state = L2BookState::new(self.symbol.clone());
        self.anchor = None;
        self.awaiting_snapshot = true;
        Ok(KucoinBookOutcome::Resnapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::{KucoinBookMode, KucoinBookOutcome, KucoinBookSync, MAX_KUCOIN_RESNAPSHOTS};
    use crate::exchange::kucoin::parser::BookMessage;
    use cryptofeed_core::{error::Error, exchange::ExchangeId, symbol::Symbol};
    use cryptofeed_orderbook::{L2Book, L2BookDelta, L2BookSnapshot, PriceLevel};
    use rust_decimal::Decimal;

    fn level(price: &str, amount: &str) -> PriceLevel {
        PriceLevel {
            price: Decimal::from_str_exact(price).unwrap(),
            amount: Decimal::from_str_exact(amount).unwrap(),
        }
    }

    fn symbol() -> Symbol {
        Symbol::spot("BTC", "USDT")
    }

    fn snapshot(seq: u64, bid: &str, amount: &str) -> BookMessage {
        BookMessage {
            symbol: symbol(),
            snapshot: true,
            first_seq: Some(seq),
            last_seq: Some(seq),
            book: L2Book::Snapshot(L2BookSnapshot {
                exchange: ExchangeId::Kucoin,
                symbol: symbol(),
                bids: vec![level(bid, amount)],
                asks: vec![level("65000.20", "0.75")],
                exchange_ts: 1.0,
                received_ts: 2.0,
            }),
        }
    }

    fn delta(first: u64, last: u64, bid: &str, amount: &str) -> BookMessage {
        BookMessage {
            symbol: symbol(),
            snapshot: false,
            first_seq: Some(first),
            last_seq: Some(last),
            book: L2Book::Delta(L2BookDelta {
                exchange: ExchangeId::Kucoin,
                symbol: symbol(),
                bids: vec![level(bid, amount)],
                asks: vec![],
                exchange_ts: 1.0,
                received_ts: 2.0,
            }),
        }
    }

    fn incremental() -> KucoinBookSync {
        KucoinBookSync::new(symbol(), KucoinBookMode::Incremental)
    }

    fn published(outcome: KucoinBookOutcome) -> Vec<L2Book> {
        match outcome {
            KucoinBookOutcome::Publish(events) => events,
            other => panic!("expected publish, got {other:?}"),
        }
    }

    #[test]
    fn snapshot_then_contiguous_delta_advances_the_anchor() {
        let mut sync = incremental();
        let events = published(
            sync.apply(snapshot(100, "64999.10", "1.25"))
                .expect("snapshot"),
        );
        assert_eq!(events.len(), 1);
        assert_eq!(sync.anchor(), Some(100));

        let events = published(
            sync.apply(delta(101, 102, "64998.50", "2.00"))
                .expect("delta"),
        );
        assert_eq!(events.len(), 1);
        assert_eq!(sync.anchor(), Some(102));
        assert_eq!(sync.state().bids().len(), 2);
    }

    #[test]
    fn stale_delta_is_dropped() {
        let mut sync = incremental();
        sync.apply(snapshot(100, "64999.10", "1.25"))
            .expect("snapshot");
        assert_eq!(
            sync.apply(delta(90, 100, "1.00", "9")).expect("stale"),
            KucoinBookOutcome::Ignored
        );
        assert_eq!(sync.anchor(), Some(100));
        assert_eq!(sync.state().bids().len(), 1);
    }

    #[test]
    fn gap_requests_a_bounded_resnapshot() {
        let mut sync = incremental();
        sync.apply(snapshot(100, "64999.10", "1.25"))
            .expect("snapshot");
        assert_eq!(
            sync.apply(delta(105, 106, "64900.00", "1.0")).expect("gap"),
            KucoinBookOutcome::Resnapshot
        );
        assert!(sync.awaiting_snapshot());
        assert!(sync.state().bids().is_empty());
        assert_eq!(sync.resnapshot_requests(), 1);

        // Each recovered gap consumes one request; success does not refund the
        // budget, so a snapshot -> immediate-gap loop cannot continue forever.
        for attempt in 2..=MAX_KUCOIN_RESNAPSHOTS {
            let seq = u64::from(attempt) * 100;
            let events = published(
                sync.apply(snapshot(seq, "64999.00", "0.5"))
                    .expect("replacement snapshot"),
            );
            assert_eq!(events.len(), 1);
            assert_eq!(
                sync.apply(delta(seq + 10, seq + 11, "64900.00", "1.0"))
                    .expect("gap"),
                KucoinBookOutcome::Resnapshot
            );
        }
        sync.apply(snapshot(400, "64999.00", "0.5"))
            .expect("replacement snapshot");
        assert!(matches!(
            sync.apply(delta(900, 901, "64900.00", "1.0")),
            Err(Error::Parse(message)) if message.contains("resnapshot request limit")
        ));
    }

    #[test]
    fn buffered_deltas_bridge_the_replacement_snapshot() {
        let mut sync = incremental();
        sync.apply(snapshot(100, "64999.10", "1.25"))
            .expect("snapshot");
        // Gap: this delta is buffered and the snapshot is requested.
        assert_eq!(
            sync.apply(delta(105, 106, "64900.00", "1.0")).expect("gap"),
            KucoinBookOutcome::Resnapshot
        );
        // The replacement snapshot starts at 105; the buffered delta bridges
        // exactly and is applied after it.
        let events = published(
            sync.apply(snapshot(104, "64999.00", "0.5"))
                .expect("snapshot"),
        );
        assert_eq!(events.len(), 2);
        assert!(matches!(events[0], L2Book::Snapshot(_)));
        assert!(matches!(events[1], L2Book::Delta(_)));
        assert_eq!(sync.anchor(), Some(106));
        assert!(!sync.awaiting_snapshot());
    }

    #[test]
    fn buffered_deltas_from_the_replaced_stream_are_discarded() {
        let mut sync = incremental();
        sync.apply(snapshot(100, "64999.10", "1.25"))
            .expect("snapshot");
        sync.apply(delta(105, 106, "64900.00", "1.0")).expect("gap");
        // The replacement snapshot is far ahead of the stale buffered delta.
        let events = published(
            sync.apply(snapshot(200, "64999.00", "0.5"))
                .expect("snapshot"),
        );
        assert_eq!(events.len(), 1);
        assert_eq!(sync.anchor(), Some(200));
        assert_eq!(sync.state().bids().len(), 1);
    }

    #[test]
    fn snapshot_replacement_clears_stale_levels() {
        let mut sync = incremental();
        sync.apply(snapshot(100, "64999.10", "1.25"))
            .expect("snapshot");
        sync.apply(snapshot(200, "64998.00", "2.0"))
            .expect("replacement");
        assert_eq!(sync.state().bids().len(), 1);
        assert_eq!(sync.state().bids()[0].price.to_string(), "64998.00");
        assert_eq!(sync.anchor(), Some(200));
    }

    #[test]
    fn snapshot_only_depth_replaces_and_rejects_deltas() {
        let mut sync = KucoinBookSync::new(symbol(), KucoinBookMode::SnapshotOnly);
        let events = published(
            sync.apply(snapshot(100, "64999.10", "1.25"))
                .expect("snapshot"),
        );
        assert_eq!(events.len(), 1);
        let events = published(
            sync.apply(snapshot(101, "64998.00", "2.0"))
                .expect("snapshot"),
        );
        assert_eq!(events.len(), 1);
        assert_eq!(sync.state().bids()[0].price.to_string(), "64998.00");
        assert!(matches!(
            sync.apply(delta(102, 103, "1.00", "1")),
            Err(Error::Parse(message)) if message.contains("snapshot-only")
        ));
    }

    #[test]
    fn mode_follows_the_configured_depth() {
        assert_eq!(KucoinBookMode::for_depth(None), KucoinBookMode::Incremental);
        for depth in [1u16, 5, 50] {
            assert_eq!(
                KucoinBookMode::for_depth(Some(depth)),
                KucoinBookMode::SnapshotOnly
            );
        }
    }
}
