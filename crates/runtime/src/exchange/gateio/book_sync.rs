use cryptofeed_core::error::{Error, Result};
use cryptofeed_core::symbol::Symbol;
use cryptofeed_orderbook::{L2Book, L2BookDelta, L2BookState};

#[derive(Clone, Debug)]
pub struct GateioDepthDelta {
    pub first_update_id: u64,
    pub last_update_id: u64,
    /// Exchange generation timestamp (seconds) of the delta (`result.t`).
    pub ts: f64,
    pub book: L2BookDelta,
}

/// One order-book update push. Gate.io periodically sends `full: true`
/// pushes carrying the complete book (announcements 44678/44722, mainnet
/// 2026-05-06); the client must overwrite its local book and re-anchor the
/// sequence at the push's `u`.
#[derive(Clone, Debug)]
pub enum GateioBookUpdate {
    Delta(GateioDepthDelta),
    Full {
        snapshot: cryptofeed_orderbook::L2BookSnapshot,
        last_update_id: u64,
    },
}

#[derive(Clone, Debug)]
pub struct GateioBookSnapshot {
    pub last_update_id: Option<u64>,
    pub generated_ts: f64,
    pub update_ts: f64,
    pub book: cryptofeed_orderbook::L2BookSnapshot,
}

#[derive(Clone, Debug)]
pub struct GateioBookSync {
    state: L2BookState,
    last_update_id: Option<u64>,
}

impl GateioBookSync {
    pub fn new(symbol: Symbol) -> Self {
        Self {
            state: L2BookState::new(symbol),
            last_update_id: None,
        }
    }

    /// Applies the snapshot and every buffered delta that bridges it,
    /// returning the full event sequence (snapshot first, then the applied
    /// deltas) so callers can dispatch handler events that reflect the
    /// buffered updates.
    ///
    /// The current REST `order_book` response no longer carries a book `id`
    /// (measured 2026-08-06), so the snapshot sequence is anchored through
    /// the buffered deltas: the snapshot must be fresh enough to cover the
    /// state before the first buffered delta (`update_ts >= first.ts`), and
    /// the first delta is anchored at `first_update_id - 1`. The legacy
    /// `id`-carrying response keeps the stricter id bridge check.
    pub fn bootstrap(
        &mut self,
        snapshot: GateioBookSnapshot,
        buffered: Vec<GateioDepthDelta>,
    ) -> Result<Vec<L2Book>> {
        let mut events = Vec::with_capacity(buffered.len() + 1);
        let mut buffered = buffered.into_iter();
        let first = buffered.next();

        match (snapshot.last_update_id, first) {
            (Some(snapshot_id), Some(first)) => {
                self.state.apply(L2Book::Snapshot(snapshot.book.clone()));
                self.last_update_id = Some(snapshot_id);
                events.push(L2Book::Snapshot(snapshot.book.clone()));
                let book = self.apply_delta(first)?;
                if let Some(book) = book {
                    events.push(book);
                }
                // Buffered replay tolerates deltas whose range straddles the
                // current anchor (a later fetch may already have covered the
                // earliest buffered range), so `apply_delta` (range-inclusive
                // bridge) applies here; the strict `apply_next_delta` stays
                // for the live stream only.
                for delta in buffered {
                    if let Some(book) = self.apply_delta(delta)? {
                        events.push(book);
                    }
                }
            }
            (None, Some(first)) => {
                if snapshot.update_ts < first.ts {
                    return Err(Error::Parse(
                        "gateio snapshot predates the buffered deltas".to_owned(),
                    ));
                }
                self.state.apply(L2Book::Snapshot(snapshot.book.clone()));
                self.last_update_id = Some(first.first_update_id - 1);
                events.push(L2Book::Snapshot(snapshot.book.clone()));
                let book = self.apply_delta(first)?;
                if let Some(book) = book {
                    events.push(book);
                }
                for delta in buffered {
                    if let Some(book) = self.apply_delta(delta)? {
                        events.push(book);
                    }
                }
            }
            (Some(snapshot_id), None) => {
                self.state.apply(L2Book::Snapshot(snapshot.book.clone()));
                self.last_update_id = Some(snapshot_id);
                events.push(L2Book::Snapshot(snapshot.book));
            }
            (None, None) => {
                self.state.apply(L2Book::Snapshot(snapshot.book.clone()));
                self.last_update_id = None;
                events.push(L2Book::Snapshot(snapshot.book));
            }
        }

        Ok(events)
    }

    /// Replaces the local book with a `full: true` push and re-anchors the
    /// sequence at the push's `last_update_id`. Deltas buffered or received
    /// before the full push are superseded by it; the next live delta must
    /// bridge `last_update_id + 1` like any other.
    pub fn reset_with_snapshot(
        &mut self,
        snapshot: cryptofeed_orderbook::L2BookSnapshot,
        last_update_id: u64,
    ) {
        self.state = L2BookState::new(snapshot.symbol.clone());
        self.state.apply(L2Book::Snapshot(snapshot));
        self.last_update_id = Some(last_update_id);
    }

    pub fn apply_delta(&mut self, delta: GateioDepthDelta) -> Result<Option<L2Book>> {
        match self.last_update_id {
            None => {
                // No snapshot anchor (no REST id and no buffered deltas when
                // the snapshot arrived): the first delta is accepted and
                // anchors the sequence.
                self.last_update_id = Some(delta.last_update_id);
                let book = L2Book::Delta(delta.book);
                self.state.apply(book.clone());
                Ok(Some(book))
            }
            Some(current) => {
                if delta.last_update_id <= current {
                    return Ok(None);
                }

                let expected_next = current + 1;
                if delta.first_update_id <= expected_next && delta.last_update_id >= expected_next {
                    self.last_update_id = Some(delta.last_update_id);
                    let book = L2Book::Delta(delta.book);
                    self.state.apply(book.clone());
                    return Ok(Some(book));
                }

                Err(Error::Parse(
                    "gateio initial delta does not bridge snapshot".to_owned(),
                ))
            }
        }
    }

    pub fn apply_next_delta(&mut self, delta: GateioDepthDelta) -> Result<Option<L2Book>> {
        let Some(current) = self.last_update_id else {
            self.last_update_id = Some(delta.last_update_id);
            let book = L2Book::Delta(delta.book);
            self.state.apply(book.clone());
            return Ok(Some(book));
        };

        if delta.last_update_id <= current {
            return Ok(None);
        }

        if delta.first_update_id != current + 1 {
            return Err(Error::Parse("gateio book sequence gap detected".to_owned()));
        }

        self.last_update_id = Some(delta.last_update_id);
        let book = L2Book::Delta(delta.book);
        self.state.apply(book.clone());
        Ok(Some(book))
    }

    pub fn state(&self) -> &L2BookState {
        &self.state
    }

    pub fn last_update_id(&self) -> Option<u64> {
        self.last_update_id
    }
}

#[cfg(test)]
mod tests {
    use cryptofeed_core::{error::Error, exchange::ExchangeId, symbol::Symbol};
    use cryptofeed_orderbook::{L2Book, L2BookDelta, L2BookSnapshot, PriceLevel};
    use rust_decimal::Decimal;

    use super::{GateioBookSnapshot, GateioBookSync, GateioDepthDelta};

    fn level(price: &str, amount: &str) -> PriceLevel {
        PriceLevel {
            price: Decimal::from_str_exact(price).unwrap(),
            amount: Decimal::from_str_exact(amount).unwrap(),
        }
    }

    fn snapshot() -> L2BookSnapshot {
        L2BookSnapshot {
            exchange: ExchangeId::Gateio,
            symbol: Symbol::spot("btc", "usdt"),
            bids: vec![level("64999.10", "1.25")],
            asks: vec![level("65000.20", "0.75")],
            exchange_ts: 1.0,
            received_ts: 2.0,
        }
    }

    fn gateio_snapshot(last_update_id: Option<u64>, update_ts: f64) -> GateioBookSnapshot {
        GateioBookSnapshot {
            last_update_id,
            generated_ts: update_ts,
            update_ts,
            book: snapshot(),
        }
    }

    fn delta(
        first: u64,
        last: u64,
        ts: f64,
        bids: Vec<PriceLevel>,
        asks: Vec<PriceLevel>,
    ) -> GateioDepthDelta {
        GateioDepthDelta {
            first_update_id: first,
            last_update_id: last,
            ts,
            book: L2BookDelta {
                exchange: ExchangeId::Gateio,
                symbol: Symbol::spot("btc", "usdt"),
                bids,
                asks,
                exchange_ts: 3.0,
                received_ts: 4.0,
            },
        }
    }

    #[test]
    fn bootstraps_with_delta_that_bridges_snapshot_id() {
        let mut sync = GateioBookSync::new(Symbol::spot("btc", "usdt"));
        let events = sync
            .bootstrap(
                gateio_snapshot(Some(100), 1.0),
                vec![delta(
                    100,
                    101,
                    1.0,
                    vec![level("64999.10", "0")],
                    vec![level("65000.20", "1.00")],
                )],
            )
            .expect("bootstrap");

        // The returned sequence contains the snapshot and the applied delta,
        // so dispatch reflects the buffered updates.
        assert_eq!(events.len(), 2);
        assert!(matches!(events[0], L2Book::Snapshot(_)));
        assert!(matches!(events[1], L2Book::Delta(_)));
        assert_eq!(sync.last_update_id(), Some(101));
        assert_eq!(sync.state().bids().len(), 0);
        assert_eq!(
            sync.state().asks()[0].amount,
            Decimal::from_str_exact("1.00").unwrap()
        );
    }

    #[test]
    fn bootstraps_without_rest_id_via_timestamp_anchor() {
        // Current REST `order_book` responses carry no `id` (measured
        // 2026-08-06): the snapshot is anchored at the first buffered
        // delta's `first_update_id - 1` when the snapshot is fresh enough.
        let mut sync = GateioBookSync::new(Symbol::spot("btc", "usdt"));
        let events = sync
            .bootstrap(
                gateio_snapshot(None, 1.0),
                vec![delta(102, 103, 1.0, vec![level("64999.10", "0")], vec![])],
            )
            .expect("timestamp-anchored bootstrap");

        assert_eq!(events.len(), 2);
        assert_eq!(sync.last_update_id(), Some(103));
        assert_eq!(sync.state().bids().len(), 0);
    }

    #[test]
    fn timestamp_anchor_rejects_stale_snapshot() {
        let mut sync = GateioBookSync::new(Symbol::spot("btc", "usdt"));
        // The snapshot (update_ts 1.0) predates the buffered delta (ts 2.0):
        // updates between them would be lost, so the bootstrap must fail and
        // trigger a resnapshot.
        let err = sync
            .bootstrap(
                gateio_snapshot(None, 1.0),
                vec![delta(102, 103, 2.0, vec![level("64999.10", "0")], vec![])],
            )
            .expect_err("stale snapshot should fail");

        match err {
            Error::Parse(message) => assert!(message.contains("predates")),
            _ => panic!("unexpected error variant"),
        }
    }

    #[test]
    fn ignores_delta_older_than_snapshot_id() {
        let mut sync = GateioBookSync::new(Symbol::spot("btc", "usdt"));
        sync.bootstrap(gateio_snapshot(Some(100), 1.0), vec![])
            .expect("bootstrap");

        let applied = sync
            .apply_delta(delta(90, 100, 1.0, vec![level("64998.50", "2.00")], vec![]))
            .expect("stale delta");

        assert!(applied.is_none());
        assert_eq!(sync.state().bids().len(), 1);
    }

    #[test]
    fn detects_gap_after_bootstrap() {
        let mut sync = GateioBookSync::new(Symbol::spot("btc", "usdt"));
        sync.bootstrap(
            gateio_snapshot(Some(100), 1.0),
            vec![delta(100, 101, 1.0, vec![level("64999.10", "0")], vec![])],
        )
        .expect("bootstrap");

        let err = sync
            .apply_next_delta(delta(
                103,
                104,
                1.0,
                vec![level("64998.50", "2.00")],
                vec![],
            ))
            .expect_err("gap should fail");

        match err {
            Error::Parse(message) => assert!(message.contains("sequence gap")),
            _ => panic!("unexpected error variant"),
        }
    }

    #[test]
    fn bootstrap_rejects_buffered_delta_that_does_not_bridge_snapshot() {
        let mut sync = GateioBookSync::new(Symbol::spot("btc", "usdt"));
        let err = sync
            .bootstrap(
                gateio_snapshot(Some(100), 1.0),
                vec![delta(
                    102,
                    103,
                    1.0,
                    vec![level("64998.50", "2.00")],
                    vec![],
                )],
            )
            .expect_err("non-bridging delta should fail");

        match err {
            Error::Parse(message) => assert!(message.contains("does not bridge")),
            _ => panic!("unexpected error variant"),
        }
    }

    #[test]
    fn full_push_replaces_book_and_reanchors_sequence() {
        let mut sync = GateioBookSync::new(Symbol::spot("btc", "usdt"));
        sync.bootstrap(gateio_snapshot(Some(100), 1.0), vec![])
            .expect("bootstrap");

        // A `full: true` push overwrites the book and re-anchors at its `u`.
        sync.reset_with_snapshot(
            cryptofeed_orderbook::L2BookSnapshot {
                bids: vec![level("64998.00", "3.00")],
                asks: vec![level("65001.00", "2.00")],
                ..snapshot()
            },
            200,
        );
        assert_eq!(sync.last_update_id(), Some(200));
        assert_eq!(sync.state().bids().len(), 1);
        assert_eq!(
            sync.state().bids()[0].price,
            Decimal::from_str_exact("64998.00").unwrap()
        );

        // The delta right after the full push bridges from the new anchor.
        let applied = sync
            .apply_next_delta(delta(
                201,
                205,
                2.0,
                vec![level("64997.00", "4.00")],
                vec![],
            ))
            .expect("delta after full push");
        assert!(applied.is_some());
        assert_eq!(sync.last_update_id(), Some(205));

        // A gap after the full push is still a hard error.
        let err = sync
            .apply_next_delta(delta(210, 212, 2.0, vec![], vec![]))
            .expect_err("gap after full push");
        match err {
            Error::Parse(message) => assert!(message.contains("sequence gap")),
            _ => panic!("unexpected error variant"),
        }
    }

    #[test]
    fn failed_bootstrap_leaves_sync_recoverable_by_fresh_snapshot() {
        let mut sync = GateioBookSync::new(Symbol::spot("btc", "usdt"));
        let err = sync.bootstrap(
            gateio_snapshot(Some(100), 1.0),
            vec![delta(
                102,
                103,
                1.0,
                vec![level("64998.50", "2.00")],
                vec![],
            )],
        );
        assert!(matches!(err, Err(Error::Parse(_))));

        // The exchange re-snapshots at a higher sequence; the previously
        // buffered delta now bridges and the book recovers without weakening
        // the bridge rule.
        sync.bootstrap(
            gateio_snapshot(Some(103), 1.0),
            vec![delta(
                103,
                104,
                1.0,
                vec![level("64999.10", "0"), level("64998.50", "2.00")],
                vec![],
            )],
        )
        .expect("fresh snapshot bridges");

        assert_eq!(sync.last_update_id(), Some(104));
        assert_eq!(sync.state().bids().len(), 1);
        assert_eq!(sync.state().bids()[0].price.to_string(), "64998.50");
    }
}
