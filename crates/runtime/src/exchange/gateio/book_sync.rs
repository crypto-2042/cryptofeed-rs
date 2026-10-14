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
    /// A REST snapshot must carry its sequence id (`with_id=true`). Neither
    /// timestamps nor the first buffered range prove the snapshot's anchor.
    /// Failed bootstrap does not publish or partially mutate the current book.
    pub fn bootstrap(
        &mut self,
        snapshot: GateioBookSnapshot,
        buffered: Vec<GateioDepthDelta>,
    ) -> Result<Vec<L2Book>> {
        let snapshot_id = snapshot.last_update_id.ok_or_else(|| {
            Error::Parse("gateio snapshot missing sequence id; cannot prove bootstrap".into())
        })?;
        let mut next = Self::new(snapshot.book.symbol.clone());
        next.reset_with_snapshot(snapshot.book.clone(), snapshot_id);
        let mut events = Vec::with_capacity(buffered.len() + 1);
        events.push(L2Book::Snapshot(snapshot.book));
        for delta in buffered {
            if let Some(book) = next.apply_delta(delta)? {
                events.push(book);
            }
        }
        *self = next;
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
        self.apply_sequence_delta(delta, "gateio initial delta does not bridge snapshot")
    }

    pub fn apply_next_delta(&mut self, delta: GateioDepthDelta) -> Result<Option<L2Book>> {
        self.apply_sequence_delta(delta, "gateio book sequence gap detected")
    }

    fn apply_sequence_delta(
        &mut self,
        delta: GateioDepthDelta,
        gap: &str,
    ) -> Result<Option<L2Book>> {
        if delta.first_update_id > delta.last_update_id {
            return Err(Error::Parse("gateio invalid update sequence range".into()));
        }
        let current = self
            .last_update_id
            .ok_or_else(|| Error::Parse("gateio book has no sequence anchor".into()))?;
        if delta.last_update_id <= current {
            return Ok(None);
        }
        // order_book_update (not obu/V2): overlap is valid when it covers
        // the next id. Absolute quantities replace levels; true gaps fail.
        if delta.first_update_id > current + 1 {
            return Err(Error::Parse(gap.into()));
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
    fn idless_snapshots_never_replay_old_quantities_or_guess_an_anchor() {
        for ts in [0.5, 1.0, 2.0] {
            let mut sync = GateioBookSync::new(Symbol::spot("btc", "usdt"));
            let mut newer = gateio_snapshot(None, ts);
            newer.book.bids[0].amount = Decimal::from(2);
            let result = sync.bootstrap(
                newer,
                vec![delta(100, 101, 1.0, vec![level("64999.10", "1")], vec![])],
            );
            assert!(
                matches!(result,Err(Error::Parse(message)) if message.contains("missing sequence id"))
            );
            assert_eq!(sync.last_update_id(), None);
            assert!(sync.state().bids().is_empty());
        }
    }

    #[test]
    fn unanchored_deltas_and_idless_empty_bootstrap_are_rejected() {
        let mut sync = GateioBookSync::new(Symbol::spot("btc", "usdt"));
        assert!(sync.bootstrap(gateio_snapshot(None, 2.0), vec![]).is_err());
        assert!(
            sync.apply_delta(delta(100, 101, 1.0, vec![], vec![]))
                .is_err()
        );
        assert!(
            sync.apply_next_delta(delta(100, 101, 1.0, vec![], vec![]))
                .is_err()
        );
        assert_eq!(sync.last_update_id(), None);
    }

    #[test]
    fn live_range_overlap_stale_adjacent_and_true_gap() {
        for (first, last, applied) in [
            (100, 102, Some(true)),
            (90, 100, Some(false)),
            (101, 102, Some(true)),
            (102, 103, None),
        ] {
            let mut sync = GateioBookSync::new(Symbol::spot("btc", "usdt"));
            sync.bootstrap(gateio_snapshot(Some(100), 1.0), vec![])
                .unwrap();
            let result = sync.apply_next_delta(delta(
                first,
                last,
                2.0,
                vec![level("64999.10", "2")],
                vec![],
            ));
            match applied {
                Some(expected) => assert_eq!(result.unwrap().is_some(), expected),
                None => assert!(result.is_err()),
            }
            assert_eq!(
                sync.last_update_id(),
                Some(if applied == Some(true) { last } else { 100 })
            );
            assert_eq!(
                sync.state().bids()[0].amount,
                if applied == Some(true) {
                    Decimal::from(2)
                } else {
                    Decimal::from_str_exact("1.25").unwrap()
                }
            );
        }
    }

    #[test]
    fn bootstrap_failure_does_not_commit_a_partial_book() {
        let mut sync = GateioBookSync::new(Symbol::spot("btc", "usdt"));
        sync.bootstrap(gateio_snapshot(Some(100), 1.0), vec![])
            .unwrap();
        assert!(
            sync.bootstrap(
                gateio_snapshot(Some(200), 2.0),
                vec![
                    delta(201, 202, 2.0, vec![level("64999.10", "2")], vec![]),
                    delta(204, 205, 2.0, vec![], vec![])
                ]
            )
            .is_err()
        );
        assert_eq!(sync.last_update_id(), Some(100));
        assert_eq!(
            sync.state().bids()[0].amount,
            Decimal::from_str_exact("1.25").unwrap()
        );
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
