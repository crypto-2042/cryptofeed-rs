//! Kraken Spot WebSocket v2 order-book checksum and snapshot/update handling.
//!
//! Every `book` push carries a CRC32 checksum over the top ten levels of the
//! resulting book. The documented algorithm (verified against the official
//! example and a live 2026-11-03 probe) is:
//!
//! 1. asks, price low to high: remove `.` from price and qty, strip leading
//!    zeros, append `price + qty`;
//! 2. bids, price high to low: same formatting;
//! 3. CRC32 over `asks_string + bids_string` (IEEE, zlib-compatible).
//!
//! The local book must be truncated to the subscribed depth after every push:
//! levels that fall out of scope never receive a `qty: 0` removal, so keeping
//! them changes which levels are the top ten and the checksum stops matching
//! (reproduced live on 2026-11-03 with `depth=10`). A checksum mismatch clears
//! the local book and enters the bounded in-session resnapshot path; a corrupt
//! book is never published.

use cryptofeed_core::error::{Error, Result};
use cryptofeed_core::exchange::ExchangeId;
use cryptofeed_core::symbol::Symbol;
use cryptofeed_orderbook::{L2Book, L2BookDelta, L2BookSnapshot, L2BookState};

use super::parser::BookUpdate;

/// Cumulative checksum-recovery requests per connection before the existing
/// reconnect/backoff supervisor takes over. Success does not refund the
/// budget, so a snapshot followed by an immediate mismatch cannot loop.
pub const MAX_KRAKEN_RESNAPSHOTS: u32 = 3;

#[derive(Clone, Debug, PartialEq)]
pub enum KrakenBookOutcome {
    Publish(L2Book),
    /// Dropped update received before the first (or a replacement) snapshot.
    Ignored,
    /// The caller must send unsubscribe/subscribe so the next push is a
    /// replacement snapshot.
    Resnapshot,
}

pub struct KrakenBookSync {
    symbol: Symbol,
    depth: usize,
    state: L2BookState,
    awaiting_snapshot: bool,
    resnapshot_requests: u32,
}

impl KrakenBookSync {
    pub fn new(symbol: Symbol, depth: u16) -> Self {
        Self {
            state: L2BookState::new(symbol.clone()),
            symbol,
            depth: usize::from(depth),
            // The subscription requests `snapshot: true`; updates received
            // before that snapshot belong to no local book.
            awaiting_snapshot: true,
            resnapshot_requests: 0,
        }
    }

    pub fn state(&self) -> &L2BookState {
        &self.state
    }

    pub fn awaiting_snapshot(&self) -> bool {
        self.awaiting_snapshot
    }

    pub fn resnapshot_requests(&self) -> u32 {
        self.resnapshot_requests
    }

    pub fn apply(&mut self, update: BookUpdate) -> Result<KrakenBookOutcome> {
        let (exchange_ts, received_ts) = (update.exchange_ts, update.received_ts);
        if update.snapshot {
            let snapshot = L2BookSnapshot {
                exchange: ExchangeId::Kraken,
                symbol: update.symbol.clone(),
                bids: update.bids,
                asks: update.asks,
                exchange_ts: update.exchange_ts,
                received_ts: update.received_ts,
            };
            self.state.apply(L2Book::Snapshot(snapshot));
            self.awaiting_snapshot = false;
            self.trim(exchange_ts, received_ts);
            if checksum_of_state(&self.state) != update.checksum {
                return self.reset_for_resnapshot();
            }
            return Ok(KrakenBookOutcome::Publish(L2Book::Snapshot(
                self.state
                    .to_snapshot(ExchangeId::Kraken, exchange_ts, received_ts),
            )));
        }

        if self.awaiting_snapshot {
            return Ok(KrakenBookOutcome::Ignored);
        }
        let delta = L2BookDelta {
            exchange: ExchangeId::Kraken,
            symbol: update.symbol.clone(),
            bids: update.bids,
            asks: update.asks,
            exchange_ts: update.exchange_ts,
            received_ts: update.received_ts,
        };
        self.state.apply(L2Book::Delta(delta.clone()));
        self.trim(exchange_ts, received_ts);
        if checksum_of_state(&self.state) != update.checksum {
            return self.reset_for_resnapshot();
        }
        Ok(KrakenBookOutcome::Publish(L2Book::Delta(delta)))
    }

    /// Truncate to the subscribed depth. Out-of-scope levels are never removed
    /// by the exchange, so they must not participate in the local book.
    fn trim(&mut self, exchange_ts: f64, received_ts: f64) {
        let snapshot = self.state.to_snapshot_with_depth(
            ExchangeId::Kraken,
            exchange_ts,
            received_ts,
            self.depth,
        );
        self.state.apply(L2Book::Snapshot(snapshot));
    }

    fn reset_for_resnapshot(&mut self) -> Result<KrakenBookOutcome> {
        self.resnapshot_requests += 1;
        if self.resnapshot_requests > MAX_KRAKEN_RESNAPSHOTS {
            return Err(Error::Parse(
                "kraken book checksum recovery request limit exceeded".to_owned(),
            ));
        }
        self.state = L2BookState::new(self.symbol.clone());
        self.awaiting_snapshot = true;
        Ok(KrakenBookOutcome::Resnapshot)
    }
}

/// Documented checksum input: top ten asks (low to high) followed by the top
/// ten bids (high to low), each level formatted as `price + qty` with the
/// decimal point removed and leading zeros stripped.
pub fn checksum_string(state: &L2BookState) -> String {
    let bids = state.bids();
    let asks = state.asks();
    let mut parts = String::new();
    for level in asks.iter().take(10) {
        parts.push_str(&format_level(
            &level.price.to_string(),
            &level.amount.to_string(),
        ));
    }
    for level in bids.iter().take(10) {
        parts.push_str(&format_level(
            &level.price.to_string(),
            &level.amount.to_string(),
        ));
    }
    parts
}

fn format_level(price: &str, qty: &str) -> String {
    let mut formatted = String::with_capacity(price.len() + qty.len());
    for value in [price, qty] {
        let digits: String = value.chars().filter(|ch| *ch != '.').collect();
        formatted.push_str(digits.trim_start_matches('0'));
    }
    formatted
}

pub fn checksum_of_state(state: &L2BookState) -> u32 {
    crc32(checksum_string(state).as_bytes())
}

/// IEEE CRC-32 (same polynomial and init as zlib): table-driven, deterministic.
fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &byte in data {
        let index = ((crc ^ u32::from(byte)) & 0xFF) as usize;
        crc = CRC32_TABLE[index] ^ (crc >> 8);
    }
    !crc
}

const fn build_crc32_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut value = i as u32;
        let mut bit = 0;
        while bit < 8 {
            value = if value & 1 != 0 {
                0xEDB8_8320 ^ (value >> 1)
            } else {
                value >> 1
            };
            bit += 1;
        }
        table[i] = value;
        i += 1;
    }
    table
}

static CRC32_TABLE: [u32; 256] = build_crc32_table();

#[cfg(test)]
mod tests {
    use super::{
        KrakenBookOutcome, KrakenBookSync, MAX_KRAKEN_RESNAPSHOTS, checksum_of_state,
        checksum_string, crc32, format_level,
    };
    use crate::exchange::kraken::parser::BookUpdate;
    use cryptofeed_core::{error::Error, exchange::ExchangeId, symbol::Symbol};
    use cryptofeed_orderbook::{L2Book, L2BookSnapshot, PriceLevel};
    use rust_decimal::Decimal;

    fn level(price: &str, amount: &str) -> PriceLevel {
        PriceLevel {
            price: Decimal::from_str_exact(price).unwrap(),
            amount: Decimal::from_str_exact(amount).unwrap(),
        }
    }

    fn symbol() -> Symbol {
        Symbol::spot("BTC", "USD")
    }

    fn update(
        snapshot: bool,
        checksum: u32,
        bids: Vec<PriceLevel>,
        asks: Vec<PriceLevel>,
    ) -> BookUpdate {
        BookUpdate {
            symbol: symbol(),
            snapshot,
            checksum,
            exchange_ts: 1.0,
            received_ts: 2.0,
            bids,
            asks,
        }
    }

    /// The official book-checksum guide's snapshot example.
    fn documented_snapshot() -> BookUpdate {
        let bids = [
            ("45283.5", "0.10000000"),
            ("45283.4", "1.54582015"),
            ("45282.1", "0.10000000"),
            ("45281.0", "0.10000000"),
            ("45280.3", "1.54592586"),
            ("45279.0", "0.07990000"),
            ("45277.6", "0.03310103"),
            ("45277.5", "0.30000000"),
            ("45277.3", "1.54602737"),
            ("45276.6", "0.15445238"),
        ]
        .into_iter()
        .map(|(price, qty)| level(price, qty))
        .collect();
        let asks = [
            ("45285.2", "0.00100000"),
            ("45286.4", "1.54571953"),
            ("45286.6", "1.54571109"),
            ("45289.6", "1.54560911"),
            ("45290.2", "0.15890660"),
            ("45291.8", "1.54553491"),
            ("45294.7", "0.04454749"),
            ("45296.1", "0.35380000"),
            ("45297.5", "0.09945542"),
            ("45299.5", "0.18772827"),
        ]
        .into_iter()
        .map(|(price, qty)| level(price, qty))
        .collect();
        update(true, 3_310_070_434, bids, asks)
    }

    #[test]
    fn crc32_matches_known_vector() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn formats_levels_per_the_documented_algorithm() {
        assert_eq!(format_level("45285.2", "0.00100000"), "452852100000");
        assert_eq!(format_level("45283.5", "0.10000000"), "45283510000000");
        assert_eq!(
            format_level("0.0736283", "12773.80559711"),
            "7362831277380559711"
        );
    }

    #[test]
    fn official_checksum_example_matches() {
        let mut sync = KrakenBookSync::new(symbol(), 10);
        let outcome = sync.apply(documented_snapshot()).expect("snapshot");
        assert!(matches!(
            outcome,
            KrakenBookOutcome::Publish(L2Book::Snapshot(_))
        ));
        assert_eq!(checksum_of_state(sync.state()), 3_310_070_434);
        // The guide publishes the exact concatenated input; compare it so a
        // formatting regression is visible directly.
        assert_eq!(
            checksum_string(sync.state()),
            "45285210000045286415457195345286615457110945289615456091145290215890660452918154553491452947445474945296135380000452975994554245299518772827452835100000004528341545820154528211000000045281010000000452803154592586452790799000045277633101034527753000000045277315460273745276615445238"
        );
    }

    #[test]
    fn update_is_published_only_when_the_checksum_matches() {
        let mut sync = KrakenBookSync::new(symbol(), 10);
        sync.apply(documented_snapshot()).expect("snapshot");

        let delta = update(false, 0, vec![level("45283.5", "0.50000000")], vec![]);
        let mut post = sync.state().clone();
        post.apply(L2Book::Delta(cryptofeed_orderbook::L2BookDelta {
            exchange: ExchangeId::Kraken,
            symbol: symbol(),
            bids: delta.bids.clone(),
            asks: vec![],
            exchange_ts: 1.0,
            received_ts: 2.0,
        }));
        let valid = checksum_of_state(&post);
        assert!(matches!(
            sync.apply(BookUpdate {
                checksum: valid,
                ..delta
            })
            .expect("update"),
            KrakenBookOutcome::Publish(L2Book::Delta(_))
        ));
        assert_eq!(sync.state().bids()[0].amount.to_string(), "0.50000000");

        let mut corrupt = update(false, valid ^ 0xFF, vec![level("45283.5", "0.25")], vec![]);
        corrupt.snapshot = false;
        assert_eq!(
            sync.apply(corrupt).expect("mismatch"),
            KrakenBookOutcome::Resnapshot
        );
    }

    #[test]
    fn checksum_mismatch_clears_the_book_and_requests_a_resnapshot() {
        let mut sync = KrakenBookSync::new(symbol(), 10);
        sync.apply(documented_snapshot()).expect("snapshot");
        assert_eq!(
            sync.apply(update(
                false,
                0xDEAD_BEEF,
                vec![level("45283.5", "0.5")],
                vec![]
            ))
            .expect("mismatch"),
            KrakenBookOutcome::Resnapshot
        );
        assert!(sync.awaiting_snapshot());
        assert!(sync.state().bids().is_empty());
        assert!(sync.state().asks().is_empty());
        assert_eq!(sync.resnapshot_requests(), 1);
    }

    #[test]
    fn recovery_requests_are_bounded_per_connection() {
        let mut sync = KrakenBookSync::new(symbol(), 10);
        for _ in 0..MAX_KRAKEN_RESNAPSHOTS {
            sync.apply(documented_snapshot()).expect("snapshot");
            assert_eq!(
                sync.apply(update(false, 1, vec![level("45283.5", "0.5")], vec![]))
                    .expect("mismatch"),
                KrakenBookOutcome::Resnapshot
            );
        }
        sync.apply(documented_snapshot()).expect("snapshot");
        assert!(matches!(
            sync.apply(update(false, 1, vec![level("45283.5", "0.5")], vec![])),
            Err(Error::Parse(message)) if message.contains("checksum recovery request limit")
        ));
    }

    #[test]
    fn truncates_to_the_subscribed_depth_and_keeps_the_top_ten_checksum() {
        let mut sync = KrakenBookSync::new(symbol(), 10);
        sync.apply(documented_snapshot()).expect("snapshot");
        // A better bid arrives; the 10th bid falls out of scope and would
        // never be removed by the exchange.
        let new_bid = level("45283.6", "0.25");
        let mut post = sync.state().clone();
        post.apply(L2Book::Delta(cryptofeed_orderbook::L2BookDelta {
            exchange: ExchangeId::Kraken,
            symbol: symbol(),
            bids: vec![new_bid.clone()],
            asks: vec![],
            exchange_ts: 1.0,
            received_ts: 2.0,
        }));
        let trimmed = post.to_snapshot_with_depth(ExchangeId::Kraken, 1.0, 2.0, 10);
        let expected = {
            let mut state = cryptofeed_orderbook::L2BookState::new(symbol());
            state.apply(L2Book::Snapshot(trimmed.clone()));
            checksum_of_state(&state)
        };
        let outcome = sync
            .apply(update(false, expected, vec![new_bid], vec![]))
            .expect("update");
        assert!(matches!(
            outcome,
            KrakenBookOutcome::Publish(L2Book::Delta(_))
        ));
        assert_eq!(sync.state().bids().len(), 10);
        assert_eq!(sync.state().asks().len(), 10);
        // The best bid is the newly added level.
        assert_eq!(sync.state().bids()[0].price.to_string(), "45283.6");
        assert_eq!(checksum_of_state(sync.state()), expected);
    }

    #[test]
    fn updates_before_the_first_snapshot_are_dropped() {
        let mut sync = KrakenBookSync::new(symbol(), 10);
        assert_eq!(
            sync.apply(update(false, 1, vec![level("45283.5", "1")], vec![]))
                .expect("ignored"),
            KrakenBookOutcome::Ignored
        );
        assert!(sync.awaiting_snapshot());
    }

    #[test]
    fn replacement_snapshot_resets_state_and_clears_awaiting() {
        let mut sync = KrakenBookSync::new(symbol(), 10);
        sync.apply(documented_snapshot()).expect("first");
        let replacement = BookUpdate {
            bids: vec![level("100.0", "1")],
            asks: vec![level("101.0", "2")],
            ..documented_snapshot()
        };
        let mut state = cryptofeed_orderbook::L2BookState::new(symbol());
        state.apply(L2Book::Snapshot(L2BookSnapshot {
            exchange: ExchangeId::Kraken,
            symbol: symbol(),
            bids: replacement.bids.clone(),
            asks: replacement.asks.clone(),
            exchange_ts: 1.0,
            received_ts: 2.0,
        }));
        let checksum = checksum_of_state(&state);
        let outcome = sync
            .apply(BookUpdate {
                checksum,
                ..replacement
            })
            .expect("replacement");
        assert!(matches!(
            outcome,
            KrakenBookOutcome::Publish(L2Book::Snapshot(_))
        ));
        assert!(!sync.awaiting_snapshot());
        assert_eq!(sync.state().bids().len(), 1);
        assert_eq!(sync.state().bids()[0].price.to_string(), "100.0");
    }
}
