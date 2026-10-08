use cryptofeed_core::error::{Error, Result};
use cryptofeed_core::symbol::Symbol;
use cryptofeed_orderbook::{L2Book, L2BookSnapshot, L2BookState};

#[derive(Clone, Debug)]
pub enum OkxBookAction {
    Snapshot,
    Update,
}

#[derive(Clone, Debug)]
pub struct OkxDepthUpdate {
    pub action: OkxBookAction,
    pub seq_id: i64,
    pub prev_seq_id: i64,
    /// Full-book checksum sent by OKX on the `books` channel. Validation is
    /// skipped when `None` (e.g. `books5`/`bbo-tbt`, whose checksum only
    /// covers the transmitted top levels and cannot be compared to the local
    /// full book).
    pub checksum: Option<u32>,
    pub book: L2Book,
}

#[derive(Clone, Debug)]
pub struct OkxBookSync {
    state: L2BookState,
    seq_id: Option<i64>,
}

impl OkxBookSync {
    pub fn new(symbol: Symbol) -> Self {
        Self {
            state: L2BookState::new(symbol),
            seq_id: None,
        }
    }

    pub fn apply(&mut self, update: OkxDepthUpdate) -> Result<Option<L2Book>> {
        match update.action {
            OkxBookAction::Snapshot => {
                let book = as_snapshot(update.book);
                self.seq_id = Some(update.seq_id);
                self.state.apply(book.clone());
                validate_checksum(&self.state, update.checksum)?;
                Ok(Some(book))
            }
            OkxBookAction::Update => {
                let current = self
                    .seq_id
                    .ok_or_else(|| Error::Parse("okx book sync not initialized".to_owned()))?;

                if update.seq_id == current && update.prev_seq_id == current {
                    return Ok(None);
                }

                if update.prev_seq_id != current {
                    return Err(Error::Parse("okx book sequence gap detected".to_owned()));
                }

                self.seq_id = Some(update.seq_id);
                self.state.apply(update.book.clone());
                validate_checksum(&self.state, update.checksum)?;
                Ok(Some(update.book))
            }
        }
    }

    pub fn state(&self) -> &L2BookState {
        &self.state
    }

    pub fn seq_id(&self) -> Option<i64> {
        self.seq_id
    }
}

fn validate_checksum(state: &L2BookState, expected: Option<u32>) -> Result<()> {
    let Some(expected) = expected else {
        return Ok(());
    };
    // The current OKX `books` channel transmits `checksum: 0` on every
    // snapshot and update (measured live on 2026-08-06), which means "no
    // checksum provided" rather than a genuine zero CRC. Skipping `0` keeps
    // the strict comparison active for any nonzero checksum the exchange
    // sends again.
    if expected == 0 {
        return Ok(());
    }
    let actual = checksum_of_state(state);
    if actual != expected {
        return Err(Error::Parse("okx book checksum mismatch".to_owned()));
    }
    Ok(())
}

/// Reconstruct the OKX checksum string from the local book: the first 25
/// levels interleaved per index — `bid1:bidSize1:ask1:askSize1:bid2:...` —
/// truncating whichever side runs short. The exchange hashes the transmitted
/// string, and `Decimal` parsing preserves the transmitted scale (trailing
/// zeros), so the round-trip is faithful as long as a level's latest
/// representation came from an OKX `books` payload.
fn checksum_string(state: &L2BookState) -> String {
    let bids = state.bids();
    let asks = state.asks();
    let mut parts = Vec::with_capacity(100);
    for index in 0..25 {
        if index >= bids.len() && index >= asks.len() {
            break;
        }
        if let Some(level) = bids.get(index) {
            parts.push(level.price.to_string());
            parts.push(level.amount.to_string());
        }
        if let Some(level) = asks.get(index) {
            parts.push(level.price.to_string());
            parts.push(level.amount.to_string());
        }
    }
    parts.join(":")
}

fn checksum_of_state(state: &L2BookState) -> u32 {
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

fn as_snapshot(book: L2Book) -> L2Book {
    match book {
        snapshot @ L2Book::Snapshot(_) => snapshot,
        L2Book::Delta(delta) => L2Book::Snapshot(L2BookSnapshot {
            exchange: delta.exchange,
            symbol: delta.symbol,
            bids: delta.bids,
            asks: delta.asks,
            exchange_ts: delta.exchange_ts,
            received_ts: delta.received_ts,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        OkxBookAction, OkxBookSync, OkxDepthUpdate, checksum_of_state, checksum_string, crc32,
    };
    use cryptofeed_core::{error::Error, exchange::ExchangeId, symbol::Symbol};
    use cryptofeed_orderbook::{L2Book, L2BookDelta, L2BookSnapshot, PriceLevel};
    use rust_decimal::Decimal;

    fn level(price: &str, amount: &str) -> PriceLevel {
        PriceLevel {
            price: Decimal::from_str_exact(price).unwrap(),
            amount: Decimal::from_str_exact(amount).unwrap(),
        }
    }

    fn update(
        action: OkxBookAction,
        seq_id: i64,
        prev_seq_id: i64,
        amount: &str,
    ) -> OkxDepthUpdate {
        OkxDepthUpdate {
            action,
            seq_id,
            prev_seq_id,
            checksum: None,
            book: L2Book::Delta(L2BookDelta {
                exchange: ExchangeId::Okx,
                symbol: Symbol::spot("btc", "usdt"),
                bids: vec![level("64999.10", amount)],
                asks: vec![level("65000.20", "0.75")],
                exchange_ts: 1.0,
                received_ts: 2.0,
            }),
        }
    }

    #[test]
    fn snapshot_initializes_sync() {
        let mut sync = OkxBookSync::new(Symbol::spot("btc", "usdt"));
        sync.apply(update(OkxBookAction::Snapshot, 100, -1, "1.25"))
            .expect("snapshot");

        assert_eq!(sync.seq_id(), Some(100));
        assert_eq!(sync.state().bids().len(), 1);
    }

    #[test]
    fn update_applies_when_prev_seq_matches() {
        let mut sync = OkxBookSync::new(Symbol::spot("btc", "usdt"));
        sync.apply(update(OkxBookAction::Snapshot, 100, -1, "1.25"))
            .expect("snapshot");
        sync.apply(update(OkxBookAction::Update, 101, 100, "0"))
            .expect("update");

        assert_eq!(sync.seq_id(), Some(101));
        assert_eq!(sync.state().bids().len(), 0);
    }

    #[test]
    fn update_detects_gap() {
        let mut sync = OkxBookSync::new(Symbol::spot("btc", "usdt"));
        sync.apply(update(OkxBookAction::Snapshot, 100, -1, "1.25"))
            .expect("snapshot");
        let err = sync
            .apply(update(OkxBookAction::Update, 102, 999, "1.25"))
            .expect_err("gap");

        match err {
            Error::Parse(message) => assert!(message.contains("sequence gap")),
            _ => panic!("unexpected error variant"),
        }
    }

    #[test]
    fn accepts_official_sequence_reset_and_idle_update() {
        let mut sync = OkxBookSync::new(Symbol::spot("btc", "usdt"));
        sync.apply(update(OkxBookAction::Snapshot, 10, -1, "1.25"))
            .expect("snapshot");

        assert!(
            sync.apply(update(OkxBookAction::Update, 10, 10, "1.25"))
                .expect("idle update")
                .is_none()
        );
        sync.apply(update(OkxBookAction::Update, 3, 10, "2.0"))
            .expect("maintenance sequence reset");
        assert_eq!(sync.seq_id(), Some(3));
    }

    #[test]
    fn replacement_snapshot_clears_stale_levels() {
        let mut sync = OkxBookSync::new(Symbol::spot("btc", "usdt"));
        sync.apply(update(OkxBookAction::Snapshot, 100, -1, "1.25"))
            .expect("first snapshot");
        let mut replacement = update(OkxBookAction::Snapshot, 200, -1, "2.0");
        if let L2Book::Delta(delta) = &mut replacement.book {
            delta.bids[0].price = Decimal::from_str_exact("64998.00").unwrap();
        }
        sync.apply(replacement).expect("replacement snapshot");

        assert_eq!(sync.state().bids().len(), 1);
        assert_eq!(sync.state().bids()[0].price.to_string(), "64998.00");
    }

    #[test]
    fn crc32_matches_known_vector() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn checksum_string_uses_transmitted_scale_bid_then_ask() {
        let mut sync = OkxBookSync::new(Symbol::spot("btc", "usdt"));
        sync.apply(update(OkxBookAction::Snapshot, 100, -1, "1.007"))
            .expect("snapshot");

        assert_eq!(
            checksum_string(sync.state()),
            "64999.10:1.007:65000.20:0.75"
        );
    }

    #[test]
    fn checksum_string_interleaves_bids_and_asks_per_index() {
        // The official OKX format alternates per index
        // (`bid1:size1:ask1:size1:bid2:size2:ask2:size2:...`), not all bids
        // followed by all asks. A book with two levels per side must produce
        // the interleaved layout.
        let mut state = cryptofeed_orderbook::L2BookState::new(Symbol::spot("btc", "usdt"));
        state.apply(L2Book::Snapshot(L2BookSnapshot {
            exchange: ExchangeId::Okx,
            symbol: Symbol::spot("btc", "usdt"),
            bids: vec![level("64999.10", "1.25"), level("64998.00", "2.50")],
            asks: vec![level("65000.20", "0.75"), level("65001.00", "1.50")],
            exchange_ts: 1.0,
            received_ts: 2.0,
        }));

        assert_eq!(
            checksum_string(&state),
            "64999.10:1.25:65000.20:0.75:64998.00:2.50:65001.00:1.50"
        );

        // A short ask side truncates at the last level present on either
        // side.
        state.apply(L2Book::Snapshot(L2BookSnapshot {
            exchange: ExchangeId::Okx,
            symbol: Symbol::spot("btc", "usdt"),
            bids: vec![level("64999.10", "1.25"), level("64998.00", "2.50")],
            asks: vec![level("65000.20", "0.75")],
            exchange_ts: 1.0,
            received_ts: 2.0,
        }));
        assert_eq!(
            checksum_string(&state),
            "64999.10:1.25:65000.20:0.75:64998.00:2.50"
        );
    }

    #[test]
    fn valid_checksum_is_accepted() {
        let mut sync = OkxBookSync::new(Symbol::spot("btc", "usdt"));
        let snapshot = update(OkxBookAction::Snapshot, 100, -1, "1.25");
        sync.apply(snapshot.clone()).expect("snapshot");
        let checksum = checksum_of_state(sync.state());

        let mut fresh = OkxBookSync::new(Symbol::spot("btc", "usdt"));
        let mut with_checksum = snapshot;
        with_checksum.checksum = Some(checksum);
        let applied = fresh
            .apply(with_checksum)
            .expect("snapshot with valid checksum");

        assert!(applied.is_some());
    }

    #[test]
    fn checksum_mismatch_is_rejected() {
        let mut sync = OkxBookSync::new(Symbol::spot("btc", "usdt"));
        let mut snapshot = update(OkxBookAction::Snapshot, 100, -1, "1.25");
        snapshot.checksum = Some(0xDEAD_BEEF);
        let err = sync
            .apply(snapshot)
            .expect_err("checksum mismatch should fail");

        match err {
            Error::Parse(message) => assert!(message.contains("checksum mismatch")),
            _ => panic!("unexpected error variant"),
        }
    }

    #[test]
    fn zero_checksum_is_treated_as_not_provided() {
        // The live `books` channel transmits `checksum: 0` on every message
        // (measured 2026-08-06); it must be skipped, not compared.
        let mut sync = OkxBookSync::new(Symbol::spot("btc", "usdt"));
        let mut snapshot = update(OkxBookAction::Snapshot, 100, -1, "1.25");
        snapshot.checksum = Some(0);
        sync.apply(snapshot).expect("zero checksum is skipped");

        let mut delta = update(OkxBookAction::Update, 101, 100, "2.00");
        delta.checksum = Some(0);
        sync.apply(delta)
            .expect("zero checksum is skipped on updates");
    }

    #[test]
    fn update_checksum_is_validated_after_apply() {
        let mut sync = OkxBookSync::new(Symbol::spot("btc", "usdt"));
        sync.apply(update(OkxBookAction::Snapshot, 100, -1, "1.25"))
            .expect("snapshot");

        let mut delta = update(OkxBookAction::Update, 101, 100, "2.00");
        let mut expected = sync.state().clone();
        expected.apply(delta.book.clone());
        delta.checksum = Some(crc32(checksum_string(&expected).as_bytes()));

        sync.apply(delta).expect("delta with valid checksum");

        let mut corrupt = update(OkxBookAction::Update, 102, 101, "2.00");
        corrupt.checksum = Some(0xDEAD_BEEF);
        let err = sync
            .apply(corrupt)
            .expect_err("corrupt checksum should fail");
        assert!(matches!(
            err,
            Error::Parse(message) if message.contains("checksum mismatch")
        ));
    }
}
