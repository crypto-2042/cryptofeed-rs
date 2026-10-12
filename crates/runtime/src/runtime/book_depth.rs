//! Project complete synchronized state into consumer-visible top-N changes.
use cryptofeed_core::error::{Error, Result};
use cryptofeed_orderbook::{L2Book, L2BookDelta, L2BookSnapshot, PriceLevel};
use rust_decimal::Decimal;
use std::collections::BTreeMap;

pub(super) fn project(
    input: &L2Book,
    current: &L2BookSnapshot,
    previous: Option<&L2BookSnapshot>,
) -> Result<L2Book> {
    if matches!(input, L2Book::Snapshot(_)) {
        return Ok(L2Book::Snapshot(current.clone()));
    }
    let previous =
        previous.ok_or_else(|| Error::MalformedData("L2 delivery delta before snapshot".into()))?;
    Ok(L2Book::Delta(L2BookDelta {
        exchange: current.exchange,
        symbol: current.symbol.clone(),
        bids: changes(&previous.bids, &current.bids, true),
        asks: changes(&previous.asks, &current.asks, false),
        exchange_ts: current.exchange_ts,
        received_ts: current.received_ts,
    }))
}

fn changes(before: &[PriceLevel], after: &[PriceLevel], descending: bool) -> Vec<PriceLevel> {
    let old: BTreeMap<_, _> = before.iter().map(|level| (level.price, level)).collect();
    let new: BTreeMap<_, _> = after.iter().map(|level| (level.price, level)).collect();
    let mut changed = BTreeMap::new();
    for level in before {
        if !new.contains_key(&level.price) {
            changed.insert(
                level.price,
                PriceLevel {
                    price: level.price,
                    amount: Decimal::ZERO,
                },
            );
        }
    }
    for level in after {
        if old.get(&level.price).is_none_or(|previous| {
            previous.amount != level.amount || previous.amount.scale() != level.amount.scale()
        }) {
            changed.insert(level.price, level.clone());
        }
    }
    if descending {
        changed.into_values().rev().collect()
    } else {
        changed.into_values().collect()
    }
}
