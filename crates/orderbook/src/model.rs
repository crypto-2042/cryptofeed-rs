use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum BookSide {
    Bid,
    Ask,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: Decimal,
    pub amount: Decimal,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct L2BookSnapshot {
    pub exchange: ExchangeId,
    pub symbol: Symbol,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub exchange_ts: f64,
    pub received_ts: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct L2BookDelta {
    pub exchange: ExchangeId,
    pub symbol: Symbol,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub exchange_ts: f64,
    pub received_ts: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum L2Book {
    Snapshot(L2BookSnapshot),
    Delta(L2BookDelta),
}

impl L2Book {
    pub fn symbol(&self) -> &Symbol {
        match self {
            L2Book::Snapshot(snapshot) => &snapshot.symbol,
            L2Book::Delta(delta) => &delta.symbol,
        }
    }

    pub fn bids(&self) -> &[PriceLevel] {
        match self {
            L2Book::Snapshot(snapshot) => &snapshot.bids,
            L2Book::Delta(delta) => &delta.bids,
        }
    }

    pub fn asks(&self) -> &[PriceLevel] {
        match self {
            L2Book::Snapshot(snapshot) => &snapshot.asks,
            L2Book::Delta(delta) => &delta.asks,
        }
    }

    pub fn exchange_ts(&self) -> f64 {
        match self {
            L2Book::Snapshot(snapshot) => snapshot.exchange_ts,
            L2Book::Delta(delta) => delta.exchange_ts,
        }
    }

    pub fn received_ts(&self) -> f64 {
        match self {
            L2Book::Snapshot(snapshot) => snapshot.received_ts,
            L2Book::Delta(delta) => delta.received_ts,
        }
    }
}

/// Top-of-book (L1) snapshot: the best bid and ask including sizes.
/// Distinct from the Ticker model, which carries prices without sizes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct L1Book {
    pub exchange: ExchangeId,
    pub symbol: Symbol,
    pub bid: PriceLevel,
    pub ask: PriceLevel,
    pub exchange_ts: f64,
    pub received_ts: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct L2BookState {
    symbol: Symbol,
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
}

impl L2BookState {
    /// Whether both sides exist and the best bid is at or above the best ask.
    pub fn is_crossed(&self) -> bool {
        match (self.bids.last_key_value(), self.asks.first_key_value()) {
            (Some((bid, _)), Some((ask, _))) => bid >= ask,
            _ => false,
        }
    }

    pub fn new(symbol: Symbol) -> Self {
        Self {
            symbol,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }

    pub fn apply(&mut self, book: L2Book) {
        match book {
            L2Book::Snapshot(snapshot) => {
                self.symbol = snapshot.symbol;
                self.bids.clear();
                self.asks.clear();
                apply_levels(&mut self.bids, snapshot.bids);
                apply_levels(&mut self.asks, snapshot.asks);
            }
            L2Book::Delta(delta) => {
                self.symbol = delta.symbol;
                apply_levels(&mut self.bids, delta.bids);
                apply_levels(&mut self.asks, delta.asks);
            }
        }
    }

    pub fn bids(&self) -> Vec<PriceLevel> {
        self.bids
            .iter()
            .rev()
            .map(|(price, amount)| PriceLevel {
                price: *price,
                amount: *amount,
            })
            .collect()
    }

    pub fn asks(&self) -> Vec<PriceLevel> {
        self.asks
            .iter()
            .map(|(price, amount)| PriceLevel {
                price: *price,
                amount: *amount,
            })
            .collect()
    }

    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    pub fn to_snapshot(
        &self,
        exchange: ExchangeId,
        exchange_ts: f64,
        received_ts: f64,
    ) -> L2BookSnapshot {
        self.to_snapshot_with_depth(exchange, exchange_ts, received_ts, usize::MAX)
    }

    /// Copy at most `maximum` best levels per side without trimming this state.
    pub fn to_snapshot_with_depth(
        &self,
        exchange: ExchangeId,
        exchange_ts: f64,
        received_ts: f64,
        maximum: usize,
    ) -> L2BookSnapshot {
        L2BookSnapshot {
            exchange,
            symbol: self.symbol.clone(),
            bids: self
                .bids
                .iter()
                .rev()
                .take(maximum)
                .map(|(price, amount)| PriceLevel {
                    price: *price,
                    amount: *amount,
                })
                .collect(),
            asks: self
                .asks
                .iter()
                .take(maximum)
                .map(|(price, amount)| PriceLevel {
                    price: *price,
                    amount: *amount,
                })
                .collect(),
            exchange_ts,
            received_ts,
        }
    }
}

fn apply_levels(side: &mut BTreeMap<Decimal, Decimal>, levels: Vec<PriceLevel>) {
    for level in levels {
        if level.amount.is_zero() {
            side.remove(&level.price);
        } else {
            side.insert(level.price, level.amount);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{L2Book, L2BookDelta, L2BookSnapshot, L2BookState, PriceLevel};
    use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
    use rust_decimal::Decimal;

    #[test]
    fn l2_cross_check_preserves_decimal_precision_and_handles_empty_unsorted_deleted_sides() {
        let symbol = Symbol::spot("BTC", "USDT");
        let level = |price: &str, amount: &str| PriceLevel {
            price: Decimal::from_str_exact(price).unwrap(),
            amount: Decimal::from_str_exact(amount).unwrap(),
        };
        let mut state = L2BookState::new(symbol.clone());
        assert!(!state.is_crossed());
        state.apply(L2Book::Snapshot(L2BookSnapshot {
            exchange: ExchangeId::Okx,
            symbol: symbol.clone(),
            bids: vec![level("99", "1"), level("100.000000000000000000000001", "1")],
            asks: vec![
                level("101", "1"),
                level("100.000000000000000000000002", "1"),
            ],
            exchange_ts: 1.0,
            received_ts: 2.0,
        }));
        assert!(!state.is_crossed());
        state.apply(L2Book::Delta(L2BookDelta {
            exchange: ExchangeId::Okx,
            symbol: symbol.clone(),
            bids: vec![level("100.000000000000000000000002", "1")],
            asks: vec![],
            exchange_ts: 1.0,
            received_ts: 2.0,
        }));
        assert!(state.is_crossed());
        state.apply(L2Book::Delta(L2BookDelta {
            exchange: ExchangeId::Okx,
            symbol: symbol.clone(),
            bids: vec![level("100.000000000000000000000002", "0")],
            asks: vec![],
            exchange_ts: 1.0,
            received_ts: 2.0,
        }));
        assert!(!state.is_crossed());
        state.apply(L2Book::Snapshot(L2BookSnapshot {
            exchange: ExchangeId::Okx,
            symbol: symbol.clone(),
            bids: vec![level("102", "1")],
            asks: vec![],
            exchange_ts: 1.0,
            received_ts: 2.0,
        }));
        assert!(!state.is_crossed());
        state.apply(L2Book::Snapshot(L2BookSnapshot {
            exchange: ExchangeId::Okx,
            symbol,
            bids: vec![],
            asks: vec![level("101", "1")],
            exchange_ts: 1.0,
            received_ts: 2.0,
        }));
        assert!(!state.is_crossed());
    }

    #[test]
    fn l2_delivery_depth_snapshot_copy_is_sorted_bounded_and_never_trims_internal_state() {
        let mut state = L2BookState::new(Symbol::spot("BTC", "USDT"));
        let level = |price: i64| PriceLevel {
            price: Decimal::from(price),
            amount: Decimal::ONE,
        };
        state.apply(L2Book::Snapshot(L2BookSnapshot {
            exchange: ExchangeId::Okx,
            symbol: Symbol::spot("BTC", "USDT"),
            bids: vec![level(98), level(100), level(99)],
            asks: vec![level(103), level(101), level(102)],
            exchange_ts: 1.25,
            received_ts: 2.5,
        }));
        let top = state.to_snapshot_with_depth(ExchangeId::Okx, 1.25, 2.5, 2);
        assert_eq!(top.bids, [level(100), level(99)]);
        assert_eq!(top.asks, [level(101), level(102)]);
        assert_eq!(top.exchange_ts, 1.25);
        assert_eq!(top.received_ts, 2.5);
        assert!(
            state
                .to_snapshot_with_depth(ExchangeId::Okx, 1.25, 2.5, 0)
                .bids
                .is_empty()
        );
        assert_eq!(state.bids().len(), 3);
        assert_eq!(
            state.to_snapshot_with_depth(ExchangeId::Okx, 1.25, 2.5, usize::MAX),
            state.to_snapshot(ExchangeId::Okx, 1.25, 2.5)
        );
    }

    #[test]
    fn snapshot_initializes_state() {
        let snapshot = L2Book::Snapshot(L2BookSnapshot {
            exchange: ExchangeId::Binance,
            symbol: Symbol::spot("btc", "usdt"),
            bids: vec![PriceLevel {
                price: Decimal::from_str_exact("64999.10").unwrap(),
                amount: Decimal::from_str_exact("1.25").unwrap(),
            }],
            asks: vec![PriceLevel {
                price: Decimal::from_str_exact("65000.20").unwrap(),
                amount: Decimal::from_str_exact("0.75").unwrap(),
            }],
            exchange_ts: 1.0,
            received_ts: 2.0,
        });

        let mut state = L2BookState::new(Symbol::spot("btc", "usdt"));
        state.apply(snapshot);

        assert_eq!(state.bids().len(), 1);
        assert_eq!(state.asks().len(), 1);
    }

    #[test]
    fn l2_book_exposes_symbol_and_timestamps() {
        let snapshot = L2Book::Snapshot(L2BookSnapshot {
            exchange: ExchangeId::Binance,
            symbol: Symbol::spot("btc", "usdt"),
            bids: vec![PriceLevel {
                price: Decimal::from_str_exact("64999.10").unwrap(),
                amount: Decimal::from_str_exact("1.25").unwrap(),
            }],
            asks: Vec::new(),
            exchange_ts: 1.5,
            received_ts: 2.5,
        });
        assert_eq!(snapshot.symbol().as_str(), "BTC-USDT");
        assert_eq!(snapshot.bids().len(), 1);
        assert!(snapshot.asks().is_empty());
        assert_eq!(snapshot.exchange_ts(), 1.5);
        assert_eq!(snapshot.received_ts(), 2.5);

        let delta = L2Book::Delta(L2BookDelta {
            exchange: ExchangeId::Binance,
            symbol: Symbol::perpetual("btc", "usdt"),
            bids: Vec::new(),
            asks: Vec::new(),
            exchange_ts: 3.5,
            received_ts: 4.5,
        });
        assert_eq!(delta.symbol().as_str(), "BTC-USDT-PERP");
        assert_eq!(delta.exchange_ts(), 3.5);
        assert_eq!(delta.received_ts(), 4.5);
    }

    #[test]
    fn delta_updates_and_deletes_levels() {
        let snapshot = L2Book::Snapshot(L2BookSnapshot {
            exchange: ExchangeId::Binance,
            symbol: Symbol::spot("btc", "usdt"),
            bids: vec![PriceLevel {
                price: Decimal::from_str_exact("64999.10").unwrap(),
                amount: Decimal::from_str_exact("1.25").unwrap(),
            }],
            asks: vec![PriceLevel {
                price: Decimal::from_str_exact("65000.20").unwrap(),
                amount: Decimal::from_str_exact("0.75").unwrap(),
            }],
            exchange_ts: 1.0,
            received_ts: 2.0,
        });
        let delta = L2Book::Delta(L2BookDelta {
            exchange: ExchangeId::Binance,
            symbol: Symbol::spot("btc", "usdt"),
            bids: vec![
                PriceLevel {
                    price: Decimal::from_str_exact("64999.10").unwrap(),
                    amount: Decimal::from_str_exact("0").unwrap(),
                },
                PriceLevel {
                    price: Decimal::from_str_exact("64998.50").unwrap(),
                    amount: Decimal::from_str_exact("2.00").unwrap(),
                },
            ],
            asks: vec![PriceLevel {
                price: Decimal::from_str_exact("65000.20").unwrap(),
                amount: Decimal::from_str_exact("1.00").unwrap(),
            }],
            exchange_ts: 3.0,
            received_ts: 4.0,
        });

        let mut state = L2BookState::new(Symbol::spot("btc", "usdt"));
        state.apply(snapshot);
        state.apply(delta);

        assert_eq!(state.bids().len(), 1);
        assert_eq!(state.asks().len(), 1);
        assert_eq!(
            state.bids()[0].price,
            Decimal::from_str_exact("64998.50").unwrap()
        );
        assert_eq!(
            state.asks()[0].amount,
            Decimal::from_str_exact("1.00").unwrap()
        );
    }

    #[test]
    fn snapshot_roundtrips_with_serde_json() {
        let snapshot = L2Book::Snapshot(L2BookSnapshot {
            exchange: ExchangeId::Binance,
            symbol: Symbol::spot("btc", "usdt"),
            bids: vec![PriceLevel {
                price: Decimal::from_str_exact("64999.10").unwrap(),
                amount: Decimal::from_str_exact("1.25").unwrap(),
            }],
            asks: vec![PriceLevel {
                price: Decimal::from_str_exact("65000.20").unwrap(),
                amount: Decimal::from_str_exact("0.75").unwrap(),
            }],
            exchange_ts: 10.0,
            received_ts: 11.0,
        });

        let json = serde_json::to_string(&snapshot).unwrap();
        let decoded: L2Book = serde_json::from_str(&json).unwrap();

        match decoded {
            L2Book::Snapshot(snapshot) => {
                assert_eq!(snapshot.symbol.as_str(), "BTC-USDT");
                assert_eq!(snapshot.bids.len(), 1);
                assert_eq!(snapshot.asks.len(), 1);
            }
            L2Book::Delta(_) => panic!("expected snapshot"),
        }
    }

    #[test]
    fn delta_roundtrips_with_serde_json() {
        let delta = L2Book::Delta(L2BookDelta {
            exchange: ExchangeId::Binance,
            symbol: Symbol::spot("btc", "usdt"),
            bids: vec![PriceLevel {
                price: Decimal::from_str_exact("64998.50").unwrap(),
                amount: Decimal::from_str_exact("2.00").unwrap(),
            }],
            asks: vec![PriceLevel {
                price: Decimal::from_str_exact("65000.20").unwrap(),
                amount: Decimal::from_str_exact("1.00").unwrap(),
            }],
            exchange_ts: 12.0,
            received_ts: 13.0,
        });

        let json = serde_json::to_string(&delta).unwrap();
        let decoded: L2Book = serde_json::from_str(&json).unwrap();

        match decoded {
            L2Book::Delta(delta) => {
                assert_eq!(delta.symbol.as_str(), "BTC-USDT");
                assert_eq!(delta.bids.len(), 1);
                assert_eq!(delta.asks.len(), 1);
            }
            L2Book::Snapshot(_) => panic!("expected delta"),
        }
    }
}
