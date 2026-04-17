use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum BookSide {
    Bid,
    Ask,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: Decimal,
    pub amount: Decimal,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct L2BookSnapshot {
    pub exchange: ExchangeId,
    pub symbol: Symbol,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub exchange_ts: f64,
    pub received_ts: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct L2BookDelta {
    pub exchange: ExchangeId,
    pub symbol: Symbol,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub exchange_ts: f64,
    pub received_ts: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum L2Book {
    Snapshot(L2BookSnapshot),
    Delta(L2BookDelta),
}

#[derive(Clone, Debug)]
pub struct L2BookState {
    symbol: Symbol,
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
}

impl L2BookState {
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
}
