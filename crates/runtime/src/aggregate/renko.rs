use crate::feed::FeedIdentity;
use cryptofeed_core::{
    error::{Error, Result},
    exchange::ExchangeId,
    symbol::Symbol,
};
use cryptofeed_trade::Trade;
use rust_decimal::Decimal;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RenkoDirection {
    Up,
    Down,
}

/// Threshold-triggered price brick matching Python RenkoFixed's price rules.
/// A gap emits one brick ending at the triggering trade, not synthetic size steps.
#[derive(Clone, Debug, PartialEq)]
pub struct RenkoBrick {
    pub identity: FeedIdentity,
    pub exchange: ExchangeId,
    pub symbol: Symbol,
    pub open: Decimal,
    pub close: Decimal,
    pub direction: RenkoDirection,
    pub exchange_ts: f64,
    pub received_ts: f64,
}
#[derive(Clone)]
struct State {
    identity: FeedIdentity,
    exchange: ExchangeId,
    symbol: Symbol,
    open: Decimal,
    close: Decimal,
    high: Decimal,
    low: Decimal,
    direction: Option<RenkoDirection>,
}
impl State {
    fn matches(&self, identity: FeedIdentity, exchange: ExchangeId, symbol: &Symbol) -> bool {
        self.identity == identity && self.exchange == exchange && &self.symbol == symbol
    }
}
/// Bounded per-source/generation/exchange/symbol Renko state, without tasks/I/O.
/// Arrival order chooses prices; wire clocks are retained, not used for ordering.
/// Remove retired series explicitly to release capacity.
pub struct RenkoFixed {
    size: Decimal,
    max_series: usize,
    states: Vec<State>,
}
impl RenkoFixed {
    pub fn new(size: Decimal, max_series: usize) -> Result<Self> {
        if size <= Decimal::ZERO || !(1..=4096).contains(&max_series) {
            return Err(Error::InvalidConfiguration(
                "Renko needs size>0 and 1..=4096 series".into(),
            ));
        }
        Ok(Self {
            size,
            max_series,
            states: Vec::new(),
        })
    }
    /// First price seeds a series without output. A threshold crossing immediately
    /// returns one brick. Continuations start at the previous close; reversals
    /// retain the previous open. No partial brick is emitted on drop/removal.
    /// Invalid input/capacity/arithmetic errors leave all state unchanged.
    pub fn push(&mut self, identity: FeedIdentity, trade: &Trade) -> Result<Option<RenkoBrick>> {
        if identity.id.as_u64() == 0
            || identity.generation == 0
            || trade.price <= Decimal::ZERO
            || !trade.exchange_ts.is_finite()
            || !trade.received_ts.is_finite()
        {
            return Err(Error::MalformedData(
                "invalid Renko price/source/clock".into(),
            ));
        }
        let index = self
            .states
            .iter()
            .position(|state| state.matches(identity, trade.exchange, &trade.symbol));
        let Some(index) = index else {
            if self.states.len() >= self.max_series {
                return Err(Error::InvalidConfiguration(
                    "Renko active series limit reached".into(),
                ));
            }
            self.states.push(State {
                identity,
                exchange: trade.exchange,
                symbol: trade.symbol.clone(),
                open: trade.price,
                close: trade.price,
                high: trade.price,
                low: trade.price,
                direction: None,
            });
            return Ok(None);
        };
        let mut state = self.states[index].clone();
        state.high = state.high.max(trade.price);
        state.low = state.low.min(trade.price);
        let (lower, upper) = match state.direction {
            None => (state.open, state.open),
            Some(RenkoDirection::Up) => (state.open, state.close),
            Some(RenkoDirection::Down) => (state.close, state.open),
        };
        let arithmetic =
            || Error::MalformedData("Renko price difference is unrepresentable".into());
        let minus = state.low.checked_sub(lower).ok_or_else(arithmetic)?;
        let plus = state.high.checked_sub(upper).ok_or_else(arithmetic)?;
        let difference = if -minus > plus { minus } else { plus };
        let brick = if difference.abs() >= self.size {
            let direction = if difference < Decimal::ZERO {
                RenkoDirection::Down
            } else {
                RenkoDirection::Up
            };
            if state.direction == Some(direction) {
                state.open = state.close;
            }
            state.close = trade.price;
            state.high = trade.price;
            state.low = trade.price;
            state.direction = Some(direction);
            Some(RenkoBrick {
                identity,
                exchange: trade.exchange,
                symbol: trade.symbol.clone(),
                open: state.open,
                close: state.close,
                direction,
                exchange_ts: trade.exchange_ts,
                received_ts: trade.received_ts,
            })
        } else {
            None
        };
        self.states[index] = state;
        Ok(brick)
    }
    /// Discard a retired or gapped series; the next trade seeds a fresh anchor.
    pub fn remove(
        &mut self,
        identity: FeedIdentity,
        exchange: ExchangeId,
        symbol: &Symbol,
    ) -> bool {
        if let Some(index) = self
            .states
            .iter()
            .position(|state| state.matches(identity, exchange, symbol))
        {
            self.states.remove(index);
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests;
