//! Bounded, caller-clocked aggregation of normalized public trades.
use crate::feed::FeedIdentity;
use cryptofeed_core::{
    error::{Error, Result},
    exchange::ExchangeId,
    symbol::Symbol,
};
use cryptofeed_trade::Trade;
use rust_decimal::Decimal;
use std::time::Duration;

/// A locally aggregated arrival-time bar, not an exchange candle.
/// Volume is the sum of native normalized trade amounts (possibly contracts).
/// VWAP weights prices by that same amount, without base/quote conversion.
#[derive(Clone, Debug, PartialEq)]
pub struct TradeBar {
    pub identity: FeedIdentity,
    pub exchange: ExchangeId,
    pub symbol: Symbol,
    /// Monotonic elapsed time from the caller's origin; not Unix time.
    pub start: Duration,
    /// Exclusive boundary. Empty windows are not synthesized.
    pub end: Duration,
    /// True once the caller's clock crosses `end`, not proof of market completeness.
    pub closed: bool,
    pub trades: u64,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub price_volume: Decimal,
    pub vwap: Decimal,
    pub first_exchange_ts: f64,
    pub last_exchange_ts: f64,
    pub first_received_ts: f64,
    pub last_received_ts: f64,
}

/// One active window, bounded series count, no tasks, I/O or hidden wall clock.
/// Call `push` for trades and `advance` on timer ticks; `finish` returns partial
/// bars. Windows are aligned to elapsed zero, and source generations never mix.
pub struct Ohlcv {
    window: Duration,
    max_series: usize,
    last_now: Duration,
    start: Duration,
    bars: Vec<TradeBar>,
}

fn invalid(message: &str) -> Error {
    Error::InvalidConfiguration(message.into())
}
fn arithmetic() -> Error {
    Error::MalformedData("OHLCV decimal/count arithmetic is unrepresentable".into())
}

impl Ohlcv {
    /// Windows must be positive whole seconds; series limit must be 1..=4096.
    pub fn new(window: Duration, max_series: usize) -> Result<Self> {
        if window.is_zero() || window.subsec_nanos() != 0 || !(1..=4096).contains(&max_series) {
            return Err(invalid(
                "OHLCV needs a positive whole-second window and 1..=4096 series",
            ));
        }
        Ok(Self {
            window,
            max_series,
            last_now: Duration::ZERO,
            start: Duration::ZERO,
            bars: Vec::new(),
        })
    }

    fn bounds(&self, now: Duration) -> Result<(Duration, Duration)> {
        if now < self.last_now {
            return Err(invalid("OHLCV clock moved backwards"));
        }
        let seconds = now.as_secs() / self.window.as_secs() * self.window.as_secs();
        let start = Duration::from_secs(seconds);
        let end = start
            .checked_add(self.window)
            .ok_or_else(|| invalid("OHLCV window boundary overflow"))?;
        Ok((start, end))
    }

    /// Emits completed bars in first-seen series order, then accepts this trade.
    /// Rejected inputs leave all state/clock unchanged. Open/close follow arrival
    /// order even if exchange timestamps are late or out of order; IDs are not
    /// deduplicated. Decimal operations use checked rust_decimal arithmetic.
    pub fn push(
        &mut self,
        identity: FeedIdentity,
        trade: &Trade,
        now: Duration,
    ) -> Result<Vec<TradeBar>> {
        if identity.id.as_u64() == 0
            || identity.generation == 0
            || trade.price <= Decimal::ZERO
            || trade.amount <= Decimal::ZERO
            || !trade.exchange_ts.is_finite()
            || !trade.received_ts.is_finite()
        {
            return Err(Error::MalformedData("invalid OHLCV trade/source".into()));
        }
        let (start, end) = self.bounds(now)?;
        let index = (start == self.start)
            .then(|| {
                self.bars.iter().position(|bar| {
                    bar.identity == identity
                        && bar.exchange == trade.exchange
                        && bar.symbol == trade.symbol
                })
            })
            .flatten();
        if start == self.start && index.is_none() && self.bars.len() >= self.max_series {
            return Err(invalid("OHLCV active series limit reached"));
        }
        let weight = trade
            .price
            .checked_mul(trade.amount)
            .ok_or_else(arithmetic)?;
        if weight.is_zero() {
            return Err(arithmetic());
        }
        let candidate = if let Some(index) = index {
            let mut bar = self.bars[index].clone();
            bar.volume = bar
                .volume
                .checked_add(trade.amount)
                .ok_or_else(arithmetic)?;
            bar.price_volume = bar
                .price_volume
                .checked_add(weight)
                .ok_or_else(arithmetic)?;
            bar.vwap = bar
                .price_volume
                .checked_div(bar.volume)
                .ok_or_else(arithmetic)?;
            bar.trades = bar.trades.checked_add(1).ok_or_else(arithmetic)?;
            bar.high = bar.high.max(trade.price);
            bar.low = bar.low.min(trade.price);
            bar.close = trade.price;
            bar.last_exchange_ts = trade.exchange_ts;
            bar.last_received_ts = trade.received_ts;
            bar
        } else {
            TradeBar {
                identity,
                exchange: trade.exchange,
                symbol: trade.symbol.clone(),
                start,
                end,
                closed: false,
                trades: 1,
                open: trade.price,
                high: trade.price,
                low: trade.price,
                close: trade.price,
                volume: trade.amount,
                price_volume: weight,
                vwap: trade.price,
                first_exchange_ts: trade.exchange_ts,
                last_exchange_ts: trade.exchange_ts,
                first_received_ts: trade.received_ts,
                last_received_ts: trade.received_ts,
            }
        };
        let completed = self.advance(now)?;
        if let Some(index) = index {
            self.bars[index] = candidate;
        } else {
            self.bars.push(candidate);
        }
        Ok(completed)
    }

    /// Closes populated windows even when no further trade arrives. A large clock
    /// jump emits only the populated previous window, with no empty filler bars.
    pub fn advance(&mut self, now: Duration) -> Result<Vec<TradeBar>> {
        let (start, _) = self.bounds(now)?;
        self.last_now = now;
        if start == self.start {
            return Ok(Vec::new());
        }
        self.start = start;
        let mut completed = std::mem::take(&mut self.bars);
        for bar in &mut completed {
            bar.closed = true;
        }
        Ok(completed)
    }

    /// Consume remaining partial bars without inventing a completed window.
    pub fn finish(self) -> Vec<TradeBar> {
        self.bars
    }
}

#[cfg(test)]
mod tests;
