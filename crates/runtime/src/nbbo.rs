//! Bounded best bid/ask selection over explicitly scoped, fresh L1 snapshots.
use crate::feed::FeedIdentity;
use cryptofeed_core::{
    error::{Error, Result},
    exchange::ExchangeId,
    symbol::{InstrumentKind, Symbol},
};
use cryptofeed_orderbook::{L1Book, PriceLevel};
use rust_decimal::Decimal;
use std::time::Duration;

/// Caller-declared units of a source's normalized L1 amounts. No conversion or
/// sum across sources is performed; Contracts can have different face values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NbboQuantityUnit {
    Base,
    Quote,
    Contracts,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NbboSource {
    pub identity: FeedIdentity,
    pub exchange: ExchangeId,
    pub quantity_unit: NbboQuantityUnit,
}

#[derive(Clone, Debug, PartialEq)]
pub struct NbboLeg {
    pub source: NbboSource,
    pub level: PriceLevel,
    pub exchange_ts: f64,
    pub received_ts: f64,
    /// Monotonic source observation time, separate from consumer/model clocks.
    pub observed_at: Duration,
}

#[derive(Clone, Debug, PartialEq)]
pub struct NbboQuote {
    pub bid: NbboLeg,
    pub ask: NbboLeg,
    /// Independent venues may cross; this is reported, never silently repaired.
    pub crossed: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct NbboUpdate {
    pub symbol: Symbol,
    /// None explicitly withdraws the last usable best quote.
    pub quote: Option<NbboQuote>,
}

struct SourceState {
    source: NbboSource,
    available: bool,
    not_before: Duration,
    last_observed: Option<Duration>,
    quote: Option<(L1Book, Duration)>,
}

/// One exact normalized public symbol, 1..=64 configured sources, no I/O/tasks.
/// Supply fresh complete L1 snapshots (or reconstructed L2 tops), never deltas.
/// Call advance on timer ticks and suspend sources on gaps/disconnects/stops.
pub struct Nbbo {
    symbol: Symbol,
    max_age: Duration,
    now: Duration,
    sources: Vec<SourceState>,
    last: Option<NbboQuote>,
}
fn invalid(message: &str) -> Error {
    Error::InvalidConfiguration(message.into())
}
fn valid_source(source: &NbboSource) -> bool {
    source.identity.id.as_u64() > 0 && source.identity.generation > 0
}
fn same_leg(a: &NbboLeg, b: &NbboLeg) -> bool {
    a.source == b.source && a.level == b.level
}
impl Nbbo {
    pub fn new(symbol: Symbol, sources: Vec<NbboSource>, max_age: Duration) -> Result<Self> {
        if !matches!(
            symbol.kind(),
            InstrumentKind::Spot | InstrumentKind::Perpetual | InstrumentKind::Futures
        ) || sources.is_empty()
            || sources.len() > 64
            || max_age.is_zero()
            || sources.iter().any(|source| !valid_source(source))
            || sources.iter().enumerate().any(|(index, source)| {
                sources[..index]
                    .iter()
                    .any(|previous| previous.identity.id == source.identity.id)
            })
        {
            return Err(invalid(
                "NBBO requires a public symbol, 1..=64 distinct feed IDs, valid generations and positive age",
            ));
        }
        Ok(Self {
            symbol,
            max_age,
            now: Duration::ZERO,
            sources: sources
                .into_iter()
                .map(|source| SourceState {
                    source,
                    available: true,
                    not_before: Duration::ZERO,
                    last_observed: None,
                    quote: None,
                })
                .collect(),
            last: None,
        })
    }
    fn check_time(&self, now: Duration) -> Result<()> {
        if now < self.now {
            return Err(invalid("NBBO consumer clock moved backwards"));
        }
        Ok(())
    }
    fn source_index(&self, identity: FeedIdentity) -> Result<usize> {
        self.sources
            .iter()
            .position(|state| state.source.identity == identity)
            .ok_or_else(|| invalid("NBBO source identity is not configured"))
    }
    /// observed_at is when this snapshot was observed, not when dequeued. The
    /// caller must use one monotonic origin for both observed_at and now.
    /// Invalid/stale/backwards inputs leave all state unchanged; use advance to
    /// expire existing quotes even while incoming inputs are invalid.
    pub fn update(
        &mut self,
        identity: FeedIdentity,
        book: &L1Book,
        observed_at: Duration,
        now: Duration,
    ) -> Result<Option<NbboUpdate>> {
        self.check_time(now)?;
        let index = self.source_index(identity)?;
        let state = &self.sources[index];
        if book.symbol != self.symbol
            || book.exchange != state.source.exchange
            || !state.available
            || observed_at > now
            || now - observed_at >= self.max_age
            || observed_at < state.not_before
            || state.last_observed.is_some_and(|last| observed_at < last)
        {
            return Err(invalid(
                "NBBO snapshot scope, availability or observation time is invalid",
            ));
        }
        if book.bid.price <= Decimal::ZERO
            || book.ask.price <= Decimal::ZERO
            || book.bid.amount <= Decimal::ZERO
            || book.ask.amount <= Decimal::ZERO
            || book.bid.price > book.ask.price
            || !book.exchange_ts.is_finite()
            || !book.received_ts.is_finite()
        {
            return Err(Error::MalformedData(
                "NBBO requires finite clocks and a usable non-crossed two-sided L1 snapshot".into(),
            ));
        }
        self.sources[index].quote = Some((book.clone(), observed_at));
        self.sources[index].last_observed = Some(observed_at);
        Ok(self.publish(now))
    }
    /// Suspend on source loss; queued updates are rejected until explicit resume.
    /// Resume clears the old quote and requires observations at/after this time.
    /// Lifecycle signals are caller-owned; this helper does not listen to feeds.
    pub fn set_available(
        &mut self,
        identity: FeedIdentity,
        available: bool,
        now: Duration,
    ) -> Result<Option<NbboUpdate>> {
        self.check_time(now)?;
        let index = self.source_index(identity)?;
        let state = &mut self.sources[index];
        if state.available != available {
            state.available = available;
            state.quote = None;
            state.not_before = now;
            state.last_observed = None;
        }
        Ok(self.publish(now))
    }
    /// Pin a newer managed generation of the same feed ID; withdraw its old
    /// quote before new input. Old buffered generations can no longer update it.
    pub fn replace_source(
        &mut self,
        old: FeedIdentity,
        source: NbboSource,
        now: Duration,
    ) -> Result<Option<NbboUpdate>> {
        self.check_time(now)?;
        let index = self.source_index(old)?;
        if !valid_source(&source)
            || source.identity.id != old.id
            || source.identity.generation <= old.generation
        {
            return Err(invalid(
                "NBBO replacement requires a newer generation of the same feed ID",
            ));
        }
        self.sources[index] = SourceState {
            source,
            available: true,
            not_before: now,
            last_observed: None,
            quote: None,
        };
        Ok(self.publish(now))
    }
    /// Expiry is inclusive: age == max_age is stale. Final-source expiry emits
    /// quote=None; unrelated equal-price refreshes do not produce keepalives.
    pub fn advance(&mut self, now: Duration) -> Result<Option<NbboUpdate>> {
        self.check_time(now)?;
        Ok(self.publish(now))
    }
    fn publish(&mut self, now: Duration) -> Option<NbboUpdate> {
        self.now = now;
        for state in &mut self.sources {
            if state
                .quote
                .as_ref()
                .is_some_and(|(_, observed)| now - *observed >= self.max_age)
            {
                state.quote = None;
            }
        }
        let quote = self.select();
        let same = match (&self.last, &quote) {
            (None, None) => true,
            (Some(a), Some(b)) => same_leg(&a.bid, &b.bid) && same_leg(&a.ask, &b.ask),
            _ => false,
        };
        self.last = quote.clone();
        (!same).then(|| NbboUpdate {
            symbol: self.symbol.clone(),
            quote,
        })
    }
    fn select(&self) -> Option<NbboQuote> {
        let mut quotes = self.sources.iter().filter_map(|state| {
            state
                .quote
                .as_ref()
                .map(|(book, observed)| (state.source, book, *observed))
        });
        let first = quotes.next()?;
        let (mut bid, mut ask) = (first, first);
        for item in quotes {
            // Stable configured-order ties, without adding incomparable sizes.
            if item.1.bid.price > bid.1.bid.price {
                bid = item;
            }
            if item.1.ask.price < ask.1.ask.price {
                ask = item;
            }
        }
        Some(NbboQuote {
            bid: NbboLeg {
                source: bid.0,
                level: bid.1.bid.clone(),
                exchange_ts: bid.1.exchange_ts,
                received_ts: bid.1.received_ts,
                observed_at: bid.2,
            },
            ask: NbboLeg {
                source: ask.0,
                level: ask.1.ask.clone(),
                exchange_ts: ask.1.exchange_ts,
                received_ts: ask.1.received_ts,
                observed_at: ask.2,
            },
            crossed: bid.1.bid.price > ask.1.ask.price,
        })
    }
}

#[cfg(test)]
mod tests;
