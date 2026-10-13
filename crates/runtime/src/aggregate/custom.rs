use crate::feed::FeedEnvelope;
use cryptofeed_core::error::{Error, Result};
use std::{panic::AssertUnwindSafe, time::Duration};

/// Caller-defined state for one elapsed-time window, not an exchange candle.
#[derive(Debug, PartialEq)]
pub struct AggregateWindow<S> {
    pub start: Duration,
    pub end: Duration,
    pub closed: bool,
    pub events: u64,
    pub state: S,
}

/// Synchronous custom calculation over identified normalized input, without I/O
/// or a worker queue. Windows align to elapsed zero and skip empty output.
/// Input count is bounded; allocation/units/source grouping inside S are caller-
/// owned. Initializer/reducer errors or caught panics poison the engine.
pub struct CustomAggregate<S, I, R> {
    window: Duration,
    max_events: u64,
    initialize: I,
    reduce: R,
    current: AggregateWindow<S>,
    last_now: Duration,
    poisoned: bool,
}

fn invalid(message: &str) -> Error {
    Error::InvalidConfiguration(message.into())
}
fn user<T>(stage: &str, work: impl FnOnce() -> Result<T>) -> Result<T> {
    std::panic::catch_unwind(AssertUnwindSafe(work))
        .map_err(|_| Error::Protocol(format!("custom aggregate {stage} panicked")))?
}
impl<S, I, R> CustomAggregate<S, I, R>
where
    I: FnMut() -> Result<S>,
    R: FnMut(&mut S, &FeedEnvelope) -> Result<()>,
{
    /// Positive windows may be subsecond; max_events must be 1..=1_000_000.
    /// initialize must create independent state at construction and each rollover.
    pub fn new(window: Duration, max_events: u64, mut initialize: I, reduce: R) -> Result<Self> {
        if window.is_zero() || !(1..=1_000_000).contains(&max_events) {
            return Err(invalid(
                "custom aggregate needs a positive window and 1..=1000000 events",
            ));
        }
        let state = user("initializer", &mut initialize)?;
        Ok(Self {
            window,
            max_events,
            initialize,
            reduce,
            current: AggregateWindow {
                start: Duration::ZERO,
                end: window,
                closed: false,
                events: 0,
                state,
            },
            last_now: Duration::ZERO,
            poisoned: false,
        })
    }
    fn bounds(&self, now: Duration) -> Result<(Duration, Duration)> {
        if self.poisoned {
            return Err(invalid("custom aggregate is poisoned"));
        }
        if now < self.last_now {
            return Err(invalid("custom aggregate clock moved backwards"));
        }
        let remainder = now.as_nanos() % self.window.as_nanos();
        let remainder = Duration::new(
            (remainder / 1_000_000_000) as u64,
            (remainder % 1_000_000_000) as u32,
        );
        let start = now - remainder;
        let end = start
            .checked_add(self.window)
            .ok_or_else(|| invalid("custom aggregate window boundary overflow"))?;
        Ok((start, end))
    }
    fn fresh(&mut self, start: Duration, end: Duration) -> Result<AggregateWindow<S>> {
        self.poisoned = true;
        let state = user("initializer", &mut self.initialize)?;
        Ok(AggregateWindow {
            start,
            end,
            closed: false,
            events: 0,
            state,
        })
    }
    fn commit(&mut self, next: AggregateWindow<S>, now: Duration) -> Option<AggregateWindow<S>> {
        let mut previous = std::mem::replace(&mut self.current, next);
        previous.closed = true;
        self.last_now = now;
        self.poisoned = false;
        (previous.events > 0).then_some(previous)
    }
    /// Return the populated previous window on rollover. The boundary event is
    /// reduced into new state before commit; user failures are terminal even if
    /// an old window was awaiting output. No partial state rollback is promised.
    /// Clock/capacity/boundary validation failures leave state unchanged.
    pub fn push(
        &mut self,
        input: &FeedEnvelope,
        now: Duration,
    ) -> Result<Option<AggregateWindow<S>>> {
        let (start, end) = self.bounds(now)?;
        if start != self.current.start {
            let mut next = self.fresh(start, end)?;
            user("reducer", || (self.reduce)(&mut next.state, input))?;
            next.events = 1;
            return Ok(self.commit(next, now));
        }
        if self.current.events >= self.max_events {
            return Err(invalid("custom aggregate window event limit reached"));
        }
        self.poisoned = true;
        user("reducer", || (self.reduce)(&mut self.current.state, input))?;
        self.current.events += 1;
        self.last_now = now;
        self.poisoned = false;
        Ok(None)
    }
    /// Timer-driven closure without new input. Large jumps reset once and emit
    /// only a populated prior window, without synthesizing empty output.
    pub fn advance(&mut self, now: Duration) -> Result<Option<AggregateWindow<S>>> {
        let (start, end) = self.bounds(now)?;
        if start == self.current.start {
            self.last_now = now;
            return Ok(None);
        }
        let next = self.fresh(start, end)?;
        Ok(self.commit(next, now))
    }
    /// Consume remaining populated state as partial. A user-code failure makes
    /// finish fail, rather than certifying a potentially partially mutated state.
    pub fn finish(self) -> Result<Option<AggregateWindow<S>>> {
        if self.poisoned {
            return Err(invalid("custom aggregate is poisoned"));
        }
        Ok((self.current.events > 0).then_some(self.current))
    }
}

#[cfg(test)]
mod tests;
