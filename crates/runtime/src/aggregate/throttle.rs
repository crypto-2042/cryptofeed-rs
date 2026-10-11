use cryptofeed_core::error::{Error, Result};
use std::time::Duration;

/// One leading-edge throttle, shared by all events passed to this instance.
/// First input passes; subsequent input must be strictly more than `window`
/// after the last accepted input, matching Python's Throttle boundary.
/// Caller supplies monotonic elapsed time; no buffering, trailing delivery or I/O.
#[derive(Debug)]
pub struct Throttle {
    window: Duration,
    last_seen: Duration,
    last_accepted: Option<Duration>,
}
impl Throttle {
    pub fn new(window: Duration) -> Result<Self> {
        if window.is_zero() {
            return Err(Error::InvalidConfiguration(
                "throttle interval must be positive".into(),
            ));
        }
        Ok(Self {
            window,
            last_seen: Duration::ZERO,
            last_accepted: None,
        })
    }
    /// False deliberately omits an event. Never use this to discard L2 deltas
    /// before reconstruction, or trades before an aggregate requiring all trades.
    /// Rejected backwards clocks leave state unchanged.
    pub fn allow(&mut self, now: Duration) -> Result<bool> {
        if now < self.last_seen {
            return Err(Error::InvalidConfiguration(
                "throttle clock moved backwards".into(),
            ));
        }
        let allowed = self
            .last_accepted
            .is_none_or(|last| now - last > self.window);
        self.last_seen = now;
        if allowed {
            self.last_accepted = Some(now);
        }
        Ok(allowed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn leading_edge_strict_boundary_and_drops_do_not_extend_window() {
        let mut throttle = Throttle::new(Duration::from_secs(1)).unwrap();
        assert!(throttle.allow(Duration::ZERO).unwrap());
        assert!(!throttle.allow(Duration::from_millis(999)).unwrap());
        assert!(!throttle.allow(Duration::from_secs(1)).unwrap());
        assert!(throttle.allow(Duration::from_nanos(1_000_000_001)).unwrap());
        assert!(!throttle.allow(Duration::from_nanos(1_000_000_001)).unwrap());
    }
    #[test]
    fn backwards_clock_after_drop_is_atomic_and_subseconds_are_supported() {
        let mut throttle = Throttle::new(Duration::from_millis(100)).unwrap();
        assert!(throttle.allow(Duration::from_millis(100)).unwrap());
        assert!(!throttle.allow(Duration::from_millis(150)).unwrap());
        assert!(throttle.allow(Duration::from_millis(149)).is_err());
        assert!(throttle.allow(Duration::from_millis(201)).unwrap());
    }
    #[test]
    fn separate_instances_have_independent_state_and_large_clocks_do_not_overflow() {
        let mut first = Throttle::new(Duration::from_secs(1)).unwrap();
        let mut second = Throttle::new(Duration::from_secs(1)).unwrap();
        assert!(first.allow(Duration::ZERO).unwrap());
        assert!(second.allow(Duration::from_millis(1)).unwrap());
        assert!(first.allow(Duration::MAX).unwrap());
        assert!(!first.allow(Duration::MAX).unwrap());
        assert!(Throttle::new(Duration::ZERO).is_err());
    }
}
