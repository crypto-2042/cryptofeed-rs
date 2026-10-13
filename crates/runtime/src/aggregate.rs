//! Caller-clocked throttling and typed normalized-event/trade aggregation.
mod custom;
pub use custom::{AggregateWindow, CustomAggregate};
mod throttle;
pub use throttle::Throttle;

#[cfg(feature = "trade")]
mod ohlcv;
#[cfg(feature = "trade")]
pub use ohlcv::{Ohlcv, TradeBar};

#[cfg(feature = "trade")]
mod renko;
#[cfg(feature = "trade")]
pub use renko::{RenkoBrick, RenkoDirection, RenkoFixed};
