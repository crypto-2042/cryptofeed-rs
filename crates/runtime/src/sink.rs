//! Sequential, fallible normalized-event sinks without a hidden worker queue.
use crate::feed::FeedEnvelope;
use cryptofeed_core::error::{Error, Result};
use futures::FutureExt;
use std::{future::Future, panic::AssertUnwindSafe, time::Duration};
use tokio::sync::{broadcast, watch};

/// Why a successfully finalized sink stopped accepting events.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SinkEnd {
    /// All source senders closed and queued events were consumed.
    Complete,
    /// Shutdown was observed between writes. Queued events are not drained.
    Stopped,
    /// The sink reached its declared capacity; its summary describes the prefix.
    LimitReached,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SinkWrite {
    /// Current event accepted; continue consuming.
    Accepted,
    /// Current event accepted; capacity reached, so finalize without more input.
    AcceptedAndFull,
    /// Current event rejected before writing; finalize the preceding prefix.
    Full,
}

/// Implementations receive one owned envelope at a time, in broadcast order.
/// `elapsed` is monotonic consumer time since `run_sink` began, not wire time.
/// Never silently drop input or retry an uncertain write. Errors, panics,
/// deadlines and cancellation drop the owned sink without calling `finish`.
/// Sink futures must be safe to drop; remote writes might already have committed.
pub trait EventSink: Send {
    type Summary;

    fn write(
        &mut self,
        event: FeedEnvelope,
        elapsed: Duration,
    ) -> impl Future<Output = Result<SinkWrite>> + Send;

    /// Consumes the sink and confirms its own flush/acknowledgement contract.
    /// Successful completion does not inherently mean durable storage.
    fn finish(self, end: SinkEnd) -> impl Future<Output = Result<Self::Summary>> + Send;
}

#[derive(Clone, Copy, Debug)]
pub struct SinkOptions {
    operation_timeout: Duration,
}
impl Default for SinkOptions {
    fn default() -> Self {
        Self {
            operation_timeout: Duration::from_secs(5),
        }
    }
}
impl SinkOptions {
    pub fn with_operation_timeout(mut self, timeout: Duration) -> Result<Self> {
        if timeout.is_zero() {
            return Err(Error::InvalidConfiguration(
                "sink operation timeout must be positive".into(),
            ));
        }
        self.operation_timeout = timeout;
        Ok(self)
    }
}

async fn stopped(shutdown: &mut watch::Receiver<bool>) {
    let _ = shutdown.wait_for(|stop| *stop).await;
}

async fn operation<T>(timeout: Duration, work: impl Future<Output = Result<T>>) -> Result<T> {
    tokio::time::timeout(timeout, AssertUnwindSafe(work).catch_unwind())
        .await
        .map_err(|_| Error::Transport("sink operation deadline".into()))?
        .map_err(|_| Error::Transport("sink operation panicked".into()))?
}

/// Run a sink over `FeedHandler::subscribe_identified()`.
/// There is no extra queue or retry. Slow writes can overflow the caller's
/// broadcast buffer: lag is terminal, never a successful shortened capture.
/// Shutdown during a write is an error (unknown commit), not a clean footer.
/// Once finalization starts it is bounded by the operation deadline.
pub async fn run_sink<S: EventSink>(
    mut sink: S,
    mut source: broadcast::Receiver<FeedEnvelope>,
    options: SinkOptions,
    mut shutdown: watch::Receiver<bool>,
) -> Result<S::Summary> {
    let started = tokio::time::Instant::now();
    let end = loop {
        let event = tokio::select! { biased;
            _ = stopped(&mut shutdown) => break SinkEnd::Stopped,
            event = source.recv() => match event {
                Ok(event) => event,
                Err(broadcast::error::RecvError::Closed) => break SinkEnd::Complete,
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    return Err(Error::Protocol(format!("sink lost {count} broadcast events")));
                }
            },
        };
        let result = tokio::select! { biased;
            _ = stopped(&mut shutdown) => {
                return Err(Error::Transport("sink interrupted during write".into()));
            }
            result = operation(options.operation_timeout, async {
                sink.write(event, started.elapsed()).await
            }) => result?,
        };
        if result != SinkWrite::Accepted {
            break SinkEnd::LimitReached;
        }
    };
    operation(options.operation_timeout, async { sink.finish(end).await }).await
}

#[cfg(all(test, feature = "trade"))]
mod tests;
