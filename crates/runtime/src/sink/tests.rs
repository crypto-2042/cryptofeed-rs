use super::*;
use crate::feed::{FeedEvent, FeedId, FeedIdentity};
use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
use cryptofeed_trade::{Side, Trade};
use std::sync::{Arc, Mutex};

fn event(identity: FeedIdentity, id: &str) -> FeedEnvelope {
    FeedEnvelope {
        identity,
        event: FeedEvent::Trade(Trade {
            exchange: ExchangeId::Okx,
            symbol: Symbol::spot("BTC", "USDT"),
            id: Some(id.into()),
            side: Side::Buy,
            price: "100.1234567890123456789012345".parse().unwrap(),
            amount: "0.1234567890123456789012345678".parse().unwrap(),
            implied_volatility: None,
            exchange_ts: 1.25,
            received_ts: 2.5,
        }),
    }
}
fn identity() -> FeedIdentity {
    FeedIdentity {
        id: FeedId::allocate(),
        generation: 2,
    }
}
#[derive(Default)]
struct State {
    events: Vec<(FeedEnvelope, Duration)>,
    finished: Vec<SinkEnd>,
    dropped: bool,
}
#[derive(Clone, Copy)]
enum Mode {
    Normal,
    Error,
    Panic,
    Pending,
    Delayed,
    FinishPending,
    FinishError,
    FinishPanic,
}
struct Probe {
    state: Arc<Mutex<State>>,
    mode: Mode,
    entered: Arc<tokio::sync::Notify>,
    released: Arc<tokio::sync::Notify>,
}
impl Drop for Probe {
    fn drop(&mut self) {
        self.state.lock().unwrap().dropped = true;
    }
}
impl EventSink for Probe {
    type Summary = SinkEnd;
    async fn write(&mut self, event: FeedEnvelope, elapsed: Duration) -> Result<SinkWrite> {
        self.entered.notify_one();
        match self.mode {
            Mode::Error => return Err(Error::Transport("backend failed".into())),
            Mode::Panic => panic!("backend panic"),
            Mode::Pending => std::future::pending().await,
            Mode::Delayed => self.released.notified().await,
            _ => {}
        }
        self.state.lock().unwrap().events.push((event, elapsed));
        Ok(SinkWrite::Accepted)
    }
    async fn finish(self, end: SinkEnd) -> Result<Self::Summary> {
        match self.mode {
            Mode::FinishPending => std::future::pending().await,
            Mode::FinishError => return Err(Error::Transport("flush failed".into())),
            Mode::FinishPanic => panic!("flush panic"),
            _ => {}
        }
        self.state.lock().unwrap().finished.push(end);
        Ok(end)
    }
}
fn probe(mode: Mode) -> Probe {
    Probe {
        state: Default::default(),
        mode,
        entered: Default::default(),
        released: Default::default(),
    }
}
#[tokio::test]
async fn ordered_owned_envelopes_preserve_generation_and_close_after_drain() {
    let sink = probe(Mode::Normal);
    let state = sink.state.clone();
    let id = identity();
    let (tx, rx) = broadcast::channel(4);
    tx.send(event(id, "first")).unwrap();
    tx.send(event(
        FeedIdentity {
            generation: 3,
            ..id
        },
        "second",
    ))
    .unwrap();
    drop(tx);
    let (_stop, shutdown) = watch::channel(false);
    assert_eq!(
        run_sink(sink, rx, SinkOptions::default(), shutdown)
            .await
            .unwrap(),
        SinkEnd::Complete
    );
    let state = state.lock().unwrap();
    assert_eq!(state.events.len(), 2);
    assert_eq!(state.events[0].0.identity, id);
    assert_eq!(state.events[1].0.identity.generation, 3);
    assert!(state.events[1].1 >= state.events[0].1);
    let FeedEvent::Trade(trade) = &state.events[0].0.event else {
        panic!("trade")
    };
    assert_eq!(trade.id.as_deref(), Some("first"));
    assert_eq!(trade.price.to_string(), "100.1234567890123456789012345");
    assert_eq!(state.finished, vec![SinkEnd::Complete]);
    assert!(state.dropped);
}
#[tokio::test]
async fn lag_is_terminal_without_a_successful_flush_or_retry() {
    let sink = probe(Mode::Normal);
    let state = sink.state.clone();
    let (tx, rx) = broadcast::channel(1);
    for id in ["1", "2", "3"] {
        tx.send(event(identity(), id)).unwrap();
    }
    let (_stop, shutdown) = watch::channel(false);
    assert!(matches!(
        run_sink(sink, rx, SinkOptions::default(), shutdown).await,
        Err(Error::Protocol(_))
    ));
    let state = state.lock().unwrap();
    assert!(state.events.is_empty());
    assert!(state.finished.is_empty());
    assert!(state.dropped);
}
#[tokio::test]
async fn slow_write_can_lag_the_source_but_never_creates_an_extra_queue() {
    let sink = probe(Mode::Delayed);
    let state = sink.state.clone();
    let entered = sink.entered.clone();
    let released = sink.released.clone();
    let (tx, rx) = broadcast::channel(1);
    tx.send(event(identity(), "first")).unwrap();
    let (_stop, shutdown) = watch::channel(false);
    let task = tokio::spawn(run_sink(sink, rx, SinkOptions::default(), shutdown));
    entered.notified().await;
    for id in ["second", "third", "fourth"] {
        tx.send(event(identity(), id)).unwrap();
    }
    released.notify_one();
    assert!(matches!(task.await.unwrap(), Err(Error::Protocol(_))));
    let state = state.lock().unwrap();
    assert_eq!(state.events.len(), 1);
    assert!(state.finished.is_empty());
    assert!(state.dropped);
}
#[tokio::test(start_paused = true)]
async fn failed_panicking_or_timed_out_writes_never_finalize() {
    for mode in [Mode::Error, Mode::Panic, Mode::Pending] {
        let sink = probe(mode);
        let state = sink.state.clone();
        let (tx, rx) = broadcast::channel(1);
        tx.send(event(identity(), "1")).unwrap();
        let (_stop, shutdown) = watch::channel(false);
        assert!(
            run_sink(sink, rx, SinkOptions::default(), shutdown)
                .await
                .is_err()
        );
        let state = state.lock().unwrap();
        assert!(state.finished.is_empty());
        assert!(state.dropped);
    }
}
#[tokio::test(start_paused = true)]
async fn finalization_errors_panics_and_deadlines_propagate() {
    for mode in [Mode::FinishError, Mode::FinishPanic, Mode::FinishPending] {
        let sink = probe(mode);
        let state = sink.state.clone();
        let (tx, rx) = broadcast::channel(1);
        drop(tx);
        let (_stop, shutdown) = watch::channel(false);
        assert!(
            run_sink(sink, rx, SinkOptions::default(), shutdown)
                .await
                .is_err()
        );
        assert!(state.lock().unwrap().dropped);
    }
}
#[tokio::test]
async fn shutdown_between_operations_finishes_without_draining() {
    for close_sender in [false, true] {
        let sink = probe(Mode::Normal);
        let state = sink.state.clone();
        let (tx, rx) = broadcast::channel(1);
        tx.send(event(identity(), "queued")).unwrap();
        let (stop, shutdown) = watch::channel(!close_sender);
        if close_sender {
            drop(stop);
        }
        assert_eq!(
            run_sink(sink, rx, SinkOptions::default(), shutdown)
                .await
                .unwrap(),
            SinkEnd::Stopped
        );
        let state = state.lock().unwrap();
        assert!(state.events.is_empty());
        assert_eq!(state.finished, vec![SinkEnd::Stopped]);
    }
}
#[tokio::test]
async fn shutdown_or_runner_drop_during_write_is_not_a_clean_finish() {
    for abort in [false, true] {
        let sink = probe(Mode::Pending);
        let entered = sink.entered.clone();
        let state = sink.state.clone();
        let (tx, rx) = broadcast::channel(1);
        tx.send(event(identity(), "1")).unwrap();
        let (stop, shutdown) = watch::channel(false);
        let task = tokio::spawn(run_sink(sink, rx, SinkOptions::default(), shutdown));
        entered.notified().await;
        if abort {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            stop.send(true).unwrap();
            assert!(task.await.unwrap().is_err());
        }
        let state = state.lock().unwrap();
        assert!(state.finished.is_empty());
        assert!(state.dropped);
    }
}
#[test]
fn operation_timeout_must_be_positive() {
    assert!(
        SinkOptions::default()
            .with_operation_timeout(Duration::ZERO)
            .is_err()
    );
}

#[cfg(feature = "recording")]
#[tokio::test]
async fn jsonl_adapter_roundtrips_scoped_precision_and_finishes_at_event_budget() {
    use crate::recording::*;
    let mut bytes = Vec::new();
    let limits = RecordingLimits::new(2, 8192, 2048).unwrap();
    let writer = RecordingWriter::new(&mut bytes, limits).await.unwrap();
    let (tx, rx) = broadcast::channel(4);
    let id = identity();
    for key in ["1", "2", "extra"] {
        tx.send(event(id, key)).unwrap();
    }
    let (_stop, shutdown) = watch::channel(false);
    let summary = run_sink(writer, rx, SinkOptions::default(), shutdown)
        .await
        .unwrap();
    assert_eq!(summary.events, 2);
    assert_eq!(summary.end, RecordingEnd::LimitReached);
    let mut reader = RecordingReader::new(bytes.as_slice(), limits);
    for key in ["1", "2"] {
        let row = reader.next_event().await.unwrap().unwrap();
        assert_eq!(row.identity, id);
        let FeedEvent::Trade(trade) = row.event else {
            panic!("trade")
        };
        assert_eq!(trade.id.as_deref(), Some(key));
        assert_eq!(trade.amount.to_string(), "0.1234567890123456789012345678");
        assert_eq!(trade.exchange_ts, 1.25);
        assert_eq!(trade.received_ts, 2.5);
    }
    assert!(reader.next_event().await.unwrap().is_none());
    assert_eq!(reader.summary(), Some(summary));
}
#[cfg(feature = "recording")]
#[tokio::test]
async fn jsonl_adapter_reports_unaccepted_oversized_event_and_lag_has_no_footer() {
    use crate::recording::*;
    for lag in [false, true] {
        let mut bytes = Vec::new();
        let limits = RecordingLimits::new(10, 4096, 256).unwrap();
        let writer = RecordingWriter::new(&mut bytes, limits).await.unwrap();
        let (tx, rx) = broadcast::channel(1);
        tx.send(event(identity(), &"x".repeat(1000))).unwrap();
        if lag {
            tx.send(event(identity(), "overflow")).unwrap();
        }
        let (_stop, shutdown) = watch::channel(false);
        let result = run_sink(writer, rx, SinkOptions::default(), shutdown).await;
        let mut reader = RecordingReader::new(bytes.as_slice(), limits);
        if lag {
            assert!(result.is_err());
            assert!(reader.next_event().await.is_err());
        } else {
            let summary = result.unwrap();
            assert_eq!(summary.events, 0);
            assert_eq!(summary.end, RecordingEnd::LimitReached);
            assert!(reader.next_event().await.unwrap().is_none());
        }
    }
}
