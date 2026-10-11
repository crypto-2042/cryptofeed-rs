use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
fn at(n: u64) -> Duration {
    Duration::from_secs(n)
}
#[test]
fn empty_timer_windows_skip_output_and_initialize_once_per_actual_rollover() {
    let calls = Arc::new(AtomicUsize::new(0));
    let init = calls.clone();
    let mut agg = CustomAggregate::new(
        Duration::from_millis(500),
        10,
        move || {
            init.fetch_add(1, Ordering::Relaxed);
            Ok(0_u64)
        },
        |_: &mut u64, _: &FeedEnvelope| Ok(()),
    )
    .unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert!(agg.advance(Duration::from_millis(499)).unwrap().is_none());
    assert!(agg.advance(Duration::from_millis(500)).unwrap().is_none());
    assert!(agg.advance(at(3600)).unwrap().is_none());
    assert_eq!(calls.load(Ordering::Relaxed), 3);
    assert!(agg.finish().unwrap().is_none());
}
#[test]
fn validation_does_not_invoke_initializer_or_advance_state() {
    let mut agg = CustomAggregate::new(
        at(1),
        1,
        || Ok(0_u64),
        |_: &mut u64, _: &FeedEnvelope| Ok(()),
    )
    .unwrap();
    agg.advance(at(10)).unwrap();
    assert!(agg.advance(at(9)).is_err());
    assert!(agg.advance(Duration::MAX).is_err());
    assert!(agg.advance(at(11)).unwrap().is_none());
    for (window, limit) in [(Duration::ZERO, 1), (at(1), 0), (at(1), 1_000_001)] {
        assert!(
            CustomAggregate::new(
                window,
                limit,
                || -> Result<u64> { panic!("must validate first") },
                |_: &mut u64, _: &FeedEnvelope| Ok(())
            )
            .is_err()
        );
    }
}
#[test]
fn initializer_errors_and_catchable_panics_are_returned() {
    let failed = CustomAggregate::new(
        at(1),
        1,
        || -> Result<u64> { Err(Error::Protocol("init failed".into())) },
        |_: &mut u64, _: &FeedEnvelope| Ok(()),
    );
    assert!(failed.is_err());
    let panicked = CustomAggregate::new(
        at(1),
        1,
        || -> Result<u64> { panic!("init panic") },
        |_: &mut u64, _: &FeedEnvelope| Ok(()),
    );
    assert!(matches!(panicked, Err(Error::Protocol(_))));
}
#[test]
fn later_initializer_failure_poison_blocks_even_empty_success_footer() {
    let mut calls = 0;
    let mut agg = CustomAggregate::new(
        at(1),
        1,
        || {
            calls += 1;
            if calls == 2 {
                return Err(Error::Protocol("reset failed".into()));
            }
            Ok(0_u64)
        },
        |_: &mut u64, _: &FeedEnvelope| Ok(()),
    )
    .unwrap();
    assert!(agg.advance(at(1)).is_err());
    assert!(agg.advance(at(2)).is_err());
    assert!(agg.finish().is_err());
}

#[cfg(feature = "trade")]
mod events {
    use super::*;
    use crate::feed::{FeedEvent, FeedId, FeedIdentity};
    use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
    use cryptofeed_trade::{Side, Trade};
    use rust_decimal::Decimal;
    fn identity() -> FeedIdentity {
        FeedIdentity {
            id: FeedId::allocate(),
            generation: 1,
        }
    }
    fn event(identity: FeedIdentity, price: i64) -> FeedEnvelope {
        FeedEnvelope {
            identity,
            event: FeedEvent::Trade(Trade {
                exchange: ExchangeId::Okx,
                symbol: Symbol::spot("BTC", "USDT"),
                side: Side::Buy,
                amount: Decimal::ONE,
                price: price.into(),
                exchange_ts: 10.5,
                received_ts: 11.5,
                id: None,
                implied_volatility: None,
            }),
        }
    }
    fn reduce(rows: &mut Vec<(FeedIdentity, Trade)>, input: &FeedEnvelope) -> Result<()> {
        let FeedEvent::Trade(trade) = &input.event else {
            return Err(invalid("trade-only calculation"));
        };
        rows.push((input.identity, trade.clone()));
        Ok(())
    }
    #[test]
    fn typed_custom_state_preserves_source_scope_order_and_partial_tail() {
        let a = identity();
        let b = identity();
        let newer = FeedIdentity { generation: 2, ..a };
        let mut agg = CustomAggregate::new(at(5), 10, || Ok(Vec::new()), reduce).unwrap();
        agg.push(&event(a, 10), at(0)).unwrap();
        agg.push(&event(b, 11), at(1)).unwrap();
        agg.push(&event(newer, 12), at(2)).unwrap();
        let closed = agg.push(&event(a, 20), at(5)).unwrap().unwrap();
        assert_eq!(
            (closed.start, closed.end, closed.closed, closed.events),
            (at(0), at(5), true, 3)
        );
        assert_eq!(
            closed.state.iter().map(|row| row.0).collect::<Vec<_>>(),
            vec![a, b, newer]
        );
        assert_eq!(closed.state[2].1.price, 12.into());
        let partial = agg.finish().unwrap().unwrap();
        assert_eq!(partial.events, 1);
        assert!(!partial.closed);
        assert_eq!(partial.state[0].1.price, 20.into());
    }
    #[test]
    fn subsecond_exact_boundary_and_idle_close_use_fixed_elapsed_grid() {
        let a = identity();
        let mut agg =
            CustomAggregate::new(Duration::from_millis(250), 10, || Ok(Vec::new()), reduce)
                .unwrap();
        agg.push(&event(a, 10), Duration::from_millis(249)).unwrap();
        let closed = agg
            .push(&event(a, 11), Duration::from_millis(250))
            .unwrap()
            .unwrap();
        assert_eq!(closed.end, Duration::from_millis(250));
        assert_eq!(closed.events, 1);
        let closed = agg.advance(Duration::from_millis(1000)).unwrap().unwrap();
        assert_eq!(
            (closed.start, closed.end),
            (Duration::from_millis(250), Duration::from_millis(500))
        );
        assert!(closed.closed);
        assert!(agg.advance(at(10)).unwrap().is_none());
        assert!(agg.finish().unwrap().is_none());
    }
    #[test]
    fn capacity_and_clock_errors_leave_callbacks_and_state_unchanged() {
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let mut agg = CustomAggregate::new(
            at(5),
            1,
            || Ok(0_u64),
            move |sum: &mut u64, _: &FeedEnvelope| {
                seen.fetch_add(1, Ordering::Relaxed);
                *sum += 1;
                Ok(())
            },
        )
        .unwrap();
        let input = event(identity(), 10);
        agg.push(&input, at(1)).unwrap();
        assert!(agg.push(&input, at(0)).is_err());
        assert!(agg.push(&input, at(4)).is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert!(agg.advance(at(2)).unwrap().is_none());
        let closed = agg.push(&input, at(5)).unwrap().unwrap();
        assert_eq!(closed.events, 1);
        assert_eq!(closed.state, 1);
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        assert_eq!(agg.finish().unwrap().unwrap().state, 1);
    }
    #[test]
    fn reducer_errors_and_panics_after_mutation_poison_all_further_output() {
        for panic in [false, true] {
            let mut agg = CustomAggregate::new(
                at(5),
                10,
                || Ok(0_u64),
                move |state: &mut u64, _: &FeedEnvelope| {
                    *state = 99;
                    if panic {
                        panic!("after mutation");
                    }
                    Err(Error::Protocol("after mutation".into()))
                },
            )
            .unwrap();
            let input = event(identity(), 10);
            assert!(agg.push(&input, at(0)).is_err());
            assert!(agg.push(&input, at(1)).is_err());
            assert!(agg.advance(at(5)).is_err());
            assert!(agg.finish().is_err());
        }
    }
    #[test]
    fn rollover_user_failure_is_terminal_instead_of_certifying_previous_pending_state() {
        let mut agg = CustomAggregate::new(
            at(5),
            10,
            || Ok(0_u64),
            |state: &mut u64, input: &FeedEnvelope| {
                *state += 1;
                let FeedEvent::Trade(t) = &input.event else {
                    return Err(invalid("trade"));
                };
                if t.price == Decimal::from(20) {
                    return Err(Error::Protocol("bad new window input".into()));
                }
                Ok(())
            },
        )
        .unwrap();
        let a = identity();
        agg.push(&event(a, 10), at(0)).unwrap();
        assert!(agg.push(&event(a, 20), at(5)).is_err());
        assert!(agg.finish().is_err());
    }
    #[test]
    fn explicit_timer_delivery_can_publish_previous_window_before_a_new_reducer_fails() {
        let mut agg = CustomAggregate::new(
            at(5),
            10,
            || Ok(0_u64),
            |state: &mut u64, input: &FeedEnvelope| {
                let FeedEvent::Trade(t) = &input.event else {
                    return Err(invalid("trade"));
                };
                if t.price == Decimal::from(20) {
                    return Err(Error::Protocol("rejected".into()));
                }
                *state += 1;
                Ok(())
            },
        )
        .unwrap();
        let a = identity();
        agg.push(&event(a, 10), at(0)).unwrap();
        let closed = agg.advance(at(5)).unwrap().unwrap();
        assert_eq!(closed.state, 1);
        assert!(closed.closed);
        assert!(agg.push(&event(a, 20), at(5)).is_err());
        assert!(agg.finish().is_err());
    }
    #[cfg(feature = "recording")]
    #[tokio::test]
    async fn replay_reproduces_all_custom_state_and_window_fields() {
        use crate::recording::*;
        let a = identity();
        let mut live = CustomAggregate::new(at(5), 10, || Ok(Vec::new()), reduce).unwrap();
        let mut expected = Vec::new();
        let mut bytes = Vec::new();
        let mut writer = RecordingWriter::new(&mut bytes, RecordingLimits::default())
            .await
            .unwrap();
        for (now, price) in [(0, 10), (1, 11), (5, 12), (6, 13)] {
            let input = event(a, price);
            expected.extend(live.push(&input, at(now)).unwrap());
            writer.append(input, at(now)).await.unwrap();
        }
        writer.finish(RecordingEnd::Complete).await.unwrap();
        expected.extend(live.finish().unwrap());
        let mut replay = CustomAggregate::new(at(5), 10, || Ok(Vec::new()), reduce).unwrap();
        let mut actual = Vec::new();
        let mut reader = RecordingReader::new(bytes.as_slice(), RecordingLimits::default());
        while let Some(row) = reader.next_event().await.unwrap() {
            actual.extend(
                replay
                    .push(
                        &FeedEnvelope {
                            identity: row.identity,
                            event: row.event,
                        },
                        Duration::from_nanos(row.elapsed_ns),
                    )
                    .unwrap(),
            );
        }
        actual.extend(replay.finish().unwrap());
        assert_eq!(actual, expected);
        assert_eq!(actual.len(), 2);
        assert!(actual[0].closed);
        assert!(!actual[1].closed);
    }
}
