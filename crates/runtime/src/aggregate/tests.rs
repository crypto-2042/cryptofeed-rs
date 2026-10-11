use super::*;
use crate::feed::FeedId;
use cryptofeed_trade::Side;

fn seconds(n: u64) -> Duration {
    Duration::from_secs(n)
}
fn identity() -> FeedIdentity {
    FeedIdentity {
        id: FeedId::allocate(),
        generation: 1,
    }
}
fn trade(price: &str, amount: &str) -> Trade {
    Trade {
        exchange: ExchangeId::Okx,
        symbol: Symbol::spot("BTC", "USDT"),
        side: Side::Buy,
        amount: amount.parse().unwrap(),
        price: price.parse().unwrap(),
        exchange_ts: 100.0,
        received_ts: 101.0,
        id: Some("same-id-is-not-deduplicated".into()),
        implied_volatility: None,
    }
}
#[test]
fn ohlcv_and_weighted_price_use_native_amounts_and_arrival_order() {
    let id = identity();
    let mut agg = Ohlcv::new(seconds(60), 4).unwrap();
    for (now, price, amount) in [
        (0, "10", "2"),
        (1, "15", "1"),
        (2, "9", "3"),
        (59, "12", "4"),
    ] {
        assert!(
            agg.push(id, &trade(price, amount), seconds(now))
                .unwrap()
                .is_empty()
        );
    }
    let bars = agg.advance(seconds(60)).unwrap();
    assert_eq!(bars.len(), 1);
    let b = &bars[0];
    assert_eq!(b.identity, id);
    assert_eq!(b.start, seconds(0));
    assert_eq!(b.end, seconds(60));
    assert!(b.closed);
    assert_eq!(b.trades, 4);
    assert_eq!(
        (b.open, b.high, b.low, b.close),
        (10.into(), 15.into(), 9.into(), 12.into())
    );
    assert_eq!(b.volume, 10.into());
    assert_eq!(b.price_volume, 110.into());
    assert_eq!(b.vwap, 11.into());
    assert!(agg.finish().is_empty());
}
#[test]
fn exact_boundary_trade_starts_new_window_and_stopping_keeps_it_partial() {
    let id = identity();
    let mut agg = Ohlcv::new(seconds(5), 1).unwrap();
    agg.push(id, &trade("10", "1"), Duration::from_millis(4999))
        .unwrap();
    let closed = agg.push(id, &trade("20", "2"), seconds(5)).unwrap();
    assert_eq!(closed.len(), 1);
    assert_eq!(closed[0].close, 10.into());
    assert!(closed[0].closed);
    let partial = agg.finish();
    assert_eq!(partial.len(), 1);
    assert_eq!(
        (partial[0].start, partial[0].end),
        (seconds(5), seconds(10))
    );
    assert_eq!(partial[0].open, 20.into());
    assert!(!partial[0].closed);
}
#[test]
fn identity_generation_exchange_symbol_and_contract_units_remain_isolated() {
    let id = identity();
    let other = identity();
    let mut agg = Ohlcv::new(seconds(60), 8).unwrap();
    let mut input = trade("10", "7");
    agg.push(id, &input, seconds(0)).unwrap();
    agg.push(
        FeedIdentity {
            generation: 2,
            ..id
        },
        &input,
        seconds(0),
    )
    .unwrap();
    agg.push(other, &input, seconds(0)).unwrap();
    input.exchange = ExchangeId::Bybit;
    agg.push(id, &input, seconds(0)).unwrap();
    input.symbol = Symbol::perpetual("BTC", "USDT");
    agg.push(id, &input, seconds(0)).unwrap();
    input.symbol = Symbol::spot("ETH", "USDT");
    agg.push(id, &input, seconds(0)).unwrap();
    let bars = agg.advance(seconds(60)).unwrap();
    assert_eq!(bars.len(), 6);
    assert_eq!(bars[0].identity, id);
    assert_eq!(bars[1].identity.generation, 2);
    assert_eq!(bars[2].identity, other);
    assert_eq!(bars[3].exchange, ExchangeId::Bybit);
    assert_eq!(bars[4].symbol, Symbol::perpetual("BTC", "USDT"));
    assert!(
        bars.iter()
            .all(|b| b.trades == 1 && b.volume == Decimal::from(7))
    );
}
#[test]
fn backwards_exchange_clock_is_retained_but_does_not_reorder_open_close() {
    let id = identity();
    let mut agg = Ohlcv::new(seconds(60), 1).unwrap();
    let first = trade("10", "1");
    agg.push(id, &first, seconds(10)).unwrap();
    let mut late = trade("20", "1");
    late.exchange_ts = 80.5;
    late.received_ts = 99.5;
    agg.push(id, &late, seconds(11)).unwrap();
    let bars = agg.finish();
    assert_eq!(bars[0].open, first.price);
    assert_eq!(bars[0].close, late.price);
    assert_eq!(
        (bars[0].first_exchange_ts, bars[0].last_exchange_ts),
        (100.0, 80.5)
    );
    assert_eq!(
        (bars[0].first_received_ts, bars[0].last_received_ts),
        (101.0, 99.5)
    );
}
#[test]
fn timer_closes_without_more_trades_and_large_jump_does_not_synthesize_gaps() {
    let mut agg = Ohlcv::new(seconds(60), 1).unwrap();
    assert!(agg.advance(seconds(3600)).unwrap().is_empty());
    agg.push(identity(), &trade("10", "1"), seconds(3601))
        .unwrap();
    let bars = agg.advance(seconds(86400)).unwrap();
    assert_eq!(bars.len(), 1);
    assert_eq!((bars[0].start, bars[0].end), (seconds(3600), seconds(3660)));
    assert!(bars[0].closed);
    assert!(agg.advance(seconds(86400)).unwrap().is_empty());
}
#[test]
fn backwards_consumer_clock_rejects_without_modifying_state_or_advancing_watermark() {
    let id = identity();
    let mut agg = Ohlcv::new(seconds(60), 1).unwrap();
    agg.push(id, &trade("10", "1"), seconds(10)).unwrap();
    assert!(agg.push(id, &trade("20", "1"), seconds(9)).is_err());
    assert!(agg.advance(seconds(9)).is_err());
    agg.push(id, &trade("11", "1"), seconds(11)).unwrap();
    let bars = agg.finish();
    assert_eq!(bars[0].trades, 2);
    assert_eq!(bars[0].volume, 2.into());
    assert_eq!(bars[0].close, 11.into());
}
#[test]
fn bounded_series_rejection_is_atomic_and_closed_window_releases_capacity() {
    let id = identity();
    let other = identity();
    let mut agg = Ohlcv::new(seconds(60), 1).unwrap();
    agg.push(id, &trade("10", "1"), seconds(1)).unwrap();
    assert!(agg.push(other, &trade("20", "1"), seconds(20)).is_err());
    agg.push(id, &trade("11", "1"), seconds(2)).unwrap();
    let bars = agg.push(other, &trade("30", "1"), seconds(60)).unwrap();
    assert_eq!(bars.len(), 1);
    assert_eq!(bars[0].trades, 2);
    let remaining = agg.finish();
    assert_eq!(remaining[0].identity, other);
    assert_eq!(remaining[0].trades, 1);
}
#[test]
fn invalid_trade_or_generation_cannot_close_or_mutate_an_existing_window() {
    let id = identity();
    let mut agg = Ohlcv::new(seconds(60), 1).unwrap();
    agg.push(id, &trade("10", "1"), seconds(1)).unwrap();
    for mode in 0..7 {
        let mut bad = trade("20", "1");
        let mut source = id;
        match mode {
            0 => bad.price = Decimal::ZERO,
            1 => bad.amount = Decimal::ZERO,
            2 => bad.amount = Decimal::NEGATIVE_ONE,
            3 => bad.exchange_ts = f64::NAN,
            4 => bad.received_ts = f64::INFINITY,
            5 => source.generation = 0,
            6 => bad.price = Decimal::NEGATIVE_ONE,
            _ => unreachable!(),
        }
        assert!(agg.push(source, &bad, seconds(60)).is_err());
    }
    let bars = agg.advance(seconds(60)).unwrap();
    assert_eq!(bars.len(), 1);
    assert_eq!(bars[0].trades, 1);
    assert_eq!(bars[0].close, 10.into());
}
#[test]
fn decimal_overflow_and_underflow_reject_atomically_including_at_rollover() {
    let id = identity();
    let mut agg = Ohlcv::new(seconds(60), 1).unwrap();
    agg.push(id, &trade("1", "1"), seconds(0)).unwrap();
    for now in [seconds(1), seconds(60)] {
        let mut huge = trade("1", "2");
        huge.price = Decimal::MAX;
        assert!(agg.push(id, &huge, now).is_err());
        assert!(
            agg.push(
                id,
                &trade(
                    "0.0000000000000000000000000001",
                    "0.0000000000000000000000000001"
                ),
                now
            )
            .is_err()
        );
    }
    let bars = agg.advance(seconds(60)).unwrap();
    assert_eq!(bars[0].trades, 1);
    let mut huge = trade("1", "1");
    huge.amount = Decimal::MAX;
    agg.push(id, &huge, seconds(61)).unwrap();
    assert!(agg.push(id, &huge, seconds(62)).is_err());
    assert_eq!(agg.finish()[0].trades, 1);
}
#[test]
fn fractional_precision_and_nonterminating_vwap_use_decimal_not_float() {
    let id = identity();
    let mut agg = Ohlcv::new(seconds(60), 1).unwrap();
    agg.push(id, &trade("0.1", "0.2"), seconds(0)).unwrap();
    agg.push(id, &trade("0.2", "0.1"), seconds(1)).unwrap();
    let b = agg.finish().pop().unwrap();
    assert_eq!(b.volume, Decimal::from_str_exact("0.3").unwrap());
    assert_eq!(b.price_volume, Decimal::from_str_exact("0.04").unwrap());
    assert_eq!(b.vwap.to_string(), "0.1333333333333333333333333333");
    let mut agg = Ohlcv::new(seconds(60), 1).unwrap();
    let input = trade("0.1234567890123456789012345678", "1");
    agg.push(id, &input, seconds(0)).unwrap();
    let b = agg.finish().pop().unwrap();
    assert_eq!(b.open, input.price);
    assert_eq!(b.vwap, input.price);
}
#[test]
fn configuration_and_unrepresentable_window_end_fail_explicitly() {
    for (window, count) in [
        (Duration::ZERO, 1),
        (Duration::from_millis(500), 1),
        (seconds(1), 0),
        (seconds(1), 4097),
    ] {
        assert!(Ohlcv::new(window, count).is_err());
    }
    let mut agg = Ohlcv::new(seconds(1), 1).unwrap();
    assert!(agg.advance(Duration::MAX).is_err());
    agg.push(identity(), &trade("1", "1"), seconds(0)).unwrap();
    assert_eq!(agg.finish().len(), 1);
}
#[cfg(feature = "recording")]
#[tokio::test]
async fn normalized_recording_replay_reproduces_all_bar_fields_and_partial_tail() {
    use crate::{
        feed::{FeedEnvelope, FeedEvent},
        recording::*,
    };
    let id = identity();
    let mut live = Ohlcv::new(seconds(5), 4).unwrap();
    let mut expected = Vec::new();
    let mut bytes = Vec::new();
    let mut writer = RecordingWriter::new(&mut bytes, RecordingLimits::default())
        .await
        .unwrap();
    for (now, price) in [(0, "10.1"), (2, "9.2"), (5, "11.3"), (7, "12.4")] {
        let t = trade(price, "0.2");
        expected.extend(live.push(id, &t, seconds(now)).unwrap());
        writer
            .append(
                FeedEnvelope {
                    identity: id,
                    event: FeedEvent::Trade(t),
                },
                seconds(now),
            )
            .await
            .unwrap();
    }
    writer.finish(RecordingEnd::Complete).await.unwrap();
    expected.extend(live.finish());
    let mut replay = Ohlcv::new(seconds(5), 4).unwrap();
    let mut actual = Vec::new();
    let mut reader = RecordingReader::new(bytes.as_slice(), RecordingLimits::default());
    while let Some(row) = reader.next_event().await.unwrap() {
        let FeedEvent::Trade(t) = row.event else {
            panic!("trade")
        };
        actual.extend(
            replay
                .push(row.identity, &t, Duration::from_nanos(row.elapsed_ns))
                .unwrap(),
        );
    }
    actual.extend(replay.finish());
    assert_eq!(actual, expected);
    assert_eq!(actual.len(), 2);
    assert!(actual[0].closed);
    assert!(!actual[1].closed);
}
