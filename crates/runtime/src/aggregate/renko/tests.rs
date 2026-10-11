use super::*;
use crate::feed::FeedId;
use cryptofeed_trade::Side;
fn identity() -> FeedIdentity {
    FeedIdentity {
        id: FeedId::allocate(),
        generation: 1,
    }
}
fn trade(price: &str) -> Trade {
    Trade {
        exchange: ExchangeId::Okx,
        symbol: Symbol::spot("BTC", "USDT"),
        price: price.parse().unwrap(),
        amount: Decimal::ONE,
        side: Side::Buy,
        exchange_ts: 10.5,
        received_ts: 11.5,
        id: Some("id".into()),
        implied_volatility: None,
    }
}
#[test]
fn continuation_and_reversal_match_python_price_thresholds_immediately() {
    let id = identity();
    let mut renko = RenkoFixed::new(10.into(), 1).unwrap();
    let mut bricks = Vec::new();
    for price in [
        "100", "111", "120", "121", "115", "101", "100", "91", "101", "111",
    ] {
        if let Some(brick) = renko.push(id, &trade(price)).unwrap() {
            bricks.push(brick);
        }
    }
    let actual: Vec<_> = bricks
        .iter()
        .map(|b| (b.open, b.close, b.direction))
        .collect();
    assert_eq!(
        actual,
        vec![
            (100.into(), 111.into(), RenkoDirection::Up),
            (111.into(), 121.into(), RenkoDirection::Up),
            (111.into(), 101.into(), RenkoDirection::Down),
            (101.into(), 91.into(), RenkoDirection::Down),
            (101.into(), 111.into(), RenkoDirection::Up),
        ]
    );
    assert!(
        bricks
            .iter()
            .all(|b| b.identity == id && b.exchange_ts == 10.5 && b.received_ts == 11.5)
    );
}
#[test]
fn first_downward_brick_and_exact_threshold_are_supported() {
    let id = identity();
    let mut renko = RenkoFixed::new(10.into(), 1).unwrap();
    assert!(renko.push(id, &trade("100")).unwrap().is_none());
    assert!(renko.push(id, &trade("91")).unwrap().is_none());
    let b = renko.push(id, &trade("90")).unwrap().unwrap();
    assert_eq!(
        (b.open, b.close, b.direction),
        (100.into(), 90.into(), RenkoDirection::Down)
    );
    let b = renko.push(id, &trade("80")).unwrap().unwrap();
    assert_eq!((b.open, b.close), (90.into(), 80.into()));
    assert!(renko.push(id, &trade("99")).unwrap().is_none());
    let b = renko.push(id, &trade("100")).unwrap().unwrap();
    assert_eq!(
        (b.open, b.close, b.direction),
        (90.into(), 100.into(), RenkoDirection::Up)
    );
}
#[test]
fn price_gaps_emit_one_actual_price_brick_and_reversal_uses_previous_open() {
    let id = identity();
    let mut renko = RenkoFixed::new(10.into(), 1).unwrap();
    renko.push(id, &trade("100")).unwrap();
    let b = renko.push(id, &trade("135")).unwrap().unwrap();
    assert_eq!((b.open, b.close), (100.into(), 135.into()));
    assert!(renko.push(id, &trade("100")).unwrap().is_none());
    let b = renko.push(id, &trade("90")).unwrap().unwrap();
    assert_eq!((b.open, b.close), (100.into(), 90.into()));
}
#[test]
fn sources_generations_exchanges_and_symbols_have_independent_anchors() {
    let id = identity();
    let other = identity();
    let newer = FeedIdentity {
        generation: 2,
        ..id
    };
    let mut renko = RenkoFixed::new(10.into(), 6).unwrap();
    for source in [id, other, newer] {
        assert!(renko.push(source, &trade("100")).unwrap().is_none());
    }
    let mut input = trade("200");
    input.exchange = ExchangeId::Bybit;
    assert!(renko.push(id, &input).unwrap().is_none());
    input.symbol = Symbol::perpetual("BTC", "USDT");
    assert!(renko.push(id, &input).unwrap().is_none());
    input.symbol = Symbol::spot("ETH", "USDT");
    assert!(renko.push(id, &input).unwrap().is_none());
    for source in [id, other, newer] {
        let b = renko.push(source, &trade("110")).unwrap().unwrap();
        assert_eq!(b.identity, source);
        assert_eq!(b.open, 100.into());
    }
    input.price = 210.into();
    let b = renko.push(id, &input).unwrap().unwrap();
    assert_eq!(b.symbol, Symbol::spot("ETH", "USDT"));
    assert_eq!(b.open, 200.into());
}
#[test]
fn capacity_rejection_and_explicit_retirement_do_not_corrupt_other_state() {
    let id = identity();
    let other = identity();
    let mut renko = RenkoFixed::new(10.into(), 1).unwrap();
    renko.push(id, &trade("100")).unwrap();
    assert!(renko.push(other, &trade("200")).is_err());
    assert!(!renko.remove(other, ExchangeId::Okx, &Symbol::spot("BTC", "USDT")));
    assert_eq!(
        renko.push(id, &trade("110")).unwrap().unwrap().open,
        100.into()
    );
    assert!(renko.remove(id, ExchangeId::Okx, &Symbol::spot("BTC", "USDT")));
    assert!(renko.push(other, &trade("200")).unwrap().is_none());
    assert_eq!(
        renko.push(other, &trade("210")).unwrap().unwrap().open,
        200.into()
    );
    assert!(renko.remove(other, ExchangeId::Okx, &Symbol::spot("BTC", "USDT")));
    assert!(renko.push(other, &trade("300")).unwrap().is_none());
}
#[test]
fn invalid_inputs_are_atomic_and_wire_clocks_do_not_reorder_prices() {
    let id = identity();
    let mut renko = RenkoFixed::new(10.into(), 1).unwrap();
    renko.push(id, &trade("100")).unwrap();
    for mode in 0..5 {
        let mut bad = trade("200");
        let mut source = id;
        match mode {
            0 => bad.price = Decimal::ZERO,
            1 => bad.price = Decimal::NEGATIVE_ONE,
            2 => bad.exchange_ts = f64::NAN,
            3 => bad.received_ts = f64::INFINITY,
            4 => source.generation = 0,
            _ => unreachable!(),
        }
        assert!(renko.push(source, &bad).is_err());
    }
    let mut late = trade("110");
    late.exchange_ts = 1.5;
    late.received_ts = 2.5;
    let b = renko.push(id, &late).unwrap().unwrap();
    assert_eq!(b.open, 100.into());
    assert_eq!(b.exchange_ts, 1.5);
    assert_eq!(b.received_ts, 2.5);
}
#[test]
fn decimal_thresholds_preserve_small_price_increments_without_floats() {
    let id = identity();
    let size = "0.0000000000000000000000000001".parse().unwrap();
    let mut renko = RenkoFixed::new(size, 1).unwrap();
    renko
        .push(id, &trade("0.1234567890123456789012345678"))
        .unwrap();
    let b = renko
        .push(id, &trade("0.1234567890123456789012345679"))
        .unwrap()
        .unwrap();
    assert_eq!(b.open.to_string(), "0.1234567890123456789012345678");
    assert_eq!(b.close.to_string(), "0.1234567890123456789012345679");
    assert_eq!(b.direction, RenkoDirection::Up);
    for (size, count) in [
        (Decimal::ZERO, 1),
        (Decimal::NEGATIVE_ONE, 1),
        (Decimal::ONE, 0),
        (Decimal::ONE, 4097),
    ] {
        assert!(RenkoFixed::new(size, count).is_err());
    }
}
#[cfg(feature = "recording")]
#[tokio::test]
async fn replay_preserves_all_brick_fields_and_throttle_decisions() {
    use crate::{
        aggregate::Throttle,
        feed::{FeedEnvelope, FeedEvent},
        recording::*,
    };
    use std::time::Duration;
    let id = identity();
    let mut live = RenkoFixed::new(10.into(), 2).unwrap();
    let mut throttle = Throttle::new(Duration::from_secs(1)).unwrap();
    let mut expected = Vec::new();
    let mut decisions = Vec::new();
    let mut bytes = Vec::new();
    let mut writer = RecordingWriter::new(&mut bytes, RecordingLimits::default())
        .await
        .unwrap();
    for (n, price) in ["100", "110", "105", "90", "80", "100"]
        .into_iter()
        .enumerate()
    {
        let t = trade(price);
        let now = Duration::from_secs(n as u64);
        expected.extend(live.push(id, &t).unwrap());
        decisions.push(throttle.allow(now).unwrap());
        writer
            .append(
                FeedEnvelope {
                    identity: id,
                    event: FeedEvent::Trade(t),
                },
                now,
            )
            .await
            .unwrap();
    }
    writer.finish(RecordingEnd::Complete).await.unwrap();
    let mut reader = RecordingReader::new(bytes.as_slice(), RecordingLimits::default());
    let mut replay = RenkoFixed::new(10.into(), 2).unwrap();
    let mut throttle = Throttle::new(Duration::from_secs(1)).unwrap();
    let mut actual = Vec::new();
    let mut replay_decisions = Vec::new();
    while let Some(row) = reader.next_event().await.unwrap() {
        let FeedEvent::Trade(t) = row.event else {
            panic!("trade")
        };
        actual.extend(replay.push(row.identity, &t).unwrap());
        replay_decisions.push(
            throttle
                .allow(Duration::from_nanos(row.elapsed_ns))
                .unwrap(),
        );
    }
    assert_eq!(actual, expected);
    assert_eq!(replay_decisions, decisions);
    assert_eq!(actual.len(), 4);
    assert_eq!(decisions, vec![true, false, true, false, true, false]);
}
