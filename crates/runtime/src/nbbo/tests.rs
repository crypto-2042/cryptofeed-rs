use super::*;
use crate::feed::FeedId;
fn at(n: u64) -> Duration {
    Duration::from_secs(n)
}
fn source(exchange: ExchangeId) -> NbboSource {
    NbboSource {
        identity: FeedIdentity {
            id: FeedId::allocate(),
            generation: 1,
        },
        exchange,
        quantity_unit: NbboQuantityUnit::Base,
    }
}
fn book(source: NbboSource, bid: i64, ask: i64) -> L1Book {
    L1Book {
        exchange: source.exchange,
        symbol: Symbol::spot("BTC", "USDT"),
        bid: PriceLevel {
            price: bid.into(),
            amount: 2.into(),
        },
        ask: PriceLevel {
            price: ask.into(),
            amount: 3.into(),
        },
        exchange_ts: 100.5,
        received_ts: 101.5,
    }
}
fn engine(sources: Vec<NbboSource>) -> Nbbo {
    Nbbo::new(Symbol::spot("BTC", "USDT"), sources, at(5)).unwrap()
}
#[test]
fn best_prices_and_selected_sizes_follow_python_rule_and_keep_source_clocks() {
    let a = source(ExchangeId::Binance);
    let b = source(ExchangeId::Okx);
    let mut n = engine(vec![a, b]);
    let q = n
        .update(a.identity, &book(a, 100, 105), at(0), at(0))
        .unwrap()
        .unwrap()
        .quote
        .unwrap();
    assert_eq!(q.bid.source, a);
    assert_eq!(q.ask.source, a);
    let q = n
        .update(b.identity, &book(b, 102, 104), at(1), at(1))
        .unwrap()
        .unwrap()
        .quote
        .unwrap();
    assert_eq!(
        (q.bid.level.price, q.ask.level.price),
        (102.into(), 104.into())
    );
    assert_eq!(q.bid.source, b);
    assert_eq!(q.ask.source, b);
    assert_eq!(q.bid.level.amount, 2.into());
    assert_eq!(q.ask.level.amount, 3.into());
    assert_eq!(
        (q.bid.exchange_ts, q.bid.received_ts, q.bid.observed_at),
        (100.5, 101.5, at(1))
    );
    let q = n
        .update(a.identity, &book(a, 103, 106), at(2), at(2))
        .unwrap()
        .unwrap()
        .quote
        .unwrap();
    assert_eq!(q.bid.source, a);
    assert_eq!(q.ask.source, b);
}
#[test]
fn deduplicate_economic_state_but_emit_selected_size_changes() {
    let a = source(ExchangeId::Binance);
    let b = source(ExchangeId::Okx);
    let mut n = engine(vec![a, b]);
    n.update(a.identity, &book(a, 100, 105), at(0), at(0))
        .unwrap();
    assert!(
        n.update(b.identity, &book(b, 99, 106), at(1), at(1))
            .unwrap()
            .is_none()
    );
    let mut refresh = book(a, 100, 105);
    refresh.exchange_ts = 110.0;
    refresh.received_ts = 111.0;
    assert!(
        n.update(a.identity, &refresh, at(2), at(2))
            .unwrap()
            .is_none()
    );
    refresh.bid.amount = 7.into();
    let q = n
        .update(a.identity, &refresh, at(3), at(3))
        .unwrap()
        .unwrap()
        .quote
        .unwrap();
    assert_eq!(q.bid.level.amount, 7.into());
    assert_eq!(q.bid.exchange_ts, 110.0);
}
#[test]
fn equal_price_ties_use_configured_order_without_summing_sizes() {
    let a = source(ExchangeId::Binance);
    let b = source(ExchangeId::Okx);
    let mut n = engine(vec![a, b]);
    n.update(b.identity, &book(b, 100, 105), at(0), at(0))
        .unwrap();
    let mut input = book(a, 100, 105);
    input.bid.amount = Decimal::ONE;
    let q = n
        .update(a.identity, &input, at(1), at(1))
        .unwrap()
        .unwrap()
        .quote
        .unwrap();
    assert_eq!(q.bid.source, a);
    assert_eq!(q.bid.level.amount, Decimal::ONE);
    assert_eq!(q.ask.source, a);
    assert_eq!(q.ask.level.amount, 3.into());
}
#[test]
fn inclusive_expiry_falls_back_then_withdraws_even_without_more_data() {
    let a = source(ExchangeId::Binance);
    let b = source(ExchangeId::Okx);
    let mut n = engine(vec![a, b]);
    assert!(n.advance(at(0)).unwrap().is_none());
    n.update(a.identity, &book(a, 102, 104), at(0), at(0))
        .unwrap();
    n.update(b.identity, &book(b, 100, 105), at(2), at(2))
        .unwrap();
    assert!(n.advance(at(4)).unwrap().is_none());
    let q = n.advance(at(5)).unwrap().unwrap().quote.unwrap();
    assert_eq!(q.bid.source, b);
    assert!(n.advance(at(7)).unwrap().unwrap().quote.is_none());
    assert!(n.advance(at(8)).unwrap().is_none());
}
#[test]
fn queued_old_observations_cannot_refresh_age_and_errors_do_not_mutate_clock() {
    let a = source(ExchangeId::Binance);
    let mut n = engine(vec![a]);
    n.update(a.identity, &book(a, 100, 105), at(0), at(0))
        .unwrap();
    assert!(
        n.update(a.identity, &book(a, 101, 105), at(0), at(5))
            .is_err()
    );
    assert!(
        n.update(a.identity, &book(a, 101, 105), at(6), at(5))
            .is_err()
    );
    // Invalid future inputs did not advance the consumer clock or replace prices.
    n.update(a.identity, &book(a, 100, 105), at(2), at(2))
        .unwrap();
    assert!(n.advance(at(6)).unwrap().is_none());
    assert!(n.advance(at(7)).unwrap().unwrap().quote.is_none());
}
#[test]
fn suspend_blocks_queued_data_and_resume_requires_new_observations() {
    let a = source(ExchangeId::Binance);
    let mut n = engine(vec![a]);
    n.update(a.identity, &book(a, 100, 105), at(0), at(0))
        .unwrap();
    assert!(
        n.set_available(a.identity, false, at(1))
            .unwrap()
            .unwrap()
            .quote
            .is_none()
    );
    assert!(
        n.update(a.identity, &book(a, 101, 105), at(2), at(2))
            .is_err()
    );
    assert!(n.set_available(a.identity, true, at(3)).unwrap().is_none());
    assert!(
        n.update(a.identity, &book(a, 101, 105), at(2), at(3))
            .is_err()
    );
    assert!(
        n.update(a.identity, &book(a, 101, 105), at(3), at(3))
            .unwrap()
            .is_some()
    );
    // Idempotent Ready notifications keep a usable quote.
    assert!(n.set_available(a.identity, true, at(4)).unwrap().is_none());
}
#[test]
fn generation_replacement_withdraws_old_scope_and_rejects_stale_buffered_input() {
    let a = source(ExchangeId::Binance);
    let b = source(ExchangeId::Okx);
    let mut n = engine(vec![a, b]);
    n.update(a.identity, &book(a, 102, 104), at(0), at(0))
        .unwrap();
    n.update(b.identity, &book(b, 100, 105), at(0), at(0))
        .unwrap();
    let new = NbboSource {
        identity: FeedIdentity {
            generation: 2,
            ..a.identity
        },
        exchange: ExchangeId::Bybit,
        quantity_unit: NbboQuantityUnit::Quote,
    };
    let q = n
        .replace_source(a.identity, new, at(1))
        .unwrap()
        .unwrap()
        .quote
        .unwrap();
    assert_eq!(q.bid.source, b);
    assert!(
        n.update(a.identity, &book(a, 110, 115), at(2), at(2))
            .is_err()
    );
    assert!(n.replace_source(a.identity, new, at(2)).is_err());
    assert!(n.replace_source(new.identity, new, at(2)).is_err());
    let q = n
        .update(new.identity, &book(new, 103, 104), at(2), at(2))
        .unwrap()
        .unwrap()
        .quote
        .unwrap();
    assert_eq!(q.bid.source, new);
}
#[test]
fn exact_symbols_and_declared_native_units_prevent_implicit_contract_size_merging() {
    let mut a = source(ExchangeId::Binance);
    a.quantity_unit = NbboQuantityUnit::Base;
    let mut b = source(ExchangeId::Okx);
    b.quantity_unit = NbboQuantityUnit::Contracts;
    let symbol = Symbol::perpetual("BTC", "USDT");
    let mut n = Nbbo::new(symbol.clone(), vec![a, b], at(5)).unwrap();
    let mut qa = book(a, 103, 106);
    qa.symbol = symbol.clone();
    qa.bid.amount = "0.1234567890123456789012345678".parse().unwrap();
    let mut qb = book(b, 102, 104);
    qb.symbol = symbol;
    qb.ask.amount = 100.into();
    n.update(a.identity, &qa, at(0), at(0)).unwrap();
    let q = n
        .update(b.identity, &qb, at(0), at(0))
        .unwrap()
        .unwrap()
        .quote
        .unwrap();
    assert_eq!(q.bid.source.quantity_unit, NbboQuantityUnit::Base);
    assert_eq!(q.ask.source.quantity_unit, NbboQuantityUnit::Contracts);
    assert_eq!(q.bid.level.amount, qa.bid.amount);
    assert_eq!(q.ask.level.amount, 100.into());
    assert!(
        n.update(a.identity, &book(a, 103, 104), at(1), at(1))
            .is_err()
    );
    let mut wrong = qa;
    wrong.symbol = Symbol::perpetual("BTC", "USD");
    assert!(n.update(a.identity, &wrong, at(1), at(1)).is_err());
}
#[test]
fn intervenue_cross_is_flagged_while_crossed_or_empty_local_snapshots_are_rejected() {
    let a = source(ExchangeId::Binance);
    let b = source(ExchangeId::Okx);
    let mut n = engine(vec![a, b]);
    n.update(a.identity, &book(a, 105, 106), at(0), at(0))
        .unwrap();
    let q = n
        .update(b.identity, &book(b, 102, 103), at(0), at(0))
        .unwrap()
        .unwrap()
        .quote
        .unwrap();
    assert!(q.crossed);
    for mode in 0..7 {
        let mut bad = book(a, 100, 101);
        match mode {
            0 => bad.bid.price = 102.into(),
            1 => bad.bid.amount = Decimal::ZERO,
            2 => bad.ask.amount = Decimal::NEGATIVE_ONE,
            3 => bad.bid.price = Decimal::ZERO,
            4 => bad.ask.price = Decimal::NEGATIVE_ONE,
            5 => bad.exchange_ts = f64::NAN,
            6 => bad.received_ts = f64::INFINITY,
            _ => unreachable!(),
        };
        assert!(n.update(a.identity, &bad, at(1), at(1)).is_err());
    }
    let q = n
        .update(a.identity, &book(a, 103, 103), at(1), at(1))
        .unwrap()
        .unwrap()
        .quote
        .unwrap();
    assert!(!q.crossed); // Locked local quote is valid.
}
#[test]
fn wrong_exchange_generation_and_backwards_clocks_fail_atomically() {
    let a = source(ExchangeId::Binance);
    let mut n = engine(vec![a]);
    n.update(a.identity, &book(a, 100, 101), at(2), at(2))
        .unwrap();
    let other = source(ExchangeId::Okx);
    assert!(
        n.update(other.identity, &book(other, 100, 101), at(3), at(3))
            .is_err()
    );
    assert!(
        n.update(a.identity, &book(other, 100, 101), at(3), at(3))
            .is_err()
    );
    assert!(
        n.update(a.identity, &book(a, 100, 101), at(1), at(3))
            .is_err()
    );
    assert!(n.advance(at(1)).is_err());
    assert!(n.set_available(a.identity, false, at(1)).is_err());
    assert!(
        n.update(a.identity, &book(a, 100, 101), at(3), at(3))
            .unwrap()
            .is_none()
    );
}
#[test]
fn constructor_bounds_and_product_scope_are_explicit() {
    let a = source(ExchangeId::Binance);
    let symbol = Symbol::spot("BTC", "USDT");
    assert!(Nbbo::new(symbol.clone(), vec![], at(1)).is_err());
    assert!(Nbbo::new(symbol.clone(), vec![a], Duration::ZERO).is_err());
    assert!(Nbbo::new(symbol.clone(), vec![a, a], at(1)).is_err());
    let newer = NbboSource {
        identity: FeedIdentity {
            generation: 2,
            ..a.identity
        },
        ..a
    };
    assert!(Nbbo::new(symbol.clone(), vec![a, newer], at(1)).is_err());
    let zero = NbboSource {
        identity: FeedIdentity {
            generation: 0,
            ..a.identity
        },
        ..a
    };
    assert!(Nbbo::new(symbol.clone(), vec![zero], at(1)).is_err());
    let many = (0..65).map(|_| source(ExchangeId::Okx)).collect();
    assert!(Nbbo::new(symbol, many, at(1)).is_err());
    assert!(Nbbo::new(Symbol::from_input("unknown"), vec![a], at(1)).is_err());
    assert!(Nbbo::new(Symbol::futures("BTC", "USDT", "261225"), vec![a], at(1)).is_ok());
}
#[cfg(feature = "recording")]
#[tokio::test]
async fn normalized_l1_replay_reproduces_every_nbbo_change_and_expiry() {
    use crate::{
        feed::{FeedEnvelope, FeedEvent},
        recording::*,
    };
    let a = source(ExchangeId::Binance);
    let b = source(ExchangeId::Okx);
    let mut live = engine(vec![a, b]);
    let mut expected = Vec::new();
    let mut bytes = Vec::new();
    let mut writer = RecordingWriter::new(&mut bytes, RecordingLimits::default())
        .await
        .unwrap();
    for (now, source, bid, ask) in [
        (0, a, 100, 105),
        (1, b, 102, 104),
        (2, a, 103, 106),
        (3, b, 101, 104),
    ] {
        let input = book(source, bid, ask);
        expected.extend(
            live.update(source.identity, &input, at(now), at(now))
                .unwrap(),
        );
        writer
            .append(
                FeedEnvelope {
                    identity: source.identity,
                    event: FeedEvent::L1Book(input),
                },
                at(now),
            )
            .await
            .unwrap();
    }
    writer.finish(RecordingEnd::Complete).await.unwrap();
    expected.extend(live.advance(at(9)).unwrap());
    let mut replay = engine(vec![a, b]);
    let mut actual = Vec::new();
    let mut reader = RecordingReader::new(bytes.as_slice(), RecordingLimits::default());
    while let Some(row) = reader.next_event().await.unwrap() {
        let FeedEvent::L1Book(input) = row.event else {
            panic!("L1")
        };
        let now = Duration::from_nanos(row.elapsed_ns);
        actual.extend(replay.update(row.identity, &input, now, now).unwrap());
    }
    actual.extend(replay.advance(at(9)).unwrap());
    assert_eq!(actual, expected);
    assert!(actual.last().unwrap().quote.is_none());
    assert_eq!(actual.len(), 4);
}
