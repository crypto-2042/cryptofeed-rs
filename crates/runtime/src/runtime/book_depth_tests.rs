use super::*;
use crate::{
    exchange::ExchangeFeedBuilder,
    feed::{FeedIdentity, FeedState},
};
use cryptofeed_core::symbol::Symbol;
use cryptofeed_orderbook::{
    L2BookDelta, L2BookSnapshot, L2BookState, OrderBookHandler, PriceLevel,
};
use rust_decimal::Decimal;
use std::sync::{Arc, Mutex};

fn level(price: &str, amount: &str) -> PriceLevel {
    PriceLevel {
        price: Decimal::from_str_exact(price).unwrap(),
        amount: Decimal::from_str_exact(amount).unwrap(),
    }
}
fn event(snapshot: bool, bids: Vec<PriceLevel>, asks: Vec<PriceLevel>) -> L2Book {
    let symbol = Symbol::spot("BTC", "USDT");
    if snapshot {
        L2Book::Snapshot(L2BookSnapshot {
            exchange: ExchangeId::Okx,
            symbol,
            bids,
            asks,
            exchange_ts: 1.25,
            received_ts: 2.5,
        })
    } else {
        L2Book::Delta(L2BookDelta {
            exchange: ExchangeId::Okx,
            symbol,
            bids,
            asks,
            exchange_ts: 1.5,
            received_ts: 3.0,
        })
    }
}
fn snapshot() -> L2Book {
    event(
        true,
        vec![level("100", "1"), level("99", "2"), level("98", "3")],
        vec![level("101", "1"), level("102", "2"), level("103", "3")],
    )
}
fn feed(maximum: usize) -> ExchangeFeed {
    ExchangeFeedBuilder::new(ExchangeId::Okx)
        .l2_book()
        .symbol("BTC-USDT")
        .l2_book_max_depth(maximum)
        .build()
}

#[test]
fn l2_delivery_depth_defaults_zero_and_sparse_planning_preserve_native_configuration() {
    let unlimited = feed(0);
    assert!(unlimited.l2_book_max_depth.is_none());
    let input = snapshot();
    assert_eq!(apply_orderbook_state(&unlimited, &input).unwrap(), input);
    assert!(unlimited.book_projections.lock().unwrap().is_empty());
    let limited = ExchangeFeedBuilder::new(ExchangeId::Bybit)
        .subscription(Channel::L2Book, ["BTC-USDT", "ETH-USDT"])
        .l2_book_depth(50)
        .l2_book_max_depth(1)
        .build();
    for group in limited.connection_feeds().unwrap() {
        assert_eq!(group.l2_book_depth, Some(50));
        assert_eq!(group.l2_book_max_depth.unwrap().get(), 1);
    }
}

#[test]
fn l2_delivery_depth_removes_displaced_levels_and_promotes_unchanged_deeper_levels() {
    let feed = feed(2);
    let first = apply_orderbook_state(&feed, &snapshot()).unwrap();
    assert_eq!(
        first
            .bids()
            .iter()
            .map(|level| level.price)
            .collect::<Vec<_>>(),
        [Decimal::from(100), Decimal::from(99)]
    );
    assert_eq!(
        feed.orderbook_states.lock().unwrap()["BTC-USDT"]
            .bids()
            .len(),
        3
    );
    let update = event(false, vec![level("100", "0")], vec![level("100.50", "4")]);
    let projected = apply_orderbook_state(&feed, &update).unwrap();
    assert!(matches!(&projected, L2Book::Delta(_)));
    assert_eq!(projected.bids(), [level("100", "0"), level("98", "3")]);
    assert_eq!(projected.asks(), [level("100.50", "4"), level("102", "0")]);
    assert_eq!(projected.exchange_ts(), 1.5);
    assert_eq!(projected.received_ts(), 3.0);
    let mut consumer = L2BookState::new(Symbol::spot("BTC", "USDT"));
    consumer.apply(first);
    consumer.apply(projected);
    assert_eq!(consumer.bids(), [level("99", "2"), level("98", "3")]);
    assert_eq!(consumer.asks(), [level("100.50", "4"), level("101", "1")]);
}

#[test]
fn l2_delivery_depth_delta_oracle_matches_full_book_top_levels_after_every_update() {
    for maximum in [1, 2, 3, 8] {
        let feed = feed(maximum);
        let mut full = L2BookState::new(Symbol::spot("BTC", "USDT"));
        let mut consumer = full.clone();
        full.apply(snapshot());
        consumer.apply(apply_orderbook_state(&feed, &snapshot()).unwrap());
        for bid in [true, false] {
            for price in ["97", "98", "99", "100", "100.50", "101", "102", "104"] {
                for amount in ["0", "1.0", "2.00"] {
                    let levels = vec![level(price, amount)];
                    let update = if bid {
                        event(false, levels, vec![])
                    } else {
                        event(false, vec![], levels)
                    };
                    full.apply(update.clone());
                    let delivered = apply_orderbook_state(&feed, &update).unwrap();
                    consumer.apply(delivered);
                    let expected_bids: Vec<_> = full.bids().into_iter().take(maximum).collect();
                    let expected_asks: Vec<_> = full.asks().into_iter().take(maximum).collect();
                    assert_eq!(consumer.bids(), expected_bids);
                    assert_eq!(consumer.asks(), expected_asks);
                    for (actual, expected) in consumer
                        .bids()
                        .iter()
                        .zip(&expected_bids)
                        .chain(consumer.asks().iter().zip(&expected_asks))
                    {
                        assert_eq!(actual.amount.scale(), expected.amount.scale());
                    }
                }
            }
        }
    }
}

#[test]
fn l2_delivery_depth_deep_only_updates_empty_sides_and_decimal_scale_remain_correct() {
    let feed = feed(1);
    apply_orderbook_state(&feed, &snapshot()).unwrap();
    let deep = apply_orderbook_state(&feed, &event(false, vec![level("98", "9")], vec![])).unwrap();
    assert!(deep.bids().is_empty() && deep.asks().is_empty());
    assert!(matches!(deep, L2Book::Delta(_)));
    let scale =
        apply_orderbook_state(&feed, &event(false, vec![level("100", "1.00")], vec![])).unwrap();
    assert_eq!(scale.bids()[0].amount.scale(), 2);
    let emptied = apply_orderbook_state(
        &feed,
        &event(
            false,
            vec![level("100", "0"), level("99", "0"), level("98", "0")],
            vec![],
        ),
    )
    .unwrap();
    assert_eq!(emptied.bids(), [level("100", "0")]);
    assert!(
        feed.orderbook_states.lock().unwrap()["BTC-USDT"]
            .bids()
            .is_empty()
    );
}

#[test]
fn l2_delivery_depth_requires_new_snapshot_after_invalidation_and_fresh_state() {
    let feed = feed(1);
    apply_orderbook_state(&feed, &snapshot()).unwrap();
    feed.invalidate_book_readiness("BTC-USDT");
    assert!(apply_orderbook_state(&feed, &event(false, vec![level("100", "2")], vec![])).is_err());
    assert!(feed.book_projections.lock().unwrap().is_empty());
    assert!(feed.orderbook_states.lock().unwrap().is_empty());
    apply_orderbook_state(&feed, &snapshot()).unwrap();
    let mut replacement = feed.clone();
    replacement.fresh_runtime_state();
    assert!(replacement.book_projections.lock().unwrap().is_empty());
    assert_eq!(feed.book_projections.lock().unwrap().len(), 1);
    feed.clear_connection_books(&[Symbol::spot("BTC", "USDT")]);
    assert!(feed.book_projections.lock().unwrap().is_empty());
}

struct Seen(Arc<Mutex<Vec<L2Book>>>);
#[async_trait::async_trait]
impl OrderBookHandler for Seen {
    async fn on_l2_book(&self, book: L2Book) {
        self.0.lock().unwrap().push(book);
    }
}

#[tokio::test]
async fn l2_delivery_depth_all_surfaces_and_recovery_use_the_same_projected_book() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut handler = FeedHandler::new();
    let mut ordinary = handler.subscribe();
    let mut identified = handler.subscribe_identified();
    let books = handler.l2_book_handle();
    let id = handler.add_feed_with_id(
        ExchangeFeedBuilder::new(ExchangeId::Okx)
            .l2_book()
            .symbol("BTC-USDT")
            .l2_book_max_depth(1)
            .orderbook_handler(Arc::new(Seen(seen.clone())))
            .build(),
    );
    let counters = handler.event_counters();
    let (mut feeds, context, _commands) = handler.into_control_parts();
    let mut feed = feeds.pop().unwrap();
    let identity = FeedIdentity { id, generation: 1 };
    let monitor = context.monitor(identity, ExchangeId::Okx);
    monitor.configure(1, vec!["BTC-USDT".into()]);
    monitor.lifecycle(FeedState::Started);
    feed.monitor = Some(monitor.clone());
    readiness::track(
        &mut feed,
        "wss://ws.okx.com/ws/v5/public",
        vec!["BTC-USDT".into()],
    );
    let attempt = readiness::begin(&mut feed).unwrap();
    attempt.connected();
    let symbol = Symbol::spot("BTC", "USDT");
    for input in [
        snapshot(),
        event(false, vec![level("100", "0")], vec![level("100.5", "4")]),
    ] {
        dispatch_l2_book(&feed, input).await.unwrap();
        let crate::feed::FeedEvent::L2Book(normal) = ordinary.try_recv().unwrap() else {
            panic!("L2 event");
        };
        let crate::feed::FeedEvent::L2Book(tagged) = identified.try_recv().unwrap().event else {
            panic!("L2 event");
        };
        assert_eq!(normal, tagged);
        assert_eq!(seen.lock().unwrap().last(), Some(&normal));
        let recovered = books.recover(identity, &symbol).snapshot.unwrap();
        assert_eq!(recovered.book.bids.len(), 1);
        assert_eq!(recovered.book.asks.len(), 1);
    }
    assert_eq!(counters.count(Channel::L2Book), 2);
    assert_eq!(
        books.recover(identity, &symbol).snapshot.unwrap().book.bids,
        [level("99", "2")]
    );
    assert_eq!(
        feed.orderbook_states.lock().unwrap()["BTC-USDT"]
            .bids()
            .len(),
        2
    );
    drop(attempt);
    assert!(books.recover(identity, &symbol).snapshot.is_none());
}

#[test]
fn l2_delivery_depth_cross_check_failure_clears_projection_before_a_new_snapshot() {
    let strict = ExchangeFeedBuilder::new(ExchangeId::Okx)
        .l2_book()
        .symbol("BTC-USDT")
        .l2_book_max_depth(1)
        .l2_book_cross_check(true)
        .build();
    apply_orderbook_state(&strict, &snapshot()).unwrap();
    assert!(
        apply_orderbook_state(&strict, &event(false, vec![level("101", "1")], vec![])).is_err()
    );
    assert!(strict.book_projections.lock().unwrap().is_empty());
    assert!(strict.orderbook_states.lock().unwrap().is_empty());
    assert!(matches!(
        apply_orderbook_state(&strict, &snapshot()).unwrap(),
        L2Book::Snapshot(_)
    ));
}

#[tokio::test]
async fn l2_delivery_depth_exact_pairs_and_connection_resets_keep_other_projections() {
    let sparse = ExchangeFeedBuilder::new(ExchangeId::Okx)
        .subscription(Channel::L2Book, ["BTC-USDT"])
        .subscription(Channel::L1Book, ["ETH-USDT"])
        .l2_book_max_depth(1)
        .build();
    let mut eth = snapshot();
    let L2Book::Snapshot(value) = &mut eth else {
        unreachable!()
    };
    value.symbol = Symbol::spot("ETH", "USDT");
    dispatch_l2_book(&sparse, eth.clone()).await.unwrap();
    assert!(sparse.book_projections.lock().unwrap().is_empty());
    let shared = ExchangeFeedBuilder::new(ExchangeId::Okx)
        .l2_book()
        .symbols(["BTC-USDT", "ETH-USDT"])
        .l2_book_max_depth(1)
        .build();
    dispatch_l2_book(&shared, snapshot()).await.unwrap();
    dispatch_l2_book(&shared, eth).await.unwrap();
    shared.clear_connection_books(&[Symbol::spot("BTC", "USDT")]);
    assert_eq!(shared.book_projections.lock().unwrap().len(), 1);
    assert!(
        shared
            .book_projections
            .lock()
            .unwrap()
            .contains_key("ETH-USDT")
    );
    assert!(matches!(
        apply_orderbook_state(&shared, &snapshot()).unwrap(),
        L2Book::Snapshot(_)
    ));
}
