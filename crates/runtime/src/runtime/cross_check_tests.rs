use super::*;
use crate::{
    exchange::ExchangeFeedBuilder,
    feed::{FeedIdentity, FeedState},
};
use cryptofeed_core::symbol::Symbol;
use cryptofeed_orderbook::{L2BookDelta, L2BookSnapshot, OrderBookHandler, PriceLevel};
use rust_decimal::Decimal;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

fn level(price: &str, amount: &str) -> PriceLevel {
    PriceLevel {
        price: Decimal::from_str_exact(price).unwrap(),
        amount: Decimal::from_str_exact(amount).unwrap(),
    }
}
fn book(
    exchange: ExchangeId,
    snapshot: bool,
    symbol: &str,
    bids: Vec<PriceLevel>,
    asks: Vec<PriceLevel>,
) -> L2Book {
    let symbol = symbol.parse::<Symbol>().unwrap();
    if snapshot {
        L2Book::Snapshot(L2BookSnapshot {
            exchange,
            symbol,
            bids,
            asks,
            exchange_ts: 1.0,
            received_ts: 2.0,
        })
    } else {
        L2Book::Delta(L2BookDelta {
            exchange,
            symbol,
            bids,
            asks,
            exchange_ts: 1.0,
            received_ts: 2.0,
        })
    }
}
fn good(exchange: ExchangeId, symbol: &str) -> L2Book {
    book(
        exchange,
        true,
        symbol,
        vec![level("99", "1")],
        vec![level("101", "1")],
    )
}
fn crossed(exchange: ExchangeId) -> L2Book {
    book(
        exchange,
        true,
        "BTC-USDT",
        vec![level("102", "1")],
        vec![level("101", "1")],
    )
}
fn feed(exchange: ExchangeId, enabled: bool) -> ExchangeFeed {
    ExchangeFeedBuilder::new(exchange)
        .l2_book()
        .symbol("BTC-USDT")
        .l2_book_cross_check(enabled)
        .build()
}

#[tokio::test]
async fn l2_cross_check_default_preserves_crossed_books_and_policy_survives_planning() {
    let default = ExchangeFeedBuilder::new(ExchangeId::Okx)
        .l2_book()
        .symbol("BTC-USDT")
        .build();
    assert!(!default.l2_book_cross_check);
    dispatch_l2_book(&default, crossed(ExchangeId::Okx))
        .await
        .unwrap();
    assert!(default.orderbook_states.lock().unwrap()["BTC-USDT"].is_crossed());
    let strict = ExchangeFeedBuilder::new(ExchangeId::Okx)
        .subscription(Channel::L2Book, ["BTC-USDT", "ETH-USDT"])
        .l2_book_cross_check(true)
        .build();
    assert!(
        strict
            .connection_feeds()
            .unwrap()
            .iter()
            .all(|feed| feed.l2_book_cross_check)
    );
}

#[tokio::test]
async fn l2_cross_check_all_exchange_dispatchers_propagate_recoverable_failure() {
    for exchange in [
        ExchangeId::Binance,
        ExchangeId::Bitget,
        ExchangeId::Bybit,
        ExchangeId::Okx,
        ExchangeId::Gateio,
    ] {
        let feed = feed(exchange, true);
        let book = crossed(exchange);
        let result = match exchange {
            ExchangeId::Binance => dispatch_binance_event(&feed, BinanceEvent::L2Book(book)).await,
            ExchangeId::Bitget => dispatch_bitget_event(&feed, BitgetEvent::L2Book(book)).await,
            ExchangeId::Bybit => dispatch_bybit_event(&feed, BybitEvent::L2Book(book)).await,
            ExchangeId::Okx => dispatch_okx_event(&feed, OkxEvent::L2Book(book)).await,
            ExchangeId::Gateio => dispatch_gateio_event(&feed, GateioEvent::L2Book(book)).await,
            _ => unreachable!(),
        };
        let error = result.unwrap_err();
        assert!(
            matches!(&error, Error::MalformedData(message) if message.contains("crossed L2 book"))
        );
        assert!(!supervisor::is_permanent(&error));
        assert!(feed.orderbook_states.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn l2_cross_check_uses_full_delta_state_and_accepts_empty_sides_and_fresh_snapshots() {
    let feed = feed(ExchangeId::Okx, true);
    dispatch_l2_book(&feed, good(ExchangeId::Okx, "BTC-USDT"))
        .await
        .unwrap();
    let delta = book(
        ExchangeId::Okx,
        false,
        "BTC-USDT",
        vec![level("101", "1")],
        vec![],
    );
    assert!(dispatch_l2_book(&feed, delta).await.is_err()); // Equality against retained ask.
    assert!(feed.orderbook_states.lock().unwrap().is_empty());
    dispatch_l2_book(&feed, good(ExchangeId::Okx, "BTC-USDT"))
        .await
        .unwrap();
    let remove_ask = book(
        ExchangeId::Okx,
        false,
        "BTC-USDT",
        vec![level("102", "1")],
        vec![level("101", "0")],
    );
    dispatch_l2_book(&feed, remove_ask).await.unwrap();
    assert!(
        feed.orderbook_states.lock().unwrap()["BTC-USDT"]
            .asks()
            .is_empty()
    );
    dispatch_l2_book(&feed, good(ExchangeId::Okx, "BTC-USDT"))
        .await
        .unwrap();
    assert!(!feed.orderbook_states.lock().unwrap()["BTC-USDT"].is_crossed());
}

#[tokio::test]
async fn l2_cross_check_filters_unsubscribed_pairs_before_inspection() {
    let feed = feed(ExchangeId::Okx, true);
    let bad = book(
        ExchangeId::Okx,
        true,
        "ETH-USDT",
        vec![level("102", "1")],
        vec![level("101", "1")],
    );
    dispatch_l2_book(&feed, bad).await.unwrap();
    assert!(feed.orderbook_states.lock().unwrap().is_empty());
    dispatch_l2_book(&feed, good(ExchangeId::Okx, "BTC-USDT"))
        .await
        .unwrap();
}

struct Seen(Arc<AtomicUsize>);
#[async_trait::async_trait]
impl OrderBookHandler for Seen {
    async fn on_l2_book(&self, _: L2Book) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn l2_cross_check_rejects_before_every_publication_and_withdraws_recovery_and_readiness() {
    let seen = Arc::new(AtomicUsize::new(0));
    let mut handler = FeedHandler::new();
    let ordinary = &mut handler.subscribe();
    let identified = &mut handler.subscribe_identified();
    let books = handler.l2_book_handle();
    let id = handler.add_feed_with_id(
        ExchangeFeedBuilder::new(ExchangeId::Okx)
            .l2_book()
            .symbol("BTC-USDT")
            .l2_book_cross_check(true)
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
    dispatch_l2_book(&feed, good(ExchangeId::Okx, "BTC-USDT"))
        .await
        .unwrap();
    ordinary.try_recv().unwrap();
    identified.try_recv().unwrap();
    assert!(books.recover(identity, &symbol).snapshot.is_some());
    assert_eq!(monitor.snapshot().books_synchronized, 1);
    assert!(
        dispatch_l2_book(&feed, crossed(ExchangeId::Okx))
            .await
            .is_err()
    );
    assert_eq!(seen.load(Ordering::SeqCst), 1);
    assert_eq!(counters.count(Channel::L2Book), 1);
    assert!(matches!(
        ordinary.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        identified.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    assert!(books.recover(identity, &symbol).snapshot.is_none());
    assert_eq!(monitor.snapshot().books_synchronized, 0);
    drop(attempt);
    let attempt = readiness::begin(&mut feed).unwrap();
    attempt.connected();
    dispatch_l2_book(&feed, good(ExchangeId::Okx, "BTC-USDT"))
        .await
        .unwrap();
    assert_eq!(
        books
            .recover(identity, &symbol)
            .snapshot
            .unwrap()
            .anchor
            .epoch,
        2
    );
}

#[tokio::test]
async fn l2_cross_check_failure_keeps_other_symbols_and_independent_feeds_usable() {
    let strict = ExchangeFeedBuilder::new(ExchangeId::Bybit)
        .l2_book()
        .symbols(["BTC-USDT", "ETH-USDT"])
        .l2_book_cross_check(true)
        .build();
    let other = feed(ExchangeId::Okx, true);
    dispatch_l2_book(&strict, good(ExchangeId::Bybit, "ETH-USDT"))
        .await
        .unwrap();
    dispatch_l2_book(&other, good(ExchangeId::Okx, "BTC-USDT"))
        .await
        .unwrap();
    let symbol = Symbol::spot("BTC", "USDT");
    strict.bybit_book_syncs.lock().unwrap().insert(
        symbol.to_string(),
        crate::exchange::bybit::book_sync::BybitBookSync::new(symbol),
    );
    assert!(
        dispatch_l2_book(&strict, crossed(ExchangeId::Bybit))
            .await
            .is_err()
    );
    assert!(strict.bybit_book_syncs.lock().unwrap().is_empty());
    assert_eq!(strict.orderbook_states.lock().unwrap().len(), 1);
    assert!(!strict.orderbook_states.lock().unwrap()["ETH-USDT"].is_crossed());
    assert!(!other.orderbook_states.lock().unwrap()["BTC-USDT"].is_crossed());
    dispatch_l2_book(&other, good(ExchangeId::Okx, "BTC-USDT"))
        .await
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn l2_cross_check_retries_with_fresh_book_state_under_existing_supervisor_budget() {
    let feed = feed(ExchangeId::Okx, true);
    let attempts = AtomicUsize::new(0);
    supervisor::retry_with_backoff(1, supervisor::Backoff::new(1, 8), || async {
        let first = attempts.fetch_add(1, Ordering::SeqCst) == 0;
        assert!(feed.orderbook_states.lock().unwrap().is_empty());
        dispatch_l2_book(
            &feed,
            if first {
                crossed(ExchangeId::Okx)
            } else {
                good(ExchangeId::Okx, "BTC-USDT")
            },
        )
        .await
    })
    .await
    .unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert!(!feed.orderbook_states.lock().unwrap()["BTC-USDT"].is_crossed());
}
