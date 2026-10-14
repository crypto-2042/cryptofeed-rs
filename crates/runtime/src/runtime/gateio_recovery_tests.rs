use super::*;
use crate::exchange::gateio::book_sync::{GateioBookSnapshot, GateioDepthDelta};
use crate::{
    exchange::gateio::Gateio,
    feed::{FeedHandler, FeedId, FeedIdentity, FeedState},
};
use cryptofeed_core::symbol::Symbol;
use cryptofeed_orderbook::{L2BookDelta, L2BookSnapshot, PriceLevel};
use std::sync::Arc;

fn snapshot(id: Option<u64>) -> GateioBookSnapshot {
    GateioBookSnapshot {
        last_update_id: id,
        generated_ts: 2.0,
        update_ts: 2.0,
        book: L2BookSnapshot {
            exchange: ExchangeId::Gateio,
            symbol: Symbol::perpetual("BTC", "USDT"),
            bids: vec![PriceLevel {
                price: 100.into(),
                amount: 2.into(),
            }],
            asks: vec![PriceLevel {
                price: 101.into(),
                amount: 1.into(),
            }],
            exchange_ts: 2.0,
            received_ts: 2.1,
        },
    }
}
fn delta(first: u64, last: u64) -> GateioDepthDelta {
    GateioDepthDelta {
        first_update_id: first,
        last_update_id: last,
        ts: 3.0,
        book: L2BookDelta {
            exchange: ExchangeId::Gateio,
            symbol: Symbol::perpetual("BTC", "USDT"),
            bids: vec![PriceLevel {
                price: 100.into(),
                amount: 3.into(),
            }],
            asks: vec![],
            exchange_ts: 3.0,
            received_ts: 3.1,
        },
    }
}
fn text(first: u64, last: u64) -> String {
    serde_json::json!({"channel":"futures.order_book_update","event":"update","result":{"s":"BTC_USDT","t":3000,"U":first,"u":last,"b":[{"p":"100","s":"3"}],"a":[]}}).to_string()
}
fn feed_plan() -> (ExchangeFeed, GateioConnectionPlan) {
    let mut feed = Gateio::new()
        .l2_book()
        .symbol("BTC-USDT-PERP")
        .exchange_symbol("BTC_USDT")
        .build();
    feed.replay_offline = true;
    let plan = GateioAdapter::connection_plans(&feed).unwrap().remove(0);
    (feed, plan)
}

#[tokio::test]
async fn gateio_bootstrap_success_then_immediate_gap_cannot_refund_request_budget() {
    let (mut feed, plan) = feed_plan();
    let identity = FeedIdentity {
        id: FeedId::allocate(),
        generation: 1,
    };
    let mut recovery_handler = FeedHandler::new();
    let recovery = recovery_handler.l2_book_handle();
    let mut monitor = readiness::FeedMonitor::new(identity, ExchangeId::Gateio, None);
    monitor.books = Some(recovery.0.clone());
    let monitor = Arc::new(monitor);
    monitor.configure(1, vec!["BTC-USDT-PERP".into()]);
    monitor.lifecycle(FeedState::Started);
    let tracker = monitor.register(
        "wss://fx-ws.gateio.ws/v4/ws/usdt".into(),
        vec!["BTC-USDT-PERP".into()],
    );
    let mut attempt = tracker.begin();
    attempt.connected();
    for message in attempt.prepare_messages(plan.subscription_messages.clone()) {
        attempt.sent(&message, tokio::time::Instant::now());
        let value: Value = serde_json::from_str(&message).unwrap();
        attempt.control(&serde_json::json!({"event":"subscribe","channel":value["channel"],"id":value["id"],"error":null,"result":{"status":"success"}})).unwrap();
    }
    feed.monitor = Some(monitor.clone());
    feed.connection_tracker = Some(tracker);
    feed.connection_epoch = Some(attempt.epoch);
    feed.identity = Some(identity);
    let mut receivers = GateioSnapshotReceivers::new();
    let mut pending = std::collections::HashMap::new();
    let mut budgets = std::collections::HashMap::new();
    process_gateio_orderbook_message_for_plan(
        &feed,
        &plan,
        &text(101, 101),
        3.1,
        &mut receivers,
        &mut pending,
        &mut budgets,
    )
    .await
    .unwrap();
    let mut snapshot_id = 100;
    for cycle in 0..=GATEIO_MAX_RESNAPSHOTS {
        receivers.insert(
            "BTC-USDT-PERP".into(),
            snapshot::SnapshotReceiver::ready(Ok(snapshot(Some(snapshot_id)))),
        );
        poll_gateio_snapshot_bootstraps_for_plan(
            &feed,
            &plan,
            &mut receivers,
            &mut pending,
            &mut budgets,
        )
        .await
        .unwrap();
        assert!(monitor.snapshot().is_ready());
        if cycle == 0 {
            let full=serde_json::json!({"channel":"futures.order_book_update","event":"update","result":{"s":"BTC_USDT","t":4000,"full":true,"u":200,"b":[{"p":"100","s":"4"}],"a":[{"p":"101","s":"1"}]}}).to_string();
            process_gateio_orderbook_message_for_plan(
                &feed,
                &plan,
                &full,
                4.1,
                &mut receivers,
                &mut pending,
                &mut budgets,
            )
            .await
            .unwrap();
            let d = monitor.snapshot().book_sync.remove(0);
            assert_eq!(d.last_delta_first, None);
            assert_eq!(d.last_delta_last, Some(200));
            assert!(d.synchronized);
            assert_eq!(d.snapshot_requests, 1);
        }
        let current = feed.gateio_book_syncs.lock().unwrap()["BTC-USDT-PERP"]
            .last_update_id()
            .unwrap();
        let result = process_gateio_orderbook_message_for_plan(
            &feed,
            &plan,
            &text(current + 2, current + 3),
            3.2,
            &mut receivers,
            &mut pending,
            &mut budgets,
        )
        .await;
        assert!(!monitor.snapshot().is_ready());
        assert_eq!(monitor.snapshot().books_synchronized, 0);
        assert!(
            recovery
                .recover(identity, &Symbol::perpetual("BTC", "USDT"))
                .snapshot
                .is_none()
        );
        if cycle < GATEIO_MAX_RESNAPSHOTS {
            result.unwrap();
        } else {
            assert!(
                matches!(result,Err(Error::Parse(message)) if message.contains("request limit"))
            );
        }
        snapshot_id = current + 1;
    }
    let diagnostic = monitor.snapshot().book_sync.remove(0);
    assert_eq!(diagnostic.snapshot_requests, 4);
    assert_eq!(diagnostic.total_snapshot_requests, 4);
    assert!(diagnostic.unsynchronized_since.is_some());
    assert_eq!(
        diagnostic.last_gap_first,
        diagnostic.last_gap_anchor.map(|id| id + 2)
    );
}

#[tokio::test(start_paused = true)]
async fn gateio_snapshot_deadline_does_not_require_a_delta_or_http_completion() {
    let (mut feed, plan) = feed_plan();
    feed.replay_offline = false;
    let mut receivers = GateioSnapshotReceivers::new();
    let (_, receiver, _) = snapshot::SnapshotReceiver::channel(snapshot::SnapshotMode::Replay);
    receivers.insert("BTC-USDT-PERP".into(), receiver);
    let mut budgets = std::collections::HashMap::new();
    gateio_snapshot_budget(&mut budgets, "BTC-USDT-PERP", true).unwrap();
    tokio::time::advance(GATEIO_BOOTSTRAP_DEADLINE + std::time::Duration::from_millis(1)).await;
    let error = poll_gateio_snapshot_bootstraps_for_plan(
        &feed,
        &plan,
        &mut receivers,
        &mut Default::default(),
        &mut budgets,
    )
    .await
    .unwrap_err();
    assert!(matches!(error,Error::Parse(message) if message.contains("deadline")));
}

#[tokio::test]
async fn gateio_idless_snapshot_is_not_published_and_retry_is_bounded() {
    let (feed, plan) = feed_plan();
    let mut receivers = GateioSnapshotReceivers::new();
    let mut budgets = std::collections::HashMap::new();
    let mut pending =
        std::collections::HashMap::from([("BTC-USDT-PERP".into(), vec![delta(100, 101)])]);
    for request in 0..=GATEIO_MAX_RESNAPSHOTS {
        receivers.insert(
            "BTC-USDT-PERP".into(),
            snapshot::SnapshotReceiver::ready(Ok(snapshot(None))),
        );
        let result = poll_gateio_snapshot_bootstraps_for_plan(
            &feed,
            &plan,
            &mut receivers,
            &mut pending,
            &mut budgets,
        )
        .await;
        if request < GATEIO_MAX_RESNAPSHOTS {
            result.unwrap();
        } else {
            assert!(result.is_err());
        }
        assert!(feed.orderbook_states.lock().unwrap().is_empty());
        assert!(feed.gateio_book_syncs.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn gateio_idle_socket_still_publishes_completed_bootstrap() {
    use tokio_tungstenite::{WebSocketStream, tungstenite::protocol::Role};
    let (mut feed, plan) = feed_plan();
    let (sender, mut events) = tokio::sync::broadcast::channel(16);
    feed.event_sender = Some(sender);
    let mut receivers = GateioSnapshotReceivers::new();
    receivers.insert(
        "BTC-USDT-PERP".into(),
        snapshot::SnapshotReceiver::ready(Ok(snapshot(Some(100)))),
    );
    let pending =
        std::collections::HashMap::from([("BTC-USDT-PERP".into(), vec![delta(101, 101)])]);
    let (client, server) = tokio::io::duplex(8192);
    let client = WebSocketStream::from_raw_socket(client, Role::Client, None).await;
    let _server = WebSocketStream::from_raw_socket(server, Role::Server, None).await;
    let mut session = Session::new(
        client,
        connection::HeartbeatPolicy::for_exchange(
            ExchangeId::Gateio,
            &Url::parse("wss://fx-ws.gateio.ws/v4/ws/usdt").unwrap(),
        ),
    );
    let (stop, shutdown) = watch::channel(false);
    let runtime =
        consume_gateio_session_with(feed, plan, shutdown, &mut session, receivers, pending);
    tokio::pin!(runtime);
    tokio::select! {biased;
        result=&mut runtime=>panic!("ended before snapshot: {result:?}"),
        result=tokio::time::timeout(std::time::Duration::from_secs(2),events.recv())=>{assert!(matches!(result.unwrap().unwrap(),crate::feed::FeedEvent::L2Book(L2Book::Snapshot(_))));}
    }
    stop.send_replace(true);
    tokio::time::timeout(std::time::Duration::from_secs(2), runtime)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn gateio_raw_replay_idless_snapshot_cannot_publish_but_valid_resnapshot_recovers() {
    use crate::recording::raw::{RawFeedInfo, RawPayload};
    let (feed, _) = feed_plan();
    let mut parser = RawParserSession::new(&RawFeedInfo::from_feed(&feed), 16, true).unwrap();
    let raw = RawPayload::Json {
        value: serde_json::from_str(&text(101, 101)).unwrap(),
        redacted: false,
    };
    assert!(parser.process(&raw, 3.0, 16).await.unwrap().is_empty());
    tokio::time::advance(GATEIO_BOOTSTRAP_DEADLINE + std::time::Duration::from_secs(1)).await;
    let mut value = serde_json::json!({"current":2.0,"update":2.0,"bids":[{"p":"100","s":"2"}],"asks":[{"p":"101","s":"1"}]});
    let raw = RawPayload::Json {
        value: value.clone(),
        redacted: false,
    };
    assert!(
        parser
            .snapshot(&Symbol::perpetual("BTC", "USDT"), None, &raw, 2.1, 16)
            .await
            .unwrap()
            .is_empty()
    );
    value["id"] = serde_json::json!(100);
    let raw = RawPayload::Json {
        value,
        redacted: false,
    };
    let events = parser
        .snapshot(&Symbol::perpetual("BTC", "USDT"), None, &raw, 2.1, 16)
        .await
        .unwrap();
    assert_eq!(events.len(), 2);
    assert!(
        matches!(&events[0],crate::feed::FeedEvent::L2Book(L2Book::Snapshot(book)) if book.bids[0].amount==2.into())
    );
    assert!(
        matches!(&events[1],crate::feed::FeedEvent::L2Book(L2Book::Delta(book)) if book.bids[0].amount==3.into())
    );
}
