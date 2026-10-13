use super::*;
use crate::{
    exchange::ExchangeFeedBuilder,
    recording::{
        RecordingEnd,
        raw::{
            RawFeedInfo, RawRecordingLimits, RawRecordingReader, RawRecordingWriter, RawReplayItem,
            RawReplayOptions, raw_capture_channel,
        },
    },
};
use serde_json::json;

#[tokio::test]
async fn consumed_http_snapshot_and_ws_processing_barrier_match_native_outputs() {
    for exchange in [ExchangeId::Binance, ExchangeId::Gateio] {
        let native = if exchange == ExchangeId::Binance {
            "BTCUSDT"
        } else {
            "BTC_USDT"
        };
        let mut feed = ExchangeFeedBuilder::new(exchange)
            .l2_book()
            .symbol("BTC-USDT")
            .exchange_symbol(native)
            .build();
        let (capture, mut raw_input) = raw_capture_channel(64, 1024 * 1024).unwrap();
        let mut trace = capture.session(RawFeedInfo::from_feed(&feed)).unwrap();
        feed.raw_session = Some(trace.link());
        feed.replay_offline = true;
        let (sender, mut normalized) = tokio::sync::broadcast::channel(64);
        feed.event_sender = Some(sender);
        let first = if exchange == ExchangeId::Binance {
            json!({"e":"depthUpdate","s":"BTCUSDT","E":1710000002000u64,"U":101,"u":101,"b":[["100","3"]],"a":[]})
        } else {
            json!({"channel":"spot.order_book_update","event":"update","result":{"s":"BTC_USDT","t":1710000002000u64,"U":101,"u":101,"b":[["100","3"]],"a":[]}})
        };
        let mut second = first.clone();
        if exchange == ExchangeId::Binance {
            second["U"] = json!(102);
            second["u"] = json!(102);
            second["b"] = json!([["100", "4"]]);
        } else {
            second["result"]["U"] = json!(102);
            second["result"]["u"] = json!(102);
            second["result"]["b"] = json!([["100", "4"]]);
        }
        let payload = if exchange == ExchangeId::Binance {
            json!({"lastUpdateId":100,"bids":[["100","2"]],"asks":[["101","1"]]})
        } else {
            json!({"current":1710000002000u64,"update":1710000002000u64,"bids":[["100","2"]],"asks":[["101","1"]]})
        };
        let first_text = serde_json::to_string(&first).unwrap();
        let second_text = serde_json::to_string(&second).unwrap();
        let sequence = trace
            .text(&first_text, Some(&first), true, 1710000002.2)
            .unwrap();
        trace.processing(sequence);
        if exchange == ExchangeId::Binance {
            let plan = BinanceAdapter::connection_plans(&feed).unwrap().remove(0);
            let mut receivers = SnapshotReceivers::new();
            let mut pending = std::collections::HashMap::new();
            let mut attempts = std::collections::HashMap::new();
            receivers.insert(
                "BTC-USDT".into(),
                snapshot::SnapshotReceiver::channel(snapshot::SnapshotMode::Replay).1,
            );
            process_binance_orderbook_message(
                &feed,
                &plan,
                &first_text,
                1710000002.2,
                &mut receivers,
                &mut pending,
            )
            .await
            .unwrap();
            let sequence = trace
                .text(&second_text, Some(&second), true, 1710000003.0)
                .unwrap();
            let (tx, receiver, raw) =
                snapshot::SnapshotReceiver::channel(snapshot::SnapshotMode::Live(true));
            *raw.unwrap().lock().unwrap() = Some(snapshot::RawSnapshot {
                payload: payload.clone(),
                received_ts: 1710000002.1,
            });
            let parsed = binance_parser::parse_l2_book_snapshot_for_instrument(
                &payload,
                &plan.instruments[0],
                1710000002.1,
            )
            .unwrap();
            tx.unwrap().send(Ok(parsed)).unwrap();
            receivers.insert("BTC-USDT".into(), receiver);
            poll_binance_snapshot_bootstraps(
                &feed,
                &plan.instruments,
                &mut receivers,
                &mut pending,
                &mut attempts,
                1000,
                false,
            )
            .await
            .unwrap();
            trace.processing(sequence);
            process_binance_orderbook_message(
                &feed,
                &plan,
                &second_text,
                1710000003.0,
                &mut receivers,
                &mut pending,
            )
            .await
            .unwrap();
        } else {
            let plan = GateioAdapter::connection_plans(&feed).unwrap().remove(0);
            let mut receivers = GateioSnapshotReceivers::new();
            let mut pending = std::collections::HashMap::new();
            let mut attempts = std::collections::HashMap::new();
            process_gateio_orderbook_message_for_plan(
                &feed,
                &plan,
                &first_text,
                1710000002.2,
                &mut receivers,
                &mut pending,
            )
            .await
            .unwrap();
            let sequence = trace
                .text(&second_text, Some(&second), true, 1710000003.0)
                .unwrap();
            let (tx, receiver, raw) =
                snapshot::SnapshotReceiver::channel(snapshot::SnapshotMode::Live(true));
            *raw.unwrap().lock().unwrap() = Some(snapshot::RawSnapshot {
                payload: payload.clone(),
                received_ts: 1710000002.1,
            });
            let parsed = gateio_parser::parse_l2_book_snapshot_for_instrument(
                &payload,
                &plan.instruments[0],
                1710000002.1,
            )
            .unwrap();
            tx.unwrap().send(Ok(parsed)).unwrap();
            receivers.insert("BTC-USDT".into(), receiver);
            poll_gateio_snapshot_bootstraps_for_plan(
                &feed,
                &plan,
                &mut receivers,
                &mut pending,
                &mut attempts,
            )
            .await
            .unwrap();
            trace.processing(sequence);
            process_gateio_orderbook_message_for_plan(
                &feed,
                &plan,
                &second_text,
                1710000003.0,
                &mut receivers,
                &mut pending,
            )
            .await
            .unwrap();
        }
        trace.close(true);
        drop(trace);
        feed.raw_session = None;
        drop(feed);
        drop(capture);
        let mut expected = Vec::new();
        while let Ok(event) = normalized.try_recv() {
            expected.push(serde_json::to_value(event).unwrap());
        }
        assert_eq!(expected.len(), 3, "{exchange:?}");
        let mut bytes = Vec::new();
        let mut writer = RawRecordingWriter::new(&mut bytes, RawRecordingLimits::default())
            .await
            .unwrap();
        let mut has_snapshot = false;
        let mut snapshot_sequence = 0;
        let mut last_received_sequence = 0;
        while let Some(record) = raw_input.recv().await.unwrap() {
            if matches!(
                record.kind,
                crate::recording::raw::RawObservationKind::HttpSnapshot { .. }
            ) {
                has_snapshot = true;
                snapshot_sequence = record.sequence;
            }
            if matches!(
                record.kind,
                crate::recording::raw::RawObservationKind::Received(_)
            ) {
                last_received_sequence = record.sequence;
            }
            writer.append(record).await.unwrap();
        }
        assert!(has_snapshot);
        writer.finish(RecordingEnd::Complete).await.unwrap();
        let mut reader = RawRecordingReader::new(bytes.as_slice(), RawRecordingLimits::default());
        let (_tx, stop) = tokio::sync::watch::channel(false);
        let mut actual = Vec::new();
        let mut origins = Vec::new();
        reader
            .replay(RawReplayOptions::default(), stop, |item| {
                if let RawReplayItem::Market(event) = item {
                    origins.push(event.observation_sequence);
                    actual.push(serde_json::to_value(event.event).unwrap());
                }
                std::future::ready(Ok(()))
            })
            .await
            .unwrap();
        assert_eq!(
            origins,
            vec![snapshot_sequence, snapshot_sequence, last_received_sequence]
        );
        assert_eq!(
            actual, expected,
            "{exchange:?}: consumption boundary, bridge and clocks"
        );
    }
}

#[tokio::test]
async fn snapshot_failure_replays_safe_status_without_online_request() {
    let mut feed = ExchangeFeedBuilder::new(ExchangeId::Binance)
        .l2_book()
        .symbol("BTC-USDT")
        .exchange_symbol("BTCUSDT")
        .build();
    let (capture, mut raw_input) = raw_capture_channel(32, 1024 * 1024).unwrap();
    let mut trace = capture.session(RawFeedInfo::from_feed(&feed)).unwrap();
    feed.raw_session = Some(trace.link());
    feed.replay_offline = true;
    let plan = BinanceAdapter::connection_plans(&feed).unwrap().remove(0);
    let value = json!({"e":"depthUpdate","s":"BTCUSDT","U":101,"u":101,"b":[],"a":[]});
    let _ = trace.text(&value.to_string(), Some(&value), true, 2.0);
    let mut receivers = SnapshotReceivers::new();
    receivers.insert(
        "BTC-USDT".into(),
        snapshot::SnapshotReceiver::ready(Err(Error::HttpStatus {
            status: 429,
            retry_after: None,
        })),
    );
    let result = poll_binance_snapshot_bootstraps(
        &feed,
        &plan.instruments,
        &mut receivers,
        &mut Default::default(),
        &mut Default::default(),
        1000,
        false,
    )
    .await;
    assert!(matches!(result, Err(Error::HttpStatus { status: 429, .. })));
    trace.close(false);
    drop(trace);
    feed.raw_session = None;
    drop(feed);
    drop(capture);
    let mut bytes = Vec::new();
    let mut writer = RawRecordingWriter::new(&mut bytes, RawRecordingLimits::default())
        .await
        .unwrap();
    while let Some(record) = raw_input.recv().await.unwrap() {
        writer.append(record).await.unwrap();
    }
    writer.finish(RecordingEnd::Complete).await.unwrap();
    let mut reader = RawRecordingReader::new(bytes.as_slice(), RawRecordingLimits::default());
    let (_tx, stop) = tokio::sync::watch::channel(false);
    assert!(matches!(
        reader
            .replay(RawReplayOptions::default(), stop, |_| std::future::ready(
                Ok(())
            ))
            .await,
        Err(Error::HttpStatus { status: 429, .. })
    ));
}

#[tokio::test]
async fn snapshot_event_rejects_missing_bootstrap_and_gap_schedules_only_offline_placeholder() {
    let feed = ExchangeFeedBuilder::new(ExchangeId::Binance)
        .l2_book()
        .symbol("BTC-USDT")
        .exchange_symbol("BTCUSDT")
        .build();
    let info = RawFeedInfo::from_feed(&feed);
    let mut replay = RawParserSession::new(&info, 1024, true).unwrap();
    let first = json!({"e":"depthUpdate","s":"BTCUSDT","U":101,"u":101,"b":[["100","3"]],"a":[]});
    let payload = |value| crate::recording::raw::RawPayload::Json {
        value,
        redacted: false,
    };
    assert!(
        replay
            .process(&payload(first), 2.0, 1024)
            .await
            .unwrap()
            .is_empty()
    );
    let body = json!({"lastUpdateId":100,"bids":[["100","2"]],"asks":[["101","1"]]});
    assert_eq!(
        replay
            .snapshot(
                &feed.symbols[0],
                Some(1000),
                &payload(body.clone()),
                1.5,
                1024
            )
            .await
            .unwrap()
            .len(),
        2
    );
    assert!(
        replay
            .snapshot(&feed.symbols[0], Some(1000), &payload(body), 1.5, 1024)
            .await
            .is_err()
    );
    // A live-next overlap is not allowed, even though buffered bridging can overlap.
    let gap = json!({"e":"depthUpdate","s":"BTCUSDT","U":101,"u":102,"b":[["100","4"]],"a":[]});
    assert!(
        replay
            .process(&payload(gap), 3.0, 1024)
            .await
            .unwrap()
            .is_empty()
    );
    let fresh = json!({"lastUpdateId":102,"bids":[["100","4"]],"asks":[["101","1"]]});
    assert_eq!(
        replay
            .snapshot(&feed.symbols[0], Some(1000), &payload(fresh), 3.5, 1024)
            .await
            .unwrap()
            .len(),
        1
    );
    let next = json!({"e":"depthUpdate","s":"BTCUSDT","U":103,"u":103,"b":[["100","5"]],"a":[]});
    assert_eq!(
        replay
            .process(&payload(next), 4.0, 1024)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn inline_v2_snapshot_reference_replays_without_http() {
    const FIXTURE:&[u8]=br#"{"header":{"format":"cryptofeed-rs.raw-ws","version":2}}
{"observation":{"record":{"version":2,"sequence":1,"elapsed_ns":1,"observed_ts":2.0,"session":{"session_id":1,"feed":{"sdk_version":"0.1.0","identity":null,"exchange":"Binance","channels":["L2Book"],"symbols":[{"value":"BTC-USDT","kind":"Spot"}],"exchange_symbols":["BTCUSDT"],"channel_subscriptions":[],"candle_interval":"1m","candle_policy":"All","l2_book_depth":null,"l2_book_interval":null}},"kind":"connected"}}}
{"observation":{"record":{"version":2,"sequence":2,"elapsed_ns":2,"observed_ts":2.0,"session":{"session_id":1,"feed":{"sdk_version":"0.1.0","identity":null,"exchange":"Binance","channels":["L2Book"],"symbols":[{"value":"BTC-USDT","kind":"Spot"}],"exchange_symbols":["BTCUSDT"],"channel_subscriptions":[],"candle_interval":"1m","candle_policy":"All","l2_book_depth":null,"l2_book_interval":null}},"kind":{"received":{"json":{"value":{"e":"depthUpdate","s":"BTCUSDT","E":1000,"U":101,"u":101,"b":[["100","3"]],"a":[]},"redacted":false}}}}}}
{"observation":{"record":{"version":2,"sequence":3,"elapsed_ns":3,"observed_ts":2.0,"session":{"session_id":1,"feed":{"sdk_version":"0.1.0","identity":null,"exchange":"Binance","channels":["L2Book"],"symbols":[{"value":"BTC-USDT","kind":"Spot"}],"exchange_symbols":["BTCUSDT"],"channel_subscriptions":[],"candle_interval":"1m","candle_policy":"All","l2_book_depth":null,"l2_book_interval":null}},"kind":{"processing":{"received_sequence":2}}}}}
{"observation":{"record":{"version":2,"sequence":4,"elapsed_ns":4,"observed_ts":3.0,"session":{"session_id":1,"feed":{"sdk_version":"0.1.0","identity":null,"exchange":"Binance","channels":["L2Book"],"symbols":[{"value":"BTC-USDT","kind":"Spot"}],"exchange_symbols":["BTCUSDT"],"channel_subscriptions":[],"candle_interval":"1m","candle_policy":"All","l2_book_depth":null,"l2_book_interval":null}},"kind":{"received":{"json":{"value":{"e":"depthUpdate","s":"BTCUSDT","E":1001,"U":102,"u":102,"b":[["100","4"]],"a":[]},"redacted":false}}}}}}
{"observation":{"record":{"version":2,"sequence":5,"elapsed_ns":5,"observed_ts":3.0,"session":{"session_id":1,"feed":{"sdk_version":"0.1.0","identity":null,"exchange":"Binance","channels":["L2Book"],"symbols":[{"value":"BTC-USDT","kind":"Spot"}],"exchange_symbols":["BTCUSDT"],"channel_subscriptions":[],"candle_interval":"1m","candle_policy":"All","l2_book_depth":null,"l2_book_interval":null}},"kind":{"http_snapshot":{"symbol":{"value":"BTC-USDT","kind":"Spot"},"depth":1000,"payload":{"json":{"value":{"lastUpdateId":100,"bids":[["100","2"]],"asks":[["101","1"]]},"redacted":false}},"received_ts":1.5}}}}}
{"observation":{"record":{"version":2,"sequence":6,"elapsed_ns":6,"observed_ts":3.0,"session":{"session_id":1,"feed":{"sdk_version":"0.1.0","identity":null,"exchange":"Binance","channels":["L2Book"],"symbols":[{"value":"BTC-USDT","kind":"Spot"}],"exchange_symbols":["BTCUSDT"],"channel_subscriptions":[],"candle_interval":"1m","candle_policy":"All","l2_book_depth":null,"l2_book_interval":null}},"kind":{"processing":{"received_sequence":4}}}}}
{"observation":{"record":{"version":2,"sequence":7,"elapsed_ns":7,"observed_ts":3.0,"session":{"session_id":1,"feed":{"sdk_version":"0.1.0","identity":null,"exchange":"Binance","channels":["L2Book"],"symbols":[{"value":"BTC-USDT","kind":"Spot"}],"exchange_symbols":["BTCUSDT"],"channel_subscriptions":[],"candle_interval":"1m","candle_policy":"All","l2_book_depth":null,"l2_book_interval":null}},"kind":{"closed":{"clean":true}}}}}
{"end":{"events":7,"reason":"complete"}}
"#;
    let mut reader = RawRecordingReader::new(FIXTURE, RawRecordingLimits::default());
    let (_tx, stop) = tokio::sync::watch::channel(false);
    let mut books = Vec::new();
    let summary = reader
        .replay(RawReplayOptions::default(), stop, |item| {
            if let RawReplayItem::Market(event) = item {
                if let crate::feed::FeedEvent::L2Book(book) = event.event {
                    books.push(book);
                }
            }
            std::future::ready(Ok(()))
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(summary.models, 3);
    assert_eq!(books[0].bids()[0].amount.to_string(), "2");
    assert_eq!(books[1].bids()[0].amount.to_string(), "3");
    assert_eq!(books[2].bids()[0].amount.to_string(), "4");
    assert_eq!(books[0].received_ts(), 1.5);
    assert_eq!(books[1].received_ts(), 2.0);
    assert_eq!(books[2].received_ts(), 3.0);
}
