use super::*;
use crate::recording::raw::replay::tests::{feed, segment, trade};
use crate::{
    feed::FeedEvent,
    recording::raw::{
        RawFeedInfo, RawObservationKind, RawPayload, RawRecordingWriter, RawSeriesWriter,
        RawSessionInfo,
    },
    recording::{RecordingEnd, RecordingLimits, ReplayOptions},
};
use cryptofeed_core::exchange::ExchangeId;
use serde_json::json;
use std::{io::Cursor, sync::Arc};
fn source(id: &str, offset: u64, bytes: Vec<u8>) -> RawMergeInput {
    RawMergeInput::recording(
        id,
        Duration::from_secs(offset),
        Cursor::new(bytes),
        RawRecordingLimits::default(),
    )
    .unwrap()
}
async fn trace(info: RawFeedInfo, payloads: Vec<serde_json::Value>) -> Vec<u8> {
    trace_end(info, payloads, RecordingEnd::Complete).await
}
async fn trace_end(
    info: RawFeedInfo,
    payloads: Vec<serde_json::Value>,
    end: RecordingEnd,
) -> Vec<u8> {
    let context = Arc::new(RawSessionInfo {
        session_id: 1,
        feed: info,
    });
    let mut kinds = vec![(0, RawObservationKind::Connected)];
    for (index, payload) in payloads.into_iter().enumerate() {
        let received_sequence = kinds.len() as u64 + 1;
        kinds.push((
            10 + index as u64 * 20,
            RawObservationKind::Received(RawPayload::Json {
                value: payload,
                redacted: false,
            }),
        ));
        kinds.push((
            20 + index as u64 * 20,
            RawObservationKind::Processing { received_sequence },
        ));
    }
    kinds.push((
        kinds.last().unwrap().0 + 10,
        RawObservationKind::Closed { clean: true },
    ));
    let mut bytes = Vec::new();
    let mut writer = RawRecordingWriter::new(&mut bytes, RawRecordingLimits::default())
        .await
        .unwrap();
    for (index, (elapsed, kind)) in kinds.into_iter().enumerate() {
        writer
            .append(RawObservation {
                version: 2,
                sequence: index as u64 + 1,
                elapsed_ns: elapsed * 1_000_000_000,
                observed_ts: 123.25,
                session: context.clone(),
                kind,
            })
            .await
            .unwrap();
    }
    writer.finish(end).await.unwrap();
    bytes
}
#[tokio::test(start_paused = true)]
async fn independent_merge_aligns_offsets_and_preserves_namespaced_native_clocks_and_sequences() {
    let a = trace(feed(ExchangeId::Okx), vec![trade(ExchangeId::Okx)]).await;
    let b = trace(feed(ExchangeId::Okx), vec![trade(ExchangeId::Okx)]).await;
    let byte_count = (a.len() + b.len()) as u64;
    let (_tx, rx) = watch::channel(false);
    let mut items = vec![];
    let summary = replay_raw_merge(
        vec![source("a", 0, a), source("b", 5, b)],
        RawMergeOptions::default(),
        rx,
        |item| {
            items.push(item);
            async { Ok(()) }
        },
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(summary.observations, 8);
    assert_eq!(summary.models, 2);
    assert_eq!(summary.bytes, byte_count);
    assert_eq!(
        items
            .iter()
            .map(|item| (item.capture_id.as_str(), item.elapsed_ns / 1_000_000_000))
            .collect::<Vec<_>>(),
        vec![
            ("a", 0),
            ("b", 5),
            ("a", 20),
            ("b", 25),
            ("a", 30),
            ("b", 35)
        ]
    );
    for item in items {
        if let RawReplayItem::Market(event) = item.item {
            assert_eq!(event.session_id, 1);
            assert_eq!(event.observation_sequence, 2);
            let FeedEvent::Trade(trade) = event.event else {
                panic!("trade expected")
            };
            assert_eq!(trade.received_ts, 123.25);
            assert_eq!(trade.exchange_ts, 1.0);
        }
    }
}
#[tokio::test(start_paused = true)]
async fn independent_merge_equal_times_follow_input_order_and_all_five_native_families_replay() {
    let mut inputs = vec![];
    for (index, exchange) in [
        ExchangeId::Binance,
        ExchangeId::Bitget,
        ExchangeId::Bybit,
        ExchangeId::Okx,
        ExchangeId::Gateio,
    ]
    .into_iter()
    .enumerate()
    {
        inputs.push(source(
            &format!("capture-{index}"),
            0,
            trace(feed(exchange), vec![trade(exchange)]).await,
        ));
    }
    let (_tx, rx) = watch::channel(false);
    let mut order = vec![];
    let summary = replay_raw_merge(inputs, RawMergeOptions::default(), rx, |item| {
        if matches!(item.item, RawReplayItem::Market(_)) {
            order.push(item.capture_id);
        }
        async { Ok(()) }
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        order,
        vec![
            "capture-0",
            "capture-1",
            "capture-2",
            "capture-3",
            "capture-4"
        ]
    );
    assert_eq!(summary.models, 5);
}
#[tokio::test(start_paused = true)]
async fn independent_merge_recorded_timing_has_one_common_origin_and_shutdown_interrupts_waits() {
    let bytes = trace(feed(ExchangeId::Okx), vec![trade(ExchangeId::Okx)]).await;
    let options = RawMergeOptions::default().replay(
        RawReplayOptions::default()
            .delivery(ReplayOptions::default().timing(ReplayTiming::Recorded)),
    );
    let start = tokio::time::Instant::now();
    let (_tx, rx) = watch::channel(false);
    let mut starts = vec![];
    replay_raw_merge(
        vec![source("a", 0, bytes.clone()), source("b", 5, bytes.clone())],
        options,
        rx,
        |item| {
            if matches!(item.item, RawReplayItem::SessionStarted(_)) {
                starts.push(tokio::time::Instant::now() - start);
            }
            async { Ok(()) }
        },
    )
    .await
    .unwrap();
    assert_eq!(starts, vec![Duration::ZERO, Duration::from_secs(5)]);
    assert_eq!(tokio::time::Instant::now() - start, Duration::from_secs(35));
    let (tx, rx) = watch::channel(false);
    let run = replay_raw_merge(vec![source("a", 100, bytes)], options, rx, |_| async {
        Ok(())
    });
    tokio::pin!(run);
    assert!(futures::poll!(run.as_mut()).is_pending());
    tx.send(false).unwrap();
    assert!(futures::poll!(run.as_mut()).is_pending());
    tx.send(true).unwrap();
    assert!(run.await.unwrap().is_none());
}
#[tokio::test(start_paused = true)]
async fn independent_merge_rejects_duplicate_ids_bounds_and_offset_overflow() {
    assert!(
        RawMergeInput::recording(
            "../bad",
            Duration::ZERO,
            Cursor::new(vec![]),
            RawRecordingLimits::default()
        )
        .is_err()
    );
    assert!(
        RawMergeInput::recording(
            "ok",
            Duration::MAX,
            Cursor::new(vec![]),
            RawRecordingLimits::default()
        )
        .is_err()
    );
    for inputs in [
        vec![],
        vec![source("same", 0, vec![]), source("same", 0, vec![])],
        (0..17)
            .map(|i| source(&format!("s{i}"), 0, vec![]))
            .collect(),
    ] {
        let (_tx, rx) = watch::channel(false);
        assert!(
            replay_raw_merge(inputs, RawMergeOptions::default(), rx, |_| async { Ok(()) })
                .await
                .is_err()
        );
    }
    let bytes = trace(feed(ExchangeId::Okx), vec![trade(ExchangeId::Okx)]).await;
    let input = RawMergeInput::recording(
        "overflow",
        Duration::from_nanos(u64::MAX),
        Cursor::new(bytes),
        RawRecordingLimits::default(),
    )
    .unwrap();
    let (_tx, rx) = watch::channel(false);
    assert!(
        replay_raw_merge(vec![input], RawMergeOptions::default(), rx, |_| async {
            Ok(())
        })
        .await
        .is_err()
    );
    assert!(RawMergeOptions::default().input_limits(0, 1).is_err());
    assert!(RawMergeOptions::default().input_limits(1, 0).is_err());
}
#[tokio::test(start_paused = true)]
async fn independent_merge_aggregate_byte_observation_and_model_budgets_fail_without_partial_success()
 {
    let bytes = trace(feed(ExchangeId::Okx), vec![trade(ExchangeId::Okx)]).await;
    let options = [
        RawMergeOptions::default().input_limits(100, 1).unwrap(),
        RawMergeOptions::default().input_limits(7, 100_000).unwrap(),
        RawMergeOptions::default()
            .replay(RawReplayOptions::default().output_limits(1024, 1).unwrap()),
    ];
    for options in options {
        let (_tx, rx) = watch::channel(false);
        let mut markets = 0;
        assert!(
            replay_raw_merge(
                vec![source("a", 0, bytes.clone()), source("b", 0, bytes.clone())],
                options,
                rx,
                |item| {
                    if matches!(item.item, RawReplayItem::Market(_)) {
                        markets += 1;
                    }
                    async { Ok(()) }
                }
            )
            .await
            .is_err()
        );
        if options.replay.maximum_models == 1 {
            assert_eq!(markets, 1);
        }
    }
}
#[tokio::test(start_paused = true)]
async fn independent_merge_corrupt_late_input_returns_error_and_pre_shutdown_does_not_read() {
    let bytes = trace(feed(ExchangeId::Okx), vec![trade(ExchangeId::Okx)]).await;
    let mut truncated = bytes.clone();
    truncated.truncate(truncated.len() - 5);
    let (_tx, rx) = watch::channel(false);
    let mut markets = 0;
    assert!(
        replay_raw_merge(
            vec![source("good", 0, bytes), source("bad", 0, truncated)],
            RawMergeOptions::default(),
            rx,
            |item| {
                if matches!(item.item, RawReplayItem::Market(_)) {
                    markets += 1;
                }
                async { Ok(()) }
            }
        )
        .await
        .is_err()
    );
    assert_eq!(markets, 2);
    let (_tx, rx) = watch::channel(true);
    assert!(
        replay_raw_merge(
            vec![source("unread", 0, vec![])],
            RawMergeOptions::default(),
            rx,
            |_| async { panic!("no callback") }
        )
        .await
        .unwrap()
        .is_none()
    );
}
#[tokio::test(start_paused = true)]
async fn independent_merge_callback_failure_panic_deadline_and_pending_read_are_terminal() {
    let bytes = trace(feed(ExchangeId::Okx), vec![trade(ExchangeId::Okx)]).await;
    let (_tx, rx) = watch::channel(false);
    assert!(
        replay_raw_merge(
            vec![source("a", 0, bytes.clone())],
            RawMergeOptions::default(),
            rx,
            |_| async { Err(Error::Protocol("callback failure".into())) }
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("callback failure")
    );
    let (_tx, rx) = watch::channel(false);
    assert!(
        replay_raw_merge(
            vec![source("a", 0, bytes.clone())],
            RawMergeOptions::default(),
            rx,
            |_| async { panic!("callback panic") }
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("panicked")
    );
    let (_tx, rx) = watch::channel(false);
    assert!(
        replay_raw_merge(
            vec![source("a", 0, bytes)],
            RawMergeOptions::default(),
            rx,
            |_| std::future::pending::<Result<()>>()
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("deadline")
    );
    let (read, _write) = tokio::io::duplex(64);
    let source = RawMergeInput::recording(
        "pending",
        Duration::ZERO,
        tokio::io::BufReader::new(read),
        RawRecordingLimits::default(),
    )
    .unwrap();
    let (tx, rx) = watch::channel(false);
    let run = replay_raw_merge(vec![source], RawMergeOptions::default(), rx, |_| async {
        Ok(())
    });
    tokio::pin!(run);
    assert!(futures::poll!(run.as_mut()).is_pending());
    tx.send(true).unwrap();
    assert!(run.await.unwrap().is_none());
}
#[tokio::test(start_paused = true)]
async fn independent_merge_reuses_native_reconnect_rules_and_reports_each_prefix_end() {
    let bytes = segment(
        feed(ExchangeId::Okx),
        vec![trade(ExchangeId::Okx), trade(ExchangeId::Okx)],
        true,
    )
    .await;
    let mut expected_ids = Vec::new();
    let mut recorded =
        RawRecordingReader::new(Cursor::new(bytes.clone()), RawRecordingLimits::default());
    while let Some(row) = recorded.next_observation().await.unwrap() {
        if matches!(row.kind, RawObservationKind::Connected) {
            expected_ids.push(row.session.session_id);
        }
    }
    assert_eq!(expected_ids.len(), 2);
    assert_ne!(expected_ids[0], expected_ids[1]);
    expected_ids.extend([1, 1]);
    let (_tx, rx) = watch::channel(false);
    let mut session_ids = vec![];
    let summary = replay_raw_merge(
        vec![
            source("reconnect", 0, bytes),
            source(
                "stopped",
                0,
                trace_end(
                    feed(ExchangeId::Okx),
                    vec![trade(ExchangeId::Okx)],
                    RecordingEnd::Stopped,
                )
                .await,
            ),
            source(
                "limited",
                0,
                trace_end(
                    feed(ExchangeId::Okx),
                    vec![trade(ExchangeId::Okx)],
                    RecordingEnd::LimitReached,
                )
                .await,
            ),
        ],
        RawMergeOptions::default(),
        rx,
        |item| {
            if let RawReplayItem::Market(event) = item.item {
                session_ids.push(event.session_id);
            }
            async { Ok(()) }
        },
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(session_ids, expected_ids);
    assert_eq!(summary.recordings[1].1.end, RecordingEnd::Stopped);
    assert_eq!(summary.recordings[2].1.end, RecordingEnd::LimitReached);
    assert_eq!(summary.recordings[0].1.end, RecordingEnd::Complete);
}
#[cfg(feature = "orderbook")]
#[tokio::test(start_paused = true)]
async fn independent_merge_isolates_identical_session_ids_and_native_l2_sequences() {
    use crate::exchange::bybit::Bybit;
    let info = RawFeedInfo::from_feed(
        &Bybit::new()
            .l2_book()
            .symbol("BTC-USDT")
            .exchange_symbol("BTCUSDT")
            .build(),
    );
    let mut inputs = vec![];
    for (id, bid, update) in [("a", 100, 1), ("b", 200, 50)] {
        let messages=(0..2).map(|delta|json!({"topic":"orderbook.50.BTCUSDT","type":if delta==0 {"snapshot"} else {"delta"},"ts":1000,"data":{"s":"BTCUSDT","b":[[bid.to_string(),(delta+1).to_string()]],"a":[[(bid+1).to_string(),"1"]],"u":update+delta,"seq":update+delta}})).collect();
        inputs.push(source(id, 0, trace(info.clone(), messages).await));
    }
    let (_tx, rx) = watch::channel(false);
    let mut levels = vec![];
    let summary = replay_raw_merge(inputs, RawMergeOptions::default(), rx, |item| {
        if let RawReplayItem::Market(event) = item.item {
            let FeedEvent::L2Book(book) = event.event else {
                panic!("book expected")
            };
            levels.push((
                item.capture_id,
                book.bids()[0].price.to_string(),
                book.bids()[0].amount.to_string(),
            ));
        }
        async { Ok(()) }
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(summary.models, 4);
    assert_eq!(
        levels,
        vec![
            ("a".into(), "100".into(), "1".into()),
            ("b".into(), "200".into(), "1".into()),
            ("a".into(), "100".into(), "2".into()),
            ("b".into(), "200".into(), "2".into())
        ]
    );
}
#[tokio::test]
async fn independent_merge_series_preserves_rotation_and_certifies_manifest_at_eof() {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let path = std::env::temp_dir().join(format!(
        "cryptofeed-independent-merge-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    ));
    tokio::fs::create_dir(&path).await.unwrap();
    let records =
        RawRecordingLimits::new(RecordingLimits::new(100, 64 * 1024, 1024).unwrap(), 10).unwrap();
    let limits = RawSeriesLimits::new(records, 1024, 20).unwrap();
    let bytes = trace(feed(ExchangeId::Okx), vec![trade(ExchangeId::Okx)]).await;
    let mut raw =
        RawRecordingReader::new(Cursor::new(bytes.clone()), RawRecordingLimits::default());
    let mut writer = RawSeriesWriter::create(&path, "series", limits)
        .await
        .unwrap();
    while let Some(row) = raw.next_observation().await.unwrap() {
        writer.append(row).await.unwrap();
    }
    writer.finish(RecordingEnd::Complete).await.unwrap();
    let reader = RawSeriesReader::open(&path, "series", limits)
        .await
        .unwrap();
    assert!(reader.part_count() > 1);
    drop(reader);
    let input = RawMergeInput::series("rotated", Duration::ZERO, &path, "series", limits)
        .await
        .unwrap();
    let (_tx, rx) = watch::channel(false);
    let summary = replay_raw_merge(
        vec![input, source("file", 0, bytes)],
        RawMergeOptions::default(),
        rx,
        |_| async { Ok(()) },
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(summary.models, 2);
    let manifest = path.join("series.manifest.json");
    let mut value: serde_json::Value =
        serde_json::from_slice(&tokio::fs::read(&manifest).await.unwrap()).unwrap();
    value["events"] = json!(99);
    tokio::fs::write(&manifest, serde_json::to_vec(&value).unwrap())
        .await
        .unwrap();
    let input = RawMergeInput::series("corrupt", Duration::ZERO, &path, "series", limits)
        .await
        .unwrap();
    let (_tx, rx) = watch::channel(false);
    assert!(
        replay_raw_merge(vec![input], RawMergeOptions::default(), rx, |_| async {
            Ok(())
        })
        .await
        .is_err()
    );
    tokio::fs::remove_dir_all(path).await.unwrap();
}

#[cfg(feature = "orderbook")]
#[tokio::test(start_paused = true)]
async fn independent_merge_keeps_consumed_http_and_processing_causality_for_binance_and_gate() {
    use crate::exchange::ExchangeFeedBuilder;
    use cryptofeed_core::symbol::Symbol;
    let mut inputs = vec![];
    for (id, exchange, offset) in [
        ("binance", ExchangeId::Binance, 0),
        ("gate", ExchangeId::Gateio, 5),
    ] {
        let native = if exchange == ExchangeId::Binance {
            "BTCUSDT"
        } else {
            "BTC_USDT"
        };
        let info = RawFeedInfo::from_feed(
            &ExchangeFeedBuilder::new(exchange)
                .l2_book()
                .symbol("BTC-USDT")
                .exchange_symbol(native)
                .build(),
        );
        let context = Arc::new(RawSessionInfo {
            session_id: 1,
            feed: info,
        });
        let first = if exchange == ExchangeId::Binance {
            json!({"e":"depthUpdate","s":"BTCUSDT","E":1710000002000u64,"U":101,"u":101,"b":[["100","3"]],"a":[]})
        } else {
            json!({"channel":"spot.order_book_update","event":"update","result":{"s":"BTC_USDT","t":1710000002000u64,"U":101,"u":101,"b":[["100","3"]],"a":[]}})
        };
        let mut second = first.clone();
        let fields = if exchange == ExchangeId::Binance {
            &mut second
        } else {
            &mut second["result"]
        };
        fields["U"] = json!(102);
        fields["u"] = json!(102);
        fields["b"] = json!([["100", "4"]]);
        let payload = if exchange == ExchangeId::Binance {
            json!({"lastUpdateId":100,"bids":[["100","2"]],"asks":[["101","1"]]})
        } else {
            json!({"current":1710000002000u64,"update":1710000002000u64,"bids":[["100","2"]],"asks":[["101","1"]]})
        };
        let kinds = vec![
            (0, RawObservationKind::Connected),
            (
                10,
                RawObservationKind::Received(RawPayload::Json {
                    value: first,
                    redacted: false,
                }),
            ),
            (
                20,
                RawObservationKind::Processing {
                    received_sequence: 2,
                },
            ),
            (
                30,
                RawObservationKind::Received(RawPayload::Json {
                    value: second,
                    redacted: false,
                }),
            ),
            (
                35,
                RawObservationKind::HttpSnapshot {
                    symbol: Symbol::spot("BTC", "USDT"),
                    depth: if exchange == ExchangeId::Binance {
                        Some(1000)
                    } else {
                        None
                    },
                    payload: RawPayload::Json {
                        value: payload,
                        redacted: false,
                    },
                    received_ts: 1710000002.1,
                },
            ),
            (
                40,
                RawObservationKind::Processing {
                    received_sequence: 4,
                },
            ),
            (50, RawObservationKind::Closed { clean: true }),
        ];
        let mut bytes = Vec::new();
        let mut writer = RawRecordingWriter::new(&mut bytes, RawRecordingLimits::default())
            .await
            .unwrap();
        for (index, (at, kind)) in kinds.into_iter().enumerate() {
            writer
                .append(RawObservation {
                    version: 2,
                    sequence: index as u64 + 1,
                    elapsed_ns: at * 1_000_000_000,
                    observed_ts: 1710000002.2,
                    session: context.clone(),
                    kind,
                })
                .await
                .unwrap();
        }
        writer.finish(RecordingEnd::Complete).await.unwrap();
        inputs.push(source(id, offset, bytes));
    }
    let (_tx, rx) = watch::channel(false);
    let mut seen = std::collections::HashMap::<String, Vec<_>>::new();
    let summary = replay_raw_merge(inputs, RawMergeOptions::default(), rx, |item| {
        if let RawReplayItem::Market(event) = item.item {
            let FeedEvent::L2Book(book) = event.event else {
                panic!("book expected")
            };
            seen.entry(item.capture_id).or_default().push((
                event.observation_sequence,
                book.bids()[0].amount.to_string(),
            ));
        }
        async { Ok(()) }
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(summary.models, 6);
    for id in ["binance", "gate"] {
        assert_eq!(
            seen[id],
            vec![(5, "2".into()), (5, "3".into()), (4, "4".into())]
        );
    }
}
