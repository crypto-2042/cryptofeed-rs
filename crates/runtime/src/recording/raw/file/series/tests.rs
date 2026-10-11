use super::*;
use crate::{
    exchange::okx::Okx,
    recording::raw::{RawFeedInfo, raw_capture_channel},
};
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "cryptofeed-series-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn limits() -> RawSeriesLimits {
    RawSeriesLimits::new(
        RawRecordingLimits::new(RecordingLimits::new(100, 64 * 1024, 2048).unwrap(), 16).unwrap(),
        2048,
        32,
    )
    .unwrap()
}
async fn observations() -> Vec<RawObservation> {
    let (capture, mut input) = raw_capture_channel(16, 4096).unwrap();
    let feed = Okx::new()
        .trade()
        .symbol("BTC-USDT")
        .exchange_symbol("BTC-USDT")
        .build();
    let mut session = capture.session(RawFeedInfo::from_feed(&feed)).unwrap();
    let payload = json!({"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","px":"10.1234567890123456789012345","sz":"0.2","side":"buy","tradeId":"1","ts":"1000"}],"padding":"x".repeat(900)});
    let received = session
        .text(&payload.to_string(), Some(&payload), true, 1.25)
        .unwrap();
    session.processing(received);
    session.close(true);
    drop(session);
    drop(capture);
    let mut observations = Vec::new();
    while let Some(row) = input.recv().await.unwrap() {
        observations.push(row);
    }
    observations
}
async fn capture(directory: &Directory) -> (Vec<RawObservation>, RecordingSummary) {
    let rows = observations().await;
    let mut writer = RawSeriesWriter::create(&directory.0, "capture", limits())
        .await
        .unwrap();
    for row in &rows {
        assert!(writer.append(row.clone()).await.unwrap());
    }
    let summary = writer.finish(RecordingEnd::Complete).await.unwrap();
    (rows, summary)
}
async fn read_all(reader: &mut RawSeriesReader) -> Result<Vec<RawObservation>> {
    let mut rows = Vec::new();
    while let Some(row) = reader.next_observation().await? {
        rows.push(row);
    }
    Ok(rows)
}
fn manifest(directory: &Directory) -> Manifest {
    serde_json::from_slice(&std::fs::read(manifest_path(&directory.0, "capture")).unwrap()).unwrap()
}
#[test]
fn fnv1a64_known_vector_and_chunked_hash_agree() {
    assert_eq!(hash(BASIS, b"hello"), 0xa430d84680aabd0b);
    assert_eq!(hash(hash(BASIS, b"hel"), b"lo"), hash(BASIS, b"hello"));
}
#[tokio::test]
async fn rotation_keeps_exact_observations_causal_refs_and_one_logical_footer() {
    let directory = Directory::new();
    let (expected, summary) = capture(&directory).await;
    let meta = manifest(&directory);
    assert!(meta.parts.len() >= 2);
    assert!(meta.parts.iter().all(|part| part.bytes <= 2048));
    let mut reader = RawSeriesReader::open(&directory.0, "capture", limits())
        .await
        .unwrap();
    assert!(reader.summary().is_none());
    assert_eq!(read_all(&mut reader).await.unwrap(), expected);
    assert_eq!(reader.summary(), Some(summary));
    let mut bytes = Vec::new();
    for index in 0..meta.parts.len() {
        bytes.extend(std::fs::read(part_path(&directory.0, "capture", index)).unwrap());
    }
    let mut plain = RawRecordingReader::new(bytes.as_slice(), limits().recording);
    let mut rows = Vec::new();
    while let Some(row) = plain.next_observation().await.unwrap() {
        rows.push(row);
    }
    assert_eq!(rows, expected);
    assert_eq!(plain.summary(), Some(summary));
}
#[tokio::test]
async fn missing_truncated_extra_reordered_and_same_length_corrupt_parts_fail_without_certificate()
{
    for mode in 0..6 {
        let directory = Directory::new();
        capture(&directory).await;
        let meta = manifest(&directory);
        let path = part_path(&directory.0, "capture", 1);
        let original = std::fs::read(&path).unwrap();
        match mode {
            0 => std::fs::remove_file(&path).unwrap(),
            1 => std::fs::write(&path, &original[..original.len() - 1]).unwrap(),
            2 => {
                let mut changed = original.clone();
                changed.push(b' ');
                std::fs::write(&path, changed).unwrap();
            }
            3 => {
                let mut changed = original.clone();
                let index = changed.iter().position(|byte| *byte == b'x').unwrap();
                changed[index] = b'y';
                std::fs::write(&path, changed).unwrap();
            }
            4 => {
                let first = part_path(&directory.0, "capture", 0);
                let bytes = std::fs::read(&first).unwrap();
                std::fs::write(&first, &original).unwrap();
                std::fs::write(&path, bytes).unwrap();
            }
            5 => std::fs::remove_file(part_path(&directory.0, "capture", meta.parts.len() - 1))
                .unwrap(),
            _ => unreachable!(),
        }
        let mut reader = RawSeriesReader::open(&directory.0, "capture", limits())
            .await
            .unwrap();
        assert!(read_all(&mut reader).await.is_err());
        assert!(reader.summary().is_none());
        assert!(reader.next_observation().await.is_err());
    }
}
#[tokio::test]
async fn malformed_manifest_identity_budget_and_footer_summary_mismatch_fail() {
    for mode in 0..7 {
        let directory = Directory::new();
        capture(&directory).await;
        let mut meta = manifest(&directory);
        match mode {
            0 => meta.id = "other".into(),
            1 => meta.version = 99,
            2 => meta.parts.clear(),
            3 => meta.bytes += 1,
            4 => meta.events += 1,
            5 => meta.end = RecordingEnd::Stopped,
            6 => meta.parts[0].bytes = 0,
            _ => unreachable!(),
        };
        std::fs::write(
            manifest_path(&directory.0, "capture"),
            serde_json::to_vec(&meta).unwrap(),
        )
        .unwrap();
        match RawSeriesReader::open(&directory.0, "capture", limits()).await {
            Err(_) => assert!(!matches!(mode, 4 | 5)),
            Ok(mut reader) => {
                assert!(matches!(mode, 4 | 5));
                assert!(read_all(&mut reader).await.is_err());
                assert!(reader.summary().is_none());
            }
        }
    }
}
#[tokio::test]
async fn structural_part_limit_or_invalid_row_poison_writer_and_leave_no_successful_manifest() {
    for bad_row in [false, true] {
        let directory = Directory::new();
        let mut bound = limits();
        bound.max_parts = 1;
        let mut writer = RawSeriesWriter::create(&directory.0, "capture", bound)
            .await
            .unwrap();
        let mut rows = observations().await;
        if bad_row {
            rows[0].sequence = 99;
            assert!(writer.append(rows[0].clone()).await.is_err());
        } else {
            assert!(writer.append(rows[0].clone()).await.unwrap());
            let mut failed = false;
            for row in rows.iter().skip(1) {
                if writer.append(row.clone()).await.is_err() {
                    failed = true;
                    break;
                }
            }
            assert!(failed);
        }
        assert!(writer.append(rows[0].clone()).await.is_err());
        assert!(writer.finish(RecordingEnd::Stopped).await.is_err());
        assert!(
            RawSeriesReader::open(&directory.0, "capture", bound)
                .await
                .is_err()
        );
    }
}
#[tokio::test]
async fn overall_observation_limit_has_a_valid_open_session_prefix_and_rejected_row_is_not_written()
{
    let directory = Directory::new();
    let recording =
        RawRecordingLimits::new(RecordingLimits::new(2, 64 * 1024, 2048).unwrap(), 16).unwrap();
    let bound = RawSeriesLimits::new(recording, 2048, 32).unwrap();
    let mut writer = RawSeriesWriter::create(&directory.0, "capture", bound)
        .await
        .unwrap();
    let rows = observations().await;
    assert!(writer.append(rows[0].clone()).await.unwrap());
    assert!(writer.append(rows[1].clone()).await.unwrap());
    assert!(!writer.append(rows[2].clone()).await.unwrap());
    let summary = writer.finish(RecordingEnd::LimitReached).await.unwrap();
    let mut reader = RawSeriesReader::open(&directory.0, "capture", bound)
        .await
        .unwrap();
    assert_eq!(read_all(&mut reader).await.unwrap(), rows[..2]);
    assert_eq!(reader.summary(), Some(summary));
}
#[tokio::test]
async fn create_new_protects_existing_files_and_unfinished_creation_is_not_readable() {
    let directory = Directory::new();
    let writer = RawSeriesWriter::create(&directory.0, "unfinished", limits())
        .await
        .unwrap();
    drop(writer);
    assert!(
        RawSeriesReader::open(&directory.0, "unfinished", limits())
            .await
            .is_err()
    );
    capture(&directory).await;
    let path = manifest_path(&directory.0, "capture");
    let old = std::fs::read(&path).unwrap();
    assert!(
        RawSeriesWriter::create(&directory.0, "capture", limits())
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(&path).unwrap(), old);
    for id in ["", "../escape", "a/b", "a.b", "with space"] {
        assert!(
            RawSeriesWriter::create(&directory.0, id, limits())
                .await
                .is_err()
        );
    }
    assert!(RawSeriesLimits::new(limits().recording, 2047, 32).is_err());
    assert!(RawSeriesLimits::new(limits().recording, 2048, 0).is_err());
    assert!(RawSeriesLimits::new(limits().recording, 2048, 1025).is_err());
}
#[cfg(feature = "trade")]
#[tokio::test]
async fn native_replay_retains_parser_state_across_parts_and_validates_end() {
    let directory = Directory::new();
    let (_, summary) = capture(&directory).await;
    let mut reader = RawSeriesReader::open(&directory.0, "capture", limits())
        .await
        .unwrap();
    let (_stop, shutdown) = watch::channel(false);
    let mut trades = Vec::new();
    let result = reader
        .replay(RawReplayOptions::default(), shutdown, |item| {
            if let RawReplayItem::Market(market) = item {
                let crate::feed::FeedEvent::Trade(trade) = market.event else {
                    panic!("trade")
                };
                trades.push(trade);
            }
            std::future::ready(Ok(()))
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.recording, summary);
    assert_eq!(result.models, 1);
    assert_eq!(trades[0].price.to_string(), "10.1234567890123456789012345");
    assert_eq!(trades[0].received_ts, 1.25);
    assert_eq!(reader.summary(), Some(summary));
}
#[cfg(feature = "trade")]
#[tokio::test]
async fn dropped_or_failed_replay_cannot_restart_and_never_certifies() {
    let directory = Directory::new();
    capture(&directory).await;
    let mut reader = RawSeriesReader::open(&directory.0, "capture", limits())
        .await
        .unwrap();
    let (_stop, shutdown) = watch::channel(false);
    {
        let replay = reader.replay(RawReplayOptions::default(), shutdown, |_| {
            std::future::pending::<Result<()>>()
        });
        tokio::pin!(replay);
        assert!(futures::poll!(replay.as_mut()).is_pending());
    }
    assert!(reader.next_observation().await.is_err());
    assert!(reader.summary().is_none());
    let mut reader = RawSeriesReader::open(&directory.0, "capture", limits())
        .await
        .unwrap();
    let (_stop, shutdown) = watch::channel(false);
    assert!(
        reader
            .replay(RawReplayOptions::default(), shutdown, |_| {
                std::future::ready(Err(Error::Protocol("sink error".into())))
            })
            .await
            .is_err()
    );
    assert!(reader.summary().is_none());
    assert!(reader.next_observation().await.is_err());
}
#[cfg(feature = "orderbook")]
#[tokio::test]
async fn http_bootstrap_processing_and_l2_replay_match_single_file_across_rotation() {
    use crate::exchange::binance::Binance;
    let directory = Directory::new();
    let recording =
        RawRecordingLimits::new(RecordingLimits::new(100, 64 * 1024, 1024).unwrap(), 16).unwrap();
    let bound = RawSeriesLimits::new(recording, 1024, 32).unwrap();
    let info = Arc::new(RawSessionInfo {
        session_id: 1,
        feed: RawFeedInfo::from_feed(
            &Binance::new()
                .l2_book()
                .symbol("BTC-USDT")
                .exchange_symbol("BTCUSDT")
                .build(),
        ),
    });
    let payload = |value| RawPayload::Json {
        value,
        redacted: false,
    };
    let kinds = vec![
        RawObservationKind::Connected,
        RawObservationKind::Received(payload(
            json!({"e":"depthUpdate","s":"BTCUSDT","E":1000,"U":101,"u":101,"b":[["100","3"]],"a":[]}),
        )),
        RawObservationKind::Processing {
            received_sequence: 2,
        },
        RawObservationKind::Received(payload(
            json!({"e":"depthUpdate","s":"BTCUSDT","E":1001,"U":102,"u":102,"b":[["100","4"]],"a":[]}),
        )),
        RawObservationKind::HttpSnapshot {
            symbol: cryptofeed_core::symbol::Symbol::spot("BTC", "USDT"),
            depth: Some(1000),
            payload: payload(json!({"lastUpdateId":100,"bids":[["100","2"]],"asks":[["101","1"]]})),
            received_ts: 1.5,
        },
        RawObservationKind::Processing {
            received_sequence: 4,
        },
        RawObservationKind::Closed { clean: true },
    ];
    let mut writer = RawSeriesWriter::create(&directory.0, "capture", bound)
        .await
        .unwrap();
    let mut bytes = Vec::new();
    let mut single = RawRecordingWriter::new(&mut bytes, recording)
        .await
        .unwrap();
    for (index, kind) in kinds.into_iter().enumerate() {
        let row = RawObservation {
            version: 2,
            sequence: index as u64 + 1,
            elapsed_ns: index as u64 + 1,
            observed_ts: 2.0,
            session: info.clone(),
            kind,
        };
        assert!(writer.append(row.clone()).await.unwrap());
        assert!(single.append(row).await.unwrap());
    }
    writer.finish(RecordingEnd::Complete).await.unwrap();
    single.finish(RecordingEnd::Complete).await.unwrap();
    assert!(manifest(&directory).parts.len() > 2);
    let mut expected = Vec::new();
    let mut plain = RawRecordingReader::new(bytes.as_slice(), recording);
    let (_stop, shutdown) = watch::channel(false);
    let base = plain
        .replay(RawReplayOptions::default(), shutdown, |item| {
            if let RawReplayItem::Market(market) = item {
                expected.push(serde_json::to_value(market.event).unwrap());
            }
            std::future::ready(Ok(()))
        })
        .await
        .unwrap()
        .unwrap();
    let mut actual = Vec::new();
    let mut reader = RawSeriesReader::open(&directory.0, "capture", bound)
        .await
        .unwrap();
    let (_stop, shutdown) = watch::channel(false);
    let rotated = reader
        .replay(RawReplayOptions::default(), shutdown, |item| {
            if let RawReplayItem::Market(market) = item {
                actual.push(serde_json::to_value(market.event).unwrap());
            }
            std::future::ready(Ok(()))
        })
        .await
        .unwrap()
        .unwrap();
    assert!(base.models > 0);
    assert_eq!(actual, expected);
    assert_eq!(rotated, base);
}
