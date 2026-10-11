use super::*;
use crate::recording::{RecordingEnd, RecordingLimits};
const URL: &str = "https://openapi.okx.com/api/v5/public/instruments?instType=SPOT";
async fn observations() -> Vec<HttpObservation> {
    let (capture, mut input) = http_capture_channel(16, 4096).unwrap();
    let first = capture.begin(URL, ExchangeId::Okx).unwrap();
    let second = capture.begin(URL, ExchangeId::Okx).unwrap();
    second.finish(&Ok(serde_json::from_str(
        r#"{"price":100.1234567890123456789012345,"api_key":"private-value","msg":"diagnostic"}"#,
    )
    .unwrap()));
    first.finish(&Err(Error::HttpStatus {
        status: 429,
        retry_after: Some(std::time::Duration::from_secs(2)),
    }));
    drop(capture.begin(URL, ExchangeId::Okx).unwrap());
    drop(capture);
    let mut records = Vec::new();
    while let Some(row) = input.recv().await.unwrap() {
        records.push(row);
    }
    records
}
#[tokio::test]
async fn request_identity_completion_order_cancellation_and_safe_payload_are_recorded() {
    let rows = observations().await;
    assert_eq!(rows.len(), 6);
    assert!(matches!(
        rows[2].kind,
        HttpObservationKind::Finished { request_id: 2, .. }
    ));
    let HttpObservationKind::Finished {
        outcome: HttpOutcome::Json { value, redacted },
        ..
    } = &rows[2].kind
    else {
        panic!("JSON")
    };
    assert!(*redacted);
    assert_eq!(value["api_key"], "[REDACTED]");
    assert_eq!(value["msg"], "[REDACTED]");
    assert_eq!(value["price"].to_string(), "100.1234567890123456789012345");
    assert!(matches!(
        rows[3].kind,
        HttpObservationKind::Finished {
            outcome: HttpOutcome::Failure {
                kind: RawSnapshotFailure::HttpStatus { status: 429 },
                retry_after: Some(delay),
                ..
            },
            ..
        } if delay == std::time::Duration::from_secs(2)
    ));
    assert!(matches!(
        rows[5].kind,
        HttpObservationKind::Cancelled { request_id: 3 }
    ));
    assert!(
        rows.windows(2)
            .all(|pair| pair[1].sequence == pair[0].sequence + 1
                && pair[1].elapsed_ns >= pair[0].elapsed_ns)
    );
}
#[tokio::test]
async fn bounded_codec_roundtrip_retains_exact_values_and_full_request_lifecycle() {
    let rows = observations().await;
    let mut bytes = Vec::new();
    let mut writer = HttpRecordingWriter::new(&mut bytes, RecordingLimits::default())
        .await
        .unwrap();
    for row in &rows {
        assert!(writer.append(row.clone()).await.unwrap());
    }
    let summary = writer.finish(RecordingEnd::Complete).await.unwrap();
    assert!(!String::from_utf8_lossy(&bytes).contains("private-value"));
    let mut reader = HttpRecordingReader::new(bytes.as_slice(), RecordingLimits::default());
    let mut decoded = Vec::new();
    while let Some(row) = reader.next_observation().await.unwrap() {
        decoded.push(row);
    }
    assert_eq!(decoded, rows);
    assert_eq!(reader.summary(), Some(summary));
}
#[tokio::test]
async fn incomplete_requests_need_explicit_prefix_and_bad_identity_or_private_payload_poison_writer()
 {
    let rows = observations().await;
    let mut bytes = Vec::new();
    let mut writer = HttpRecordingWriter::new(&mut bytes, RecordingLimits::default())
        .await
        .unwrap();
    writer.append(rows[0].clone()).await.unwrap();
    assert!(writer.finish(RecordingEnd::Complete).await.is_err());
    let mut bytes = Vec::new();
    let mut writer = HttpRecordingWriter::new(&mut bytes, RecordingLimits::default())
        .await
        .unwrap();
    writer.append(rows[0].clone()).await.unwrap();
    writer.finish(RecordingEnd::Stopped).await.unwrap();
    let mut reader = HttpRecordingReader::new(bytes.as_slice(), RecordingLimits::default());
    assert!(reader.next_observation().await.unwrap().is_some());
    assert!(reader.next_observation().await.unwrap().is_none());
    for mode in 0..3 {
        let mut bytes = Vec::new();
        let mut writer = HttpRecordingWriter::new(&mut bytes, RecordingLimits::default())
            .await
            .unwrap();
        let mut row = rows[0].clone();
        match mode {
            0 => row.sequence = 99,
            1 => row.observed_ts = f64::NAN,
            2 => {
                let HttpObservationKind::Request { url, .. } = &mut row.kind else {
                    unreachable!()
                };
                *url = "https://openapi.okx.com/api/v5/account/balance".into();
            }
            _ => unreachable!(),
        };
        assert!(writer.append(row).await.is_err());
        assert!(writer.finish(RecordingEnd::Stopped).await.is_err());
    }
}
#[tokio::test]
async fn manifest_free_jsonl_rejects_truncation_extra_input_missing_or_duplicate_completion_and_unsafe_data()
 {
    let rows = observations().await;
    let mut bytes = Vec::new();
    let mut writer = HttpRecordingWriter::new(&mut bytes, RecordingLimits::default())
        .await
        .unwrap();
    for row in rows {
        writer.append(row).await.unwrap();
    }
    writer.finish(RecordingEnd::Complete).await.unwrap();
    let original: Vec<Value> = String::from_utf8(bytes)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    for mode in 0..9 {
        let mut lines = original.clone();
        match mode {
            0 => {
                lines.pop();
            }
            1 => lines.push(lines.last().unwrap().clone()),
            2 => lines[0]["header"]["version"] = 99.into(),
            3 => lines[3]["observation"]["record"]["kind"]["finished"]["request_id"] = 99.into(),
            4 => {
                lines[3]["observation"]["record"]["kind"]["finished"]["outcome"]["json"]["value"]
                    ["api_key"] = "secret".into()
            }
            5 => {
                lines[1]["observation"]["record"]["kind"]["request"]["url"] =
                    "https://openapi.okx.com/api/v5/account/balance".into()
            }
            6 => lines[7]["end"]["events"] = 99.into(),
            7 => {
                lines[4]["observation"]["record"]["kind"]["finished"]["outcome"]["failure"]["retry_after"]
                    ["api_key"] = "secret".into()
            }
            8 => lines[4]["observation"]["record"]["kind"]["finished"]["request_id"] = 2.into(),
            _ => unreachable!(),
        }
        let bytes = lines
            .into_iter()
            .map(|line| line.to_string() + "\n")
            .collect::<String>();
        let mut reader = HttpRecordingReader::new(bytes.as_bytes(), RecordingLimits::default());
        let mut failed = false;
        for _ in 0..10 {
            match reader.next_observation().await {
                Err(_) => {
                    failed = true;
                    break;
                }
                Ok(None) => break,
                Ok(Some(_)) => {}
            }
        }
        assert!(failed);
        assert!(reader.summary().is_none());
        assert!(reader.next_observation().await.is_err());
    }
}
#[tokio::test]
async fn overflow_oversize_private_url_and_private_operation_are_sticky_without_blocking_caller() {
    for mode in 0..4 {
        let (capture, mut input) = http_capture_channel(1, 512).unwrap();
        match mode {
            0 => {
                let guard = capture.begin(URL, ExchangeId::Okx).unwrap();
                guard.finish(&Ok(serde_json::json!({"value":1})));
            }
            1 => {
                capture.begin(
                    "https://openapi.okx.com/api/v5/account/balance",
                    ExchangeId::Okx,
                );
            }
            2 => {
                let guard = capture.begin(URL, ExchangeId::Okx).unwrap();
                let _ = input.recv().await.unwrap();
                guard.finish(&Ok(serde_json::json!({"padding":"x".repeat(1000)})));
            }
            3 => {
                let guard = capture.begin(URL, ExchangeId::Okx).unwrap();
                let _ = input.recv().await.unwrap();
                guard.finish(&Ok(serde_json::json!({"op":"login"})));
            }
            _ => unreachable!(),
        }
        assert!(input.recv().await.is_err());
        assert!(capture.begin(URL, ExchangeId::Okx).is_none());
    }
}
#[test]
fn only_current_public_sdk_routes_and_safe_query_keys_are_accepted() {
    for (exchange, url) in [
        (
            ExchangeId::Binance,
            "https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT&interval=1m",
        ),
        (
            ExchangeId::Bitget,
            "https://api.bitget.com/api/v3/market/instruments?category=SPOT",
        ),
        (
            ExchangeId::Bybit,
            "https://api.bybit.com/v5/market/instruments-info?category=linear&cursor=opaque",
        ),
        (ExchangeId::Okx, URL),
        (
            ExchangeId::Gateio,
            "https://api.gateio.ws/api/v4/futures/usdt/trades?contract=BTC_USDT&offset=100&from=1&to=2",
        ),
    ] {
        public_url(exchange, url).unwrap();
    }
    for url in [
        "http://openapi.okx.com/api/v5/public/instruments",
        "https://user:pass@openapi.okx.com/api/v5/public/instruments",
        "https://openapi.okx.com/api/v5/public/instruments?api_key=secret",
        "https://openapi.okx.com/api/v5/public/instruments?instType=SPOT&instType=SWAP",
        "https://openapi.okx.com/api/v5/public/instruments#secret",
        "https://evil.example/api/v5/public/instruments",
    ] {
        assert!(public_url(ExchangeId::Okx, url).is_err());
    }
    assert!(public_url(ExchangeId::Binance, "https://api.binance.com/api/v3/order").is_err());
    assert!(
        public_url(
            ExchangeId::Bitget,
            "https://api.bitget.com/api/v2/spot/public/symbols"
        )
        .is_err()
    );
}
#[tokio::test]
async fn transport_clones_share_capture_cache_scope_proxy_auth_keeps_it_and_new_capture_isolated() {
    let (capture, _input) = http_capture_channel(8, 4096).unwrap();
    let transport = crate::TransportConfig::direct().with_http_capture(capture.clone());
    assert_eq!(transport.cache_key(URL), transport.clone().cache_key(URL));
    assert_ne!(
        transport.cache_key(URL),
        crate::TransportConfig::direct().cache_key(URL)
    );
    let (second, _input) = http_capture_channel(8, 4096).unwrap();
    assert_ne!(
        transport.cache_key(URL),
        crate::TransportConfig::direct()
            .with_http_capture(second)
            .cache_key(URL)
    );
    let proxy = crate::TransportConfig::http_proxy("http://127.0.0.1:8888")
        .unwrap()
        .with_http_capture(capture)
        .basic_auth("user", "pass")
        .unwrap();
    let guard = proxy.begin_http(URL, ExchangeId::Okx);
    assert!(guard.is_some());
}
#[tokio::test]
async fn failure_without_a_queued_record_wakes_an_already_waiting_receiver() {
    let (capture, mut input) = http_capture_channel(8, 4096).unwrap();
    let waiting = input.recv();
    tokio::pin!(waiting);
    assert!(futures::poll!(waiting.as_mut()).is_pending());
    assert!(
        capture
            .begin(
                "https://openapi.okx.com/api/v5/account/balance",
                ExchangeId::Okx
            )
            .is_none()
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .is_err()
    );
}
