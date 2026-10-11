use super::*;
use crate::{
    TransportConfig,
    recording::{RecordingEnd, RecordingLimits},
};
const URL: &str = "https://openapi.okx.com/api/v5/public/instruments?instType=SPOT";
fn request(id: u64, url: &str) -> HttpObservationKind {
    HttpObservationKind::Request {
        request_id: id,
        exchange: ExchangeId::Okx,
        url: url.into(),
    }
}
fn finish(id: u64, value: Value) -> HttpObservationKind {
    HttpObservationKind::Finished {
        request_id: id,
        outcome: HttpOutcome::Json {
            value,
            redacted: false,
        },
    }
}
async fn bytes(kinds: Vec<HttpObservationKind>, end: RecordingEnd) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut writer = HttpRecordingWriter::new(&mut bytes, RecordingLimits::default())
        .await
        .unwrap();
    for (index, kind) in kinds.into_iter().enumerate() {
        writer
            .append(HttpObservation {
                sequence: index as u64 + 1,
                elapsed_ns: index as u64 + 1,
                observed_ts: 100.0,
                kind,
            })
            .await
            .unwrap();
    }
    writer.finish(end).await.unwrap();
    bytes
}
async fn archive(kinds: Vec<HttpObservationKind>, end: RecordingEnd) -> HttpReplay {
    let bytes = bytes(kinds, end).await;
    HttpRecordingReader::new(bytes.as_slice(), RecordingLimits::default())
        .into_replay()
        .await
        .unwrap()
}
#[tokio::test]
async fn repeated_url_consumes_invocation_order_not_completion_order_and_clones_share_cursor() {
    let replay = archive(
        vec![
            request(1, URL),
            request(2, URL),
            finish(2, serde_json::json!({"value":2})),
            finish(
                1,
                serde_json::from_str(r#"{"value":100.1234567890123456789012345}"#).unwrap(),
            ),
        ],
        RecordingEnd::Complete,
    )
    .await;
    assert_eq!(
        replay.take(ExchangeId::Okx, URL, 4096).unwrap()["value"].to_string(),
        "100.1234567890123456789012345"
    );
    assert_eq!(
        replay.clone().take(ExchangeId::Okx, URL, 4096).unwrap()["value"],
        2
    );
    assert_eq!(replay.verify_complete().unwrap().consumed, 2);
    assert!(replay.take(ExchangeId::Okx, URL, 4096).is_err());
    assert_eq!(replay.report().mismatches, 1);
    assert!(replay.verify_complete().is_err());
}
#[tokio::test]
async fn pending_head_never_borrows_later_success_and_unrelated_keys_remain_usable() {
    let second = "https://openapi.okx.com/api/v5/market/ticker?instId=BTC-USDT";
    let replay = archive(
        vec![
            request(1, URL),
            request(2, URL),
            request(3, second),
            finish(2, serde_json::json!({"value":2})),
            finish(3, serde_json::json!({"value":3})),
        ],
        RecordingEnd::Stopped,
    )
    .await;
    assert!(replay.take(ExchangeId::Okx, URL, 4096).is_err());
    assert!(replay.take(ExchangeId::Okx, URL, 4096).is_err());
    assert_eq!(
        replay.take(ExchangeId::Okx, second, 4096).unwrap()["value"],
        3
    );
    assert_eq!(replay.report().consumed, 1);
    assert_eq!(replay.report().unfinished, 1);
    assert!(replay.verify_complete().is_err());
}
#[tokio::test]
async fn recorded_failures_and_cancellation_are_explicit_outcomes_not_silently_skipped() {
    let replay = archive(
        vec![
            request(1, URL),
            HttpObservationKind::Finished {
                request_id: 1,
                outcome: HttpOutcome::Failure {
                    kind: RawSnapshotFailure::HttpStatus { status: 429 },
                    retry_after: Some(std::time::Duration::new(2, 3)),
                    configuration_error: None,
                },
            },
            request(2, URL),
            HttpObservationKind::Cancelled { request_id: 2 },
            request(3, URL),
            finish(3, serde_json::json!({"value":3})),
        ],
        RecordingEnd::Complete,
    )
    .await;
    assert!(
        matches!(replay.take(ExchangeId::Okx,URL,4096),Err(Error::HttpStatus {status:429,retry_after:Some(delay)}) if delay==std::time::Duration::new(2,3))
    );
    assert!(matches!(
        replay.take(ExchangeId::Okx, URL, 4096),
        Err(Error::InvalidConfiguration(_))
    ));
    assert_eq!(replay.take(ExchangeId::Okx, URL, 4096).unwrap()["value"], 3);
    let report = replay.verify_complete().unwrap();
    assert_eq!((report.consumed, report.cancelled), (3, 1));
}
#[tokio::test]
async fn wrong_scope_missing_url_and_body_budget_do_not_consume_or_fall_back() {
    let replay = archive(
        vec![
            request(1, URL),
            finish(1, serde_json::json!({"padding":"x".repeat(1000)})),
        ],
        RecordingEnd::Complete,
    )
    .await;
    assert!(replay.take(ExchangeId::Binance, URL, 4096).is_err());
    assert!(
        replay
            .take(ExchangeId::Okx, &URL.replace("SPOT", "SWAP"), 4096)
            .is_err()
    );
    assert!(replay.take(ExchangeId::Okx, URL, 16).is_err());
    assert_eq!(replay.report().consumed, 0);
    assert!(replay.take(ExchangeId::Okx, URL, 4096).is_ok());
    assert_eq!(replay.report().mismatches, 3);
    assert!(replay.verify_complete().is_err());
}
#[tokio::test]
async fn invalid_or_partially_read_files_cannot_publish_an_archive() {
    let bytes = bytes(
        vec![request(1, URL), finish(1, serde_json::json!({"value":1}))],
        RecordingEnd::Complete,
    )
    .await;
    let mut reader = HttpRecordingReader::new(bytes.as_slice(), RecordingLimits::default());
    reader.next_observation().await.unwrap();
    assert!(reader.into_replay().await.is_err());
    let truncated = &bytes[..bytes.len() - 1];
    assert!(
        HttpRecordingReader::new(truncated, RecordingLimits::default())
            .into_replay()
            .await
            .is_err()
    );
}
#[tokio::test(start_paused = true)]
async fn snapshot_entrypoint_intercepts_before_http_limiter_or_client_and_ws_is_disabled() {
    let replay = archive(
        vec![request(1, URL), finish(1, serde_json::json!({"value":1}))],
        RecordingEnd::Complete,
    )
    .await;
    let transport = TransportConfig::direct().with_http_replay(replay.clone());
    let now = tokio::time::Instant::now();
    let value = crate::runtime::snapshot::fetch_json_with_limits(
        URL,
        &transport,
        ExchangeId::Okx,
        std::time::Duration::from_secs(1),
        4096,
    )
    .await
    .unwrap();
    assert_eq!(value["value"], 1);
    assert_eq!(now.elapsed(), std::time::Duration::ZERO);
    assert!(transport.http_client().is_err());
    assert!(matches!(
        transport
            .websocket(
                &Url::parse("wss://127.0.0.1:9/").unwrap(),
                tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
            )
            .await,
        Err(Error::UnsupportedCapability(_))
    ));
    assert!(replay.verify_complete().is_ok());
    assert!(
        crate::runtime::snapshot::fetch_json_with_limits(
            URL,
            &transport,
            ExchangeId::Okx,
            std::time::Duration::from_secs(1),
            4096
        )
        .await
        .is_err()
    );
}
#[tokio::test]
async fn replay_cache_namespaces_are_isolated_and_proxy_auth_cannot_enable_network() {
    let data = bytes(
        vec![request(1, URL), finish(1, serde_json::json!({"value":1}))],
        RecordingEnd::Complete,
    )
    .await;
    let first = HttpRecordingReader::new(data.as_slice(), RecordingLimits::default())
        .into_replay()
        .await
        .unwrap();
    let second = HttpRecordingReader::new(data.as_slice(), RecordingLimits::default())
        .into_replay()
        .await
        .unwrap();
    let transport = TransportConfig::http_proxy("http://127.0.0.1:8888")
        .unwrap()
        .with_http_replay(first);
    let authenticated = transport.clone().basic_auth("user", "pass").unwrap();
    assert!(authenticated.http_client().is_err());
    assert_eq!(transport.cache_key(URL), transport.clone().cache_key(URL));
    assert_ne!(
        transport.cache_key(URL),
        TransportConfig::direct()
            .with_http_replay(second)
            .cache_key(URL)
    );
}
#[cfg(all(feature = "ticker", feature = "orderbook", feature = "trade"))]
#[tokio::test]
async fn existing_catalog_and_public_rest_apis_decode_offline_payloads_and_cache_hits_consume_nothing()
 {
    use crate::{MarketCatalog, PublicRestClient};
    use cryptofeed_core::symbol::{InstrumentKind, Symbol};
    let ticker = "https://openapi.okx.com/api/v5/market/ticker?instId=BTC-USDT";
    let book = "https://openapi.okx.com/api/v5/market/books?instId=BTC-USDT&sz=5";
    let trades = "https://openapi.okx.com/api/v5/market/trades?instId=BTC-USDT&limit=5";
    let replay=archive(vec![request(1,URL),finish(1,serde_json::json!({"code":"0","msg":"","data":[{"instType":"SPOT","instId":"BTC-USDT","baseCcy":"BTC","quoteCcy":"USDT","state":"live","tickSz":"0.01","lotSz":"0.0001"}]})),request(2,ticker),finish(2,serde_json::json!({"code":"0","msg":"","data":[{"instId":"BTC-USDT","bidPx":"100.01","askPx":"100.02","ts":"1000"}]})),request(3,book),finish(3,serde_json::json!({"code":"0","msg":"","data":[{"bids":[["100.01","2","0","1"]],"asks":[["100.02","3","0","1"]],"ts":"1000","seqId":7}]})),request(4,trades),finish(4,serde_json::json!({"code":"0","msg":"","data":[{"instId":"BTC-USDT","tradeId":"1","px":"100.01","sz":"0.2","side":"buy","ts":"1000"}]}))],RecordingEnd::Complete).await;
    let transport = TransportConfig::direct().with_http_replay(replay.clone());
    let client = PublicRestClient::load_with_transport(
        ExchangeId::Okx,
        InstrumentKind::Spot,
        transport.clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        client.catalog().symbols(),
        vec![Symbol::spot("BTC", "USDT")]
    );
    assert_eq!(replay.report().consumed, 1);
    let again =
        MarketCatalog::load_with_transport(ExchangeId::Okx, InstrumentKind::Spot, &transport)
            .await
            .unwrap();
    assert_eq!(again.symbols(), client.catalog().symbols());
    assert_eq!(replay.report().consumed, 1);
    let symbol = Symbol::spot("BTC", "USDT");
    assert_eq!(
        client.ticker(&symbol).await.unwrap().data.bid.to_string(),
        "100.01"
    );
    assert_eq!(
        client.l2_book(&symbol, 5).await.unwrap().data.bids[0]
            .amount
            .to_string(),
        "2"
    );
    assert_eq!(
        client.recent_trades(&symbol, 5).await.unwrap()[0]
            .amount
            .to_string(),
        "0.2"
    );
    assert_eq!(replay.verify_complete().unwrap().consumed, 4);
}
#[tokio::test]
async fn cancelled_catalog_call_cannot_automatically_borrow_the_next_explicit_calls_response() {
    use crate::MarketCatalog;
    use cryptofeed_core::symbol::InstrumentKind;
    let data = serde_json::json!({"code":"0","msg":"","data":[{"instType":"SPOT","instId":"BTC-USDT","baseCcy":"BTC","quoteCcy":"USDT","state":"live"}]});
    let replay = archive(
        vec![
            request(1, URL),
            HttpObservationKind::Cancelled { request_id: 1 },
            request(2, URL),
            finish(2, data),
        ],
        RecordingEnd::Complete,
    )
    .await;
    let transport = TransportConfig::direct().with_http_replay(replay.clone());
    assert!(matches!(
        MarketCatalog::load_with_transport(ExchangeId::Okx, InstrumentKind::Spot, &transport).await,
        Err(Error::InvalidConfiguration(_))
    ));
    assert_eq!(replay.report().consumed, 1);
    assert!(
        MarketCatalog::load_with_transport(ExchangeId::Okx, InstrumentKind::Spot, &transport)
            .await
            .is_ok()
    );
    assert_eq!(replay.verify_complete().unwrap().consumed, 2);
}
#[tokio::test]
async fn real_recorded_transport_failure_retries_but_cancellation_on_retry_is_terminal() {
    use crate::MarketCatalog;
    use cryptofeed_core::symbol::InstrumentKind;
    for cancelled in [false, true] {
        let data = serde_json::json!({"code":"0","msg":"","data":[{"instType":"SPOT","instId":"BTC-USDT","baseCcy":"BTC","quoteCcy":"USDT","state":"live"}]});
        let mut kinds = vec![
            request(1, URL),
            HttpObservationKind::Finished {
                request_id: 1,
                outcome: HttpOutcome::Failure {
                    kind: RawSnapshotFailure::Transport,
                    retry_after: None,
                    configuration_error: None,
                },
            },
            request(2, URL),
        ];
        if cancelled {
            kinds.extend([
                HttpObservationKind::Cancelled { request_id: 2 },
                request(3, URL),
                finish(3, data),
            ]);
        } else {
            kinds.push(finish(2, data));
        }
        let replay = archive(kinds, RecordingEnd::Complete).await;
        let transport = TransportConfig::direct().with_http_replay(replay.clone());
        let result =
            MarketCatalog::load_with_transport(ExchangeId::Okx, InstrumentKind::Spot, &transport)
                .await;
        if cancelled {
            assert!(matches!(result, Err(Error::InvalidConfiguration(_))));
            assert_eq!(replay.report().consumed, 2);
            assert!(
                MarketCatalog::load_with_transport(
                    ExchangeId::Okx,
                    InstrumentKind::Spot,
                    &transport
                )
                .await
                .is_ok()
            );
        } else {
            assert!(result.is_ok());
        }
        assert_eq!(
            replay.verify_complete().unwrap().consumed,
            if cancelled { 3 } else { 2 }
        );
    }
}
#[tokio::test]
async fn rerecorded_replay_configuration_errors_remain_terminal_and_v1_cannot_claim_them() {
    use crate::MarketCatalog;
    use cryptofeed_core::symbol::InstrumentKind;
    let data = serde_json::json!({"code":"0","msg":"","data":[{"instType":"SPOT","instId":"BTC-USDT","baseCcy":"BTC","quoteCcy":"USDT","state":"live"}]});
    let original = archive(
        vec![
            request(1, URL),
            HttpObservationKind::Cancelled { request_id: 1 },
            request(2, URL),
            finish(2, data),
        ],
        RecordingEnd::Complete,
    )
    .await;
    let (capture, mut input) = http_capture_channel(8, 4096).unwrap();
    let transport = TransportConfig::direct()
        .with_http_replay(original)
        .with_http_capture(capture);
    assert!(
        MarketCatalog::load_with_transport(ExchangeId::Okx, InstrumentKind::Spot, &transport)
            .await
            .is_err()
    );
    assert!(
        MarketCatalog::load_with_transport(ExchangeId::Okx, InstrumentKind::Spot, &transport)
            .await
            .is_ok()
    );
    drop(transport);
    let mut saved = Vec::new();
    let mut writer = HttpRecordingWriter::new(&mut saved, RecordingLimits::default())
        .await
        .unwrap();
    while let Some(row) = input.recv().await.unwrap() {
        writer.append(row).await.unwrap();
    }
    writer.finish(RecordingEnd::Complete).await.unwrap();
    let replay = HttpRecordingReader::new(saved.as_slice(), RecordingLimits::default())
        .into_replay()
        .await
        .unwrap();
    let transport = TransportConfig::direct().with_http_replay(replay.clone());
    assert!(matches!(
        MarketCatalog::load_with_transport(ExchangeId::Okx, InstrumentKind::Spot, &transport).await,
        Err(Error::InvalidConfiguration(_))
    ));
    assert_eq!(replay.report().consumed, 1);
    assert!(
        MarketCatalog::load_with_transport(ExchangeId::Okx, InstrumentKind::Spot, &transport)
            .await
            .is_ok()
    );
    assert!(replay.verify_complete().is_ok());
    let old = String::from_utf8(saved)
        .unwrap()
        .replacen("\"version\":2", "\"version\":1", 1);
    assert!(
        HttpRecordingReader::new(old.as_bytes(), RecordingLimits::default())
            .into_replay()
            .await
            .is_err()
    );
}
