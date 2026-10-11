use super::*;
#[test]
fn policies_cannot_relax_sdk_capacities_or_start_intervals() {
    let base = ResourcePolicy::default();
    assert_eq!(base.http_capacity(), 4);
    assert_eq!(base.websocket_capacity(), 100);
    for limit in [0, 5] {
        assert!(base.http(limit, HTTP_INTERVAL).is_err());
    }
    for limit in [0, 101] {
        assert!(base.websocket(limit, WS_INTERVAL).is_err());
    }
    assert!(base.http(1, Duration::from_millis(999)).is_err());
    assert!(base.websocket(1, Duration::from_millis(1099)).is_err());
    assert!(base.http(1, Duration::from_secs(86401)).is_err());
    assert!(base.websocket(1, Duration::MAX).is_err());
    assert_eq!(
        base.http(1, Duration::from_secs(2))
            .unwrap()
            .http_start_interval(),
        Duration::from_secs(2)
    );
}
#[test]
fn clones_share_gates_per_exchange_and_auth_rebuilding_preserves_the_scope() {
    let budget = ResourceBudget::new(ResourcePolicy::default().websocket(1, WS_INTERVAL).unwrap());
    assert_eq!(budget.id(), budget.clone().id());
    assert_ne!(
        budget.id(),
        ResourceBudget::new(ResourcePolicy::default()).id()
    );
    let binance = budget.websocket(ExchangeId::Binance);
    assert!(Arc::ptr_eq(
        &binance,
        &budget.clone().websocket(ExchangeId::Binance)
    ));
    let held = binance.slots.clone().try_acquire_owned().unwrap();
    assert!(
        budget
            .clone()
            .websocket(ExchangeId::Binance)
            .slots
            .clone()
            .try_acquire_owned()
            .is_err()
    );
    assert!(
        budget
            .websocket(ExchangeId::Okx)
            .slots
            .clone()
            .try_acquire_owned()
            .is_ok()
    );
    drop(held);
    let transport = crate::TransportConfig::http_proxy("http://127.0.0.1:8888")
        .unwrap()
        .with_resource_budget(budget.clone())
        .basic_auth("user", "pass")
        .unwrap();
    assert_eq!(transport.resource_budget().unwrap().id(), budget.id());
}
#[test]
fn known_shared_connection_plans_fail_before_connecting_while_independent_groups_fit() {
    use crate::{TransportConfig, exchange::binance::Binance};
    let shared = ResourceBudget::new(ResourcePolicy::default().websocket(1, WS_INTERVAL).unwrap());
    let first = Binance::new()
        .trade()
        .symbol("BTC-USDT")
        .exchange_symbol("BTCUSDT")
        .transport(TransportConfig::direct().with_resource_budget(shared.clone()))
        .build();
    let second = Binance::new()
        .trade()
        .symbol("ETH-USDT")
        .exchange_symbol("ETHUSDT")
        .transport(TransportConfig::direct().with_resource_budget(shared))
        .build();
    assert!(
        crate::runtime::planning::validate_connection_counts(&[first.clone(), second.clone()])
            .is_err()
    );
    let independent =
        ResourceBudget::new(ResourcePolicy::default().websocket(1, WS_INTERVAL).unwrap());
    let mut second = second;
    second.transport = TransportConfig::direct().with_resource_budget(independent);
    assert!(crate::runtime::planning::validate_connection_counts(&[first, second]).is_ok());
}
#[cfg(feature = "recording")]
#[tokio::test]
async fn offline_ws_rejection_occurs_before_resource_reservation() {
    use crate::recording::{
        RecordingEnd, RecordingLimits,
        http::{HttpRecordingReader, HttpRecordingWriter},
    };
    let mut bytes = Vec::new();
    HttpRecordingWriter::new(&mut bytes, RecordingLimits::default())
        .await
        .unwrap()
        .finish(RecordingEnd::Complete)
        .await
        .unwrap();
    let replay = HttpRecordingReader::new(bytes.as_slice(), RecordingLimits::default())
        .into_replay()
        .await
        .unwrap();
    let budget = ResourceBudget::new(ResourcePolicy::default());
    let transport = crate::TransportConfig::direct()
        .with_resource_budget(budget.clone())
        .with_http_replay(replay);
    let clock = *budget.http().next.lock().await;
    assert!(
        crate::runtime::snapshot::fetch_json_with_limits(
            "https://openapi.okx.com/api/v5/public/instruments?instType=SPOT",
            &transport,
            ExchangeId::Okx,
            Duration::from_secs(1),
            4096,
        )
        .await
        .is_err()
    );
    assert_eq!(*budget.http().next.lock().await, clock);
    assert_eq!(budget.http().slots.available_permits(), HTTP_LIMIT);
    let connection = crate::runtime::connection::WsConnection::new(
        url::Url::parse("wss://127.0.0.1:9/").unwrap(),
        ExchangeId::Okx,
    )
    .transport(transport);
    assert!(matches!(
        connection.connect().await,
        Err(Error::UnsupportedCapability(_))
    ));
    assert!(budget.0.websockets.lock().unwrap().is_empty());
}
