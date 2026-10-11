use super::*;
fn cost(path: &str) -> u32 {
    charge(
        ExchangeId::Binance,
        &format!("https://api.binance.com/api/v3/{path}"),
    )
    .unwrap()
    .unwrap()
    .cost
}
#[test]
fn official_spot_costs_and_depth_boundaries_use_current_weights() {
    for (path, expected) in [
        ("exchangeInfo", 20),
        ("trades?symbol=BTCUSDT", 25),
        ("aggTrades?symbol=BTCUSDT", 4),
        ("klines?symbol=BTCUSDT", 2),
        ("ticker/bookTicker?symbol=BTCUSDT", 2),
        ("ticker/bookTicker", 4),
        ("ticker/bookTicker?symbols=%5B%22BTCUSDT%22%5D", 4),
    ] {
        assert_eq!(cost(path), expected);
    }
    for (limit, expected) in [
        (1, 5),
        (100, 5),
        (101, 25),
        (500, 25),
        (501, 50),
        (1000, 50),
        (1001, 250),
        (5000, 250),
    ] {
        assert_eq!(cost(&format!("depth?limit={limit}")), expected);
    }
}
#[test]
fn derivative_tiers_and_funding_windows_are_not_copied_from_spot() {
    for host in ["fapi.binance.com", "dapi.binance.com"] {
        let prefix = if host.starts_with('f') {
            "fapi"
        } else {
            "dapi"
        };
        for (path, expected) in [
            ("exchangeInfo", 1),
            ("trades", 5),
            ("aggTrades", 20),
            ("ticker/bookTicker?symbol=BTCUSDT", 2),
            ("ticker/bookTicker", 5),
            ("depth?limit=50", 2),
            ("depth?limit=100", 5),
            ("depth?limit=500", 10),
            ("depth?limit=1000", 20),
            ("klines?limit=99", 1),
            ("klines?limit=100", 2),
            ("klines?limit=499", 2),
            ("klines?limit=500", 5),
            ("klines?limit=1000", 5),
            ("klines?limit=1001", 10),
        ] {
            assert_eq!(
                charge(
                    ExchangeId::Binance,
                    &format!("https://{host}/{prefix}/v1/{path}")
                )
                .unwrap()
                .unwrap()
                .cost,
                expected
            );
        }
    }
    let um = charge(
        ExchangeId::Binance,
        "https://fapi.binance.com/fapi/v1/fundingRate",
    )
    .unwrap()
    .unwrap();
    assert_eq!(um.bucket, Bucket::UsdFunding);
    assert_eq!(um.bucket.defaults(), (500, Duration::from_secs(300)));
    assert_eq!(
        charge(
            ExchangeId::Binance,
            "https://dapi.binance.com/dapi/v1/fundingRate"
        )
        .unwrap()
        .unwrap()
        .bucket,
        Bucket::Futures
    );
}
#[test]
fn unknown_or_malformed_binance_routes_never_guess_a_low_cost() {
    for url in [
        "https://api.binance.com/api/v3/order",
        "https://api.binance.com/api/v3/depth?limit=0",
        "https://api.binance.com/api/v3/depth?limit=5001",
        "https://api.binance.com/api/v3/depth?limit=abc",
        "https://fapi.binance.com/fapi/v1/depth?limit=99",
        "https://api.binance.com/api/v3/depth?limit=1&limit=2",
        "https://unknown.example/api/v3/depth",
    ] {
        assert!(charge(ExchangeId::Binance, url).is_err());
    }
    assert!(
        charge(
            ExchangeId::Okx,
            "https://openapi.okx.com/api/v5/market/books"
        )
        .unwrap()
        .is_none()
    );
}
#[tokio::test(start_paused = true)]
async fn rolling_window_checks_the_whole_interval_not_only_reset_boundaries() {
    let now = Instant::now();
    let mut window = Window {
        capacity: 10,
        duration: Duration::from_secs(60),
        used: 0,
        entries: VecDeque::new(),
    };
    assert_eq!(window.ready(now, 6, 10).unwrap(), now);
    window.commit(now, 6);
    let later = now + Duration::from_secs(59);
    assert_eq!(window.ready(later, 4, 10).unwrap(), later);
    window.commit(later, 4);
    assert_eq!(
        window.ready(later, 5, 10).unwrap(),
        now + Duration::from_secs(60)
    );
    assert_eq!(
        window.ready(now + Duration::from_secs(60), 7, 10).unwrap(),
        later + Duration::from_secs(60)
    );
    assert!(window.ready(now, 11, 10).is_err());
}
#[tokio::test(start_paused = true)]
async fn directory_limits_can_only_tighten_and_private_or_other_windows_are_ignored() {
    let mut tracker = Tracker::default();
    let url = "https://api.binance.com/api/v3/exchangeInfo";
    tracker.observe_catalog(ExchangeId::Binance,url,&serde_json::json!({"symbols":[],"rateLimits":[{"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":100}]}));
    assert_eq!(tracker.windows[&Bucket::Spot].capacity, 100);
    tracker.observe_catalog(ExchangeId::Binance,url,&serde_json::json!({"symbols":[],"rateLimits":[{"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":99999},{"rateLimitType":"ORDERS","interval":"MINUTE","intervalNum":1,"limit":1}]}));
    assert_eq!(tracker.windows[&Bucket::Spot].capacity, 100);
}
